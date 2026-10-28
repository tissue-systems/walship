//! axum routes that put a [`Receiver`] on the network.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use walship::{server::Routes, Receiver};
//!
//! let receiver = Arc::new(Receiver::new("/var/lib/myapp/replicas")?);
//! let app = Routes::new(receiver).token("change me").into_router();
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:7350").await?;
//! axum::serve(listener, app).await?;
//! # Ok(())
//! # }
//! ```
//!
//! The router can be nested into an existing app. It only handles paths
//! under `/replica`. The request and response bodies are in [`proto`].
//!
//! [`proto`]: crate::proto

use std::{path::PathBuf, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::TryStreamExt;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::{
    proto::{Deleted, Refusal, ReplicaInfo, ReplicaList},
    receiver::Reject,
    valid_name, Error, Receiver,
};

/// The largest frames body accepted. The shipper sends at most 512 frames,
/// which is 32 MiB at the largest page size.
const MAX_FRAMES_BODY: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BASE: u64 = 8 * 1024 * 1024 * 1024;

/// Builds the router. See the [module docs](self).
#[derive(Debug)]
pub struct Routes {
    receiver: Arc<Receiver>,
    token: Option<String>,
    max_base_bytes: u64,
}

impl Routes {
    /// Routes for `receiver`, with no token and the default base limit.
    #[must_use]
    pub fn new(receiver: Arc<Receiver>) -> Self {
        Self {
            receiver,
            token: None,
            max_base_bytes: DEFAULT_MAX_BASE,
        }
    }

    /// Require `Authorization: Bearer <token>` on every request. Without a
    /// token the routes are open, which is only reasonable on a private
    /// network.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// The largest base accepted, in bytes. Defaults to 8 GiB. Bases are
    /// streamed to disk, so this limits disk use, not memory.
    #[must_use]
    pub fn max_base_bytes(mut self, max: u64) -> Self {
        self.max_base_bytes = max;
        self
    }

    /// Builds the axum router. It has its own state, so it can be merged or
    /// nested into a router of any state type.
    pub fn into_router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route("/replica", get(list))
            .route("/replica/{name}", get(status).delete(delete))
            .route(
                "/replica/{name}/base",
                post(base).layer(DefaultBodyLimit::disable()),
            )
            .route(
                "/replica/{name}/frames",
                post(frames).layer(DefaultBodyLimit::max(MAX_FRAMES_BODY)),
            )
            .layer(middleware::from_fn_with_state(Arc::clone(&state), auth))
            .with_state(state)
    }
}

type AppState = State<Arc<Routes>>;

/// An error reply with a [`Refusal`] body.
struct Refused(StatusCode, Refusal);

impl Refused {
    fn new(code: StatusCode, msg: impl Into<String>) -> Self {
        Self(code, Refusal::new(msg))
    }

    fn internal(e: impl std::fmt::Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        (self.0, Json(self.1)).into_response()
    }
}

impl From<Reject> for Refused {
    fn from(reject: Reject) -> Self {
        let code = match reject {
            Reject::NeedsBase(_) | Reject::Mismatch { .. } => StatusCode::CONFLICT,
            Reject::Busy => StatusCode::SERVICE_UNAVAILABLE,
            Reject::Bad(_) => StatusCode::BAD_REQUEST,
        };
        Self(code, Refusal::from(&reject))
    }
}

/// Compares in time that depends only on the length, so the token can't be
/// found a byte at a time by timing replies.
fn same_token(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn auth(State(routes): AppState, req: Request, next: Next) -> Response {
    if let Some(token) = &routes.token {
        let given = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
        if !given.is_some_and(|g| same_token(g, token.as_bytes())) {
            return Refused::new(StatusCode::UNAUTHORIZED, "bad or missing token").into_response();
        }
    }
    next.run(req).await
}

/// Runs a blocking receiver call off the async threads.
async fn blocking<T, F>(routes: &Routes, f: F) -> Result<T, Refused>
where
    F: FnOnce(&Receiver) -> T + Send + 'static,
    T: Send + 'static,
{
    let rx = Arc::clone(&routes.receiver);
    tokio::task::spawn_blocking(move || f(&rx))
        .await
        .map_err(Refused::internal)
}

async fn list(State(routes): AppState) -> Result<Json<ReplicaList>, Refused> {
    let replicas = blocking(&routes, Receiver::list).await?;
    Ok(Json(ReplicaList {
        replicas: replicas
            .into_iter()
            .map(|(name, s)| ReplicaInfo::new(name, s))
            .collect(),
    }))
}

async fn status(
    State(routes): AppState,
    Path(name): Path<String>,
) -> Result<Json<ReplicaInfo>, Refused> {
    let n = name.clone();
    match blocking(&routes, move |rx| rx.status(&n)).await? {
        Some(s) => Ok(Json(ReplicaInfo::new(name, s))),
        None => Err(Refused::new(StatusCode::NOT_FOUND, "no such replica")),
    }
}

async fn delete(
    State(routes): AppState,
    Path(name): Path<String>,
) -> Result<Json<Deleted>, Refused> {
    let deleted = blocking(&routes, move |rx| rx.delete(&name))
        .await?
        .map_err(|e| match e {
            Error::InvalidName(_) => Refused::new(StatusCode::BAD_REQUEST, e.to_string()),
            e => Refused::internal(e),
        })?;
    Ok(Json(Deleted { deleted }))
}

#[derive(Deserialize)]
struct BaseQuery {
    generation: u64,
}

/// Removes a staged base that never made it into place.
struct Staged(PathBuf);

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

async fn base(
    State(routes): AppState,
    Path(name): Path<String>,
    Query(q): Query<BaseQuery>,
    body: Body,
) -> Result<Json<ReplicaInfo>, Refused> {
    if !valid_name(&name) {
        return Err(Refused::new(
            StatusCode::BAD_REQUEST,
            "invalid replica name",
        ));
    }
    let staged = Staged(routes.receiver.staging_path(&name));
    let max = routes.max_base_bytes;
    let mut out = tokio::fs::File::create(&staged.0)
        .await
        .map_err(Refused::internal)?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|e| Refused::new(StatusCode::BAD_REQUEST, format!("upload failed: {e}")))?
    {
        written += chunk.len() as u64;
        if written > max {
            return Err(Refused::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("base is over the {max}-byte limit"),
            ));
        }
        out.write_all(&chunk).await.map_err(Refused::internal)?;
    }
    out.sync_all().await.map_err(Refused::internal)?;
    drop(out);

    let (n, path) = (name.clone(), staged.0.clone());
    let status = blocking(&routes, move |rx| rx.install_base(&n, &path, q.generation))
        .await?
        .map_err(|e| match e {
            Error::NotADatabase(_) => Refused::new(StatusCode::BAD_REQUEST, e.to_string()),
            e => Refused::internal(e),
        })?;
    Ok(Json(ReplicaInfo::new(name, status)))
}

#[derive(Deserialize)]
struct FramesQuery {
    generation: u64,
    first: u32,
    page_size: u32,
}

async fn frames(
    State(routes): AppState,
    Path(name): Path<String>,
    Query(q): Query<FramesQuery>,
    body: Bytes,
) -> Result<Json<ReplicaInfo>, Refused> {
    let n = name.clone();
    let status = blocking(&routes, move |rx| {
        rx.apply_frames(&n, q.generation, q.first, q.page_size, &body)
    })
    .await??;
    Ok(Json(ReplicaInfo::new(name, status)))
}

#[cfg(test)]
mod tests {
    use super::same_token;

    #[test]
    fn tokens_must_match_in_length_and_content() {
        assert!(same_token(b"abc", b"abc"));
        assert!(!same_token(b"abc", b"abd"));
        assert!(!same_token(b"abc", b"abcd"));
        assert!(!same_token(b"", b"x"));
    }
}
