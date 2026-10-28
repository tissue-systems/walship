//! JSON bodies of the HTTP protocol.
//!
//! The shipper and the [`server`](crate::server) routes both use these, so
//! the two ends can't drift apart. They're public for anyone serving a
//! [`Receiver`](crate::Receiver) from a framework other than axum.
//!
//! | Request | Success | Failure |
//! |---|---|---|
//! | `POST /replica/{name}/base?generation=G` | [`ReplicaInfo`] | 400, 413 |
//! | `POST /replica/{name}/frames?generation=G&first=F&page_size=P` | [`ReplicaInfo`] | 400, 409, 503 |
//! | `GET /replica` | [`ReplicaList`] | |
//! | `GET /replica/{name}` | [`ReplicaInfo`] | 404 |
//! | `DELETE /replica/{name}` | [`Deleted`] | 400 |
//!
//! Any request can also fail with 401 when the token is wrong or missing,
//! and with 500 when the receiver hits an I/O or SQLite error. Every failure
//! has a [`Refusal`] body.
//!
//! A base body is the raw main database file. A frames body is whole WAL
//! frames, each a 24-byte header followed by one page, numbered from `first`.
//! Every request carries `Authorization: Bearer <token>` when the receiver
//! was given a token.

use serde::{Deserialize, Serialize};

use crate::receiver::{Reject, Status};

/// Where one replica stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub name: String,
    pub generation: u64,
    /// Committed frames in the replica's WAL.
    pub frames: u32,
    /// An insert session is open, waiting for the rest of a transaction.
    pub pending: bool,
    /// Size of the database file plus its WAL.
    pub bytes: u64,
}

impl ReplicaInfo {
    pub fn new(name: impl Into<String>, status: Status) -> Self {
        Self {
            name: name.into(),
            generation: status.generation,
            frames: status.frames,
            pending: status.pending,
            bytes: status.bytes,
        }
    }
}

/// The answer to `GET /replica`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaList {
    pub replicas: Vec<ReplicaInfo>,
}

/// The answer to `DELETE /replica/{name}`. `deleted` is false if there was
/// nothing to delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deleted {
    pub deleted: bool,
}

/// Why a request was turned down.
///
/// On a 409, `needs_base: false` comes with the replica's position, and the
/// shipper can resend from `frames + 1`. `needs_base: true` means only a new
/// base will do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub error: String,
    #[serde(default)]
    pub needs_base: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frames: Option<u32>,
}

impl Refusal {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            needs_base: false,
            generation: None,
            frames: None,
        }
    }
}

impl From<&Reject> for Refusal {
    fn from(reject: &Reject) -> Self {
        let mut refusal = Self::new(reject.to_string());
        match *reject {
            Reject::NeedsBase(_) => refusal.needs_base = true,
            Reject::Mismatch { generation, frames } => {
                refusal.generation = Some(generation);
                refusal.frames = Some(frames);
            }
            Reject::Busy | Reject::Bad(_) => {}
        }
        refusal
    }
}
