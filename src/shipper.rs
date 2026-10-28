//! The sending side: keeps a peer's replica of each database here within a
//! batch of the local WAL.
//!
//! Your code tells the [`Shipper`] which databases exist ([`track`],
//! [`forget`], [`reset`]) and when one was written ([`wake`]). A small pool
//! of worker threads does the rest, one database per thread at a time:
//!
//! 1. A database the peer has no base for (a fresh one, a restored one, or
//!    one whose stream broke) is rebased. A `TRUNCATE` checkpoint folds the
//!    WAL into the main file, the generation goes up by one, and the main
//!    file is streamed to `POST /replica/{name}/base`. The WAL then starts at
//!    frame 1 of that generation.
//! 2. Otherwise the committed frames past the last acknowledged one are read
//!    straight out of the WAL and posted, at most 512 per request, to
//!    `POST /replica/{name}/frames`. The peer answers with its committed frame
//!    count, and that's where a resend starts after any failure.
//! 3. An idle database with a long WAL gets a `PASSIVE` checkpoint, taken
//!    while nothing else is writing, so exactly the acknowledged frames go
//!    into the main file. SQLite restarts a fully folded log on the next
//!    write. The shipper reads that restart as the next generation, and the
//!    peer checkpoints to match when the first batch of it arrives.
//!
//! Every connection to a shipped database has to have checkpointing turned
//! off, yours included (see [`disable_checkpoint`](crate::disable_checkpoint)).
//! Otherwise SQLite folds frames into the main file before they've been
//! shipped. If that happens anyway (another process checkpoints, or a crash
//! lands mid-header) the WAL shows a different salt or a head below the
//! acknowledged count, and the shipper sends a base.
//!
//! Two more passes run beside the per-database steps:
//!
//! - Ownership. When several nodes can hold the same database, the
//!   [`owner`](Builder::owner) callback says which one is serving it. A
//!   database owned elsewhere isn't shipped, and the replica this node left on
//!   the peer is deleted on the way out. Otherwise, after a failover, the old
//!   owner keeps shipping a stale copy into the peer, and the peer may promote
//!   it.
//! - Reconcile. Once a minute the peer's `GET /replica` list is compared with
//!   the tracked set, and a database the peer has no replica of is sent a
//!   base. Shipping is otherwise driven by writes, so a replica the peer lost
//!   (to a promotion, a delete, a rebuilt disk) wouldn't come back until
//!   something wrote to the database.
//!
//! State that has to survive a restart is one small JSON file per database
//! under `<data_dir>/.walship/state/`. Everything else is rebuilt from the
//! WAL and the peer's answers.
//!
//! [`track`]: Shipper::track
//! [`forget`]: Shipper::forget
//! [`reset`]: Shipper::reset
//! [`wake`]: Shipper::wake

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use reqwest::{header, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::{
    fs::{sidecar, write_atomic},
    proto::{Refusal, ReplicaInfo, ReplicaList},
    wal::{self, RawDb},
    Error, Result,
};

/// Frames per request. At the default 4 KiB page that's 2 MiB on the wire.
const MAX_BATCH: u32 = 512;
/// An idle database whose WAL is this long gets checkpointed.
const CHECKPOINT_AFTER: u32 = 1000;
/// A database that can't reach the peer stops growing its WAL past this and
/// takes a base later instead. At 4 KiB pages that's about 200 MiB.
const WAL_CAP_FRAMES: u32 = 50_000;
/// How long to wait for a writer before looking at the WAL head again.
const HEAD_WAIT: Duration = Duration::from_secs(2);
/// A tracked database is looked at this often even without a wake.
const SWEEP_EVERY: Duration = Duration::from_secs(5);
const RECONCILE_EVERY: Duration = Duration::from_secs(60);
/// The first reconcile waits a little, so databases tracked at startup have a
/// chance to send their bases before anything is called missing.
const RECONCILE_FIRST: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A base can be big and the link slow. Frame batches are small.
const BASE_TIMEOUT: Duration = Duration::from_secs(3600);
const FRAMES_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);

const STATE_DIR: &str = ".walship/state";
/// Where a rebase puts the copy it streams. A subdirectory, so that anything
/// globbing `*.db` in the data directory doesn't pick it up as a database.
const SNAPSHOT_DIR: &str = ".walship/snapshot";

/// Who is serving a database. See [`Builder::owner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    /// This node serves it, so it ships.
    Here,
    /// Another node serves it. The string names that node; it's only used in
    /// logs and [`DbStatus::owner`].
    Elsewhere(String),
}

type OwnerFn = dyn Fn(&str) -> Owner + Send + Sync;
type FilterFn = dyn Fn(&str) -> bool + Send + Sync;

/// The per-database record on disk.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Persisted {
    generation: u64,
    acked: u32,
    needs_base: bool,
    /// Frames folded into the main file by the last checkpoint here, when that
    /// was the whole log. Equal to `acked` while a log restart is the expected
    /// next thing to see.
    #[serde(default)]
    folded: u32,
    /// Salt of the WAL header the acknowledged frames were read from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    salt: Option<u64>,
}

impl Persisted {
    /// What a database with no state file starts from: it owes a base.
    fn fresh() -> Self {
        Self {
            needs_base: true,
            ..Self::default()
        }
    }
}

/// What a worker carries while it steps one database. It lives in the track
/// between steps and is taken out while a step runs, so the track's lock is
/// never held across I/O.
struct Job {
    persisted: Persisted,
    /// Frames posted this generation, acknowledged or not.
    sent: u32,
    handle: Option<RawDb>,
    consecutive_failures: u32,
    consecutive_mismatches: u32,
    /// Set while the peer is unreachable, so the log gets one line going in
    /// and one coming out instead of one per retry.
    in_failure: bool,
    last_error: Option<String>,
    last_ok: Option<Instant>,
    /// WAL frame count read by the last step.
    head: u32,
    /// The node serving this database while it isn't this one.
    away: Option<String>,
}

impl Job {
    fn new(persisted: Persisted) -> Self {
        Self {
            persisted,
            sent: 0,
            handle: None,
            consecutive_failures: 0,
            consecutive_mismatches: 0,
            in_failure: false,
            last_error: None,
            last_ok: None,
            head: 0,
            away: None,
        }
    }

    /// Forget everything about the stream and owe a base.
    fn restart(&mut self) {
        self.persisted.needs_base = true;
        self.sent = 0;
    }

    /// Start the next generation from scratch: nothing acknowledged, nothing
    /// folded, no WAL seen yet, and a base owed.
    fn next_generation(&mut self) {
        let p = &mut self.persisted;
        p.generation += 1;
        p.acked = 0;
        p.folded = 0;
        p.salt = None;
        self.head = 0;
        self.restart();
    }

    /// Whether a peer listing taken at `listed` that didn't include this
    /// database means it needs a base. Not when it's owned elsewhere, not
    /// when a base is owed already, and not when something reached the peer
    /// after the listing was asked for: that may be the base itself, and a
    /// base is expensive to send twice.
    fn lost_by_peer(&self, listed: Instant) -> bool {
        self.away.is_none()
            && !self.persisted.needs_base
            && self.last_ok.is_none_or(|ok| ok < listed)
    }
}

struct Track {
    /// Bumped by `reset`. A step that started under an older epoch drops its
    /// handle when it returns, because the file was replaced underneath it.
    epoch: u64,
    busy: bool,
    queued: bool,
    /// A wake arrived while a step was running: run again when it returns.
    rerun: bool,
    /// A reconcile found no replica on the peer while a step was running.
    /// Holds when the listing was asked for, so the worker can make the same
    /// call once the step returns.
    missing_since: Option<Instant>,
    retry_at: Option<Instant>,
    last_step: Option<Instant>,
    job: Option<Box<Job>>,
    /// The job's numbers as of the last step, for `status` while it's out.
    last_seen: Option<DbStatus>,
}

/// What the last reconcile saw on the peer.
#[derive(Default)]
struct PeerView {
    at: Option<Instant>,
    replicas: usize,
    missing: usize,
    orphans: usize,
    error: Option<String>,
}

#[derive(Default)]
struct Inner {
    tracks: BTreeMap<String, Track>,
    queue: VecDeque<String>,
    stopping: bool,
    peer: PeerView,
}

enum Outcome {
    Idle,
    More,
    Retry(Duration),
    Gone,
}

/// Configures and starts a [`Shipper`]. Made by [`Shipper::builder`].
pub struct Builder {
    data_dir: PathBuf,
    peer: String,
    token: Option<String>,
    workers: usize,
    owner: Option<Arc<OwnerFn>>,
    filter: Option<Arc<FilterFn>>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("data_dir", &self.data_dir)
            .field("peer", &self.peer)
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

impl Builder {
    /// Sent as `Authorization: Bearer <token>` on every request.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Worker threads. Each one steps one database at a time. Defaults to 2.
    #[must_use]
    pub fn workers(mut self, n: usize) -> Self {
        self.workers = n.max(1);
        self
    }

    /// Asked before every step which node is serving a database.
    ///
    /// Leave it unset if each database lives on exactly one node. Set it when
    /// databases can move between nodes, for instance after a failover. A
    /// database owned elsewhere isn't shipped, and its replica on the peer is
    /// deleted as soon as ownership moves, because that replica is a stale
    /// copy of a file somebody else is writing to. When ownership comes back,
    /// the database is sent a full base.
    ///
    /// The callback runs on the worker threads and should be cheap: read a
    /// map, don't make a network call.
    #[must_use]
    pub fn owner<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> Owner + Send + Sync + 'static,
    {
        self.owner = Some(Arc::new(f));
        self
    }

    /// Databases for which this returns false are never shipped, whatever
    /// calls the shipper gets about them. Handy for local bookkeeping
    /// databases kept in the same directory.
    #[must_use]
    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.filter = Some(Arc::new(f));
        self
    }

    /// Starts the worker threads. HTTP requests run on `rt`; the workers are
    /// plain threads that block on it, so SQLite never runs on an async task.
    pub fn start(self, rt: Handle) -> Result<Shipper> {
        let peer = self.peer.trim().trim_end_matches('/').to_owned();
        if !(peer.starts_with("http://") || peer.starts_with("https://")) {
            return Err(Error::InvalidPeerUrl(peer));
        }
        let state_dir = self.data_dir.join(STATE_DIR);
        std::fs::create_dir_all(&state_dir)?;
        // A snapshot is worth nothing after a restart, and a process killed
        // mid-base leaves one behind.
        let snapshot_dir = self.data_dir.join(SNAPSHOT_DIR);
        let _ = std::fs::remove_dir_all(&snapshot_dir);
        std::fs::create_dir_all(&snapshot_dir)?;

        let mut headers = header::HeaderMap::new();
        if let Some(token) = &self.token {
            let mut value = header::HeaderValue::try_from(format!("Bearer {token}"))
                .map_err(|_| Error::InvalidToken)?;
            value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .default_headers(headers)
            .build()?;

        let shared = Arc::new(Shared {
            data_dir: self.data_dir,
            state_dir,
            snapshot_dir,
            peer,
            client,
            rt,
            owner: self.owner,
            filter: self.filter,
            started: Instant::now(),
            inner: Mutex::default(),
            work: Condvar::new(),
            tick: Condvar::new(),
        });
        let mut threads = Vec::with_capacity(self.workers + 1);
        for i in 0..self.workers {
            let s = Arc::clone(&shared);
            threads.push(
                std::thread::Builder::new()
                    .name(format!("walship-{i}"))
                    .spawn(move || s.worker())?,
            );
        }
        let s = Arc::clone(&shared);
        threads.push(
            std::thread::Builder::new()
                .name("walship-sweep".into())
                .spawn(move || s.sweeper())?,
        );
        Ok(Shipper { shared, threads })
    }
}

/// Ships the WAL of each tracked database to one peer.
///
/// Databases live in one data directory as `<name>.db`. Dropping the shipper
/// tells its threads to stop without waiting for them; [`shutdown`] waits.
///
/// [`shutdown`]: Shipper::shutdown
pub struct Shipper {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
}

impl fmt::Debug for Shipper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shipper")
            .field("data_dir", &self.shared.data_dir)
            .field("peer", &self.shared.peer)
            .finish_non_exhaustive()
    }
}

/// Where one tracked database stands. Part of [`ShipperStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DbStatus {
    pub name: String,
    pub generation: u64,
    /// The peer's committed frame count in this generation.
    pub acked: u32,
    /// The local WAL's frame count at the last step.
    pub head: u32,
    /// Frames written here that the peer hasn't confirmed: the replica's lag.
    pub pending: u32,
    pub needs_base: bool,
    /// Consecutive failed steps.
    pub failures: u32,
    /// A worker is stepping this database. The other numbers are from before
    /// the step.
    pub stepping: bool,
    /// Set while another node owns the database. See [`Builder::owner`].
    pub owner: Option<String>,
    pub last_error: Option<String>,
    /// Seconds since the peer last accepted something for this database. A
    /// database that has never reached the peer counts from when the shipper
    /// started, so a "behind for N seconds" alert can't read a failure since
    /// startup as zero.
    pub ok_secs_ago: u64,
    pub retry_in_secs: Option<u64>,
}

/// What the last reconcile pass found on the peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerStatus {
    pub seen_secs_ago: u64,
    /// Replicas the peer holds, from any sender.
    pub replicas: usize,
    /// Databases tracked here that the peer has no replica of.
    pub missing: usize,
    /// Replicas on the peer with no database of that name tracked here. A
    /// database forgotten while the peer was down leaves one behind.
    pub orphans: usize,
}

/// Everything [`Shipper::status`] knows, ready to serialize for a health
/// endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShipperStatus {
    pub peer_url: String,
    pub databases: Vec<DbStatus>,
    /// `None` until the first reconcile pass has run.
    pub peer: Option<PeerStatus>,
    pub peer_error: Option<String>,
}

impl Shipper {
    /// A shipper for the databases in `data_dir`, sending to the receiver at
    /// `peer_url` (for example `http://standby.internal:7350`).
    pub fn builder(data_dir: impl Into<PathBuf>, peer_url: impl Into<String>) -> Builder {
        Builder {
            data_dir: data_dir.into(),
            peer: peer_url.into(),
            token: None,
            workers: 2,
            owner: None,
            filter: None,
        }
    }

    #[must_use]
    pub fn peer_url(&self) -> &str {
        &self.shared.peer
    }

    /// Starts shipping `name`. Call it for every database at startup and for
    /// each one you create. Calling it again does nothing.
    pub fn track(&self, name: &str) {
        self.shared.track(name);
    }

    /// `name` was written: look at its WAL soon. Cheap enough to call after
    /// every write. Without it, a tracked database is still looked at every
    /// few seconds.
    pub fn wake(&self, name: &str) {
        self.shared.wake(name);
    }

    /// The file behind `name` was replaced, by a restore or anything else
    /// that swaps the file out. The peer needs a new base, and any handle the
    /// shipper holds on the old file is dropped.
    pub fn reset(&self, name: &str) {
        self.shared.reset(name);
    }

    /// `name` was deleted here. Stops shipping it, drops its state, and asks
    /// the peer to delete its replica. The delete is sent once; if the peer
    /// is down it shows up later as an orphan in [`PeerStatus`].
    pub fn forget(&self, name: &str) {
        self.shared.forget(name);
    }

    /// Compares the peer's replica list with the tracked set right away,
    /// instead of waiting for the next pass. Blocks on the request, so call
    /// it from a blocking context.
    pub fn reconcile(&self) {
        self.shared.reconcile();
    }

    #[must_use]
    pub fn status(&self) -> ShipperStatus {
        self.shared.status()
    }

    /// Stops the threads and waits for them. A step in progress finishes
    /// first, and that can be a base upload. Call it from a blocking context,
    /// not from an async task on the shipper's own runtime.
    pub fn shutdown(mut self) {
        self.shared.stop();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

impl Drop for Shipper {
    fn drop(&mut self) {
        self.shared.stop();
    }
}

/// Marks the peer's replica of `name` as owing a fresh base, with no shipper
/// running.
///
/// [`Shipper::reset`] covers a database replaced while a shipper is running.
/// This is for one replaced while shipping was off: its state file still
/// describes the file that was replaced, and without this, starting the
/// shipper again resumes a frame stream on top of a base that's no longer the
/// same database. Usually the WAL salt catches that. It doesn't when no
/// frames were shipped since the last base, because then there's no salt
/// recorded to disagree with.
///
/// A database with no state file already owes a base, so a missing file is
/// fine.
pub fn mark_needs_base(data_dir: &Path, name: &str) -> Result<()> {
    let path = data_dir.join(STATE_DIR).join(format!("{name}.json"));
    let Ok(bytes) = std::fs::read(&path) else {
        return Ok(());
    };
    let mut p: Persisted = serde_json::from_slice(&bytes)?;
    if !p.needs_base {
        p.needs_base = true;
        write_atomic(&path, &serde_json::to_vec(&p)?)?;
    }
    Ok(())
}

/// The copy of a main database file that a rebase streams. Removed when it
/// goes out of scope.
struct Snapshot {
    path: PathBuf,
}

impl Snapshot {
    fn take(db: &RawDb, dir: &Path, name: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let snapshot = Self {
            path: dir.join(format!("{name}.snapshot")),
        };
        db.copy_main_file(&snapshot.path)?;
        Ok(snapshot)
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// What the worker threads and the handle share.
struct Shared {
    data_dir: PathBuf,
    state_dir: PathBuf,
    snapshot_dir: PathBuf,
    peer: String,
    client: reqwest::Client,
    rt: Handle,
    owner: Option<Arc<OwnerFn>>,
    filter: Option<Arc<FilterFn>>,
    started: Instant,
    inner: Mutex<Inner>,
    /// Workers wait here for something in the queue.
    work: Condvar,
    /// The sweeper waits here between passes, so `stop` doesn't have to wait
    /// out its sleep.
    tick: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sends a request and reads back JSON, whatever the status. A body that
/// isn't the expected JSON comes back as `None`.
async fn send<T: DeserializeOwned>(
    req: reqwest::RequestBuilder,
) -> Result<(StatusCode, Option<T>)> {
    let resp = req.send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    Ok((status, serde_json::from_slice(&body).ok()))
}

impl Shared {
    fn ships(&self, name: &str) -> bool {
        self.filter.as_ref().is_none_or(|f| f(name))
    }

    fn owner_of(&self, name: &str) -> Owner {
        self.owner.as_ref().map_or(Owner::Here, |f| f(name))
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.state_dir.join(format!("{name}.json"))
    }

    fn db_path(&self, name: &str) -> PathBuf {
        self.data_dir.join(format!("{name}.db"))
    }

    fn replica_url(&self, name: &str) -> String {
        format!("{}/replica/{name}", self.peer)
    }

    fn load(&self, name: &str) -> Persisted {
        std::fs::read(self.state_path(name))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_else(Persisted::fresh)
    }

    fn persist(&self, name: &str, p: &Persisted) -> Result<()> {
        write_atomic(&self.state_path(name), &serde_json::to_vec(p)?)?;
        Ok(())
    }

    fn stop(&self) {
        lock(&self.inner).stopping = true;
        self.work.notify_all();
        self.tick.notify_all();
    }

    fn enqueue(&self, inner: &mut Inner, name: &str) {
        let Some(t) = inner.tracks.get_mut(name) else {
            return;
        };
        if !t.queued && !t.busy {
            t.queued = true;
            inner.queue.push_back(name.to_owned());
            self.work.notify_one();
        }
    }

    fn track(&self, name: &str) {
        if !self.ships(name) {
            return;
        }
        let mut inner = lock(&self.inner);
        if !inner.tracks.contains_key(name) {
            let job = Job::new(self.load(name));
            let last_seen = Some(self.describe(name, &job, Instant::now()));
            inner.tracks.insert(
                name.to_owned(),
                Track {
                    epoch: 0,
                    busy: false,
                    queued: false,
                    rerun: false,
                    missing_since: None,
                    retry_at: None,
                    last_step: None,
                    last_seen,
                    job: Some(Box::new(job)),
                },
            );
        }
        self.enqueue(&mut inner, name);
    }

    fn wake(&self, name: &str) {
        if !self.ships(name) {
            return;
        }
        let mut inner = lock(&self.inner);
        let Some(t) = inner.tracks.get_mut(name) else {
            return;
        };
        if t.retry_at.is_some_and(|at| at > Instant::now()) {
            return;
        }
        if t.busy {
            t.rerun = true;
            return;
        }
        self.enqueue(&mut inner, name);
    }

    fn reset(&self, name: &str) {
        if !self.ships(name) {
            return;
        }
        self.track(name);
        let mut inner = lock(&self.inner);
        let Some(t) = inner.tracks.get_mut(name) else {
            return;
        };
        t.epoch += 1;
        t.retry_at = None;
        match t.job.as_mut() {
            Some(job) => {
                job.handle = None;
                job.restart();
                let _ = self.persist(name, &job.persisted);
            }
            // Mid-step: the epoch check sorts it out when the step returns.
            None => t.rerun = true,
        }
        self.enqueue(&mut inner, name);
    }

    fn forget(&self, name: &str) {
        if !self.ships(name) {
            return;
        }
        {
            let mut inner = lock(&self.inner);
            inner.tracks.remove(name);
            inner.queue.retain(|n| n != name);
        }
        let _ = std::fs::remove_file(self.state_path(name));
        self.drop_peer_replica(name);
    }

    /// Asks the peer to delete its replica of `name`, once, without waiting.
    /// The reconcile pass reports anything left behind.
    fn drop_peer_replica(&self, name: &str) {
        let req = self
            .client
            .delete(self.replica_url(name))
            .timeout(FRAMES_TIMEOUT);
        self.rt.spawn(async move {
            let _ = req.send().await;
        });
    }

    fn status(&self) -> ShipperStatus {
        let inner = lock(&self.inner);
        let now = Instant::now();
        let databases = inner
            .tracks
            .iter()
            .filter_map(|(name, t)| {
                let mut row = if let Some(job) = &t.job {
                    self.describe(name, job, now)
                } else {
                    let mut row = t.last_seen.clone()?;
                    row.stepping = true;
                    row
                };
                row.retry_in_secs = t
                    .retry_at
                    .map(|at| at.saturating_duration_since(now).as_secs());
                Some(row)
            })
            .collect();
        let peer = inner.peer.at.map(|at| PeerStatus {
            seen_secs_ago: now.duration_since(at).as_secs(),
            replicas: inner.peer.replicas,
            missing: inner.peer.missing,
            orphans: inner.peer.orphans,
        });
        ShipperStatus {
            peer_url: self.peer.clone(),
            databases,
            peer,
            peer_error: inner.peer.error.clone(),
        }
    }

    fn describe(&self, name: &str, job: &Job, now: Instant) -> DbStatus {
        let p = &job.persisted;
        DbStatus {
            name: name.to_owned(),
            generation: p.generation,
            acked: p.acked,
            head: job.head,
            pending: job.head.saturating_sub(p.acked),
            needs_base: p.needs_base,
            failures: job.consecutive_failures,
            stepping: false,
            owner: job.away.clone(),
            last_error: job.last_error.clone(),
            ok_secs_ago: now
                .duration_since(job.last_ok.unwrap_or(self.started))
                .as_secs(),
            retry_in_secs: None,
        }
    }

    fn sweeper(&self) {
        let mut next_reconcile = Instant::now() + RECONCILE_FIRST;
        let mut inner = lock(&self.inner);
        loop {
            inner = self
                .tick
                .wait_timeout(inner, Duration::from_secs(1))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
            if inner.stopping {
                return;
            }
            if Instant::now() >= next_reconcile {
                next_reconcile = Instant::now() + RECONCILE_EVERY;
                drop(inner);
                self.reconcile();
                inner = lock(&self.inner);
                if inner.stopping {
                    return;
                }
            }
            let now = Instant::now();
            let due: Vec<String> = inner
                .tracks
                .iter()
                .filter(|(_, t)| !t.busy && !t.queued)
                .filter(|(_, t)| match t.retry_at {
                    Some(at) => at <= now,
                    None => t
                        .last_step
                        .is_none_or(|l| now.duration_since(l) >= SWEEP_EVERY),
                })
                .map(|(name, _)| name.clone())
                .collect();
            for name in due {
                if let Some(t) = inner.tracks.get_mut(&name) {
                    t.retry_at = None;
                }
                self.enqueue(&mut inner, &name);
            }
        }
    }

    /// Compares the peer's replica list with the tracked set, and sends a
    /// base for each tracked database the peer has no replica of.
    ///
    /// [`Job::lost_by_peer`] makes the call. A database with no success at
    /// all is fair game, since that's what an idle database looks like after
    /// a restart, and it's the case this pass is for. One that's mid-step is
    /// marked, and its worker decides when the step returns.
    fn reconcile(&self) {
        let started = Instant::now();
        let names = match self.peer_replica_names() {
            Ok(names) => names,
            Err(e) => {
                lock(&self.inner).peer.error = Some(e.to_string());
                return;
            }
        };
        let mut inner = lock(&self.inner);
        let mut missing = 0;
        let mut rebase = Vec::new();
        for (name, t) in &mut inner.tracks {
            if names.contains(name) {
                continue;
            }
            let Some(job) = t.job.as_mut() else {
                t.missing_since = Some(started);
                missing += 1;
                continue;
            };
            // Owned elsewhere: the peer isn't supposed to have it from us.
            if job.away.is_some() {
                continue;
            }
            missing += 1;
            if job.lost_by_peer(started) {
                job.restart();
                let _ = self.persist(name, &job.persisted);
                rebase.push(name.clone());
            }
        }
        let orphans = names
            .iter()
            .filter(|n| !inner.tracks.contains_key(*n))
            .count();
        inner.peer = PeerView {
            at: Some(started),
            replicas: names.len(),
            missing,
            orphans,
            error: None,
        };
        if !rebase.is_empty() {
            tracing::info!(
                peer = %self.peer,
                "{} database(s) have no replica on the peer, sending bases: {}",
                rebase.len(),
                rebase.join(", ")
            );
        }
        for name in &rebase {
            self.enqueue(&mut inner, name);
        }
    }

    fn peer_replica_names(&self) -> Result<BTreeSet<String>> {
        let req = self
            .client
            .get(format!("{}/replica", self.peer))
            .timeout(FRAMES_TIMEOUT);
        let (status, list) = self.rt.block_on(send::<ReplicaList>(req))?;
        match list {
            Some(list) if status.is_success() => {
                Ok(list.replicas.into_iter().map(|r| r.name).collect())
            }
            _ => Err(Error::Peer(format!("GET /replica answered {status}"))),
        }
    }

    fn worker(&self) {
        loop {
            let (name, epoch, mut job) = {
                let mut inner = lock(&self.inner);
                loop {
                    if inner.stopping {
                        return;
                    }
                    if let Some(name) = inner.queue.pop_front() {
                        let Some(t) = inner.tracks.get_mut(&name) else {
                            continue;
                        };
                        t.queued = false;
                        if t.busy {
                            continue;
                        }
                        let Some(job) = t.job.take() else { continue };
                        t.busy = true;
                        t.rerun = false;
                        break (name, t.epoch, job);
                    }
                    inner = self
                        .work
                        .wait(inner)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };

            let mut outcome = self.step(&name, &mut job);

            let mut inner = lock(&self.inner);
            let now = Instant::now();
            let row = self.describe(&name, &job, now);
            // Forgotten while it was stepping.
            let Some(t) = inner.tracks.get_mut(&name) else {
                continue;
            };
            t.busy = false;
            t.last_step = Some(now);
            t.last_seen = Some(row);
            if t.epoch != epoch {
                // Replaced while it was stepping: whatever went out came from
                // the old file.
                job.handle = None;
                job.restart();
                let _ = self.persist(&name, &job.persisted);
                t.retry_at = None;
                t.job = Some(job);
                self.enqueue(&mut inner, &name);
                continue;
            }
            let rerun = std::mem::take(&mut t.rerun);
            if let Some(listed) = t.missing_since.take() {
                if !matches!(outcome, Outcome::Gone) && job.lost_by_peer(listed) {
                    tracing::info!(db = %name, peer = %self.peer, "no replica on the peer, sending a base");
                    job.restart();
                    let _ = self.persist(&name, &job.persisted);
                    t.retry_at = None;
                    outcome = Outcome::More;
                }
            }
            match outcome {
                Outcome::Gone => {
                    inner.tracks.remove(&name);
                }
                Outcome::Retry(after) => {
                    t.retry_at = Some(now + after);
                    t.job = Some(job);
                }
                Outcome::More => {
                    t.job = Some(job);
                    self.enqueue(&mut inner, &name);
                }
                Outcome::Idle => {
                    t.job = Some(job);
                    if rerun {
                        self.enqueue(&mut inner, &name);
                    }
                }
            }
        }
    }

    fn step(&self, name: &str, job: &mut Job) -> Outcome {
        let path = self.db_path(name);
        if !path.exists() {
            return Outcome::Gone;
        }
        self.step_inner(name, &path, job)
            .unwrap_or_else(|e| self.failed(name, job, &e))
    }

    fn handle<'a>(path: &Path, job: &'a mut Job) -> Result<&'a RawDb> {
        let db = if let Some(db) = job.handle.take() {
            db
        } else {
            let db = RawDb::open(path, OPEN_TIMEOUT)?;
            db.disable_checkpoint()?;
            db
        };
        Ok(job.handle.insert(db))
    }

    fn step_inner(&self, name: &str, path: &Path, job: &mut Job) -> Result<Outcome> {
        if self.ownership_moved_away(name, job) {
            return Ok(Outcome::Idle);
        }
        if job.persisted.needs_base {
            return self.rebase(name, path, job);
        }
        let Some(head) = Self::handle(path, job)?.head(HEAD_WAIT)? else {
            return Ok(Outcome::Retry(Duration::from_millis(200)));
        };
        job.head = head;

        // The WAL's identity. A restart of its numbering shows up here.
        let salt = wal::wal_salt(&sidecar(path, "-wal"))?.map(u64::from_be_bytes);
        let p = &mut job.persisted;
        let salt_changed = matches!((p.salt, salt), (Some(stored), Some(cur)) if stored != cur);
        if salt_changed || head < p.acked {
            // The log restarted. That's expected after a checkpoint here that
            // folded exactly the acknowledged frames: the main file holds
            // everything the peer has, and the restarted log is the next
            // generation. Any other restart means frames went into the main
            // file without being shipped.
            if salt_changed && p.folded == p.acked && p.acked > 0 {
                p.generation += 1;
                p.acked = 0;
                p.folded = 0;
                p.salt = salt;
                job.sent = 0;
                self.persist(name, p)?;
            } else {
                p.needs_base = true;
                self.persist(name, p)?;
                return Ok(Outcome::More);
            }
        }
        if p.salt.is_none() && salt.is_some() && head > 0 {
            p.salt = salt;
            self.persist(name, p)?;
        }
        if head >= WAL_CAP_FRAMES {
            // Readers kept every checkpoint from completing. Fold it the hard
            // way and send a base.
            p.needs_base = true;
            self.persist(name, p)?;
            return Ok(Outcome::More);
        }

        if head == p.acked {
            // The peer has every frame. Anything sent past the head belonged
            // to a transaction that rolled back.
            job.sent = head;
            if head >= CHECKPOINT_AFTER && p.folded != head {
                return self.checkpoint(name, job, head);
            }
            return Ok(Outcome::Idle);
        }
        if job.sent >= head {
            // Nothing new to send, yet the peer is behind: it acknowledged
            // less than it was sent, or it restarted. Start again from what
            // it has acknowledged.
            job.sent = p.acked;
        }
        self.ship(name, path, job, head)
    }

    /// Checks the owner callback. Returns true when another node owns the
    /// database and the step should stop here.
    ///
    /// The peer's replica is deleted on the way out, not on every step. That
    /// replica came from here and is a stale copy of a file the new owner
    /// is writing to, so leaving it on the peer invites promoting the wrong
    /// data. Everything acknowledged is forgotten at the same time: the way
    /// back is a full base, since the peer has nothing and the local file may
    /// have been replaced in the meantime.
    fn ownership_moved_away(&self, name: &str, job: &mut Job) -> bool {
        match (self.owner_of(name), job.away.take()) {
            (Owner::Elsewhere(owner), Some(was)) => {
                if was != owner {
                    tracing::info!(db = name, "owned by {owner} (was {was}), not shipping");
                }
                job.away = Some(owner);
                true
            }
            (Owner::Elsewhere(owner), None) => {
                tracing::info!(
                    db = name,
                    peer = %self.peer,
                    "owned by {owner}, deleting the replica on the peer"
                );
                self.drop_peer_replica(name);
                let p = &mut job.persisted;
                p.acked = 0;
                p.folded = 0;
                p.salt = None;
                job.restart();
                job.head = 0;
                job.handle = None;
                job.last_error = None;
                let _ = self.persist(name, &job.persisted);
                job.away = Some(owner);
                true
            }
            (Owner::Here, Some(was)) => {
                tracing::info!(db = name, "owned here again (was {was}), sending a base");
                job.restart();
                false
            }
            (Owner::Here, None) => false,
        }
    }

    /// Folds the WAL into the main file and sends the file as a base.
    fn rebase(&self, name: &str, path: &Path, job: &mut Job) -> Result<Outcome> {
        let db = Self::handle(path, job)?;
        let ck = db.checkpoint_truncate()?;
        if !ck.complete() {
            job.last_error = Some(format!(
                "checkpoint before base incomplete ({}/{} frames, busy={})",
                ck.checkpointed, ck.log, ck.busy
            ));
            return Ok(Outcome::Retry(BACKOFF_MIN));
        }
        // Stream a copy, not the live file. Opening the database a second time
        // with anything but SQLite drops this process's POSIX locks on it as
        // soon as that descriptor closes (see `RawDb::copy_main_file`). The
        // copy is taken before any state changes, so a failure here leaves the
        // stream as it was.
        let snapshot = Snapshot::take(db, &self.snapshot_dir, name)?;

        job.next_generation();
        self.persist(name, &job.persisted)?;

        let generation = job.persisted.generation;
        let url = format!("{}/base?generation={generation}", self.replica_url(name));
        let (status, info) = self.rt.block_on(async {
            let file = tokio::fs::File::open(&snapshot.path).await?;
            let len = file.metadata().await?.len();
            let req = self
                .client
                .post(&url)
                .header(header::CONTENT_LENGTH, len)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .timeout(BASE_TIMEOUT)
                .body(reqwest::Body::wrap_stream(
                    tokio_util::io::ReaderStream::new(file),
                ));
            send::<ReplicaInfo>(req).await
        })?;
        let installed = match info {
            Some(info) if status.is_success() => info.generation,
            _ => return Err(Error::Peer(format!("base refused with {status}"))),
        };
        if installed != generation {
            return Err(Error::Peer(format!(
                "base installed as generation {installed}, sent {generation}"
            )));
        }
        job.persisted.needs_base = false;
        self.persist(name, &job.persisted)?;
        self.recovered(name, job);
        tracing::info!(db = name, "rebased at generation {generation}");
        Ok(Outcome::More)
    }

    /// Folds the WAL of an idle database into its main file.
    ///
    /// The checkpoint is `PASSIVE`: it doesn't wait for anyone or restart the
    /// log, and it works from the frame count it read when it started, which
    /// it reports as `log`. So `log == head` says no writer slipped in first,
    /// and `checkpointed == head` that every acknowledged frame went in and no
    /// other. After a whole-log fold the next writer restarts the log itself,
    /// which `step_inner` reads as the next generation. Readers on an older
    /// snapshot make the fold partial; the WAL keeps its frames and the next
    /// idle moment tries again.
    fn checkpoint(&self, name: &str, job: &mut Job, head: u32) -> Result<Outcome> {
        let db = job.handle.as_ref().expect("step_inner opened the handle");
        let ck = db.checkpoint_passive()?;
        let p = &mut job.persisted;
        if ck.checkpointed > head {
            // Frames the peer never saw went into the main file.
            p.needs_base = true;
            self.persist(name, p)?;
            return Ok(Outcome::More);
        }
        if ck.log == head && ck.checkpointed == head {
            p.folded = head;
            self.persist(name, p)?;
            return Ok(Outcome::Idle);
        }
        if ck.log > head {
            // A writer got in first. Ship what it wrote and fold later.
            return Ok(Outcome::More);
        }
        job.last_error = Some(format!(
            "checkpoint incomplete ({}/{} frames, busy={})",
            ck.checkpointed, ck.log, ck.busy
        ));
        Ok(Outcome::Idle)
    }

    fn ship(&self, name: &str, path: &Path, job: &mut Job, head: u32) -> Result<Outcome> {
        let from = job.sent.max(job.persisted.acked) + 1;
        let to = head.min(from + MAX_BATCH - 1);
        let db = Self::handle(path, job)?;
        let page_size = db.page_size()?;
        let frame_len = wal::FRAME_HEADER + page_size as usize;
        let mut body = Vec::with_capacity((to - from + 1) as usize * frame_len);
        for no in from..=to {
            body.extend(db.get_frame(no, page_size)?);
        }
        let generation = job.persisted.generation;
        let req = self
            .client
            .post(format!(
                "{}/frames?generation={generation}&first={from}&page_size={page_size}",
                self.replica_url(name)
            ))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .timeout(FRAMES_TIMEOUT)
            .body(body);
        let (status, reply) = self.rt.block_on(send::<serde_json::Value>(req))?;
        let reply = reply.unwrap_or_default();

        match status {
            StatusCode::OK => {
                let info: ReplicaInfo = serde_json::from_value(reply)?;
                job.sent = to;
                job.consecutive_mismatches = 0;
                if info.frames != job.persisted.acked {
                    job.persisted.acked = info.frames;
                    self.persist(name, &job.persisted)?;
                }
                self.recovered(name, job);
                Ok(if to < head {
                    Outcome::More
                } else {
                    Outcome::Idle
                })
            }
            StatusCode::CONFLICT => {
                let refusal: Refusal = serde_json::from_value(reply).unwrap_or_else(|_| Refusal {
                    needs_base: true,
                    ..Refusal::new("conflict")
                });
                job.consecutive_mismatches += 1;
                // The peer is on this generation and behind or level with the
                // head: resend from where it is. Only once in a row, so two
                // sides that keep disagreeing end up with a base.
                let resume_at = refusal.frames.filter(|&f| {
                    !refusal.needs_base
                        && job.consecutive_mismatches == 1
                        && refusal.generation == Some(generation)
                        && f <= head
                });
                if let Some(frames) = resume_at {
                    job.persisted.acked = frames;
                    job.sent = frames;
                    self.persist(name, &job.persisted)?;
                    return Ok(Outcome::More);
                }
                job.consecutive_mismatches = 0;
                job.last_error = Some(format!(
                    "peer refused frames {from}..={to}: {}",
                    refusal.error
                ));
                job.persisted.needs_base = true;
                self.persist(name, &job.persisted)?;
                Ok(Outcome::More)
            }
            StatusCode::SERVICE_UNAVAILABLE => Ok(Outcome::Retry(BACKOFF_MIN)),
            _ => {
                let error = reply.get("error").and_then(|e| e.as_str()).unwrap_or("");
                Err(Error::Peer(format!(
                    "{status} for frames {from}..={to}: {error}"
                )))
            }
        }
    }

    fn failed(&self, name: &str, job: &mut Job, e: &Error) -> Outcome {
        job.consecutive_failures = job.consecutive_failures.saturating_add(1);
        job.last_error = Some(e.to_string());
        if !job.in_failure {
            job.in_failure = true;
            tracing::warn!(db = name, peer = %self.peer, "can't ship: {e}");
        }
        if job.consecutive_failures >= 3 && !job.persisted.needs_base {
            self.cap_wal(name, job);
        }
        let exp = job.consecutive_failures.min(6) - 1;
        Outcome::Retry((BACKOFF_MIN * 2u32.pow(exp)).min(BACKOFF_MAX))
    }

    /// While the peer is away the WAL grows with every write, because nothing
    /// may fold it. Past the cap, fold it here and owe the peer a base.
    fn cap_wal(&self, name: &str, job: &mut Job) {
        let Some(db) = job.handle.as_ref() else {
            return;
        };
        let Ok(Some(head)) = db.head(Duration::from_millis(200)) else {
            return;
        };
        if head.saturating_sub(job.persisted.acked) <= WAL_CAP_FRAMES {
            return;
        }
        if !db.checkpoint_truncate().is_ok_and(|ck| ck.complete()) {
            return;
        }
        job.next_generation();
        let _ = self.persist(name, &job.persisted);
        tracing::warn!(
            db = name,
            "WAL passed {WAL_CAP_FRAMES} frames, folded it; the peer gets a base"
        );
    }

    fn recovered(&self, name: &str, job: &mut Job) {
        job.last_ok = Some(Instant::now());
        job.consecutive_failures = 0;
        job.last_error = None;
        if std::mem::take(&mut job.in_failure) {
            tracing::info!(db = name, peer = %self.peer, "shipping again");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_needs_base_flags_the_state_file_and_skips_a_missing_one() {
        let dir = std::env::temp_dir().join(format!("walship-mark-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join(STATE_DIR)).unwrap();

        // No state file: nothing to do, and that's not an error.
        mark_needs_base(&dir, "x").unwrap();
        assert!(!dir.join(STATE_DIR).join("x.json").exists());

        let path = dir.join(STATE_DIR).join("x.json");
        let p = Persisted {
            generation: 4,
            acked: 17,
            salt: Some(0xdead_beef),
            ..Persisted::default()
        };
        std::fs::write(&path, serde_json::to_vec(&p).unwrap()).unwrap();
        mark_needs_base(&dir, "x").unwrap();
        let back: Persisted = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            back,
            Persisted {
                needs_base: true,
                ..p
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_peer_url_without_a_scheme_is_refused() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = Shipper::builder(std::env::temp_dir(), "standby.internal:7350")
            .start(rt.handle().clone())
            .unwrap_err();
        assert!(matches!(err, Error::InvalidPeerUrl(_)), "{err}");
    }
}
