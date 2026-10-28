//! The receiving side: warm copies of databases that live somewhere else.
//!
//! A replica is a database file plus its `-wal` in the receiver's directory.
//! Nothing writes to it through SQL. It's either waiting for a base, or it
//! sits at a `(generation, frame)` position in the sender's WAL stream, and
//! the [`Shipper`](crate::Shipper) on the other end moves it forward:
//!
//! - A base is the sender's whole main database file, taken right after a
//!   `TRUNCATE` checkpoint, so it holds every committed page and the sender's
//!   WAL is empty. Installing one replaces whatever was there.
//! - Frames `F, F+1, ...` of generation `G` are accepted only where they
//!   belong: `F` is the next frame the replica hasn't committed, or `F == 1`
//!   with a `G` past the stored generation. The second case means the sender
//!   restarted its WAL, and the replica checkpoints to match. Anything else
//!   is rejected with the replica's position, so the sender can resend from
//!   there or start over with a base.
//!
//! A batch can end in the middle of a transaction. The insert session stays
//! open and the next batch picks it up. A batch that doesn't continue it
//! aborts the session first, which throws away the uncommitted frames.
//! Either way, a reader of the replica never sees half a transaction.
//!
//! Every handle here has checkpointing turned off, so the `-wal` survives a
//! restart and the stream picks up again from the replica's committed frame
//! count. The only checkpoint a replica ever runs is the one that follows the
//! sender's, when the generation changes.

use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    fs::{sidecar, sync_parent, temp_suffix, valid_name, write_atomic},
    wal::{self, RawDb},
    Error, Result,
};

/// How long an insert or checkpoint waits on a reader of the replica.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The first 16 bytes of every SQLite database file.
const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Kept on disk next to each replica as `<name>.json`. The frame position
/// isn't in here: the replica's own WAL is the record of that.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    generation: u64,
}

struct Replica {
    generation: u64,
    /// Opened on first use. Dropping it closes any open insert session.
    db: Option<RawDb>,
    /// The next frame number expected while an insert session is open.
    pending: Option<u32>,
    /// Committed frames, as of the last look at the WAL.
    committed: u32,
    /// A base has been installed.
    present: bool,
}

/// Where a replica stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// The sender's WAL generation the replica is following.
    pub generation: u64,
    /// Committed frames in the replica's WAL.
    pub frames: u32,
    /// An insert session is open, waiting for the rest of a transaction.
    pub pending: bool,
    /// Size of the database file plus its WAL.
    pub bytes: u64,
}

/// Why a batch of frames wasn't applied.
#[derive(Debug, thiserror::Error)]
pub enum Reject {
    /// There's no base, or what's here conflicts with the batch. Only a new
    /// base can fix it.
    #[error("needs a new base: {0}")]
    NeedsBase(String),
    /// The batch doesn't start where the replica is.
    #[error(
        "batch doesn't continue the replica (it is at generation {generation}, frame {frames})"
    )]
    Mismatch { generation: u64, frames: u32 },
    /// A reader held off the checkpoint that goes with a generation change.
    /// Try again.
    #[error("replica busy")]
    Busy,
    /// The request itself is wrong: a bad page size, or a body that isn't a
    /// whole number of frames.
    #[error("{0}")]
    Bad(String),
}

/// What [`Receiver::take`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Take {
    /// The replica was checkpointed and its file moved to the destination.
    Moved {
        generation: u64,
        frames: u32,
        bytes: u64,
    },
    /// There's no replica by that name.
    Absent,
    /// A reader held off the checkpoint. The replica wasn't touched; try
    /// again.
    Busy,
}

/// A directory of replicas.
///
/// Methods block on file I/O and SQLite. From async code, call them inside
/// `spawn_blocking`, the way the [`server`](crate::server) routes do.
pub struct Receiver {
    dir: PathBuf,
    replicas: Mutex<HashMap<String, Arc<Mutex<Replica>>>>,
}

impl std::fmt::Debug for Receiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding one of these leaves nothing half-written that
    // the next caller can't cope with, so poisoning is ignored.
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Receiver {
    /// A receiver keeping its replicas in `dir`. The directory is created if
    /// it doesn't exist. Replicas already there are picked up as they are.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            replicas: Mutex::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The replica's database file. Open it read-only to look inside; a
    /// write would break the stream.
    pub fn db_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db"))
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.json"))
    }

    /// A fresh path in the receiver's directory to write an incoming base
    /// to before handing it to [`install_base`](Self::install_base).
    pub fn staging_path(&self, name: &str) -> PathBuf {
        self.dir
            .join(format!("{name}.db.base-{}.tmp", temp_suffix()))
    }

    fn entry(&self, name: &str) -> Result<Arc<Mutex<Replica>>> {
        if !valid_name(name) {
            return Err(Error::InvalidName(name.to_owned()));
        }
        let mut replicas = lock(&self.replicas);
        if let Some(r) = replicas.get(name) {
            return Ok(Arc::clone(r));
        }
        let saved: Saved = std::fs::read(self.state_path(name))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let replica = Arc::new(Mutex::new(Replica {
            generation: saved.generation,
            db: None,
            pending: None,
            committed: 0,
            present: self.db_path(name).exists(),
        }));
        replicas.insert(name.to_owned(), Arc::clone(&replica));
        Ok(replica)
    }

    /// Like `entry`, but `None` for a name with nothing on disk instead of
    /// adding it to the map. Status and delete requests can name anything,
    /// and the map would otherwise keep every name it was asked about.
    fn existing(&self, name: &str) -> Result<Option<Arc<Mutex<Replica>>>> {
        if !valid_name(name) {
            return Err(Error::InvalidName(name.to_owned()));
        }
        if let Some(r) = lock(&self.replicas).get(name) {
            return Ok(Some(Arc::clone(r)));
        }
        if !self.db_path(name).exists() && !self.state_path(name).exists() {
            return Ok(None);
        }
        self.entry(name).map(Some)
    }

    fn save(&self, name: &str, r: &Replica) -> Result<()> {
        let bytes = serde_json::to_vec(&Saved {
            generation: r.generation,
        })?;
        write_atomic(&self.state_path(name), &bytes)?;
        Ok(())
    }

    /// The replica's handle, opened with checkpointing off on first use.
    fn open<'a>(&self, name: &str, r: &'a mut Replica) -> Result<&'a RawDb> {
        let db = if let Some(db) = r.db.take() {
            db
        } else {
            let db = RawDb::open(&self.db_path(name), BUSY_TIMEOUT)?;
            db.disable_checkpoint()?;
            r.committed = db.head(BUSY_TIMEOUT)?.ok_or_else(|| Error::Sqlite {
                context: format!("opening replica {name}"),
                code: libsql_ffi::SQLITE_BUSY,
                message: "held by a writer".into(),
            })?;
            db
        };
        Ok(r.db.insert(db))
    }

    /// Installs `staged` as the replica of `name` at `generation`.
    ///
    /// The staged file is moved into place on success and removed on
    /// failure. It must be a SQLite main database file, or empty: a database
    /// that has never been checkpointed has nothing in its main file yet, and
    /// that's a valid base.
    pub fn install_base(&self, name: &str, staged: &Path, generation: u64) -> Result<Status> {
        let result = self.install_base_inner(name, staged, generation);
        if result.is_err() {
            let _ = std::fs::remove_file(staged);
        }
        result
    }

    fn install_base_inner(&self, name: &str, staged: &Path, generation: u64) -> Result<Status> {
        check_base(staged)?;
        let entry = self.entry(name)?;
        let mut r = lock(&entry);
        // With checkpointing off, closing the handle leaves the old -wal
        // behind. It has to go before the new file arrives, or SQLite would
        // replay it into the wrong database.
        r.db = None;
        r.pending = None;
        let target = self.db_path(name);
        remove_sidecars(&target);
        std::fs::rename(staged, &target)?;
        sync_parent(&target)?;
        r.generation = generation;
        r.committed = 0;
        r.present = true;
        self.save(name, &r)?;
        // Open right away so the -shm exists for read-only readers.
        self.open(name, &mut r)?;
        Ok(self.status_of(name, &r))
    }

    /// Applies a batch of frames: `body` is whole frames of
    /// `FRAME_HEADER + page_size` bytes each, numbered from `first`.
    pub fn apply_frames(
        &self,
        name: &str,
        generation: u64,
        first: u32,
        page_size: u32,
        body: &[u8],
    ) -> Result<Status, Reject> {
        if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(Reject::Bad(format!("{page_size} isn't a SQLite page size")));
        }
        let frame_len = wal::FRAME_HEADER + page_size as usize;
        if body.is_empty() || !body.len().is_multiple_of(frame_len) {
            return Err(Reject::Bad(format!(
                "body is {} bytes, not a whole number of {frame_len}-byte frames",
                body.len()
            )));
        }
        if first == 0 {
            return Err(Reject::Bad("frames are numbered from 1".into()));
        }
        let needs_base = |e: Error| Reject::NeedsBase(e.to_string());
        let entry = self.entry(name).map_err(|e| Reject::Bad(e.to_string()))?;
        let mut guard = lock(&entry);
        // Reborrow through a plain &mut so the handle and the counters can be
        // borrowed separately.
        let r: &mut Replica = &mut guard;
        if !r.present {
            return Err(Reject::NeedsBase("no base installed".into()));
        }
        let replica_page_size = self
            .open(name, r)
            .and_then(RawDb::page_size)
            .map_err(needs_base)?;
        if replica_page_size != page_size {
            return Err(Reject::Bad(format!(
                "replica page size is {replica_page_size}, batch says {page_size}"
            )));
        }
        let db = r.db.as_ref().expect("opened above");

        let continues_session = r.pending == Some(first) && r.generation == generation;
        if !continues_session {
            if r.pending.take().is_some() {
                // This batch doesn't pick up the open session, so abort it.
                // Its frames sit past the committed count and get overwritten
                // by whatever comes next.
                let _ = db.insert_end();
            }
            let committed = match db.head(BUSY_TIMEOUT) {
                Ok(Some(n)) => n,
                Ok(None) => return Err(Reject::Busy),
                Err(e) => return Err(needs_base(e)),
            };
            r.committed = committed;
            if generation == r.generation && first == committed + 1 {
                // Carries on where the replica is.
            } else if generation > r.generation && first == 1 {
                // The sender checkpointed and restarted its WAL at frame 1.
                // Do the same so the frame numbers line up again.
                match db.checkpoint_truncate() {
                    Ok(ck) if ck.complete() => {}
                    Ok(_) => return Err(Reject::Busy),
                    Err(e) => return Err(needs_base(e)),
                }
                r.generation = generation;
                r.committed = 0;
                self.save(name, r).map_err(needs_base)?;
            } else {
                return Err(Reject::Mismatch {
                    generation: r.generation,
                    frames: committed,
                });
            }
            db.insert_begin().map_err(needs_base)?;
        }

        let mut no = first;
        let mut in_session = true;
        let mut committed = r.committed;
        for frame in body.chunks_exact(frame_len) {
            if !in_session {
                db.insert_begin().map_err(needs_base)?;
                in_session = true;
            }
            if let Err(e) = db.insert_frame(no, frame) {
                let _ = db.insert_end();
                r.pending = None;
                r.committed = committed;
                return Err(needs_base(e));
            }
            if wal::size_after(frame) > 0 {
                if let Err(e) = db.insert_end() {
                    r.pending = None;
                    return Err(needs_base(e));
                }
                in_session = false;
                committed = no;
            }
            no += 1;
        }
        r.pending = in_session.then_some(no);
        r.committed = committed;
        Ok(self.status_of(name, r))
    }

    fn status_of(&self, name: &str, r: &Replica) -> Status {
        let db = self.db_path(name);
        let len = |p: &Path| std::fs::metadata(p).map_or(0, |m| m.len());
        Status {
            generation: r.generation,
            frames: r.committed,
            pending: r.pending.is_some(),
            bytes: len(&db) + len(&sidecar(&db, "-wal")),
        }
    }

    /// The position of one replica, or `None` if there isn't one.
    pub fn status(&self, name: &str) -> Option<Status> {
        let entry = self.existing(name).ok()??;
        let mut r = lock(&entry);
        if !r.present {
            return None;
        }
        // The frame count needs the WAL write lock, which an open insert
        // session holds, so it's only refreshed between sessions.
        if r.pending.is_none() {
            if let Ok(db) = self.open(name, &mut r) {
                if let Ok(Some(n)) = db.head(Duration::from_millis(200)) {
                    r.committed = n;
                }
            }
        }
        Some(self.status_of(name, &r))
    }

    /// Every replica in the directory, sorted by name.
    pub fn list(&self) -> Vec<(String, Status)> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .filter_map(|e| {
                let path = e.ok()?.path();
                if path.extension()? != "db" {
                    return None;
                }
                Some(path.file_stem()?.to_str()?.to_owned())
            })
            .collect();
        names.sort_unstable();
        names
            .into_iter()
            .filter_map(|name| self.status(&name).map(|s| (name, s)))
            .collect()
    }

    /// Promotes a replica: folds its WAL into the main file and moves the
    /// file to `dest`, leaving no replica behind. Open `dest` as an ordinary
    /// database afterwards.
    ///
    /// An insert session that's still open ends without its commit, so the
    /// file holds exactly the transactions the sender had committed.
    ///
    /// `dest` can be on another filesystem. If `rename(2)` fails with
    /// `EXDEV`, the file is copied, synced and the original removed.
    pub fn take(&self, name: &str, dest: &Path) -> Result<Take> {
        let entry = self.entry(name)?;
        let mut r = lock(&entry);
        if !r.present {
            return Ok(Take::Absent);
        }
        r.db = None;
        r.pending = None;
        let db_path = self.db_path(name);
        // This handle keeps checkpointing on, so closing it removes the
        // -wal and -shm.
        let db = RawDb::open(&db_path, BUSY_TIMEOUT)?;
        let frames = db.head(BUSY_TIMEOUT)?.unwrap_or(0);
        let ck = db.checkpoint_truncate()?;
        drop(db);
        if !ck.complete() {
            self.open(name, &mut r)?;
            return Ok(Take::Busy);
        }
        remove_sidecars(&db_path);
        move_file(&db_path, dest)?;
        let _ = std::fs::remove_file(self.state_path(name));
        let generation = std::mem::take(&mut r.generation);
        r.present = false;
        r.committed = 0;
        let bytes = std::fs::metadata(dest).map_or(0, |m| m.len());
        Ok(Take::Moved {
            generation,
            frames,
            bytes,
        })
    }

    /// Removes a replica and its state. Returns `false` if there wasn't one.
    pub fn delete(&self, name: &str) -> Result<bool> {
        let Some(entry) = self.existing(name)? else {
            return Ok(false);
        };
        let mut r = lock(&entry);
        r.db = None;
        r.pending = None;
        let db_path = self.db_path(name);
        let _ = std::fs::remove_file(&db_path);
        remove_sidecars(&db_path);
        let _ = std::fs::remove_file(self.state_path(name));
        r.generation = 0;
        r.committed = 0;
        Ok(std::mem::take(&mut r.present))
    }
}

fn remove_sidecars(db: &Path) {
    let _ = std::fs::remove_file(sidecar(db, "-wal"));
    let _ = std::fs::remove_file(sidecar(db, "-shm"));
}

/// Moves `src` to `dst`, falling back to copy-and-delete when `rename(2)`
/// says `EXDEV`.
///
/// Two directories on the same disk can still be two mount points. systemd
/// does this: with `ProtectSystem=strict`, each `ReadWritePaths` entry is its
/// own bind mount inside the service's namespace, and a rename between them
/// fails even though `df` and `stat` say it's one filesystem. Promotion is
/// exactly the case where the replica directory and the data directory are
/// likely to be listed separately.
fn move_file(src: &Path, dst: &Path) -> io::Result<()> {
    move_file_with(src, dst, |a, b| std::fs::rename(a, b))
}

fn move_file_with<R>(src: &Path, dst: &Path, rename: R) -> io::Result<()>
where
    R: Fn(&Path, &Path) -> io::Result<()>,
{
    match rename(src, dst) {
        Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {}
        other => return other,
    }
    std::fs::copy(src, dst)?;
    // Once src is gone dst is the only copy, so both its bytes and its
    // directory entry hit the disk first.
    File::options().write(true).open(dst)?.sync_all()?;
    let _ = sync_parent(dst);
    // If src won't go away there are two copies of a database that's about
    // to be written under one name. Report it so the caller can drop dst and
    // keep the replica.
    std::fs::remove_file(src)
}

/// A base is either empty or a SQLite main file: the magic header and a
/// whole number of pages.
fn check_base(path: &Path) -> Result<()> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(());
    }
    let mut header = [0u8; 100];
    file.read_exact(&mut header)
        .map_err(|_| Error::NotADatabase("shorter than the header".into()))?;
    if &header[..16] != SQLITE_MAGIC {
        return Err(Error::NotADatabase("bad magic".into()));
    }
    // A stored page size of 1 means 65536, which doesn't fit in a u16.
    let page_size = match u16::from_be_bytes([header[16], header[17]]) {
        1 => 65536,
        n => u64::from(n),
    };
    if page_size < 512 || len % page_size != 0 {
        return Err(Error::NotADatabase(format!(
            "{len} bytes isn't a whole number of {page_size}-byte pages"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("walship-rx-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn frames(db: &RawDb, from: u32, to: u32, page_size: u32) -> Vec<u8> {
        (from..=to)
            .flat_map(|n| db.get_frame(n, page_size).unwrap())
            .collect()
    }

    fn read_only(path: &Path) -> RawDb {
        let db = RawDb::open(path, BUSY_TIMEOUT).unwrap();
        db.exec("PRAGMA query_only=1").unwrap();
        db.disable_checkpoint().unwrap();
        db
    }

    fn rows(db: &RawDb) -> u32 {
        db.query_u32("SELECT count(*) FROM t").unwrap()
    }

    #[test]
    fn follows_a_primary() {
        let dir = scratch("follow");
        let src = RawDb::open(&dir.join("src.db"), BUSY_TIMEOUT).unwrap();
        src.disable_checkpoint().unwrap();
        let ps = src.page_size().unwrap();
        src.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        src.exec("INSERT INTO t(v) VALUES ('a'),('b')").unwrap();

        let rx = Receiver::new(dir.join("replicas")).unwrap();
        let head = src.head(BUSY_TIMEOUT).unwrap().unwrap();
        assert!(matches!(
            rx.apply_frames("x", 1, 1, ps, &frames(&src, 1, head, ps)),
            Err(Reject::NeedsBase(_))
        ));

        // An empty base is fine: everything the database has is in its WAL.
        let staged = rx.staging_path("x");
        std::fs::write(&staged, b"").unwrap();
        let s = rx.install_base("x", &staged, 1).unwrap();
        assert_eq!((s.generation, s.frames), (1, 0));
        assert!(!staged.exists());

        // One big transaction, sent in two batches split in the middle.
        src.exec("BEGIN").unwrap();
        for i in 0..1500 {
            src.exec(&format!(
                "INSERT INTO t(v) VALUES ('{}')",
                "y".repeat(100 + i % 5)
            ))
            .unwrap();
        }
        src.exec("COMMIT").unwrap();
        let head = src.head(BUSY_TIMEOUT).unwrap().unwrap();
        let cut = head / 2;
        let s = rx
            .apply_frames("x", 1, 1, ps, &frames(&src, 1, cut, ps))
            .unwrap();
        assert!(s.pending);
        assert!(s.frames < cut);
        let reader = read_only(&rx.db_path("x"));
        assert_eq!(rows(&reader), 2, "half a transaction must stay invisible");
        let s = rx
            .apply_frames("x", 1, cut + 1, ps, &frames(&src, cut + 1, head, ps))
            .unwrap();
        assert_eq!((s.frames, s.pending), (head, false));
        assert_eq!(rows(&reader), 1502);

        // Out of place: the reply says where the replica is.
        assert!(matches!(
            rx.apply_frames("x", 1, head + 5, ps, &frames(&src, 1, 1, ps)),
            Err(Reject::Mismatch { generation: 1, frames }) if frames == head
        ));
        assert!(matches!(
            rx.apply_frames("x", 1, head, ps, &frames(&src, head, head, ps)),
            Err(Reject::Mismatch { .. })
        ));

        // A restarted receiver picks its state up from disk.
        drop(rx);
        let rx = Receiver::new(dir.join("replicas")).unwrap();
        assert_eq!(rx.status("x").unwrap().frames, head);
        src.exec("INSERT INTO t(v) VALUES ('after restart')")
            .unwrap();
        let head2 = src.head(BUSY_TIMEOUT).unwrap().unwrap();
        let s = rx
            .apply_frames("x", 1, head + 1, ps, &frames(&src, head + 1, head2, ps))
            .unwrap();
        assert_eq!(s.frames, head2);
        assert_eq!(rows(&reader), 1503);

        // The sender truncates its WAL; the replica follows.
        assert!(src.checkpoint_truncate().unwrap().complete());
        src.exec("INSERT INTO t(v) VALUES ('gen2')").unwrap();
        assert_eq!(src.head(BUSY_TIMEOUT).unwrap(), Some(1));
        let s = rx
            .apply_frames("x", 2, 1, ps, &frames(&src, 1, 1, ps))
            .unwrap();
        assert_eq!((s.generation, s.frames), (2, 1));
        assert_eq!(rows(&reader), 1504);
        assert_eq!(reader.query_text("PRAGMA integrity_check").unwrap(), "ok");
        assert!(matches!(
            rx.apply_frames("x", 1, 2, ps, &frames(&src, 1, 1, ps)),
            Err(Reject::Mismatch { .. })
        ));

        // A new base replaces the replica.
        drop(reader);
        assert!(src.checkpoint_truncate().unwrap().complete());
        let staged = rx.staging_path("x");
        src.copy_main_file(&staged).unwrap();
        let s = rx.install_base("x", &staged, 3).unwrap();
        assert_eq!((s.generation, s.frames), (3, 0));
        src.exec("INSERT INTO t(v) VALUES ('gen3')").unwrap();
        assert_eq!(
            rx.apply_frames("x", 3, 1, ps, &frames(&src, 1, 1, ps))
                .unwrap()
                .frames,
            1
        );
        assert_eq!(rows(&read_only(&rx.db_path("x"))), 1505);

        assert_eq!(rx.list().len(), 1);
        assert!(rx.delete("x").unwrap());
        assert!(rx.status("x").is_none());
        assert!(!rx.db_path("x").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_base_must_be_a_database() {
        let dir = scratch("base");
        let rx = Receiver::new(&dir).unwrap();
        let staged = rx.staging_path("y");
        std::fs::write(&staged, b"hello, not a database").unwrap();
        assert!(matches!(
            rx.install_base("y", &staged, 1),
            Err(Error::NotADatabase(_))
        ));
        assert!(!staged.exists(), "a rejected base should be removed");
        assert!(rx.status("y").is_none());
        assert!(rx.status("../y").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn asking_about_a_missing_replica_leaves_no_trace() {
        let dir = scratch("missing");
        let rx = Receiver::new(&dir).unwrap();
        for i in 0..100 {
            assert!(rx.status(&format!("nope{i}")).is_none());
            assert!(!rx.delete(&format!("nope{i}")).unwrap());
        }
        assert!(lock(&rx.replicas).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn take_promotes_the_replica() {
        let dir = scratch("take");
        let src = RawDb::open(&dir.join("src.db"), BUSY_TIMEOUT).unwrap();
        src.disable_checkpoint().unwrap();
        let ps = src.page_size().unwrap();
        src.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        src.exec("INSERT INTO t(v) VALUES ('a'),('b'),('c')")
            .unwrap();

        let rx = Receiver::new(dir.join("replicas")).unwrap();
        let staged = rx.staging_path("x");
        std::fs::write(&staged, b"").unwrap();
        rx.install_base("x", &staged, 4).unwrap();
        let head = src.head(BUSY_TIMEOUT).unwrap().unwrap();
        rx.apply_frames("x", 4, 1, ps, &frames(&src, 1, head, ps))
            .unwrap();

        let dest = dir.join("promoted.db");
        let Take::Moved {
            generation,
            frames,
            bytes,
        } = rx.take("x", &dest).unwrap()
        else {
            panic!("expected the replica to move");
        };
        assert_eq!((generation, frames), (4, head));
        assert_eq!(bytes, std::fs::metadata(&dest).unwrap().len());
        let promoted = RawDb::open(&dest, BUSY_TIMEOUT).unwrap();
        assert_eq!(rows(&promoted), 3);
        assert_eq!(promoted.query_text("PRAGMA integrity_check").unwrap(), "ok");
        for leftover in ["x.db", "x.db-wal", "x.db-shm", "x.json"] {
            assert!(
                !rx.dir().join(leftover).exists(),
                "{leftover} is still there"
            );
        }
        assert_eq!(rx.take("x", &dir.join("again.db")).unwrap(), Take::Absent);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn move_falls_back_to_copy_across_mounts() {
        let dir = scratch("exdev");
        let src = dir.join("from.db");
        let dst = dir.join("to.db");
        std::fs::write(&src, b"pages").unwrap();

        let exdev = |_: &Path, _: &Path| Err(io::ErrorKind::CrossesDevices.into());
        move_file_with(&src, &dst, exdev).unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"pages");
        assert!(!src.exists());

        // Any other error is passed through, not retried as a copy.
        std::fs::write(&src, b"pages").unwrap();
        let denied = |_: &Path, _: &Path| Err(io::ErrorKind::PermissionDenied.into());
        assert_eq!(
            move_file_with(&src, &dst, denied).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(src.exists());

        let renamed = dir.join("renamed.db");
        move_file(&src, &renamed).unwrap();
        assert_eq!(std::fs::read(&renamed).unwrap(), b"pages");
        assert!(!src.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
