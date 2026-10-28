//! The WAL primitives everything else is built on.
//!
//! libSQL adds a handful of C functions to SQLite for reading frames out of a
//! WAL and writing them into another one (`libsql_wal_frame_count`,
//! `libsql_wal_get_frame`, `libsql_wal_insert_*`). [`RawDb`] is a thin, safe
//! wrapper around one `sqlite3` handle that calls them.
//!
//! Some behaviour of those functions isn't written down anywhere, and the
//! rest of the crate depends on it. Each point below is checked by a test in
//! this module or by `examples/stress.rs`:
//!
//! - `frame_count` takes the WAL write lock without waiting, so it returns
//!   `SQLITE_BUSY` while any writer has a transaction open. It also returns
//!   the connection's cached copy of the wal-index header, which only a read
//!   that touches the database refreshes. [`RawDb::head`] does both.
//! - Inserting a frame numbered past `frame_count + 1` doesn't fail. The
//!   frame is appended at `frame_count + 1`. Inserting one at or below
//!   `frame_count` is a no-op when the page matches and a conflict when it
//!   doesn't. Checking for gaps is the caller's job.
//! - An insert session that ends without a commit frame leaves its frames on
//!   disk but out of the wal-index, so the next session starts again at the
//!   last commit. That's why a batch can safely end in the middle of a
//!   transaction.
//! - Inserted frames go through the normal WAL append path, so the replica's
//!   WAL has its own salts and checksums and survives a crash like any other.

use std::{
    ffi::{c_char, c_int, c_uint, CStr, CString},
    fs::File,
    io::{self, Read, Write},
    path::Path,
    ptr,
    time::{Duration, Instant},
};

use libsql_ffi::{
    libsql_wal_disable_checkpoint, libsql_wal_frame_count, libsql_wal_get_frame,
    libsql_wal_insert_begin, libsql_wal_insert_end, libsql_wal_insert_frame, sqlite3,
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_errmsg, sqlite3_exec,
    sqlite3_file, sqlite3_file_control, sqlite3_finalize, sqlite3_free, sqlite3_int64,
    sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_step, SQLITE_BUSY, SQLITE_DONE, SQLITE_ERROR,
    SQLITE_FCNTL_FILE_POINTER, SQLITE_MISMATCH, SQLITE_MISUSE, SQLITE_OK, SQLITE_OPEN_CREATE,
    SQLITE_OPEN_FULLMUTEX, SQLITE_OPEN_READWRITE, SQLITE_ROW, SQLITE_TOOBIG,
};

use crate::{Error, Result};

/// Size of the header in front of the page in every WAL frame.
pub const FRAME_HEADER: usize = 24;

/// Size of the `-wal` file header, in front of the first frame.
const WAL_HEADER: usize = 32;

/// How much [`RawDb::copy_main_file`] reads per call.
const COPY_CHUNK: usize = 1 << 20;

/// Result of a `PRAGMA wal_checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    /// SQLite couldn't finish because a reader or writer held it off past the
    /// busy timeout. Some frames may have been copied anyway.
    pub busy: bool,
    /// Frames in the WAL when the checkpoint ran.
    pub log: u32,
    /// How many of those were copied into the main file.
    pub checkpointed: u32,
}

impl Checkpoint {
    /// Every frame made it into the main file.
    #[must_use]
    pub fn complete(&self) -> bool {
        !self.busy && self.log == self.checkpointed
    }
}

/// One `sqlite3` handle, opened in WAL mode.
///
/// The handle is opened with `SQLITE_OPEN_FULLMUTEX`, so it's safe to move
/// between threads. It isn't `Sync`: walship only ever uses a handle from one
/// thread at a time and there's no reason to promise more.
pub struct RawDb {
    // A connection opened by `open_with_flags` and closed only by `Drop`.
    // The FFI calls below rely on it being valid for the life of the value.
    handle: *mut sqlite3,
}

// SAFETY: the handle is opened FULLMUTEX, so SQLite serialises every call on
// it no matter which thread makes it.
unsafe impl Send for RawDb {}

impl Drop for RawDb {
    fn drop(&mut self) {
        // SAFETY: the handle came from a successful sqlite3_open_v2 and is
        // closed only here. Every statement this type prepares is finalized
        // before the method returns, so sqlite3_close can't fail with
        // SQLITE_BUSY and leak the handle.
        unsafe { sqlite3_close(self.handle) };
    }
}

impl std::fmt::Debug for RawDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDb").finish_non_exhaustive()
    }
}

impl RawDb {
    /// Opens `path` read-write, creating it if needed, sets the busy timeout
    /// and switches it to WAL mode.
    ///
    /// The timeout goes first. Setting the journal mode reads the schema, and
    /// on a file another connection is opening or recovering at that moment
    /// it would fail with SQLITE_BUSY instead of waiting.
    pub fn open(path: &Path, busy_timeout: Duration) -> Result<Self> {
        let db = Self::open_with_flags(path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)?;
        db.exec(&format!("PRAGMA busy_timeout={}", busy_timeout.as_millis()))?;
        let mode = db.query_text("PRAGMA journal_mode=WAL")?;
        if mode != "wal" {
            return Err(Error::Sqlite {
                context: format!("enabling WAL on {}", path.display()),
                code: SQLITE_ERROR,
                message: format!("journal_mode is {mode}"),
            });
        }
        Ok(db)
    }

    fn open_with_flags(path: &Path, flags: c_int) -> Result<Self> {
        let c_path = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| Error::Nul("database path"))?;
        let mut handle = ptr::null_mut();
        let rc = unsafe {
            sqlite3_open_v2(
                c_path.as_ptr(),
                &raw mut handle,
                flags | SQLITE_OPEN_FULLMUTEX,
                ptr::null(),
            )
        };
        if rc != SQLITE_OK {
            let message = if handle.is_null() {
                "out of memory".to_owned()
            } else {
                let m = unsafe { CStr::from_ptr(sqlite3_errmsg(handle)) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { sqlite3_close(handle) };
                m
            };
            return Err(Error::Sqlite {
                context: format!("opening {}", path.display()),
                code: rc,
                message,
            });
        }
        Ok(Self { handle })
    }

    /// The raw handle, for calling into libSQL directly. It stays owned by
    /// this `RawDb`; don't close it.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sqlite3 {
        self.handle
    }

    fn check(&self, rc: c_int, context: impl FnOnce() -> String) -> Result<()> {
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error(rc, context()))
        }
    }

    fn error(&self, rc: c_int, context: String) -> Error {
        Error::Sqlite {
            context,
            code: rc,
            message: self.errmsg(),
        }
    }

    fn errmsg(&self) -> String {
        unsafe { CStr::from_ptr(sqlite3_errmsg(self.handle)) }
            .to_string_lossy()
            .into_owned()
    }

    /// Turns off both the automatic checkpoint after a commit and the one
    /// SQLite runs when the last connection closes. See
    /// [`disable_checkpoint`].
    pub fn disable_checkpoint(&self) -> Result<()> {
        let rc = unsafe { libsql_wal_disable_checkpoint(self.handle) };
        self.check(rc, || "wal_disable_checkpoint".into())
    }

    /// Runs one or more statements and throws away any rows.
    pub fn exec(&self, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql).map_err(|_| Error::Nul("SQL"))?;
        let mut err: *mut c_char = ptr::null_mut();
        let rc = unsafe {
            sqlite3_exec(
                self.handle,
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                &raw mut err,
            )
        };
        if rc == SQLITE_OK {
            return Ok(());
        }
        let message = if err.is_null() {
            self.errmsg()
        } else {
            let m = unsafe { CStr::from_ptr(err) }
                .to_string_lossy()
                .into_owned();
            unsafe { sqlite3_free(err.cast()) };
            m
        };
        Err(Error::Sqlite {
            context: format!("exec `{sql}`"),
            code: rc,
            message,
        })
    }

    /// The first row of `sql` with every column as text, or an empty `Vec`
    /// if there are no rows.
    pub fn query_row(&self, sql: &str) -> Result<Vec<String>> {
        let c_sql = CString::new(sql).map_err(|_| Error::Nul("SQL"))?;
        let mut stmt = ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                self.handle,
                c_sql.as_ptr(),
                -1,
                &raw mut stmt,
                ptr::null_mut(),
            )
        };
        self.check(rc, || format!("prepare `{sql}`"))?;

        let result = match unsafe { sqlite3_step(stmt) } {
            SQLITE_ROW => {
                let n = unsafe { sqlite3_column_count(stmt) };
                Ok((0..n)
                    .map(|i| {
                        let p = unsafe { sqlite3_column_text(stmt, i) };
                        if p.is_null() {
                            String::new()
                        } else {
                            // SAFETY: column text is NUL-terminated and stays
                            // valid until the next step or finalize.
                            unsafe { CStr::from_ptr(p.cast()) }
                                .to_string_lossy()
                                .into_owned()
                        }
                    })
                    .collect())
            }
            SQLITE_DONE => Ok(Vec::new()),
            rc => Err(self.error(rc, format!("step `{sql}`"))),
        };
        unsafe { sqlite3_finalize(stmt) };
        result
    }

    /// The first column of the first row, or an empty string.
    pub fn query_text(&self, sql: &str) -> Result<String> {
        Ok(self.query_row(sql)?.into_iter().next().unwrap_or_default())
    }

    /// The first column of the first row, parsed as a `u32`.
    pub fn query_u32(&self, sql: &str) -> Result<u32> {
        let text = self.query_text(sql)?;
        text.parse().map_err(|_| Error::Sqlite {
            context: format!("query `{sql}`"),
            code: SQLITE_MISMATCH,
            message: format!("expected a number, got {text:?}"),
        })
    }

    /// The database page size in bytes.
    pub fn page_size(&self) -> Result<u32> {
        self.query_u32("PRAGMA page_size")
    }

    /// Frames in the WAL as this connection last saw them, or `None` while a
    /// writer holds the WAL lock. Doesn't wait. You almost always want
    /// [`head`](Self::head) instead.
    pub fn try_frame_count(&self) -> Result<Option<u32>> {
        let mut n: c_uint = 0;
        match unsafe { libsql_wal_frame_count(self.handle, &raw mut n) } {
            SQLITE_OK => Ok(Some(n)),
            SQLITE_BUSY => Ok(None),
            rc => Err(self.error(rc, "wal_frame_count".into())),
        }
    }

    /// The number of committed frames in the WAL.
    ///
    /// Runs a read that touches the database first, because the frame count
    /// comes from a cached header that only a real read refreshes (a bare
    /// `SELECT 1` doesn't count). Then retries for up to `wait` while a
    /// writer holds the lock. Returns `None` if the writer held it the whole
    /// time.
    pub fn head(&self, wait: Duration) -> Result<Option<u32>> {
        self.query_row("SELECT count(*) FROM sqlite_master")?;
        let start = Instant::now();
        loop {
            if let Some(n) = self.try_frame_count()? {
                return Ok(Some(n));
            }
            if start.elapsed() >= wait {
                return Ok(None);
            }
            std::thread::sleep(Duration::from_micros(500));
        }
    }

    /// Frame `no` (1-based), header and page together.
    ///
    /// libSQL doesn't check `no` against the frame count, so the caller has
    /// to: only ask for frames at or below a count you got from
    /// [`head`](Self::head).
    pub fn get_frame(&self, no: u32, page_size: u32) -> Result<Vec<u8>> {
        if !(512..=65536).contains(&page_size) {
            return Err(Error::Sqlite {
                context: format!("wal_get_frame {no}"),
                code: SQLITE_MISUSE,
                message: format!("{page_size} isn't a SQLite page size"),
            });
        }
        let mut buf = vec![0u8; FRAME_HEADER + page_size as usize];
        // At most 24 + 65536 bytes, checked above.
        let len = buf.len() as c_uint;
        // SAFETY: libSQL writes at most `len` bytes, the size of `buf`.
        let rc = unsafe { libsql_wal_get_frame(self.handle, no, buf.as_mut_ptr().cast(), len) };
        self.check(rc, || format!("wal_get_frame {no}"))?;
        Ok(buf)
    }

    /// Starts an insert session. Frames go in with
    /// [`insert_frame`](Self::insert_frame) until [`insert_end`](Self::insert_end).
    pub fn insert_begin(&self) -> Result<()> {
        let rc = unsafe { libsql_wal_insert_begin(self.handle) };
        self.check(rc, || "wal_insert_begin".into())
    }

    /// Ends an insert session. Frames after the last commit frame are
    /// dropped.
    pub fn insert_end(&self) -> Result<()> {
        let rc = unsafe { libsql_wal_insert_end(self.handle) };
        self.check(rc, || "wal_insert_end".into())
    }

    /// Writes one frame inside an insert session. Returns
    /// [`Error::Conflict`] if the WAL already holds a different page at that
    /// frame number.
    pub fn insert_frame(&self, no: u32, frame: &[u8]) -> Result<()> {
        let len = c_uint::try_from(frame.len()).map_err(|_| Error::Sqlite {
            context: format!("wal_insert_frame {no}"),
            code: SQLITE_TOOBIG,
            message: "frame too large".into(),
        })?;
        let mut conflict: c_int = 0;
        // SAFETY: `frame` is valid for `len` bytes. libSQL takes a mutable
        // pointer but only reads through it.
        let rc = unsafe {
            libsql_wal_insert_frame(
                self.handle,
                no,
                frame.as_ptr().cast_mut().cast(),
                len,
                &raw mut conflict,
            )
        };
        if conflict != 0 {
            return Err(Error::Conflict { frame: no });
        }
        self.check(rc, || format!("wal_insert_frame {no}"))
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`: copy every frame into the main file
    /// and reset the WAL to zero frames.
    ///
    /// Waits on readers up to the busy timeout. A writer that doesn't let go
    /// shows up as `busy`. A complete truncate reports
    /// `log == checkpointed == 0`, because the counts are read after the log
    /// was reset.
    pub fn checkpoint_truncate(&self) -> Result<Checkpoint> {
        self.checkpoint("TRUNCATE")
    }

    /// `PRAGMA wal_checkpoint(PASSIVE)`: copy every frame that no reader
    /// still needs into the main file, without waiting and without
    /// restarting the log.
    ///
    /// When `log == checkpointed` the whole log is in the main file, and the
    /// next write restarts the log at frame 1 with a new salt.
    pub fn checkpoint_passive(&self) -> Result<Checkpoint> {
        self.checkpoint("PASSIVE")
    }

    fn checkpoint(&self, mode: &str) -> Result<Checkpoint> {
        let row = self.query_row(&format!("PRAGMA wal_checkpoint({mode})"))?;
        let bad = || Error::Sqlite {
            context: format!("wal_checkpoint({mode})"),
            code: SQLITE_ERROR,
            message: format!("unexpected result {row:?}"),
        };
        let [busy, log, checkpointed] = row.as_slice() else {
            return Err(bad());
        };
        let num = |s: &str| s.parse::<i64>().map_err(|_| bad());
        // SQLite reports -1 for both counts when the database isn't in WAL
        // mode. Clamp so that can't wrap.
        let count = |s: &str| num(s).map(|n| u32::try_from(n.max(0)).unwrap_or(u32::MAX));
        Ok(Checkpoint {
            busy: num(busy)? != 0,
            log: count(log)?,
            checkpointed: count(checkpointed)?,
        })
    }

    /// Copies the main database file, byte for byte, to `dest` and returns
    /// the number of bytes written.
    ///
    /// The bytes are read through the file descriptor SQLite already has
    /// open, not through a second `open(2)` of the same path. POSIX drops
    /// every `fcntl` lock a process holds on a file as soon as *any*
    /// descriptor for that file is closed, so opening and closing a
    /// live database with `File::open` silently releases the SHARED lock
    /// that WAL mode depends on. SQLite inside the process doesn't notice.
    /// Other processes do: the next one to open the database read-write
    /// checkpoints the WAL and deletes `-wal` and `-shm` on close, while
    /// your process is still writing to them.
    ///
    /// Only the main file is copied, never the WAL, so the copy is the
    /// database as of frame 0. That's what a base for a stream of WAL frames
    /// has to be. `VACUUM INTO` or the backup API would give you the logical
    /// database instead, which is the wrong thing here.
    pub fn copy_main_file(&self, dest: &Path) -> Result<u64> {
        let mut file: *mut sqlite3_file = ptr::null_mut();
        let rc = unsafe {
            sqlite3_file_control(
                self.handle,
                c"main".as_ptr(),
                SQLITE_FCNTL_FILE_POINTER,
                ptr::from_mut(&mut file).cast(),
            )
        };
        self.check(rc, || "file_control(FILE_POINTER)".into())?;

        let not_open = || Error::Sqlite {
            context: "copying the main file".into(),
            code: SQLITE_MISUSE,
            message: "the main database file isn't open".into(),
        };
        // A file the VFS never opened has null methods.
        // SAFETY: FILE_POINTER hands back the connection's own sqlite3_file,
        // which lives as long as the connection, or null.
        let methods = unsafe { file.as_ref() }
            .map(|f| f.pMethods)
            .filter(|m| !m.is_null())
            .ok_or_else(not_open)?;
        let (Some(x_read), Some(x_file_size)) =
            (unsafe { ((*methods).xRead, (*methods).xFileSize) })
        else {
            return Err(not_open());
        };

        let mut size: sqlite3_int64 = 0;
        let rc = unsafe { x_file_size(file, &raw mut size) };
        self.check(rc, || "xFileSize".into())?;

        let mut out = File::create(dest)?;
        let mut buf = vec![0u8; COPY_CHUNK];
        let mut offset: sqlite3_int64 = 0;
        while offset < size {
            let n = usize::try_from(size - offset).map_or(buf.len(), |left| left.min(buf.len()));
            // SAFETY: `buf` holds at least `n` bytes, and n is at most
            // COPY_CHUNK, so the casts can't truncate. On the unix VFS xRead
            // is a pread: no lock, no shared offset, and the connection's
            // lock state is left alone.
            let rc = unsafe { x_read(file, buf.as_mut_ptr().cast(), n as c_int, offset) };
            self.check(rc, || format!("xRead of {n} bytes at {offset}"))?;
            out.write_all(&buf[..n])?;
            offset += n as sqlite3_int64;
        }
        out.sync_all()?;
        Ok(size.unsigned_abs())
    }
}

/// Turns off checkpointing on a connection you opened yourself.
///
/// Every connection that writes to a shipped database needs this, not only
/// the shipper's own. A checkpoint folds frames into the main file and lets
/// SQLite restart the WAL, and a frame that's gone from the WAL before it
/// was shipped can't be shipped. With checkpointing off on every connection,
/// the shipper decides when it happens.
///
/// # Safety
///
/// `db` must be a valid, open `sqlite3` handle from the same libSQL library
/// walship links against (the one `libsql-ffi` builds), not from a separate
/// copy of SQLite.
pub unsafe fn disable_checkpoint(db: *mut sqlite3) -> Result<()> {
    match unsafe { libsql_wal_disable_checkpoint(db) } {
        SQLITE_OK => Ok(()),
        rc => Err(Error::Sqlite {
            context: "wal_disable_checkpoint".into(),
            code: rc,
            message: unsafe { CStr::from_ptr(sqlite3_errmsg(db)) }
                .to_string_lossy()
                .into_owned(),
        }),
    }
}

/// The database size in pages that this frame commits, or 0 if the frame is
/// in the middle of a transaction. Non-zero means it's a commit frame.
///
/// # Panics
///
/// If `frame` is shorter than a frame header.
#[must_use]
pub fn size_after(frame: &[u8]) -> u32 {
    u32::from_be_bytes(frame[4..8].try_into().unwrap())
}

/// The salt from a `-wal` file's header. SQLite picks a new salt every time
/// it restarts the log, so a changed salt means the frame numbers start over.
///
/// Returns `None` if the file doesn't exist or has no header yet (a
/// truncated WAL is empty until the next write).
pub fn wal_salt(wal_path: &Path) -> io::Result<Option<[u8; 8]>> {
    let mut file = match File::open(wal_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut header = [0u8; WAL_HEADER];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(Some(header[16..24].try_into().unwrap())),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("walship-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Asks another process whether it can take a write lock on SQLite's
    /// shared-lock range of `path`. POSIX locks never conflict with the
    /// process holding them, so asking from inside the process proves
    /// nothing.
    fn locked_from_another_process(path: &Path) -> bool {
        // SQLite's shared-lock range on the main file: 510 bytes starting two
        // bytes past PENDING_BYTE (see os_unix.c).
        const SHARED_FIRST: libc::off_t = 0x4000_0000 + 2;
        const SHARED_SIZE: libc::off_t = 510;
        let c_path = CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        match unsafe { libc::fork() } {
            -1 => panic!("fork failed"),
            0 => {
                // Only async-signal-safe calls in the child.
                let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR) };
                if fd < 0 {
                    unsafe { libc::_exit(2) };
                }
                let mut lock: libc::flock = unsafe { std::mem::zeroed() };
                lock.l_type = libc::F_WRLCK as _;
                lock.l_whence = libc::SEEK_SET as _;
                lock.l_start = SHARED_FIRST;
                lock.l_len = SHARED_SIZE;
                let rc = unsafe { libc::fcntl(fd, libc::F_SETLK, &mut lock) };
                unsafe { libc::_exit(i32::from(rc != 0)) };
            }
            pid => {
                let mut status = 0;
                assert_eq!(unsafe { libc::waitpid(pid, &raw mut status, 0) }, pid);
                assert!(libc::WIFEXITED(status), "lock probe was killed");
                let code = libc::WEXITSTATUS(status);
                assert!(code < 2, "lock probe couldn't open {}", path.display());
                code == 1
            }
        }
    }

    #[test]
    fn copying_the_main_file_keeps_the_lock() {
        let dir = scratch("copy");
        let path = dir.join("live.db");
        let snapshot = dir.join("live.snapshot");

        let db = RawDb::open(&path, SECOND).unwrap();
        db.disable_checkpoint().unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.exec("INSERT INTO t(v) VALUES ('a'),('b')").unwrap();
        assert!(locked_from_another_process(&path));
        assert!(db.checkpoint_truncate().unwrap().complete());

        // `metadata` is a stat, which doesn't open the file. Reading the live
        // file to compare bytes would break the very lock under test, so that
        // happens last.
        let n = db.copy_main_file(&snapshot).unwrap();
        assert_eq!(n, std::fs::metadata(&path).unwrap().len());
        assert!(locked_from_another_process(&path), "copy dropped the lock");

        let bytes = std::fs::read(&snapshot).unwrap();
        // Bytes 18 and 19 are the read/write format versions, 2 for WAL.
        assert_eq!((bytes[18], bytes[19]), (2, 2));
        let copy = RawDb::open(&snapshot, SECOND).unwrap();
        assert_eq!(copy.query_text("PRAGMA integrity_check").unwrap(), "ok");
        assert_eq!(copy.query_u32("SELECT count(*) FROM t").unwrap(), 2);
        drop(copy);
        assert!(locked_from_another_process(&path));

        // Writes keep going to the WAL and the main file stays put.
        db.exec("INSERT INTO t(v) VALUES ('c')").unwrap();
        assert!(db.head(SECOND).unwrap().unwrap() > 0);
        assert_eq!(db.query_u32("SELECT count(*) FROM t").unwrap(), 3);

        // The same read done the naive way, for contrast.
        let live = std::fs::read(&path).unwrap();
        assert!(
            !locked_from_another_process(&path),
            "a plain open and close should have dropped the lock"
        );
        assert_eq!(bytes, live);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn frames_round_trip_between_two_handles() {
        let dir = scratch("ship");
        let src = RawDb::open(&dir.join("p.db"), SECOND).unwrap();
        src.disable_checkpoint().unwrap();
        let page_size = src.page_size().unwrap();
        src.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        src.exec("INSERT INTO t(v) VALUES ('a'),('b')").unwrap();
        // One big transaction, so a batch can end inside it.
        src.exec("BEGIN").unwrap();
        for i in 0..2000 {
            src.exec(&format!(
                "INSERT INTO t(v) VALUES ('{}')",
                "x".repeat(200 + i % 7)
            ))
            .unwrap();
        }
        src.exec("COMMIT").unwrap();
        let head = src.head(SECOND).unwrap().unwrap();
        assert!(head > 10, "head={head}");

        let frames: Vec<Vec<u8>> = (1..=head)
            .map(|n| src.get_frame(n, page_size).unwrap())
            .collect();
        let frame = |n: u32| &frames[n as usize - 1];
        assert!(size_after(frame(head)) > 0, "last frame should commit");
        let commits: Vec<u32> = (1..=head).filter(|&n| size_after(frame(n)) > 0).collect();

        // First session stops three frames into the big transaction.
        let last_small = commits[commits.len() - 2];
        let cut = last_small + 3;
        assert!(cut < head && size_after(frame(cut)) == 0);
        {
            let dst = RawDb::open(&dir.join("r.db"), SECOND).unwrap();
            dst.disable_checkpoint().unwrap();
            dst.insert_begin().unwrap();
            for n in 1..=cut {
                dst.insert_frame(n, frame(n)).unwrap();
                if size_after(frame(n)) > 0 {
                    dst.insert_end().unwrap();
                    dst.insert_begin().unwrap();
                }
            }
            dst.insert_end().unwrap();
            assert_eq!(dst.head(SECOND).unwrap(), Some(last_small));
        }

        // The committed part survives a close and reopen.
        let dst = RawDb::open(&dir.join("r.db"), SECOND).unwrap();
        dst.disable_checkpoint().unwrap();
        assert_eq!(dst.head(SECOND).unwrap(), Some(last_small));
        assert_eq!(dst.query_u32("SELECT count(*) FROM t").unwrap(), 2);

        dst.insert_begin().unwrap();
        for n in last_small + 1..=head {
            dst.insert_frame(n, frame(n)).unwrap();
        }
        dst.insert_end().unwrap();
        assert_eq!(dst.head(SECOND).unwrap(), Some(head));
        assert_eq!(dst.query_u32("SELECT count(*) FROM t").unwrap(), 2002);
        assert_eq!(dst.query_text("PRAGMA integrity_check").unwrap(), "ok");

        // Same frame again is a no-op, a different page is a conflict.
        dst.insert_begin().unwrap();
        dst.insert_frame(head, frame(head)).unwrap();
        let mut bad = frame(head).clone();
        bad[FRAME_HEADER + 100] ^= 0xff;
        assert!(matches!(
            dst.insert_frame(head, &bad),
            Err(Error::Conflict { frame }) if frame == head
        ));
        dst.insert_end().unwrap();

        // The salt changes when the log restarts.
        let wal = dir.join("p.db-wal");
        let salt = wal_salt(&wal).unwrap().unwrap();
        assert!(src.checkpoint_truncate().unwrap().complete());
        assert_eq!(wal_salt(&wal).unwrap(), None);
        src.exec("INSERT INTO t(v) VALUES ('z')").unwrap();
        assert_eq!(src.head(SECOND).unwrap(), Some(1));
        assert_ne!(wal_salt(&wal).unwrap().unwrap(), salt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn head_waits_for_a_writer() {
        let dir = scratch("busy");
        let path = dir.join("p.db");
        let reader = RawDb::open(&path, SECOND).unwrap();
        reader.exec("CREATE TABLE t(v)").unwrap();
        let writer = RawDb::open(&path, SECOND).unwrap();
        writer.exec("BEGIN IMMEDIATE").unwrap();
        writer.exec("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(reader.try_frame_count().unwrap(), None);
        assert_eq!(reader.head(Duration::from_millis(50)).unwrap(), None);

        let commit = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            writer.exec("COMMIT").unwrap();
        });
        let n = reader.head(Duration::from_secs(2)).unwrap();
        commit.join().unwrap();
        assert!(n.unwrap() >= 2, "{n:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // The tests below pin libSQL behaviour the shipper and the receiver are
    // built around. None of it is documented, so a libsql-ffi upgrade that
    // changes any of it should fail here first.

    /// A primary with `n` single-row transactions after the schema, and all
    /// of its frames.
    fn primary_with(dir: &Path, n: usize) -> (RawDb, Vec<Vec<u8>>) {
        let src = RawDb::open(&dir.join("p.db"), SECOND).unwrap();
        src.disable_checkpoint().unwrap();
        src.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..n {
            src.exec(&format!("INSERT INTO t(v) VALUES ('row {i}')"))
                .unwrap();
        }
        let page_size = src.page_size().unwrap();
        let head = src.head(SECOND).unwrap().unwrap();
        let frames = (1..=head)
            .map(|no| src.get_frame(no, page_size).unwrap())
            .collect();
        (src, frames)
    }

    #[test]
    fn the_frame_count_is_cached_until_a_schema_read() {
        let dir = scratch("stale");
        let path = dir.join("p.db");
        let reader = RawDb::open(&path, SECOND).unwrap();
        reader.disable_checkpoint().unwrap();
        reader.exec("CREATE TABLE t(v)").unwrap();
        let before = reader.try_frame_count().unwrap().unwrap();

        let writer = RawDb::open(&path, SECOND).unwrap();
        writer.disable_checkpoint().unwrap();
        writer.exec("INSERT INTO t VALUES (1)").unwrap();

        assert_eq!(reader.try_frame_count().unwrap(), Some(before));
        // A read that touches no page leaves the cached header alone.
        reader.query_text("SELECT 1").unwrap();
        assert_eq!(reader.try_frame_count().unwrap(), Some(before));
        // `head` reads sqlite_master first, which refreshes it.
        assert!(reader.head(SECOND).unwrap().unwrap() > before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_insert_past_the_end_is_appended_without_complaint() {
        let dir = scratch("gap");
        let (_src, frames) = primary_with(&dir, 4);
        let commits: Vec<u32> = (1..)
            .zip(&frames)
            .filter(|(_, f)| size_after(f) > 0)
            .map(|(no, _)| no)
            .collect();
        // The schema, then one single-frame transaction per row.
        let first = commits[0];
        assert_eq!(&commits[1..3], &[first + 1, first + 2], "{commits:?}");

        let dst = RawDb::open(&dir.join("r.db"), SECOND).unwrap();
        dst.disable_checkpoint().unwrap();
        dst.insert_begin().unwrap();
        for (no, frame) in (1..=first).zip(&frames) {
            dst.insert_frame(no, frame).unwrap();
        }
        dst.insert_end().unwrap();
        assert_eq!(dst.head(SECOND).unwrap(), Some(first));

        // The frame after next, offered with one missing, lands in the
        // missing one's place. This is why the receiver checks `first`
        // against its own count before inserting anything.
        let skip = first + 2;
        dst.insert_begin().unwrap();
        dst.insert_frame(skip, &frames[skip as usize - 1]).unwrap();
        dst.insert_end().unwrap();
        assert_eq!(dst.head(SECOND).unwrap(), Some(first + 1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reader_sees_a_transaction_once_its_commit_frame_is_in() {
        let dir = scratch("visible");
        let (_src, frames) = primary_with(&dir, 3);
        let path = dir.join("r.db");
        let dst = RawDb::open(&path, SECOND).unwrap();
        dst.disable_checkpoint().unwrap();
        dst.insert_begin().unwrap();
        for (no, frame) in (1..).zip(&frames) {
            dst.insert_frame(no, frame).unwrap();
        }

        // The session is still open, and a reader on the replica already has
        // every row: the commit frame publishes the transaction, not
        // `insert_end`.
        let reader = RawDb::open(&path, SECOND).unwrap();
        assert_eq!(reader.query_u32("SELECT count(*) FROM t").unwrap(), 3);
        dst.insert_end().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_full_passive_fold_restarts_the_log_on_the_next_write() {
        let dir = scratch("fold");
        let (src, _) = primary_with(&dir, 5);
        let wal = dir.join("p.db-wal");
        let salt = wal_salt(&wal).unwrap().unwrap();

        // Automatic checkpoints are off on this handle, and the restart
        // happens anyway. The shipper reads the new salt as the start of the
        // next generation.
        let ck = src.checkpoint_passive().unwrap();
        assert!(!ck.busy && ck.log == ck.checkpointed, "{ck:?}");
        src.exec("INSERT INTO t(v) VALUES ('after')").unwrap();
        assert_eq!(src.head(SECOND).unwrap(), Some(1));
        assert_ne!(wal_salt(&wal).unwrap().unwrap(), salt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn frames_onto_a_vacuumed_copy_go_in_and_give_wrong_rows() {
        let dir = scratch("vacuum");
        let src = RawDb::open(&dir.join("p.db"), SECOND).unwrap();
        src.disable_checkpoint().unwrap();
        src.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        src.exec("CREATE INDEX t_v ON t(v)").unwrap();
        for i in 0..300 {
            src.exec(&format!(
                "INSERT INTO t(v) VALUES ('{i}-{}')",
                "x".repeat(i % 90)
            ))
            .unwrap();
        }
        src.exec("DELETE FROM t WHERE id % 3 = 0").unwrap();
        assert!(src.checkpoint_truncate().unwrap().complete());
        let copy = dir.join("r.db");
        src.exec(&format!("VACUUM INTO '{}'", copy.display()))
            .unwrap();
        for i in 0..50 {
            src.exec(&format!("INSERT INTO t(v) VALUES ('late {i}')"))
                .unwrap();
        }
        let page_size = src.page_size().unwrap();
        let head = src.head(SECOND).unwrap().unwrap();

        let dst = RawDb::open(&copy, SECOND).unwrap();
        dst.disable_checkpoint().unwrap();
        dst.insert_begin().unwrap();
        for no in 1..=head {
            dst.insert_frame(no, &src.get_frame(no, page_size).unwrap())
                .unwrap();
        }
        dst.insert_end().unwrap();
        assert_eq!(dst.head(SECOND).unwrap(), Some(head));
        // Every frame went in, the file passes its own check, and it holds
        // the wrong rows: the frames were page images of a different layout.
        assert_eq!(dst.query_text("PRAGMA integrity_check").unwrap(), "ok");
        assert_ne!(
            dst.query_u32("SELECT count(*) FROM t").unwrap(),
            src.query_u32("SELECT count(*) FROM t").unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
