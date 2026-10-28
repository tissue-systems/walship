//! Small file helpers shared by both sides.

use std::{
    ffi::OsString,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// A suffix for temporary files that two writers in one process, or two
/// processes sharing a directory, won't both pick.
pub(crate) fn temp_suffix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}-{n:x}-{:x}", std::process::id())
}

/// Write `bytes` to `path` through a temporary file and a rename, so a
/// reader sees the old contents or the new ones and never half of each.
///
/// The file is synced before the rename and the directory after it, so the
/// new contents survive a power cut once this returns.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}.tmp", temp_suffix()));
    let tmp = PathBuf::from(tmp);
    let written = File::create(&tmp)
        .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&tmp, path));
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    sync_parent(path)
}

/// Makes a rename or a new file in `path`'s directory durable. Windows
/// can't open a directory as a file and doesn't need this.
pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// `foo.db` plus `-wal` is `foo.db-wal`. `Path::with_extension` can't do it.
pub(crate) fn sidecar(base: &Path, suffix: &str) -> PathBuf {
    let mut s = OsString::from(base.as_os_str());
    s.push(suffix);
    PathBuf::from(s)
}

/// Database names become file names, so they're kept to a safe alphabet:
/// ASCII letters, digits, `_`, `-` and `.`, not starting with a dot, at most
/// 128 bytes.
#[must_use]
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_kept_to_a_safe_alphabet() {
        assert!(valid_name("orders"));
        assert!(valid_name("a0123abc_orders.v2"));
        assert!(!valid_name(""));
        assert!(!valid_name("../x"));
        assert!(!valid_name(".hidden"));
        assert!(!valid_name("a/b"));
        assert!(!valid_name(&"x".repeat(129)));
    }

    #[test]
    fn sidecar_appends_to_the_file_name() {
        assert_eq!(
            sidecar(Path::new("/d/x.db"), "-wal"),
            Path::new("/d/x.db-wal")
        );
    }

    #[test]
    fn write_atomic_replaces_the_file_and_cleans_up() {
        let dir = std::env::temp_dir().join(format!("walship-fs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");

        write_atomic(&path, b"one").unwrap();
        write_atomic(&path, b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);

        // A failed write leaves no temporary file behind.
        let missing = dir.join("no-such-dir").join("state.json");
        assert!(write_atomic(&missing, b"three").is_err());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
