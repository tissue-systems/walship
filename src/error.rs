use std::io;

/// Everything that can go wrong in walship.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A call into SQLite failed. `code` is the SQLite result code.
    #[error("{context}: {message} (code {code})")]
    Sqlite {
        context: String,
        code: i32,
        message: String,
    },

    /// The replica already holds a different page at this frame number. The
    /// stream can't continue from here; it needs a new base.
    #[error("WAL frame {frame} conflicts with the frame already in the replica")]
    Conflict { frame: u32 },

    /// A file offered as a base isn't a SQLite database.
    #[error("not a SQLite database: {0}")]
    NotADatabase(String),

    /// A database name that walship won't turn into a file name.
    #[error("invalid database name {0:?}")]
    InvalidName(String),

    /// A path or SQL string with a NUL byte in it.
    #[error("{0} contains a NUL byte")]
    Nul(&'static str),

    /// A peer URL without `http://` or `https://` in front.
    #[error("peer URL must start with http:// or https://, got {0:?}")]
    InvalidPeerUrl(String),

    /// A token that can't go in an HTTP header.
    #[error("the token has characters that can't go in an HTTP header")]
    InvalidToken,

    /// The peer answered, but not with what the protocol expects.
    #[error("peer: {0}")]
    Peer(String),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "shipper")]
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
