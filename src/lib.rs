//! Ship libSQL WAL frames to a warm replica on another machine.
//!
//! walship keeps a copy of each SQLite database on a second machine, a batch
//! of frames behind the original. It reads committed frames out of the
//! primary's WAL and writes them into the replica's WAL with the frame APIs
//! libSQL adds to SQLite. No SQL is replayed and there's no log format of its
//! own: the replica's WAL ends up holding the same pages as the primary's.
//!
//! There are two halves:
//!
//! - A [`Shipper`] runs next to your databases. You tell it which ones exist
//!   and when they're written, and its threads post frames to the peer.
//! - A [`Receiver`] keeps the replicas in a directory. [`server::Routes`]
//!   serves one over HTTP with axum. To turn a replica back into a database
//!   you can open and write, call [`Receiver::take`].
//!
//! Each node ships to one peer, and a receiver accepts from any number of
//! shippers, so nodes can pair up, form a ring, or all ship to one standby.
//!
//! # Requirements
//!
//! Your databases must be opened through libSQL's build of SQLite (the
//! `libsql-ffi` crate this crate links), in WAL mode, with automatic
//! checkpoints turned off on every connection. SQLite would otherwise fold
//! frames into the main file before they're shipped. [`disable_checkpoint`]
//! does that for a raw `sqlite3` handle. The `libsql` crate doesn't expose
//! one; `examples/with_libsql.rs` shows what to do instead. Linking a second
//! copy of SQLite into the same process breaks SQLite's file locks; don't.
//!
//! # Example
//!
//! ```no_run
//! # fn run(rt: tokio::runtime::Handle) -> walship::Result<()> {
//! let shipper = walship::Shipper::builder("/var/lib/myapp/db", "http://standby.internal:7350")
//!     .token(std::env::var("WALSHIP_TOKEN").expect("WALSHIP_TOKEN is set"))
//!     .start(rt)?;
//! shipper.track("orders");
//! // ...after each write to orders.db:
//! shipper.wake("orders");
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `shipper` (default): [`Shipper`], with reqwest and tokio.
//! - `server` (default): [`server::Routes`], with axum.
//!
//! The [`Receiver`] and the [`wal`] primitives are always built.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(missing_debug_implementations, rust_2018_idioms)]

mod error;
mod fs;
pub mod proto;
pub mod receiver;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "shipper")]
pub mod shipper;
pub mod wal;

pub use error::{Error, Result};
pub use fs::valid_name;
pub use receiver::Receiver;
#[cfg(feature = "shipper")]
pub use shipper::{Owner, Shipper};
pub use wal::{disable_checkpoint, RawDb};
