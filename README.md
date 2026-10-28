# walship

[![crates.io](https://img.shields.io/crates/v/walship.svg)](https://crates.io/crates/walship)
[![docs.rs](https://img.shields.io/docsrs/walship)](https://docs.rs/walship)
[![CI](https://github.com/tissue-systems/walship/actions/workflows/ci.yml/badge.svg)](https://github.com/tissue-systems/walship/actions/workflows/ci.yml)
![MSRV 1.88](https://img.shields.io/badge/rustc-1.88+-blue.svg)

Physical replication for libSQL databases. walship reads committed frames out
of a database's WAL and posts them over HTTP to a peer, which writes them into
its own copy through libSQL's WAL insert API. The copy on the peer is a normal
SQLite database, and one call turns it back into a live one.

```rust
let shipper = Shipper::builder("/var/lib/app/db", "http://standby.internal:7350")
    .token(token)
    .start(tokio::runtime::Handle::current())?;
shipper.track("orders");

// after a commit
shipper.wake("orders");
```

- frame-level replication, with no FUSE layer and no proxy in front of the
  database
- while the peer is reachable, a replica trails the primary by about one batch
- a transaction split across batches is applied whole or not at all
- both sides keep their position across restarts and crashes
- any number of nodes, each owning some of the databases
- promotion folds the WAL and moves the file into place

walship has replicated customer databases in production since September 2026
(see [About](#about)), but the API isn't stable yet. Expect breaking changes
before 1.0.

## Installation

```toml
[dependencies]
walship = "0.1"
```

libSQL is a hard requirement. [libSQL](https://github.com/tursodatabase/libsql)
is a fork of SQLite's C code that takes outside contributions, which upstream
SQLite doesn't. Upstream has no API for reading frames out of the WAL or
writing them back in. walship uses the ones libSQL adds
(`libsql_wal_get_frame`, `libsql_wal_insert_frame` and friends) through the
`libsql-ffi` crate, which compiles libSQL into your binary.

Your app has to open its databases through the same build, either with the
`libsql` crate or with `libsql-ffi` directly, on the same 0.9 line so Cargo
links one copy. A process that links two copies of SQLite gets two sets of
POSIX locks that don't see each other, and that corrupts databases.

Cargo won't stop that on its own. `libsqlite3-sys`, the crate under
rusqlite and sqlx, declares `links = "sqlite3"`, so Cargo refuses two
versions of it. `libsql-ffi` declares no `links` key, so a second version of
it, or `libsqlite3-sys` next to it, builds without complaint. This check
fails when your `Cargo.lock` holds more than one, on any platform. Run it in
CI:

```sh
libs=$(awk '
  /^\[\[package\]\]/ { name = "" }
  /^name = / { name = $3 }
  /^version = / && name ~ /^"(libsql-ffi|libsqlite3-sys)"$/ { print name, $3 }
' Cargo.lock)
if [ "$(printf '%s\n' "$libs" | grep -c .)" -gt 1 ]; then
  echo "more than one SQLite C library in Cargo.lock:"
  echo "$libs"
  exit 1
fi
```

`cargo tree -i libsql-ffi` then shows what pulled each one in.

Cargo features:

- `shipper` (default): the sending side, with reqwest and tokio.
- `server` (default): axum routes for the receiving side.

The `Receiver` and the WAL primitives in `walship::wal` build without either.

## Usage

### Primary

Each database is a file named `<name>.db` in one data directory. Names are
ASCII letters, digits, `_`, `-` and `.`, at most 128 bytes, and can't start
with a dot (`walship::valid_name`).

```rust
use walship::Shipper;

let shipper = Shipper::builder("/var/lib/app/db", "http://standby.internal:7350")
    .token(std::env::var("WALSHIP_TOKEN")?)
    .start(tokio::runtime::Handle::current())?;

shipper.track("orders");   // start shipping a database
shipper.wake("orders");    // optional: ship right away after a commit
shipper.reset("orders");   // the file was replaced (restore, import): send a new base
shipper.forget("orders");  // stop shipping and delete the replica on the peer
```

Without `wake`, every tracked database is looked at every 5 seconds.
`shipper.status()` returns a serializable snapshot of where each database is,
which is easy to put behind an admin endpoint.

Your own connections must not checkpoint. A checkpoint folds frames into the
main file, and if they haven't been shipped yet the shipper has to start over
with a full copy. How you turn checkpoints off depends on how you open
connections.

With `libsql-ffi`, or anything else that gives you the raw `sqlite3` pointer,
one call turns off both the checkpoints after a commit and the one SQLite runs
when the last connection closes:

```rust
// `handle` is the raw sqlite3 pointer of a connection you opened.
unsafe { walship::disable_checkpoint(handle) }?;
```

The `libsql` crate keeps that pointer private. Run this on every connection
you open instead:

```rust
conn.query("PRAGMA wal_autocheckpoint=0", ()).await?;
```

That leaves the checkpoint on close, which only runs when the last connection
to a database closes. The shipper holds a connection of its own, with
checkpoints off, for every database it tracks, so yours are never the last.
Drop your connections before you call `shipper.shutdown()`.
[`examples/with_libsql.rs`](examples/with_libsql.rs) does all of this.

#### Patching the `libsql` crate

You don't need this to use walship. It's for when you'd rather turn
checkpoints off with a method call on a `libsql::Connection` than rely on the
pragma and the shipper's held connection.

The crate already has a `wal_disable_checkpoint` method, but it's private.
[`patches/libsql-0.9.30.patch`](patches/libsql-0.9.30.patch) makes it public
and fixes a double close in `disconnect` ([libsql#2251]): dropping a local
connection closes its handle twice, and the second close can hit another
thread's connection once the memory has been reused. The patch only touches
the Rust wrapper. libSQL itself and `libsql-ffi` are unchanged.

Both halves are on their way upstream. The double-close fix was merged as
[libsql#2261] and shipped in `libsql` 0.10.0, which walship doesn't support
yet since it's built against `libsql-ffi` 0.9. Making the method public is
[libsql#2306], which is still open. Once a release includes it, you won't
need this patch.

To use it, copy the published crate into your project, apply the patch, and
tell Cargo to build from the copy:

```sh
# With libsql = "=0.9.30" already in your [dependencies] (below), download
# it into Cargo's registry cache, then copy it out.
cargo fetch
mkdir -p vendor
cp -R ~/.cargo/registry/src/index.crates.io-*/libsql-0.9.30 vendor/

cd vendor/libsql-0.9.30
patch -p1 < /path/to/walship/patches/libsql-0.9.30.patch
```

```toml
# Your app's Cargo.toml (or the workspace root's).
[dependencies]
libsql = { version = "=0.9.30", default-features = false, features = ["core"] }

[patch.crates-io]
libsql = { path = "vendor/libsql-0.9.30" }
```

```rust
let conn = db.connect()?;
conn.wal_disable_checkpoint()?;
```

Every use of `libsql` in your dependency tree then builds from `vendor/`,
including the ones your other dependencies pull in. A few things to know
before you commit to it:

- Cargo only reads `[patch]` from the top-level project. A library can't
  patch a crate for the apps that use it, so every workspace that builds
  your code needs its own `[patch.crates-io]` section and its own copy.
- The `=0.9.30` pin matters. If Cargo resolves a different 0.9 release from
  crates.io, it ignores the patch and prints a warning that it was unused.
- You own the copy. Upgrading `libsql` means fetching the new release and
  applying the patch again, by hand. The patch is small enough to redo if it
  stops applying.
- Cargo shows lint warnings for path dependencies but hides them for crates
  from a registry, so expect warnings from the vendored crate in your
  build. Adding `[lints.rust] warnings = "allow"` to its `Cargo.toml` puts
  them back to how they were.

[libsql#2251]: https://github.com/tursodatabase/libsql/issues/2251
[libsql#2261]: https://github.com/tursodatabase/libsql/pull/2261
[libsql#2306]: https://github.com/tursodatabase/libsql/pull/2306

### Replica

```rust
use std::sync::Arc;
use walship::{server::Routes, Receiver};

let receiver = Arc::new(Receiver::new("/var/lib/app/replicas")?);
let app = Routes::new(Arc::clone(&receiver))
    .token(std::env::var("WALSHIP_TOKEN")?)
    .into_router();
axum::serve(tokio::net::TcpListener::bind("0.0.0.0:7350").await?, app).await?;
```

The router only handles paths under `/replica`, so it can be merged into an
app you already run. One receiver accepts streams from any number of
shippers. Database names have to be unique across all of them.

### Promotion

```rust
use std::path::Path;
use walship::receiver::Take;

match receiver.take("orders", Path::new("/var/lib/app/db/orders.db"))? {
    Take::Moved { generation, frames, bytes } => { /* open it */ }
    Take::Busy => { /* a reader blocked the checkpoint, try again */ }
    Take::Absent => { /* no replica by that name */ }
}
```

`take` runs a `TRUNCATE` checkpoint on the replica and moves the file to the
destination. If a transaction was only partly received, it's dropped, so the
file holds exactly what the primary had committed. walship doesn't detect
failures or decide when to promote. That's up to you.

## Topologies

Each node runs a `Shipper` for the databases it owns and a `Receiver` for the
ones it holds replicas of. The `owner` callback on the builder says which is
which, and walship asks it again every time it looks at a database. A node
that loses ownership of a database stops shipping it and deletes the replica
it had left on its peer. Otherwise, after a failover, the new owner would
still be holding an old replica of a database it's serving.

```rust
use walship::{Owner, Shipper};

let shipper = Shipper::builder(dir, peer)
    .owner(|name| match placement.lookup(name) {
        Some(node) if node != me => Owner::Elsewhere(node),
        _ => Owner::Here,
    })
    .start(handle)?;
```

Any layout where every owner has one peer works. In a ring, each node
replicates to the next one:

```text
a ──▶ b ──▶ c ──▶ d ──▶ e
▲                       │
└───────────────────────┘
```

You can also send everything to one standby, or pair nodes up. Every database
gets one replica, on the peer of the node that owns it.

## How it works

Each database is replicated in generations. A generation starts with a
*base*: the primary runs a `TRUNCATE` checkpoint, then streams its main file
to the peer. WAL frames written after that are numbered from 1.

The shipper reads committed frames past the last acknowledged one and posts
them in batches of up to 512 (2 MiB with 4 KiB pages). The replica answers
with its committed frame count. After any error, the next send resumes from
that count.

Checkpointing is off on every connection to a shipped database. Once a
database has gone idle with its WAL past 1,000 frames, all of them
acknowledged, the shipper runs a `PASSIVE` checkpoint. It counts the fold only
if it took exactly the acknowledged frames; if a writer got in first, the new
frames are shipped and the fold waits for the next idle moment. After a full
fold SQLite restarts the WAL on the next write, and the shipper treats that as
the start of the next generation. The replica checkpoints its own copy
when the first frames of the new generation arrive.

Something else can still change the WAL behind the shipper's back: a
checkpoint from another process, or a crash in the middle of a WAL header
write. That shows up as a different salt in the WAL header, or as a frame
count lower than the acknowledged one. Either way the shipper sends a fresh
base.

A batch can end partway through a transaction. The replica keeps its insert
session open until a commit frame arrives, so readers on the replica never see
a partial transaction. If the replica restarts with a session still open, the
uncommitted frames are thrown away and the shipper resends them from the last
commit.

Every 60 seconds (the first time 30 seconds after start) the shipper compares
its databases with `GET /replica` on the peer, and sends a base for any the
peer doesn't have.

If the peer is unreachable, the shipper retries with backoff from 1 to 30
seconds while the WAL keeps growing. Once 50,000 frames are waiting (about
200 MiB with 4 KiB pages) it runs a `TRUNCATE` checkpoint anyway and sends a
full base when the peer is back.

## Protocol

When the receiver has a token, every request carries
`Authorization: Bearer <token>`. Request bodies are raw bytes, and replies are
JSON. The types are in `walship::proto`.

```text
POST   /replica/{name}/base?generation=G
POST   /replica/{name}/frames?generation=G&first=F&page_size=P
GET    /replica
GET    /replica/{name}
DELETE /replica/{name}
```

A `frames` body is a run of whole WAL frames, each a 24-byte header followed by
one page. If `generation` or `first` doesn't line up with what the replica
has, it answers `409` with either its own position, which the shipper resends
from, or a request for a new base. A `503` means a reader on the replica held off
the checkpoint that starts a new generation; the shipper tries again. A wrong
or missing token gets `401`, and an I/O or SQLite error on the replica gets
`500`. Every error reply has the same JSON shape, `walship::proto::Refusal`.

## libSQL notes

The WAL functions behave in ways that aren't documented. We measured the
following against libsql-ffi 0.9. Tests in `src/wal.rs` pin down most of it,
so an upgrade that changes it fails the suite:

- `libsql_wal_frame_count` takes the WAL write lock without waiting, so it
  returns `SQLITE_BUSY` whenever a write transaction is open. It also returns
  the connection's cached wal-index header. `SELECT 1` doesn't refresh it, a
  read of `sqlite_master` does, and that's what `RawDb::head` runs first.
- If you insert a frame numbered past `frame_count + 1`, it's quietly appended
  at `frame_count + 1`. If you insert one at or below `frame_count`, nothing
  happens when the page matches and you get a conflict when it doesn't. You
  have to check for gaps yourself. The receiver does.
- Frames are page images, so they only make sense on top of the exact file
  they were written against. libSQL accepts them onto anything else, such as
  a copy made with `VACUUM INTO`. In our test that replica still passed
  `PRAGMA integrity_check` and was missing a fifth of its rows. That's why a
  base is a byte copy of the main file taken right after a `TRUNCATE`
  checkpoint.
- When an insert session ends without a commit frame, its frames stay on disk
  but never make it into the wal-index. The next session starts again from the
  last commit.
- A reader on the replica sees a transaction as soon as its commit frame is
  inserted, before `libsql_wal_insert_end`.
- Inserted frames go through the normal WAL append path. The replica's WAL has
  its own salts and checksums and recovers from a crash like any other WAL.
- After a `PASSIVE` checkpoint that folds the whole log, the next write
  restarts the WAL at frame 1 with a new salt, even on a connection with
  automatic checkpoints turned off. The shipper's generations depend on this.

[Turso](https://github.com/tursodatabase/turso), the Rust rewrite of SQLite,
has matching WAL calls behind its non-default `conn_raw_api` feature. walship
doesn't support it yet. With automatic checkpoints off, Turso also skips the
log restart after a full fold, so the shipper would never see a new
generation.

## Limitations

- Each database has one replica.
- Only the latest state is kept, so there's no point-in-time restore. Use
  [Litestream](https://litestream.io) if you need that, next to walship or
  instead of it.
- Read-only connections to a replica work and only see committed
  transactions. walship doesn't route any reads there for you.
- A base is a full copy of the database. Uploading one times out after an
  hour, and the receiver refuses bases over 8 GiB unless you raise
  `Routes::max_base_bytes`.
- Data goes over the wire uncompressed. Use TLS, a private network or a
  tunnel.
- The page size can't change. A replica refuses frames whose page size
  doesn't match its base.

## Why walship exists

We needed a standby copy of many small databases that a single
process creates, restores and drops while it runs, and we wanted each copy on
another machine, ready to open in seconds. Nothing we looked at did that.

[Litestream](https://litestream.io) came closest. It streams the WAL to
object storage, and getting a database back means restoring it from there.
It's set up per database, and it runs as a separate process watching files
that another process owns, so it loses its place when that process swaps a
file underneath it. For us the database server was the one process that knew
when a file was created, replaced or dropped, so that's where replication
went.

libSQL has replication of its own. Embedded replicas pull frames from a
`sqld` primary, and the sync in Turso's Rust rewrite pushes and pulls against
Turso Cloud. Both put a server in the middle. walship uses libSQL's WAL calls
and leaves the transport to two of your own processes.

If you want backups or point-in-time restore, use Litestream. It's mature,
it works with stock SQLite from any language, and walship does neither of
those things. walship is for a Rust app on libSQL that wants a hot standby
and nothing between primary and standby except HTTP.

## Related projects

- [Litestream](https://litestream.io) streams the WAL to object storage and
  restores from it.
- [LiteFS](https://github.com/superfly/litefs) uses a FUSE filesystem and a
  primary lease, and replicates to any number of nodes.
- libSQL embedded replicas sync from a `sqld` primary.
- [rqlite](https://rqlite.io) and [dqlite](https://dqlite.io) replicate SQL
  statements through Raft, so writes keep going when a node dies, at the cost
  of running a cluster.

## Testing

```sh
cargo test
cargo run --release --example stress -- /tmp/walstress 30 50
cargo run --example two_nodes
cargo run --example with_libsql
```

The stress example keeps a writer, a shipper and a reader on the replica
running together over a simulated 50 ms link for the given number of seconds.
Then it cuts a batch mid-transaction and restarts the receiver, resends frames
the replica already has, leaves a gap, blocks a checkpoint with a reader and
starts a new generation. Each step prints what happened, and it ends by
comparing a checksum of every row on both sides.

## About

walship is written and maintained by
[Tissue Systems](https://tissue.systems/?ref=walship). We built it for Tissue
Cloud, a serverless platform for JavaScript and WebAssembly functions, where
it keeps a standby copy of every customer database. We also make
[Tissue Mast](https://tissue.systems/mast?ref=walship), an on-call paging and
monitoring service. Bug reports and pull requests are welcome whether or not
you use either.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
