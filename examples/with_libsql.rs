//! Shipping a database that the app reads and writes through the `libsql`
//! crate, without patching it.
//!
//! ```text
//! cargo run --example with_libsql
//! ```
//!
//! The `libsql` crate never hands out the raw `sqlite3` pointer, so
//! [`walship::disable_checkpoint`] can't be called on its connections. Two
//! things cover for it:
//!
//! - `PRAGMA wal_autocheckpoint=0` on every connection the app opens stops
//!   the checkpoints SQLite runs after a commit.
//! - SQLite also checkpoints when the last connection to a database closes.
//!   The shipper keeps a connection of its own open, with checkpoints off,
//!   for every database it tracks, so the app's connections are never the
//!   last. Close them before shutting the shipper down.
//!
//! If a checkpoint gets through anyway, the shipper sees the WAL change under
//! it and sends a fresh base. That costs a full copy, and the replica stays
//! correct.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use libsql::{Builder, Connection, OpenFlags};
use walship::{server::Routes, Receiver, Shipper};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

/// Runs a statement that returns rows, like most PRAGMAs, and throws them
/// away. `execute` refuses a statement that returns rows.
async fn pragma(conn: &Connection, sql: &str) -> Res<()> {
    let mut rows = conn.query(sql, ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

/// What every connection to a shipped database needs, every time it's
/// opened. `journal_mode` sticks to the file, the other two don't.
async fn connect(db: &libsql::Database) -> Res<Connection> {
    let conn = db.connect()?;
    pragma(&conn, "PRAGMA journal_mode=WAL").await?;
    pragma(&conn, "PRAGMA wal_autocheckpoint=0").await?;
    pragma(&conn, "PRAGMA busy_timeout=5000").await?;
    Ok(conn)
}

async fn count(conn: &Connection) -> Res<i64> {
    let mut rows = conn.query("SELECT count(*) FROM orders", ()).await?;
    let row = rows.next().await?.ok_or("no row")?;
    Ok(row.get(0)?)
}

/// Opened read-only, so closing it can't checkpoint the replica either. Fails
/// until the first base has arrived.
async fn replica_rows(path: &std::path::Path) -> Res<i64> {
    let db = Builder::new_local(path)
        .flags(OpenFlags::SQLITE_OPEN_READ_ONLY)
        .build()
        .await?;
    count(&db.connect()?).await
}

#[tokio::main]
async fn main() -> Res<()> {
    let root = std::env::temp_dir().join(format!("walship-libsql-{}", std::process::id()));
    let primary_dir = root.join("primary");
    std::fs::create_dir_all(&primary_dir)?;

    let receiver = Arc::new(Receiver::new(root.join("replicas"))?);
    let app = Routes::new(Arc::clone(&receiver))
        .token("s3cret")
        .into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let db = Builder::new_local(primary_dir.join("orders.db"))
        .build()
        .await?;
    let conn = connect(&db).await?;
    conn.execute(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)",
        (),
    )
    .await?;

    let shipper = Shipper::builder(&primary_dir, format!("http://{addr}"))
        .token("s3cret")
        .start(tokio::runtime::Handle::current())?;
    shipper.track("orders");

    // A connection per batch, the way a server would open one per request.
    // Each close leaves the shipper's connection open, so none checkpoints.
    for batch in 0..10 {
        let conn = connect(&db).await?;
        let tx = conn.transaction().await?;
        for i in 0..50 {
            tx.execute(
                "INSERT INTO orders(item, qty) VALUES (?1, ?2)",
                (format!("widget {batch}-{i}"), i % 7),
            )
            .await?;
        }
        tx.commit().await?;
        shipper.wake("orders");
    }

    let start = Instant::now();
    loop {
        let rows = replica_rows(&receiver.db_path("orders")).await.ok();
        if rows == Some(500) {
            break;
        }
        if start.elapsed() > Duration::from_secs(20) {
            return Err(format!("replica stuck at {rows:?} rows").into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Generation 1 means one base and frames ever since. A checkpoint that
    // got past the shipper would have forced a second base.
    let generation = receiver.status("orders").map_or(0, |r| r.generation);
    println!(
        "replica has 500 orders after {:?}, generation {generation}",
        start.elapsed()
    );

    // The app's connections go first, then the shipper. The other way round,
    // the app's last close would be the one that checkpoints.
    drop(conn);
    drop(db);
    shipper.shutdown();
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
