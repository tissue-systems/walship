//! A primary and a standby in one process, talking over HTTP on localhost.
//!
//! ```text
//! cargo run --example two_nodes
//! ```
//!
//! Writes a few hundred rows on the primary, waits for the standby's replica
//! to catch up, then promotes the replica into a database of its own.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use walship::{receiver::Take, server::Routes, RawDb, Receiver, Shipper};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

fn main() -> Res<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "walship=info".into()),
        )
        .init();

    let rt = tokio::runtime::Runtime::new()?;
    let root = std::env::temp_dir().join(format!("walship-two-nodes-{}", std::process::id()));
    let primary_dir = root.join("primary");
    let standby_dir = root.join("standby");
    std::fs::create_dir_all(&primary_dir)?;

    // The standby: a receiver behind the axum routes.
    let receiver = Arc::new(Receiver::new(standby_dir.join("replicas"))?);
    let app = Routes::new(Arc::clone(&receiver))
        .token("s3cret")
        .into_router();
    let listener = rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let addr = listener.local_addr()?;
    rt.spawn(async move { axum::serve(listener, app).await });
    println!("standby listening on {addr}");

    // The primary: a database with checkpoints off, and a shipper for it.
    let db = RawDb::open(&primary_dir.join("orders.db"), Duration::from_secs(5))?;
    db.disable_checkpoint()?;
    db.exec("CREATE TABLE orders(id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)")?;

    let shipper = Shipper::builder(&primary_dir, format!("http://{addr}"))
        .token("s3cret")
        .start(rt.handle().clone())?;
    shipper.track("orders");

    for i in 0..500 {
        db.exec(&format!(
            "INSERT INTO orders(item, qty) VALUES ('widget {i}', {})",
            i % 7
        ))?;
        if i % 50 == 0 {
            shipper.wake("orders");
        }
    }
    shipper.wake("orders");

    // Count rows rather than compare frame numbers: when the base is taken
    // after the inserts, they're all in it and the replica has no frames.
    let start = Instant::now();
    let replica_path = receiver.db_path("orders");
    let replica_rows = || -> Option<String> {
        let db = RawDb::open(&replica_path, Duration::from_secs(5)).ok()?;
        db.query_text("SELECT count(*) FROM orders").ok()
    };
    while replica_rows().as_deref() != Some("500") {
        if start.elapsed() > Duration::from_secs(20) {
            let replica = receiver.status("orders");
            return Err(format!("replica stuck at {replica:?}").into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let frames = receiver.status("orders").map_or(0, |r| r.frames);
    println!(
        "replica caught up ({frames} frames past its base) in {:?}",
        start.elapsed()
    );
    println!("{}", serde_json::to_string_pretty(&shipper.status())?);

    // The primary goes away. Promote the replica on the standby.
    shipper.shutdown();
    drop(db);
    let promoted = standby_dir.join("orders.db");
    match receiver.take("orders", &promoted)? {
        Take::Moved { frames, .. } => println!("promoted with {frames} frames"),
        other => return Err(format!("couldn't promote: {other:?}").into()),
    }
    let db = RawDb::open(&promoted, Duration::from_secs(5))?;
    println!(
        "standby has {} orders",
        db.query_text("SELECT count(*) FROM orders")?
    );
    drop(db);
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
