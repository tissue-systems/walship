//! End-to-end: a shipper and a receiver on localhost, one temp directory each.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::runtime::Runtime;
use walship::{server::Routes, Owner, RawDb, Receiver, Shipper};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

struct Pair {
    rt: Runtime,
    primary: PathBuf,
    receiver: Arc<Receiver>,
    url: String,
    root: PathBuf,
}

impl Pair {
    fn new(test: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let root = std::env::temp_dir().join(format!("walship-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let primary = root.join("primary");
        std::fs::create_dir_all(&primary).unwrap();
        let receiver = Arc::new(Receiver::new(root.join("replicas")).unwrap());
        let app = Routes::new(Arc::clone(&receiver)).token("t").into_router();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        rt.spawn(async move { axum::serve(listener, app).await });
        Self {
            rt,
            primary,
            receiver,
            url,
            root,
        }
    }

    fn shipper(&self) -> walship::shipper::Builder {
        Shipper::builder(&self.primary, &self.url).token("t")
    }

    fn create(&self, name: &str) -> RawDb {
        let db = RawDb::open(&self.primary.join(format!("{name}.db")), BUSY_TIMEOUT).unwrap();
        db.disable_checkpoint().unwrap();
        db.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db
    }

    fn replica_rows(&self, name: &str) -> Option<u32> {
        let path = self.receiver.db_path(name);
        if !path.exists() {
            return None;
        }
        RawDb::open(&path, BUSY_TIMEOUT)
            .and_then(|db| db.query_u32("SELECT count(*) FROM t"))
            .ok()
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn wait_for(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// One transaction per row, so every row is at least one WAL frame.
fn insert_each(db: &RawDb, n: usize) {
    for i in 0..n {
        db.exec(&format!("INSERT INTO t(v) VALUES (printf('%0200d', {i}))"))
            .unwrap();
    }
}

fn insert(db: &RawDb, n: usize) {
    db.exec("BEGIN").unwrap();
    for i in 0..n {
        db.exec(&format!("INSERT INTO t(v) VALUES ('row {i}')"))
            .unwrap();
    }
    db.exec("COMMIT").unwrap();
}

#[test]
fn keeps_the_peer_current() {
    let pair = Pair::new("current");
    let db = pair.create("src");
    let shipper = pair.shipper().start(pair.rt.handle().clone()).unwrap();
    shipper.track("src");

    insert(&db, 2);
    shipper.wake("src");
    wait_for("two rows", 20, || pair.replica_rows("src") == Some(2));

    // Three batches, and a WAL long enough to be folded once it's idle.
    insert_each(&db, 1500);
    shipper.wake("src");
    wait_for("1502 rows", 30, || pair.replica_rows("src") == Some(1502));
    // A replica reader sees the rows as soon as the commit frame is in, which
    // is before the shipper has the response that moves its acked count.
    wait_for("the ack", 10, || shipper.status().databases[0].pending == 0);
    let st = shipper.status();
    let row = &st.databases[0];
    assert_eq!(
        (row.name.as_str(), row.pending, row.needs_base),
        ("src", 0, false),
        "{st:?}"
    );

    // Idle with a long WAL: the shipper folds it into the main file. Whether
    // SQLite restarts the log on the next write depends on whether a reader
    // is mid-snapshot at that moment. A restart is the next generation and
    // needs no base; no restart means the frames carry on in this one.
    let generation = row.generation;
    let main_file = pair.primary.join("src.db");
    shipper.wake("src");
    wait_for("the fold", 20, || {
        std::fs::metadata(&main_file).is_ok_and(|m| m.len() > 16 * 4096)
    });
    insert(&db, 1);
    shipper.wake("src");
    wait_for("1503 rows", 20, || pair.replica_rows("src") == Some(1503));
    let replica = pair.receiver.status("src").unwrap();
    assert!(
        (generation..=generation + 1).contains(&replica.generation),
        "generation {} after {generation}",
        replica.generation
    );

    // A replaced file gets a base.
    shipper.reset("src");
    insert(&db, 1);
    shipper.wake("src");
    wait_for("1504 rows", 20, || pair.replica_rows("src") == Some(1504));

    // Forgetting a database deletes its replica.
    shipper.forget("src");
    wait_for("the replica deleted", 20, || {
        pair.receiver.status("src").is_none()
    });
    shipper.shutdown();
}

#[test]
fn reconcile_restores_a_lost_replica() {
    let pair = Pair::new("reconcile");
    let db = pair.create("src");
    insert(&db, 5);
    let shipper = pair.shipper().start(pair.rt.handle().clone()).unwrap();
    shipper.track("src");
    wait_for("five rows", 20, || pair.replica_rows("src") == Some(5));
    // The rows land before the shipper has the reply. A listing taken before
    // the reply is read as possibly racing the base, and left for later.
    wait_for("the base acknowledged", 20, || {
        let st = shipper.status();
        !st.databases[0].stepping && !st.databases[0].needs_base
    });

    // The peer loses the replica and nothing writes to the database.
    assert!(pair.receiver.delete("src").unwrap());
    shipper.reconcile();
    wait_for("the replica back", 20, || {
        pair.replica_rows("src") == Some(5)
    });
    let peer = shipper.status().peer.expect("a reconcile ran");
    assert_eq!(peer.missing, 1, "the listing that drove the rebase");
    shipper.shutdown();
}

#[test]
fn a_database_owned_elsewhere_is_not_shipped() {
    let pair = Pair::new("owner");
    let owners: Arc<Mutex<HashMap<String, String>>> = Arc::default();
    let map = Arc::clone(&owners);
    let shipper = pair
        .shipper()
        .owner(move |name| match map.lock().unwrap().get(name) {
            Some(node) => Owner::Elsewhere(node.clone()),
            None => Owner::Here,
        })
        .filter(|name| !name.starts_with('_'))
        .start(pair.rt.handle().clone())
        .unwrap();

    let db = pair.create("src");
    let local = pair.create("_local");
    insert(&db, 3);
    insert(&local, 3);
    shipper.track("src");
    shipper.track("_local");
    wait_for("three rows", 20, || pair.replica_rows("src") == Some(3));

    // Ownership moves: the replica on the peer goes, and writes here stay here.
    owners.lock().unwrap().insert("src".into(), "node-b".into());
    shipper.wake("src");
    wait_for("the replica deleted", 20, || {
        pair.receiver.status("src").is_none()
    });
    insert(&db, 1);
    shipper.wake("src");
    std::thread::sleep(Duration::from_millis(500));
    assert!(pair.receiver.status("src").is_none());
    let st = shipper.status();
    assert_eq!(st.databases.len(), 1, "the filter keeps _local out: {st:?}");
    assert_eq!(st.databases[0].owner.as_deref(), Some("node-b"));

    // It comes back: a full base, with the write made while it was away.
    owners.lock().unwrap().clear();
    shipper.wake("src");
    wait_for("four rows", 20, || pair.replica_rows("src") == Some(4));
    assert!(pair.receiver.status("_local").is_none());
    shipper.shutdown();
}

#[test]
fn a_wrong_token_is_refused() {
    let pair = Pair::new("token");
    let db = pair.create("src");
    insert(&db, 1);
    let shipper = Shipper::builder(&pair.primary, &pair.url)
        .token("wrong")
        .start(pair.rt.handle().clone())
        .unwrap();
    shipper.track("src");
    wait_for("a failure", 20, || {
        shipper.status().databases[0]
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("401"))
    });
    assert!(pair.receiver.status("src").is_none());
    shipper.shutdown();
}
