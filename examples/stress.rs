//! Runs a primary and a replica side by side under load and checks that the
//! replica ends up with the same rows.
//!
//! No HTTP here: frames go from the primary's WAL straight into a
//! [`Receiver`], with a sleep standing in for the round trip. That keeps the
//! test about the WAL behaviour the crate depends on.
//!
//! ```text
//! cargo run --release --example stress -- [dir] [seconds] [rtt-ms]
//! ```
//!
//! The defaults are a temp directory, 6 seconds and 50 ms.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use walship::{
    receiver::{Reject, Status},
    wal::{size_after, FRAME_HEADER},
    RawDb, Receiver,
};

const NAME: &str = "stress";
const BATCH: u32 = 256;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn open(path: &Path) -> Res<RawDb> {
    let db = RawDb::open(path, BUSY_TIMEOUT)?;
    db.disable_checkpoint()?;
    Ok(db)
}

/// Row count plus a checksum over every row, cheap enough to compare often.
fn digest(db: &RawDb) -> Res<String> {
    Ok(db.query_text("SELECT count(*) || ':' || coalesce(sum(id * 31 + length(v)), 0) FROM t")?)
}

/// Copies the primary's main file into the receiver as a new generation.
/// The WAL has to be checkpointed first, so the file is the whole database.
fn send_base(primary: &RawDb, rx: &Receiver, generation: u64) -> Res<Status> {
    let ck = primary.checkpoint_truncate()?;
    assert!(ck.complete(), "checkpoint before base: {ck:?}");
    let staged = rx.staging_path(NAME);
    primary.copy_main_file(&staged)?;
    Ok(rx.install_base(NAME, &staged, generation)?)
}

/// Sends frames `from..=to` in one request, the way the shipper would.
fn send_frames(
    primary: &RawDb,
    rx: &Receiver,
    generation: u64,
    from: u32,
    to: u32,
) -> Res<Result<Status, Reject>> {
    let page_size = primary.page_size()?;
    let mut body =
        Vec::with_capacity((to + 1 - from) as usize * (FRAME_HEADER + page_size as usize));
    for no in from..=to {
        body.extend(primary.get_frame(no, page_size)?);
    }
    Ok(rx.apply_frames(NAME, generation, from, page_size, &body))
}

/// Ships everything committed past `acked`, in batches. Returns the
/// replica's frame count afterwards.
fn catch_up(
    primary: &RawDb,
    rx: &Receiver,
    generation: u64,
    mut acked: u32,
    rtt: Duration,
) -> Res<u32> {
    let head = primary
        .head(BUSY_TIMEOUT)?
        .expect("the writer let go of the WAL");
    while acked < head {
        let to = head.min(acked + BATCH);
        thread::sleep(rtt);
        match send_frames(primary, rx, generation, acked + 1, to)? {
            Ok(status) => acked = status.frames.max(acked),
            Err(e) => return Err(format!("frames {}..={to}: {e}", acked + 1).into()),
        }
        // A batch that ends mid-transaction leaves `frames` at the last
        // commit, so the next one starts there.
        if acked < to && to == head {
            break;
        }
    }
    Ok(acked)
}

fn main() -> Res<()> {
    let mut args = std::env::args().skip(1);
    let dir: PathBuf = args.next().map_or_else(
        || std::env::temp_dir().join("walship-stress"),
        PathBuf::from,
    );
    let secs: u64 = args.next().map_or(Ok(6), |s| s.parse())?;
    let rtt = Duration::from_millis(args.next().map_or(Ok(50), |s| s.parse())?);

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let primary_path = dir.join("primary.db");
    let rx = Arc::new(Receiver::new(dir.join("replicas"))?);

    let primary = open(&primary_path)?;
    primary.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    primary.exec("INSERT INTO t(v) VALUES ('seed')")?;
    let mut generation = 1;
    send_base(&primary, &rx, generation)?;
    let replica_path = rx.db_path(NAME);

    println!("writer, shipper ({rtt:?} round trip) and replica reader for {secs}s");
    let stop = Arc::new(AtomicBool::new(false));
    let txns = Arc::new(AtomicU32::new(0));

    let writer = {
        let (stop, txns, path) = (Arc::clone(&stop), Arc::clone(&txns), primary_path.clone());
        thread::spawn(move || -> Res<()> {
            let w = open(&path)?;
            let mut seed = 0x9e37_79b9_u32;
            while !stop.load(Ordering::Relaxed) {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                w.exec("BEGIN")?;
                for i in 0..=seed % 40 {
                    let pad = "y".repeat((seed % 200) as usize);
                    w.exec(&format!("INSERT INTO t(v) VALUES ('{i}-{pad}')"))?;
                }
                w.exec("COMMIT")?;
                txns.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(3));
            }
            Ok(())
        })
    };

    let shipper = {
        let (stop, rx, path) = (Arc::clone(&stop), Arc::clone(&rx), primary_path.clone());
        thread::spawn(move || -> Res<(u32, u32)> {
            let p = open(&path)?;
            let (mut acked, mut max_lag) = (0, 0);
            while !stop.load(Ordering::Relaxed) {
                let head = p.head(BUSY_TIMEOUT)?.unwrap_or(acked);
                assert!(
                    head >= acked,
                    "WAL restarted under the shipper: {head} < {acked}"
                );
                max_lag = max_lag.max(head - acked);
                if head == acked {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                acked = catch_up(&p, &rx, generation, acked, rtt)?;
            }
            Ok((catch_up(&p, &rx, generation, acked, rtt)?, max_lag))
        })
    };

    let reader = {
        let (stop, path) = (Arc::clone(&stop), replica_path.clone());
        thread::spawn(move || -> Res<(u32, u32)> {
            let r = RawDb::open(&path, BUSY_TIMEOUT)?;
            let (mut reads, mut last) = (0, 0);
            while !stop.load(Ordering::Relaxed) {
                let n = r.query_u32("SELECT count(*) FROM t")?;
                assert!(n >= last, "replica went backwards: {n} rows after {last}");
                last = n;
                if reads % 20 == 0 {
                    assert_eq!(r.query_text("PRAGMA quick_check")?, "ok");
                }
                reads += 1;
                thread::sleep(Duration::from_millis(5));
            }
            Ok((reads, last))
        })
    };

    let start = Instant::now();
    thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer panicked")?;
    let (mut acked, max_lag) = shipper.join().expect("shipper panicked")?;
    let (reads, _) = reader.join().expect("reader panicked")?;
    let elapsed = start.elapsed().as_secs_f64();
    let n = txns.load(Ordering::Relaxed);
    println!(
        "  {n} transactions ({:.0}/s), {acked} frames shipped, worst lag {max_lag} frames",
        f64::from(n) / elapsed
    );
    println!("  {reads} reads on the replica, none went backwards");
    let replica = RawDb::open(&replica_path, BUSY_TIMEOUT)?;
    assert_eq!(digest(&primary)?, digest(&replica)?);
    drop(replica);

    println!("a batch that stops mid-transaction, then a receiver restart");
    primary.exec("BEGIN")?;
    for i in 0..2000 {
        primary.exec(&format!("INSERT INTO t(v) VALUES ('mid-{i}')"))?;
    }
    primary.exec("COMMIT")?;
    let head = primary.head(BUSY_TIMEOUT)?.unwrap();
    let half = acked + (head - acked) / 2;
    assert_eq!(
        size_after(&primary.get_frame(half, primary.page_size()?)?),
        0
    );
    let status = send_frames(&primary, &rx, generation, acked + 1, half)?.expect("first half");
    println!(
        "  sent {}..={half} of {head}: replica at {}, pending={}",
        acked + 1,
        status.frames,
        status.pending
    );
    assert_eq!(status.frames, acked);
    assert!(status.pending);
    drop(rx);
    let rx = Receiver::new(dir.join("replicas"))?;
    let status = rx.status(NAME).expect("replica survives a restart");
    println!(
        "  after restart: replica at {}, pending={}",
        status.frames, status.pending
    );
    assert_eq!(status.frames, acked);
    acked = catch_up(&primary, &rx, generation, acked, Duration::ZERO)?;
    assert_eq!(acked, head);

    println!("a resend of frames the replica already has");
    // Turned away with the replica's position, which is where a shipper
    // carries on from. Nothing is written twice.
    let res = send_frames(&primary, &rx, generation, acked - 10, acked)?;
    assert!(
        matches!(res, Err(Reject::Mismatch { frames, .. }) if frames == acked),
        "{res:?}"
    );
    println!("  refused, replica still at {acked}");

    println!("a gap");
    primary.exec("INSERT INTO t(v) VALUES ('gap')")?;
    let head = primary.head(BUSY_TIMEOUT)?.unwrap();
    primary.exec("INSERT INTO t(v) VALUES ('gap 2')")?;
    let res = send_frames(
        &primary,
        &rx,
        generation,
        head + 1,
        primary.head(BUSY_TIMEOUT)?.unwrap(),
    )?;
    println!(
        "  frames past {head} without {}..={head}: {res:?}",
        acked + 1
    );
    assert!(matches!(res, Err(Reject::Mismatch { frames, .. }) if frames == acked));
    acked = catch_up(&primary, &rx, generation, acked, Duration::ZERO)?;

    println!("a checkpoint blocked by a reader, then a new generation");
    let reader = open(&primary_path)?;
    reader.exec("BEGIN")?;
    reader.query_u32("SELECT count(*) FROM t")?;
    let ck = primary.checkpoint_truncate()?;
    println!("  with a read open: {ck:?}");
    assert!(!ck.complete());
    reader.exec("COMMIT")?;
    drop(reader);
    generation += 1;
    let status = send_base(&primary, &rx, generation)?;
    println!("  base for generation {generation}: {status:?}");
    assert_eq!(status.frames, 0);
    // A batch from the generation before is turned away with the replica's
    // position, which a shipper reads as "send a base".
    let page_size = primary.page_size()?;
    let junk = vec![0; FRAME_HEADER + page_size as usize];
    let res = rx.apply_frames(NAME, generation - 1, acked + 1, page_size, &junk);
    assert!(
        matches!(res, Err(Reject::Mismatch { generation: g, .. }) if g == generation),
        "{res:?}"
    );
    for i in 0..300 {
        primary.exec(&format!("INSERT INTO t(v) VALUES ('gen-{i}')"))?;
    }
    acked = catch_up(&primary, &rx, generation, 0, Duration::ZERO)?;
    println!("  generation {generation} at frame {acked}");

    let replica = RawDb::open(&rx.db_path(NAME), BUSY_TIMEOUT)?;
    assert_eq!(replica.query_text("PRAGMA quick_check")?, "ok");
    assert_eq!(digest(&primary)?, digest(&replica)?);
    println!("ok: {}", digest(&replica)?);
    Ok(())
}
