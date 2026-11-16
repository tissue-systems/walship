//! Upload routes refuse work past their concurrency limit instead of queueing
//! it, so a flood of uploads can't take the receiver's memory or disk.

use std::{sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use walship::{server::Routes, Receiver};

async fn post_head(addr: std::net::SocketAddr, path: &str, len: usize) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer t\r\nContent-Length: {len}\r\n\r\n"
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s
}

async fn status_line(s: &mut TcpStream) -> String {
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("no reply")
        .unwrap();
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_upload_past_the_limit_gets_503() {
    let root = std::env::temp_dir().join(format!("walship-limits-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let receiver = Arc::new(Receiver::new(root.join("replicas")).unwrap());
    let app = Routes::new(receiver)
        .token("t")
        .concurrency(1, 1)
        .into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });

    for path in [
        "/replica/a/base?generation=1",
        "/replica/a/frames?generation=1&first=1&page_size=4096",
    ] {
        // The first upload sends half its body and stalls, holding the slot.
        let mut first = post_head(addr, path, 2048).await;
        first.write_all(&[0u8; 1024]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut second = post_head(addr, path, 16).await;
        assert!(status_line(&mut second).await.contains(" 503 "), "{path}");

        // Once the first finishes, the slot is free again.
        first.write_all(&[0u8; 1024]).await.unwrap();
        let reply = status_line(&mut first).await;
        assert!(!reply.contains(" 503 "), "{path}: {reply}");
        let mut third = post_head(addr, path, 0).await;
        assert!(!status_line(&mut third).await.contains(" 503 "), "{path}");
    }
    let _ = std::fs::remove_dir_all(&root);
}
