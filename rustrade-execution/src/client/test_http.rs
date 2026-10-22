//! A REST stub served on the test's own runtime, for tests on a paused clock.
//!
//! A paused clock jumps to the next timer whenever the runtime has nothing to run. A stub served
//! from another thread, as `wiremock` serves its mocks, answers in real time, so the clock can
//! jump past timeouts and backoffs while a request is in flight, and the waits a test measures
//! are off by that much. This stub runs on the test's runtime, so the runtime only goes idle once
//! the answer has been written.

use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};

/// Serves `answer(method, path)`, a status and a JSON body, for every request; `path` is without
/// its query. Each connection
/// carries one request and is closed after the answer. Returns the `http://` base URL.
pub(crate) async fn serve(answer: fn(&str, &str) -> (u16, String)) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            // Without it, an answer can wait in the kernel for the client's delayed ACK, which
            // is real time a paused clock does not wait for.
            stream.set_nodelay(true).unwrap();
            tokio::spawn(async move {
                let Some(request) = read_request(&mut stream).await else {
                    return;
                };
                let mut line = request.split_whitespace();
                let method = line.next().unwrap_or("");
                let target = line.next().unwrap_or("");
                let path = target.split_once('?').map_or(target, |(path, _)| path);
                let (status, body) = answer(method, path);
                let response = format!(
                    "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    reason(status),
                    body.len(),
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

/// Reads one request, its body included, and returns its head. `None` if the client hung up
/// first.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<String> {
    let mut read = Vec::new();
    let mut buf = [0; 4096];
    let head_end = loop {
        if let Some(at) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => read.extend_from_slice(&buf[..n]),
        }
    };
    let head = String::from_utf8_lossy(&read[..head_end]).into_owned();
    let body_len = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while read.len() < head_end + body_len {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => read.extend_from_slice(&buf[..n]),
        }
    }
    Some(head)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Unknown",
    }
}
