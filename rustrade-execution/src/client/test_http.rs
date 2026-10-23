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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpStream;

    fn echo(method: &str, path: &str) -> (u16, String) {
        let status = match path {
            "/ok" => 200,
            "/missing" => 404,
            _ => 500,
        };
        (status, format!("{method} {path}"))
    }

    async fn get(url: &str, target: &str) -> String {
        let mut stream = TcpStream::connect(url.trim_start_matches("http://"))
            .await
            .unwrap();
        stream
            .write_all(format!("GET {target} HTTP/1.1\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        answer
    }

    #[tokio::test(start_paused = true)]
    async fn serve_answers_with_status_line_and_body_for_the_path_without_its_query() {
        let url = serve(echo).await;

        let ok = get(&url, "/ok?a=1").await;
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(ok.ends_with("\r\n\r\nGET /ok"), "{ok}");
        assert!(ok.contains("content-length: 7\r\n"), "{ok}");

        let missing = get(&url, "/missing").await;
        assert!(
            missing.starts_with("HTTP/1.1 404 Not Found\r\n"),
            "{missing}"
        );

        let other = get(&url, "/other").await;
        assert!(other.starts_with("HTTP/1.1 500 Unknown\r\n"), "{other}");
    }

    #[tokio::test(start_paused = true)]
    async fn read_request_waits_for_the_whole_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let mut read = tokio::spawn(async move { read_request(&mut server).await });

        client
            .write_all(b"POST /x HTTP/1.1\r\nContent-Length: 10\r\n\r\n01234")
            .await
            .unwrap();
        // The head and half the body are in: the request is not complete yet.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut read)
                .await
                .is_err()
        );

        client.write_all(b"56789").await.unwrap();
        let head = read.await.unwrap().unwrap();
        assert!(head.starts_with("POST /x HTTP/1.1\r\n"), "{head}");
    }

    #[tokio::test(start_paused = true)]
    async fn read_request_without_body_returns_at_the_end_of_the_head() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();

        client.write_all(b"GET /x HTTP/1.1\r\n\r\n").await.unwrap();
        let head = read_request(&mut server).await.unwrap();
        assert_eq!(head, "GET /x HTTP/1.1\r\n\r\n");

        drop(client);
        assert!(read_request(&mut server).await.is_none());
    }
}
