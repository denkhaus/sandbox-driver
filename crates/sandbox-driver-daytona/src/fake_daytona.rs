//! A fake Daytona control plane and toolbox on a local socket, for exec
//! tests that need a session to start, stream, and be deleted without a
//! live account. Speaks enough HTTP/1.1 for the SDK clients, records
//! every request, and can hold one route open until the client leaves.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use daytona_sdk::{Client, DaytonaConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time;

use crate::exec::DaytonaTransport;

/// What the fake answers one request with.
pub(crate) enum Reply {
    /// A JSON body under this status.
    Json(u16, String),
    /// Nothing, until the client closes the connection.
    Hang,
}

pub(crate) type Script = Arc<dyn Fn(&str, &str) -> Reply + Send + Sync>;

/// Sandbox `sb-1` in `state`, with every field the SDK requires to parse it.
pub(crate) fn sandbox_json(state: &str) -> String {
    serde_json::json!({
        "id": "sb-1", "organizationId": "org-1", "name": "sb", "user": "daytona",
        "env": {}, "labels": {}, "public": false, "networkBlockAll": false,
        "target": "us", "cpu": 1.0, "gpu": 0.0, "memory": 1.0, "disk": 1.0,
        "state": state, "toolboxProxyUrl": "http://unused",
    })
    .to_string()
}

/// The Daytona control plane and one sandbox's toolbox on a local
/// socket: enough HTTP/1.1 for the SDK clients, every request
/// recorded, and one route allowed to hang until the client goes away.
pub(crate) struct FakeDaytona {
    address:     SocketAddr,
    requests:    Arc<StdMutex<Vec<String>>>,
    hang_closed: Arc<Notify>,
}

impl FakeDaytona {
    pub(crate) async fn start(script: impl FnOnce(SocketAddr) -> Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let script = script(address);
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let hang_closed = Arc::new(Notify::new());
        tokio::spawn({
            let requests = Arc::clone(&requests);
            let hang_closed = Arc::clone(&hang_closed);
            async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let script = Arc::clone(&script);
                    let requests = Arc::clone(&requests);
                    let hang_closed = Arc::clone(&hang_closed);
                    tokio::spawn(async move {
                        let mut buffer = Vec::new();
                        let mut chunk = [0_u8; 4096];
                        loop {
                            let head_end = loop {
                                if let Some(end) =
                                    buffer.windows(4).position(|window| window == b"\r\n\r\n")
                                {
                                    break end + 4;
                                }
                                match socket.read(&mut chunk).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                                }
                            };
                            let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
                            let mut lines = head.lines();
                            let mut request_line = lines.next().unwrap_or_default().split(' ');
                            let method = request_line.next().unwrap_or_default().to_owned();
                            let target = request_line.next().unwrap_or_default().to_owned();
                            let content_length = lines
                                .filter_map(|line| line.split_once(':'))
                                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            while buffer.len() < head_end + content_length {
                                match socket.read(&mut chunk).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                                }
                            }
                            buffer.drain(..head_end + content_length);
                            requests
                                .lock()
                                .expect("requests lock")
                                .push(format!("{method} {target}"));
                            match script(&method, &target) {
                                Reply::Json(status, body) => {
                                    let reason = match status {
                                        200 => "OK",
                                        201 => "Created",
                                        404 => "Not Found",
                                        _ => "Internal Server Error",
                                    };
                                    let response = format!(
                                        "HTTP/1.1 {status} {reason}\r\nContent-Type: \
                                         application/json\r\nContent-Length: {}\r\n\r\n{body}",
                                        body.len()
                                    );
                                    if socket.write_all(response.as_bytes()).await.is_err() {
                                        return;
                                    }
                                }
                                Reply::Hang => {
                                    while let Ok(read) = socket.read(&mut chunk).await {
                                        if read == 0 {
                                            break;
                                        }
                                    }
                                    hang_closed.notify_one();
                                    return;
                                }
                            }
                        }
                    });
                }
            }
        });
        Self {
            address,
            requests,
            hang_closed,
        }
    }

    pub(crate) async fn transport(&self) -> DaytonaTransport {
        DaytonaTransport::new(
            Arc::new(self.client().await),
            "sb-1".to_owned(),
            "/workspace".to_owned(),
        )
    }

    pub(crate) async fn client(&self) -> Client {
        Client::new_with_config(DaytonaConfig {
            api_key: Some("test-key".to_owned()),
            api_url: Some(format!("http://{}", self.address)),
            ..DaytonaConfig::default()
        })
        .await
        .expect("client")
    }

    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests lock").clone()
    }

    pub(crate) fn deletes(&self) -> usize {
        self.requests()
            .iter()
            .filter(|request| request.starts_with("DELETE "))
            .count()
    }

    /// Resolves once a held-open stream connection has been closed by the
    /// client.
    pub(crate) async fn stream_hang_closed(&self) {
        self.hang_closed.notified().await;
    }

    pub(crate) async fn saw(&self, needle: &str) {
        time::timeout(Duration::from_secs(5), async {
            while !self
                .requests()
                .iter()
                .any(|request| request.contains(needle))
            {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no request matched {needle:?}: {:?}", self.requests()));
    }
}
