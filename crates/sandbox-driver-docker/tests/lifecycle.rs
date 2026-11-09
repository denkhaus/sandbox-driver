//! Lifecycle failures must preserve the sandbox so callers can retry cleanup.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bollard::Docker;
use sandbox_driver::contract::Error as ContractError;
use sandbox_driver::{
    Error, Sandbox, SandboxFilter, SandboxId, SandboxProvider, SandboxSource, SandboxSpec,
};
use sandbox_driver_docker::DockerProvider;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex, split};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct Daemon {
    provider: Arc<DockerProvider>,
    requests: Arc<Mutex<Vec<String>>>,
    stop:     CancellationToken,
    task:     JoinHandle<()>,
}

impl Daemon {
    async fn new(reply: impl Fn(&str) -> (u16, Value) + Send + 'static) -> Self {
        Self::serve(json_bytes(reply), true).await
    }

    /// Fences are still created automatically, but `reply` answers their
    /// release.
    async fn with_manual_release(reply: impl Fn(&str) -> (u16, Value) + Send + 'static) -> Self {
        Self::serve(json_bytes(reply), false).await
    }

    async fn serve(
        reply: impl Fn(&str) -> (u16, Vec<u8>) + Send + 'static,
        automatic_release: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test operation succeeds");
        let address = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let docker = Docker::connect_with_http(&address, 2, bollard::API_DEFAULT_VERSION)
            .expect("Docker client");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = tokio::select! {
                    () = stopped.cancelled() => break,
                    connection = listener.accept() => connection.expect("test connection"),
                };
                let mut bytes = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let count = stream
                        .read(&mut buffer)
                        .await
                        .expect("test operation succeeds");
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let length = headers
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map_or(0, |(_, value)| {
                                value.trim().parse::<usize>().expect("content length")
                            });
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let request = String::from_utf8(bytes).expect("HTTP request is text");
                recorded.lock().unwrap().push(request.clone());
                let request = request.lines().next().expect("request line");
                let (status, body) =
                    if request.contains("/containers/create?name=sandbox-driver-fence-") {
                        (
                            201,
                            json!({"Id": "fence", "Warnings": []})
                                .to_string()
                                .into_bytes(),
                        )
                    } else if automatic_release
                        && request.starts_with("DELETE ")
                        && request.contains("/containers/fence?")
                    {
                        (204, Vec::new())
                    } else {
                        reply(request)
                    };
                // A zero status injects a connection loss after reading the request.
                if status == 0 {
                    continue;
                }
                let body = if status == 204 || status == 304 {
                    Vec::new()
                } else {
                    body
                };
                let response = format!(
                    "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("test operation succeeds");
                stream.write_all(&body).await.expect("response body");
            }
        });
        Self {
            provider: Arc::new(DockerProvider::from_client(docker)),
            requests,
            stop,
            task,
        }
    }

    async fn attach_main(&self) -> Arc<dyn Sandbox> {
        let id = SandboxId::try_new("main").expect("valid sandbox ID");
        self.provider.attach(&id, None).await.expect("attach main")
    }

    async fn finish(self) -> Vec<String> {
        self.stop.cancel();
        self.task.await.expect("test operation succeeds");
        self.requests.lock().unwrap().clone()
    }
}

/// The last daemon request before the automatic fence release.
fn before_fence_release(requests: &[String]) -> &str {
    requests
        .iter()
        .rev()
        .nth(1)
        .expect("a request before the fence release")
}

fn json_bytes(
    reply: impl Fn(&str) -> (u16, Value) + Send + 'static,
) -> impl Fn(&str) -> (u16, Vec<u8>) + Send + 'static {
    move |request| {
        let (status, body) = reply(request);
        (status, body.to_string().into_bytes())
    }
}

fn inspect(running: bool) -> Value {
    json!({
        "Id": "main", "Image": "sha256:test",
        "Config": {"Labels": {"sh.sandbox-driver.managed": "true"}, "WorkingDir": "/workspace"},
        "HostConfig": {"NetworkMode": "example-net"},
        "State": {"Running": running, "Paused": false}
    })
}

fn cleanup_reply(request: &str) -> (u16, Value) {
    if request.contains("/containers/main/json") {
        (200, inspect(true))
    } else if request.contains("/containers/json?") {
        let id = if request.contains("one-shot") {
            "action"
        } else {
            "service"
        };
        (200, json!([{"Id": id}]))
    } else if request.starts_with("GET ") && request.contains("/networks/example-net") {
        (200, json!({"Containers": {"main": {"Name": "main"}}}))
    } else {
        (204, Value::Null)
    }
}

#[tokio::test]
async fn delete_keeps_the_primary_resource_until_dependents_are_removed() {
    for failure in [
        "one-shot",
        "/containers/action?",
        "network%3D",
        "/containers/service?",
        "network-delete",
    ] {
        let daemon = Daemon::new(move |request| {
            // API versions are selected by Bollard; match the endpoint for
            // network deletion without depending on the selected version.
            let fail = if failure == "network-delete" {
                request.starts_with("DELETE ") && request.contains("/networks/example-net")
            } else {
                request.contains(failure)
            };
            if fail {
                (500, json!({"message": "injected cleanup failure"}))
            } else {
                cleanup_reply(request)
            }
        })
        .await;
        let result = daemon
            .provider
            .delete(&SandboxId::try_new("main").unwrap(), None)
            .await;
        let requests = daemon.finish().await;
        assert!(result.is_err(), "failure {failure}: {requests:?}");
        assert!(
            !requests
                .iter()
                .any(|request| request.starts_with("DELETE ")
                    && request.contains("/containers/main?")),
            "primary removed after {failure}: {requests:?}"
        );
    }
}

#[tokio::test]
async fn missing_dependents_are_successful_cleanup() {
    let daemon = Daemon::new(|request| {
        if request.starts_with("DELETE ") && !request.contains("/containers/main?") {
            (404, json!({"message": "already removed"}))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    daemon
        .provider
        .delete(&SandboxId::try_new("main").unwrap(), None)
        .await
        .unwrap();
    let requests = daemon.finish().await;
    assert!(before_fence_release(&requests).contains("/containers/main?"));
}

#[tokio::test]
async fn delete_retries_a_network_failure_after_disconnecting_the_sandbox() {
    let state = Mutex::new((true, true));
    let daemon = Daemon::new(move |request| {
        let mut state = state.lock().unwrap();
        if request.starts_with("GET ") && request.contains("/networks/example-net") {
            let containers = if state.0 {
                json!({"main": {"Name": "main"}})
            } else {
                json!({})
            };
            (200, json!({"Containers": containers}))
        } else if request.contains("/networks/example-net/disconnect") {
            assert!(state.0, "retry must not disconnect an absent endpoint");
            state.0 = false;
            (204, Value::Null)
        } else if request.starts_with("DELETE ")
            && request.contains("/networks/example-net")
            && state.1
        {
            state.1 = false;
            (500, json!({"message": "network is temporarily busy"}))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    let id = SandboxId::try_new("main").unwrap();
    let sandbox = daemon
        .provider
        .attach(&id, None)
        .await
        .expect("test operation succeeds");
    assert!(sandbox.delete().await.is_err());
    // A fresh provider call must be able to recover from the main
    // container's preserved network mode, without the original handle.
    daemon
        .provider
        .delete(&id, None)
        .await
        .expect("test operation succeeds");
    let requests = daemon.finish().await;
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("/disconnect"))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(
                |request| request.starts_with("DELETE ") && request.contains("/containers/main?")
            )
            .count(),
        1
    );
    assert!(before_fence_release(&requests).contains("/containers/main?"));
}

/// A handle whose container vanished reports `NotFound` from every verb
/// that inspects it first, as the provider's own inspect does.
#[tokio::test]
async fn a_vanished_container_is_not_found_by_environment_and_start() {
    let gone = Arc::new(AtomicBool::new(false));
    let seen_gone = gone.clone();
    let daemon = Daemon::new(move |request| {
        if request.contains("/containers/main/json") {
            if seen_gone.load(Ordering::SeqCst) {
                (404, json!({"message": "no such container"}))
            } else {
                (200, inspect(true))
            }
        } else {
            (204, Value::Null)
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    gone.store(true, Ordering::SeqCst);
    assert!(matches!(
        sandbox.environment().await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(sandbox.start().await, Err(Error::NotFound { .. })));
    daemon.finish().await;
}

#[tokio::test]
async fn stop_reports_failed_one_shot_cleanup() {
    let daemon = Daemon::new(|request| {
        if request.contains("one-shot") {
            (500, json!({"message": "list failed"}))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    assert!(sandbox.stop().await.is_err());
    let requests = daemon.finish().await;
    assert!(
        !requests
            .iter()
            .any(|request| request.contains("/containers/main/stop"))
    );
}

#[tokio::test]
async fn start_is_idempotent_and_waits_for_service_health() {
    let health_reads = Mutex::new(0);
    let daemon = Daemon::new(move |request| {
        if request.contains("/containers/main/json") {
            (200, inspect(true))
        } else if request.contains("/containers/json?") {
            (200, json!([{"Id": "service"}]))
        } else if request.contains("/containers/service/json") {
            let mut reads = health_reads.lock().unwrap();
            *reads += 1;
            let health = if *reads < 3 { "starting" } else { "healthy" };
            (
                200,
                json!({"State": {"Status": "running", "Health": {"Status": health}}}),
            )
        } else if request.contains("/start") {
            (304, Value::Null)
        } else {
            panic!("unexpected request: {request}")
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    timeout(Duration::from_secs(3), sandbox.start())
        .await
        .unwrap()
        .unwrap();
    let requests = daemon.finish().await;
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("/containers/service/json"))
            .count(),
        3
    );
    assert!(before_fence_release(&requests).contains("/containers/main/start"));
}

#[tokio::test]
async fn interrupted_service_creation_remains_discoverable_for_cleanup() {
    let retrying = Arc::new(AtomicBool::new(false));
    let retry = retrying.clone();
    let daemon = Daemon::new(move |request| {
        if request.contains("/images/") && request.contains("/json") {
            (200, json!({}))
        } else if request.contains("/containers/create?name=example-net-db") {
            (500, json!({"message": "original service create failure"}))
        } else if request.contains("/containers/create?") {
            (
                201,
                json!({"Id": "main", "Image": "sha256:test", "Warnings": []}),
            )
        } else if request.contains("/networks/create") {
            (201, json!({"Id": "network", "Warning": ""}))
        } else if request.contains("/containers/json?") && request.contains("network%3D") {
            if retry.load(Ordering::SeqCst) {
                (200, json!([]))
            } else {
                (500, json!({"message": "cleanup unavailable"}))
            }
        } else if request.contains("/containers/json?") {
            if request.contains("one-shot") {
                (200, json!([]))
            } else {
                (200, json!([{"Id": "main"}]))
            }
        } else if request.contains("/containers/main/json") {
            (
                200,
                json!({
                    "Id": "main", "Image": "sha256:test", "Config": {"Labels": {
                        "sh.sandbox-driver.managed": "true",
                        "sh.sandbox-driver.sidecar-network": "example-net",
                        "petri.workspace": "test/work"
                    }}, "HostConfig": {"NetworkMode": "none"},
                    "State": {"Status": "created", "Running": false},
                    "NetworkSettings": {"Networks": {"none": {}}}
                }),
            )
        } else if request.starts_with("GET ") && request.contains("/networks/example-net") {
            (200, json!({"Containers": {}}))
        } else {
            (204, Value::Null)
        }
    })
    .await;
    let mut spec = SandboxSpec::new(SandboxSource::Image {
        reference: "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17".to_owned(),
    })
    .name("example")
    .label("petri.workspace", "test/work");
    spec.provider_config = json!({"sidecars": [{"name": "db", "image": "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17"}]});
    let error = daemon
        .provider
        .create(&spec, None)
        .await
        .err()
        .expect("create fails");
    assert!(error.to_string().contains("creating sidecar"), "{error}");
    let Error::Provider(failure) = error else {
        panic!("expected incomplete cleanup");
    };
    assert_eq!(failure.code.as_deref(), Some("incomplete_cleanup"));
    let detail = failure.detail.expect("structured cleanup");
    assert!(
        detail["cause"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("creating sidecar")),
        "{detail}"
    );
    let projection: ContractError = serde_json::from_value(detail).expect("shared error");
    assert!(
        matches!(projection, ContractError::IncompleteCleanup { remaining, .. }
        if remaining.iter().any(|resource| resource.id == "main")
        && remaining.iter().any(|resource| resource.id == "example-net"))
    );
    let mut filter = SandboxFilter::default();
    filter
        .labels
        .insert("petri.workspace".to_owned(), "test/work".to_owned());
    let found = daemon
        .provider
        .list(&filter)
        .await
        .expect("allocation remains discoverable");
    assert_eq!(found.len(), 1);
    let sandbox = daemon
        .provider
        .attach(&found[0].id, None)
        .await
        .expect("attach partial allocation");
    assert!(
        sandbox.start().await.is_err(),
        "incomplete services must not become a usable sandbox"
    );
    retrying.store(true, Ordering::SeqCst);
    daemon
        .provider
        .delete(&found[0].id, None)
        .await
        .expect("retry cleans the allocation");
    let requests = daemon.finish().await;
    let primary = requests
        .iter()
        .position(|request| request.contains("/containers/create?name=example "))
        .expect("primary create");
    let network = requests
        .iter()
        .position(|request| request.contains("/networks/create"))
        .expect("network create");
    assert!(
        primary < network,
        "primary must precede every dependent: {requests:?}"
    );
    assert!(requests[primary].contains("sh.sandbox-driver.sidecar-network"));
    assert!(requests[primary].contains("petri.workspace"));
    assert_eq!(
        requests
            .iter()
            .filter(
                |request| request.starts_with("DELETE ") && request.contains("/containers/main?")
            )
            .count(),
        1
    );
    assert!(before_fence_release(&requests).contains("/containers/main?"));
}

#[tokio::test]
async fn a_failed_fence_release_is_not_a_successful_stop() {
    let stopped = AtomicBool::new(false);
    let daemon = Daemon::with_manual_release(move |request| {
        if request.contains("/containers/main/stop") {
            stopped.store(true, Ordering::SeqCst);
        }
        if request.contains("/containers/main/json") {
            return (200, inspect(!stopped.load(Ordering::SeqCst)));
        }
        if request.contains("/containers/service/json") {
            return (200, inspect(false));
        }
        if request.starts_with("DELETE ") && request.contains("/containers/fence?") {
            (500, json!({"message": "release not confirmed"}))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    let error = sandbox
        .stop()
        .await
        .expect_err("unconfirmed release must fail");
    let Error::Provider(error) = error else {
        panic!("provider error");
    };
    assert_eq!(error.code.as_deref(), Some("transport"));
    assert_eq!(error.detail.unwrap()["fence_container"], "fence");
    let requests = daemon.finish().await;
    assert!(
        requests
            .iter()
            .any(|request| request.contains("/containers/main/stop"))
    );
}

#[tokio::test]
async fn unknown_exec_admission_retains_the_fence_without_replaying() {
    let daemon = Daemon::new(|request| {
        if request.contains("/containers/main/exec") {
            (201, json!("not a create-exec response"))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    let error = sandbox
        .exec()
        .run(&sandbox_driver::ExecSpec::new("true"))
        .await
        .expect_err("unknown exec ID");
    let Error::Provider(error) = error else {
        panic!("provider error");
    };
    assert_eq!(error.code.as_deref(), Some("transport"));
    let requests = daemon.finish().await;
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("/containers/main/exec"))
            .count(),
        1
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.starts_with("DELETE "))
    );
}

#[tokio::test]
async fn absent_primary_does_not_hide_retained_ownership() {
    let daemon = Daemon::new(|request| {
        if request.contains("/containers/main/json") {
            (404, json!({"message": "primary removed"}))
        } else if request.contains("/containers/json?") && request.contains("fence") {
            (
                200,
                json!([{"Id": "retained-fence", "Labels": {"sh.sandbox-driver.fence": "main"}}]),
            )
        } else {
            panic!("unexpected request: {request}");
        }
    })
    .await;
    let error = daemon
        .provider
        .delete(&SandboxId::try_new("main").unwrap(), None)
        .await
        .expect_err("ownership remains");
    assert!(matches!(error, Error::Provider(error) if error.code.as_deref() == Some("busy")));
    daemon.finish().await;
}

#[tokio::test]
async fn failed_delete_release_remains_busy_by_name_and_short_id() {
    const ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let removed = AtomicBool::new(false);
    let claim_labels = Arc::new(Mutex::new(Value::Null));
    let labels = Arc::clone(&claim_labels);
    let daemon = Daemon::with_manual_release(move |request| {
        if request.starts_with("GET ") && request.contains("/json ") {
            if removed.load(Ordering::SeqCst) {
                return (404, json!({"message": "primary removed"}));
            }
            let mut primary = inspect(true);
            primary["Id"] = json!(ID);
            primary["Name"] = json!("/example");
            primary["HostConfig"]["NetworkMode"] = json!("default");
            return (200, primary);
        }
        if request.starts_with("DELETE ") && request.contains(&format!("/containers/{ID}?")) {
            removed.store(true, Ordering::SeqCst);
            return (204, Value::Null);
        }
        if request.starts_with("DELETE ") && request.contains("/containers/fence?") {
            return (500, json!({"message": "injected fence release failure"}));
        }
        if request.contains("/containers/json?") {
            if request.contains("fence") {
                // Honor both label-presence discovery and the old exact-ID
                // query, so the regression fails with the original lookup.
                let labels = labels.lock().unwrap();
                let owner = labels["sh.sandbox-driver.fence"].as_str().unwrap();
                if !request.contains("fence%3D") || request.contains(&format!("fence%3D{owner}%22"))
                {
                    return (200, json!([{"Id": "fence", "Labels": *labels}]));
                }
            }
            return (200, json!([]));
        }
        panic!("unexpected request: {request}");
    })
    .await;
    let result = daemon
        .provider
        .delete(&SandboxId::try_new("example").unwrap(), None)
        .await;
    assert!(
        matches!(result, Err(Error::Provider(error)) if error.code.as_deref() == Some("transport"))
    );
    // Discover the actual claim metadata emitted by the provider, rather than
    // hard-coding the new name label into the simulated daemon's response.
    {
        let requests = daemon.requests.lock().unwrap();
        let request = requests
            .iter()
            .find(|request| request.contains("/containers/create?name=sandbox-driver-fence-"))
            .unwrap();
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        let claim: Value = serde_json::from_str(body).unwrap();
        *claim_labels.lock().unwrap() = claim["Labels"].clone();
    }
    for alias in [ID, &ID[..12], "example"] {
        let result = daemon
            .provider
            .delete(&SandboxId::try_new(alias).unwrap(), None)
            .await;
        assert!(
            matches!(result, Err(Error::Provider(error)) if error.code.as_deref() == Some("busy")),
            "retained ownership was missed for {alias}"
        );
    }
    for unrelated in ["exam", "another-sandbox", "bbbbbbbbbbbb"] {
        daemon
            .provider
            .delete(&SandboxId::try_new(unrelated).unwrap(), None)
            .await
            .expect("unrelated names remain absent");
    }
    daemon.finish().await;
}

#[tokio::test]
async fn a_stop_acknowledgement_with_live_processes_is_a_leaked_fence() {
    let daemon = Daemon::new(cleanup_reply).await;
    let sandbox = daemon.attach_main().await;
    let error = sandbox
        .stop()
        .await
        .expect_err("daemon still reports running");
    assert!(
        matches!(error, Error::Provider(error) if error.code.as_deref() == Some("fence_leaked"))
    );
    let requests = daemon.finish().await;
    assert!(
        requests
            .iter()
            .any(|request| request.contains("/containers/service/stop")),
        "an unconfirmed primary must not leave its services running"
    );
    assert!(!requests.iter().any(|request| request.starts_with("DELETE ") && request.contains("/containers/fence?")));
}

#[tokio::test]
async fn an_unidentifiable_one_shot_prevents_confirmed_stop() {
    let daemon = Daemon::new(|request| {
        if request.contains("one-shot") {
            (200, json!([{"State": "running"}]))
        } else {
            cleanup_reply(request)
        }
    })
    .await;
    let sandbox = daemon.attach_main().await;
    assert!(sandbox.stop().await.is_err());
    let requests = daemon.finish().await;
    assert!(
        !requests
            .iter()
            .any(|request| request.contains("/containers/main/stop"))
    );
    // A complete listing is not an unknown outcome, so the claim is released.
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("DELETE ") && request.contains("/containers/fence?"))
    );
}

#[tokio::test]
async fn failed_creation_retains_ownership_if_creation_or_cleanup_is_uncertain() {
    for (creation_status, cleanup_status) in [500, 0, 502, 504]
        .into_iter()
        .flat_map(|creation| [500, 0, 502, 504].map(|cleanup| (creation, cleanup)))
    {
        let daemon = Daemon::new(move |request| {
            if request.contains("/images/") && request.contains("/json") {
                (200, json!({}))
            } else if request.contains("/containers/create?name=example-net-db") {
                (
                    creation_status,
                    json!({"message": "service creation failed"}),
                )
            } else if request.contains("/containers/create?name=example ") {
                (201, json!({"Id": "main", "Warnings": []}))
            } else if request.contains("/networks/create") {
                (201, json!({"Id": "network", "Warning": ""}))
            } else if request.contains("/containers/json?") && request.contains("network%3D") {
                (cleanup_status, json!({"message": "cleanup listing failed"}))
            } else {
                panic!("unexpected request: {request}");
            }
        })
        .await;
        let mut spec = SandboxSpec::new(SandboxSource::Image {
            reference: "test-image".to_owned(),
        })
        .name("example");
        spec.provider_config = json!({"sidecars": [{"name": "db", "image": "test-image"}]});
        let error = daemon
            .provider
            .create(&spec, None)
            .await
            .err()
            .expect("create fails");
        let Error::Provider(failure) = error else {
            panic!("provider error expected")
        };
        assert_eq!(failure.code.as_deref(), Some("incomplete_cleanup"));
        let detail = failure.detail.expect("cleanup details");
        assert!(
            detail["cause"]["message"]
                .as_str()
                .unwrap()
                .contains("creating sidecar"),
            "{detail}"
        );
        assert!(
            detail["cleanup_cause"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("listing sidecars")),
            "{detail}"
        );
        let requests = daemon.finish().await;
        let released = requests.iter().any(|request| {
            request.starts_with("DELETE ") && request.contains("/containers/fence?")
        });
        let retain = creation_status != 500 || cleanup_status != 500;
        assert_eq!(
            released, !retain,
            "creation_status={creation_status}, cleanup_status={cleanup_status}: {requests:#?}"
        );
        let projection: ContractError =
            serde_json::from_value(detail).expect("shared cleanup error");
        let ContractError::IncompleteCleanup { remaining, .. } = projection else {
            panic!("incomplete cleanup expected");
        };
        assert_eq!(
            remaining
                .iter()
                .any(|resource| resource.kind == "fence_container" && resource.id == "fence"),
            retain
        );
    }
}

#[tokio::test]
async fn stop_preserves_retention_when_both_termination_checks_fail() {
    // A definite inspection refusal must not mask the other resource's
    // known-live or unknown state, whichever resource fails first.
    for (uncertain, expected_code) in [
        (inspect(true), "fence_leaked"),
        (json!("invalid inspect response"), "transport"),
    ] {
        for uncertain_service in [true, false] {
            let stopped = AtomicBool::new(false);
            let uncertain = uncertain.clone();
            let daemon = Daemon::new(move |request| {
                if request.contains("/containers/main/stop") {
                    stopped.store(true, Ordering::SeqCst);
                }
                let primary =
                    request.contains("/containers/main/json") && stopped.load(Ordering::SeqCst);
                let service = request.contains("/containers/service/json");
                if primary || service {
                    return if service == uncertain_service {
                        (200, uncertain.clone())
                    } else {
                        (500, json!({"message": "inspection unavailable"}))
                    };
                }
                cleanup_reply(request)
            })
            .await;
            let sandbox = daemon.attach_main().await;
            let error = sandbox
                .stop()
                .await
                .expect_err("termination is not confirmed");
            let requests = daemon.finish().await;
            let Error::Provider(error) = error else {
                panic!("expected provider error");
            };
            assert_eq!(error.code.as_deref(), Some(expected_code));
            assert_eq!(
                error.detail.expect("retained fence detail")["fence_container"],
                "fence"
            );
            assert!(
                requests
                    .iter()
                    .any(|request| request.contains("/containers/service/stop"))
            );
            assert!(
                requests
                    .iter()
                    .any(|request| request.contains("/containers/service/json"))
            );
            assert!(
                !requests.iter().any(|request| request.starts_with("DELETE ")
                    && request.contains("/containers/fence?"))
            );
        }
    }
}

#[tokio::test]
async fn definite_inspection_refusals_release_the_stop_claim() {
    let stopped = AtomicBool::new(false);
    let daemon = Daemon::new(move |request| {
        if request.contains("/containers/main/stop") {
            stopped.store(true, Ordering::SeqCst);
        }
        if (request.contains("/containers/main/json") && stopped.load(Ordering::SeqCst))
            || request.contains("/containers/service/json")
        {
            return (500, json!({"message": "inspection unavailable"}));
        }
        cleanup_reply(request)
    })
    .await;
    let sandbox = daemon.attach_main().await;
    let error = sandbox.stop().await.expect_err("inspection was refused");
    let requests = daemon.finish().await;
    assert!(matches!(error, Error::Provider(error) if error.code.as_deref() == Some("500")));
    assert!(requests.iter().any(|request| request.starts_with("DELETE ") && request.contains("/containers/fence?")));
}

async fn verify_stop_status_retention(status: u16, should_retain: bool) {
    let daemon = Daemon::new(move |request| {
        if request.contains("/containers/main/stop") {
            return (
                status,
                json!({"message": "upstream stop did not provide a timely response"}),
            );
        }
        if request.contains("/containers/service/json") {
            return (200, inspect(false));
        }
        cleanup_reply(request)
    })
    .await;
    let sandbox = daemon.attach_main().await;
    let error = sandbox.stop().await.expect_err("stop failed");
    let requests = daemon.finish().await;
    let released = requests
        .iter()
        .any(|request| request.starts_with("DELETE ") && request.contains("/containers/fence?"));
    let trace: Vec<&str> = requests
        .iter()
        .map(|request| request.lines().next().expect("request line"))
        .collect();
    assert_eq!(
        !released, should_retain,
        "HTTP {status}; error: {error}; trace: {trace:#?}"
    );
    assert!(
        trace
            .iter()
            .any(|request| request.contains("/containers/service/stop")),
        "HTTP {status}: a failed primary stop left services running; trace: {trace:#?}"
    );
}

#[tokio::test]
async fn gateway_timeout_must_not_release_an_in_flight_stop() {
    for status in [502, 504] {
        verify_stop_status_retention(status, true).await;
    }
}

#[tokio::test]
async fn control_definite_permission_refusal_can_release_stop() {
    for status in [403, 409, 500, 503] {
        verify_stop_status_retention(status, false).await;
    }
}

#[tokio::test]
async fn operation_and_release_outcomes_are_preserved() {
    for work_fails in [false, true] {
        for release_status in [204, 0, 500, 504] {
            let stopped = AtomicBool::new(false);
            let daemon = Daemon::with_manual_release(move |request| {
                if work_fails && request.contains("one-shot") {
                    return (500, json!({"message": "ORIGINAL_SWEEP_FAILURE"}));
                }
                if request.starts_with("DELETE ") && request.contains("/containers/fence?") {
                    return (release_status, json!({"message": "RELEASE_FAILURE"}));
                }
                if request.contains("/containers/main/stop") {
                    stopped.store(true, Ordering::SeqCst);
                }
                if request.contains("/containers/main/json") {
                    return (200, inspect(!stopped.load(Ordering::SeqCst)));
                }
                if request.contains("/containers/service/json") {
                    return (200, inspect(false));
                }
                cleanup_reply(request)
            })
            .await;
            let result = daemon.attach_main().await.stop().await;
            let requests = daemon.finish().await;
            assert_eq!(result.is_ok(), !work_fails && release_status == 204);
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.starts_with("DELETE ")
                        && request.contains("/containers/fence?"))
                    .count(),
                1
            );
            if let Err(error) = result {
                if work_fails {
                    assert!(
                        format!("{error:?}").contains("ORIGINAL_SWEEP_FAILURE"),
                        "{error:?}"
                    );
                }
                if release_status != 204 {
                    let Error::Provider(failure) = error else {
                        panic!("provider failure")
                    };
                    assert_eq!(failure.code.as_deref(), Some("transport"));
                    let detail = failure.detail.unwrap();
                    assert_eq!(
                        detail["operation_outcome"],
                        if work_fails { "failed" } else { "succeeded" }
                    );
                    assert_eq!(detail["operation_error"].is_null(), !work_fails);
                    assert_eq!(detail["fence_container"], "fence");
                    assert!(!detail["release_error"].is_null());
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StopObservation {
    Stopped,
    Refused,
    Running,
    Disconnected,
}

impl StopObservation {
    fn reply(self, resource: &str) -> (u16, Value) {
        match self {
            Self::Stopped => (200, inspect(false)),
            Self::Refused => (
                500,
                json!({"message": format!("{resource} inspection refused")}),
            ),
            Self::Running => (200, inspect(true)),
            Self::Disconnected => (0, Value::Null),
        }
    }

    fn retains(self) -> bool {
        matches!(self, Self::Running | Self::Disconnected)
    }
}

#[tokio::test]
async fn stop_combines_all_primary_and_service_outcomes() {
    let observations = [
        StopObservation::Stopped,
        StopObservation::Refused,
        StopObservation::Running,
        StopObservation::Disconnected,
    ];
    for primary in observations {
        for service in observations {
            let stopped = AtomicBool::new(false);
            let daemon = Daemon::new(move |request| {
                if request.contains("/containers/main/stop") {
                    stopped.store(true, Ordering::SeqCst);
                }
                if request.contains("/containers/main/json") && stopped.load(Ordering::SeqCst) {
                    return primary.reply("primary");
                }
                if request.contains("/containers/service/json") {
                    return service.reply("service");
                }
                cleanup_reply(request)
            })
            .await;
            let result = daemon.attach_main().await.stop().await;
            let requests = daemon.finish().await;
            let released = requests.iter().any(|request| {
                request.starts_with("DELETE ") && request.contains("/containers/fence?")
            });
            assert_eq!(
                released,
                !(primary.retains() || service.retains()),
                "primary={primary:?}, service={service:?}"
            );
            assert_eq!(
                result.is_ok(),
                matches!(
                    (primary, service),
                    (StopObservation::Stopped, StopObservation::Stopped)
                )
            );
            assert!(
                requests
                    .iter()
                    .any(|request| request.contains("/containers/service/json"))
            );
        }
    }
}

#[tokio::test]
async fn ambiguous_claim_acquisition_reports_the_claim_without_starting_work() {
    for status in [502, 504] {
        let daemon = Daemon::new(move |request| {
            if request.contains("/containers/main/json") {
                let mut primary = inspect(true);
                primary["Config"]["Labels"]["sh.sandbox-driver.fence-name"] = json!("test-claim");
                return (200, primary);
            }
            if request.contains("/containers/create?name=test-claim") {
                return (
                    status,
                    json!({"message": "gateway could not confirm claim allocation"}),
                );
            }
            panic!("unexpected request: {request}");
        })
        .await;
        let error = daemon
            .attach_main()
            .await
            .stop()
            .await
            .expect_err("unknown acquisition");
        let Error::Provider(failure) = error else {
            panic!("provider failure")
        };
        assert_eq!(failure.code.as_deref(), Some("transport"));
        assert_eq!(failure.detail.unwrap()["fence_container"], "test-claim");
        let requests = daemon.finish().await;
        assert!(
            !requests
                .iter()
                .any(|request| request.contains("/containers/main/stop"))
        );
        assert!(
            !requests
                .iter()
                .any(|request| request.starts_with("DELETE "))
        );
    }
}

#[tokio::test]
async fn paired_stop_errors_survive_the_plugin_protocol() {
    for service_disconnects in [false, true] {
        let stopped = AtomicBool::new(false);
        let daemon = Daemon::new(move |request| {
            if request.contains("/containers/main/stop") {
                stopped.store(true, Ordering::SeqCst);
            }
            if request.contains("/containers/main/json") && stopped.load(Ordering::SeqCst) {
                return (500, json!({"message": "PRIMARY_INSPECTION_FAILURE"}));
            }
            if request.contains("/containers/service/json") {
                return if service_disconnects {
                    (0, Value::Null)
                } else {
                    (200, inspect(true))
                };
            }
            cleanup_reply(request)
        })
        .await;
        let (host, plugin) = duplex(1024 * 1024);
        let (host_read, host_write) = split(host);
        let (plugin_read, plugin_write) = split(plugin);
        let serving = tokio::spawn(sandbox_driver_protocol::serve(
            daemon.provider.clone() as Arc<dyn SandboxProvider>,
            plugin_read,
            plugin_write,
        ));
        let remote = sandbox_driver_protocol::PluginProvider::connect(host_read, host_write)
            .await
            .expect("connect plugin");
        let sandbox = remote
            .attach(&SandboxId::try_new("main").unwrap(), None)
            .await
            .expect("attach");
        let error = sandbox.stop().await.expect_err("combined stop failure");
        serving.abort();
        let _ = serving.await;
        let requests = daemon.finish().await;
        let Error::Provider(failure) = error else {
            panic!("provider failure")
        };
        let detail = failure.detail.expect("paired diagnostic");
        assert!(
            detail.to_string().contains("PRIMARY_INSPECTION_FAILURE"),
            "{detail}"
        );
        assert!(detail.to_string().contains("related_error"), "{detail}");
        assert_eq!(detail["fence_container"], "fence");
        assert!(!requests.iter().any(
            |request| request.starts_with("DELETE ") && request.contains("/containers/fence?")
        ));
    }
}

#[tokio::test]
async fn completed_creation_is_identifiable_when_release_fails() {
    let daemon = Daemon::with_manual_release(|request| {
        if request.contains("/images/") {
            return (200, json!({}));
        }
        if request.contains("/containers/create?") {
            return (201, json!({"Id": "main", "Warnings": []}));
        }
        if request.contains("/containers/main/json") {
            let mut primary = inspect(true);
            primary["State"]["Status"] = json!("running");
            primary["HostConfig"]["NetworkMode"] = json!("default");
            return (200, primary);
        }
        if request.contains("/containers/main/start") {
            return (204, Value::Null);
        }
        if request.starts_with("DELETE ") && request.contains("/containers/fence?") {
            return (500, json!({"message": "RELEASE_FAILURE"}));
        }
        panic!("unexpected request: {request}");
    })
    .await;
    let spec = SandboxSpec::new(SandboxSource::Image {
        reference: "test-image".to_owned(),
    })
    .name("example");
    let error = daemon
        .provider
        .create(&spec, None)
        .await
        .err()
        .expect("release fails");
    let Error::Provider(failure) = error else {
        panic!("provider failure")
    };
    let detail = failure.detail.unwrap();
    assert_eq!(detail["operation_outcome"], "succeeded");
    assert_eq!(detail["sandbox_id"], "main");
    assert_eq!(detail["sandbox"], "example");
    assert_eq!(detail["fence_container"], "fence");
    assert!(detail["operation_error"].is_null());
    daemon.finish().await;
}

#[tokio::test]
async fn one_shot_admission_retains_uncertain_cleanup_without_replaying_it() {
    for cleanup_status in [204, 403, 0, 502, 504] {
        let daemon = Daemon::new(move |request| {
            if request.contains("/images/") { return (200, json!({})); }
            if request.contains("/containers/main/json") {
                let mut primary = inspect(true);
            primary["State"]["Status"] = json!("running");
                primary["Mounts"] = json!([{"Type": "volume", "Name": "workspace", "Destination": "/workspace", "RW": true}]);
                return (200, primary);
            }
            if request.contains("/containers/create ") { return (201, json!({"Id": "action", "Warnings": []})); }
            if request.contains("/containers/action/attach?") { return (403, json!({"message": "ATTACH_REFUSED"})); }
            if request.starts_with("DELETE ") && request.contains("/containers/action?") {
                return (cleanup_status, json!({"message": "CLEANUP_FAILED"}));
            }
            panic!("unexpected request: {request}");
        }).await;
        let sandbox = daemon.attach_main().await;
        let error = sandbox
            .one_shot()
            .expect("workspace one-shot")
            .run(
                &sandbox_driver::OneShotSpec::registry("test-image"),
                sandbox_driver::ExecControls::buffered(),
            )
            .await
            .expect_err("admission refused");
        let requests = daemon.finish().await;
        let removals: Vec<_> = requests
            .iter()
            .filter(|request| {
                request.starts_with("DELETE ") && request.contains("/containers/action?")
            })
            .collect();
        assert_eq!(removals.len(), 1, "cleanup must not replay: {requests:#?}");
        let released = requests.iter().any(|request| {
            request.starts_with("DELETE ") && request.contains("/containers/fence?")
        });
        assert_eq!(
            released,
            matches!(cleanup_status, 204 | 403),
            "status={cleanup_status}; {error:?}"
        );
        assert!(format!("{error:?}").contains("ATTACH_REFUSED"));
    }
}
