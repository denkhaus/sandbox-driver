//! Real daemon tests of the cross-client fence. Run explicitly with
//! `cargo test -p sandbox-driver-docker --test fence -- --ignored`.

use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
};
use sandbox_driver::{
    Error, ExecControls, ExecSpec, OneShotSpec, SandboxProvider, SandboxSource, SandboxSpec,
    Termination,
};
use sandbox_driver_docker::DockerProvider;
use sandbox_driver_protocol::PluginProvider;
use tokio::process::Command;
use tokio::time::{sleep, timeout};

const IMAGE: &str = "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17";

#[track_caller]
fn assert_busy<T>(result: sandbox_driver::Result<T>) {
    assert!(
        matches!(&result, Err(Error::Provider(error)) if error.code.as_deref() == Some("busy")),
        "expected busy, got {:?}",
        result.err()
    );
}

async fn labeled(docker: &Docker, label: &str, id: &str) -> Vec<String> {
    docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: HashMap::from([("label".to_owned(), vec![format!("{label}={id}")])]),
            ..Default::default()
        }))
        .await
        .expect("list test resources")
        .into_iter()
        .filter_map(|c| c.id)
        .collect()
}

async fn wait_for_label(docker: &Docker, label: &str, id: &str) {
    timeout(Duration::from_secs(10), async {
        while labeled(docker, label, id).await.is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("operation reached admission");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn stop_fences_other_clients_exec_and_one_shots_and_blocks_start_during_drain() {
    let provider = DockerProvider::connect().await.expect("Docker is required");
    let other = DockerProvider::connect().await.expect("second provider");
    verify_stop_fences_clients(&provider, &other, "embedded").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn separate_driver_processes_fence_exec_one_shots_and_restart() {
    let provider = PluginProvider::spawn(Command::new(env!("CARGO_BIN_EXE_sandbox-driver-docker")))
        .await
        .expect("first Docker driver process");
    let other = PluginProvider::spawn(Command::new(env!("CARGO_BIN_EXE_sandbox-driver-docker")))
        .await
        .expect("second Docker driver process");
    verify_stop_fences_clients(&provider, &other, "spawned").await;
    provider.shutdown().await.expect("first driver shuts down");
    other.shutdown().await.expect("second driver shuts down");
}

async fn verify_stop_fences_clients(
    provider: &dyn SandboxProvider,
    other: &dyn SandboxProvider,
    scenario: &str,
) {
    let docker = Docker::connect_with_local_defaults().expect("Docker client");
    let mut spec = SandboxSpec::new(SandboxSource::Image {
        reference: IMAGE.to_owned(),
    })
    .name(format!("fence-services-{scenario}-{}", process::id()));
    spec.provider_config = serde_json::json!({"sidecars": [{
        "name": "service", "image": IMAGE, "entrypoint": ["sleep", "600"]
    }]});
    let sandbox = provider.create(&spec, None).await.expect("create");
    let peer = other
        .attach(sandbox.id(), None)
        .await
        .expect("independent attach");
    let exec_owner = Arc::clone(&sandbox);
    let command = tokio::spawn(async move {
        exec_owner.exec().run(&ExecSpec::new("sh").args([
            "-c", "echo ready > exec-ready; while :; do echo old >> exec-ticks; sleep 0.1; done",
        ]).timeout(Duration::from_secs(30))).await
    });
    let one_shot_owner = Arc::clone(&sandbox);
    let one_shot = tokio::spawn(async move {
        one_shot_owner
            .one_shot()
            .expect("one-shot")
            .run(
                &OneShotSpec::registry(IMAGE)
                    .args([
                        "sh",
                        "-c",
                        "while :; do echo old >> one-shot-ticks; sleep 0.1; done",
                    ])
                    .timeout(Duration::from_secs(30)),
                ExecControls::buffered(),
            )
            .await
    });
    let result = async {
        wait_for_label(&docker, "sh.sandbox-driver.one-shot", sandbox.id().as_str()).await;
        // Ensure the ordinary exec is admitted before stop; otherwise its
        // refusal would not prove the stop killed already-running execution.
        timeout(Duration::from_secs(10), async {
            loop {
                let ready = sandbox.exec().run(&ExecSpec::new("test").args(["-f", "exec-ready"])).await;
                if ready.is_ok_and(|result| result.success()) { break; }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("exec began");
        let stopper = Arc::clone(&peer);
        let stopping = tokio::spawn(async move { stopper.stop().await });
        wait_for_label(&docker, "sh.sandbox-driver.fence", sandbox.id().as_str()).await;
        assert_busy(sandbox.start().await);
        assert_busy(sandbox.exec().run(&ExecSpec::new("true")).await);
        stopping.await.expect("stop task")?;
        assert!(labeled(&docker, "sh.sandbox-driver.one-shot", sandbox.id().as_str()).await.is_empty());
        let _ = timeout(Duration::from_secs(10), command).await.expect("old exec ended").expect("exec task");
        let _ = timeout(Duration::from_secs(10), one_shot).await.expect("old one-shot ended").expect("one-shot task");
        let network = format!("{}-net", spec.name.as_deref().expect("named sandbox"));
        for service in labeled(&docker, "sh.sandbox-driver.network", &network).await {
            let inspect = docker.inspect_container(&service, None).await.expect("service inspect");
            assert_eq!(inspect.state.and_then(|state| state.running), Some(false));
        }
        peer.start().await?;
        let unchanged = peer.exec().run(&ExecSpec::new("sh").args([
            "-c", "before=$(wc -l < exec-ticks); sleep 1; test \"$before\" = \"$(wc -l < exec-ticks)\"",
        ])).await?;
        assert!(unchanged.success(), "old generation continued after restart");
        Ok::<_, Error>(())
    }.await;
    let cleanup = sandbox.delete().await;
    result.expect("fence behavior");
    cleanup.expect("cleanup sandbox");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn cancelled_drain_retains_ownership_and_never_replays_or_takes_over() {
    let provider = DockerProvider::connect().await.expect("Docker is required");
    let other = DockerProvider::connect().await.expect("second provider");
    let docker = Docker::connect_with_local_defaults().expect("Docker client");
    let name = format!("fence-cancelled-{}", process::id());
    let sandbox = provider
        .create(
            &SandboxSpec::new(SandboxSource::Image {
                reference: IMAGE.to_owned(),
            })
            .name(&name),
            None,
        )
        .await
        .expect("create");
    let peer = other
        .attach(sandbox.id(), None)
        .await
        .expect("independent attach");
    let stopper = Arc::clone(&sandbox);
    let stopping = tokio::spawn(async move { stopper.stop().await });
    wait_for_label(&docker, "sh.sandbox-driver.fence", sandbox.id().as_str()).await;
    stopping.abort();
    let _ = stopping.await;
    assert_busy(peer.start().await);
    assert_busy(peer.stop().await);
    // Test-only operator reconciliation: removal confirms the primary can no
    // longer execute. The cancelled request only stops resources; it cannot
    // recreate them. Do not infer safety from an elapsed lease timeout.
    let markers = labeled(&docker, "sh.sandbox-driver.fence", sandbox.id().as_str()).await;
    docker
        .remove_container(
            sandbox.id().as_str(),
            Some(RemoveContainerOptions {
                force: true,
                v: true,
                ..Default::default()
            }),
        )
        .await
        .expect("remove test primary");
    // The retained claim must still be found when Docker can no longer
    // resolve the original name or short ID through the primary container.
    let mut retries = Vec::new();
    for alias in [sandbox.id().as_str(), &sandbox.id().as_str()[..12], &name] {
        retries.push(
            other
                .delete(
                    &sandbox_driver::SandboxId::try_new(alias).expect("alias"),
                    None,
                )
                .await,
        );
    }
    let retained = labeled(&docker, "sh.sandbox-driver.fence", sandbox.id().as_str()).await;
    for container in markers {
        docker
            .remove_container(
                &container,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("remove test primary and markers");
    }
    sandbox.delete().await.expect("cleanup");
    assert!(
        !retained.is_empty(),
        "retry must not release retained ownership"
    );
    for retry in retries {
        assert_busy(retry);
    }
    other
        .delete(
            &sandbox_driver::SandboxId::try_new(name).expect("name"),
            None,
        )
        .await
        .expect("delete by name after reconciliation");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn a_visible_but_unready_creation_cannot_be_started_or_stopped_by_another_client() {
    let provider = DockerProvider::connect().await.expect("Docker is required");
    let other = DockerProvider::connect().await.expect("second provider");
    let docker = Docker::connect_with_local_defaults().expect("Docker client");
    let name = format!("fence-creation-{}", process::id());
    let mut spec = SandboxSpec::new(SandboxSource::Image {
        reference: IMAGE.to_owned(),
    })
    .name(&name);
    spec.provider_config = serde_json::json!({"sidecars": [{
        "name": "service", "image": IMAGE,
        "entrypoint": ["sh", "-c", "sleep 3; touch /ready; exec sleep 600"],
        "health": {"cmd": "test -e /ready", "interval_ms": 100, "retries": 100}
    }]});
    let creating = tokio::spawn(async move { provider.create(&spec, None).await });
    timeout(Duration::from_secs(10), async {
        loop {
            if docker.inspect_container(&name, None).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("primary became visible");
    let peer = other
        .attach(
            &sandbox_driver::SandboxId::try_new(name).expect("name"),
            None,
        )
        .await
        .expect("attach incomplete allocation");
    assert_busy(peer.start().await);
    assert_busy(peer.stop().await);
    assert_busy(peer.exec().run(&ExecSpec::new("true")).await);
    let sandbox = creating
        .await
        .expect("create task")
        .expect("ready creation");
    let exec = sandbox.exec().run(&ExecSpec::new("true")).await;
    let cleanup = sandbox.delete().await;
    assert!(exec.expect("exec after create").success());
    cleanup.expect("cleanup");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn a_held_lifecycle_claim_does_not_keep_a_timeout_from_stopping_a_command() {
    let provider = DockerProvider::connect().await.expect("Docker is required");
    let docker = Docker::connect_with_local_defaults().expect("Docker client");
    let spec = SandboxSpec::new(SandboxSource::Image {
        reference: IMAGE.to_owned(),
    })
    .name(format!("fence-control-{}", process::id()));
    let sandbox = Arc::new(provider.create(&spec, None).await.expect("create"));
    let runner = Arc::clone(&sandbox);
    let running = tokio::spawn(async move {
        runner
            .exec()
            .run(
                &ExecSpec::new("sleep")
                    .args(["600"])
                    .timeout(Duration::from_secs(3)),
            )
            .await
    });
    // Once the command is admitted, stand in for another client's lifecycle
    // claim, using the marker name recorded on the primary.
    sleep(Duration::from_secs(1)).await;
    let id = sandbox.id().as_str();
    let inspect = docker
        .inspect_container(id, None)
        .await
        .expect("inspect primary");
    let name = inspect
        .config
        .and_then(|config| config.labels)
        .and_then(|labels| labels.get("sh.sandbox-driver.fence-name").cloned())
        .expect("primary records its fence name");
    let marker = docker
        .create_container(
            Some(CreateContainerOptions {
                name:     name.clone(),
                platform: None,
            }),
            Config {
                image: inspect.image.clone(),
                entrypoint: Some(vec!["/bin/false".to_owned()]),
                labels: Some(HashMap::from([
                    ("sh.sandbox-driver.fence".to_owned(), id.to_owned()),
                    (
                        "sh.sandbox-driver.fence-operation".to_owned(),
                        "start".to_owned(),
                    ),
                ])),
                ..Default::default()
            },
        )
        .await
        .expect("hold the claim");
    let result = timeout(Duration::from_secs(30), running)
        .await
        .expect("timeout ends the command")
        .expect("exec task");
    docker
        .remove_container(
            &marker.id,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .expect("release the claim");
    let cleanup = sandbox.delete().await;
    assert_eq!(
        result.expect("stopped run").termination,
        Termination::TimedOut
    );
    cleanup.expect("cleanup");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn an_in_flight_creation_refuses_a_delete_by_name() {
    let provider = DockerProvider::connect().await.expect("Docker is required");
    let other = DockerProvider::connect().await.expect("second provider");
    let docker = Docker::connect_with_local_defaults().expect("Docker client");
    let name = format!("fence-creating-{}", process::id());
    let mut spec = SandboxSpec::new(SandboxSource::Image {
        reference: IMAGE.to_owned(),
    })
    .name(&name);
    spec.provider_config = serde_json::json!({"sidecars": [{
        "name": "service", "image": IMAGE,
        "entrypoint": ["sh", "-c", "sleep 3; touch /ready; exec sleep 600"],
        "health": {"cmd": "test -e /ready", "interval_ms": 100, "retries": 100}
    }]});
    let creating = tokio::spawn(async move { provider.create(&spec, None).await });
    // Whether or not the primary is visible yet, the claim is found by name.
    wait_for_label(&docker, "sh.sandbox-driver.fence", &name).await;
    assert_busy(
        other
            .delete(
                &sandbox_driver::SandboxId::try_new(name).expect("name"),
                None,
            )
            .await,
    );
    let sandbox = creating
        .await
        .expect("create task")
        .expect("ready creation");
    sandbox.delete().await.expect("cleanup");
}
