//! The Docker provider through the shared contract's catalog, against a live
//! daemon: the common conformance suite, then the optional capabilities the
//! descriptor declares (services and one-shot containers).

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::{ListContainersOptions, RemoveContainerOptions};
use bollard::network::ListNetworksOptions;
use sandbox_driver::ProviderKind;
use sandbox_driver::contract::{
    AccessScope, CatalogOptions, Durability, Environment, EnvironmentName, Error, ExecControls,
    ExecSpec, LabelFilter, OneShotImage, OneShotSpec, OutputSink, ProviderEntry, ProviderName,
    ReadinessCheck, Registry, SandboxSpec, SandboxState, Sandboxes, SecretResolver, ServiceSpec,
    Source, StaticEnvironments, Termination,
};
use sandbox_driver_conformance::contract::{Commands, Conformance};
use sandbox_driver_docker::DockerFactory;

const IMAGE: &str = "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17";
const SERVICE_IMAGE: &str = "redis:7-alpine";

struct NoSecrets;

#[async_trait]
impl SecretResolver for NoSecrets {
    async fn resolve(&self, _: &str) -> Result<Option<String>, Error> {
        Ok(None)
    }
}

/// A catalog with one `docker` entry and the given environments. Each call
/// activates its own runtime, as a separate process would.
fn catalog(environments: BTreeMap<&str, SandboxSpec>) -> Sandboxes {
    let entry: ProviderEntry =
        serde_json::from_value(serde_json::json!({"plugin": "docker"})).expect("entry");
    let provider = ProviderName::try_new("docker").expect("name");
    let environments = environments
        .into_iter()
        .map(|(name, spec)| {
            (EnvironmentName::try_new(name).expect("name"), Environment {
                provider: provider.clone(),
                spec,
            })
        })
        .collect();
    let registry = Registry::new()
        .builtin(
            ProviderKind::try_new("docker").expect("kind"),
            Arc::new(DockerFactory),
        )
        .expect("registry");
    Sandboxes::try_new(
        [(provider, entry)],
        Arc::new(StaticEnvironments(environments)),
        &registry,
        Arc::new(NoSecrets),
        CatalogOptions::default(),
    )
    .expect("catalog")
}

/// The daemon the factory connects to by default, whatever `DOCKER_HOST`
/// says, so leftover checks look where the runtime worked.
fn daemon() -> Docker {
    Docker::connect_with_unix("/var/run/docker.sock", 120, bollard::API_DEFAULT_VERSION)
        .expect("docker")
}

fn workload() -> SandboxSpec {
    SandboxSpec::new(Source::Image(IMAGE.to_owned()))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
#[expect(
    clippy::print_stderr,
    reason = "the per-check report is the test's evidence"
)]
async fn docker_passes_the_shared_contract_suite() {
    let catalog = catalog(BTreeMap::from([("default", workload())]));
    let peer = self::catalog(BTreeMap::from([("default", workload())]));
    let descriptor = catalog
        .provider("docker")
        .expect("entry")
        .describe()
        .await
        .expect("describe");
    assert_eq!(descriptor.access, AccessScope::Shared);
    assert_eq!(descriptor.durability, Durability::Machine);
    assert!(descriptor.namespace.id.as_str().starts_with("docker:"));
    let report = Conformance::new(catalog.clone(), "default", Commands {
        probe:        ExecSpec::argv(["printf", "workload\\n"]),
        probe_output: b"workload\n".to_vec(),
        long_running: ExecSpec::argv(["sh", "-c", "echo ready; exec sleep 600"]),
    })
    .with_peer(peer.clone())
    .run()
    .await;
    eprintln!("{report}");
    report.assert_pass();
    peer.shutdown().await.expect("peer shutdown");
    catalog.shutdown().await.expect("shutdown");
}

fn capture() -> (OutputSink, Arc<Mutex<Vec<u8>>>) {
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink_output = Arc::clone(&output);
    let sink: OutputSink = Arc::new(move |_, bytes| {
        sink_output.lock().expect("output").extend(bytes);
        Box::pin(async { Ok(()) })
    });
    (sink, output)
}

/// A service is ready before create returns, reachable by name from a
/// one-shot that shares the workload's workspace, stops with the sandbox,
/// and is ready again after start. Destroy leaves nothing behind.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn services_and_one_shots_share_the_workload() {
    let mut spec = workload();
    let mut redis = ServiceSpec::new(SERVICE_IMAGE);
    redis.ports = vec![NonZeroU16::new(6379).expect("port")];
    redis.ready = Some(ReadinessCheck {
        command:     vec!["redis-cli".to_owned(), "ping".to_owned()],
        interval_ms: NonZeroU64::new(250).expect("interval"),
        timeout_ms:  NonZeroU64::new(2_000).expect("timeout"),
        deadline_ms: NonZeroU64::new(60_000).expect("deadline"),
        retries:     NonZeroU32::new(100).expect("retries"),
    });
    spec.services.insert("cache".to_owned(), redis);
    let catalog = catalog(BTreeMap::from([("services", spec)]));
    let sandbox = catalog
        .builder("services")
        .await
        .expect("builder")
        .label("contract-test", "services")
        .create()
        .await
        .expect("create waits for readiness");
    let reference = sandbox.reference();
    async {
        let status = sandbox.status().await.expect("status");
        let cache = status.services.get("cache").expect("service address");
        assert_eq!(cache.host, "cache");
        assert_eq!(cache.ports.get(&6379), Some(&6379));

        // The one-shot writes into the shared workspace after reaching the
        // service by its name.
        let mut one_shot = OneShotSpec::new(OneShotImage::Image(SERVICE_IMAGE.to_owned()));
        one_shot.entrypoint = Some("sh".to_owned());
        one_shot.args = vec![
            "-c".to_owned(),
            "redis-cli -h cache ping > from-one-shot.txt".to_owned(),
        ];
        let outcome = sandbox
            .containers()
            .expect("containers capability")
            .run(one_shot, ExecControls::default())
            .await
            .expect("one-shot");
        assert_eq!(
            (outcome.termination, outcome.exit_code),
            (Termination::Exited, Some(0))
        );
        let (sink, output) = capture();
        let outcome = sandbox
            .exec(ExecSpec::argv(["cat", "from-one-shot.txt"]), ExecControls {
                sink: Some(sink),
                ..ExecControls::default()
            })
            .await
            .expect("exec");
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(output.lock().expect("output").as_slice(), b"PONG\n");

        sandbox.stop().await.expect("stop");
        let stopped = sandbox.status().await.expect("status");
        assert_eq!(stopped.state, SandboxState::Stopped);
        assert!(
            stopped.services.is_empty(),
            "stopped services are not current"
        );
        sandbox.start().await.expect("start waits for readiness");
        let restarted = sandbox.status().await.expect("status");
        assert!(restarted.services.contains_key("cache"));
    }
    .await;
    catalog.destroy(&reference).await.expect("destroy");
    assert!(matches!(
        catalog.attach(&reference).await.err(),
        Some(Error::NotFound { .. })
    ));
    let docker = daemon();
    let leftovers = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: true,
            filters: HashMap::from([("label".to_owned(), vec![format!(
                "sh.sandbox-driver.sandbox={}",
                reference.id.as_str()
            )])]),
            ..Default::default()
        }))
        .await
        .expect("list");
    assert!(leftovers.is_empty(), "services left: {leftovers:?}");
}

/// Only the caller's labels come back, though the image carries its own,
/// and a stopped sandbox refuses a command instead of reporting it killed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn labels_round_trip_and_a_stopped_sandbox_refuses_exec() {
    let catalog = catalog(BTreeMap::from([("labels", workload())]));
    let sandbox = catalog
        .builder("labels")
        .await
        .expect("builder")
        .label("contract-test", "labels")
        .create()
        .await
        .expect("create");
    let reference = sandbox.reference();
    async {
        let labels = sandbox.status().await.expect("status").labels;
        assert_eq!(
            labels.get("contract-test").map(String::as_str),
            Some("labels")
        );
        assert!(
            !labels.keys().any(|key| key.starts_with("com.docker.dhi.")),
            "image labels are not the caller's: {labels:?}"
        );
        let found = catalog
            .find(LabelFilter::eq("contract-test", "labels"))
            .await
            .expect("find");
        let [summary] = found.sandboxes.as_slice() else {
            panic!("one sandbox: {found:?}");
        };
        assert_eq!(summary.labels, labels, "list and status agree");
        let by_image_label = catalog
            .find(LabelFilter::eq("com.docker.dhi.shell", "busybox"))
            .await
            .expect("find");
        assert!(
            by_image_label
                .sandboxes
                .iter()
                .all(|found| found.reference != reference),
            "a filter does not match the image's labels"
        );

        sandbox.stop().await.expect("stop");
        let refused = sandbox
            .exec(ExecSpec::argv(["true"]), ExecControls::default())
            .await;
        assert!(refused.is_err(), "exec on a stopped sandbox: {refused:?}");
    }
    .await;
    catalog.destroy(&reference).await.expect("destroy");
}

/// Unsupported requests fail before anything is created.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn unsupported_requests_are_refused_before_admission() {
    let mut timed = workload();
    timed.timers.auto_stop_ms = Some(60_000);
    let catalog = catalog(BTreeMap::from([("timed", timed)]));
    let error = catalog
        .builder("timed")
        .await
        .expect("builder")
        .label("contract-test", "refused")
        .create()
        .await
        .err();
    assert!(
        matches!(&error, Some(Error::Unsupported { capability }) if capability == "timers.auto_stop_ms"),
        "{error:?}"
    );
    let found = catalog
        .find(LabelFilter::eq("contract-test", "refused"))
        .await
        .expect("find");
    assert!(found.sandboxes.is_empty());
}

/// A primary removed behind the provider's back leaves its services and
/// network. Destroy by ref reconciles them by the owner label instead of
/// reporting an already-absent sandbox as clean.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a live Docker daemon"]
async fn destroy_reconciles_services_of_a_vanished_primary() {
    let mut spec = workload();
    spec.services
        .insert("cache".to_owned(), ServiceSpec::new(SERVICE_IMAGE));
    let catalog = catalog(BTreeMap::from([("orphans", spec)]));
    let sandbox = catalog
        .builder("orphans")
        .await
        .expect("builder")
        .create()
        .await
        .expect("create");
    let reference = sandbox.reference();
    let docker = daemon();
    let owned = || async {
        let filter = HashMap::from([("label".to_owned(), vec![format!(
            "sh.sandbox-driver.sandbox={}",
            reference.id.as_str()
        )])]);
        let containers = docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: filter.clone(),
                ..Default::default()
            }))
            .await
            .expect("list containers");
        let networks = docker
            .list_networks(Some(ListNetworksOptions { filters: filter }))
            .await
            .expect("list networks");
        (containers.len(), networks.len())
    };
    assert_eq!(owned().await, (1, 1), "one service on one network");
    docker
        .remove_container(
            reference.id.as_str(),
            Some(RemoveContainerOptions {
                force: true,
                v: true,
                ..Default::default()
            }),
        )
        .await
        .expect("remove primary directly");
    catalog
        .destroy(&reference)
        .await
        .expect("destroy reconciles");
    assert_eq!(owned().await, (0, 0), "services and network removed");
}
