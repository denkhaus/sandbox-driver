//! The Docker provider under the shared contract (`sandbox_driver::contract`).
//!
//! [`DockerFactory`] is the built-in registration a [`Registry`] takes. It
//! activates one runtime per provider entry from that entry's `config` alone;
//! nothing is read from the ambient environment. The runtime reports the
//! daemon's engine ID as its namespace, so refs stay bound to one daemon, and
//! drives the same container plan, admission claim and stop fence as the
//! legacy provider: lifecycle changes are coordinated across every provider
//! instance on the daemon (`access: shared`), and sandboxes outlive the
//! process (`durability: machine`).
//!
//! [`Registry`]: sandbox_driver::contract::Registry

mod error;
mod io;
mod spec;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bollard::container::ListContainersOptions;
use bollard::models::ContainerInspectResponse;
use bollard::{API_DEFAULT_VERSION, Docker};
use sandbox_driver::contract::{
    AccessScope, Activation, ByteStream, CapabilitySet, ContainerField, Containers, Descriptor,
    Durability, EndpointCapability, Endpoints, Error as ContractError, ExecControls, ExecOutcome,
    ExecSpec, Expose, Exposure, FileEntry, FileRange, Files, Health, JsonSchema, LabelFilter,
    Lifecycle, LifecycleCapability, Locality, MutationOutcome, Namespace, NamespaceId,
    NamespaceScope, NetworkMode, NetworkPolicy, OneShotImage, OneShotSpec, OutputStream,
    PluginLocation, PortSupport, Ports, ProviderFactory, ProviderName, ProviderRuntime,
    ProviderSchemas, ResourceCapability, Resources, Sandbox, SandboxRef, SandboxState,
    SandboxSummary, ServiceAddress, SourceKind, Status, Termination, WorkloadAdmission,
};
use sandbox_driver::{
    self as legacy, Exec, ExecStreamingResult, Isolation, OneShot, PreviewUrls, SandboxId,
    SandboxProvider, ShellCommand, StdinSource,
};
use serde::Deserialize;
use tokio::time;

pub(crate) use self::error::to_contract;
use crate::daemon::{POSIX_SH, docker_error, docker_kind};
use crate::inspect::{caller_labels, is_primary, map_state, map_status, normalized_container_name};
use crate::provider::DockerProvider;
use crate::sandbox::DockerSandbox;
use crate::{MANAGED_LABEL, fence, sidecars};

/// Seconds a daemon request may take, as for a local-defaults connection.
const REQUEST_TIMEOUT_SECS: u64 = 120;
/// How long a finished command waits for a stop holding the claim to end
/// its container run before the command's own outcome stands.
const DRAIN_WAIT: Duration = Duration::from_secs(10);
const DRAIN_POLL: Duration = Duration::from_millis(100);
/// The daemon a provider entry reaches when its config names none.
const DEFAULT_HOST: &str = "unix:///var/run/docker.sock";

/// The built-in Docker provider factory.
#[derive(Clone, Copy, Debug, Default)]
pub struct DockerFactory;

/// The entry `config` the factory accepts.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct EntryConfig {
    host: Option<String>,
}

impl DockerFactory {
    /// The credential-free metadata of every Docker entry.
    pub fn schemas() -> ProviderSchemas {
        ProviderSchemas {
            kind:          docker_kind(),
            version:       env!("CARGO_PKG_VERSION").to_owned(),
            config_schema: schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "host": {
                        "description": "The daemon endpoint: unix:///path, tcp://host:port or http://host:port. Defaults to unix:///var/run/docker.sock.",
                        "type": "string",
                        "pattern": "^(unix|tcp|http)://.+",
                    },
                },
            })),
            spec_schema:   Some(schema(spec::spec_schema())),
            secrets:       Vec::new(),
        }
    }
}

fn schema(value: serde_json::Value) -> JsonSchema {
    serde_json::from_value(value).expect("constant schema is an object")
}

#[async_trait]
impl ProviderFactory for DockerFactory {
    async fn discover(&self, _: &PluginLocation) -> Result<ProviderSchemas, ContractError> {
        Ok(Self::schemas())
    }

    async fn activate(
        &self,
        activation: Activation,
    ) -> Result<Arc<dyn ProviderRuntime>, ContractError> {
        let invalid = |path: &str, message: String| ContractError::InvalidConfig {
            entry: activation.name.clone(),
            path: path.to_owned(),
            message,
        };
        let config: EntryConfig = serde_json::from_value(serde_json::Value::Object(
            activation.entry.config.clone().into_iter().collect(),
        ))
        .map_err(|error| invalid("config", error.to_string()))?;
        let host = config.host.as_deref().unwrap_or(DEFAULT_HOST);
        let (docker, local) = if host.starts_with("unix://") {
            let docker = Docker::connect_with_unix(host, REQUEST_TIMEOUT_SECS, API_DEFAULT_VERSION);
            (docker, true)
        } else if host.starts_with("tcp://") || host.starts_with("http://") {
            let docker = Docker::connect_with_http(host, REQUEST_TIMEOUT_SECS, API_DEFAULT_VERSION);
            (docker, false)
        } else {
            return Err(invalid(
                "config.host",
                "expected a unix://, tcp:// or http:// endpoint".to_owned(),
            ));
        };
        let docker = docker.map_err(|error| invalid("config.host", error.to_string()))?;
        Ok(Arc::new(DockerRuntime {
            entry: activation.name,
            provider: DockerProvider::from_client(docker.clone()),
            docker,
            local,
            namespace: Mutex::new(None),
        }))
    }
}

struct DockerRuntime {
    entry:     ProviderName,
    provider:  DockerProvider,
    docker:    Docker,
    /// Whether the daemon is on this machine, so host paths can be bound.
    local:     bool,
    /// The namespace `describe` last observed, which handles are bound to.
    namespace: Mutex<Option<NamespaceId>>,
}

impl DockerRuntime {
    /// The daemon's engine ID: stable across daemon restarts and different
    /// for a different daemon, whatever endpoint reaches it.
    async fn observe_namespace(&self) -> Result<NamespaceId, ContractError> {
        let info = self
            .docker
            .info()
            .await
            .map_err(|error| ContractError::Unavailable {
                message: docker_error("reading daemon identity", error).to_string(),
                outcome: MutationOutcome::NotStarted,
            })?;
        let id =
            info.id
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| ContractError::Transport {
                    message: "the daemon reported no engine ID".to_owned(),
                    outcome: MutationOutcome::NotStarted,
                })?;
        let namespace = NamespaceId::try_new(format!("docker:{id}")).map_err(|error| {
            ContractError::Transport {
                message: format!("the daemon's engine ID is not a namespace: {error}"),
                outcome: MutationOutcome::NotStarted,
            }
        })?;
        *self.namespace.lock().expect("namespace lock") = Some(namespace.clone());
        Ok(namespace)
    }

    async fn namespace(&self) -> Result<NamespaceId, ContractError> {
        let cached = self.namespace.lock().expect("namespace lock").clone();
        match cached {
            Some(namespace) => Ok(namespace),
            None => self.observe_namespace().await,
        }
    }

    fn bind(&self, namespace: NamespaceId, inner: Arc<DockerSandbox>) -> Box<dyn Sandbox> {
        Box::new(DockerContractSandbox {
            reference: SandboxRef {
                provider: self.entry.clone(),
                namespace,
                id: inner.id.clone(),
            },
            inner,
        })
    }
}

#[async_trait]
impl ProviderRuntime for DockerRuntime {
    async fn describe(&self) -> Result<Descriptor, ContractError> {
        Ok(Descriptor {
            schemas:      DockerFactory::schemas(),
            namespace:    Namespace {
                id:    self.observe_namespace().await?,
                scope: NamespaceScope::Machine,
            },
            isolation:    Isolation::Container,
            durability:   Durability::Machine,
            access:       AccessScope::Shared,
            sources:      BTreeSet::from([SourceKind::Image]),
            locality:     Locality {
                host_address:      None,
                mounts_host_paths: self.local,
            },
            resources:    BTreeSet::from([ResourceCapability::Cpu, ResourceCapability::Memory]),
            network:      BTreeSet::from([NetworkMode::AllowAll, NetworkMode::Block]),
            ports:        PortSupport::Any,
            capabilities: CapabilitySet {
                lifecycle:        BTreeSet::from([
                    LifecycleCapability::Pause,
                    LifecycleCapability::Resume,
                ]),
                endpoints:        BTreeSet::from([EndpointCapability::ShellCommand]),
                services:         true,
                containers:       true,
                container_fields: BTreeSet::from([
                    ContainerField::Init,
                    ContainerField::Privileged,
                    ContainerField::CapabilitiesAdd,
                    ContainerField::CapabilitiesDrop,
                    ContainerField::Dns,
                    ContainerField::ExtraHosts,
                    ContainerField::ShmSizeMb,
                ]),
                timers:           BTreeSet::new(),
            },
        })
    }

    async fn health(&self) -> Result<Health, ContractError> {
        let health = SandboxProvider::health(&self.provider)
            .await
            .map_err(to_contract)?;
        Ok(Health {
            healthy: health.status == legacy::HealthStatus::Ok,
            message: health.message,
        })
    }

    async fn create(
        &self,
        admission: WorkloadAdmission,
    ) -> Result<Box<dyn Sandbox>, ContractError> {
        let plan = spec::plan(&admission.spec, &admission.credentials, self.local)?;
        // Read before creating, so a running sandbox is never reported as
        // not started because the namespace could not be read afterwards.
        let namespace = self.namespace().await?;
        let inner = self
            .provider
            .create_handle(Box::new(plan), admission.spec.name.clone(), None)
            .await
            .map_err(to_contract)?;
        Ok(self.bind(namespace, inner))
    }

    async fn attach(&self, id: &SandboxId) -> Result<Box<dyn Sandbox>, ContractError> {
        let inner = self
            .provider
            .attach_handle(id, None)
            .await
            .map_err(to_contract)?;
        Ok(self.bind(self.namespace().await?, inner))
    }

    /// Every sandbox on the daemon whose labels match. Labels are compared
    /// here rather than by the daemon, so any value round-trips exactly.
    async fn list(&self, filter: &LabelFilter) -> Result<Vec<SandboxSummary>, ContractError> {
        let namespace = self.namespace().await?;
        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: HashMap::from([("label".to_owned(), vec![format!(
                    "{MANAGED_LABEL}=true"
                )])]),
                ..Default::default()
            }))
            .await
            .map_err(|error| to_contract(docker_error("listing containers", error)))?;
        let mut summaries = Vec::new();
        for container in containers {
            let all_labels = container.labels.unwrap_or_default();
            if !is_primary(&all_labels) {
                continue;
            }
            let labels = caller_labels(&all_labels);
            if !filter.matches(&labels) {
                continue;
            }
            let id = container.id.ok_or_else(|| ContractError::Transport {
                message: "the daemon listed a container without an ID".to_owned(),
                outcome: MutationOutcome::NotStarted,
            })?;
            summaries.push(SandboxSummary {
                reference: SandboxRef {
                    provider:  self.entry.clone(),
                    namespace: namespace.clone(),
                    id:        SandboxId::try_new(id).map_err(|error| {
                        ContractError::Transport {
                            message: error.to_string(),
                            outcome: MutationOutcome::NotStarted,
                        }
                    })?,
                },
                name: normalized_container_name(
                    container
                        .names
                        .as_ref()
                        .and_then(|names| names.first())
                        .map(String::as_str),
                ),
                state: listed_state(container.state.as_deref()),
                labels,
            });
        }
        Ok(summaries)
    }

    async fn destroy(&self, id: &SandboxId) -> Result<(), ContractError> {
        self.provider.delete(id, None).await.map_err(to_contract)
    }

    /// Nothing runs between calls: sandboxes stay on the daemon, which is
    /// what `durability: machine` promises.
    async fn shutdown(&self) -> Result<(), ContractError> {
        Ok(())
    }
}

/// A listed container's state, through the same table as `status`.
fn listed_state(state: Option<&str>) -> SandboxState {
    contract_state(map_status(state.and_then(|state| state.parse().ok())))
}

/// When the inspected container's current run started, or `None` when it is
/// not running. Every process in the workload belongs to one run.
fn current_run(inspect: &ContainerInspectResponse) -> Option<&str> {
    inspect
        .state
        .as_ref()
        .filter(|state| state.running == Some(true))
        .and_then(|state| state.started_at.as_deref())
}

fn contract_state(state: legacy::SandboxState) -> SandboxState {
    match state {
        legacy::SandboxState::Running => SandboxState::Running,
        legacy::SandboxState::Stopped => SandboxState::Stopped,
        legacy::SandboxState::Paused => SandboxState::Paused,
        legacy::SandboxState::Starting => SandboxState::Starting,
        legacy::SandboxState::Deleting => SandboxState::Deleting,
        legacy::SandboxState::Error => SandboxState::Unhealthy,
        _ => SandboxState::Unknown,
    }
}

/// A Docker sandbox through the shared contract.
struct DockerContractSandbox {
    reference: SandboxRef,
    inner:     Arc<DockerSandbox>,
}

impl DockerContractSandbox {
    /// When the primary container's current run started, or `None` when it
    /// is not running. Every process in the workload belongs to one run.
    async fn run_started(&self) -> Result<Option<String>, ContractError> {
        let inspect = self.inner.container.inspect().await.map_err(to_contract)?;
        Ok(current_run(&inspect).map(str::to_owned))
    }

    /// Whether the container run a command was admitted into is over, which
    /// ended every process in it. A stop that holds the claim may still be
    /// killing that run while Docker reports it live, so wait (bounded) for
    /// the stop to land before deciding; an unfinished stop is no evidence.
    async fn run_ended(&self, admitted: &str) -> Result<bool, ContractError> {
        let deadline = Instant::now() + DRAIN_WAIT;
        loop {
            let inspect = self.inner.container.inspect().await.map_err(to_contract)?;
            if current_run(&inspect) != Some(admitted) {
                return Ok(true);
            }
            let draining = fence::draining(&self.inner.container.docker, &inspect)
                .await
                .map_err(to_contract)?;
            if !draining || Instant::now() >= deadline {
                return Ok(false);
            }
            time::sleep(DRAIN_POLL).await;
        }
    }

    /// Maps a finished command. One whose stream ended because a stop ended
    /// its container run was killed by that stop, not lost or exited; an
    /// exit code is kept unless that is established. With no run observed
    /// at admission, nothing is reclassified.
    async fn outcome(
        &self,
        result: ExecStreamingResult,
        admitted: Option<&str>,
    ) -> Result<ExecOutcome, ContractError> {
        let exec = result.result;
        let mut termination = match exec.termination {
            legacy::Termination::Exited => Termination::Exited,
            legacy::Termination::TimedOut => Termination::TimedOut,
            legacy::Termination::Cancelled => Termination::Terminated,
            legacy::Termination::Killed => Termination::Killed,
            _ => Termination::Lost,
        };
        let unexplained = match termination {
            Termination::Lost => true,
            Termination::Exited => exec.exit_code.is_none_or(|code| code >= 128),
            _ => false,
        };
        if let Some(admitted) = admitted.filter(|_| unexplained) {
            if self.run_ended(admitted).await? {
                termination = Termination::Killed;
            }
        }
        Ok(ExecOutcome {
            termination,
            exit_code: (termination == Termination::Exited)
                .then_some(exec.exit_code)
                .flatten(),
            signal: exec.signal,
            output_loss: result.output_loss,
        })
    }

    /// Runs `run` with the contract's controls as legacy controls. A sink
    /// failure stops the command and is returned as the call's error.
    async fn streamed<F, Fut>(
        &self,
        controls: ExecControls,
        run: F,
    ) -> Result<ExecOutcome, ContractError>
    where
        F: FnOnce(legacy::ExecControls) -> Fut,
        Fut: future::Future<Output = legacy::Result<ExecStreamingResult>>,
    {
        let sink_failure = Arc::new(Mutex::new(None));
        let sink = controls.sink.map(|sink| {
            let failure = Arc::clone(&sink_failure);
            Arc::new(move |stream: legacy::OutputStream, bytes: Vec<u8>| {
                let stream = match stream {
                    legacy::OutputStream::Stdout => OutputStream::Stdout,
                    legacy::OutputStream::Stderr => OutputStream::Stderr,
                };
                let sent = sink(stream, bytes);
                let failure = Arc::clone(&failure);
                Box::pin(async move {
                    sent.await.map_err(|error| {
                        let message = error.to_string();
                        *failure.lock().expect("sink failure lock") = Some(error);
                        legacy::Error::Transport(legacy::TransportError::new(message))
                    })
                })
                    as Pin<Box<dyn future::Future<Output = legacy::Result<()>> + Send>>
            }) as legacy::OutputSink
        });
        let legacy_controls = legacy::ExecControls {
            term: controls.term,
            kill: controls.kill,
            stdin: controls.stdin.map(StdinSource::new),
            sink,
            ..legacy::ExecControls::default()
        };
        let admitted = self.run_started().await?;
        let result = run(legacy_controls).await;
        if let Some(error) = sink_failure.lock().expect("sink failure lock").take() {
            return Err(error);
        }
        let admitted = admitted.as_deref();
        let error = match result {
            Ok(result) => return self.outcome(result, admitted).await,
            Err(error) => error,
        };
        // An output stream that broke because a stop ended the run the
        // command was admitted into: the command ended with it. A definite
        // refusal (a busy claim, a container Docker would not exec in) means
        // the command was never admitted, so it keeps its refusal.
        let error = to_contract(error);
        let uncertain = matches!(error, ContractError::Transport { .. });
        if let Some(admitted) = admitted.filter(|_| uncertain) {
            if self.run_ended(admitted).await.unwrap_or(false) {
                tracing::debug!(error = %error, "command ended with its stopped sandbox");
                return Ok(ExecOutcome {
                    termination: Termination::Killed,
                    exit_code:   None,
                    signal:      None,
                    output_loss: legacy::OutputLoss::default(),
                });
            }
        }
        Err(error)
    }
}

#[async_trait]
impl Sandbox for DockerContractSandbox {
    fn reference(&self) -> SandboxRef {
        self.reference.clone()
    }

    fn working_directory(&self) -> &Path {
        Path::new(&self.inner.container.working_dir)
    }

    fn env(&self) -> &BTreeMap<String, String> {
        self.inner.exec.base_env()
    }

    async fn status(&self) -> Result<Status, ContractError> {
        let inspect = self.inner.container.inspect().await.map_err(to_contract)?;
        let host = inspect.host_config.as_ref();
        let resources = Resources {
            cpu_cores: host
                .and_then(|host| host.cpu_quota)
                .and_then(|quota| u64::try_from(quota).ok())
                .and_then(|quota| u32::try_from(quota.div_ceil(100_000)).ok())
                .and_then(NonZeroU32::new),
            memory_mb: host
                .and_then(|host| host.memory)
                .and_then(|bytes| u64::try_from(bytes).ok())
                .map(|bytes| bytes.div_ceil(1024 * 1024))
                .and_then(NonZeroU64::new),
            disk_mb:   None,
            gpus:      None,
        };
        let network = match host.and_then(|host| host.network_mode.as_deref()) {
            Some("none") if self.inner.network.is_none() => Some(NetworkPolicy::Block),
            Some("bridge") => Some(NetworkPolicy::AllowAll),
            Some("default") | None if self.inner.network.is_none() => {
                Some(NetworkPolicy::ProviderDefault)
            }
            _ => None,
        };
        let state = contract_state(map_state(&inspect));
        let mut services = BTreeMap::new();
        if let (Some(network), SandboxState::Running) = (&self.inner.network, state) {
            let running = sidecars::running(&self.inner.container.docker, network)
                .await
                .map_err(to_contract)?;
            for (name, ports) in running {
                services.insert(name.clone(), ServiceAddress {
                    host:  name,
                    ports: ports.into_iter().map(|port| (port, port)).collect(),
                });
            }
        }
        Ok(Status {
            reference: self.reference.clone(),
            state,
            name: self.inner.name.clone(),
            labels: self.inner.labels.clone(),
            image: inspect
                .config
                .as_ref()
                .and_then(|config| config.image.clone()),
            snapshot: None,
            resources,
            network,
            services,
            web_url: None,
        })
    }

    async fn start(&self) -> Result<(), ContractError> {
        legacy::Sandbox::start(self.inner.as_ref())
            .await
            .map_err(to_contract)
    }

    async fn stop(&self) -> Result<(), ContractError> {
        legacy::Sandbox::stop(self.inner.as_ref())
            .await
            .map_err(to_contract)
    }

    async fn exec(
        &self,
        spec: ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecOutcome, ContractError> {
        spec.validate()?;
        if spec.tty {
            return Err(ContractError::Unsupported {
                capability: "exec.tty".to_owned(),
            });
        }
        let (program, args) = spec.argv.split_first().expect("validated argv");
        let mut legacy_spec = legacy::ExecSpec::new(program.as_str())
            .args(args.iter().cloned())
            .no_timeout();
        if let Some(ms) = spec.timeout_ms {
            legacy_spec = legacy_spec.timeout(Duration::from_millis(ms));
        }
        if let Some(directory) = &spec.working_directory {
            legacy_spec = legacy_spec.working_dir(directory.as_str());
        }
        for (key, value) in &spec.env {
            legacy_spec = legacy_spec.env_var(key.as_str(), value.as_str());
        }
        let exec = Arc::clone(&self.inner.exec);
        self.streamed(controls, |controls| async move {
            exec.run_streaming(&legacy_spec, controls).await
        })
        .await
    }

    fn files(&self) -> &dyn Files {
        self
    }

    fn ports(&self) -> &dyn Ports {
        self
    }

    fn lifecycle(&self) -> Option<&dyn Lifecycle> {
        Some(self)
    }

    fn endpoints(&self) -> Option<&dyn Endpoints> {
        Some(self)
    }

    fn containers(&self) -> Option<&dyn Containers> {
        self.inner
            .one_shot
            .as_ref()
            .map(|_| self as &dyn Containers)
    }
}

#[async_trait]
impl Files for DockerContractSandbox {
    async fn get(&self, path: &str, range: Option<FileRange>) -> Result<ByteStream, ContractError> {
        let (offset, length) = range.map_or((0, None), |range| (range.offset, range.length));
        io::read(Arc::clone(&self.inner), path.to_owned(), offset, length).await
    }

    async fn put(&self, path: &str, bytes: ByteStream) -> Result<(), ContractError> {
        io::write(&self.inner.fs, path, bytes).await
    }

    /// Lists over POSIX `sh` and `find`, so it needs no `bash`: the paths
    /// travel as arguments and come back NUL-separated, so no name is
    /// misread.
    async fn list(&self, path: &str, depth: u32) -> Result<Vec<FileEntry>, ContractError> {
        let root = self.inner.container.resolve(path);
        let spec = legacy::ExecSpec::new(POSIX_SH)
            .args([
                "-c".to_owned(),
                LIST_SCRIPT.to_owned(),
                "sandbox-driver".to_owned(),
                root.clone(),
                depth.saturating_add(1).to_string(),
            ])
            .timeout(LIST_TIMEOUT);
        let result = self.inner.exec.run(&spec).await.map_err(to_contract)?;
        if result.exit_code == Some(LIST_MISSING) {
            return Err(ContractError::NotFound {
                resource: "directory".to_owned(),
                id:       root,
            });
        }
        if !result.success() {
            return Err(ContractError::Unhealthy {
                message: format!(
                    "listing {root} failed: {}",
                    String::from_utf8_lossy(&result.stderr).trim()
                ),
            });
        }
        parse_listing(&root, &result.stdout)
    }
}

/// `$1` is the directory, `$2` the `find` depth. Each entry is printed as
/// `kind size ./relative-path` and a NUL. Only a path that does not exist
/// exits 3; one that cannot be entered or read fails the listing.
const LIST_SCRIPT: &str = r#"[ -e "$1" ] || [ -L "$1" ] || exit 3
cd -- "$1" || exit 4
find . -mindepth 1 -maxdepth "$2" -exec sh -c '
for p do
  if [ -L "$p" ]; then k=l; n=0
  elif [ -d "$p" ]; then k=d; n=0
  elif [ -f "$p" ]; then k=f; n=$(wc -c < "$p") || exit 1; n=$((n))
  else k=o; n=0
  fi
  printf "%s %s %s\0" "$k" "$n" "$p"
done' sh {} +"#;
/// The exit code of [`LIST_SCRIPT`] when the directory does not exist.
const LIST_MISSING: i32 = 3;
const LIST_TIMEOUT: Duration = Duration::from_secs(60);

fn parse_listing(root: &str, output: &[u8]) -> Result<Vec<FileEntry>, ContractError> {
    let malformed = || ContractError::Transport {
        message: "the directory listing was malformed".to_owned(),
        outcome: MutationOutcome::NotStarted,
    };
    let mut entries = Vec::new();
    for record in output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let record = String::from_utf8_lossy(record);
        let mut fields = record.splitn(3, ' ');
        let (Some(kind), Some(size), Some(relative)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(malformed());
        };
        let kind = match kind {
            "f" => legacy::FileKind::File,
            "d" => legacy::FileKind::Directory,
            "l" => legacy::FileKind::Symlink,
            _ => legacy::FileKind::Other,
        };
        entries.push(FileEntry {
            path: format!(
                "{}/{}",
                root.trim_end_matches('/'),
                relative.trim_start_matches("./")
            ),
            kind,
            size: size.parse().map_err(|_| malformed())?,
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

#[async_trait]
impl Ports for DockerContractSandbox {
    async fn expose(&self, port: NonZeroU16, options: Expose) -> Result<Exposure, ContractError> {
        if options.signed {
            return Err(ContractError::Unsupported {
                capability: "ports.signed".to_owned(),
            });
        }
        if options.ttl_ms.is_some() {
            return Err(ContractError::Unsupported {
                capability: "ports.ttl".to_owned(),
            });
        }
        let preview = self
            .inner
            .forwards
            .preview_url(port.get())
            .await
            .map_err(to_contract)?;
        Ok(Exposure {
            url:           preview.url,
            headers:       preview.headers,
            expires_at_ms: None,
        })
    }

    async fn release(&self, port: NonZeroU16) -> Result<(), ContractError> {
        self.inner
            .forwards
            .release_preview_url(port.get())
            .await
            .map_err(to_contract)
    }
}

#[async_trait]
impl Lifecycle for DockerContractSandbox {
    async fn apply(&self, action: LifecycleCapability) -> Result<(), ContractError> {
        let capability = match action {
            LifecycleCapability::Pause => {
                return legacy::Sandbox::pause(self.inner.as_ref())
                    .await
                    .map_err(to_contract);
            }
            LifecycleCapability::Resume => {
                return legacy::Sandbox::resume(self.inner.as_ref())
                    .await
                    .map_err(to_contract);
            }
            LifecycleCapability::Archive => "lifecycle.archive",
            LifecycleCapability::Recover => "lifecycle.recover",
        };
        Err(ContractError::Unsupported {
            capability: capability.to_owned(),
        })
    }
}

#[async_trait]
impl Endpoints for DockerContractSandbox {
    async fn ssh(&self, _: u64) -> Result<legacy::SshAccessInfo, ContractError> {
        Err(ContractError::Unsupported {
            capability: "endpoints.ssh".to_owned(),
        })
    }

    async fn ssh_revoke(&self, _: &str) -> Result<(), ContractError> {
        Err(ContractError::Unsupported {
            capability: "endpoints.ssh".to_owned(),
        })
    }

    async fn shell_command(&self) -> Result<String, ContractError> {
        self.inner
            .shell_command
            .shell_command()
            .await
            .map_err(to_contract)
    }

    async fn vnc(&self) -> Result<legacy::VncConnection, ContractError> {
        Err(ContractError::Unsupported {
            capability: "endpoints.vnc".to_owned(),
        })
    }
}

#[async_trait]
impl Containers for DockerContractSandbox {
    async fn run(
        &self,
        spec: OneShotSpec,
        controls: ExecControls,
    ) -> Result<ExecOutcome, ContractError> {
        spec.validate()?;
        let image = match spec.image {
            OneShotImage::Image(reference) => legacy::OneShotImage::Registry { reference },
            OneShotImage::Build {
                context,
                dockerfile,
                tag,
                reuse,
            } => legacy::OneShotImage::Build {
                context,
                dockerfile,
                tag,
                reuse,
            },
        };
        let mut legacy_spec = legacy::OneShotSpec::new(image);
        legacy_spec.entrypoint = spec.entrypoint;
        legacy_spec.args = spec.args;
        legacy_spec.env = spec.env;
        legacy_spec.working_dir = spec.working_directory;
        legacy_spec.timeout = spec.timeout_ms.map(Duration::from_millis);
        let one_shot = self
            .inner
            .one_shot
            .as_ref()
            .expect("containers() offers one-shots only with a workspace");
        self.streamed(controls, |controls| async move {
            one_shot.run(&legacy_spec, controls).await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listings_keep_names_with_spaces_and_newlines() {
        let output = b"d 0 ./dir\0f 12 ./dir/a file\0f 3 ./line\nbreak\0l 0 ./link\0o 0 ./fifo\0";
        let entries = parse_listing("/work/", output).expect("listing");
        let found: Vec<(&str, legacy::FileKind, u64)> = entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.kind, entry.size))
            .collect();
        assert_eq!(found, vec![
            ("/work/dir", legacy::FileKind::Directory, 0),
            ("/work/dir/a file", legacy::FileKind::File, 12),
            ("/work/fifo", legacy::FileKind::Other, 0),
            ("/work/line\nbreak", legacy::FileKind::File, 3),
            ("/work/link", legacy::FileKind::Symlink, 0),
        ]);
        assert!(parse_listing("/work", b"").expect("empty").is_empty());
    }

    #[test]
    fn a_malformed_listing_is_refused() {
        for output in [&b"f ./missing-size\0"[..], b"f big ./file\0"] {
            assert!(matches!(
                parse_listing("/work", output),
                Err(ContractError::Transport { .. })
            ));
        }
    }
}
