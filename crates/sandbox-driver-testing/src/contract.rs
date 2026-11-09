//! Deterministic in-memory provider for caller tests. No commands run on the
//! host. Exec responses are explicitly scripted; an unscripted command fails.
use std::collections::{BTreeMap, BTreeSet};
use std::future;
use std::io::Cursor;
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sandbox_driver::contract::{
    AccessScope, Activation, ByteStream, CapabilitySet, CatalogOptions, Descriptor, Durability,
    Environment, EnvironmentName, Error, ExecControls, ExecOutcome, ExecSpec, Expose, Exposure,
    FileEntry, FileRange, Files, Health, JsonSchema, LabelFilter, Locality, MutationOutcome,
    Namespace, NamespaceId, NamespaceScope, NetworkMode, OutputStream, PluginLocation, PortSupport,
    Ports, ProviderEntry, ProviderFactory, ProviderName, ProviderRuntime, ProviderSchemas,
    Registry, Sandbox, SandboxRef, SandboxSpec, SandboxState, SandboxSummary, Sandboxes,
    SecretResolver, Source, SourceKind, StaticEnvironments, Status, Termination, WorkloadAdmission,
};
use sandbox_driver::{FileKind, Isolation, OutputLoss, ProviderKind, SandboxId};
use tokio::io::{self, AsyncReadExt};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::fs;

static NEXT_NAMESPACE: AtomicU64 = AtomicU64::new(1);

/// A scripted command. Output is delivered through the real sink. Pending
/// commands finish only on cancellation, timeout or sandbox stop/destroy.
#[derive(Clone, Debug, Default)]
pub struct FakeExec {
    pub stdout:        Vec<u8>,
    pub stderr:        Vec<u8>,
    pub exit_code:     i32,
    pub wait_for_stop: bool,
}
/// Observable calls contain IDs/operation names, never runtime secret values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeCall {
    pub operation: String,
    pub id:        Option<SandboxId>,
}

/// One fake backend. Clones share state; each factory activation remains bound
/// to its entry. It declares process durability and no optional capabilities.
#[derive(Clone)]
pub struct FakeProvider {
    inner: Arc<FakeState>,
}
struct FakeState {
    descriptor: Descriptor,
    next_id:    AtomicU64,
    records:    Mutex<BTreeMap<SandboxId, Arc<Record>>>,
    calls:      Mutex<Vec<FakeCall>>,
    scripts:    Mutex<BTreeMap<Vec<String>, FakeExec>>,
}
struct Runtime {
    fake:   FakeProvider,
    entry:  ProviderName,
    closed: CancellationToken,
}
struct Record {
    reference: SandboxRef,
    spec:      SandboxSpec,
    directory: PathBuf,
    state:     Mutex<RunningState>,
    fence:     RwLock<()>,
    files:     Mutex<BTreeMap<String, Vec<u8>>>,
}
struct RunningState {
    state: SandboxState,
    stop:  CancellationToken,
}
struct Handle {
    record: Arc<Record>,
    fake:   FakeProvider,
    closed: CancellationToken,
}

impl Default for FakeProvider {
    fn default() -> Self {
        Self::new()
    }
}
impl FakeProvider {
    pub fn new() -> Self {
        let namespace = NamespaceId::try_new(format!(
            "fake:{}",
            NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed)
        ))
        .expect("generated namespace");
        Self {
            inner: Arc::new(FakeState {
                descriptor: Descriptor {
                    schemas:      Self::schemas(),
                    namespace:    Namespace {
                        id:    namespace,
                        scope: NamespaceScope::Process,
                    },
                    isolation:    Isolation::None,
                    durability:   Durability::Process,
                    access:       AccessScope::Shared,
                    sources:      [SourceKind::Image].into(),
                    locality:     Locality::default(),
                    resources:    BTreeSet::default(),
                    network:      [NetworkMode::ProviderDefault].into(),
                    ports:        PortSupport::None,
                    capabilities: CapabilitySet::default(),
                },
                next_id:    AtomicU64::new(1),
                records:    Mutex::new(BTreeMap::new()),
                calls:      Mutex::new(Vec::new()),
                scripts:    Mutex::new(BTreeMap::new()),
            }),
        }
    }
    pub async fn script(
        &self,
        argv: impl IntoIterator<Item = impl Into<String>>,
        response: FakeExec,
    ) {
        self.inner
            .scripts
            .lock()
            .await
            .insert(argv.into_iter().map(Into::into).collect(), response);
    }
    pub async fn calls(&self) -> Vec<FakeCall> {
        self.inner.calls.lock().await.clone()
    }
    pub fn schemas() -> ProviderSchemas {
        let closed: JsonSchema = serde_json::from_value(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": false,
        }))
        .expect("constant schema");
        ProviderSchemas {
            kind:          ProviderKind::try_new("fake").expect("constant kind"),
            version:       "1".into(),
            config_schema: closed.clone(),
            spec_schema:   Some(closed),
            secrets:       Vec::new(),
        }
    }
    pub fn descriptor(&self) -> Descriptor {
        self.inner.descriptor.clone()
    }
    async fn record(&self, operation: &str, id: Option<&SandboxId>) {
        self.inner.calls.lock().await.push(FakeCall {
            operation: operation.into(),
            id:        id.cloned(),
        });
    }
    /// Creates a catalog with `fake` provider and `default` environment. These
    /// names are test fixtures only; production catalogs have no seeded
    /// entries.
    pub fn sandboxes(&self) -> Sandboxes {
        struct NoSecrets;
        #[async_trait]
        impl SecretResolver for NoSecrets {
            async fn resolve(&self, _: &str) -> Result<Option<String>, Error> {
                Ok(None)
            }
        }
        let name = ProviderName::try_new("fake").expect("constant name");
        let entry: ProviderEntry =
            serde_json::from_value(serde_json::json!({"plugin":"fake"})).expect("constant entry");
        let environment = Environment {
            provider: name.clone(),
            spec:     SandboxSpec::new(Source::Image("fake:workload".into())),
        };
        let registry = Registry::new()
            .builtin(entry.plugin.clone(), Arc::new(self.clone()))
            .expect("unique fake registration");
        Sandboxes::try_new(
            [(name, entry)],
            Arc::new(StaticEnvironments(BTreeMap::from([(
                EnvironmentName::try_new("default").expect("constant name"),
                environment,
            )]))),
            &registry,
            Arc::new(NoSecrets),
            CatalogOptions {
                environment: BTreeMap::new(),
                ..CatalogOptions::default()
            },
        )
        .expect("valid fake catalog")
    }
}
/// Creates a catalog with a fresh in-memory fake and the `default` environment.
pub fn fake() -> Sandboxes {
    FakeProvider::new().sandboxes()
}

#[async_trait]
impl ProviderFactory for FakeProvider {
    async fn discover(&self, _: &PluginLocation) -> Result<ProviderSchemas, Error> {
        self.record("discover", None).await;
        Ok(Self::schemas())
    }
    async fn activate(&self, activation: Activation) -> Result<Arc<dyn ProviderRuntime>, Error> {
        self.record("activate", None).await;
        Ok(Arc::new(Runtime {
            fake:   self.clone(),
            entry:  activation.name,
            closed: CancellationToken::new(),
        }))
    }
}
impl Runtime {
    fn bind(&self, record: Arc<Record>) -> Handle {
        Handle {
            record,
            fake: self.fake.clone(),
            closed: self.closed.clone(),
        }
    }
    async fn handle(&self, id: &SandboxId) -> Result<Handle, Error> {
        ensure_open(&self.closed)?;
        let record = self
            .fake
            .inner
            .records
            .lock()
            .await
            .get(id)
            .filter(|record| record.reference.provider == self.entry)
            .cloned()
            .ok_or_else(|| absent(id))?;
        Ok(self.bind(record))
    }
}
#[async_trait]
impl ProviderRuntime for Runtime {
    async fn describe(&self) -> Result<Descriptor, Error> {
        ensure_open(&self.closed)?;
        self.fake.record("describe", None).await;
        Ok(self.fake.descriptor())
    }
    async fn health(&self) -> Result<Health, Error> {
        ensure_open(&self.closed)?;
        self.fake.record("health", None).await;
        Ok(Health {
            healthy: true,
            message: None,
        })
    }
    async fn create(&self, admission: WorkloadAdmission) -> Result<Box<dyn Sandbox>, Error> {
        ensure_open(&self.closed)?;
        admission.spec.validate_for(&self.fake.inner.descriptor)?;
        if !admission.spec.provider_config.is_empty() {
            return Err(Error::InvalidSpec {
                path:    "provider_config".into(),
                message: "fake accepts no options".into(),
            });
        }
        let mut records = self.fake.inner.records.lock().await;
        if let Some(name) = &admission.spec.name {
            if let Some(record) = records.values().find(|record| {
                record.reference.provider == self.entry && record.spec.name.as_ref() == Some(name)
            }) {
                return Err(Error::AlreadyExists {
                    name: name.clone(),
                    id:   record.reference.id.to_string(),
                });
            }
        }
        let id = SandboxId::try_new(format!(
            "sandbox-{}",
            self.fake.inner.next_id.fetch_add(1, Ordering::Relaxed)
        ))
        .expect("generated id");
        let directory = PathBuf::from(
            admission
                .spec
                .working_directory
                .as_deref()
                .unwrap_or("/workspace"),
        );
        let record = Arc::new(Record {
            reference: SandboxRef {
                provider:  self.entry.clone(),
                namespace: self.fake.inner.descriptor.namespace.id.clone(),
                id:        id.clone(),
            },
            spec: admission.spec,
            directory,
            state: Mutex::new(RunningState {
                state: SandboxState::Running,
                stop:  CancellationToken::new(),
            }),
            fence: RwLock::new(()),
            files: Mutex::new(BTreeMap::new()),
        });
        records.insert(id.clone(), Arc::clone(&record));
        drop(records);
        self.fake.record("create", Some(&id)).await;
        Ok(Box::new(self.bind(record)))
    }
    async fn attach(&self, id: &SandboxId) -> Result<Box<dyn Sandbox>, Error> {
        let handle = self.handle(id).await?;
        self.fake.record("attach", Some(id)).await;
        Ok(Box::new(handle))
    }
    async fn list(&self, filter: &LabelFilter) -> Result<Vec<SandboxSummary>, Error> {
        ensure_open(&self.closed)?;
        self.fake.record("list", None).await;
        let records: Vec<_> = self
            .fake
            .inner
            .records
            .lock()
            .await
            .values()
            .filter(|record| {
                record.reference.provider == self.entry && filter.matches(&record.spec.labels)
            })
            .cloned()
            .collect();
        let mut summaries = Vec::new();
        for record in records {
            summaries.push(SandboxSummary {
                reference: record.reference.clone(),
                name:      record.spec.name.clone(),
                state:     record.state.lock().await.state,
                labels:    record.spec.labels.clone(),
            });
        }
        Ok(summaries)
    }
    async fn destroy(&self, id: &SandboxId) -> Result<(), Error> {
        let handle = match self.handle(id).await {
            Ok(handle) => handle,
            Err(Error::NotFound { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        handle.destroy().await;
        self.fake.inner.records.lock().await.remove(id);
        self.fake.record("destroy", Some(id)).await;
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), Error> {
        // Each activation owns its handles; closed cancels all its live execs.
        self.closed.cancel();
        self.fake.record("shutdown", None).await;
        Ok(())
    }
}

impl Handle {
    fn path(&self, path: &str) -> Result<String, Error> {
        if path.is_empty() || path.contains('\0') {
            return Err(Error::InvalidSpec {
                path:    "path".into(),
                message: "requires a non-empty NUL-free workload path".into(),
            });
        }
        Ok(fs::resolve(&self.record.directory.to_string_lossy(), path))
    }
    async fn log(&self, operation: &str) {
        self.fake
            .record(operation, Some(&self.record.reference.id))
            .await;
    }
    /// Admits work inside the stop fence. The guard holds stop/destroy off
    /// until the work ends; the token fires when either begins.
    async fn admit(&self) -> Result<(RwLockReadGuard<'_, ()>, CancellationToken), Error> {
        let fence = self.record.fence.read().await;
        let state = self.record.state.lock().await;
        if state.state != SandboxState::Running {
            return Err(busy());
        }
        Ok((fence, state.stop.clone()))
    }
    async fn destroy(&self) {
        {
            let mut state = self.record.state.lock().await;
            if state.state == SandboxState::Deleted {
                return;
            }
            state.state = SandboxState::Deleting;
            state.stop.cancel();
        }
        let _fence = self.record.fence.write().await;
        let mut state = self.record.state.lock().await;
        self.record.files.lock().await.clear();
        state.state = SandboxState::Deleted;
    }
}
#[async_trait]
impl Sandbox for Handle {
    fn reference(&self) -> SandboxRef {
        self.record.reference.clone()
    }
    fn working_directory(&self) -> &Path {
        &self.record.directory
    }
    fn env(&self) -> &BTreeMap<String, String> {
        &self.record.spec.env
    }
    async fn status(&self) -> Result<Status, Error> {
        ensure_open(&self.closed)?;
        self.log("status").await;
        let state = self.record.state.lock().await.state;
        Ok(Status {
            reference: self.reference(),
            state,
            name: self.record.spec.name.clone(),
            labels: self.record.spec.labels.clone(),
            image: match &self.record.spec.source {
                Source::Image(image) => Some(image.clone()),
                _ => None,
            },
            snapshot: None,
            resources: self.record.spec.resources.clone(),
            network: Some(self.record.spec.network.clone()),
            services: BTreeMap::new(),
            web_url: None,
        })
    }
    async fn start(&self) -> Result<(), Error> {
        ensure_open(&self.closed)?;
        let _fence = self.record.fence.try_write().map_err(|_| busy())?;
        let mut state = self.record.state.lock().await;
        match state.state {
            SandboxState::Stopping | SandboxState::Deleting => return Err(busy()),
            SandboxState::Deleted => return Err(absent(&self.record.reference.id)),
            SandboxState::Running => return Ok(()),
            _ => {}
        }
        state.stop = CancellationToken::new();
        state.state = SandboxState::Running;
        self.log("start").await;
        Ok(())
    }
    async fn stop(&self) -> Result<(), Error> {
        ensure_open(&self.closed)?;
        {
            let mut state = self.record.state.lock().await;
            match state.state {
                SandboxState::Deleted => return Err(absent(&self.record.reference.id)),
                SandboxState::Deleting => return Err(busy()),
                _ => {}
            }
            state.state = SandboxState::Stopping;
            state.stop.cancel();
        }
        let _fence = self.record.fence.write().await;
        let mut state = self.record.state.lock().await;
        if state.state == SandboxState::Stopping {
            state.state = SandboxState::Stopped;
        }
        drop(state);
        self.log("stop").await;
        Ok(())
    }
    async fn exec(&self, spec: ExecSpec, controls: ExecControls) -> Result<ExecOutcome, Error> {
        ensure_open(&self.closed)?;
        spec.validate()?;
        let (_fence, stop) = self.admit().await?;
        let response = self
            .fake
            .inner
            .scripts
            .lock()
            .await
            .get(&spec.argv)
            .cloned()
            .ok_or_else(|| Error::Unsupported {
                capability: "unscripted fake command".into(),
            })?;
        self.log("exec").await;
        let work = async {
            if let Some(mut stdin) = controls.stdin {
                io::copy(&mut stdin, &mut io::sink())
                    .await
                    .map_err(|_| transport("fake stdin failed"))?;
            }
            if let Some(sink) = controls.sink {
                let (stdout, stderr) = if spec.tty {
                    (OutputStream::Terminal, OutputStream::Terminal)
                } else {
                    (OutputStream::Stdout, OutputStream::Stderr)
                };
                for (stream, bytes) in [(stdout, response.stdout), (stderr, response.stderr)] {
                    if !bytes.is_empty() {
                        sink(stream, bytes).await?;
                    }
                }
            }
            if response.wait_for_stop {
                future::pending::<()>().await;
            }
            Ok::<_, Error>(Termination::Exited)
        };
        let deadline = async {
            if let Some(ms) = spec.timeout_ms {
                time::sleep(Duration::from_millis(ms)).await;
            } else {
                future::pending::<()>().await;
            }
        };
        let termination = tokio::select! {
            biased;
            () = self.closed.cancelled() => {
                return Err(Error::Stale { outcome: MutationOutcome::Unknown });
            }
            () = stop.cancelled() => Termination::Killed,
            () = sandbox_driver::stop_signal(controls.kill.as_ref()) => Termination::Killed,
            () = sandbox_driver::stop_signal(controls.term.as_ref()) => Termination::Terminated,
            () = deadline => Termination::TimedOut,
            result = work => result?,
        };
        Ok(ExecOutcome {
            termination,
            exit_code: (termination == Termination::Exited).then_some(response.exit_code),
            signal: None,
            output_loss: OutputLoss::default(),
        })
    }
    fn files(&self) -> &dyn Files {
        self
    }
    fn ports(&self) -> &dyn Ports {
        self
    }
}

#[async_trait]
impl Files for Handle {
    async fn get(&self, path: &str, range: Option<FileRange>) -> Result<ByteStream, Error> {
        ensure_open(&self.closed)?;
        self.log("files.get").await;
        let files = self.record.files.lock().await;
        let path = self.path(path)?;
        let bytes = files.get(&path).ok_or_else(|| Error::NotFound {
            resource: "file".into(),
            id:       path,
        })?;
        let start = range
            .map_or(0, |range| {
                usize::try_from(range.offset).unwrap_or(usize::MAX)
            })
            .min(bytes.len());
        let end = range
            .and_then(|range| range.length)
            .map_or(bytes.len(), |length| {
                start
                    .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
                    .min(bytes.len())
            });
        Ok(Box::pin(Cursor::new(bytes[start..end].to_vec())))
    }
    async fn put(&self, path: &str, mut bytes: ByteStream) -> Result<(), Error> {
        ensure_open(&self.closed)?;
        let path = self.path(path)?;
        let (_fence, stop) = self.admit().await?;
        let mut buffer = Vec::new();
        tokio::select! {
            () = self.closed.cancelled() => return Err(stale()),
            () = stop.cancelled() => return Err(busy()),
            result = bytes.read_to_end(&mut buffer) => {
                result.map_err(|_| transport("fake file input failed"))?;
            }
        }
        self.record.files.lock().await.insert(path, buffer);
        self.log("files.put").await;
        Ok(())
    }
    async fn list(&self, path: &str, depth: u32) -> Result<Vec<FileEntry>, Error> {
        ensure_open(&self.closed)?;
        self.log("files.list").await;
        let path = self.path(path)?;
        let prefix = format!("{}/", path.trim_end_matches('/'));
        let files = self.record.files.lock().await;
        let mut entries = BTreeMap::new();
        let below = files
            .range(prefix.clone()..)
            .take_while(|(path, _)| path.starts_with(&prefix));
        for (path, bytes) in below {
            let parts: Vec<_> = path[prefix.len()..].split('/').collect();
            for index in 0..parts.len().min((depth as usize).saturating_add(1)) {
                let entry_path = format!("{}{}", prefix, parts[..=index].join("/"));
                let (kind, size) = if index + 1 == parts.len() {
                    (FileKind::File, bytes.len() as u64)
                } else {
                    (FileKind::Directory, 0)
                };
                entries.insert(entry_path.clone(), FileEntry {
                    path: entry_path,
                    kind,
                    size,
                });
            }
        }
        Ok(entries.into_values().collect())
    }
}
#[async_trait]
impl Ports for Handle {
    async fn expose(&self, _: NonZeroU16, _: Expose) -> Result<Exposure, Error> {
        ensure_open(&self.closed)?;
        self.log("ports").await;
        Err(Error::Unsupported {
            capability: "ports".into(),
        })
    }
    async fn release(&self, _: NonZeroU16) -> Result<(), Error> {
        ensure_open(&self.closed)?;
        self.log("ports").await;
        Err(Error::Unsupported {
            capability: "ports".into(),
        })
    }
}
fn ensure_open(closed: &CancellationToken) -> Result<(), Error> {
    if closed.is_cancelled() {
        Err(stale())
    } else {
        Ok(())
    }
}
fn absent(id: &SandboxId) -> Error {
    Error::NotFound {
        resource: "sandbox".into(),
        id:       id.to_string(),
    }
}
fn busy() -> Error {
    Error::Busy {
        message: "fake sandbox is not accepting execution".into(),
    }
}
fn stale() -> Error {
    Error::Stale {
        outcome: MutationOutcome::NotStarted,
    }
}
fn transport(message: &str) -> Error {
    Error::Transport {
        message: message.into(),
        outcome: MutationOutcome::Unknown,
    }
}
