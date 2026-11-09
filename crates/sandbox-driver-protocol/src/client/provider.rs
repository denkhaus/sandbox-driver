//! [`PluginProvider`]: the host's view of a served plugin, plus the
//! snapshot and volume services it exposes.

use std::process::{self, Stdio};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sandbox_driver::{
    Capabilities, Error, EventContext, HealthStatus, LogSink, ProviderHealth, ProviderKind, Result,
    Sandbox, SandboxFilter, SandboxId, SandboxSpec, SandboxStatus, SnapshotFilter, SnapshotId,
    SnapshotProvider, SnapshotSpec, SnapshotStatus, TransportError, VolumeId, VolumeProvider,
    VolumeSpec, VolumeStatus,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::process::{Child, Command};
use tokio::runtime::Handle as RuntimeHandle;
use tokio::time;

use super::Client;
use super::sandbox::SandboxHandle;
use super::streams::follow_log_stream;
use crate::channel::{ChannelListener, TrustedPeer};
use crate::wire::is_method_not_found;
use crate::{TransportDiagnostics, TransportLimits, methods as m};

/// A provider served by a JSON-RPC plugin over a byte stream.
///
/// `connect` binds the data transport, performs the `initialize`
/// handshake, and returns a provider whose capabilities are the plugin's
/// declared set. The background read and write tasks end when the
/// transport closes or the provider is dropped.
pub struct PluginProvider {
    pub(super) client:       Arc<Client>,
    pub(super) kind:         ProviderKind,
    pub(super) capabilities: Capabilities,
    pub(super) snapshots:    Option<ProviderSnapshots>,
    pub(super) volumes:      Option<ProviderVolumes>,
    /// The plugin child process, when this provider spawned one.
    pub(super) child:        Option<Mutex<Option<Child>>>,
}

impl PluginProvider {
    #[tracing::instrument(skip_all, err)]
    pub async fn connect(
        reader: impl AsyncRead + Unpin + Send + 'static,
        writer: impl AsyncWrite + Unpin + Send + 'static,
    ) -> Result<Self> {
        Self::connect_with_limits(
            reader,
            writer,
            TransportLimits::default(),
            TrustedPeer::Process(process::id()),
        )
        .await
    }

    /// Connects to an explicitly trusted data peer with finite transport
    /// budgets.
    pub async fn connect_with_limits(
        reader: impl AsyncRead + Unpin + Send + 'static,
        writer: impl AsyncWrite + Unpin + Send + 'static,
        limits: TransportLimits,
        peer: TrustedPeer,
    ) -> Result<Self> {
        let listener = ChannelListener::bind_with_limits(limits.clone(), peer)?;
        let transport = listener.transport();
        let client = Client::start(reader, writer, listener, limits);
        let result: m::InitializeResult = client
            .call(m::INITIALIZE, &m::InitializeParams {
                protocol_version: m::PROTOCOL_VERSION,
                data_transport:   transport,
            })
            .await?;
        if result.protocol_version != m::PROTOCOL_VERSION {
            return Err(Error::invalid_spec(
                "protocol_version",
                format!(
                    "host speaks protocol version {} but the plugin answered {}",
                    m::PROTOCOL_VERSION,
                    result.protocol_version
                ),
            ));
        }
        let mut capabilities = result.capabilities;
        mask_wire_capabilities(&mut capabilities);
        let snapshots = capabilities.snapshots.is_some().then(|| ProviderSnapshots {
            client: Arc::clone(&client),
        });
        let volumes = capabilities.volumes.is_some().then(|| ProviderVolumes {
            client: Arc::clone(&client),
        });
        Ok(Self {
            client,
            kind: result.provider.kind,
            capabilities,
            snapshots,
            volumes,
            child: None,
        })
    }

    /// Spawns a plugin binary as a child process and connects over its
    /// stdin/stdout.
    ///
    /// The caller configures the command (arguments, environment
    /// scrubbing, working directory — transport trust is host policy);
    /// this constructor pipes stdio, leaves stderr inherited so plugin
    /// logs reach the host's stderr, and marks the child kill-on-drop.
    /// [`PluginProvider::shutdown`] asks the plugin to exit and reaps it,
    /// killing after a grace period.
    #[tracing::instrument(skip_all, err)]
    pub async fn spawn(command: Command) -> Result<Self> {
        Self::spawn_with_limits(command, TransportLimits::default()).await
    }

    pub async fn spawn_with_limits(mut command: Command, limits: TransportLimits) -> Result<Self> {
        limits.validate()?;
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| Error::io("spawning plugin process", error))?;
        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");
        let pid = child.id().ok_or_else(|| {
            Error::Transport(TransportError::new("plugin exited before initialization"))
        })?;
        let mut provider =
            Self::connect_with_limits(stdout, stdin, limits, TrustedPeer::Process(pid)).await?;
        provider.child = Some(Mutex::new(Some(child)));
        Ok(provider)
    }

    /// Whether the control transport is still open. `false` once the
    /// plugin exited or its stdio broke; every later call fails at once.
    pub fn is_closed(&self) -> bool {
        self.client.closed.load(Ordering::SeqCst)
    }

    /// Snapshot local ownership and plugin registries. Older peers may not
    /// implement the optional `transport/diagnostics` method.
    pub async fn transport_diagnostics(&self) -> Result<TransportDiagnostics> {
        let server = self
            .client
            .call(m::TRANSPORT_DIAGNOSTICS, &m::Empty)
            .await?;
        let mut client = self.client.listener.diagnostics();
        client.stop_requests = self.client.stop_requests.load(Ordering::Relaxed);
        client.stop_acknowledgments = self.client.stop_acknowledgments.load(Ordering::Relaxed);
        client.max_stop_acknowledgment_us = self
            .client
            .max_stop_acknowledgment_us
            .load(Ordering::Relaxed);
        client.pending_requests = self.client.pending.lock().expect("pending lock").len();
        let mut cleanup = self.client.cleanup.lock().expect("cleanup tasks lock");
        while cleanup.try_join_next().is_some() {}
        client.cleanup_tasks = cleanup.len();
        client.failed_cleanups = self
            .client
            .failed_cleanup
            .lock()
            .expect("failed cleanup lock")
            .len();
        client.event_routes = self
            .client
            .event_routes
            .lock()
            .expect("event routes lock")
            .len();
        client.event_contexts = self
            .client
            .event_contexts
            .lock()
            .expect("event contexts lock")
            .len();
        client.runtime_tasks = RuntimeHandle::current().metrics().num_alive_tasks();
        Ok(TransportDiagnostics { client, server })
    }

    /// Takes the bounded recent channel-setup measurements, in microseconds.
    pub fn take_channel_setup_samples_us(&self) -> Vec<u64> {
        self.client.listener.take_setup_samples_us()
    }

    /// PID for process supervision and resource measurements, when spawned.
    pub fn process_id(&self) -> Option<u32> {
        self.child.as_ref().and_then(|slot| {
            slot.lock()
                .expect("child lock")
                .as_ref()
                .and_then(Child::id)
        })
    }

    /// Failure of this connection's ordered event subscription. Control calls
    /// continue, but applications must treat the event history as incomplete.
    pub fn event_delivery_error(&self) -> Option<TransportError> {
        self.client
            .event_failure
            .lock()
            .expect("event failure lock")
            .clone()
    }

    /// Last failed or timed-out local cleanup. Its I/O admission remains
    /// occupied until this connection is dropped; other operations continue.
    pub fn cleanup_error(&self) -> Option<TransportError> {
        self.client
            .failed_cleanup
            .lock()
            .expect("failed cleanup lock")
            .last()
            .map(|(_, error)| error.clone())
    }

    /// Asks the plugin to shut down cleanly and reaps a spawned child.
    /// Acknowledgment and process exit share a five-second deadline;
    /// failure or timeout kills the child and returns an error.
    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    pub async fn shutdown(&self) -> Result<()> {
        let mut child = self
            .child
            .as_ref()
            .and_then(|slot| slot.lock().expect("child lock").take());
        self.client.shutdown_requested.store(true, Ordering::SeqCst);
        let outcome = time::timeout(self.client.limits.shutdown_timeout, async {
            let _: m::Empty = self.client.call(m::SHUTDOWN, &m::Empty).await?;
            if let Some(child) = &mut child {
                child
                    .wait()
                    .await
                    .map_err(|error| Error::io("reaping plugin process", error))?;
            }
            Ok(())
        })
        .await
        .unwrap_or_else(|_| {
            Err(Error::Transport(TransportError::new(
                "plugin shutdown timed out before acknowledgment or process exit",
            )))
        });
        if outcome.is_err() {
            if let Some(child) = &mut child {
                if let Err(error) = child.start_kill() {
                    tracing::warn!(provider_kind = %self.kind, error = ?error, "plugin process kill failed");
                }
                // Do not add an unbounded wait after the deadline. Tokio's
                // child drop transfers any remaining reap to its reaper.
            }
        }
        outcome
    }

    /// Attaches to or restores an existing sandbox by id: the two calls
    /// differ only in their method.
    async fn open_handle(
        &self,
        method: &str,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::HandleInfo> = self
            .client
            .call(method, &m::AttachParams {
                sandbox_id: id.as_str().to_owned(),
                events:     event_request.clone(),
            })
            .await;
        Ok(self.wrap_handle(outcome?, events))
    }

    fn wrap_handle(
        &self,
        mut info: m::HandleInfo,
        events: Option<EventContext>,
    ) -> Arc<dyn Sandbox> {
        mask_wire_capabilities(&mut info.capabilities);
        if let Some(context) = &events {
            self.client.remember_event_context(&info.status.id, context);
        }
        Arc::new(SandboxHandle::new(Arc::clone(&self.client), info, events))
    }
}

#[async_trait]
impl sandbox_driver::SandboxProvider for PluginProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn create(
        &self,
        spec: &SandboxSpec,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::HandleInfo> = self
            .client
            .call(m::SANDBOX_CREATE, &m::CreateParams {
                spec:   m::SandboxSpecDto::try_from(spec)?,
                events: event_request.clone(),
            })
            .await;
        Ok(self.wrap_handle(outcome?, events))
    }

    async fn attach(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        self.open_handle(m::SANDBOX_ATTACH, id, events).await
    }

    async fn undelete(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        self.open_handle(m::SANDBOX_UNDELETE, id, events).await
    }

    async fn delete(&self, id: &SandboxId, events: Option<EventContext>) -> Result<()> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::Empty> = self
            .client
            .call(m::SANDBOX_DELETE, &m::AttachParams {
                sandbox_id: id.as_str().to_owned(),
                events:     event_request.clone(),
            })
            .await;
        outcome.map(|_| ())
    }

    async fn list(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>> {
        let result: m::ListResult = self
            .client
            .call(m::SANDBOX_LIST, &m::ListParams {
                filter: filter.clone(),
            })
            .await?;
        Ok(result.sandboxes)
    }

    async fn health(&self) -> Result<ProviderHealth> {
        match self
            .client
            .call::<_, m::HealthResult>(m::PROVIDER_HEALTH, &m::Empty)
            .await
        {
            Ok(result) => Ok(result.health),
            // A plugin without provider/health answers -32601; that is
            // "no health check", not a failed one.
            Err(error) if is_method_not_found(&error) => {
                Ok(ProviderHealth::new(HealthStatus::Unknown))
            }
            Err(error) => Err(error),
        }
    }

    fn snapshots(&self) -> Option<&dyn SnapshotProvider> {
        self.snapshots
            .as_ref()
            .map(|service| service as &dyn SnapshotProvider)
    }

    fn volumes(&self) -> Option<&dyn VolumeProvider> {
        self.volumes
            .as_ref()
            .map(|service| service as &dyn VolumeProvider)
    }
}

/// Snapshot service backed by the plugin.
pub(super) struct ProviderSnapshots {
    client: Arc<Client>,
}

impl ProviderSnapshots {
    /// A snapshot action that carries only the id and an event route.
    async fn with_events(
        &self,
        method: &str,
        id: &SnapshotId,
        events: Option<EventContext>,
    ) -> Result<()> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::Empty> = self
            .client
            .call(method, &m::SnapshotIdParams {
                snapshot_id: id.as_str().to_owned(),
                events:      event_request.clone(),
            })
            .await;
        outcome?;
        Ok(())
    }
}

#[async_trait]
impl SnapshotProvider for ProviderSnapshots {
    async fn create(
        &self,
        spec: &SnapshotSpec,
        events: Option<EventContext>,
    ) -> Result<SnapshotId> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::SnapshotIdResult> = self
            .client
            .call(m::SNAPSHOT_CREATE, &m::SnapshotCreateParams {
                spec:   m::SnapshotSpecDto::try_from(spec)?,
                events: event_request.clone(),
            })
            .await;
        let result = outcome?;
        SnapshotId::try_new(result.snapshot_id)
            .map_err(|error| Error::invalid_spec("snapshot_id", error.to_string()))
    }

    async fn get(&self, id: &SnapshotId) -> Result<SnapshotStatus> {
        let result: m::SnapshotStatusResult = self
            .client
            .call(m::SNAPSHOT_GET, &m::SnapshotIdParams {
                snapshot_id: id.as_str().to_owned(),
                events:      None,
            })
            .await?;
        Ok(result.status)
    }

    async fn list(&self, filter: &SnapshotFilter) -> Result<Vec<SnapshotStatus>> {
        let result: m::SnapshotListResult = self
            .client
            .call(m::SNAPSHOT_LIST, &m::SnapshotListParams {
                filter: filter.clone(),
            })
            .await?;
        Ok(result.snapshots)
    }

    async fn delete(&self, id: &SnapshotId, events: Option<EventContext>) -> Result<()> {
        self.with_events(m::SNAPSHOT_DELETE, id, events).await
    }

    async fn build_logs(&self, id: &SnapshotId, follow: bool, sink: LogSink) -> Result<()> {
        let stream_id = self.client.next_stream_id("snapshot");
        let (channel, receiver) = self.client.listener.expect()?;
        follow_log_stream(
            &self.client,
            m::SNAPSHOT_BUILD_LOGS,
            &m::SnapshotBuildLogsParams {
                snapshot_id: id.as_str().to_owned(),
                stream_id: stream_id.clone(),
                channel,
                follow,
            },
            &stream_id,
            receiver,
            sink,
        )
        .await
    }

    async fn activate(&self, id: &SnapshotId, events: Option<EventContext>) -> Result<()> {
        self.with_events(m::SNAPSHOT_ACTIVATE, id, events).await
    }

    async fn deactivate(&self, id: &SnapshotId, events: Option<EventContext>) -> Result<()> {
        self.with_events(m::SNAPSHOT_DEACTIVATE, id, events).await
    }
}

/// Volume service backed by the plugin.
pub(super) struct ProviderVolumes {
    client: Arc<Client>,
}

#[async_trait]
impl VolumeProvider for ProviderVolumes {
    async fn create(&self, spec: &VolumeSpec, events: Option<EventContext>) -> Result<VolumeId> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::VolumeIdResult> = self
            .client
            .call(m::VOLUME_CREATE, &m::VolumeCreateParams {
                spec:   spec.clone(),
                events: event_request.clone(),
            })
            .await;
        let result = outcome?;
        VolumeId::try_new(result.volume_id)
            .map_err(|error| Error::invalid_spec("volume_id", error.to_string()))
    }

    async fn get(&self, id: &VolumeId) -> Result<VolumeStatus> {
        let result: m::VolumeStatusResult = self
            .client
            .call(m::VOLUME_GET, &m::VolumeIdParams {
                volume_id: id.as_str().to_owned(),
                events:    None,
            })
            .await?;
        Ok(result.status)
    }

    async fn list(&self) -> Result<Vec<VolumeStatus>> {
        let result: m::VolumeListResult = self.client.call(m::VOLUME_LIST, &m::Empty).await?;
        Ok(result.volumes)
    }

    async fn delete(&self, id: &VolumeId, events: Option<EventContext>) -> Result<()> {
        let event_request = self.client.register_events(events.as_ref());
        let outcome: Result<m::Empty> = self
            .client
            .call(m::VOLUME_DELETE, &m::VolumeIdParams {
                volume_id: id.as_str().to_owned(),
                events:    event_request.clone(),
            })
            .await;
        outcome?;
        Ok(())
    }
}

/// Removes capabilities the protocol cannot deliver through the wire.
/// Native search/service passthrough and local shell commands remain
/// process-local; everything else, streamed stdin, the effective
/// environment, and the provider's own git clone included, crosses in
/// version 2. `git.native` stays as declared: when true the host sends
/// `git/clone` so the plugin runs its native clone, and the host derives
/// the remaining operations either way.
fn mask_wire_capabilities(capabilities: &mut Capabilities) {
    capabilities.search.native = false;
    capabilities.services.native = false;
    capabilities.access.shell_command = false;
}
