//! The Daytona provider: connect, create, attach, undelete, health, list.

use std::collections::HashMap;
use std::env;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use daytona_api_client::models::SandboxClass as DaytonaSandboxClass;
use daytona_api_client::models::api_key_list::Permissions;
use daytona_sdk::{
    Client, CreateParams, CreateSandboxOptions, DaytonaConfig, DaytonaError, SandboxBaseParams,
    SandboxListItem, SetFilePermissionsOptions, SnapshotParams,
};
use futures_util::TryStreamExt;
use sandbox_driver::{
    Action, AuthError, Capabilities, Error, EventContext, EventEmitter, EventSubject, HealthStatus,
    Isolation, LogsCaps, NetworkPolicy, ProviderError, ProviderHealth, ProviderKind, PtyCaps,
    ResourceKind, Resources, Result, Sandbox, SandboxFilter, SandboxId, SandboxKind,
    SandboxProvider, SandboxSource, SandboxSpec, SandboxState, SandboxStatus, SnapshotCaps,
    SnapshotId, SnapshotProvider, SnapshotSource, SnapshotSpec, VolumeCaps, VolumeProvider,
};
use sandbox_driver_daytona_config::{DaytonaProviderConfig, NestedDockerConfig};
use serde::Deserialize;
use tokio::time;

use crate::labels::{MANAGED_LABEL, stored_labels};
use crate::sandbox::{DaytonaSandbox, status_from_sdk};
use crate::sdk::{
    DaytonaClient, auto_delete_minutes, daytona_error, fetch_error, gigabytes, map_state, minutes,
    sandbox_kind_from_class,
};
use crate::snapshots::DaytonaSnapshots;
use crate::volumes::DaytonaVolumes;
use crate::{
    CREATE_POLL, CREATE_TIMEOUT, DOCKERFILE_CREATE_TIMEOUT, FAILED_CREATE_CLEANUP_BUDGET,
    LIST_PAGE_SIZE, RUNTIME_DIRECTORY, RUNTIME_DIRECTORY_PARENT, cleanup, nested, toolbox,
};

/// The current-key endpoint includes its effective organization, which the
/// generated SDK model currently discards. Read only health metadata here;
/// neither the credential nor its masked value belongs in provider identity.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentApiKey {
    name:            String,
    permissions:     Vec<Permissions>,
    #[serde(default)]
    organization_id: Option<String>,
}

/// Scopes every sandbox-driver Daytona operation may need, paired with
/// their wire names, in the order an operator reads them when regenerating
/// a key: snapshots are built before sandboxes are created from them.
/// [`ProviderHealth::required_permissions`] and
/// [`ProviderHealth::missing_permissions`] follow this order.
const REQUIRED_PERMISSIONS: &[(Permissions, &str)] = &[
    (Permissions::WRITE_SNAPSHOTS, "write:snapshots"),
    (Permissions::DELETE_SNAPSHOTS, "delete:snapshots"),
    (Permissions::WRITE_SANDBOXES, "write:sandboxes"),
    (Permissions::DELETE_SANDBOXES, "delete:sandboxes"),
];

/// The wire names of every required permission, in documented order.
fn required_permission_names() -> Vec<String> {
    REQUIRED_PERMISSIONS
        .iter()
        .map(|(_, name)| (*name).to_owned())
        .collect()
}

/// The required permissions `granted` lacks, in documented order.
fn missing_permission_names(granted: &[Permissions]) -> Vec<String> {
    REQUIRED_PERMISSIONS
        .iter()
        .filter(|(permission, _)| !granted.contains(permission))
        .map(|(_, name)| (*name).to_owned())
        .collect()
}

/// The name of the snapshot an image or Dockerfile builds into, for the
/// given resources and kind: `sandbox-driver-` and 32 hex digits of an
/// HMAC over the inputs, keyed by the credential so a rotated key never
/// collides with another tenant's snapshots. The manifest carries a
/// version; changing it renames every existing snapshot.
fn cached_snapshot_name(
    secret: &str,
    source: &SnapshotSource,
    resources: &Resources,
    kind: Option<SandboxKind>,
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    let source_line = match source {
        SnapshotSource::Image { reference } => format!("image {reference}"),
        SnapshotSource::Dockerfile { content } => {
            format!("dockerfile {:x}", Sha256::digest(content.as_bytes()))
        }
        _ => "other".to_owned(),
    };
    let manifest = format!(
        "sandbox-driver snapshot v1\n{kind:?}\n{source_line}\ncpu {:?}\nmemory_gb {:?}\ndisk_gb {:?}\ngpu {:?}\n",
        resources.cpu_cores,
        resources.memory_mb.map(gigabytes),
        resources.disk_mb.map(gigabytes),
        resources.gpus,
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts keys of any length");
    mac.update(manifest.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut name = String::from("sandbox-driver-");
    for byte in &digest[..16] {
        let _ = write!(name, "{byte:02x}");
    }
    name
}

/// The Daytona provider.
pub struct DaytonaProvider {
    kind:              ProviderKind,
    capabilities:      Capabilities,
    pub(crate) client: DaytonaClient,
    uses_api_key:      bool,
    snapshots:         DaytonaSnapshots,
    volumes:           DaytonaVolumes,
}

impl DaytonaProvider {
    /// Connects using the SDK's environment configuration.
    #[tracing::instrument(skip_all, fields(provider_kind = "daytona"), err)]
    pub async fn connect() -> Result<Self> {
        Self::connect_with_config(DaytonaConfig::default()).await
    }

    /// Connects using explicit SDK configuration.
    ///
    /// Values set in `config` take precedence over the SDK's environment
    /// variables. Unset values retain the SDK's environment fallbacks.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is invalid or the SDK client
    /// cannot be initialized.
    #[tracing::instrument(skip_all, fields(provider_kind = "daytona"), err)]
    pub async fn connect_with_config(mut config: DaytonaConfig) -> Result<Self> {
        // Resolve the SDK's API-key fallback once, before it consumes config.
        // An explicitly empty key disables that fallback and selects JWT.
        config.api_key = Some(
            config
                .api_key
                .unwrap_or_else(|| env::var("DAYTONA_API_KEY").unwrap_or_default()),
        );
        Self::connect_resolved(config).await
    }

    /// Connects using only the values in `config`, never the process
    /// environment.
    ///
    /// An embedding application that resolves credentials itself (from a
    /// vault, a server secret store, or per-request input) passes them
    /// here so nothing in the worker's environment can substitute for
    /// them. Every unset field takes the SDK's built-in default: the
    /// hosted API URL, no organization header, no target region, no
    /// injected HTTP client. `config.user_agent` identifies the
    /// application to the control plane.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Auth`] when `config` carries neither an API key
    /// nor a JWT with an organization id, and a provider error when the
    /// SDK client cannot be initialized.
    #[tracing::instrument(skip_all, fields(provider_kind = "daytona"), err)]
    pub async fn connect_explicit(mut config: DaytonaConfig) -> Result<Self> {
        let kind = ProviderKind::try_new("daytona").expect("static kind is valid");
        let has_api_key = config.api_key.as_deref().is_some_and(|key| !key.is_empty());
        let has_jwt = config
            .jwt_token
            .as_deref()
            .is_some_and(|jwt| !jwt.is_empty());
        if !has_api_key && !has_jwt {
            return Err(Error::Auth(AuthError::new(
                kind,
                "no Daytona API key or JWT was supplied and the process environment is not                  consulted",
            )));
        }
        // The SDK reads the environment for every field left `None` and
        // treats an empty string as "unset without fallback", so blank
        // every field the caller did not set.
        for field in [
            &mut config.api_key,
            &mut config.jwt_token,
            &mut config.organization_id,
            &mut config.api_url,
            &mut config.target,
        ] {
            field.get_or_insert_with(String::new);
        }
        Self::connect_resolved(config).await
    }

    async fn connect_resolved(config: DaytonaConfig) -> Result<Self> {
        let uses_api_key = config.api_key.as_ref().is_some_and(|key| !key.is_empty());
        let client = Client::new_with_config(config)
            .await
            .map_err(|error| daytona_error("connecting to daytona", error))?;
        Ok(Self::from_client(client, uses_api_key))
    }

    fn from_client(client: Client, uses_api_key: bool) -> Self {
        let client = Arc::new(client);
        let kind = ProviderKind::try_new("daytona").expect("static kind is valid");
        Self {
            kind: kind.clone(),
            capabilities: daytona_capabilities(),
            snapshots: DaytonaSnapshots {
                client: Arc::clone(&client),
                kind:   kind.clone(),
            },
            volumes: DaytonaVolumes {
                client: Arc::clone(&client),
                kind,
            },
            client,
            uses_api_key,
        }
    }

    fn jwt_health_identity(&self) -> Option<String> {
        // An API key ignores the organization header. Only JWT authentication
        // can use that header as its effective resource namespace.
        if self.uses_api_key {
            return None;
        }
        self.client
            .organization_id()
            .map(|id| format!("organization:{id}"))
    }

    /// Creates the sandbox and waits for it to start under `budget`,
    /// deleting the half-created (and billed) sandbox when the wait
    /// fails so nothing is silently leaked.
    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn create_inner(
        &self,
        params: CreateParams,
        budget: Duration,
    ) -> Result<daytona_sdk::Sandbox> {
        let options = CreateSandboxOptions {
            timeout:        Some(budget),
            wait_for_start: false,
            log_sender:     None,
        };
        let created = self
            .client
            .create(params, options)
            .await
            .map_err(|error| daytona_error("creating sandbox", error))?;
        let sdk_id = created.id.clone();
        match self.wait_for_started(&sdk_id, budget).await {
            Ok(sdk) => Ok(sdk),
            Err(error) => Err(self.cleanup_failed_create(&sdk_id, error).await),
        }
    }

    async fn wait_for_started(
        &self,
        sdk_id: &str,
        budget: Duration,
    ) -> Result<daytona_sdk::Sandbox> {
        time::timeout(budget, self.poll_started(sdk_id))
            .await
            .map_err(|_| Error::Timeout {
                operation: "creating sandbox".to_owned(),
                elapsed:   budget,
            })?
    }

    async fn poll_started(&self, sdk_id: &str) -> Result<daytona_sdk::Sandbox> {
        let mut attempt = 0_u64;
        loop {
            attempt += 1;
            let sdk = self
                .client
                .get(sdk_id)
                .await
                .map_err(|error| daytona_error("fetching created sandbox", error))?;
            let state = map_state(sdk.state);
            tracing::debug!(attempt, state = ?state, "sandbox create state observed");
            match state {
                SandboxState::Running => return Ok(sdk),
                SandboxState::Error => {
                    return Err(Error::Provider(ProviderError::new(
                        self.kind.clone(),
                        sdk.error_reason
                            .unwrap_or_else(|| "sandbox entered the error state".to_owned()),
                    )));
                }
                _ => {}
            }
            time::sleep(CREATE_POLL).await;
        }
    }

    /// Confirm deletion after a failed create, within one cleanup budget.
    /// An accepted DELETE only starts cleanup. Retain the sandbox ID and
    /// original failure if removal cannot be confirmed.
    async fn cleanup_failed_create(&self, sdk_id: &str, cause: Error) -> Error {
        cleanup::failed_create(
            &self.client,
            &self.kind,
            sdk_id,
            cause,
            FAILED_CREATE_CLEANUP_BUDGET,
        )
        .await
    }

    async fn handle(
        &self,
        sdk: daytona_sdk::Sandbox,
        events: EventEmitter,
    ) -> Result<Arc<DaytonaSandbox>> {
        DaytonaSandbox::build(&self.client, &self.capabilities, sdk, events).await
    }

    /// Every step after the sandbox exists: the kind check, the runtime
    /// directories, the handle, and nested Docker provisioning. A failure
    /// anywhere here leaves a billed sandbox behind, so `create` wraps
    /// this one call in the cleanup.
    async fn finish_create(
        &self,
        created: daytona_sdk::Sandbox,
        spec: &SandboxSpec,
        nested_docker: Option<&NestedDockerConfig>,
        events: EventEmitter,
    ) -> Result<Arc<DaytonaSandbox>> {
        if let Some(requested) = spec.sandbox_kind {
            let actual = created.sandbox_class.map(sandbox_kind_from_class);
            if actual != Some(requested) {
                return Err(Error::invalid_spec(
                    "sandbox_kind",
                    format!("Daytona created a sandbox with kind {actual:?}, not {requested:?}"),
                ));
            }
        }
        initialize_directories(&created, spec.working_directory.as_deref()).await?;
        let handle = self.handle(created, events).await?;
        if let (Some(config), Some(nested)) = (nested_docker, handle.nested.as_ref()) {
            nested.create(config, &spec.env).await?;
        }
        Ok(handle)
    }

    /// The sandbox `id` as a managed sandbox, or `NotFound`. A sandbox this
    /// provider did not create carries no managed label and is reported
    /// missing rather than handed out; `attach` and `undelete` both gate
    /// on this one check.
    async fn managed_sandbox(&self, id: &SandboxId) -> Result<daytona_sdk::Sandbox> {
        let sdk = self.client.get(id.as_str()).await.map_err(fetch_error(
            ResourceKind::Sandbox,
            id.as_str(),
            "fetching sandbox",
        ))?;
        if sdk.labels.get(MANAGED_LABEL).map(String::as_str) != Some("true") {
            return Err(Error::NotFound {
                resource: ResourceKind::Sandbox,
                id:       id.as_str().to_owned(),
            });
        }
        Ok(sdk)
    }

    /// The snapshot `spec`'s image or Dockerfile source builds into, made
    /// active: found, reactivated, built, or waited for, with the work
    /// reported through `events`. The name is derived from the source and
    /// resources under the credential, so the same inputs reuse one
    /// snapshot and two tenants never share a name.
    async fn cached_snapshot(
        &self,
        spec: &SandboxSpec,
        events: Option<EventContext>,
    ) -> Result<SnapshotId> {
        let source = match &spec.source {
            SandboxSource::Image { reference } => SnapshotSource::Image {
                reference: reference.clone(),
            },
            SandboxSource::Dockerfile { content } => SnapshotSource::Dockerfile {
                content: content.clone(),
            },
            _ => return Err(Error::invalid_spec("source", "not an image or Dockerfile")),
        };
        let secret = self
            .client
            .api_configuration()
            .bearer_access_token
            .clone()
            .unwrap_or_default();
        let name = cached_snapshot_name(&secret, &source, &spec.resources, spec.sandbox_kind);
        let mut snapshot = SnapshotSpec::new(source).name(name);
        snapshot.sandbox_kind = spec.sandbox_kind.or(Some(SandboxKind::Container));
        snapshot.region.clone_from(&spec.region);
        snapshot.resources = spec.resources;
        let budget = if matches!(spec.source, SandboxSource::Dockerfile { .. }) {
            DOCKERFILE_CREATE_TIMEOUT
        } else {
            CREATE_TIMEOUT
        };
        self.snapshots.ensure(&snapshot, budget, events).await
    }

    async fn ensure_snapshot_kind(
        &self,
        id: &SnapshotId,
        requested: Option<SandboxKind>,
    ) -> Result<()> {
        let Some(requested) = requested else {
            return Ok(());
        };
        let snapshot = self
            .client
            .snapshot
            .get(id.as_str())
            .await
            .map_err(fetch_error(
                ResourceKind::Snapshot,
                id.as_str(),
                "fetching snapshot kind",
            ))?;
        let Some(actual) = snapshot.sandbox_class.map(sandbox_kind_from_class) else {
            return Err(Error::invalid_spec(
                "sandbox_kind",
                "the snapshot does not report a sandbox kind",
            ));
        };
        if actual != requested {
            return Err(Error::invalid_spec(
                "sandbox_kind",
                format!("the snapshot has kind {actual:?}, not {requested:?}"),
            ));
        }
        Ok(())
    }
}

async fn initialize_directories(
    sdk: &daytona_sdk::Sandbox,
    working_directory: Option<&str>,
) -> Result<()> {
    let fs = toolbox::fs_of(sdk).await?;
    if let Some(working_directory) = working_directory {
        fs.create_folder(working_directory, Some("0755"))
            .await
            .map_err(|error| daytona_error("creating requested working directory", error))?;
    }
    for path in [RUNTIME_DIRECTORY_PARENT, RUNTIME_DIRECTORY] {
        fs.create_folder(path, Some("0700"))
            .await
            .map_err(|error| daytona_error("creating runtime directory", error))?;
        fs.set_file_permissions(path, SetFilePermissionsOptions {
            mode:  Some("0700".to_owned()),
            owner: None,
            group: None,
        })
        .await
        .map_err(|error| daytona_error("setting runtime directory permissions", error))?;
    }
    Ok(())
}

pub(crate) fn daytona_capabilities() -> Capabilities {
    let mut caps = Capabilities::minimal(Isolation::Vm);
    caps.lifecycle.archive = true;
    // VM-class-only verbs are declared at the provider level (the upper
    // bound); the per-sandbox set narrows them by class.
    caps.lifecycle.pause = true;
    caps.lifecycle.fork = true;
    caps.lifecycle.snapshot_sandbox = true;
    // Daytona's "recover" endpoint is an undelete (restore within 24
    // hours of deletion), not recovery from the Error state.
    caps.lifecycle.undelete = true;
    caps.lifecycle.resize = false;
    caps.lifecycle.refresh_activity = true;
    caps.lifecycle.timers = true;
    caps.lifecycle.labels = true;
    caps.lifecycle.update_network = true;
    caps.exec.stdin = true;
    caps.exec.stop = true;
    caps.exec.live_streaming = true;
    caps.exec.streams_separated = true;
    // Session-backed; UTF-8 payloads only (the ACP use case) — see the
    // stdio module.
    caps.exec.stdio_process = true;
    caps.exec.environment = true;
    caps.exec.stdin_stream = true;
    caps.one_shot = Some(nested::one_shot_capabilities());
    caps.fs.native = true;
    caps.fs.upload = true;
    caps.fs.download = true;
    caps.fs.permissions = true;
    caps.search.supported = true;
    caps.git.supported = true;
    caps.git.native = true;
    caps.services.supported = true;
    caps.pty = Some({
        let mut pty = PtyCaps::default();
        pty.resize = true;
        pty
    });
    caps.logs = Some({
        let mut logs = LogsCaps::default();
        logs.entrypoint = true;
        logs
    });
    caps.access.preview_urls = true;
    caps.access.signed_preview_urls = true;
    caps.access.ssh = true;
    caps.access.ssh_ttl = true;
    caps.access.ssh_revoke = true;
    caps.access.web_terminal = true;
    caps.access.vnc = true;
    caps.network.allow_all = true;
    caps.network.block_all = true;
    caps.network.cidr_allow_list = true;
    caps.network.domain_allow_list = true;
    caps.snapshots = Some({
        let mut snapshots = SnapshotCaps::default();
        snapshots.from_image = true;
        snapshots.from_dockerfile = true;
        snapshots.from_image_kinds.container = true;
        snapshots.from_image_kinds.virtual_machine = true;
        snapshots.from_dockerfile_kinds.container = true;
        snapshots.filesystem_from_sandbox = true;
        snapshots.live_process_state_from_sandbox = true;
        snapshots.build_logs = true;
        snapshots.activation = true;
        snapshots
    });
    caps.volumes = Some({
        let mut volumes = VolumeCaps::default();
        volumes.create_time_attach = true;
        volumes
    });
    caps
}

/// Narrows the provider's upper-bound capability set to one sandbox.
///
/// Pause and fork are VM-class operations; archive is a container-class
/// operation. Live snapshots are supported on the hosted default sandbox
/// class and remain declared. An unknown or unreported class keeps the
/// upper bound — the typed `Unsupported`/provider error at the call is
/// still the enforcement.
pub(crate) fn narrowed_capabilities(
    base: &Capabilities,
    class: Option<DaytonaSandboxClass>,
) -> Capabilities {
    let mut caps = base.clone();
    if matches!(
        class,
        Some(DaytonaSandboxClass::CONTAINER | DaytonaSandboxClass::ANDROID)
    ) {
        caps.lifecycle.pause = false;
        caps.lifecycle.fork = false;
        if let Some(snapshots) = &mut caps.snapshots {
            snapshots.live_process_state_from_sandbox = false;
        }
    }
    if matches!(
        class,
        Some(
            DaytonaSandboxClass::LINUX_VM
                | DaytonaSandboxClass::ANDROID
                | DaytonaSandboxClass::WINDOWS
        )
    ) {
        caps.lifecycle.archive = false;
    }
    caps
}

fn provider_config(value: &serde_json::Value) -> Result<DaytonaProviderConfig> {
    match value {
        serde_json::Value::Null => Ok(DaytonaProviderConfig::default()),
        serde_json::Value::Object(_) => DaytonaProviderConfig::deserialize(value)
            .map_err(|error| Error::invalid_spec("provider_config", error.to_string())),
        _ => Err(Error::invalid_spec("provider_config", "expected an object")),
    }
}

fn base_params(spec: &SandboxSpec) -> Result<SandboxBaseParams> {
    let config = provider_config(&spec.provider_config)?;
    let (network_block_all, network_allow_list, domain_allow_list) = match &spec.network {
        NetworkPolicy::ProviderDefault => (None, None, None),
        NetworkPolicy::AllowAll => (Some(false), None, None),
        NetworkPolicy::Block => (Some(true), None, None),
        NetworkPolicy::CidrAllowList { cidrs } => (None, Some(cidrs.clone()), None),
        NetworkPolicy::DomainAllowList { domains } => (None, None, Some(domains.clone())),
        _ => return Err(Error::invalid_spec("network", "unsupported network policy")),
    };
    if let Some(docker) = &config.docker {
        nested::validate(docker, spec)?;
    }
    let labels = stored_labels(
        &spec.labels,
        spec.working_directory.as_deref(),
        config.docker.as_ref().map(|docker| docker.target),
    );
    Ok(SandboxBaseParams {
        name: spec.name.clone(),
        user: spec.user.clone(),
        language: None,
        env_vars: Some({
            let mut env: HashMap<String, String> = spec
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            // Blank BASH_ENV in the sandbox's own environment, overriding
            // any caller or snapshot value. The per-exec strip is not
            // enough: the inner bash of every session exec sources
            // $BASH_ENV at startup, before the stripped script runs.
            env.insert("BASH_ENV".to_owned(), String::new());
            env
        }),
        labels: Some(labels),
        public: spec.public,
        target: spec.region.clone(),
        auto_stop_interval: spec.timers.auto_stop_after_idle.map(minutes),
        auto_pause_interval: spec.timers.auto_pause_after_idle.map(minutes),
        auto_archive_interval: spec.timers.auto_archive_after_stop.map(minutes),
        auto_delete_interval: spec
            .timers
            .auto_delete_after_stop
            .map(auto_delete_minutes)
            .or(if spec.ephemeral { Some(0) } else { None }),
        ttl_minutes: spec.timers.ttl.map(minutes),
        volumes: (!spec.volumes.is_empty()).then(|| {
            spec.volumes
                .iter()
                .map(|mount| daytona_sdk::VolumeMount {
                    volume_id:  mount.volume.clone(),
                    mount_path: mount.mount_path.clone(),
                    subpath:    mount.subpath.clone(),
                })
                .collect()
        }),
        network_block_all,
        network_allow_list,
        domain_allow_list,
        outbound_proxy_url: config.outbound_proxy_url,
        ephemeral: spec.ephemeral.then_some(true),
    })
}

fn validate_supported_creation_fields(spec: &SandboxSpec) -> Result<()> {
    if matches!(&spec.source, SandboxSource::Snapshot { .. })
        && spec.resources != Resources::default()
    {
        return Err(Error::invalid_spec(
            "resources",
            "a Daytona sandbox created from a snapshot inherits the snapshot resources",
        ));
    }
    Ok(())
}

#[async_trait]
impl SandboxProvider for DaytonaProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn create(
        &self,
        spec: &SandboxSpec,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        spec.validate()?;
        validate_supported_creation_fields(spec)?;
        let base = base_params(spec)?;
        let nested_docker = provider_config(&spec.provider_config)?.docker;
        let params = match &spec.source {
            SandboxSource::Image { .. } | SandboxSource::Dockerfile { .. } => {
                if spec.sandbox_kind == Some(SandboxKind::VirtualMachine) {
                    return Err(Error::invalid_spec(
                        "sandbox_kind",
                        "Daytona virtual machines must be created from a virtual-machine snapshot",
                    ));
                }
                // An image or Dockerfile is built once into a snapshot named
                // by its inputs, and every later create with the same inputs
                // reuses it; the resources size the snapshot.
                let snapshot = self.cached_snapshot(spec, events.clone()).await?;
                CreateParams::Snapshot(SnapshotParams {
                    base,
                    snapshot: snapshot.as_str().to_owned(),
                })
            }
            SandboxSource::Snapshot { id } => {
                self.ensure_snapshot_kind(id, spec.sandbox_kind).await?;
                CreateParams::Snapshot(SnapshotParams {
                    base,
                    snapshot: id.as_str().to_owned(),
                })
            }
            SandboxSource::HostDirectory => {
                return Err(Error::invalid_spec(
                    "source",
                    "the daytona provider needs an image, dockerfile, or snapshot source",
                ));
            }
            _ => return Err(Error::invalid_spec("source", "unsupported sandbox source")),
        };

        let budget = if matches!(spec.source, SandboxSource::Dockerfile { .. }) {
            DOCKERFILE_CREATE_TIMEOUT
        } else {
            CREATE_TIMEOUT
        };
        let emitter = EventEmitter::new(self.kind.clone(), events);
        let handle_emitter = emitter.clone();
        emitter
            .run(
                EventSubject::pending_sandbox(spec.name.clone()),
                Action::Create,
                |reporter| async move {
                    let created = self.create_inner(params, budget).await?;
                    let event_id = SandboxId::try_new(created.id.clone())
                        .map_err(|error| Error::invalid_spec("sandbox_id", error.to_string()))?;
                    reporter.set_subject(EventSubject::sandbox(Some(event_id)));
                    let sdk_id = created.id.clone();
                    match self
                        .finish_create(created, spec, nested_docker.as_ref(), handle_emitter)
                        .await
                    {
                        Ok(handle) => Ok(handle as Arc<dyn Sandbox>),
                        Err(error) => Err(self.cleanup_failed_create(&sdk_id, error).await),
                    }
                },
            )
            .await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, sandbox_id = %id),
        err
    )]
    async fn attach(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        let emitter = EventEmitter::new(self.kind.clone(), events);
        let handle_emitter = emitter.clone();
        emitter
            .run(
                EventSubject::sandbox(Some(id.clone())),
                Action::Attach,
                |_| async move {
                    let sdk = self.managed_sandbox(id).await?;
                    Ok(self.handle(sdk, handle_emitter).await? as Arc<dyn Sandbox>)
                },
            )
            .await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, sandbox_id = %id),
        err
    )]
    async fn undelete(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        let emitter = EventEmitter::new(self.kind.clone(), events);
        let handle_emitter = emitter.clone();
        emitter
            .run(
                EventSubject::sandbox(Some(id.clone())),
                Action::Undelete,
                |_| async move {
                    // Daytona names this "recover": a deleted sandbox stays
                    // restorable for 24 hours.
                    let mut sdk = self.managed_sandbox(id).await?;
                    sdk.recover()
                        .await
                        .map_err(|error| daytona_error("undeleting sandbox", error))?;
                    let refreshed = self
                        .client
                        .get(id.as_str())
                        .await
                        .map_err(|error| daytona_error("fetching undeleted sandbox", error))?;
                    Ok(self.handle(refreshed, handle_emitter).await? as Arc<dyn Sandbox>)
                },
            )
            .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn health(&self) -> Result<ProviderHealth> {
        // Reachability and credential acceptance: the cheapest
        // authenticated call.
        if let Err(error) = self.client.list(None, None, Some(1)).await {
            tracing::warn!("daytona provider health check failed");
            let status = match &error {
                DaytonaError::Api {
                    status_code: 401 | 403,
                    ..
                } => HealthStatus::Unauthorized,
                _ => HealthStatus::Unreachable,
            };
            let mut health = ProviderHealth::new(status);
            health.message = Some(format!("listing sandboxes failed: {error}"));
            return Ok(health);
        }
        let mut health = ProviderHealth::new(HealthStatus::Ok);
        // JWT authentication requires an explicit organization. A successful
        // authenticated list above verifies access to that namespace.
        health.identity = self.jwt_health_identity();
        // Scope enumeration works only for API-key credentials; a JWT
        // credential proved itself above and skips it, as does a control
        // plane without key introspection.
        let config = self.client.api_configuration();
        let mut request = config
            .client
            .get(format!("{}/api-keys/current", config.base_path));
        if let Some(token) = &config.bearer_access_token {
            request = request.bearer_auth(token);
        }
        if let Some(organization) = self.client.organization_id() {
            request = request.header("X-Daytona-Organization-ID", organization);
        }
        let key = async {
            let response = request.send().await.ok()?.error_for_status().ok()?;
            response.json::<CurrentApiKey>().await.ok()
        }
        .await;
        if let Some(key) = key {
            // API keys select their organization even when configuration
            // supplies no organization header. The server's value wins.
            if let Some(organization) = key.organization_id.filter(|id| !id.is_empty()) {
                health.identity = Some(format!("organization:{organization}"));
            }
            health.required_permissions = required_permission_names();
            health.missing_permissions = missing_permission_names(&key.permissions);
            if !health.missing_permissions.is_empty() {
                tracing::warn!(
                    missing_permission_count = health.missing_permissions.len(),
                    "daytona credentials lack required permissions"
                );
                health.status = HealthStatus::Unauthorized;
                health.message = Some(format!(
                    "API key {:?} is missing required scopes: {}",
                    key.name,
                    health.missing_permissions.join(", ")
                ));
            }
        }
        Ok(health)
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, label_count = filter.labels.len()),
        err
    )]
    async fn list(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>> {
        let mut labels: HashMap<String, String> = filter
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        labels.insert(MANAGED_LABEL.to_owned(), "true".to_owned());
        let items: Vec<SandboxListItem> = self
            .client
            .list_all(Some(&labels), Some(LIST_PAGE_SIZE))
            .try_collect()
            .await
            .map_err(|error| daytona_error("listing sandboxes", error))?;
        tracing::debug!(item_count = items.len(), "sandbox listing received");
        items
            .iter()
            .map(|item| status_from_sdk(&self.client, item))
            .collect()
    }

    fn snapshots(&self) -> Option<&dyn SnapshotProvider> {
        Some(&self.snapshots)
    }

    fn volumes(&self) -> Option<&dyn VolumeProvider> {
        Some(&self.volumes)
    }
}

#[cfg(test)]
mod tests {
    use sandbox_driver_daytona_config::DockerExecutionTarget;

    use super::*;
    use crate::labels::{TARGET_LABEL, WORKING_DIRECTORY_LABEL};

    #[tokio::test]
    async fn stalled_create_observation_times_out_and_cleans_up_the_known_sandbox() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use crate::fake_daytona::{FakeDaytona, Reply, sandbox_json};

        let deleted = AtomicBool::new(false);
        let created = sandbox_json("creating");
        let fake = FakeDaytona::start(|_| {
            Arc::new(move |method, _| match method {
                "POST" => Reply::Json(201, created.clone()),
                "DELETE" => {
                    deleted.store(true, Ordering::SeqCst);
                    Reply::Json(200, created.clone())
                }
                "GET" if deleted.load(Ordering::SeqCst) => Reply::Json(404, "{}".to_owned()),
                "GET" => Reply::Hang,
                _ => panic!("unexpected method {method}"),
            })
        })
        .await;
        let provider = DaytonaProvider::from_client(fake.client().await, true);
        let params = CreateParams::Snapshot(SnapshotParams {
            base:     SandboxBaseParams::default(),
            snapshot: "base".to_owned(),
        });
        let error = time::timeout(
            Duration::from_secs(3),
            provider.create_inner(params, Duration::from_millis(100)),
        )
        .await
        .expect("the create wait and cleanup are bounded")
        .expect_err("observation times out");
        assert!(matches!(error, Error::Timeout { .. }), "{error:?}");
        assert_eq!(fake.deletes(), 1);
        assert_eq!(
            fake.requests()
                .iter()
                .filter(|request| request.starts_with("POST "))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn connect_explicit_rejects_missing_credentials_without_consulting_the_environment() {
        // Whatever DAYTONA_* the test process carries, an explicit
        // configuration with no credential is an authentication error.
        let Err(error) = DaytonaProvider::connect_explicit(DaytonaConfig::default()).await else {
            panic!("no credential must be rejected");
        };
        assert!(matches!(error, Error::Auth(_)), "{error}");
        let Err(error) = DaytonaProvider::connect_explicit(DaytonaConfig {
            api_key: Some(String::new()),
            jwt_token: Some(String::new()),
            ..DaytonaConfig::default()
        })
        .await
        else {
            panic!("blank credentials must be rejected");
        };
        assert!(matches!(error, Error::Auth(_)), "{error}");
    }

    #[tokio::test]
    async fn connect_explicit_uses_only_the_supplied_values() {
        let provider = DaytonaProvider::connect_explicit(DaytonaConfig {
            api_key: Some("explicit-key".to_owned()),
            user_agent: Some("embedding-app/1.0".to_owned()),
            ..DaytonaConfig::default()
        })
        .await
        .unwrap_or_else(|error| panic!("explicit API key connects: {error}"));
        assert!(provider.uses_api_key);
        let api = provider.client.api_configuration();
        // Unset fields take the SDK defaults rather than environment values.
        assert_eq!(api.base_path, "https://app.daytona.io/api");
        assert_eq!(api.bearer_access_token.as_deref(), Some("explicit-key"));
        assert_eq!(api.user_agent.as_deref(), Some("embedding-app/1.0"));
        assert_eq!(provider.client.organization_id(), None);

        let provider = DaytonaProvider::connect_explicit(DaytonaConfig {
            jwt_token: Some("explicit-jwt".to_owned()),
            organization_id: Some("org-1".to_owned()),
            api_url: Some("https://daytona.example/api".to_owned()),
            ..DaytonaConfig::default()
        })
        .await
        .unwrap_or_else(|error| panic!("explicit JWT connects: {error}"));
        assert!(!provider.uses_api_key);
        assert_eq!(
            provider.client.api_configuration().base_path,
            "https://daytona.example/api"
        );
        assert_eq!(provider.client.organization_id(), Some("org-1"));
    }

    #[test]
    fn missing_permissions_follow_the_documented_order() {
        assert_eq!(required_permission_names(), [
            "write:snapshots",
            "delete:snapshots",
            "write:sandboxes",
            "delete:sandboxes"
        ]);
        // Granted out of order and partial: the report keeps the documented order.
        let granted = [Permissions::DELETE_SANDBOXES, Permissions::WRITE_SNAPSHOTS];
        assert_eq!(missing_permission_names(&granted), [
            "delete:snapshots",
            "write:sandboxes"
        ]);
        assert!(
            missing_permission_names(&[
                Permissions::WRITE_SNAPSHOTS,
                Permissions::DELETE_SNAPSHOTS,
                Permissions::WRITE_SANDBOXES,
                Permissions::DELETE_SANDBOXES,
            ])
            .is_empty()
        );
    }

    #[test]
    fn current_key_identity_uses_its_organization_and_ignores_credentials() {
        let first: CurrentApiKey = serde_json::from_value(serde_json::json!({
            "name": "original", "value": "masked-original", "userId": "user-a",
            "organizationId": "organization-a", "permissions": []
        }))
        .expect("current key");
        let rotated: CurrentApiKey = serde_json::from_value(serde_json::json!({
            "name": "rotated", "value": "masked-rotated", "userId": "user-b",
            "organizationId": "organization-a", "permissions": []
        }))
        .expect("rotated key in the same organization");
        let changed: CurrentApiKey = serde_json::from_value(serde_json::json!({
            "name": "other", "organizationId": "organization-b", "permissions": []
        }))
        .expect("key in another organization");
        assert_eq!(first.organization_id, rotated.organization_id);
        assert_ne!(first.organization_id, changed.organization_id);
    }

    #[tokio::test]
    async fn explicit_connection_config_reaches_the_sdk_client() {
        let provider = DaytonaProvider::connect_with_config(DaytonaConfig {
            api_key: Some("dtn_vault_resolved".to_owned()),
            organization_id: Some("org-1".to_owned()),
            api_url: Some("https://daytona.example/api".to_owned()),
            ..DaytonaConfig::default()
        })
        .await
        .expect("explicit Daytona configuration should initialize the SDK client");

        let sdk_config = provider.client.api_configuration();
        assert_eq!(
            sdk_config.bearer_access_token.as_deref(),
            Some("dtn_vault_resolved")
        );
        assert_eq!(sdk_config.base_path, "https://daytona.example/api");
        assert_eq!(provider.client.organization_id(), Some("org-1"));
        assert_eq!(
            provider.jwt_health_identity(),
            None,
            "an API key's effective organization must come from the server"
        );
    }

    #[tokio::test]
    async fn jwt_credentials_use_the_configured_organization_as_health_identity() {
        let provider = DaytonaProvider::connect_with_config(DaytonaConfig {
            api_key: Some(String::new()),
            jwt_token: Some("jwt-vault-resolved".to_owned()),
            organization_id: Some("org-1".to_owned()),
            api_url: Some("https://daytona.example/api".to_owned()),
            ..DaytonaConfig::default()
        })
        .await
        .expect("explicit JWT configuration initializes the SDK client");
        assert_eq!(
            provider.jwt_health_identity().as_deref(),
            Some("organization:org-1")
        );
        assert_eq!(
            provider
                .client
                .api_configuration()
                .bearer_access_token
                .as_deref(),
            Some("jwt-vault-resolved")
        );
    }

    #[test]
    fn sandbox_class_narrows_lifecycle_capabilities() {
        let base = daytona_capabilities();

        let container = narrowed_capabilities(&base, Some(DaytonaSandboxClass::CONTAINER));
        assert!(container.lifecycle.archive);
        assert!(!container.lifecycle.pause);
        assert!(!container.lifecycle.fork);

        let linux_vm = narrowed_capabilities(&base, Some(DaytonaSandboxClass::LINUX_VM));
        assert!(!linux_vm.lifecycle.archive);
        assert!(linux_vm.lifecycle.pause);
        assert!(linux_vm.lifecycle.fork);

        let android = narrowed_capabilities(&base, Some(DaytonaSandboxClass::ANDROID));
        assert!(!android.lifecycle.archive);
        assert!(!android.lifecycle.pause);
        assert!(!android.lifecycle.fork);
    }

    #[test]
    fn snapshot_capabilities_distinguish_container_and_vm_builds() {
        let caps = daytona_capabilities();
        let snapshots = caps.snapshots.expect("snapshot capabilities");
        assert!(snapshots.from_image_kinds.container);
        assert!(snapshots.from_image_kinds.virtual_machine);
        assert!(snapshots.from_dockerfile_kinds.container);
        assert!(!snapshots.from_dockerfile_kinds.virtual_machine);
    }

    #[test]
    fn base_params_store_the_requested_working_directory_as_internal_metadata() {
        let spec = SandboxSpec::new(SandboxSource::Image {
            reference: "debian:stable-slim".to_owned(),
        })
        .label(WORKING_DIRECTORY_LABEL, "/caller-cannot-override")
        .working_directory("/workspace/final");

        let base = base_params(&spec).expect("valid base params");
        let labels = base.labels.expect("managed labels");
        assert_eq!(
            labels.get(WORKING_DIRECTORY_LABEL).map(String::as_str),
            Some("/workspace/final")
        );
        assert_eq!(labels.get(MANAGED_LABEL).map(String::as_str), Some("true"));
    }

    #[test]
    fn base_params_blank_bash_env_over_any_caller_value() {
        let spec = SandboxSpec::new(SandboxSource::Image {
            reference: "debian:stable-slim".to_owned(),
        })
        .env_var("BASH_ENV", "/etc/injected.sh")
        .env_var("KEEP", "1");

        let base = base_params(&spec).expect("valid base params");
        let env = base.env_vars.expect("env vars");
        // Sandbox-level blank: the inner bash of a session exec sources
        // $BASH_ENV at startup, before any per-exec strip can run.
        assert_eq!(env.get("BASH_ENV").map(String::as_str), Some(""));
        assert_eq!(env.get("KEEP").map(String::as_str), Some("1"));
    }

    #[test]
    fn nested_docker_preserves_visibility_and_only_nonsecret_target_metadata() {
        let mut spec = SandboxSpec::new(SandboxSource::Image {
            reference: "runner:dind".to_owned(),
        });
        spec.labels
            .insert(TARGET_LABEL.to_owned(), "spoofed".to_owned());
        let options = sandbox_driver_docker::DockerProviderConfig {
            registry_auth: Some(sandbox_driver_docker::RegistryAuth {
                username: "private-user".to_owned(),
                password: "private-password".to_owned(),
                server:   None,
            }),
            ..Default::default()
        };
        spec.provider_config = DaytonaProviderConfig {
            docker: Some(NestedDockerConfig {
                image: "private-image".to_owned(),
                target: DockerExecutionTarget::Container,
                user: None,
                options,
            }),
            ..Default::default()
        }
        .into_value();
        let base = base_params(&spec).unwrap();
        assert_eq!(base.public, None);
        let labels = base.labels.unwrap();
        assert_eq!(labels[TARGET_LABEL], "container");
        assert!(!format!("{labels:?}").contains("private-"));
        spec.public = Some(true);
        assert_eq!(base_params(&spec).unwrap().public, Some(true));
    }

    #[test]
    fn nested_docker_rejects_vm_sidecars_and_caller_binds_before_provisioning() {
        let mut spec = SandboxSpec::new(SandboxSource::Image {
            reference: "runner:dind".to_owned(),
        });
        for docker in [
            serde_json::json!({"image":"alpine", "target":"virtual_machine", "options":{"sidecars":[{"name":"db","image":"postgres"}]}}),
            serde_json::json!({"image":"alpine", "options":{"binds":[{"host":"/private","container":"/workspace"}]}}),
            serde_json::json!({"image":"alpine", "options":{"host_network":true}}),
            serde_json::json!({"image":" "}),
            serde_json::json!({"image":"alpine", "typo":true}),
        ] {
            spec.provider_config = serde_json::json!({"docker":docker});
            assert!(matches!(base_params(&spec), Err(Error::InvalidSpec { .. })));
        }
    }

    #[test]
    fn base_params_do_not_accept_internal_metadata_as_a_user_label() {
        let spec = SandboxSpec::new(SandboxSource::Image {
            reference: "debian:stable-slim".to_owned(),
        })
        .label(WORKING_DIRECTORY_LABEL, "/caller-injected");

        let base = base_params(&spec).expect("valid base params");
        let labels = base.labels.expect("managed labels");
        assert!(!labels.contains_key(WORKING_DIRECTORY_LABEL));
    }

    #[test]
    fn snapshot_sandbox_resource_overrides_return_invalid_spec() {
        let mut spec = SandboxSpec::new(SandboxSource::Snapshot {
            id: SnapshotId::try_new("snapshot").expect("valid snapshot id"),
        });
        spec.resources.cpu_cores = Some(2);

        let error = validate_supported_creation_fields(&spec)
            .expect_err("snapshot resources should be inherited");
        assert!(matches!(error, Error::InvalidSpec { field, .. } if field == "resources"));
    }

    #[test]
    fn image_sandbox_resource_overrides_pass_creation_field_validation() {
        let mut spec = SandboxSpec::new(SandboxSource::Image {
            reference: "debian:stable-slim".to_owned(),
        });
        spec.resources.cpu_cores = Some(2);
        spec.resources.memory_mb = Some(4096);
        spec.resources.disk_mb = Some(10 * 1024);
        spec.resources.gpus = Some(1);

        validate_supported_creation_fields(&spec).expect("image resources are supported");
    }

    #[test]
    fn cached_snapshot_names_are_stable_per_tenant_and_input() {
        let image = SnapshotSource::Image {
            reference: "ubuntu:24.04".to_owned(),
        };
        let mut resources = Resources::default();
        resources.cpu_cores = Some(2);
        resources.memory_mb = Some(4096);
        let name = cached_snapshot_name("key-a", &image, &resources, None);
        assert!(name.starts_with("sandbox-driver-"), "{name}");
        assert_eq!(name.len(), "sandbox-driver-".len() + 32, "{name}");
        assert_eq!(
            name,
            cached_snapshot_name("key-a", &image, &resources, None),
            "the same inputs name the same snapshot"
        );
        assert_ne!(
            name,
            cached_snapshot_name("key-b", &image, &resources, None),
            "another credential never shares a name"
        );
        let mut larger = resources;
        larger.memory_mb = Some(8192);
        assert_ne!(name, cached_snapshot_name("key-a", &image, &larger, None));
        let dockerfile = SnapshotSource::Dockerfile {
            content: "FROM ubuntu:24.04".to_owned(),
        };
        assert_ne!(
            name,
            cached_snapshot_name("key-a", &dockerfile, &resources, None)
        );
        // Sizing rounds to whole gigabytes, so two requests inside one
        // gigabyte share a snapshot.
        let mut rounded = resources;
        rounded.memory_mb = Some(3900);
        assert_eq!(name, cached_snapshot_name("key-a", &image, &rounded, None));
    }
}
