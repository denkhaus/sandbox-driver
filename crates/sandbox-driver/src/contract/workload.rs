use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use serde::{Deserialize, Serialize, de};
use serde_json::Value;

use super::units::{Mib, Millis};
use super::{ContainerField, Error, NetworkMode, ResourceCapability, SourceKind, TimerCapability};
use crate::id::validate_label;
use crate::redact::EnvKeys;
use crate::{SnapshotId, WorkspaceOwnership};

/// The caller's workload, never a helper image or outer runner.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Image(String),
    /// Dockerfile content.
    Dockerfile(String),
    Snapshot(SnapshotId),
    /// No image. Workspace path/ownership are carried by the spec.
    HostDirectory,
}

// Dockerfile content can embed build secrets; `Debug` redacts it.
impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image(reference) => f.debug_tuple("Image").field(reference).finish(),
            Self::Dockerfile(_) => f.debug_tuple("Dockerfile").field(&"<redacted>").finish(),
            Self::Snapshot(id) => f.debug_tuple("Snapshot").field(id).finish(),
            Self::HostDirectory => f.write_str("HostDirectory"),
        }
    }
}

impl Source {
    pub(super) fn kind(&self) -> SourceKind {
        match self {
            Self::Image(_) => SourceKind::Image,
            Self::Dockerfile(_) => SourceKind::Dockerfile,
            Self::Snapshot(_) => SourceKind::Snapshot,
            Self::HostDirectory => SourceKind::HostDirectory,
        }
    }
}

/// Resource requests in whole cores and MiB. Decoding also accepts the
/// template spelling, `cpu = 2` and `memory = "4GB"`, through the one size
/// grammar; the value is normalized once and always serializes as MiB, so
/// providers never see the text.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Resources {
    #[serde(alias = "cpu")]
    pub cpu_cores: Option<NonZeroU32>,
    #[serde(alias = "memory", deserialize_with = "size_mib")]
    pub memory_mb: Option<NonZeroU64>,
    #[serde(alias = "disk", deserialize_with = "size_mib")]
    pub disk_mb:   Option<NonZeroU64>,
    pub gpus:      Option<u32>,
}

impl Resources {
    /// Capabilities this request needs, with their spec paths. Destructuring
    /// makes every new field declare its capability here.
    pub(super) fn requested(&self) -> impl Iterator<Item = (ResourceCapability, &'static str)> {
        let Self {
            cpu_cores,
            memory_mb,
            disk_mb,
            gpus,
        } = self;
        set_fields([
            (
                cpu_cores.is_some(),
                ResourceCapability::Cpu,
                "resources.cpu_cores",
            ),
            (
                memory_mb.is_some(),
                ResourceCapability::Memory,
                "resources.memory_mb",
            ),
            (
                disk_mb.is_some(),
                ResourceCapability::Disk,
                "resources.disk_mb",
            ),
            (
                gpus.is_some_and(|count| count != 0),
                ResourceCapability::Gpus,
                "resources.gpus",
            ),
        ])
    }
}

/// Keeps the capabilities whose field is set.
fn set_fields<C, const N: usize>(
    fields: [(bool, C, &'static str); N],
) -> impl Iterator<Item = (C, &'static str)> {
    fields
        .into_iter()
        .filter_map(|(set, capability, path)| set.then_some((capability, path)))
}

fn size_mib<'de, D: de::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<NonZeroU64>, D::Error> {
    Option::<Mib>::deserialize(deserializer)?
        .map(|Mib(value)| {
            NonZeroU64::new(value).ok_or_else(|| de::Error::custom("must not be zero"))
        })
        .transpose()
}

fn duration_ms<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    Ok(Option::<Millis>::deserialize(deserializer)?.map(|Millis(value)| value))
}

/// Egress policy. The allow-list modes carry their entries under one `allow`
/// key; the mode says how to read them. Decoding rejects an allow-list on a
/// mode that does not take one, so a misplaced restriction is never silently
/// dropped.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", try_from = "RawNetworkPolicy")]
pub enum NetworkPolicy {
    #[default]
    ProviderDefault,
    AllowAll,
    Block,
    CidrAllowList {
        #[serde(rename = "allow")]
        cidrs: Vec<String>,
    },
    DomainAllowList {
        #[serde(rename = "allow")]
        domains: Vec<String>,
    },
}

// Serde's `deny_unknown_fields` does not reach unit variants of an internally
// tagged enum, so decoding goes through this flat form instead.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNetworkPolicy {
    mode:  NetworkMode,
    allow: Option<Vec<String>>,
}

impl TryFrom<RawNetworkPolicy> for NetworkPolicy {
    type Error = &'static str;

    fn try_from(raw: RawNetworkPolicy) -> Result<Self, Self::Error> {
        match (raw.mode, raw.allow) {
            (NetworkMode::CidrAllowList, Some(cidrs)) => Ok(Self::CidrAllowList { cidrs }),
            (NetworkMode::DomainAllowList, Some(domains)) => Ok(Self::DomainAllowList { domains }),
            (NetworkMode::CidrAllowList | NetworkMode::DomainAllowList, None) => {
                Err("an allow-list mode requires allow")
            }
            (_, Some(_)) => Err("allow requires mode cidr_allow_list or domain_allow_list"),
            (NetworkMode::ProviderDefault, None) => Ok(Self::ProviderDefault),
            (NetworkMode::AllowAll, None) => Ok(Self::AllowAll),
            (NetworkMode::Block, None) => Ok(Self::Block),
        }
    }
}

impl NetworkPolicy {
    /// The capability this policy needs, with its spec path. Every provider
    /// honours its own default, so the default requests nothing, whether the
    /// field is omitted or explicit.
    pub(super) fn requested(&self) -> Option<(NetworkMode, &'static str)> {
        let mode = match self {
            Self::ProviderDefault => return None,
            Self::AllowAll => NetworkMode::AllowAll,
            Self::Block => NetworkMode::Block,
            Self::CidrAllowList { .. } => NetworkMode::CidrAllowList,
            Self::DomainAllowList { .. } => NetworkMode::DomainAllowList,
        };
        Some((mode, "network"))
    }

    /// Rejects an empty allow-list: providers treat it as no restriction, the
    /// opposite of what an emptied-out filter intends.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::CidrAllowList { cidrs: allow } | Self::DomainAllowList { domains: allow }
                if allow.is_empty() =>
            {
                Err(Error::invalid_spec("network.allow", "must not be empty"))
            }
            _ => Ok(()),
        }
    }
}

/// Backend timers, in milliseconds. None inherits a provider default; zero
/// explicitly disables the timer. Application run-end policy is not a timer.
/// Decoding also accepts the template spelling, `auto_stop = "60m"`, and
/// always serializes milliseconds.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Timers {
    #[serde(alias = "auto_stop", deserialize_with = "duration_ms")]
    pub auto_stop_ms:    Option<u64>,
    #[serde(alias = "auto_archive", deserialize_with = "duration_ms")]
    pub auto_archive_ms: Option<u64>,
    #[serde(alias = "auto_delete", deserialize_with = "duration_ms")]
    pub auto_delete_ms:  Option<u64>,
    #[serde(alias = "ttl", deserialize_with = "duration_ms")]
    pub ttl_ms:          Option<u64>,
}

impl Timers {
    /// Capabilities this request needs, with their spec paths. Zero disables a
    /// timer, which is still a request.
    pub(super) fn requested(&self) -> impl Iterator<Item = (TimerCapability, &'static str)> {
        let Self {
            auto_stop_ms,
            auto_archive_ms,
            auto_delete_ms,
            ttl_ms,
        } = self;
        set_fields([
            (
                auto_stop_ms.is_some(),
                TimerCapability::AutoStop,
                "timers.auto_stop_ms",
            ),
            (
                auto_archive_ms.is_some(),
                TimerCapability::AutoArchive,
                "timers.auto_archive_ms",
            ),
            (
                auto_delete_ms.is_some(),
                TimerCapability::AutoDelete,
                "timers.auto_delete_ms",
            ),
            (ttl_ms.is_some(), TimerCapability::Ttl, "timers.ttl_ms"),
        ])
    }
}

/// Portable workload declaration. This unvalidated DTO is deliberately
/// separate from `crate::SandboxSpec`: legacy providers must not silently
/// ignore services, container options, or the new workload-target semantics.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxSpec {
    pub source:              Source,
    #[serde(default)]
    pub name:                Option<String>,
    #[serde(default)]
    pub resources:           Resources,
    #[serde(default)]
    pub network:             NetworkPolicy,
    #[serde(default)]
    pub timers:              Timers,
    #[serde(default)]
    pub labels:              BTreeMap<String, String>,
    #[serde(default)]
    pub env:                 BTreeMap<String, String>,
    #[serde(default)]
    pub working_directory:   Option<String>,
    #[serde(default)]
    pub workspace_ownership: Option<WorkspaceOwnership>,
    #[serde(default)]
    pub user:                Option<String>,
    #[serde(default)]
    pub services:            BTreeMap<String, ServiceSpec>,
    #[serde(default)]
    pub container:           Option<ContainerSpec>,
    #[serde(default)]
    pub provider_config:     BTreeMap<String, Value>,
}

impl SandboxSpec {
    pub fn new(source: Source) -> Self {
        Self {
            source,
            name: None,
            resources: Resources::default(),
            network: NetworkPolicy::default(),
            timers: Timers::default(),
            labels: BTreeMap::new(),
            env: BTreeMap::new(),
            working_directory: None,
            workspace_ownership: None,
            user: None,
            services: BTreeMap::new(),
            container: None,
            provider_config: BTreeMap::new(),
        }
    }

    /// Checks portable invariants only. Descriptor capabilities, provider
    /// schemas, credentials, and backend-specific restrictions still need
    /// validation before admission; a successful check does not start work.
    pub fn validate(&self) -> Result<(), Error> {
        match &self.source {
            Source::Image(value) | Source::Dockerfile(value) if value.trim().is_empty() => {
                return Err(Error::invalid_spec("source", "must not be empty"));
            }
            _ => {}
        }
        for (path, value) in [
            ("name", &self.name),
            ("user", &self.user),
            ("working_directory", &self.working_directory),
        ] {
            if value.as_deref() == Some("") {
                return Err(Error::invalid_spec(path, "must not be empty"));
            }
        }
        validate_env("env", &self.env)?;
        if self.workspace_ownership.is_some() && self.source != Source::HostDirectory {
            return Err(Error::invalid_spec(
                "workspace_ownership",
                "requires host_directory",
            ));
        }
        if self.workspace_ownership == Some(WorkspaceOwnership::Designated)
            && self.working_directory.is_none()
        {
            return Err(Error::invalid_spec(
                "working_directory",
                "designated workspace requires a path",
            ));
        }
        self.network.validate()?;
        if let Some(container) = &self.container {
            container.validate("container")?;
        }
        for (name, service) in &self.services {
            // DNS label: service names become network aliases.
            validate_label("service name", name, 63)
                .map_err(|error| Error::invalid_spec("services", error.to_string()))?;
            service.validate(&format!("services.{name}"))?;
        }
        Ok(())
    }
}

/// Environment keys become `KEY=VALUE` pairs, so `=` and NUL would change or
/// truncate the variable that reaches the workload.
pub(super) fn validate_env(path: &str, env: &BTreeMap<String, String>) -> Result<(), Error> {
    for (key, value) in env {
        if !valid_env_key(key) {
            return Err(Error::invalid_spec(
                path,
                "keys must be non-empty and must not contain '=' or NUL",
            ));
        }
        if value.contains('\0') {
            return Err(Error::invalid_spec(path, "values must not contain NUL"));
        }
    }
    Ok(())
}

pub(super) fn valid_env_key(key: &str) -> bool {
    !key.is_empty() && !key.contains(['=', '\0'])
}

/// Argv is executed without a shell: the executable must be named, and NUL
/// would truncate an argument.
pub(super) fn validate_argv(path: &str, argv: &[String]) -> Result<(), Error> {
    if argv.first().is_none_or(String::is_empty) {
        return Err(Error::invalid_spec(path, "requires a non-empty executable"));
    }
    if argv.iter().any(|arg| arg.contains('\0')) {
        return Err(Error::invalid_spec(path, "arguments must not contain NUL"));
    }
    Ok(())
}

/// Paths, references and entrypoints reach the backend as C strings, so an
/// empty value names nothing and NUL would truncate it.
pub(super) fn validate_name(path: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() || value.contains('\0') {
        return Err(Error::invalid_spec(path, "must be non-empty and NUL-free"));
    }
    Ok(())
}

// `env` and `provider_config` can carry credentials, so `Debug` shows env keys
// only. Destructuring makes every new field choose a representation here.
impl fmt::Debug for SandboxSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            source,
            name,
            resources,
            network,
            timers,
            labels,
            env,
            working_directory,
            workspace_ownership,
            user,
            services,
            container,
            provider_config: _,
        } = self;
        f.debug_struct("SandboxSpec")
            .field("source", source)
            .field("name", name)
            .field("resources", resources)
            .field("network", network)
            .field("timers", timers)
            .field("labels", labels)
            .field("env_keys", &EnvKeys(env))
            .field("working_directory", working_directory)
            .field("workspace_ownership", workspace_ownership)
            .field("user", user)
            .field("services", services)
            .field("container", container)
            .field("provider_config", &"<redacted>")
            .finish()
    }
}

/// A sibling service container. Credentials are names resolved by the driver,
/// not registry passwords. Readiness commands execute *inside this service*.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSpec {
    pub image:      String,
    #[serde(default)]
    pub env:        BTreeMap<String, String>,
    /// TCP ports to make reachable from the workload; not host-public mappings.
    #[serde(default)]
    pub ports:      Vec<NonZeroU16>,
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default)]
    pub ready:      Option<ReadinessCheck>,
}

impl ServiceSpec {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image:      image.into(),
            env:        BTreeMap::new(),
            ports:      Vec::new(),
            credential: None,
            ready:      None,
        }
    }

    /// `path` locates this service in the enclosing spec's error paths.
    fn validate(&self, path: &str) -> Result<(), Error> {
        if self.image.trim().is_empty() {
            return Err(Error::invalid_spec(
                format!("{path}.image"),
                "must not be empty",
            ));
        }
        if self.credential.as_deref() == Some("") {
            return Err(Error::invalid_spec(
                format!("{path}.credential"),
                "must not be empty",
            ));
        }
        validate_env(&format!("{path}.env"), &self.env)?;
        if let Some(ready) = &self.ready {
            ready.validate(&format!("{path}.ready"))?;
        }
        Ok(())
    }
}

impl fmt::Debug for ServiceSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            image,
            env,
            ports,
            credential,
            ready,
        } = self;
        f.debug_struct("ServiceSpec")
            .field("image", image)
            .field("env_keys", &EnvKeys(env))
            .field("ports", ports)
            .field("credential", credential)
            .field("ready", ready)
            .finish()
    }
}

/// Finite readiness budget. `timeout_ms` caps each attempt, `deadline_ms` caps
/// all attempts and waits, and `retries` is the maximum number of attempts.
/// Each attempt must fit within the deadline, and so must the interval when a
/// second attempt is allowed. A zero exit code passes; timeout/nonzero fails
/// that attempt. No shell is implied. Absence of a check requires only that the
/// service has started.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessCheck {
    pub command:     Vec<String>,
    pub interval_ms: NonZeroU64,
    pub timeout_ms:  NonZeroU64,
    pub deadline_ms: NonZeroU64,
    pub retries:     NonZeroU32,
}

impl ReadinessCheck {
    /// `path` locates this check in the enclosing spec's error paths.
    fn validate(&self, path: &str) -> Result<(), Error> {
        validate_argv(&format!("{path}.command"), &self.command)?;
        if self.timeout_ms > self.deadline_ms {
            return Err(Error::invalid_spec(
                format!("{path}.timeout_ms"),
                "must not exceed deadline_ms",
            ));
        }
        if self.retries.get() > 1 && self.interval_ms >= self.deadline_ms {
            return Err(Error::invalid_spec(
                format!("{path}.interval_ms"),
                "must be shorter than deadline_ms when retries allows a second attempt",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ReadinessCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            command: _,
            interval_ms,
            timeout_ms,
            deadline_ms,
            retries,
        } = self;
        f.debug_struct("ReadinessCheck")
            .field("command", &"<redacted>")
            .field("interval_ms", interval_ms)
            .field("timeout_ms", timeout_ms)
            .field("deadline_ms", deadline_ms)
            .field("retries", retries)
            .finish()
    }
}

/// Requests about the workload container, never instructions for an outer
/// runner. None leaves an option unspecified; Some(false) explicitly disables
/// it and must not be silently ignored on an unsupported provider.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContainerSpec {
    pub init:              Option<bool>,
    pub privileged:        Option<bool>,
    pub capabilities_add:  Vec<String>,
    pub capabilities_drop: Vec<String>,
    pub dns:               Vec<String>,
    pub extra_hosts:       BTreeMap<String, String>,
    pub shm_size_mb:       Option<NonZeroU64>,
}

impl ContainerSpec {
    /// Fields this request sets, with their spec paths. An explicit `false`
    /// is still a request. Destructuring makes every new field declare its
    /// capability here.
    pub(super) fn requested(&self) -> impl Iterator<Item = (ContainerField, &'static str)> {
        let Self {
            init,
            privileged,
            capabilities_add,
            capabilities_drop,
            dns,
            extra_hosts,
            shm_size_mb,
        } = self;
        set_fields([
            (init.is_some(), ContainerField::Init, "container.init"),
            (
                privileged.is_some(),
                ContainerField::Privileged,
                "container.privileged",
            ),
            (
                !capabilities_add.is_empty(),
                ContainerField::CapabilitiesAdd,
                "container.capabilities_add",
            ),
            (
                !capabilities_drop.is_empty(),
                ContainerField::CapabilitiesDrop,
                "container.capabilities_drop",
            ),
            (!dns.is_empty(), ContainerField::Dns, "container.dns"),
            (
                !extra_hosts.is_empty(),
                ContainerField::ExtraHosts,
                "container.extra_hosts",
            ),
            (
                shm_size_mb.is_some(),
                ContainerField::ShmSizeMb,
                "container.shm_size_mb",
            ),
        ])
    }

    /// `path` locates these options in the enclosing spec's error paths.
    fn validate(&self, path: &str) -> Result<(), Error> {
        for (field, entries) in [
            ("capabilities_add", &self.capabilities_add),
            ("capabilities_drop", &self.capabilities_drop),
            ("dns", &self.dns),
        ] {
            if entries.iter().any(|entry| entry.trim().is_empty()) {
                return Err(Error::invalid_spec(
                    format!("{path}.{field}"),
                    "entries must not be empty",
                ));
            }
        }
        if self
            .extra_hosts
            .iter()
            .any(|(host, address)| host.trim().is_empty() || address.trim().is_empty())
        {
            return Err(Error::invalid_spec(
                format!("{path}.extra_hosts"),
                "hosts and addresses must not be empty",
            ));
        }
        Ok(())
    }
}
