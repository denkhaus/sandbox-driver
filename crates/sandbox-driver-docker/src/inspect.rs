//! Readback helpers: what a container inspect says about the sandbox it
//! backs, and the status built from it.

use std::collections::BTreeMap;

use bollard::models::{
    ContainerInspectResponse, ContainerStateStatusEnum, Mount, MountPointTypeEnum, MountTypeEnum,
};
use sandbox_driver::{
    BASH_ENV_VAR, NetworkPolicy, SandboxId, SandboxKind, SandboxState, SandboxStatus,
};

use crate::one_shot::ONE_SHOT_LABEL;
use crate::{DEFAULT_WORKING_DIRECTORY, MANAGED_LABEL, SIDECAR_NETWORK_LABEL, fence};

/// What a sandbox handle needs to know about its container, read from
/// one inspect. `create` and `attach` build handles from the same facts,
/// so a handle behaves the same however it was obtained.
pub(crate) struct ContainerFacts {
    pub(crate) id:              String,
    pub(crate) name:            Option<String>,
    pub(crate) working_dir:     String,
    /// The caller's labels, without the provider's own.
    pub(crate) labels:          BTreeMap<String, String>,
    /// The container's configured environment, which every exec reapplies
    /// past the wrapper shell.
    pub(crate) env:             BTreeMap<String, String>,
    pub(crate) sidecar_network: Option<String>,
    pub(crate) workspace:       Option<Mount>,
}

impl ContainerFacts {
    /// Reads the facts from an inspect; `fallback_id` names the container
    /// when the daemon's answer carries no id.
    pub(crate) fn from_inspect(inspect: &ContainerInspectResponse, fallback_id: &str) -> Self {
        let config = inspect.config.as_ref();
        let working_dir = config
            .and_then(|config| config.working_dir.clone())
            .unwrap_or_else(|| DEFAULT_WORKING_DIRECTORY.to_owned());
        let labels = config
            .and_then(|config| config.labels.as_ref())
            .map(|labels| {
                labels
                    .iter()
                    .filter(|(key, _)| !is_internal_label(key))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            id: inspect.id.clone().unwrap_or_else(|| fallback_id.to_owned()),
            name: normalized_container_name(inspect.name.as_deref()),
            workspace: workspace_of(inspect, &working_dir),
            working_dir,
            labels,
            env: configured_env(inspect),
            sidecar_network: sidecar_network_of(inspect),
        }
    }
}

/// The container's configured environment: the image's plus what create
/// set, which the daemon records on `.Config.Env`. `BASH_ENV` is an
/// internal blank and never part of it.
pub(crate) fn configured_env(inspect: &ContainerInspectResponse) -> BTreeMap<String, String> {
    let entries = inspect
        .config
        .as_ref()
        .and_then(|config| config.env.as_ref())
        .into_iter()
        .flatten();
    let mut env = BTreeMap::new();
    for entry in entries {
        if let Some((key, value)) = entry.split_once('=') {
            if key != BASH_ENV_VAR {
                env.insert(key.to_owned(), value.to_owned());
            }
        }
    }
    env
}

pub(crate) fn map_state(inspect: &ContainerInspectResponse) -> SandboxState {
    let Some(state) = &inspect.state else {
        return SandboxState::Unknown;
    };
    if state.paused == Some(true) {
        return SandboxState::Paused;
    }
    match state.status {
        Some(ContainerStateStatusEnum::RUNNING) => SandboxState::Running,
        Some(ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::EXITED) => {
            SandboxState::Stopped
        }
        Some(ContainerStateStatusEnum::PAUSED) => SandboxState::Paused,
        Some(ContainerStateStatusEnum::RESTARTING) => SandboxState::Starting,
        Some(ContainerStateStatusEnum::REMOVING) => SandboxState::Deleting,
        Some(ContainerStateStatusEnum::DEAD) => SandboxState::Error,
        Some(ContainerStateStatusEnum::EMPTY) | None => SandboxState::Unknown,
    }
}

pub(crate) fn status_from_inspect(
    id: SandboxId,
    inspect: &ContainerInspectResponse,
) -> SandboxStatus {
    let mut status = SandboxStatus::new(id, map_state(inspect));
    status.name = normalized_container_name(inspect.name.as_deref());
    status.sandbox_kind = Some(SandboxKind::Container);
    status.provider_state = inspect
        .state
        .as_ref()
        .and_then(|state| state.status)
        .map(|state| state.to_string())
        .unwrap_or_default();
    if let Some(config) = &inspect.config {
        if let Some(labels) = &config.labels {
            status.labels = labels
                .iter()
                .filter(|(key, _)| !is_internal_label(key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
        }
        status.image.clone_from(&config.image);
    }
    status.network = network_of(inspect);
    status
}

/// The network policy an inspected container runs under, read back from
/// its network mode: `none` is blocked, the default bridge and host
/// networking are unrestricted. A sandbox on a managed sidecar network, or
/// any other mode, reports nothing rather than guess.
fn network_of(inspect: &ContainerInspectResponse) -> Option<NetworkPolicy> {
    let mode = inspect
        .host_config
        .as_ref()
        .and_then(|host| host.network_mode.as_deref())
        .unwrap_or("default");
    match mode {
        "none" => Some(NetworkPolicy::Block),
        "default" | "bridge" | "host" => Some(NetworkPolicy::AllowAll),
        _ => None,
    }
}

/// Whether an inspected container is one this provider created. Anything
/// else is not a sandbox, whatever else it is.
pub(crate) fn is_managed(inspect: &ContainerInspectResponse) -> bool {
    label_of(inspect, MANAGED_LABEL) == Some("true")
}

/// One label of an inspected container.
pub(crate) fn label_of<'a>(inspect: &'a ContainerInspectResponse, key: &str) -> Option<&'a str> {
    inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.as_ref())
        .and_then(|labels| labels.get(key))
        .map(String::as_str)
}

/// The managed sidecar network an inspected container joined, if any: a
/// user-defined network named after the sandbox. New containers record it
/// before creating dependencies; older containers used their network mode.
pub(crate) fn sidecar_network_of(inspect: &ContainerInspectResponse) -> Option<String> {
    label_of(inspect, SIDECAR_NETWORK_LABEL)
        .map(str::to_owned)
        .or_else(|| {
            inspect
                .host_config
                .as_ref()
                .and_then(|host| host.network_mode.clone())
                .filter(|mode| is_sidecar_network(mode))
        })
}

/// Where an inspected container's workspace lives: the volume or bind
/// mounted at its working directory, which its one-shot containers share.
pub(crate) fn workspace_of(inspect: &ContainerInspectResponse, working_dir: &str) -> Option<Mount> {
    let target = working_dir.trim_end_matches('/');
    inspect.mounts.as_ref()?.iter().find_map(|mount| {
        let destination = mount.destination.as_deref()?.trim_end_matches('/');
        if destination != target {
            return None;
        }
        let (typ, source) = match mount.typ {
            Some(MountPointTypeEnum::VOLUME) => (MountTypeEnum::VOLUME, mount.name.clone()?),
            Some(MountPointTypeEnum::BIND) => (MountTypeEnum::BIND, mount.source.clone()?),
            _ => return None,
        };
        Some(Mount {
            target: Some(working_dir.to_owned()),
            source: Some(source),
            typ: Some(typ),
            read_only: mount.rw.map(|writable| !writable),
            ..Default::default()
        })
    })
}

/// Labels the provider writes for itself, never reported as the caller's.
pub(crate) fn is_internal_label(key: &str) -> bool {
    key == MANAGED_LABEL
        || key == ONE_SHOT_LABEL
        || key == SIDECAR_NETWORK_LABEL
        || key == fence::FENCE_NAME_LABEL
}

/// Whether a container's network mode names a managed sidecar network
/// (`<sandbox>-net`) rather than a standard Docker mode or another
/// container's namespace.
fn is_sidecar_network(mode: &str) -> bool {
    mode.ends_with("-net") && !mode.starts_with("container:")
}

pub(crate) fn normalized_container_name(name: Option<&str>) -> Option<String> {
    name.map(|name| name.trim_start_matches('/'))
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bollard::models::{ContainerConfig, MountPoint};

    use super::*;

    #[test]
    fn container_facts_come_from_the_inspect_alone() {
        let labels = HashMap::from([
            (MANAGED_LABEL.to_owned(), "true".to_owned()),
            (SIDECAR_NETWORK_LABEL.to_owned(), "demo-net".to_owned()),
            ("team".to_owned(), "petri".to_owned()),
        ]);
        let inspect = ContainerInspectResponse {
            id: Some("abc123".to_owned()),
            name: Some("/demo".to_owned()),
            config: Some(ContainerConfig {
                working_dir: Some("/srv/app".to_owned()),
                labels: Some(labels),
                env: Some(vec![
                    "IFS=x".to_owned(),
                    format!("{BASH_ENV_VAR}="),
                    "PATH=/usr/bin".to_owned(),
                    "MALFORMED".to_owned(),
                ]),
                ..ContainerConfig::default()
            }),
            mounts: Some(vec![MountPoint {
                typ: Some(MountPointTypeEnum::VOLUME),
                name: Some("workspace-volume".to_owned()),
                destination: Some("/srv/app".to_owned()),
                rw: Some(true),
                ..MountPoint::default()
            }]),
            ..ContainerInspectResponse::default()
        };
        let facts = ContainerFacts::from_inspect(&inspect, "fallback");
        assert_eq!(facts.id, "abc123");
        assert_eq!(facts.name.as_deref(), Some("demo"));
        assert_eq!(facts.working_dir, "/srv/app");
        assert_eq!(
            facts.labels,
            BTreeMap::from([("team".to_owned(), "petri".to_owned())])
        );
        assert_eq!(
            facts.env,
            BTreeMap::from([
                ("IFS".to_owned(), "x".to_owned()),
                ("PATH".to_owned(), "/usr/bin".to_owned()),
            ])
        );
        assert_eq!(facts.sidecar_network.as_deref(), Some("demo-net"));
        let workspace = facts.workspace.expect("workspace mount");
        assert_eq!(workspace.source.as_deref(), Some("workspace-volume"));
        assert_eq!(workspace.read_only, Some(false));
    }

    #[test]
    fn container_facts_fall_back_when_the_inspect_is_bare() {
        let facts = ContainerFacts::from_inspect(&ContainerInspectResponse::default(), "fallback");
        assert_eq!(facts.id, "fallback");
        assert_eq!(facts.name, None);
        assert_eq!(facts.working_dir, DEFAULT_WORKING_DIRECTORY);
        assert!(facts.labels.is_empty());
        assert!(facts.env.is_empty());
        assert_eq!(facts.sidecar_network, None);
        assert!(facts.workspace.is_none());
    }

    #[test]
    fn workspace_mount_preserves_read_only_access() {
        let inspect = ContainerInspectResponse {
            mounts: Some(vec![MountPoint {
                typ: Some(MountPointTypeEnum::BIND),
                source: Some("/tmp/workspace".to_owned()),
                destination: Some("/workspace".to_owned()),
                rw: Some(false),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let mount = workspace_of(&inspect, "/workspace/").expect("workspace");
        assert_eq!(mount.source.as_deref(), Some("/tmp/workspace"));
        assert_eq!(mount.typ, Some(MountTypeEnum::BIND));
        assert_eq!(mount.read_only, Some(true));
    }

    #[test]
    fn the_network_policy_is_read_back_from_the_network_mode() {
        use bollard::models::HostConfig;
        let inspect = |mode: Option<&str>| ContainerInspectResponse {
            host_config: Some(HostConfig {
                network_mode: mode.map(str::to_owned),
                ..HostConfig::default()
            }),
            ..ContainerInspectResponse::default()
        };
        assert!(matches!(
            network_of(&inspect(Some("none"))),
            Some(NetworkPolicy::Block)
        ));
        assert!(matches!(
            network_of(&inspect(Some("bridge"))),
            Some(NetworkPolicy::AllowAll)
        ));
        assert!(matches!(
            network_of(&inspect(None)),
            Some(NetworkPolicy::AllowAll)
        ));
        assert!(network_of(&inspect(Some("demo-net"))).is_none());
    }
}
