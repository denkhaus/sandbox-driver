//! Translates the shared contract's portable workload into the container
//! plan the provider already knows how to create.
//!
//! Every field is either honoured or refused by name: the source image, cpu
//! and memory, the network modes Docker can enforce, every `container`
//! option, and services with exec-form readiness checks. Timers, disk, GPUs
//! and other sources are refused rather than dropped.

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard::models::HealthConfig;
use sandbox_driver::contract::{
    ContainerSpec, Error, NetworkPolicy, ReadinessCheck, SandboxSpec, SecretValues, ServiceSpec,
    Source,
};
use sandbox_driver::{self as legacy, SandboxSource};
use sandbox_driver_docker_config::{BindMount, DockerProviderConfig, RegistryAuth, Sidecar};
use serde::Deserialize;

use crate::create::ContainerPlan;
use crate::sidecars::{HEALTH_WAIT, NO_HEALTHCHECK, Service};
use crate::{fence, runtime};

/// Labels under this prefix are the provider's own bookkeeping.
const RESERVED_LABEL_PREFIX: &str = "sh.sandbox-driver.";

/// The few per-sandbox settings with no provider-neutral meaning, validated
/// by the descriptor's `spec_schema` and again here.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DockerSpecOptions {
    auto_pull:    Option<bool>,
    platform:     Option<String>,
    binds:        Vec<BindMount>,
    host_network: bool,
}

/// The JSON Schema of [`DockerSpecOptions`].
pub(crate) fn spec_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "auto_pull": {"type": "boolean"},
            "platform": {"type": "string", "minLength": 1},
            "host_network": {"type": "boolean"},
            "binds": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["host", "container"],
                    "properties": {
                        "host": {"type": "string", "minLength": 1},
                        "container": {"type": "string", "minLength": 1},
                        "mode": {"enum": ["rw", "ro"]},
                    },
                },
            },
        },
    })
}

/// The container plan for an admitted workload. Credentials are read here,
/// at admission, and never stored in the plan's labels. `local` says the
/// daemon shares this host's filesystem, which host-path binds need.
pub(crate) fn plan(
    spec: &SandboxSpec,
    credentials: &SecretValues,
    local: bool,
) -> Result<ContainerPlan, Error> {
    spec.validate()?;
    let Source::Image(image) = &spec.source else {
        return Err(unsupported("source"));
    };
    let resources = &spec.resources;
    if resources.disk_mb.is_some() {
        return Err(unsupported("resources.disk_mb"));
    }
    if resources.gpus.is_some_and(|gpus| gpus != 0) {
        return Err(unsupported("resources.gpus"));
    }
    let timers = &spec.timers;
    for (set, path) in [
        (timers.auto_stop_ms.is_some(), "timers.auto_stop_ms"),
        (timers.auto_archive_ms.is_some(), "timers.auto_archive_ms"),
        (timers.auto_delete_ms.is_some(), "timers.auto_delete_ms"),
        (timers.ttl_ms.is_some(), "timers.ttl_ms"),
    ] {
        if set {
            return Err(unsupported(path));
        }
    }
    if spec.workspace_ownership.is_some() {
        return Err(unsupported("workspace_ownership"));
    }
    let network = match &spec.network {
        NetworkPolicy::ProviderDefault => legacy::NetworkPolicy::ProviderDefault,
        NetworkPolicy::AllowAll => legacy::NetworkPolicy::AllowAll,
        // Services share a user-defined network with the workload, which
        // would give a blocked workload egress through it.
        NetworkPolicy::Block if !spec.services.is_empty() => {
            return Err(Error::InvalidSpec {
                path:    "network".to_owned(),
                message: "docker cannot block the network of a workload with services".to_owned(),
            });
        }
        NetworkPolicy::Block => legacy::NetworkPolicy::Block,
        NetworkPolicy::CidrAllowList { .. } | NetworkPolicy::DomainAllowList { .. } => {
            return Err(unsupported("network"));
        }
    };
    if let Some(key) = spec
        .labels
        .keys()
        .find(|key| key.starts_with(RESERVED_LABEL_PREFIX))
    {
        return Err(Error::InvalidSpec {
            path:    format!("labels.{key}"),
            message: format!("labels under {RESERVED_LABEL_PREFIX} are reserved"),
        });
    }
    let options: DockerSpecOptions = serde_json::from_value(serde_json::Value::Object(
        spec.provider_config.clone().into_iter().collect(),
    ))
    .map_err(|error| Error::InvalidSpec {
        path:    "provider_config".to_owned(),
        message: error.to_string(),
    })?;
    if !local && !options.binds.is_empty() {
        return Err(Error::InvalidSpec {
            path:    "provider_config.binds".to_owned(),
            message: "host paths cannot be mounted from a remote docker daemon".to_owned(),
        });
    }

    let container = spec.container.clone().unwrap_or_default();
    let mut legacy_spec = legacy::SandboxSpec::new(SandboxSource::Image {
        reference: image.clone(),
    })
    .network(network)
    .provider_config(provider_config(options, &container).into_value());
    legacy_spec.resources.cpu_cores = resources.cpu_cores.map(NonZeroU32::get);
    legacy_spec.resources.memory_mb = resources.memory_mb.map(NonZeroU64::get);
    legacy_spec.labels = spec.labels.clone();
    legacy_spec.env = spec.env.clone();
    legacy_spec.user.clone_from(&spec.user);
    legacy_spec
        .working_directory
        .clone_from(&spec.working_directory);
    // The name is a hint; a workload with services needs one, because their
    // network is named after it.
    legacy_spec.name = spec
        .name
        .clone()
        .or_else(|| (!spec.services.is_empty()).then(generated_name));

    let services = spec
        .services
        .iter()
        .map(|(name, service)| translate_service(name, service, credentials))
        .collect::<Result<Vec<_>, _>>()?;
    let mut plan = ContainerPlan::plan(&legacy_spec, services).map_err(runtime::to_contract)?;
    let host = plan
        .config
        .host_config
        .as_mut()
        .expect("a planned container has a host config");
    // An explicit `false` overrides a daemon configured to add an init.
    host.init = container.init;
    if !container.capabilities_drop.is_empty() {
        host.cap_drop = Some(container.capabilities_drop.clone());
    }
    if let Some(mb) = container.shm_size_mb {
        let bytes = mb.get().saturating_mul(1024 * 1024);
        host.shm_size = Some(i64::try_from(bytes).unwrap_or(i64::MAX));
    }
    Ok(plan)
}

/// The Docker options the legacy plan reads: the portable `container`
/// options plus the Docker-only ones from `provider_config`.
fn provider_config(options: DockerSpecOptions, container: &ContainerSpec) -> DockerProviderConfig {
    let defaults = DockerProviderConfig::default();
    DockerProviderConfig {
        auto_pull: options.auto_pull.unwrap_or(defaults.auto_pull),
        privileged: container.privileged.unwrap_or(false),
        // The legacy plan refuses host networking beside services.
        host_network: options.host_network,
        platform: options.platform,
        binds: options.binds,
        extra_hosts: container
            .extra_hosts
            .iter()
            .map(|(host, address)| format!("{host}:{address}"))
            .collect(),
        dns: container.dns.clone(),
        cap_add: container.capabilities_add.clone(),
        ..defaults
    }
}

fn translate_service(
    name: &str,
    service: &ServiceSpec,
    credentials: &SecretValues,
) -> Result<Service, Error> {
    let path = format!("services.{name}");
    let mut sidecar = Sidecar::new(name, service.image.as_str());
    sidecar.env = service
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    if let Some(credential) = &service.credential {
        sidecar.registry_auth = Some(registry_auth(&path, credential, credentials.values())?);
    }
    let (health, deadline) = match &service.ready {
        Some(ready) => readiness(&path, ready)?,
        // Without a check the driver only confirms the service started, so
        // the image's own HEALTHCHECK is turned off: it would otherwise
        // gate a restart that create never waited for.
        None => (
            Some(HealthConfig {
                test: Some(vec![NO_HEALTHCHECK.to_owned()]),
                ..HealthConfig::default()
            }),
            HEALTH_WAIT,
        ),
    };
    Ok(Service {
        sidecar,
        health,
        deadline,
        ports: service.ports.iter().map(|port| port.get()).collect(),
    })
}

/// A registry credential's value is a JSON object with `username`,
/// `password` and an optional `server`, so one secret carries the pair.
fn registry_auth(
    path: &str,
    name: &str,
    values: &BTreeMap<String, String>,
) -> Result<RegistryAuth, Error> {
    let invalid = |message: &str| Error::InvalidSpec {
        path:    format!("{path}.credential"),
        message: message.to_owned(),
    };
    let value = values
        .get(name)
        .ok_or_else(|| invalid("the credential was not resolved"))?;
    // The parse error could quote the value, so it is not passed on.
    serde_json::from_str(value).map_err(|_| {
        invalid("the credential must be a JSON object with username, password and optional server")
    })
}

/// Maps a readiness check onto Docker's own health check, run as argv in
/// the service without a shell. Docker marks the service unhealthy after
/// `retries` consecutive failures; the deadline bounds the whole wait.
fn readiness(
    path: &str,
    ready: &ReadinessCheck,
) -> Result<(Option<HealthConfig>, Duration), Error> {
    let deadline = Duration::from_millis(ready.deadline_ms.get());
    // Docker runs the first probe one interval after the service starts,
    // so a check whose interval reaches the deadline could never pass.
    if ready.interval_ms >= ready.deadline_ms {
        return Err(Error::InvalidSpec {
            path:    format!("{path}.ready.interval_ms"),
            message: "docker runs the first check one interval after start, so the interval \
                      must be shorter than the deadline"
                .to_owned(),
        });
    }
    let limit = HEALTH_WAIT.min(fence::PROVISIONING_WORK);
    if deadline > limit {
        return Err(Error::InvalidSpec {
            path:    format!("{path}.ready.deadline_ms"),
            message: format!(
                "docker waits at most {} ms for service readiness",
                limit.as_millis()
            ),
        });
    }
    let mut test = vec!["CMD".to_owned()];
    test.extend(ready.command.iter().cloned());
    Ok((
        Some(HealthConfig {
            test:           Some(test),
            interval:       Some(nanos(ready.interval_ms.get())),
            timeout:        Some(nanos(ready.timeout_ms.get())),
            retries:        Some(i64::from(ready.retries.get())),
            start_period:   None,
            start_interval: None,
        }),
        deadline,
    ))
}

fn nanos(millis: u64) -> i64 {
    i64::try_from(millis.saturating_mul(1_000_000)).unwrap_or(i64::MAX)
}

fn generated_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "sandbox-{}-{time}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn unsupported(capability: &str) -> Error {
    Error::Unsupported {
        capability: capability.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use serde_json::json;

    use super::*;

    fn image() -> SandboxSpec {
        SandboxSpec::new(Source::Image("alpine:3".to_owned()))
    }

    fn check(command: &[&str]) -> ReadinessCheck {
        ReadinessCheck {
            command:     command.iter().map(|arg| (*arg).to_owned()).collect(),
            interval_ms: NonZeroU64::new(500).unwrap(),
            timeout_ms:  NonZeroU64::new(1_000).unwrap(),
            deadline_ms: NonZeroU64::new(30_000).unwrap(),
            retries:     NonZeroU32::new(10).unwrap(),
        }
    }

    #[test]
    fn container_options_reach_the_host_config() {
        let mut spec = image();
        spec.container = Some(ContainerSpec {
            init:              Some(true),
            privileged:        Some(false),
            capabilities_add:  vec!["SYS_PTRACE".to_owned()],
            capabilities_drop: vec!["NET_RAW".to_owned()],
            dns:               vec!["1.1.1.1".to_owned()],
            extra_hosts:       BTreeMap::from([("db".to_owned(), "10.0.0.2".to_owned())]),
            shm_size_mb:       NonZeroU64::new(64),
        });
        spec.resources.cpu_cores = NonZeroU32::new(2);
        spec.resources.memory_mb = NonZeroU64::new(512);
        let plan = plan(&spec, &SecretValues::default(), true).expect("plan");
        let host = plan.config.host_config.as_ref().expect("host config");
        assert_eq!(host.init, Some(true));
        assert_eq!(host.privileged, None);
        assert_eq!(host.cap_add, Some(vec!["SYS_PTRACE".to_owned()]));
        assert_eq!(host.cap_drop, Some(vec!["NET_RAW".to_owned()]));
        assert_eq!(host.dns, Some(vec!["1.1.1.1".to_owned()]));
        assert_eq!(host.extra_hosts, Some(vec!["db:10.0.0.2".to_owned()]));
        assert_eq!(host.shm_size, Some(64 * 1024 * 1024));
        assert_eq!(host.memory, Some(512 * 1024 * 1024));
        assert_eq!(host.cpu_quota, Some(200_000));
        let mut spec = image();
        spec.container = Some(ContainerSpec {
            init: Some(false),
            ..ContainerSpec::default()
        });
        let without_init = super::plan(&spec, &SecretValues::default(), true).expect("plan");
        let host = without_init
            .config
            .host_config
            .as_ref()
            .expect("host config");
        assert_eq!(host.init, Some(false), "an explicit false is kept");
    }

    #[test]
    fn services_get_an_exec_form_check_and_their_own_deadline() {
        let mut spec = image();
        let mut db = ServiceSpec::new("postgres:16");
        db.ports = vec![NonZeroU16::new(5432).unwrap()];
        db.ready = Some(check(&["pg_isready", "-h", "localhost"]));
        spec.services.insert("db".to_owned(), db);
        let plan = plan(&spec, &SecretValues::default(), true).expect("plan");
        assert!(
            plan.sidecar_network.is_some(),
            "an unnamed workload gets a name"
        );
        let [service] = plan.sidecars.as_slice() else {
            panic!("one service");
        };
        assert_eq!(service.sidecar.name, "db");
        assert_eq!(service.ports, vec![5432]);
        assert_eq!(service.deadline, Duration::from_secs(30));
        let health = service.health.as_ref().expect("readiness check");
        assert_eq!(
            health.test.as_deref(),
            Some(
                ["CMD", "pg_isready", "-h", "localhost"]
                    .map(str::to_owned)
                    .as_slice()
            )
        );
        assert_eq!(health.interval, Some(500_000_000));
        assert_eq!(health.retries, Some(10));
        let mut spec = image();
        spec.services
            .insert("cache".to_owned(), ServiceSpec::new("redis:7"));
        let unchecked = super::plan(&spec, &SecretValues::default(), true).expect("plan");
        let health = unchecked.sidecars[0]
            .health
            .as_ref()
            .expect("disabled check");
        assert_eq!(
            health.test.as_deref(),
            Some([NO_HEALTHCHECK.to_owned()].as_slice()),
            "an unchecked service turns off the image's own check"
        );
    }

    #[test]
    fn a_service_credential_becomes_registry_auth() {
        let mut spec = image();
        let mut db = ServiceSpec::new("registry.example/db:1");
        db.credential = Some("REGISTRY".to_owned());
        spec.services.insert("db".to_owned(), db);
        let secrets = SecretValues::new(BTreeMap::from([(
            "REGISTRY".to_owned(),
            json!({"username": "u", "password": "p"}).to_string(),
        )]));
        let plan = plan(&spec, &secrets, true).expect("plan");
        let auth = plan.sidecars[0]
            .sidecar
            .registry_auth
            .as_ref()
            .expect("auth");
        assert_eq!((auth.username.as_str(), auth.password.as_str()), ("u", "p"));
        let error = super::plan(&spec, &SecretValues::default(), true).expect_err("unresolved");
        assert!(
            matches!(error, Error::InvalidSpec { path, .. } if path == "services.db.credential")
        );
    }

    #[test]
    fn unsupported_requests_are_refused_by_name() {
        let refused = |spec: SandboxSpec| plan(&spec, &SecretValues::default(), true).err();
        let mut spec = image();
        spec.resources.disk_mb = NonZeroU64::new(1024);
        assert_eq!(refused(spec), Some(unsupported("resources.disk_mb")));
        let mut spec = image();
        spec.timers.auto_stop_ms = Some(0);
        assert_eq!(refused(spec), Some(unsupported("timers.auto_stop_ms")));
        let mut spec = image();
        spec.network = NetworkPolicy::DomainAllowList {
            domains: vec!["example.com".to_owned()],
        };
        assert_eq!(refused(spec), Some(unsupported("network")));
        let spec = SandboxSpec::new(Source::Dockerfile("FROM alpine".to_owned()));
        assert_eq!(refused(spec), Some(unsupported("source")));
        let mut spec = image();
        spec.network = NetworkPolicy::Block;
        spec.services
            .insert("db".to_owned(), ServiceSpec::new("postgres:16"));
        assert!(
            matches!(refused(spec), Some(Error::InvalidSpec { path, .. }) if path == "network")
        );
        let mut spec = image();
        spec.labels
            .insert("sh.sandbox-driver.managed".to_owned(), "false".to_owned());
        assert!(matches!(refused(spec), Some(Error::InvalidSpec { .. })));
        let mut spec = image();
        spec.provider_config
            .insert("sidecars".to_owned(), json!([]));
        assert!(
            matches!(refused(spec), Some(Error::InvalidSpec { path, .. }) if path == "provider_config")
        );
        let mut spec = image();
        spec.provider_config.insert(
            "binds".to_owned(),
            json!([{"host": "/tmp", "container": "/data"}]),
        );
        assert!(plan(&spec, &SecretValues::default(), true).is_ok());
        assert!(matches!(
            plan(&spec, &SecretValues::default(), false).err(),
            Some(Error::InvalidSpec { path, .. }) if path == "provider_config.binds"
        ));
        let mut spec = image();
        let mut late = ServiceSpec::new("postgres:16");
        let mut ready = check(&["true"]);
        ready.interval_ms = ready.deadline_ms;
        late.ready = Some(ready);
        spec.services.insert("db".to_owned(), late);
        assert!(matches!(
            refused(spec),
            Some(Error::InvalidSpec { path, .. }) if path == "services.db.ready.interval_ms"
        ));
        let mut spec = image();
        let mut slow = ServiceSpec::new("postgres:16");
        let mut ready = check(&["true"]);
        ready.deadline_ms = NonZeroU64::new(600_000).unwrap();
        slow.ready = Some(ready);
        spec.services.insert("db".to_owned(), slow);
        assert!(matches!(refused(spec), Some(Error::InvalidSpec { .. })));
    }
}
