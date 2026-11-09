use std::collections::{BTreeMap, BTreeSet, HashSet, btree_map};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{env, fmt};

use async_trait::async_trait;
use futures_util::{StreamExt, future, stream};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};
use tokio::{fs, time};

use super::schema::Schemas;
use super::workload::{valid_env_key, validate_env, validate_name};
use super::{
    Activation, Descriptor, Environment, EnvironmentName, EnvironmentSource, Error, Health,
    LabelFilter, MutationOutcome, NetworkPolicy, PluginLocation, ProviderEntry, ProviderFactory,
    ProviderName, ProviderRuntime, Sandbox, SandboxRef, SandboxSpec, SandboxSummary,
    SecretResolver, SecretValues, Source, Timers, WorkloadAdmission, parse_size_mib,
};
use crate::ProviderKind;

/// Adapter registrations. No provider is implicitly installed or bundled.
#[derive(Clone, Default)]
pub struct Registry {
    builtins: BTreeMap<ProviderKind, Arc<dyn ProviderFactory>>,
    plugins:  Option<Arc<dyn ProviderFactory>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn plugins(mut self, factory: Arc<dyn ProviderFactory>) -> Self {
        self.plugins = Some(factory);
        self
    }
    /// Host always uses the ordinary plugin loader, never a built-in fallback.
    pub fn builtin(
        mut self,
        kind: ProviderKind,
        factory: Arc<dyn ProviderFactory>,
    ) -> Result<Self, Error> {
        if kind.as_str() == "host" || self.builtins.contains_key(&kind) {
            return Err(Error::Unsupported {
                capability: "duplicate registration or built-in Host".into(),
            });
        }
        self.builtins.insert(kind, factory);
        Ok(self)
    }
    fn factory(&self, entry: &ProviderEntry) -> Option<Arc<dyn ProviderFactory>> {
        if entry.path.is_some() || entry.trust.is_some() {
            self.plugins.clone()
        } else {
            self.builtins
                .get(&entry.plugin)
                .or(self.plugins.as_ref())
                .cloned()
        }
    }
}

/// Bounded inventory and explicit process-environment snapshot. Callers may
/// pass an empty map to avoid all ambient inheritance. Built-ins never read it.
#[derive(Clone)]
pub struct CatalogOptions {
    pub environment:      BTreeMap<String, String>,
    pub find_concurrency: NonZeroUsize,
    pub find_timeout:     Duration,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            environment:      env::vars_os()
                .filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                })
                .collect(),
            find_concurrency: NonZeroUsize::new(8).expect("positive constant"),
            find_timeout:     Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for CatalogOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogOptions")
            .field("environment", &"<redacted>")
            .field("find_concurrency", &self.find_concurrency)
            .field("find_timeout", &self.find_timeout)
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct StaticEnvironments(pub BTreeMap<EnvironmentName, Environment>);
#[async_trait]
impl EnvironmentSource for StaticEnvironments {
    async fn names(&self) -> Result<Vec<EnvironmentName>, Error> {
        Ok(self.0.keys().cloned().collect())
    }
    async fn get(&self, name: &str) -> Result<Option<Environment>, Error> {
        Ok(EnvironmentName::try_new(name)
            .ok()
            .and_then(|name| self.0.get(&name).cloned()))
    }
}

struct Active {
    runtime:    Arc<dyn ProviderRuntime>,
    schemas:    Schemas,
    descriptor: Descriptor,
}

struct Entry {
    name:        ProviderName,
    config:      ProviderEntry,
    environment: BTreeMap<String, String>,
    factory:     Option<Arc<dyn ProviderFactory>>,
    local_error: Option<Error>,
    // Held through initialization to serialize activation and shutdown, never
    // through sandbox operations. A cancelled factory must clean up its launch.
    active:      Mutex<Option<Arc<Active>>>,
}

struct Catalog {
    entries:          BTreeMap<ProviderName, Arc<Entry>>,
    environments:     Arc<dyn EnvironmentSource>,
    secrets:          Arc<dyn SecretResolver>,
    find_concurrency: NonZeroUsize,
    find_timeout:     Duration,
    closed:           AtomicBool,
    shutdown_result:  OnceCell<Result<(), Error>>,
}

/// Immutable provider bindings with fresh environment snapshots per builder.
/// Construction performs local checks only; discovery, credentials and runtime
/// startup are lazy per entry. Async work runs on the caller's Tokio runtime.
#[derive(Clone)]
pub struct Sandboxes {
    catalog: Arc<Catalog>,
}

impl Sandboxes {
    pub fn try_new(
        providers: impl IntoIterator<Item = (ProviderName, ProviderEntry)>,
        environments: Arc<dyn EnvironmentSource>,
        registry: &Registry,
        secrets: Arc<dyn SecretResolver>,
        options: CatalogOptions,
    ) -> Result<Self, Error> {
        // The ambient snapshot is only needed to build launch environments.
        let CatalogOptions {
            environment: ambient,
            find_concurrency,
            find_timeout,
        } = options;
        let mut entries = BTreeMap::new();
        for (name, config) in providers {
            if entries.contains_key(&name) {
                return Err(Error::invalid_config(
                    &name,
                    "provider",
                    "duplicate provider entry",
                ));
            }
            let environment = launch_environment(&config, &ambient);
            let local_error = validate_entry(&name, &config)
                .and_then(|()| {
                    validate_env("env", &environment).map_err(|_| {
                        Error::invalid_config(&name, "inherit_env", "invalid inherited environment")
                    })
                })
                .err();
            let factory = registry.factory(&config);
            entries.insert(
                name.clone(),
                Arc::new(Entry {
                    name,
                    config,
                    environment,
                    factory,
                    local_error,
                    active: Mutex::new(None),
                }),
            );
        }
        Ok(Self {
            catalog: Arc::new(Catalog {
                entries,
                environments,
                secrets,
                find_concurrency,
                find_timeout,
                closed: AtomicBool::new(false),
                shutdown_result: OnceCell::new(),
            }),
        })
    }

    /// Reads an explicit file once. Applications select default paths and
    /// pass their credential store and installed adapters explicitly.
    pub async fn from_config_file(
        path: &Path,
        registry: &Registry,
        secrets: Arc<dyn SecretResolver>,
        options: CatalogOptions,
    ) -> Result<Self, Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            providers:    BTreeMap<ProviderName, ProviderEntry>,
            environments: BTreeMap<EnvironmentName, Environment>,
        }
        let content = fs::read_to_string(path).await.map_err(|_| {
            unavailable(
                "catalog file could not be read",
                MutationOutcome::NotStarted,
            )
        })?;
        let file: File = toml::from_str(&content).map_err(|_| Error::InvalidSpec {
            path:    "catalog".into(),
            message: "invalid catalog TOML".into(),
        })?;
        Self::try_new(
            file.providers,
            Arc::new(StaticEnvironments(file.environments)),
            registry,
            secrets,
            options,
        )
    }

    pub fn providers(&self) -> Vec<ProviderName> {
        self.catalog.entries.keys().cloned().collect()
    }
    pub fn provider(&self, name: &str) -> Result<ProviderHandle, Error> {
        let name = ProviderName::try_new(name)
            .map_err(|_| Error::invalid_spec("provider", "invalid provider entry name"))?;
        self.handle(&name)
    }
    fn handle(&self, name: &ProviderName) -> Result<ProviderHandle, Error> {
        let entry = self
            .catalog
            .entries
            .get(name)
            .ok_or_else(|| Error::NoProvider {
                entry: name.clone(),
            })?;
        Ok(self.bind(entry))
    }
    fn bind(&self, entry: &Arc<Entry>) -> ProviderHandle {
        ProviderHandle {
            catalog: Arc::clone(&self.catalog),
            entry:   Arc::clone(entry),
        }
    }
    /// Snapshot local configuration diagnostics without probing any provider.
    /// Mutable environment references are checked against the current source.
    pub async fn diagnostics(&self) -> Result<Vec<CatalogDiagnostic>, Error> {
        let mut diagnostics = Vec::new();
        for entry in self.catalog.entries.values() {
            if let Some(error) = &entry.local_error {
                diagnostics.push(CatalogDiagnostic {
                    environment: None,
                    entry:       entry.name.clone(),
                    error:       error.clone(),
                });
            }
        }
        for name in self.catalog.environments.names().await? {
            if let Some(environment) = self.catalog.environments.get(name.as_str()).await? {
                if !self.catalog.entries.contains_key(&environment.provider) {
                    diagnostics.push(CatalogDiagnostic {
                        environment: Some(name),
                        entry:       environment.provider.clone(),
                        error:       Error::NoProvider {
                            entry: environment.provider,
                        },
                    });
                }
            }
        }
        Ok(diagnostics)
    }

    pub async fn environments(&self) -> Result<Vec<EnvironmentName>, Error> {
        self.catalog.environments.names().await
    }
    pub async fn environment(&self, name: &str) -> Result<Environment, Error> {
        let validated = EnvironmentName::try_new(name)
            .map_err(|_| Error::invalid_spec("environment", "invalid environment name"))?;
        self.catalog
            .environments
            .get(name)
            .await?
            .ok_or(Error::NoEnvironment { name: validated })
    }
    /// Resolves one environment snapshot now; later edits cannot alter this
    /// builder's spec or its provider binding. No provider is started here.
    pub async fn builder(&self, environment: &str) -> Result<SandboxBuilder, Error> {
        let environment = self.environment(environment).await?;
        let provider = self.handle(&environment.provider)?;
        Ok(SandboxBuilder {
            provider,
            spec: environment.spec,
            error: None,
        })
    }
    pub async fn attach(&self, reference: &SandboxRef) -> Result<Box<dyn Sandbox>, Error> {
        let sandbox = self
            .route(reference)
            .await?
            .runtime
            .attach(&reference.id)
            .await?;
        if sandbox.reference() != *reference {
            return Err(transport("attach returned a different sandbox reference"));
        }
        Ok(sandbox)
    }
    pub async fn destroy(&self, reference: &SandboxRef) -> Result<(), Error> {
        self.route(reference)
            .await?
            .runtime
            .destroy(&reference.id)
            .await
    }
    /// Uses the reference's saved entry and refuses a changed namespace
    /// before anything is dispatched.
    async fn route(&self, reference: &SandboxRef) -> Result<Arc<Active>, Error> {
        let (active, descriptor) = self.handle(&reference.provider)?.ready().await?;
        reference.check_namespace(&descriptor.namespace.id)?;
        Ok(active)
    }
    /// Every entry is attempted, at most the configured number concurrently.
    /// Per-entry timeout/errors are explicit failures; successful entries
    /// remain usable. Equal repeated refs collapse; conflicting duplicates
    /// invalidate that entry's entire batch. Aliased entries remain
    /// separate identities.
    pub async fn find(&self, filter: LabelFilter) -> Result<Found, Error> {
        self.catalog.ensure_open()?;
        let mut results = stream::iter(self.catalog.entries.values())
            .map(|entry| {
                let handle = self.bind(entry);
                let filter = &filter;
                async move {
                    let result = time::timeout(self.catalog.find_timeout, handle.list(filter))
                        .await
                        .unwrap_or_else(|_| {
                            Err(unavailable(
                                "inventory deadline elapsed",
                                MutationOutcome::NotStarted,
                            ))
                        });
                    (handle.entry.name.clone(), result)
                }
            })
            .buffer_unordered(self.catalog.find_concurrency.get())
            .collect::<Vec<_>>()
            .await;
        // Each entry's matches share its provider and namespace and arrive
        // sorted by id, so ordering by entry orders every field by identity.
        results.sort_by(|left, right| left.0.cmp(&right.0));
        let mut found = Found::default();
        for (entry, result) in results {
            match result {
                Ok(mut matches) => found.sandboxes.append(&mut matches),
                Err(error) => {
                    found.unavailable.push(entry.clone());
                    found.failures.push(ProviderFailure { entry, error });
                }
            }
        }
        Ok(found)
    }
    /// Closes this catalog and all its clones. All activated entries are tried
    /// even when one fails. No unused entry is started to shut it down.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.catalog.closed.store(true, Ordering::Release);
        self.catalog
            .shutdown_result
            .get_or_init(|| async {
                let results =
                    future::join_all(self.catalog.entries.values().map(|entry| async move {
                        match entry.active.lock().await.as_ref() {
                            Some(active) => active.runtime.shutdown().await,
                            None => Ok(()),
                        }
                    }))
                    .await;
                // Every entry was tried; report the first failure by name.
                results.into_iter().collect()
            })
            .await
            .clone()
    }
}

impl Catalog {
    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::Stale {
                outcome: MutationOutcome::NotStarted,
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct ProviderHandle {
    catalog: Arc<Catalog>,
    entry:   Arc<Entry>,
}
impl ProviderHandle {
    async fn active(&self) -> Result<Arc<Active>, Error> {
        self.catalog.ensure_open()?;
        let mut active = self.entry.active.lock().await;
        self.catalog.ensure_open()?;
        if let Some(active) = active.as_ref() {
            return Ok(Arc::clone(active));
        }
        if let Some(error) = &self.entry.local_error {
            return Err(error.clone());
        }
        let entry = &self.entry.config;
        let factory = self.entry.factory.as_ref().ok_or_else(|| {
            unavailable(
                "provider adapter is not installed",
                MutationOutcome::NotStarted,
            )
        })?;
        let metadata = factory
            .discover(&PluginLocation {
                kind:  entry.plugin.clone(),
                path:  entry.path.clone(),
                trust: entry.trust.clone(),
            })
            .await?;
        if metadata.kind != entry.plugin {
            return Err(Error::invalid_config(
                &self.entry.name,
                "plugin",
                "metadata kind differs from configured plugin",
            ));
        }
        let schemas = Schemas::compile(&self.entry.name, &metadata)?;
        schemas.config(&self.entry.name, &entry.config)?;
        for name in &metadata.secrets {
            if !entry.secrets.contains(name) {
                return Err(Error::MissingSecret {
                    entry: self.entry.name.clone(),
                    name:  name.clone(),
                });
            }
        }
        let secrets = self.resolve_secrets(entry.secrets.iter().cloned()).await?;
        let runtime = factory
            .activate(Activation {
                name: self.entry.name.clone(),
                entry: entry.clone(),
                schemas: metadata.clone(),
                environment: self.entry.environment.clone(),
                secrets,
            })
            .await?;
        let checked = runtime.describe().await.and_then(|descriptor| {
            if descriptor.schemas == metadata {
                Ok(descriptor)
            } else {
                Err(Error::invalid_config(
                    &self.entry.name,
                    "schemas",
                    "runtime metadata disagrees with discovery",
                ))
            }
        });
        let descriptor = match checked {
            Ok(descriptor) => descriptor,
            Err(error) => {
                // Activation admitted no sandbox mutations. Cleanup is awaited;
                // preserve an honest cleanup failure over the metadata error.
                runtime.shutdown().await?;
                return Err(error);
            }
        };
        let initialized = Arc::new(Active {
            runtime,
            schemas,
            descriptor,
        });
        *active = Some(Arc::clone(&initialized));
        Ok(initialized)
    }
    async fn resolve_secrets(
        &self,
        names: impl IntoIterator<Item = String> + Send,
    ) -> Result<SecretValues, Error> {
        let mut values = BTreeMap::new();
        for name in names {
            if values.contains_key(&name) {
                continue;
            }
            let value =
                self.catalog
                    .secrets
                    .resolve(&name)
                    .await?
                    .ok_or_else(|| Error::MissingSecret {
                        entry: self.entry.name.clone(),
                        name:  name.clone(),
                    })?;
            if value.contains('\0') {
                return Err(Error::invalid_config(
                    &self.entry.name,
                    "secrets",
                    "secret value contains NUL",
                ));
            }
            values.insert(name, value);
        }
        Ok(SecretValues::new(values))
    }
    /// Activates the entry and confirms its metadata and namespace still match
    /// what activation saw. Every operation passes through here first.
    async fn ready(&self) -> Result<(Arc<Active>, Descriptor), Error> {
        let active = self.active().await?;
        self.catalog.ensure_open()?;
        let descriptor = active.runtime.describe().await?;
        if descriptor.schemas != active.descriptor.schemas {
            return Err(transport("runtime metadata changed after activation"));
        }
        if descriptor.namespace != active.descriptor.namespace {
            return Err(Error::NamespaceMismatch {
                expected: active.descriptor.namespace.id.clone(),
                actual:   descriptor.namespace.id,
            });
        }
        Ok((active, descriptor))
    }
    /// Portable checks run before activation, so an invalid spec starts
    /// nothing; provider checks follow. Credentials are not resolved here.
    async fn admit(&self, spec: &SandboxSpec) -> Result<(Arc<Active>, Descriptor), Error> {
        spec.validate()?;
        let (active, descriptor) = self.ready().await?;
        spec.validate_for(&descriptor)?;
        active.schemas.spec(&spec.provider_config)?;
        Ok((active, descriptor))
    }
    pub async fn describe(&self) -> Result<Descriptor, Error> {
        Ok(self.ready().await?.1)
    }
    /// Validates a proposed template/override without admitting a workload or
    /// resolving service credentials. Explicit inspection may activate the
    /// entry.
    pub async fn validate_spec(&self, spec: &SandboxSpec) -> Result<(), Error> {
        self.admit(spec).await?;
        Ok(())
    }

    pub async fn health(&self) -> Result<Health, Error> {
        self.active().await?.runtime.health().await
    }
    async fn list(&self, filter: &LabelFilter) -> Result<Vec<SandboxSummary>, Error> {
        let (active, descriptor) = self.ready().await?;
        let matches = active.runtime.list(filter).await?;
        let mut unique = BTreeMap::new();
        for summary in matches {
            if summary.reference.provider != self.entry.name
                || summary.reference.namespace != descriptor.namespace.id
                || !filter.matches(&summary.labels)
            {
                return Err(transport(
                    "inventory returned a mismatched reference or labels",
                ));
            }
            match unique.entry(summary.reference.id.clone()) {
                btree_map::Entry::Vacant(slot) => {
                    slot.insert(summary);
                }
                btree_map::Entry::Occupied(slot) if *slot.get() != summary => {
                    return Err(transport(
                        "inventory returned conflicting duplicate references",
                    ));
                }
                btree_map::Entry::Occupied(_) => {}
            }
        }
        Ok(unique.into_values().collect())
    }
}

#[derive(Clone, Debug)]
pub struct CatalogDiagnostic {
    pub environment: Option<EnvironmentName>,
    pub entry:       ProviderName,
    pub error:       Error,
}

#[derive(Clone, Debug)]
pub struct ProviderFailure {
    pub entry: ProviderName,
    pub error: Error,
}
#[derive(Clone, Debug, Default)]
pub struct Found {
    pub sandboxes:   Vec<SandboxSummary>,
    /// Every entry that failed, not a claim that its resources are absent.
    pub unavailable: Vec<ProviderName>,
    /// Structured reasons, including permission/configuration errors.
    pub failures:    Vec<ProviderFailure>,
}

pub struct SandboxBuilder {
    provider: ProviderHandle,
    spec:     SandboxSpec,
    error:    Option<Error>,
}
impl SandboxBuilder {
    #[must_use]
    pub fn spec(mut self, spec: SandboxSpec) -> Self {
        self.spec = spec;
        self
    }
    #[must_use]
    pub fn image(mut self, image: &str) -> Self {
        self.spec.source = Source::Image(image.into());
        self
    }
    #[must_use]
    pub fn dockerfile(mut self, dockerfile: &str) -> Self {
        self.spec.source = Source::Dockerfile(dockerfile.into());
        self
    }
    #[must_use]
    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.spec.labels.insert(key.into(), value.into());
        self
    }
    #[must_use]
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.spec.env.insert(key.into(), value.into());
        self
    }
    #[must_use]
    pub fn working_directory(mut self, path: &str) -> Self {
        self.spec.working_directory = Some(path.into());
        self
    }
    #[must_use]
    pub fn user(mut self, user: &str) -> Self {
        self.spec.user = Some(user.into());
        self
    }
    #[must_use]
    pub fn network(mut self, network: NetworkPolicy) -> Self {
        self.spec.network = network;
        self
    }
    #[must_use]
    pub fn timers(mut self, timers: Timers) -> Self {
        self.spec.timers = timers;
        self
    }
    #[must_use]
    pub fn cpu(mut self, cores: NonZeroU32) -> Self {
        self.spec.resources.cpu_cores = Some(cores);
        self
    }
    #[must_use]
    pub fn memory(mut self, value: &str) -> Self {
        if let Some(size) = self.size("resources.memory", value) {
            self.spec.resources.memory_mb = Some(size);
        }
        self
    }
    #[must_use]
    pub fn disk(mut self, value: &str) -> Self {
        if let Some(size) = self.size("resources.disk", value) {
            self.spec.resources.disk_mb = Some(size);
        }
        self
    }
    /// The first invalid size is kept as a deferred admission error.
    fn size(&mut self, path: &str, value: &str) -> Option<NonZeroU64> {
        let size = parse_size_mib(value).ok().and_then(NonZeroU64::new);
        if size.is_none() {
            self.error.get_or_insert_with(|| {
                Error::invalid_spec(path, "requires a positive size with units")
            });
        }
        size
    }
    /// Merges opaque keys; the selected provider's spec_schema validates them.
    #[must_use]
    pub fn provider_config(mut self, values: BTreeMap<String, Value>) -> Self {
        self.spec.provider_config.extend(values);
        self
    }
    pub fn resolved_spec(&self) -> &SandboxSpec {
        &self.spec
    }
    pub async fn create(self) -> Result<Box<dyn Sandbox>, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let (active, descriptor) = self.provider.admit(&self.spec).await?;
        let credentials = self
            .provider
            .resolve_secrets(
                self.spec
                    .services
                    .values()
                    .filter_map(|service| service.credential.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        let sandbox = active
            .runtime
            .create(WorkloadAdmission {
                spec: self.spec,
                credentials,
            })
            .await?;
        let reference = sandbox.reference();
        if reference.provider != self.provider.entry.name
            || reference.namespace != descriptor.namespace.id
        {
            // Do not guess an ID to destroy after a bad provider response.
            return Err(transport(
                "create returned a mismatched provider or namespace; outcome is unknown",
            ));
        }
        Ok(sandbox)
    }
}

fn transport(message: &str) -> Error {
    Error::Transport {
        message: message.into(),
        outcome: MutationOutcome::Unknown,
    }
}
fn unavailable(message: &str, outcome: MutationOutcome) -> Error {
    Error::Unavailable {
        message: message.into(),
        outcome,
    }
}
fn validate_entry(name: &ProviderName, entry: &ProviderEntry) -> Result<(), Error> {
    validate_env("env", &entry.env)
        .map_err(|_| Error::invalid_config(name, "env", "invalid launch environment"))?;
    let secrets: HashSet<_> = entry.secrets.iter().collect();
    if secrets.len() != entry.secrets.len() || secrets.iter().any(|key| !valid_env_key(key)) {
        return Err(Error::invalid_config(
            name,
            "secrets",
            "secret names must be unique valid environment keys",
        ));
    }
    if entry.env.keys().any(|key| secrets.contains(key)) {
        return Err(Error::invalid_config(
            name,
            "env",
            "explicit environment conflicts with a secret name",
        ));
    }
    if entry
        .inherit_env
        .as_ref()
        .is_some_and(|names| names.iter().any(|name| !valid_env_key(name)))
    {
        return Err(Error::invalid_config(
            name,
            "inherit_env",
            "invalid environment key",
        ));
    }
    if entry.args.iter().any(|arg| arg.contains('\0'))
        || entry
            .path
            .as_deref()
            .is_some_and(|path| validate_name("path", path).is_err())
    {
        return Err(Error::invalid_config(
            name,
            "launch",
            "invalid executable path or argument",
        ));
    }
    Ok(())
}
fn launch_environment(
    entry: &ProviderEntry,
    ambient: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let defaults = ["PATH", "HOME", "TMPDIR", "USER"].map(str::to_owned);
    let names = entry.inherit_env.as_deref().unwrap_or(&defaults);
    let secret_names: BTreeSet<_> = entry.secrets.iter().collect();
    let mut result: BTreeMap<_, _> = names
        .iter()
        .filter(|name| !secret_names.contains(name))
        .filter_map(|name| ambient.get(name).map(|value| (name.clone(), value.clone())))
        .collect();
    result.extend(entry.env.clone());
    result
}
