use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sandbox_driver::SandboxId;
use sandbox_driver::contract::*;
use sandbox_driver_testing::contract::FakeProvider;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Default)]
pub(super) struct Secrets {
    pub(super) calls:  AtomicUsize,
    pub(super) values: BTreeMap<String, String>,
}
#[async_trait]
impl SecretResolver for Secrets {
    async fn resolve(&self, name: &str) -> Result<Option<String>, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.values.get(name).cloned())
    }
}

pub(super) struct Factory {
    pub(super) fake:           FakeProvider,
    pub(super) schemas:        ProviderSchemas,
    pub(super) activations:    Mutex<Vec<Activation>>,
    pub(super) namespace:      Mutex<Option<NamespaceId>>,
    pub(super) list_results:   Mutex<Option<Result<Vec<SandboxSummary>, Error>>>,
    pub(super) shutdown_error: Option<Error>,
    pub(super) create_error:   Option<Error>,
    pub(super) creates:        AtomicUsize,
    pub(super) mutations:      AtomicUsize,
    pub(super) discovery:      AtomicUsize,
    pub(super) list_delay:     Duration,
    pub(super) inflight:       AtomicUsize,
    pub(super) peak:           AtomicUsize,
}
impl Default for Factory {
    fn default() -> Self {
        Self {
            fake:           FakeProvider::new(),
            schemas:        FakeProvider::schemas(),
            activations:    Mutex::new(Vec::new()),
            namespace:      Mutex::new(None),
            list_results:   Mutex::new(None),
            shutdown_error: None,
            create_error:   None,
            creates:        AtomicUsize::new(0),
            mutations:      AtomicUsize::new(0),
            discovery:      AtomicUsize::new(0),
            list_delay:     Duration::ZERO,
            inflight:       AtomicUsize::new(0),
            peak:           AtomicUsize::new(0),
        }
    }
}
struct FactoryAdapter(Arc<Factory>);
#[async_trait]
impl ProviderFactory for FactoryAdapter {
    async fn discover(&self, _: &PluginLocation) -> Result<ProviderSchemas, Error> {
        self.0.discovery.fetch_add(1, Ordering::SeqCst);
        Ok(self.0.schemas.clone())
    }
    async fn activate(&self, activation: Activation) -> Result<Arc<dyn ProviderRuntime>, Error> {
        self.0.activations.lock().await.push(activation.clone());
        let runtime = self.0.fake.activate(activation).await?;
        Ok(Arc::new(Runtime {
            factory: Arc::clone(&self.0),
            runtime,
        }))
    }
}
struct Runtime {
    factory: Arc<Factory>,
    runtime: Arc<dyn ProviderRuntime>,
}
#[async_trait]
impl ProviderRuntime for Runtime {
    async fn describe(&self) -> Result<Descriptor, Error> {
        let mut descriptor = self.runtime.describe().await?;
        descriptor.schemas = self.factory.schemas.clone();
        if let Some(namespace) = self.factory.namespace.lock().await.as_ref() {
            descriptor.namespace.id = namespace.clone();
        }
        Ok(descriptor)
    }
    async fn health(&self) -> Result<Health, Error> {
        self.runtime.health().await
    }
    async fn create(&self, admission: WorkloadAdmission) -> Result<Box<dyn Sandbox>, Error> {
        self.factory.creates.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = &self.factory.create_error {
            return Err(error.clone());
        }
        self.runtime.create(admission).await
    }
    async fn attach(&self, id: &SandboxId) -> Result<Box<dyn Sandbox>, Error> {
        self.factory.mutations.fetch_add(1, Ordering::SeqCst);
        self.runtime.attach(id).await
    }
    async fn destroy(&self, id: &SandboxId) -> Result<(), Error> {
        self.factory.mutations.fetch_add(1, Ordering::SeqCst);
        self.runtime.destroy(id).await
    }
    async fn list(&self, filter: &LabelFilter) -> Result<Vec<SandboxSummary>, Error> {
        struct Guard<'a>(&'a AtomicUsize);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let count = self.factory.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let _guard = Guard(&self.factory.inflight);
        self.factory.peak.fetch_max(count, Ordering::SeqCst);
        time::sleep(self.factory.list_delay).await;
        if let Some(result) = self.factory.list_results.lock().await.as_ref() {
            return result.clone();
        }
        self.runtime.list(filter).await
    }
    async fn shutdown(&self) -> Result<(), Error> {
        self.runtime.shutdown().await?;
        self.factory.shutdown_error.clone().map_or(Ok(()), Err)
    }
}

pub(super) fn entry() -> ProviderEntry {
    serde_json::from_value(serde_json::json!({"plugin":"fake"})).expect("entry")
}
pub(super) fn name(value: &str) -> ProviderName {
    ProviderName::try_new(value).expect("name")
}
pub(super) fn env_name(value: &str) -> EnvironmentName {
    EnvironmentName::try_new(value).expect("name")
}
pub(super) fn environments() -> Arc<StaticEnvironments> {
    Arc::new(StaticEnvironments(BTreeMap::from([(
        env_name("test"),
        Environment {
            provider: name("entry"),
            spec:     SandboxSpec::new(Source::Image("workload".into())),
        },
    )])))
}
pub(super) fn registry(factory: &Arc<Factory>) -> Registry {
    Registry::new()
        .builtin(
            entry().plugin,
            Arc::new(FactoryAdapter(Arc::clone(factory))),
        )
        .expect("register")
}
pub(super) fn options() -> CatalogOptions {
    CatalogOptions {
        environment: BTreeMap::new(),
        ..CatalogOptions::default()
    }
}
pub(super) fn catalog(factory: &Arc<Factory>) -> Sandboxes {
    catalog_with(factory, [(name("entry"), entry())], options())
}
pub(super) fn catalog_with(
    factory: &Arc<Factory>,
    providers: impl IntoIterator<Item = (ProviderName, ProviderEntry)>,
    options: CatalogOptions,
) -> Sandboxes {
    Sandboxes::try_new(
        providers,
        environments(),
        &registry(factory),
        Arc::new(Secrets::default()),
        options,
    )
    .expect("catalog")
}
