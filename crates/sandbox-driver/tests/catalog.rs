#[path = "support/catalog.rs"]
mod support;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use sandbox_driver::contract::*;
use serde_json::json;
use support::*;
use tokio::sync::RwLock;

#[tokio::test]
async fn cold_catalog_is_local_and_failures_are_selected_entry_only() {
    let factory = Arc::new(Factory::default());
    let secrets = Arc::new(Secrets::default());
    let mut missing = entry();
    missing.secrets.push("MISSING".into());
    let catalog = Sandboxes::try_new(
        [(name("entry"), entry()), (name("missing"), missing)],
        environments(),
        &registry(&factory),
        secrets.clone(),
        options(),
    )
    .expect("cold catalog");
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 0);
    assert_eq!(secrets.calls.load(Ordering::SeqCst), 0);
    catalog
        .builder("test")
        .await
        .expect("builder")
        .create()
        .await
        .expect("unrelated entry works");
    assert!(matches!(
        catalog.provider("missing").expect("entry").describe().await,
        Err(Error::MissingSecret { .. })
    ));
    assert_eq!(factory.activations.lock().await.len(), 1);
    catalog.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_calls_activate_only_once() {
    let factory = Arc::new(Factory::default());
    let catalog = catalog(&factory);
    let provider = catalog.provider("entry").expect("entry");
    let (first, second) = tokio::join!(provider.describe(), provider.health());
    first.expect("describe");
    second.expect("health");
    assert_eq!(factory.activations.lock().await.len(), 1);
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn namespace_change_refuses_attach_and_destroy_before_dispatch() {
    let factory = Arc::new(Factory::default());
    let catalog = catalog(&factory);
    let sandbox = catalog
        .builder("test")
        .await
        .expect("builder")
        .create()
        .await
        .expect("create");
    *factory.namespace.lock().await = Some(NamespaceId::try_new("replacement").expect("namespace"));
    assert!(matches!(
        catalog.attach(&sandbox.reference()).await,
        Err(Error::NamespaceMismatch { .. })
    ));
    assert!(matches!(
        catalog.destroy(&sandbox.reference()).await,
        Err(Error::NamespaceMismatch { .. })
    ));
    assert_eq!(factory.mutations.load(Ordering::SeqCst), 0);
    let mut renamed = sandbox.reference();
    renamed.provider = name("renamed");
    assert!(matches!(
        catalog.attach(&renamed).await,
        Err(Error::NoProvider { .. })
    ));
}

struct Editable(RwLock<Option<Environment>>);
#[async_trait]
impl EnvironmentSource for Editable {
    async fn names(&self) -> Result<Vec<EnvironmentName>, Error> {
        Ok(vec![env_name("test")])
    }
    async fn get(&self, _: &str) -> Result<Option<Environment>, Error> {
        Ok(self.0.read().await.clone())
    }
}
#[tokio::test]
async fn builders_freeze_templates_and_refs_survive_deletion() {
    let source = Arc::new(Editable(RwLock::new(Some(Environment {
        provider: name("entry"),
        spec:     SandboxSpec::new(Source::Image("original".into())),
    }))));
    let factory = Arc::new(Factory::default());
    let catalog = Sandboxes::try_new(
        [(name("entry"), entry())],
        source.clone(),
        &registry(&factory),
        Arc::new(Secrets::default()),
        options(),
    )
    .expect("catalog");
    let builder = catalog
        .builder("test")
        .await
        .expect("snapshot")
        .label("a", "b")
        .env("MODE", "override");
    *source.0.write().await = None;
    let sandbox = builder.create().await.expect("frozen admission");
    assert_eq!(
        sandbox.status().await.expect("status").image.as_deref(),
        Some("original")
    );
    assert_eq!(sandbox.env()["MODE"], "override");
    catalog
        .attach(&sandbox.reference())
        .await
        .expect("ref independent of environment");
    assert!(matches!(
        catalog.builder("test").await,
        Err(Error::NoEnvironment { .. })
    ));
}

#[tokio::test]
async fn config_and_spec_schemas_are_separate_and_rejections_precede_create() {
    let mut f = Factory::default();
    f.schemas.config_schema=serde_json::from_value(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"endpoint":{"type":"string"}},"required":["endpoint"],"additionalProperties":false})).expect("schema");
    f.schemas.spec_schema=Some(serde_json::from_value(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"size":{"type":"integer"}},"additionalProperties":false})).expect("schema"));
    let factory = Arc::new(f);
    let mut config = entry();
    config
        .config
        .insert("endpoint".into(), json!("secret-endpoint"));
    let catalog = catalog_with(&factory, [(name("entry"), config)], options());
    catalog
        .provider("entry")
        .expect("entry")
        .describe()
        .await
        .expect("config schema only");
    let error = catalog
        .builder("test")
        .await
        .expect("builder")
        .provider_config(BTreeMap::from([("endpoint".into(), json!("secret-spec"))]))
        .create()
        .await
        .err()
        .expect("spec schema rejects connection key");
    assert!(matches!(error, Error::InvalidSpec { .. }));
    assert!(!error.to_string().contains("secret-spec"));
    assert_eq!(factory.creates.load(Ordering::SeqCst), 0);
    catalog
        .builder("test")
        .await
        .expect("builder")
        .create()
        .await
        .expect("empty valid spec config");
}

#[tokio::test]
async fn bad_schemas_never_activate_or_resolve_credentials() {
    for schema in [
        json!({"type":"object"}),
        json!({"$schema":"future"}),
        json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"https://example.invalid/private"}),
        json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"file:///etc/passwd"}),
    ] {
        let mut f = Factory::default();
        f.schemas.config_schema = serde_json::from_value(schema).expect("schema");
        let factory = Arc::new(f);
        let catalog = catalog(&factory);
        assert!(matches!(
            catalog.provider("entry").expect("entry").describe().await,
            Err(Error::InvalidConfig { .. })
        ));
        assert!(factory.activations.lock().await.is_empty());
    }
}

#[tokio::test]
async fn inheritance_replaces_defaults_explicit_env_wins_and_secrets_are_separate() {
    let factory = Arc::new(Factory::default());
    let secrets = Arc::new(Secrets {
        values: BTreeMap::from([("TOKEN".into(), "resolved-secret".into())]),
        ..Secrets::default()
    });
    let mut config = entry();
    config.secrets = vec!["TOKEN".into()];
    config.inherit_env = Some(vec!["TOKEN".into(), "CUSTOM".into()]);
    config.env.insert("CUSTOM".into(), "explicit".into());
    let mut opts = options();
    opts.environment = BTreeMap::from([
        ("TOKEN".into(), "ambient-secret".into()),
        ("PATH".into(), "ambient-path".into()),
        ("CUSTOM".into(), "ambient".into()),
    ]);
    let catalog = Sandboxes::try_new(
        [(name("entry"), config)],
        environments(),
        &registry(&factory),
        secrets,
        opts,
    )
    .expect("catalog");
    catalog
        .provider("entry")
        .expect("entry")
        .describe()
        .await
        .expect("activation");
    let activations = factory.activations.lock().await;
    let activation = &activations[0];
    assert_eq!(
        activation.environment,
        BTreeMap::from([("CUSTOM".into(), "explicit".into())])
    );
    assert_eq!(activation.secrets.values()["TOKEN"], "resolved-secret");
    assert!(!format!("{activation:?}").contains("resolved-secret"));
}

#[tokio::test]
async fn secret_env_conflict_is_local_and_empty_inheritance_is_empty() {
    let factory = Arc::new(Factory::default());
    let mut bad = entry();
    bad.secrets.push("TOKEN".into());
    bad.env.insert("TOKEN".into(), "bad".into());
    let mut clean = entry();
    clean.inherit_env = Some(vec![]);
    let mut opts = options();
    opts.environment.insert("PATH".into(), "ambient".into());
    let catalog = catalog_with(&factory, [(name("entry"), clean), (name("bad"), bad)], opts);
    assert!(matches!(
        catalog.provider("bad").expect("entry").describe().await,
        Err(Error::InvalidConfig { .. })
    ));
    catalog
        .provider("entry")
        .expect("entry")
        .describe()
        .await
        .expect("unrelated entry");
    assert!(factory.activations.lock().await[0].environment.is_empty());
}

#[tokio::test(start_paused = true)]
async fn inventory_is_bounded_and_timeouts_leave_other_results_usable() {
    let factory = Arc::new(Factory {
        list_delay: Duration::from_secs(1),
        ..Factory::default()
    });
    let mut opts = options();
    opts.find_concurrency = NonZeroUsize::new(2).expect("nonzero");
    opts.find_timeout = Duration::from_secs(10);
    let catalog = catalog_with(
        &factory,
        (0..5).map(|n| (name(&format!("entry{n}")), entry())),
        opts,
    );
    assert!(
        catalog
            .find(LabelFilter::default())
            .await
            .expect("find")
            .failures
            .is_empty()
    );
    assert_eq!(factory.peak.load(Ordering::SeqCst), 2);
    assert_eq!(factory.inflight.load(Ordering::SeqCst), 0);
    let slow = Arc::new(Factory {
        list_delay: Duration::from_secs(60),
        ..Factory::default()
    });
    let catalog = catalog_with(&slow, [(name("entry"), entry())], CatalogOptions {
        find_timeout: Duration::from_secs(1),
        ..options()
    });
    let found = catalog
        .find(LabelFilter::default())
        .await
        .expect("partial response");
    assert_eq!(found.unavailable, vec![name("entry")]);
    assert!(matches!(found.failures[0].error, Error::Unavailable { .. }));
    assert_eq!(slow.inflight.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_and_foreign_inventory_results_have_explicit_failure_semantics() {
    let factory = Arc::new(Factory::default());
    let catalog = catalog(&factory);
    let sandbox = catalog
        .builder("test")
        .await
        .expect("builder")
        .label("key", "value")
        .create()
        .await
        .expect("create");
    let summary = SandboxSummary {
        reference: sandbox.reference(),
        name:      None,
        state:     SandboxState::Running,
        labels:    BTreeMap::from([("key".into(), "value".into())]),
    };
    *factory.list_results.lock().await = Some(Ok(vec![summary.clone(), summary.clone()]));
    assert_eq!(
        catalog
            .find(LabelFilter::eq("key", "value"))
            .await
            .expect("find")
            .sandboxes
            .len(),
        1
    );
    let mut conflict = summary.clone();
    conflict.state = SandboxState::Stopped;
    *factory.list_results.lock().await = Some(Ok(vec![summary.clone(), conflict]));
    let found = catalog
        .find(LabelFilter::default())
        .await
        .expect("partial failure");
    assert!(found.sandboxes.is_empty());
    assert!(matches!(found.failures[0].error, Error::Transport { .. }));
    let mut foreign = summary;
    foreign.reference.provider = name("other");
    *factory.list_results.lock().await = Some(Ok(vec![foreign]));
    assert_eq!(
        catalog
            .find(LabelFilter::default())
            .await
            .expect("find")
            .unavailable,
        vec![name("entry")]
    );
}

#[tokio::test]
async fn closed_catalog_does_not_reactivate_even_through_clones_or_builders() {
    let factory = Arc::new(Factory::default());
    let catalog = catalog(&factory);
    let clone = catalog.clone();
    let builder = catalog.builder("test").await.expect("builder");
    catalog.shutdown().await.expect("shutdown");
    assert!(matches!(builder.create().await, Err(Error::Stale { .. })));
    assert!(matches!(
        clone.find(LabelFilter::default()).await,
        Err(Error::Stale { .. })
    ));
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn uncertain_create_is_not_replayed() {
    let factory = Arc::new(Factory {
        create_error: Some(Error::Transport {
            message: "connection lost after admission".into(),
            outcome: MutationOutcome::Unknown,
        }),
        ..Factory::default()
    });
    let catalog = catalog(&factory);
    assert!(matches!(
        catalog
            .builder("test")
            .await
            .expect("builder")
            .create()
            .await,
        Err(Error::Transport {
            outcome: MutationOutcome::Unknown,
            ..
        })
    ));
    assert_eq!(factory.creates.load(Ordering::SeqCst), 1);
    assert_eq!(factory.activations.lock().await.len(), 1);
}

#[tokio::test]
async fn inventory_preserves_successes_beside_missing_provider() {
    let factory = Arc::new(Factory::default());
    let unavailable: ProviderEntry =
        serde_json::from_value(json!({"plugin":"missing"})).expect("entry");
    let catalog = catalog_with(
        &factory,
        [(name("entry"), entry()), (name("offline"), unavailable)],
        options(),
    );
    let sandbox = catalog
        .builder("test")
        .await
        .expect("builder")
        .label("job", "test")
        .create()
        .await
        .expect("create");
    let found = catalog
        .find(LabelFilter::eq("job", "test"))
        .await
        .expect("partial result");
    assert_eq!(found.sandboxes.len(), 1);
    assert_eq!(found.sandboxes[0].reference, sandbox.reference());
    assert_eq!(found.unavailable, vec![name("offline")]);
}

#[tokio::test]
async fn environment_store_failure_does_not_become_no_environment() {
    struct Unreachable;
    #[async_trait]
    impl EnvironmentSource for Unreachable {
        async fn names(&self) -> Result<Vec<EnvironmentName>, Error> {
            Err(Error::Unavailable {
                message: "store offline".into(),
                outcome: MutationOutcome::NotStarted,
            })
        }
        async fn get(&self, _: &str) -> Result<Option<Environment>, Error> {
            Err(Error::Unavailable {
                message: "store offline".into(),
                outcome: MutationOutcome::NotStarted,
            })
        }
    }
    let factory = Arc::new(Factory::default());
    let catalog = Sandboxes::try_new(
        [(name("entry"), entry())],
        Arc::new(Unreachable),
        &registry(&factory),
        Arc::new(Secrets::default()),
        options(),
    )
    .expect("cold catalog");
    assert!(matches!(
        catalog.builder("test").await,
        Err(Error::Unavailable { .. })
    ));
    assert!(matches!(
        catalog.environments().await,
        Err(Error::Unavailable { .. })
    ));
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unresolved_environment_diagnostics_do_not_probe_or_disable_other_entries() {
    let factory = Arc::new(Factory::default());
    let mut source = environments().0.clone();
    source.insert(env_name("broken"), Environment {
        provider: name("not-installed"),
        spec:     SandboxSpec::new(Source::Image("workload".into())),
    });
    let catalog = Sandboxes::try_new(
        [(name("entry"), entry())],
        Arc::new(StaticEnvironments(source)),
        &registry(&factory),
        Arc::new(Secrets::default()),
        options(),
    )
    .expect("catalog");
    let diagnostics = catalog.diagnostics().await.expect("diagnostics");
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].environment, Some(env_name("broken")));
    assert!(matches!(diagnostics[0].error, Error::NoProvider { .. }));
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 0);
    assert!(matches!(
        catalog.builder("broken").await,
        Err(Error::NoProvider { .. })
    ));
    catalog
        .builder("test")
        .await
        .expect("builder")
        .create()
        .await
        .expect("good environment works");
}

#[tokio::test]
async fn repeated_shutdown_preserves_unconfirmed_cleanup() {
    let factory = Arc::new(Factory {
        shutdown_error: Some(Error::FenceLeaked {
            message: "cleanup unconfirmed".into(),
        }),
        ..Factory::default()
    });
    let catalog = catalog(&factory);
    catalog
        .provider("entry")
        .expect("entry")
        .describe()
        .await
        .expect("activate");
    let clone = catalog.clone();
    assert!(matches!(
        catalog.shutdown().await,
        Err(Error::FenceLeaked { .. })
    ));
    assert!(matches!(
        clone.shutdown().await,
        Err(Error::FenceLeaked { .. })
    ));
    assert_eq!(
        factory
            .fake
            .calls()
            .await
            .iter()
            .filter(|call| call.operation == "shutdown")
            .count(),
        1
    );
}

#[tokio::test]
async fn explicit_plugin_trust_never_falls_back_to_a_same_kind_builtin() {
    let factory = Arc::new(Factory::default());
    let mut configured = entry();
    configured.trust = Some(PluginTrust::Development);
    let catalog = catalog_with(&factory, [(name("entry"), configured)], options());
    assert!(matches!(
        catalog.provider("entry").expect("entry").describe().await,
        Err(Error::Unavailable { .. })
    ));
    assert_eq!(factory.discovery.load(Ordering::SeqCst), 0);
}
