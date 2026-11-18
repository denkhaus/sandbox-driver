//! Fork-only presence and behavior pin for the one-shot facet's
//! mint-time image eviction (work#1440, fork branch `fabro`). A
//! `just vendor import` from a wrong fork state rewrites
//! `vendor/PINS.toml` cleanly and would silently drop the feature;
//! this test keeps it observable.
//!
//! Requires a reachable Docker daemon; the test passes trivially
//! without one, as the conformance run does.

use std::process;

use bollard::Docker;
use bollard::image::TagImageOptions;
use sandbox_driver::{SandboxProvider, SandboxSource, SandboxSpec};
use sandbox_driver_docker::DockerProvider;

const ALPINE: &str = "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17";

/// Tagging the base image with an eviction-prefixed alias and evicting
/// with `keep = 0` must return the alias and remove it from the
/// daemon. The alias is one tag on the shared base image, so the sweep
/// untags it and the base image survives under its own references.
#[tokio::test(flavor = "multi_thread")]
async fn evict_lru_images_removes_prefixed_aliases_beyond_the_ceiling() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let name = format!("evict-lru-presence-{}", process::id());
    let sandbox = provider
        .create(
            &SandboxSpec::new(SandboxSource::Image {
                reference: ALPINE.to_owned(),
            })
            .name(name.clone()),
            None,
        )
        .await
        .expect("create sandbox");

    let docker = Docker::connect_with_local_defaults().expect("docker");
    let alias = format!("petri-env-fork-evict-test:{name}");
    docker
        .tag_image(
            ALPINE,
            Some(TagImageOptions {
                repo: "petri-env-fork-evict-test".to_owned(),
                tag:  name.clone(),
            }),
        )
        .await
        .expect("tag alias");

    let evicted = sandbox
        .one_shot()
        .expect("facet")
        .evict_lru_images("petri-env-fork-evict-test", 0)
        .await
        .expect("eviction sweep");
    assert!(
        evicted.iter().any(|reference| reference == &alias),
        "the alias must be evicted: {evicted:?}"
    );
    let present = match docker.inspect_image(&alias).await {
        Ok(_) => true,
        Err(error) => {
            let text = format!("{error}");
            assert!(
                text.contains("404") || text.contains("not found"),
                "inspecting the alias failed unexpectedly: {text}"
            );
            false
        }
    };
    assert!(!present, "the evicted alias must be gone from the daemon");

    sandbox.delete().await.expect("delete");
}
