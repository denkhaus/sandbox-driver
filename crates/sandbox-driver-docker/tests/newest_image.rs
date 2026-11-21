//! Fork-only presence and behavior pin for the one-shot facet's
//! newest-image lookup (work#1551, fork branch `fabro`). A
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

/// The newest-image lookup orders by the daemon's creation time with the
/// reference breaking ties (the eviction sweep's ordering), answers
/// `None` for a prefix the daemon holds nothing under, and agrees with
/// the eviction sweep on which image is newest. The aliases ride one
/// base image, so both carry the same creation stamp and the pin
/// exercises the deterministic tiebreak — the exact case a fleet of
/// tags minted in the same second presents.
#[tokio::test(flavor = "multi_thread")]
async fn newest_image_orders_prefixed_references_and_agrees_with_the_sweep() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let name = format!("newest-image-presence-{}", process::id());
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
    let repo = format!("petri-env-fork-newest-test-{name}");
    for alias in ["a", "z"] {
        docker
            .tag_image(
                ALPINE,
                Some(TagImageOptions {
                    repo: repo.clone(),
                    tag:  alias.to_owned(),
                }),
            )
            .await
            .expect("tag alias");
    }

    let one_shot = sandbox.one_shot().expect("facet");
    let prefix = format!("{repo}:");
    let newest = one_shot
        .newest_image(&prefix)
        .await
        .expect("newest-image lookup");
    assert!(
        newest.as_deref().is_some_and(
            |reference| reference == format!("{repo}:a") || reference == format!("{repo}:z")
        ),
        "the tiebreak must pick one of the prefixed aliases: {newest:?}"
    );
    let absent = one_shot
        .newest_image("petri-env-fork-newest-test-absent-")
        .await
        .expect("absent lookup");
    assert_eq!(absent, None, "an unknown prefix must answer None");

    // The eviction sweep and the lookup must agree on "newest": with
    // keep = 1 the image the lookup named stays on the daemon. Both
    // aliases ride one base image, so the sweep — which works at IMAGE
    // granularity, one reference per image — keeps that image and
    // evicts nothing; the agreement, not an eviction, is the pin.
    let evicted = one_shot
        .evict_lru_images(&prefix, 1)
        .await
        .expect("eviction sweep");
    let kept = one_shot
        .newest_image(&prefix)
        .await
        .expect("newest-image lookup after the sweep");
    assert_eq!(
        kept, newest,
        "the sweep must keep what the lookup named newest (evicted: {evicted:?})"
    );
    assert!(
        evicted.is_empty(),
        "one image carries both aliases, so nothing older exists to evict: {evicted:?}"
    );

    sandbox.delete().await.expect("delete");
}
