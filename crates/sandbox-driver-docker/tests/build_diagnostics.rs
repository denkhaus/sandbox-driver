//! The image-build path's failure diagnostics: a failed build carries
//! the daemon's verdict AND the tail of its streamed output, and a
//! stream-level failure carries the walked cause chain (work#1253,
//! fork branch `fabro`).
//!
//! Requires a reachable Docker daemon; tests pass trivially without
//! one, as the conformance run does.

use std::process;
use std::time::Duration;

use bollard::Docker;
use sandbox_driver::{
    ExecControls, OneShotImage, OneShotSpec, SandboxProvider, SandboxSource, SandboxSpec,
};
use sandbox_driver_docker::DockerProvider;

const ALPINE: &str = "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17";

/// A build whose RUN prints a marker line and then fails: the daemon's
/// verdict text and the printed marker must BOTH reach the failure
/// message — on this daemon the verdict rides the stream detail
/// (bollard's decoder rejects the classic error frame), and the tail
/// names what the builder did last.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_build_carries_the_daemon_verdict_and_the_output_tail() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let name = format!("build-diagnostics-verdict-{}", process::id());
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
    sandbox
        .fs()
        .write(
            "action/Dockerfile",
            format!("FROM {ALPINE}\nRUN echo build-tail-marker && exit 3\n").as_bytes(),
        )
        .await
        .expect("write dockerfile");
    let tag = format!("sandbox-driver-build-diagnostics-verdict:{name}");
    let spec = OneShotSpec::new(OneShotImage::Build {
        context:    "action".to_owned(),
        dockerfile: None,
        tag:        tag.clone(),
        reuse:      false,
    })
    .timeout(Duration::from_secs(120));
    let run = sandbox
        .one_shot()
        .expect("facet")
        .run(&spec, ExecControls::default())
        .await;
    let message = run
        .expect_err("the failing build must fail the one-shot")
        .to_string();
    sandbox.delete().await.expect("delete sandbox");
    let _ = Docker::connect_with_local_defaults()
        .expect("docker")
        .remove_image(&tag, None, None)
        .await;
    assert!(
        message.contains("non-zero"),
        "the daemon verdict must be carried: {message}"
    );
    assert!(
        message.contains("build-tail-marker"),
        "the build output tail must be carried: {message}"
    );
}
