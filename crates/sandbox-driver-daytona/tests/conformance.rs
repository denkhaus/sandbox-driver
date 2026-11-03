//! The daytona provider against the black-box conformance suite.
//!
//! Live test: requires `DAYTONA_API_KEY` and creates roughly a dozen
//! short-lived sandboxes from the default snapshot. Ignored by default;
//! run it with `mise run test:live`.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use sandbox_driver::{SandboxKind, SandboxSource, SandboxSpec, SnapshotId};
use sandbox_driver_conformance::{Conformance, SpecFactory};
use sandbox_driver_daytona::DaytonaProvider;

mod support;

use support::init_diagnostics;

const TEST_SNAPSHOT: &str = "daytona-medium";
const TEST_GIT_REPOSITORY: &str = "https://github.com/octocat/Hello-World.git";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
#[expect(
    clippy::print_stderr,
    reason = "the per-check report is the test's evidence"
)]
async fn daytona_provider_passes_conformance() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect()
        .await
        .expect("connect with credentials");
    let specs = SpecFactory::new(default_spec)
        .with_git_clone_url(TEST_GIT_REPOSITORY)
        .with_entrypoint_logs(entrypoint_logs_spec);
    let mut conformance = Conformance::new(Arc::new(provider), specs);
    conformance.check_timeout = Duration::from_secs(900);
    conformance.wait.deadline = Some(Duration::from_secs(300));
    // One named check reruns alone while it is being fixed.
    let report = match env::var("SANDBOX_DRIVER_DAYTONA_CONFORMANCE_CHECK") {
        Ok(only) => conformance.run_matching(|name| name == only).await,
        Err(_) => conformance.run().await,
    };
    eprintln!("{report}");
    report.assert_pass();
}

fn default_spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .sandbox_kind(SandboxKind::Container)
    .working_directory("/home/daytona/sandbox-driver-conformance")
    .ephemeral(true)
}

fn entrypoint_logs_spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Dockerfile {
        content: r#"FROM debian:stable-slim
ENTRYPOINT ["/bin/sh", "-c", "echo conformance-entrypoint; echo conformance-entrypoint-error >&2; exec sleep 600"]
"#
        .to_owned(),
    })
    .sandbox_kind(SandboxKind::Container)
    .ephemeral(true)
}
