//! The git clone contract on both transports. Daytona is the one bundled
//! provider whose clone is native, so it is the one that can prove
//! `git/clone` selects the same implementation over the wire as
//! in-process: branch, pinned SHA, depth, credentials, and the
//! unavailable-commit failure must behave the same either way.
//!
//! Live test: requires `DAYTONA_API_KEY`; run with `mise run test:live`.

use std::sync::Arc;
use std::time::Duration;
use std::{env, process};

use sandbox_driver::{
    Capability, Error, ExecSpec, Git, GitCloneOptions, GitCredentials, GitFailureKind, Sandbox,
    SandboxKind, SandboxProvider, SandboxSource, SandboxSpec, SnapshotId,
};
use sandbox_driver_daytona::DaytonaProvider;
use sandbox_driver_protocol::{PluginProvider, serve};
use tokio::io::{duplex, split};

mod support;

use support::init_diagnostics;

const TEST_SNAPSHOT: &str = "daytona-medium";
/// A public repository with a stable history. `master` has more than one
/// commit, so a depth-1 clone pinned to the root commit proves the pin
/// is fetched directly rather than found in the branch's shallow window.
const REPOSITORY: &str = "https://github.com/octocat/Hello-World.git";
const BRANCH: &str = "master";
const ROOT_COMMIT: &str = "553c2077f0edc3d5dc5d17262f6aa498e69d6f8e";
const MISSING_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

fn spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .sandbox_kind(SandboxKind::Container)
    .working_directory(format!("/home/daytona/sd-wire-git-{}", process::id()))
    .ephemeral(true)
}

async fn served() -> PluginProvider {
    let provider = DaytonaProvider::connect()
        .await
        .expect("connect with credentials");
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    tokio::spawn(serve(Arc::new(provider), plugin_read, plugin_write));
    PluginProvider::connect(host_read, host_write)
        .await
        .expect("handshake")
}

async fn head(sandbox: &dyn Sandbox, repo: &str) -> String {
    let result = sandbox
        .exec()
        .run(
            &ExecSpec::new("git")
                .args(["rev-parse", "HEAD"])
                .working_dir(repo)
                .timeout(Duration::from_secs(30)),
        )
        .await
        .expect("rev-parse");
    assert!(result.success(), "stderr: {}", result.stderr_lossy());
    result.stdout_lossy().trim().to_owned()
}

async fn commit_count(sandbox: &dyn Sandbox, repo: &str) -> usize {
    let result = sandbox
        .exec()
        .run(
            &ExecSpec::new("git")
                .args(["rev-list", "--count", "HEAD"])
                .working_dir(repo)
                .timeout(Duration::from_secs(30)),
        )
        .await
        .expect("rev-list");
    assert!(result.success(), "stderr: {}", result.stderr_lossy());
    result.stdout_lossy().trim().parse().expect("count")
}

/// Runs the clone contract against one sandbox and reports every
/// deviation instead of stopping at the first, so one live run tells the
/// whole story.
async fn clone_contract(sandbox: &dyn Sandbox, transport: &str) -> Result<(), String> {
    if !sandbox.capabilities().supports(Capability::Git) || !sandbox.capabilities().git.native {
        return Err(format!(
            "{transport}: Daytona must declare its native git facet on both transports"
        ));
    }
    let git = sandbox
        .git()
        .ok_or_else(|| format!("{transport}: git facet missing"))?;

    // Branch clone at depth 1: exactly the branch head, one commit deep.
    let mut branch = GitCloneOptions::default();
    branch.branch = Some(BRANCH.to_owned());
    branch.depth = Some(1);
    git.clone_repo(REPOSITORY, "branch", &branch)
        .await
        .map_err(|error| format!("{transport}: branch clone: {error}"))?;
    let status = git
        .status("branch")
        .await
        .map_err(|error| format!("{transport}: branch status: {error}"))?;
    if status.current_branch.as_deref() != Some(BRANCH) || status.detached {
        return Err(format!("{transport}: branch clone status: {status:?}"));
    }
    if commit_count(sandbox, "branch").await != 1 {
        return Err(format!("{transport}: depth 1 was not honored"));
    }

    // Pinned clone: the root commit is outside the depth-1 window of the
    // branch head, so only a direct fetch of the SHA can satisfy it, and
    // the checkout must end attached to the admitted branch.
    let mut pinned = GitCloneOptions::default();
    pinned.branch = Some(BRANCH.to_owned());
    pinned.commit = Some(ROOT_COMMIT.to_owned());
    pinned.depth = Some(1);
    git.clone_repo(REPOSITORY, "pinned", &pinned)
        .await
        .map_err(|error| format!("{transport}: pinned clone: {error}"))?;
    let status = git
        .status("pinned")
        .await
        .map_err(|error| format!("{transport}: pinned status: {error}"))?;
    if status.current_branch.as_deref() != Some(BRANCH) || status.detached {
        return Err(format!("{transport}: pinned clone status: {status:?}"));
    }
    let pinned_head = head(sandbox, "pinned").await;
    if pinned_head != ROOT_COMMIT {
        return Err(format!(
            "{transport}: pinned clone checked out {pinned_head}, not the pin"
        ));
    }

    // Credentials cross to the clone: GitHub rejects a bogus token even on
    // a public repository, so the failure proves the credential reached the
    // remote, and it must be classified the same way on both transports.
    let mut authed = GitCloneOptions::default();
    authed.branch = Some(BRANCH.to_owned());
    authed.depth = Some(1);
    authed.credentials = Some(GitCredentials::new("x-access-token", "not-a-real-token"));
    match git.clone_repo(REPOSITORY, "authed", &authed).await {
        Ok(()) => {
            return Err(format!(
                "{transport}: a clone with a bogus credential succeeded, so the credential \
                 never reached the remote"
            ));
        }
        Err(Error::Git(failure)) if failure.kind() == GitFailureKind::AuthRejected => {}
        Err(error) => {
            return Err(format!(
                "{transport}: bogus credential failed with an unexpected kind: {error}"
            ));
        }
    }

    // An unavailable commit fails and never falls back to the branch head.
    let mut missing = GitCloneOptions::default();
    missing.branch = Some(BRANCH.to_owned());
    missing.commit = Some(MISSING_COMMIT.to_owned());
    match git.clone_repo(REPOSITORY, "missing", &missing).await {
        Ok(()) => {
            return Err(format!(
                "{transport}: a clone pinned to an unavailable commit succeeded"
            ));
        }
        Err(Error::Git(failure)) if failure.kind() == GitFailureKind::RefNotFound => {}
        Err(error) => {
            return Err(format!(
                "{transport}: unavailable commit failed with an unexpected kind: {error}"
            ));
        }
    }
    let leftover = sandbox
        .exec()
        .run(
            &ExecSpec::new("git")
                .args(["rev-parse", "--verify", "HEAD"])
                .working_dir("missing")
                .timeout(Duration::from_secs(30)),
        )
        .await;
    if leftover.is_ok_and(|result| result.success()) {
        return Err(format!(
            "{transport}: a failed pinned clone left a checkout at the branch head"
        ));
    }

    // A malformed pin is rejected before any network operation.
    let mut malformed = GitCloneOptions::default();
    malformed.commit = Some("-q".to_owned());
    match git.clone_repo(REPOSITORY, "malformed", &malformed).await {
        Err(Error::InvalidSpec { .. }) => Ok(()),
        Err(error) => Err(format!(
            "{transport}: flag-shaped commit failed with an unexpected kind: {error}"
        )),
        Ok(()) => Err(format!("{transport}: a flag-shaped commit was accepted")),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn git_clone_selects_the_same_implementation_on_both_transports() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();

    let in_process = DaytonaProvider::connect()
        .await
        .expect("connect with credentials");
    let local = in_process.create(&spec(), None).await.expect("create");
    let local_outcome = clone_contract(local.as_ref(), "in-process").await;
    let _ = local.delete().await;

    let remote = served().await;
    let wire = remote
        .create(&spec(), None)
        .await
        .expect("create over wire");
    let wire_outcome = clone_contract(wire.as_ref(), "wire").await;
    let _ = wire.delete().await;
    remote.shutdown().await.expect("shutdown");

    local_outcome.expect("in-process git clone contract");
    wire_outcome.expect("wire git clone contract");
}
