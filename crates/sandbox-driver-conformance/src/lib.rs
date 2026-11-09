//! Black-box conformance suite for sandbox-driver providers.
//!
//! Runs the same end-to-end battery against any [`SandboxProvider`] —
//! in-process or behind the JSON-RPC protocol — and reports per-check
//! outcomes. Checks are **capability-gated in both directions**: a
//! declared capability must work, and an undeclared one must return
//! [`Error::Unsupported`]. That is the capability-honesty contract from
//! the design.
//!
//! ```ignore
//! let report = Conformance::new(provider, SpecFactory::new(make_spec))
//!     .run()
//!     .await;
//! report.assert_pass();
//! ```
//!
//! Each check provisions its own sandbox from the provider-appropriate
//! spec the factory returns and deletes it afterwards, so the suite is
//! safe to run against real (billed) providers — expect roughly three dozen
//! short-lived sandboxes per run.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, mem};

use sandbox_driver::{Capability, Sandbox, SandboxProvider, SandboxSpec, WaitOptions, activate};
use tokio::time;

use crate::check::{CheckFn, CheckOutcome, Verdict, fail, require_on};

mod check;
pub mod contract;
mod exec;
mod exec_stop;
mod exec_streaming;
mod facets;
mod fs;
mod git;
mod honesty;
mod lifecycle;
mod volumes;

/// Produces a provider-appropriate creation spec for each check.
pub struct SpecFactory {
    make:                 Box<dyn Fn() -> SandboxSpec + Send + Sync>,
    make_entrypoint_logs: Option<Box<dyn Fn() -> SandboxSpec + Send + Sync>>,
    git_clone_url:        Option<String>,
    one_shot_image:       Option<String>,
}

impl SpecFactory {
    pub fn new(make: impl Fn() -> SandboxSpec + Send + Sync + 'static) -> Self {
        Self {
            make:                 Box::new(make),
            make_entrypoint_logs: None,
            git_clone_url:        None,
            one_shot_image:       None,
        }
    }

    /// A registry image with `sh`, `cat`, `printf`, and `sleep`, for the
    /// one-shot container check. Providers that declare
    /// [`sandbox_driver::OneShot`] must configure one.
    #[must_use]
    pub fn with_one_shot_image(mut self, reference: impl Into<String>) -> Self {
        self.one_shot_image = Some(reference.into());
        self
    }

    /// Adds a spec whose entrypoint emits output and remains running.
    /// Providers that declare [`sandbox_driver::Logs`] use this fixture
    /// for the behavioral follow-stream check.
    #[must_use]
    pub fn with_entrypoint_logs(
        mut self,
        make: impl Fn() -> SandboxSpec + Send + Sync + 'static,
    ) -> Self {
        self.make_entrypoint_logs = Some(Box::new(make));
        self
    }

    /// Overrides the sandbox-local `file://` clone fixture.
    ///
    /// Use this for providers whose native Git API accepts remote repository
    /// URLs only. The repository needs no specific contents or default branch;
    /// the check normalizes the clone before testing worktree operations.
    #[must_use]
    pub fn with_git_clone_url(mut self, url: impl Into<String>) -> Self {
        self.git_clone_url = Some(url.into());
        self
    }

    pub(crate) fn spec(&self) -> SandboxSpec {
        (self.make)()
    }

    pub(crate) fn entrypoint_logs_spec(&self) -> Option<SandboxSpec> {
        self.make_entrypoint_logs.as_ref().map(|make| make())
    }

    pub(crate) fn git_clone_url(&self) -> Option<&str> {
        self.git_clone_url.as_deref()
    }

    pub(crate) fn one_shot_image(&self) -> Option<&str> {
        self.one_shot_image.as_deref()
    }
}

/// Suite configuration.
pub struct Conformance {
    pub(crate) provider: Arc<dyn SandboxProvider>,
    pub(crate) specs:    SpecFactory,
    /// Per-check wall-clock budget; a hung provider fails, not hangs.
    pub check_timeout:   Duration,
    /// Wait options for state transitions (slow cloud providers need a
    /// longer deadline).
    pub wait:            WaitOptions,
    /// Sandboxes a running check has created and not yet deleted. The
    /// harness deletes whatever is still here once the check settles, so
    /// a check whose future is dropped by the budget cannot leak one.
    leased:              Mutex<Vec<Arc<dyn Sandbox>>>,
}

/// How long the harness gives each leftover delete after a check settles.
/// This is separate from `check_timeout`: the check has already spent
/// its budget when this runs.
const LEASE_CLEANUP_BUDGET: Duration = Duration::from_secs(120);

impl Conformance {
    pub fn new(provider: Arc<dyn SandboxProvider>, specs: SpecFactory) -> Self {
        Self {
            provider,
            specs,
            check_timeout: Duration::from_secs(300),
            wait: WaitOptions {
                interval: Duration::from_secs(1),
                deadline: Some(Duration::from_secs(120)),
            },
            leased: Mutex::new(Vec::new()),
        }
    }

    /// Runs the whole battery.
    pub async fn run(&self) -> Report {
        self.run_matching(|_| true).await
    }

    /// Runs checks selected by name, for focused reruns against expensive
    /// providers. The report contains only checks accepted by `include`.
    pub async fn run_matching(&self, include: impl Fn(&str) -> bool) -> Report {
        self.run_checks(CHECKS, include).await
    }

    async fn run_checks(
        &self,
        checks: &[(&'static str, CheckFn)],
        include: impl Fn(&str) -> bool,
    ) -> Report {
        let mut results = Vec::new();
        for (name, check) in checks {
            if !include(name) {
                continue;
            }
            tracing::info!(check = name, "conformance check starting");
            let outcome = match time::timeout(self.check_timeout, check(self)).await {
                Ok(Ok(())) => Outcome::Passed,
                Ok(Err(Verdict::Skipped(reason))) => Outcome::Skipped { reason },
                Ok(Err(Verdict::Failed(reason))) => Outcome::Failed { reason },
                Err(_) => Outcome::Failed {
                    reason: format!("check exceeded the {:?} budget", self.check_timeout),
                },
            };
            self.reap_leases(name).await;
            tracing::info!(check = name, ?outcome, "conformance check complete");
            results.push(CheckResult { name, outcome });
        }
        Report { results }
    }

    /// Creates a sandbox from the default spec and hands it back without
    /// a lease: the callers judge its deletion themselves.
    pub(crate) async fn create(&self) -> Result<Arc<dyn Sandbox>, Verdict> {
        let spec = self.specs.spec();
        self.create_from_spec(&spec).await
    }

    /// Creates a sandbox from `spec`, unleased, like [`Conformance::create`].
    pub(crate) async fn create_from_spec(
        &self,
        spec: &SandboxSpec,
    ) -> Result<Arc<dyn Sandbox>, Verdict> {
        self.provider
            .create(spec, None)
            .await
            .map_err(|error| Verdict::Failed(format!("create failed: {error}")))
    }

    /// Creates and activates a sandbox from the default spec. The sandbox
    /// is leased; end it with [`Conformance::cleanup`].
    pub(crate) async fn ready(&self) -> Result<Arc<dyn Sandbox>, Verdict> {
        let spec = self.specs.spec();
        self.ready_from_spec(&spec).await
    }

    /// Creates and activates a sandbox from `spec`. The sandbox is leased
    /// from the moment it exists, so a check budget that expires during
    /// activation still deletes it; end the lease with
    /// [`Conformance::cleanup`].
    pub(crate) async fn ready_from_spec(
        &self,
        spec: &SandboxSpec,
    ) -> Result<Arc<dyn Sandbox>, Verdict> {
        let sandbox = self.create_from_spec(spec).await?;
        self.lease(&sandbox);
        if let Err(error) = activate(sandbox.as_ref(), &self.wait).await {
            self.cleanup(&sandbox).await;
            return fail(format!("activate failed: {error}"));
        }
        Ok(sandbox)
    }

    /// Runs `body` against a sandbox provisioned as `provision` asks and
    /// deletes the sandbox on every return: pass, skip, or fail.
    ///
    /// The delete after `body` is not cancellation-safe on its own: the
    /// harness wraps each check in `check_timeout` and drops the check
    /// future when it fires. The sandbox is therefore leased from the
    /// moment it exists, and `run_matching` deletes whatever is still
    /// leased once the check settles, on its own budget.
    pub(crate) async fn with_sandbox<F, Fut>(
        &self,
        provision: Provision<'_>,
        body: F,
    ) -> CheckOutcome
    where
        F: FnOnce(Arc<dyn Sandbox>) -> Fut,
        Fut: Future<Output = CheckOutcome>,
    {
        let sandbox = match provision {
            Provision::Created => self.create().await?,
            Provision::CreatedFrom(spec) => self.create_from_spec(spec).await?,
            Provision::Ready => self.ready().await?,
            Provision::ReadyFrom(spec) => self.ready_from_spec(spec).await?,
        };
        // A created sandbox is unleased; `ready` leased its own. Leasing
        // here is safe: nothing awaits between the sandbox existing and
        // this line.
        self.lease(&sandbox);
        let outcome = body(Arc::clone(&sandbox)).await;
        self.cleanup(&sandbox).await;
        outcome
    }

    /// [`Conformance::with_sandbox`] with [`Provision::Ready`], the shape
    /// most checks want.
    pub(crate) async fn with_ready<F, Fut>(&self, body: F) -> CheckOutcome
    where
        F: FnOnce(Arc<dyn Sandbox>) -> Fut,
        Fut: Future<Output = CheckOutcome>,
    {
        self.with_sandbox(Provision::Ready, body).await
    }

    /// Puts `sandbox` under the harness's care until [`Conformance::cleanup`]
    /// or the end of the check, whichever comes first. Leasing twice is
    /// harmless.
    pub(crate) fn lease(&self, sandbox: &Arc<dyn Sandbox>) {
        let mut leased = self.leased.lock().expect("leases lock");
        if !leased.iter().any(|held| held.id() == sandbox.id()) {
            leased.push(Arc::clone(sandbox));
        }
    }

    /// Deletes `sandbox` as best it can and ends its lease. Checks that
    /// judge the delete itself use [`Conformance::delete`].
    pub(crate) async fn cleanup(&self, sandbox: &Arc<dyn Sandbox>) {
        let _ = self.delete(sandbox).await;
    }

    /// Deletes `sandbox`, ends its lease, and returns the provider's own
    /// delete result.
    pub(crate) async fn delete(&self, sandbox: &Arc<dyn Sandbox>) -> sandbox_driver::Result<()> {
        // Delete first: a check budget that expires mid-delete leaves the
        // lease in place, and the reap retries the (idempotent) delete.
        let deleted = sandbox.delete().await;
        self.leased
            .lock()
            .expect("leases lock")
            .retain(|held| held.id() != sandbox.id());
        deleted
    }

    /// Deletes every sandbox `check` leased and did not clean up. After a
    /// normal return there is nothing here; after the budget expired the
    /// check future was dropped mid-body and this is the only delete.
    async fn reap_leases(&self, check: &str) {
        let leaked = mem::take(&mut *self.leased.lock().expect("leases lock"));
        for sandbox in leaked {
            let sandbox_id = sandbox.id().clone();
            tracing::warn!(check, %sandbox_id, "deleting a sandbox the check left behind");
            match time::timeout(LEASE_CLEANUP_BUDGET, sandbox.delete()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(check, %sandbox_id, %error, "leftover sandbox delete failed");
                }
                Err(_) => {
                    tracing::warn!(check, %sandbox_id, "leftover sandbox delete exceeded its budget");
                }
            }
        }
    }

    /// Skips the check unless the provider declares `capability`.
    pub(crate) fn require(&self, capability: Capability) -> Result<(), Verdict> {
        require_on(self.caps(), capability)
    }

    pub(crate) fn caps(&self) -> &sandbox_driver::Capabilities {
        self.provider.capabilities()
    }
}

/// How [`Conformance::with_sandbox`] provisions the sandbox it hands to
/// a check body.
#[derive(Clone, Copy)]
pub(crate) enum Provision<'a> {
    /// `create` from the default spec and nothing more: the check judges
    /// the fresh handle.
    Created,
    /// `create` from `spec`, without activation.
    CreatedFrom(&'a SandboxSpec),
    /// `create` from the default spec, then `activate`.
    Ready,
    /// `create` from `spec`, then `activate`.
    ReadyFrom(&'a SandboxSpec),
}

/// Builds the check registry from bare identifiers, so a check's report
/// name is always the name of the function that runs it.
macro_rules! checks {
    ($($module:ident :: $check:ident),* $(,)?) => {
        &[$((stringify!($check), |ctx| Box::pin($module::$check(ctx)))),*]
    };
}

/// Every check, in report order. The order is the suite's history, not
/// its module layout, so a consumer's report reads as it always has.
const CHECKS: &[(&str, CheckFn)] = checks![
    lifecycle::provider_identity_and_list,
    lifecycle::create_describe_delete,
    lifecycle::delete_is_idempotent,
    lifecycle::provider_deletes_by_id,
    lifecycle::attach_unknown_id_is_not_found,
    lifecycle::activate_passes_bash_probe,
    lifecycle::working_directory_is_effective,
    lifecycle::runtime_directory_is_private,
    exec::relative_working_dir_resolves,
    exec::exec_reports_exit_codes,
    exec::exec_env_vars_apply,
    exec::bash_helper_ignores_a_caller_bash_env,
    exec::exec_argv_is_literal,
    exec::exec_output_is_binary_safe,
    exec::exec_output_sanitization_is_consistent,
    exec::exec_stdin_round_trips,
    exec_stop::exec_timeout_terminates,
    exec_stop::exec_term_terminates,
    exec_stop::exec_kill_terminates,
    exec_stop::exec_term_does_not_escalate,
    exec_stop::exec_stop_grace_escalates_a_timeout,
    exec_stop::exec_stop_grace_escalates_a_term,
    exec::exec_reports_a_foreign_signal,
    exec::exec_reports_environment,
    exec_streaming::exec_streams_stdin,
    exec_streaming::exec_streaming_is_honest,
    exec_streaming::exec_streams_large_output_in_order,
    exec_streaming::exec_output_keeps_a_partial_last_line,
    facets::preview_url_reaches_a_listening_port,
    exec_streaming::exec_retention_accounting_is_consistent,
    exec_streaming::concurrent_streams_do_not_starve_each_other,
    fs::fs_round_trips,
    fs::search_greps_directories_and_single_files,
    git::git_round_trip,
    git::git_clone_pins_a_tag,
    git::git_ambient_credentials_apply,
    git::git_verbs_report_typed_results,
    honesty::unsupported_actions_say_so,
    lifecycle::pause_resume_cycle,
    lifecycle::fork_preserves_live_process_state,
    honesty::snapshot_modes_are_honest,
    facets::stdio_process_round_trips,
    facets::pty_is_bidirectional,
    facets::logs_follow_streams_and_cancels,
    lifecycle::attach_and_list_by_label,
    lifecycle::create_emits_terminal_events,
    honesty::services_match_capabilities,
    facets::services_wait_for_ports_and_list_them,
    honesty::ssh_access_matches_capabilities,
    honesty::shell_command_access_matches_capabilities,
    honesty::exec_rejects_undeclared_stdin_and_stop,
    fs::fs_range_and_append_round_trip,
    facets::background_services_round_trip,
    lifecycle::provider_health_answers,
    volumes::volume_round_trip,
    facets::one_shot_shares_the_sandbox_world,
    fs::fs_missing_file_is_not_found,
];

/// Outcome of one check.
#[derive(Clone, Debug)]
pub enum Outcome {
    Passed,
    Skipped { reason: String },
    Failed { reason: String },
}

/// One line of the report.
#[derive(Clone, Debug)]
pub struct CheckResult {
    pub name:    &'static str,
    pub outcome: Outcome,
}

/// The suite's result.
#[derive(Clone, Debug)]
pub struct Report {
    pub results: Vec<CheckResult>,
}

impl Report {
    pub fn failures(&self) -> Vec<&CheckResult> {
        self.results
            .iter()
            .filter(|result| matches!(result.outcome, Outcome::Failed { .. }))
            .collect()
    }

    /// Panics with the full report when any check failed.
    pub fn assert_pass(&self) {
        assert!(self.failures().is_empty(), "conformance failures:\n{self}");
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for result in &self.results {
            match &result.outcome {
                Outcome::Passed => writeln!(f, "PASS {}", result.name)?,
                Outcome::Skipped { reason } => {
                    writeln!(f, "SKIP {} ({reason})", result.name)?;
                }
                Outcome::Failed { reason } => writeln!(f, "FAIL {}: {reason}", result.name)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::pin::Pin;

    use sandbox_driver::{SandboxSource, SandboxState};
    use sandbox_driver_testing::ScriptedProvider;

    use super::*;
    use crate::check::{PASS, skip};

    type Check = (&'static str, CheckFn);

    /// A harness over a scripted provider with a budget short enough for
    /// a test to hang past.
    fn harness(provider: &Arc<ScriptedProvider>) -> Conformance {
        let provider: Arc<dyn SandboxProvider> = Arc::clone(provider) as Arc<dyn SandboxProvider>;
        let specs = SpecFactory::new(|| SandboxSpec::new(SandboxSource::HostDirectory));
        let mut conformance = Conformance::new(provider, specs);
        conformance.check_timeout = Duration::from_millis(200);
        conformance
    }

    fn outcome_of(report: &Report, name: &str) -> Outcome {
        report
            .results
            .iter()
            .find(|result| result.name == name)
            .unwrap_or_else(|| panic!("{name} is in the report"))
            .outcome
            .clone()
    }

    /// The check creates its sandbox and then never returns, so the
    /// budget drops it mid-body: the one path where the body's own
    /// cleanup cannot run.
    fn hangs_after_create(
        ctx: &Conformance,
    ) -> Pin<Box<dyn Future<Output = CheckOutcome> + Send + '_>> {
        Box::pin(ctx.with_ready(|_sandbox| pending()))
    }

    #[tokio::test]
    async fn a_check_that_exceeds_its_budget_still_deletes_its_sandbox() {
        let provider = Arc::new(ScriptedProvider::default());
        let harness = harness(&provider);
        let checks: &[Check] = &[("hangs_after_create", hangs_after_create)];

        let report = harness.run_checks(checks, |_| true).await;

        let outcome = outcome_of(&report, "hangs_after_create");
        assert!(
            matches!(&outcome, Outcome::Failed { reason } if reason.contains("budget")),
            "{outcome:?}"
        );
        let sandboxes = provider.sandboxes();
        assert_eq!(sandboxes.len(), 1, "the check created one sandbox");
        assert_eq!(
            sandboxes[0].delete_count(),
            1,
            "the harness deleted it once"
        );
        assert_eq!(sandboxes[0].current_state(), SandboxState::Deleted);
        assert!(harness.leased.lock().expect("leases lock").is_empty());
    }

    #[tokio::test]
    async fn with_sandbox_deletes_on_pass_skip_and_fail() {
        fn passes(ctx: &Conformance) -> Pin<Box<dyn Future<Output = CheckOutcome> + Send + '_>> {
            Box::pin(ctx.with_ready(|_sandbox| async { PASS }))
        }
        fn skips(ctx: &Conformance) -> Pin<Box<dyn Future<Output = CheckOutcome> + Send + '_>> {
            Box::pin(ctx.with_ready(|_sandbox| async { skip("not today") }))
        }
        fn fails(ctx: &Conformance) -> Pin<Box<dyn Future<Output = CheckOutcome> + Send + '_>> {
            Box::pin(ctx.with_ready(|_sandbox| async { fail("broken") }))
        }
        let provider = Arc::new(ScriptedProvider::default());
        let harness = harness(&provider);
        let checks: &[Check] = &[("passes", passes), ("skips", skips), ("fails", fails)];

        let report = harness.run_checks(checks, |_| true).await;

        assert!(matches!(outcome_of(&report, "passes"), Outcome::Passed));
        assert!(matches!(
            outcome_of(&report, "skips"),
            Outcome::Skipped { reason } if reason == "not today"
        ));
        assert!(matches!(
            outcome_of(&report, "fails"),
            Outcome::Failed { reason } if reason == "broken"
        ));
        let sandboxes = provider.sandboxes();
        assert_eq!(sandboxes.len(), 3, "one sandbox per check");
        for sandbox in &sandboxes {
            assert_eq!(
                sandbox.delete_count(),
                1,
                "{} was deleted once",
                sandbox.id()
            );
        }
    }

    #[tokio::test]
    async fn require_skips_with_the_capability_name_and_creates_nothing() {
        fn needs_pause(
            ctx: &Conformance,
        ) -> Pin<Box<dyn Future<Output = CheckOutcome> + Send + '_>> {
            Box::pin(async move {
                ctx.require(Capability::LifecyclePause)?;
                ctx.with_ready(|_sandbox| async { PASS }).await
            })
        }
        let provider = Arc::new(ScriptedProvider::default());
        let harness = harness(&provider);
        let checks: &[Check] = &[("needs_pause", needs_pause)];

        let report = harness.run_checks(checks, |_| true).await;

        assert!(matches!(
            outcome_of(&report, "needs_pause"),
            Outcome::Skipped { reason } if reason == "capability lifecycle.pause not declared"
        ));
        assert!(provider.sandboxes().is_empty());
    }

    #[tokio::test]
    async fn run_matching_keeps_only_the_included_checks_in_order() {
        let provider = Arc::new(ScriptedProvider::default());
        let harness = harness(&provider);

        let report = harness
            .run_matching(|name| {
                matches!(
                    name,
                    "provider_health_answers" | "provider_identity_and_list"
                )
            })
            .await;

        let names: Vec<&str> = report.results.iter().map(|result| result.name).collect();
        assert_eq!(names, [
            "provider_identity_and_list",
            "provider_health_answers"
        ]);
        report.assert_pass();
    }
}
