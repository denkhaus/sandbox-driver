//! Provider identity, sandbox lifecycle, attach and list, health, and
//! lifecycle events.

use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sandbox_driver::{
    Action, Capability, Error, Event, EventBody, EventContext, EventObserver, ExecSpec,
    HealthStatus, Sandbox, SandboxFilter, SandboxId, SandboxState, wait_for_state,
};
use tokio::sync::Notify;
use tokio::time;

use crate::check::{COMMAND_TIMEOUT, CheckOutcome, PASS, fail, run_ok, skip};
use crate::{Conformance, Provision};

/// How long a listing may lag behind a create before the check fails.
const LIST_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
const LIST_VISIBILITY_POLL: Duration = Duration::from_millis(500);

/// Names and discovery labels must distinguish suites in the same process.
fn sandbox_marker() -> String {
    static NEXT_MARKER: AtomicU64 = AtomicU64::new(0);
    format!(
        "sandbox-driver-conformance-{}-{}",
        process::id(),
        NEXT_MARKER.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Default)]
struct RecordingEventObserver {
    events:  Mutex<Vec<Event>>,
    changed: Notify,
}

#[async_trait]
impl EventObserver for RecordingEventObserver {
    async fn observe(&self, event: Event) {
        self.events.lock().expect("events lock").push(event);
        self.changed.notify_one();
    }
}

impl RecordingEventObserver {
    async fn completed(&self, action: Action) -> Result<(), String> {
        time::timeout(Duration::from_secs(30), async {
            loop {
                let notified = self.changed.notified();
                if self.events.lock().expect("events lock").iter().any(|event| matches!(event.body, EventBody::OperationCompleted { action: seen, .. } if seen == action)) { return; }
                notified.await;
            }
        }).await.map_err(|_| "event completion was not delivered".to_owned())
    }
}

pub(super) async fn provider_identity_and_list(ctx: &Conformance) -> CheckOutcome {
    if ctx.provider.kind().as_str().is_empty() {
        return fail("provider kind is empty");
    }
    ctx.provider
        .list(&SandboxFilter::default())
        .await
        .map_err(|error| format!("list failed: {error}"))?;
    if let Some(snapshots) = &ctx.provider.capabilities().snapshots {
        if (snapshots.from_image_kinds.container || snapshots.from_image_kinds.virtual_machine)
            && !snapshots.from_image
        {
            return fail("exact image snapshot support is set but aggregate support is false");
        }
        if (snapshots.from_dockerfile_kinds.container
            || snapshots.from_dockerfile_kinds.virtual_machine)
            && !snapshots.from_dockerfile
        {
            return fail("exact Dockerfile snapshot support is set but aggregate support is false");
        }
    }
    PASS
}

pub(super) async fn create_describe_delete(ctx: &Conformance) -> CheckOutcome {
    let mut spec = ctx.specs.spec();
    if spec.name.is_none() {
        spec.name = Some(sandbox_marker());
    }
    let sandbox = ctx.create_from_spec(&spec).await?;
    ctx.lease(&sandbox);
    let outcome = async {
        let status = sandbox
            .describe()
            .await
            .map_err(|error| format!("describe failed: {error}"))?;
        if status.id != *sandbox.id() {
            return fail("describe returned a different sandbox id");
        }
        if status.name != spec.name {
            return fail(format!(
                "requested display name {:?}, observed {:?}",
                spec.name, status.name
            ));
        }
        if let Some(requested) = spec.sandbox_kind {
            if status.sandbox_kind != Some(requested) {
                return fail(format!(
                    "requested sandbox kind {requested:?}, observed {:?}",
                    status.sandbox_kind
                ));
            }
        }
        if let Some(requested) = &spec.region {
            if status.region.as_deref() != Some(requested) {
                return fail(format!(
                    "requested region {requested:?}, observed {:?}",
                    status.region
                ));
            }
        }
        if sandbox.working_directory().is_empty() {
            return fail("working_directory is empty");
        }
        let platform = sandbox
            .platform_info()
            .await
            .map_err(|error| format!("platform_info failed: {error}"))?;
        if platform.os.is_empty() || platform.os != platform.os.to_lowercase() {
            return fail(format!("platform os {:?} is not lowercase", platform.os));
        }
        // A kernel release ("6.8.0-…") in the arch field is the classic
        // uname field-order mixup; real architectures have no dots.
        if platform.arch.is_empty() || platform.arch.contains('.') {
            return fail(format!(
                "platform arch {:?} does not look like an architecture",
                platform.arch
            ));
        }
        PASS
    }
    .await;
    ctx.cleanup(&sandbox).await;
    outcome?;

    let sandbox_id = sandbox.id().clone();
    // After delete, describe (via re-attach) must not report a live sandbox.
    match ctx.provider.attach(&sandbox_id, None).await {
        Err(_) => PASS,
        Ok(handle) => {
            let state = handle
                .describe()
                .await
                .map_or(SandboxState::Deleted, |status| status.state);
            if matches!(state, SandboxState::Deleted | SandboxState::Deleting) {
                PASS
            } else {
                fail(format!("sandbox still {state:?} after delete"))
            }
        }
    }
}

pub(super) async fn delete_is_idempotent(ctx: &Conformance) -> CheckOutcome {
    let sandbox = ctx.create().await?;
    sandbox
        .delete()
        .await
        .map_err(|error| format!("first delete failed: {error}"))?;
    sandbox
        .delete()
        .await
        .map_err(|error| format!("second delete failed: {error}"))?;
    PASS
}

/// `SandboxProvider::delete` removes a sandbox with no handle involved,
/// and succeeds again for the same id and for an id the provider never
/// had.
pub(super) async fn provider_deletes_by_id(ctx: &Conformance) -> CheckOutcome {
    let sandbox = ctx.create().await?;
    let id = sandbox.id().clone();
    drop(sandbox);
    ctx.provider
        .delete(&id, None)
        .await
        .map_err(|error| format!("delete by id failed: {error}"))?;
    if let Ok(handle) = ctx.provider.attach(&id, None).await {
        let state = handle
            .describe()
            .await
            .map_or(SandboxState::Deleted, |status| status.state);
        if !matches!(state, SandboxState::Deleted | SandboxState::Deleting) {
            ctx.cleanup(&handle).await;
            return fail(format!("sandbox still {state:?} after delete by id"));
        }
    }
    ctx.provider
        .delete(&id, None)
        .await
        .map_err(|error| format!("second delete by id failed: {error}"))?;
    let unknown =
        SandboxId::try_new("conformance-does-not-exist").map_err(|error| error.to_string())?;
    ctx.provider
        .delete(&unknown, None)
        .await
        .map_err(|error| format!("delete of an unknown id failed: {error}"))?;
    PASS
}

pub(super) async fn attach_unknown_id_is_not_found(ctx: &Conformance) -> CheckOutcome {
    let id = SandboxId::try_new("conformance-does-not-exist").map_err(|error| error.to_string())?;
    match ctx.provider.attach(&id, None).await {
        Err(Error::NotFound { .. }) => PASS,
        Err(other) => fail(format!("expected NotFound, got: {other}")),
        Ok(_) => fail("attach to an unknown id succeeded"),
    }
}

pub(super) async fn activate_passes_bash_probe(ctx: &Conformance) -> CheckOutcome {
    let sandbox = ctx.ready().await?;
    ctx.cleanup(&sandbox).await;
    PASS
}

pub(super) async fn working_directory_is_effective(ctx: &Conformance) -> CheckOutcome {
    let spec = ctx.specs.spec();
    let requested = spec.working_directory.clone();
    ctx.with_sandbox(Provision::ReadyFrom(&spec), |sandbox| async move {
        if let Some(requested) = &requested {
            if sandbox.working_directory() != requested {
                return fail(format!(
                    "requested working_directory {requested:?}, handle returned {:?}",
                    sandbox.working_directory()
                ));
            }
        }
        let result = sandbox
            .exec()
            .run(&ExecSpec::new("pwd").timeout(COMMAND_TIMEOUT))
            .await
            .map_err(|error| format!("exec failed: {error}"))?;
        let pwd = result.stdout_lossy().trim().to_owned();
        let expected = sandbox.working_directory();
        if pwd != expected {
            return fail(format!("pwd is {pwd:?}, working_directory is {expected:?}"));
        }
        let attached = ctx
            .provider
            .attach(sandbox.id(), None)
            .await
            .map_err(|error| format!("attach failed: {error}"))?;
        if attached.working_directory() != expected {
            return fail(format!(
                "attached working_directory is {:?}, expected {expected:?}",
                attached.working_directory()
            ));
        }
        let attached_result = attached
            .exec()
            .run(&ExecSpec::new("pwd").timeout(COMMAND_TIMEOUT))
            .await
            .map_err(|error| format!("attached exec failed: {error}"))?;
        let attached_pwd = attached_result.stdout_lossy().trim().to_owned();
        if attached_pwd != expected {
            return fail(format!(
                "attached pwd is {attached_pwd:?}, working_directory is {expected:?}"
            ));
        }
        PASS
    })
    .await
}

pub(super) async fn runtime_directory_is_private(ctx: &Conformance) -> CheckOutcome {
    ctx.with_ready(|sandbox| async move {
        let Some(runtime_directory) = sandbox.runtime_directory() else {
            return PASS;
        };
        if !runtime_directory.starts_with('/') {
            return fail(format!(
                "runtime_directory {runtime_directory:?} is not absolute"
            ));
        }
        let workspace = sandbox.working_directory().trim_end_matches('/');
        let workspace_prefix = if workspace.is_empty() {
            "/".to_owned()
        } else {
            format!("{workspace}/")
        };
        if runtime_directory == workspace || runtime_directory.starts_with(&workspace_prefix) {
            return fail(format!(
                "runtime_directory {runtime_directory:?} is inside working_directory \
                 {workspace:?}"
            ));
        }
        let metadata = sandbox
            .fs()
            .metadata(runtime_directory)
            .await
            .map_err(|error| format!("runtime_directory metadata failed: {error}"))?;
        if metadata.kind != sandbox_driver::FileKind::Directory {
            return fail(format!(
                "runtime_directory has kind {:?}, not Directory",
                metadata.kind
            ));
        }
        if metadata.mode.map(|mode| mode & 0o777) != Some(0o700) {
            return fail(format!(
                "runtime_directory mode is {:?}, expected 0700",
                metadata.mode
            ));
        }
        let attached = ctx
            .provider
            .attach(sandbox.id(), None)
            .await
            .map_err(|error| format!("attach failed: {error}"))?;
        if attached.runtime_directory() != Some(runtime_directory) {
            return fail(format!(
                "attached runtime_directory is {:?}, expected {runtime_directory:?}",
                attached.runtime_directory()
            ));
        }
        PASS
    })
    .await
}

pub(super) async fn pause_resume_cycle(ctx: &Conformance) -> CheckOutcome {
    ctx.require(Capability::LifecyclePause)?;
    ctx.with_ready(|sandbox| async move {
        // The provider's upper bound may be narrowed per sandbox class.
        if !sandbox.capabilities().lifecycle.pause {
            return skip("lifecycle.pause masked for this sandbox's class");
        }
        sandbox
            .pause()
            .await
            .map_err(|error| format!("pause failed: {error}"))?;
        wait_for_state(sandbox.as_ref(), SandboxState::Paused, &ctx.wait)
            .await
            .map_err(|error| format!("never reached Paused: {error}"))?;
        sandbox
            .resume()
            .await
            .map_err(|error| format!("resume failed: {error}"))?;
        wait_for_state(sandbox.as_ref(), SandboxState::Running, &ctx.wait)
            .await
            .map_err(|error| format!("never returned to Running: {error}"))?;
        // The sandbox must still work after the cycle.
        let result = sandbox
            .exec()
            .run(&ExecSpec::new("echo").arg("alive").timeout(COMMAND_TIMEOUT))
            .await
            .map_err(|error| format!("exec after resume failed: {error}"))?;
        if !result.success() {
            return fail("exec after resume did not succeed");
        }
        PASS
    })
    .await
}

pub(super) async fn fork_preserves_live_process_state(ctx: &Conformance) -> CheckOutcome {
    ctx.require(Capability::LifecycleFork)?;
    ctx.with_ready(|sandbox| async move {
        if !sandbox.capabilities().lifecycle.fork {
            return skip("lifecycle.fork masked for this sandbox's class");
        }

        let prepare = run_ok(
            sandbox.as_ref(),
            "fork process setup",
            &ExecSpec::bash(
                "printf preserved > /tmp/sandbox-driver-fork-marker; \
                 nohup sh -c 'echo $$ > /tmp/sandbox-driver-fork-pid; \
                 while :; do sleep 1; done' </dev/null >/dev/null 2>&1 & \
                 for i in 1 2 3 4 5; do \
                     test -s /tmp/sandbox-driver-fork-pid && break; sleep 1; \
                 done; \
                 cat /tmp/sandbox-driver-fork-pid",
            )
            .timeout(COMMAND_TIMEOUT),
        )
        .await?;
        let source_pid = prepare.stdout_lossy().trim().to_owned();

        let forked = sandbox
            .fork(&sandbox_driver::ForkOptions::default())
            .await
            .map_err(|error| format!("fork failed: {error}"))?;
        // The fork is a second billed sandbox; lease it so a budget that
        // expires below still deletes it.
        ctx.lease(&forked);
        let outcome = check_fork(&forked, &source_pid).await;
        ctx.cleanup(&forked).await;
        outcome
    })
    .await
}

/// The forked sandbox is running, the source's background process is
/// alive in it under the same PID, and the source's file is present.
async fn check_fork(forked: &Arc<dyn Sandbox>, source_pid: &str) -> CheckOutcome {
    let status = forked
        .describe()
        .await
        .map_err(|error| format!("describing fork failed: {error}"))?;
    if status.state != SandboxState::Running {
        return fail(format!("fork returned in state {:?}", status.state));
    }
    let result = forked
        .exec()
        .run(
            &ExecSpec::bash(
                "pid=$(cat /tmp/sandbox-driver-fork-pid); \
                 kill -0 \"$pid\"; printf '%s ' \"$pid\"; \
                 cat /tmp/sandbox-driver-fork-marker",
            )
            .timeout(COMMAND_TIMEOUT),
        )
        .await
        .map_err(|error| format!("checking forked process failed: {error}"))?;
    if !result.success() {
        return fail(format!(
            "forked process is not running: {}",
            result.stderr_lossy()
        ));
    }
    let expected = format!("{source_pid} preserved");
    if result.stdout_lossy().trim() != expected {
        return fail(format!(
            "fork did not preserve PID and filesystem: got {:?}, expected {expected:?}",
            result.stdout_lossy().trim()
        ));
    }
    PASS
}

pub(super) async fn attach_and_list_by_label(ctx: &Conformance) -> CheckOutcome {
    let marker = sandbox_marker();
    let mut spec = ctx.specs.spec();
    spec.labels
        .insert("sandbox-driver-conformance".to_owned(), marker.clone());
    ctx.with_sandbox(Provision::CreatedFrom(&spec), |sandbox| async move {
        let attached = ctx
            .provider
            .attach(sandbox.id(), None)
            .await
            .map_err(|error| format!("attach failed: {error}"))?;
        if attached.id() != sandbox.id() {
            return fail("attach returned a different sandbox");
        }
        let mut filter = SandboxFilter::default();
        filter
            .labels
            .insert("sandbox-driver-conformance".to_owned(), marker.clone());
        // Listings may lag behind a create, so poll until the new sandbox
        // is the filter's only match.
        let mut listed = Vec::new();
        let visible = time::timeout(LIST_VISIBILITY_TIMEOUT, async {
            loop {
                listed = ctx
                    .provider
                    .list(&filter)
                    .await
                    .map_err(|error| format!("list failed: {error}"))?;
                if listed.len() == 1 && listed[0].id == *sandbox.id() {
                    return Ok::<_, String>(());
                }
                time::sleep(LIST_VISIBILITY_POLL).await;
            }
        })
        .await;
        match visible {
            Ok(listing) => {
                listing?;
                PASS
            }
            Err(_) => fail(format!(
                "label filter returned {} sandboxes after {LIST_VISIBILITY_TIMEOUT:?}",
                listed.len()
            )),
        }
    })
    .await
}

/// `health` must answer — a working provider (this suite just created
/// sandboxes on it) must not report itself unreachable or unauthorized.
pub(super) async fn provider_health_answers(ctx: &Conformance) -> CheckOutcome {
    let health = ctx
        .provider
        .health()
        .await
        .map_err(|error| format!("health failed: {error}"))?;
    match health.status {
        HealthStatus::Ok | HealthStatus::Unknown => PASS,
        status => fail(format!(
            "a working provider reported {status:?}: {:?} (missing: {:?})",
            health.message, health.missing_permissions
        )),
    }
}

pub(super) async fn create_emits_terminal_events(ctx: &Conformance) -> CheckOutcome {
    let observer = Arc::new(RecordingEventObserver::default());
    let context = EventContext::new(observer.clone());
    let sandbox = ctx
        .provider
        .create(&ctx.specs.spec(), Some(context))
        .await
        .map_err(|error| format!("create failed: {error}"))?;
    ctx.lease(&sandbox);
    observer.completed(Action::Create).await?;
    let seen = observer.events.lock().expect("events lock").clone();
    ctx.delete(&sandbox)
        .await
        .map_err(|error| format!("delete failed: {error}"))?;
    observer.completed(Action::Delete).await?;
    let all_seen = observer.events.lock().expect("events lock").clone();

    let Some(first) = seen.first() else {
        return fail("create emitted no events");
    };
    if !matches!(first.body, EventBody::OperationStarted {
        action: Action::Create,
    }) {
        return fail(format!("first create event was {:?}", first.body));
    }
    let Some(last) = seen.last() else {
        unreachable!("the first event exists");
    };
    if !matches!(last.body, EventBody::OperationCompleted {
        action: Action::Create,
        ..
    }) {
        return fail(format!(
            "last create event was not completed: {:?}",
            last.body
        ));
    }
    if first.operation_id.is_none() || first.operation_id != last.operation_id {
        return fail("create start and completion have different operation ids");
    }
    if !matches!(
        &last.subject,
        sandbox_driver::EventSubject::Sandbox { id: Some(id), .. } if id == sandbox.id()
    ) {
        return fail("create completion does not identify the created sandbox");
    }
    if !is_contiguous(&seen) {
        return fail("create event source or sequence is not continuous");
    }
    let delete_events: Vec<&Event> = all_seen
        .iter()
        .filter(|event| {
            matches!(
                event.body,
                EventBody::OperationStarted {
                    action: Action::Delete,
                } | EventBody::OperationProgress {
                    action: Action::Delete,
                    ..
                } | EventBody::OperationCompleted {
                    action: Action::Delete,
                    ..
                } | EventBody::OperationFailed {
                    action: Action::Delete,
                    ..
                }
            )
        })
        .collect();
    let (Some(delete_started), Some(delete_terminal)) =
        (delete_events.first(), delete_events.last())
    else {
        return fail(format!(
            "delete emitted {} lifecycle events",
            delete_events.len()
        ));
    };
    if !matches!(delete_started.body, EventBody::OperationStarted {
        action: Action::Delete,
    }) {
        return fail(format!("first delete event was {:?}", delete_started.body));
    }
    if !matches!(delete_terminal.body, EventBody::OperationCompleted {
        action: Action::Delete,
        ..
    }) {
        return fail(format!(
            "last delete event was not completed: {:?}",
            delete_terminal.body
        ));
    }
    if delete_events
        .iter()
        .any(|event| event.operation_id != delete_started.operation_id)
    {
        return fail("delete events do not share one operation id");
    }
    let starts = delete_events
        .iter()
        .filter(|event| matches!(event.body, EventBody::OperationStarted { .. }))
        .count();
    if starts != 1 {
        return fail(format!("delete emitted {starts} start events, expected 1"));
    }
    let terminals = delete_events
        .iter()
        .filter(|event| event.body.is_terminal())
        .count();
    if terminals != 1 {
        return fail(format!(
            "delete emitted {terminals} terminal events, expected 1"
        ));
    }
    if !is_contiguous(&all_seen) {
        return fail("event source or sequence changed across create and delete");
    }
    PASS
}

/// Consecutive events come from one source and their sequence numbers
/// step by one.
fn is_contiguous(events: &[Event]) -> bool {
    events.windows(2).all(|pair| {
        pair[0].source_id() == pair[1].source_id()
            && pair[0].sequence().checked_add(1) == Some(pair[1].sequence())
    })
}
