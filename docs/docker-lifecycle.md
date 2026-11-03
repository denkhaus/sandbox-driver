# Docker lifecycle coordination

The Docker provider serializes resource creation, command admission, one-shot
admission, and lifecycle mutations through a daemon-scoped claim. Independent
provider instances that connect to the same daemon use the same claim. Existing
library calls and protocol v2 use this implementation; it does not declare that
the Docker provider implements the entire next API.

The claim is an unstarted container with a unique Docker name. It carries
`sh.sandbox-driver.fence` and `sh.sandbox-driver.fence-operation` labels and is
excluded from sandbox inventory. Creation acquires a claim before exposing the
primary container; its `sh.sandbox-driver.fence` label is the requested sandbox
name, so deleting that name finds the claim before the primary exists. The
primary's `sh.sandbox-driver.fence-name` label records
that claim name for subsequent clients. Older sandboxes without this label use
the canonical container ID to identify their claim.

Lifecycle and admission claims also record the primary's Docker name in
`sh.sandbox-driver.fence-sandbox-name`. If the primary disappears before claim
release, retries by name, full ID, or short ID still report `busy`. Discovery
lists claims and matches their preserved identities; unrelated names remain
absent. Claims created by older versions without the name label must be
reconciled using their recorded identifier when the primary is already gone.

An admitted exec releases its claim after Docker starts the attached execution;
the command itself can then run concurrently with other commands. One-shots
release admission after their container has started and carry the sandbox ID
for later discovery. Image preparation precedes one-shot admission. A prepared
one-shot must still acquire the claim and observe a running sandbox before it
can start.

Stop holds the claim while removing one-shots, stopping the primary container,
and stopping its services. It inspects the primary and services to confirm they
are no longer running; missing or still-running state returns `fence_leaked`
and retains the claim. Start, exec, one-shot admission, pause, resume, and
delete cannot overtake that drain. A competing lifecycle claim is refused as a
legacy `ProviderError` with code `busy`. Short concurrent execution admissions
can wait up to five seconds for one another. Describe and inventory remain
available while a claim is held, and so do the stop-request and PTY cleanup
execs that end already-admitted commands: they start no new work, so a held
claim must not keep a command from being stopped.

## Failure and reconciliation

A completed daemon refusal releases the claim so the application can decide
whether to retry. A transport failure, deadline, or dropped caller retains it:
the provider cannot establish that the remote mutation finished. There is no
lease expiry, automatic takeover, or mutation replay. The error uses code
`transport`, with `outcome: unknown` and the known claim container ID or name in
`detail` (or the sandbox ID if acquisition did not reveal the claim). A cancelled future cannot return an error, so the labels provide the
reconciliation record.

HTTP 502 and 504 responses also retain ownership. A gateway can return these
after forwarding a mutation, without establishing the daemon's outcome. This
applies to claim acquisition as well as operations inside an acquired claim;
neither is replayed automatically.

Internally, operation failures carry a typed claim disposition separately from
their diagnostic error. Combining primary/service or creation/cleanup outcomes
retains any requirement for reconciliation before choosing the public error.
The selected diagnostic cannot turn an unresolved operation into a releasable
claim. Both stop failures remain available, including over the plugin protocol,
using `related_error` in provider detail (nested under `cause` when wrapped as a
transport failure).

If claim release fails, `detail.operation_outcome` distinguishes completed work
from failed work. `operation_error` and `release_error` preserve both reports,
including provider codes and structured details. `sandbox` records the operation's
target; a completed creation also records its actual `sandbox_id`, so a release
failure does not discard the new resource's identity. Success is still withheld
until release is confirmed.

One-shot admission keeps its partial container and cleanup-attempt state until
the claim outcome is known. An attach/start failure combined with uncertain
cleanup retains ownership and both diagnostics; admission does not retry a
cleanup request whose outcome is unknown.

The claim must not be removed merely because its creator disconnected or a
period of time passed. An operator or application recovery procedure must first
establish that the original request can no longer change the sandbox and that
any required termination or cleanup is complete. Different clients, including
older provider versions and direct Docker commands, must not bypass this
coordination while managed work is active. The mechanism coordinates cooperating
provider clients; it is not an authorization boundary against Docker admins.

Each daemon call that acquires, discovers, or releases a claim has a 60-second
bound. Execution admission, stop, pause, resume, and delete have a 60-second
operation bound; create and start have a six-minute bound to allow the existing
service readiness budget. A failure to confirm claim release is an error even if
the workload mutation completed.

A failed create attempts dependent cleanup before removing the primary, with a
30-second cleanup budget. Unconfirmed cleanup uses `incomplete_cleanup` and
serializes the shared contract's `IncompleteCleanup` error in `detail`, including
resource identifiers whose removal remains unconfirmed, plus the original
creation failure as an error report under `cause` and the cleanup failure under
`cleanup_cause`. A transport failure during either creation or cleanup retains
the claim and includes it in the remaining resources, even if the other failure
was a completed daemon refusal. Creation failures whose cleanup succeeds
preserve their original error.

## Verification and migration scope

The real-daemon fence tests are explicit integration tests:

```sh
cargo test --locked -p sandbox-driver-docker --test fence -- --ignored
```

They exercise independent provider clients, stop versus exec/one-shots/services,
start during draining, cancelled ownership, and attachment during creation.
The lifecycle failure-injection tests cover release failure, uncertain admission,
retained ownership after primary removal, and incomplete creation cleanup.

Portable workload translation, namespace and descriptor reporting, next-API
provider integration, and the new cross-provider conformance harness are separate
migration work. Passing the legacy suite or these focused tests is not evidence
that Docker passes that full contract.
