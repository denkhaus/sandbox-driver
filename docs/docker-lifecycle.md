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
legacy `ProviderError` with code `busy`. Any operation waits up to five
seconds behind a held admission claim, since admissions release quickly; a held
lifecycle claim refuses it at once. Describe and inventory remain
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
any required termination or cleanup is complete. Then it removes the claim named
in the error's `fence_container` with `docker rm <fence_container>`. Claims are
unstarted containers, so `docker container prune` and `docker system prune`
remove held and retained claims without any check; do not prune a daemon that
has managed sandboxes in use. Different clients, including
older provider versions and direct Docker commands, must not bypass this
coordination while managed work is active. The mechanism coordinates cooperating
provider clients; it is not an authorization boundary against Docker admins.

Each daemon call that acquires, discovers, or releases a claim has a 60-second
bound. Execution admission, stop, pause, resume, and delete have a 60-second
operation bound; create and start have a six-minute bound to allow the service
readiness budget. Create pulls the primary and service images before it takes
the claim. Under the claim, the work after the primary exists is bounded to five
minutes, and a timeout runs the same failed-create cleanup as any other creation
failure. Services are started together and share one five-minute readiness
deadline, on create and on start. A failure to confirm claim release is an error
even if the workload mutation completed.

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

Run the Docker verification path against a real daemon:

```sh
mise run test:docker
```

This builds the provider executables, runs failure injection and concurrent
in-process/wire conformance, explicitly enables the live fence tests, and runs
the `lithos-sandbox` CLI workflow with live Docker required. The CLI launches the
actual `sandbox-driver-docker` executable; the process-boundary fence test also
starts two independent driver executables against the same sandbox. An
unreachable daemon fails this task instead of silently skipping live coverage.

To run only the real-daemon fence tests:

```sh
cargo test --locked -p sandbox-driver-docker --test fence -- --ignored
```

They exercise independent provider clients, stop versus exec/one-shots/services,
start during draining, cancelled ownership, and attachment during creation.
The separate-process case runs a command and a one-shot through one executable,
stops their sandbox through the other, observes `busy` during draining, and
checks that services stop and the old command stays dead after restart.
The lifecycle failure-injection tests cover release failure, uncertain admission,
retained ownership after primary removal, and incomplete creation cleanup.

For a direct manual CLI check after `mise run dev`:

```sh
export SANDBOX_DRIVER_PLUGIN_DEV=1
id=$(target/debug/lithos-sandbox --provider docker --output id sandbox create \
  --image ghcr.io/lithoscomputer/ubuntu-24.04:slim-df708f910111)
target/debug/lithos-sandbox --provider docker sandbox exec "$id" -- uname -a
target/debug/lithos-sandbox --provider docker sandbox stop "$id" --wait
target/debug/lithos-sandbox --provider docker sandbox start "$id" --wait
target/debug/lithos-sandbox --provider docker sandbox delete "$id"
```

## Shared contract runtime

`sandbox_driver_docker::DockerFactory` registers Docker as a built-in with a
`contract::Registry`. An entry's only `config` key is `host` (`unix://`,
`tcp://` or `http://`; default `unix:///var/run/docker.sock`); nothing is read
from the ambient environment. The runtime drives the same container plan,
claims and stop fence described above, so legacy and new-contract clients
coordinate on one daemon.

The descriptor reports the namespace `docker:<engine ID>` (stable across daemon
restarts, different per daemon), `durability: machine`, `access: shared`, image
sources, cpu and memory, the `allow_all` and `block` network modes, every
`container` field, services, one-shot containers, pause and resume, and the
shell-command endpoint. No timers are enforced, so any timer request is
`unsupported`, as are disk, GPUs, other sources and allow-list networks.

- **Services** become sidecars on the workload's network, reachable by their
  service name. A readiness check runs as an exec-form Docker health check in
  the service, with its own deadline (at most five minutes), on create and
  again on start. Docker runs the first check one interval after the service
  starts, so `interval_ms` must be shorter than `deadline_ms`. A service
  without a check is only confirmed started, and its image's own
  `HEALTHCHECK` is turned off. `status.services` lists only services running
  now. A service
  `credential` names a secret whose value is a JSON object with `username`,
  `password` and an optional `server`.
- **`provider_config`** takes `auto_pull`, `platform`, `binds` and
  `host_network`, described by the descriptor's `spec_schema`. `binds` is
  refused unless the daemon is local (`unix://`), matching the descriptor's
  `mounts_host_paths`.
- **Labels** under `sh.sandbox-driver.` are reserved. Docker merges an image's
  own labels into the container's, so the caller's keys are recorded in
  `sh.sandbox-driver.labels` and only those are reported by `status` and
  `list` or matched by a label filter. Sandboxes created before that label
  existed report every label the provider did not write.
- **Exec** runs argv without a shell; `tty` is unsupported. A command on a
  sandbox that is not running is refused, never reported as an outcome. A
  command that loses its stream reports `killed` only once its container run
  is confirmed ended: at once if the run changed, or after waiting up to ten
  seconds while a stop or delete holds the claim; otherwise the transport
  error is returned. `files.list` uses POSIX `sh` and `find`, so it needs no
  `bash`; a directory that exists but cannot be read fails the listing
  instead of reporting `not_found`.
- **Errors:** a daemon 404 maps to `not_found` only when the primary is gone;
  a missing image or service is `unhealthy`, so callers do not treat the
  sandbox as removed.
- **Reconciliation:** sidecars and their network carry
  `sh.sandbox-driver.sandbox=<full sandbox ID>`. Destroying a sandbox whose
  primary is already gone removes them by that label before reporting success.
  Neither inventory reports sidecars or one-shots as sandboxes.

`cargo test -p sandbox-driver-docker --test contract -- --ignored` runs the
shared conformance suite (with a second activated catalog for the
cross-instance fence), the services and one-shot workload, label round-trip
and exec on a stopped sandbox, refusal before admission, and reconciliation
of a vanished primary against a live daemon.
Ports are exposed as unsigned local forwards without a TTL; signed or expiring
exposure is `unsupported`. Plugin-protocol transport for this runtime is the
plugin lane's work.
