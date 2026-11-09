# Shared sandbox contract

This is the implementation handoff for `sandbox_driver::contract`. The API
itself, as a consumer or plugin author reads it, is [docs/next-api.md](next-api.md);
this document states the required behavior behind those shapes and the
migration rules. It defines common data and required behavior for the next API. It does **not** claim the
existing providers implement that behavior yet. The current crate-root traits,
errors, specs, provider libraries, executables, and [protocol v2](protocol.md)
remain supported during migration. The additive façade, registration boundary,
scripted fake and common conformance checks now consume these declarations.
No new transport or consumer cutover is introduced by core.

## Compatibility boundary

| Current surface | New declarations | Migration rule |
| --- | --- | --- |
| `sandbox_driver::SandboxSpec` | `contract::SandboxSpec` | Distinct types; no blanket conversion. Translate only after the provider implements workload targeting and rejects unsupported fields. |
| `sandbox_driver::Error` | `contract::Error` | Keep existing error matching and wire error mapping unchanged. New adapters classify using evidence, not message text. |
| `Capabilities` and background `Services` facet | `contract::Descriptor`, `CapabilitySet`, workload services | No inference from old flags to new guarantees. Workload services are not a new accessor. |
| `SandboxId`, legacy attach | `contract::SandboxRef` | No inferred namespace for old IDs. Consumers must explicitly migrate saved records or retain old readers. |
| v2 initialize and operation DTOs | Common serializable declarations | No v2 handshake additions or version bump here. The protocol work negotiates the next version and adds explicit adapters. |

The `contract` module is the shared import location for all new provider and
client work. Existing
public exports and required trait methods are unchanged. A new provider may
reuse legacy implementation internals, but must not claim the new semantics
merely by wrapping the old traits. Unsupported new operations must fail before
admission. In particular, serializing a new spec and deserializing it as a v2
spec is **not** an adapter: older readers ignore fields and can run the wrong
workload. There is no automatic fallback to v2 after a new-contract mutation.

Request DTOs reject unknown fields/options rather than dropping instructions.
That includes fields the selected mode does not take: an `allow` list on a
non-allow-list network mode, or a `sha256` pin together with `dev = true`, is a
decoding error. Descriptors may acquire additional object fields; unknown enum capabilities
fail decoding rather than being interpreted as supported. Unknown status states
become `Unknown`, never a terminal state. `unavailable` means unreachable and
not known to be gone; `lost` is established absence without a confirmed
destroy, never inferred from failed contact. The closed error enum serializes a
stable snake-case `code` plus structured fields. A future unknown error code is
a decode/transport failure with unknown outcome, never `NotFound` or success.
Rejected credentials (`Unauthenticated`), insufficient permission
(`PermissionDenied`), and quota or rate limits (`ResourceExhausted`, with an
optional `retry_after_ms`) are refusals with their own codes, distinct from
`Unavailable`, so applications can tell bad credentials from an outage.
Wire envelopes and negotiation remain part of the protocol work; this document
is not a replacement specification for v2.

## Identity, admission, and catalog

`SandboxRef { provider, namespace, id }` is the durable identity value. `provider`
is the operator's **provider-entry name**, not its kind and not an environment.
`namespace` is a non-secret backend-issued resource-domain identifier. The same
configured backend reports the same namespace across reconnects; a different
backend must report a different namespace even if sandbox IDs happen to match.
Identifiers are nonempty, at most 256 bytes, and contain no whitespace/control
characters. Namespace values are opaque: clients must not synthesize them from
URLs, credentials, the current environment, or provider-kind guesses.

Attach/destroy resolve the saved provider binding and compare the runtime
namespace with `SandboxRef::check_namespace` **before sending a sandbox
operation**. A mismatch is `NamespaceMismatch`. A missing binding is
`NoProvider`; failed contact is `Unavailable`. Neither is evidence of deletion.
The library must not scan for another kind/entry and silently redirect a ref.
An operator may explicitly migrate a provider-entry name after verifying the
same namespace and access rights. Renaming an entry otherwise needs an alias
or saved-record migration. Deleting or editing an environment needs neither:
refs and inventory do not depend on environments.

A ref is routing data, not authorization or proof of consumer ownership.
Existing ownership checks and application lease records remain necessary;
namespace agreement must not bypass them.

`ProviderEntry` describes a connection (`plugin`, `config`, secret names, and
for a third-party plugin its `path`, a `sha256` pin or `dev = true`, `args`,
`env` and `inherit_env`). The two trust keys are flat because operators write
them by hand; in Rust they decode to `PluginTrust`. `Environment` is a
provider entry plus a spec template. `EnvironmentSource` returns snapshots from
application-owned storage. A storage failure is `Unavailable`; only `Ok(None)`
means the environment does not exist. Admission saves the resolved spec and provider
binding; a later start or recovery must not reread an edited environment.
Applications persist their own admission/lease records. The driver persists
provider state needed to implement its declared durability.

These are raw configuration DTOs, not a file loader or an admission validator.
The core API implementation owns loading, merging, and schema validation.
Sizes and durations have one grammar, exported as `parse_size_mib` and
`parse_duration_ms`. A template or override may say `memory = "4GB"` or
`auto_stop = "60m"`; decoding normalizes it once to whole MiB and
milliseconds, and the value always serializes in that integer form
(`memory_mb`, `auto_stop_ms`), so providers never see the text.

`ProviderSchemas.config_schema` validates **only `ProviderEntry.config`**.
`spec_schema` validates **only `SandboxSpec.provider_config`**, in addition to
common spec validation and descriptor capability checks. No schema means the
provider validates opaque options itself; it never means ignoring them.
JSON schemas are objects with a declared dialect; adapters must reject
unsupported dialects and must not fetch external references implicitly.
Schema discovery and validation are not implemented by deserializing these DTOs.

Secret references never contain credential values. Entry secrets are resolved
at entry activation through the application's resolver. Service `credential`
names use that same explicitly supplied credential store, resolved by the driver
at workload admission. Runtime secrets must not appear in descriptors, saved
specs, or errors. Provider config, explicit environment, and arguments may
contain sensitive values; their `Debug` output is redacted.

`inherit_env = None` selects PATH/HOME/TMPDIR/USER. An explicit list replaces
that list, including `[]` for no inheritance. Explicit `env` wins. Secrets are
injected explicitly and must not be overridden by convenience inheritance;
conflicting explicit names are configuration errors. Built-ins receive resolved
inputs and do not read ambient settings. Workers receive resolved admission
configuration, with secret references resolved through the same store, rather
than reopening mutable settings files.

## Schema discovery and lazy startup

A cold catalog must not start every plugin or require every executable/backend
and credential to be available. It checks local structural configuration;
unresolved environment-to-entry references are recorded as per-entry diagnostics
and rejected when selected, not a reason to disable unrelated entries.
Missing or unusable entries are reported on use or explicit
per-entry inspection; unrelated entries remain usable. The catalog therefore
does not validate every plugin's schema at construction, and it is not
startup-free: it does the local structural checks above.

The plugin loader and SDK implement this sequence:

1. **Explicit metadata discovery** (also used on first activation): resolve the
   selected executable and verify its configured checksum or trust record.
   Missing trust fails closed unless `dev = true` is explicit.
   Run the executable with `--describe-schema` in a bounded, short-lived metadata
   process. It returns one JSON `ProviderSchemas` object on stdout and exits;
   diagnostics go to stderr. No initialize request, data socket, backend access,
   workload, connection config, runtime arguments, or secrets are needed.
   The probe receives only the minimal platform launch environment and must
   succeed without credentials. A metadata probe executes trusted plugin code;
   it is not a sandbox or a protection against a malicious plugin.
2. Cache metadata by verified executable digest, not filename or kind alone.
   A changed binary invalidates the cache and trust decision. Built-ins expose
   the same metadata directly. Offline forms may use a verified cached schema,
   but must distinguish it from live namespace/capability facts.
3. Validate connection config against `config_schema`, resolve required secret
   names, and freeze launch inputs. Launch one configured runtime per entry per
   client process **only when that entry is used**. Send config in initialize;
   pass secrets explicitly to its process environment. This is the runtime's
   first control request. Runtime metadata must match the discovered kind,
   version, and schemas; disagreement fails before any sandbox operation.
4. Initialize returns configured runtime facts in `Descriptor`, including the
   namespace, access scope, durability, locality and supported features. Validate
   the requested spec against those facts and `spec_schema` before mutation.
   A descriptor requires a valid namespace; the metadata-only response does not
   invent one. Explicit `describe` may activate the selected runtime.

Checksums, installation, probe time/size limits, schema engine, initialize DTOs,
and supervision are left to the core API and plugin loader implementations. The agreed
sequence does not require a new transport. Keep JSON control over stdio and a
separate authenticated per-operation socket connection for output, input, and
file contents. Preserve reserved control capacity for stop/cancel/health and
bounded data buffering. The v2 framing is retained unless separately negotiated.
A failed metadata probe or missing Host must never trigger an in-process Host
fallback.

## Workload and execution targets

`SandboxSpec.source` describes the caller's workload. New source JSON is
`{"image":"..."}`, `{"dockerfile":"..."}`, `{"snapshot":"..."}`, or
`"host_directory"` (directory and ownership are separate spec fields).
The source is required. It is not a provider's VM runner or nested-Docker helper.
`SandboxSpec::validate` checks portable invariants; capability/schema/backend
validation is additionally required before admission. Portable checks include
environment variables: keys must be non-empty and contain no `=` or NUL, and
values contain no NUL, because either would change the variable the workload
receives.

All sandbox exec, effective environment, working-directory and file operations
address **that workload and its workspace**. No caller-selectable outer-runner
execution target is exposed. A readiness command is argv executed inside its
named service, without an implicit shell. A one-shot container uses its own
requested image while sharing the workload workspace, network and service
reachability. Its lifecycle participates in the sandbox fence.

The Docker and Daytona drivers own helper images, outer runners, snapshot
construction/caching, networking, service wiring, nested execution, and setup
recipes. Petri/Fabro supply portable source/services/container data. Neither
client chooses VM versus nested-container targets or knows the runner recipe.

Services are a map keyed by lowercase DNS label (avoiding case-folded alias
collisions). Each has an image, environment, TCP ports,
optional registry credential name, and optional readiness command. Ports are
reachable **from the workload**, not public host port mappings. Readiness
requires finite nonzero per-attempt timeout, interval, attempt count, and total
deadline; zero exit passes. The total deadline bounds waits and attempts even
when the attempt budget would take longer. A per-attempt timeout longer than
the deadline, or an interval at least as long as the deadline when a second
attempt is allowed, is rejected because the check could never use its budget. Without a check the driver only
confirms service start; it makes no application-readiness claim.
`Status.services` reports current workload-reachable host/port mappings, which
can change on start. Unavailable status must not present stale addresses as
current observations. `Status` also carries the image and snapshot in use, the
resources actually allocated after any provider rounding, the effective
network policy, and a console URL when the provider has one; an absent field
was not reported, not zero.

Container options explicitly cover init, privilege, capability additions/drops,
DNS, extra hosts, and shared-memory MiB. `None` is unspecified, while
`Some(false)` is an explicit request. Descriptor `container_fields` lists exactly
which requests are supported, separately from `services` and one-shot
`containers`. Unsupported requests return `Unsupported` naming the field,
including unsupported explicit false values. Providers cannot silently drop
resource, network, timer, service, or container requests. The provider-default
network mode, omitted or explicit, is not a request and needs no descriptor
entry. Descriptor `timers` lists each enforced timer (`auto_stop`,
`auto_archive`, `auto_delete`, `ttl`) separately; a request for an unlisted timer
is `Unsupported`. Host rejects service and container workloads.

## Lifecycle, fences, and uncertainty

- `create` and `start` succeed only once the workload accepts execution and all
  declared service checks pass. Start uses the admitted spec and checks services
  again. A failed create attempts bounded cleanup of everything it provisioned.
- `stop` blocks new exec, service and one-shot admissions before draining. Success
  confirms that all previously admitted workload execution, services and
  one-shot containers have ended, preserving the workspace. The fence covers
  every handle and provider instance accessing that sandbox, not just one
  client's connection. Providers coordinate a common fence or enforce one
  mutating owner and return `Busy` for conflicts. Start returns `Busy` while the
  previous generation is draining. Stop then start cannot leave old execution.
- A stop acknowledgement is not a fence. Unconfirmed termination is
  `FenceLeaked`; failed contact is `Unavailable` or `Transport` with unknown
  outcome. No timeout or plugin exit may be promoted to successful stop.
- `destroy` confirms removal of all associated managed resources, including
  services, one-shots, networks and helpers. Already-absent is success only when
  associated cleanup is established. Failed create/destroy cleanup returns
  `IncompleteCleanup` with known remaining resource kinds/IDs. An empty list
  means IDs are unknown, not that nothing remains. Providers retain labels or
  discovery metadata for reconciliation, including partially created resources.
- `NotFound` requires authoritative absence in the checked namespace.
  `Unavailable`, status `Unknown`, and exec termination `Lost` do not mean gone.
  A failed readiness check may follow partial creation. No error code alone is a
  safe-retry instruction. `MutationOutcome::NotStarted` requires proof of no
  admission/effects; omitted outcome defaults to `Unknown`.
- A crashed/replaced plugin invalidates handles (`Stale`). Reconnection restores
  transport and attach restores access; neither restores a running application
  nor resolves the outcome of an earlier mutation. A supervisor may restore
  transport on demand, but never automatically replays uncertain mutations or
  restarts sandbox workloads. Future cancellation requests bounded cleanup;
  dropping a future is not confirmation that remote work stopped.

Applications own lease policy and decisions to inspect, wait, retry, stop,
restart, replace or fail work. Providers supply reliable primitives and honest
errors. Legacy error adapters must preserve these distinctions and must retain
underlying causes in provider diagnostics rather than converting every error
into absence or success.

## Host ownership and retained workspaces

Host's target distribution is **only a separately installed plugin on execution
machines**, reached through the ordinary configured plugin loader. Fabro must
not ultimately link or bundle its implementation or executable. Existing Host
library/executable paths remain for current consumers until migration and
packaging are proven. Docker and Daytona may remain built-in providers.

The initial Host contract is `durability: process`, `access: owner_process`, with
a machine-scoped namespace that is stable for that installation's machine.
Sandbox IDs must include or otherwise uniquely bind to their owning runtime
generation so restarting cannot reuse another live/old sandbox's identity.
One live plugin instance owns admissions, process groups and lifecycle state.
A second independently launched plugin is not an observer or a shared registry
and cannot acquire live access merely from a path or ref. If a conflicting live
owner is known, refuse with `Busy`; if ownership/liveness cannot be established,
report `Unavailable`. Never report `NotFound` merely because a local registry
is empty. An expired handle is `Stale`; reattach after owner death is not promised.

The plugin must attempt bounded owner cleanup on stdin EOF/shutdown, but process
loss alone is not proof children died. The Host implementation must demonstrate
confirmed fencing or honest failure. Separate server/worker processes cannot
independently attach to the same Host workload under this ownership model.
Applications needing live UI access must route operations to the actual owner;
a run record can display history but is not a live sandbox handle. Shared access
would require a separately designed coordinator and conformance evidence, not
just moving code into a plugin.

A designated workspace remains caller-owned and is never removed by destroy.
A managed workspace is provider-owned and removed on confirmed destroy.
Designated path defaults to designated ownership; no path defaults to a managed
temporary directory. Explicit managed ownership transfers deletion rights.
Retained files after a worker/plugin exit are not a running or attachable sandbox,
and filesystem existence proves neither execution termination nor ownership.
Applications may explicitly create a new sandbox over a retained designated
workspace, after resolving prior-owner safety, or choose a durable provider for
restartable execution. This is application recovery policy, never an automatic
Host restart or adoption mechanism. Missing/unconfigured Host affects only work
that selects that entry.

## Handoff and verification

- **Core API:** consume `contract` types; implement catalog/schema admission,
  environment overrides, immutable bindings, namespace-checked refs, façade,
  fake and validation. Do not synthesize refs for legacy records.
- **Plugin loader and Host:** implement metadata discovery and configured lazy
  launch, next-version negotiation/adapters, same control/data transport, stale
  handles, owner fencing and separate Host distribution. Keep v2 usable until
  migration.
- **Docker:** translate portable workload/services/container options and
  report real namespace/capabilities; prove readiness and cross-instance fence.
- **Daytona:** own runner/snapshot/nested-container recipes and route every
  operation to the workload; prove service addressing, readiness and cleanup.
  Translate timers carefully: here zero disables a timer, while Daytona's
  `auto_delete_interval` of zero means delete on stop.

The focused tests in `crates/sandbox-driver/tests/shared_contract.rs` cover
persisted identity independent of environments, namespace refusal, configuration
and workload decoding, readiness bounds, error uncertainty, schema separation,
status tolerance and redaction. The existing core/provider/protocol suites must
continue to pass. These declaration tests are not provider conformance evidence:
integration tests must exercise actual Host plugin, Docker and Daytona
behavior before any consumer cutover or legacy removal.

### Provider interface implementation

The additive provider boundary is exported from `sandbox_driver::contract`:

- `ProviderFactory::discover(&PluginLocation)` receives only kind, path and
  trust. The plugin adapter owns verification, bounded credential-free probing,
  and caching by verified digest. Built-in factories return metadata directly.
- `ProviderFactory::activate(Activation)` receives the selected entry name,
  frozen connection/launch configuration, discovered schemas, resolved launch
  environment and separate redacted `SecretValues`. It returns an
  `Arc<dyn ProviderRuntime>`. Built-ins do not read ambient configuration.
- `ProviderRuntime` supplies `describe`, `health`, `create(WorkloadAdmission)`,
  `attach(&SandboxId)`, `list(&LabelFilter)`, `destroy(&SandboxId)` and `shutdown`.
  The catalog checks namespace before routing attach/destroy. A runtime must
  also prevent reconnect races from moving an admitted call to another
  namespace, and invalidate old sandbox handles on generation changes.
- `Sandbox` supplies reference/workload environment/workspace, status, start,
  stop, exec, files and ports. Optional lifecycle, endpoints and containers
  return `None` unless supported. All errors use `contract::Error`.
- `ExecSpec`/`ExecControls`/`ExecOutcome` are distinct from the legacy exec API.
  Controls carry one owned stdin reader, an awaited output sink, term/kill
  tokens and a latest-value terminal resize receiver. `Files` transfers owned
  byte streams, allowing adapters to keep bulk bytes off control connections.
- `OneShotSpec` takes its own `OneShotImage`, never the workload `Source`: a
  registry `image`, or a `build` from a context directory in the workload
  workspace with an optional Dockerfile path, a tag that keys the build cache
  and a `reuse` flag. Its command is an optional `entrypoint` plus `args`, not
  exec argv, so arguments alone reach the image's own entrypoint.
- `SandboxSpec::validate_for` and `Environment::validate` check portable
  invariants and descriptor capabilities. They do not replace JSON Schema or
  backend-specific validation. `WorkloadAdmission` separates credential values
  from the persistable spec; providers must retain the admission for restart.

These are implementation interfaces, not proof of provider conformance. The
façade, schema engine, scripted fake and reusable conformance suite are
implemented. Real provider/transport conformance remains an integration gate.
Existing crate-root traits and protocol v2 are unchanged.

### Scripted fake provider

`sandbox_driver_testing::contract::FakeProvider` implements the provider
factory/runtime boundary in memory. Consumers use `sandbox-driver-testing` as
a dev-dependency; the core library exports no fake. `script` supplies command
responses; unscripted commands fail without executing on the host. Clones share
workload state and call history. The fake supports required lifecycle, identity,
files and streaming execution, and advertises no optional workload capabilities.
Its focused tests exercise cancellation, shutdown uncertainty and workload-relative
file paths directly through the provider interfaces. The testing crate provides
`FakeProvider::sandboxes` and `contract::fake` helpers; the reusable conformance
suite exercises the fake through those helpers.
