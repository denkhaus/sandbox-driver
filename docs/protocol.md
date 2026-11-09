# sandbox-driver plugin protocol, version 2

The additive [shared contract](shared-contract.md) defines next-API data and
schema bootstrap requirements. It does not change this version's handshake,
error encoding, operation DTOs, or transport. New adapters require explicit
version negotiation; new workload specs must not be sent as v2 specs.

This is the normative specification of the wire protocol between a
**host** (an application using the protocol client, primarily Petri) and a **plugin** (an executable serving one sandbox provider). It
is written so a plugin can be implemented in any language without reading
the Rust source. The Rust implementation lives in the
`sandbox-driver-protocol` crate: `serve_stdio()` is the plugin side,
`PluginProvider` the host side, and the compatibility tests in
`crates/sandbox-driver-protocol/tests/` verify the encodings and
tolerance rules shown here — behaviorally, not as full-shape pins.

Version 2 replaces version 1. It moves every byte stream off the control
connection onto a per-operation **data channel** (§2.1), removes the
base64 payload methods and notifications that carried them, adds
`sandbox/environment` and `one_shot/run`, and lifts the version-1 masks
on streamed stdin and the effective environment. A version-1 peer fails
the handshake by version number (§4).

Normative words: **must**, **must not**, **may**.

## 1. Model

This protocol is one of two supported ways to reach a provider. The
bundled Host, Docker, and Daytona providers are also libraries an
application can link in-process; each provider package builds its plugin
executable from that same library. Third-party providers ship as plugins
only. Both paths present the same trait family, so a host written against
the traits works with either; §5 lists the few capabilities that cannot
cross the wire.

A plugin serves exactly one provider **kind** (e.g. `docker`) and
multiplexes every sandbox of that kind over one connection. The host
speaks first and drives the conversation; the plugin answers requests
and emits notifications. One plugin process per provider kind, not per
sandbox.

The protocol is a serialization of the `sandbox-driver` trait family:
anything expressible over the wire is expressible in-process and vice
versa, minus the capability mask in §5.

## 2. Transport and framing

- The **control transport** is the plugin's **stdin/stdout**. Stdout
  belongs exclusively to the protocol; a plugin must log to stderr only.
  The host may leave the plugin's stderr inherited or capture it.
- Control messages are **newline-delimited JSON** (NDJSON): one complete
  JSON object per line, UTF-8, terminated by `\n`. A message must not
  contain a raw newline. Blank lines are ignored.
- Each side must impose finite admission limits on outstanding requests.
  **Responses may arrive in any order**; the `id` correlates them. A
  plugin must not serialize request handling: a slow call must not
  block an unrelated fast call (see §9 and the conformance suite's
  interleaving checks).
- **No bytes cross the control transport.** Command output and input,
  stdio and PTY traffic, logs, and file contents ride data channels. The
  only exception is the bounded output sample inside an `exec` error
  report (§7), which is base64.

### 2.1 Data channels

The host owns a **Unix domain socket** in a private directory (mode
`0700`) and announces its path at `initialize` (§4). Every request that
moves bytes carries a `channel` object:

```json
{"channel_id": 7, "token": "3f9a…"}
```

`channel_id` is host-generated and unique for the connection; `token` is
a one-use secret of at least 128 bits. The plugin handles such a request
by **connecting to the socket** and sending one `open` frame carrying the
JSON `{"channel_id": 7, "token": "3f9a…"}`. The host binds the
connection to the waiting operation when both values match, and closes
any connection whose open frame does not match, arrives late, or is not
the first frame. A plugin must open the channel **before** it starts the
operation's work, and must close its side (an `eof` frame) **before** it
sends the operation's response, so a result never arrives ahead of the
bytes it describes.

Frames are binary: one byte of kind, four bytes of big-endian payload
length, then the payload.

| kind | byte | direction | carries |
| --- | --- | --- | --- |
| `open` | 0 | plugin → host | the JSON open payload, once, first |
| `stdout` | 1 | plugin → host | command stdout, PTY output, log and file bytes |
| `stderr` | 2 | plugin → host | command stderr |
| `stdin` | 3 | host → plugin | command or PTY input, file content to write |
| `eof` | 4 | either | the sender has no more data |

`initialize` negotiates `max_frame_bytes`; this implementation offers and
accepts 65536. A receiver must reject a frame whose length exceeds the
limit before it allocates the payload, and a sender must split larger
chunks. An `eof` frame has an empty payload. Each side sends at most one
`eof` and nothing after it. A connection closed before its `eof` is an
incomplete transfer, even if the control response reports success. An
unexpected frame kind must fail that operation; it must not become data.

Backpressure is the socket's: a plugin that cannot write because the
host is slow to read must stall the producing process, never drop or
buffer output without bound, and the stall affects that one operation.
Control responses, `exec/stop`, and every other channel keep flowing.

An additive `data_transport.open_ack` boolean requests authentication
acknowledgment. Its default is false for older peers. When true, the plugin
adds `"acknowledge":true` to the open payload and waits for an empty `open`
frame from the host before sending data. The host sends this acknowledgment
only after peer identity, channel ID, and the one-use token pass validation.
An open without `acknowledge` receives no acknowledgment. This prevents a
tiny operation from closing before the host checks its peer credentials.

### 2.2 Finite transport budgets

Admission must precede provider work. Pending opens consume active I/O
capacity. Overload is an application error with `report.kind:"overloaded"`
and `detail:{"limit":"active_io","not_started":true}` (using the actual
exhausted limit name). Only a rejection before work starts may make this
claim. A transport failure or buffer limit error may follow provider effects;
neither authorizes automatic replay.

Both peers enforce local limits. The Rust implementation exposes
`TransportLimits`, `connect_with_limits`, `spawn_with_limits`, and
`serve_with_limits`. Each limit below applies to one connection on each side,
except where the accounting boundary says otherwise. These are finite defaults,
not measured capacity claims. See [transport validation](transport-validation.md)
for measurements and the tested configuration.

| Limit | Default | Accounting boundary |
| --- | ---: | --- |
| `active_io` | 1,024 | Pending and open data operations; permits remain with both channel halves |
| `pending_opens` | 1,024 | Host channel expectations and plugin connection attempts before authentication |
| `unauthenticated_handshakes` | 64 | Accepted host sockets before authentication |
| `provider_requests` | 2,048 | Ordinary requests before dispatch through reply delivery |
| `reserved_requests` | 64 | Stop, terminate, close, cancel, preview release, shutdown, delete, health, and diagnostics calls |
| `control_message_bytes` | 1 MiB | Serialized JSON line including newline, before allocation beyond the cap |
| `queued_control_bytes` | 8 MiB | Ordinary serialized control delivery, including the writer's current message |
| `reserved_control_bytes` | 1 MiB | Reserved control delivery, including the current message |
| `event_queue_bytes` | 1 MiB | Separate event delivery queue on each side, including the active callback |
| `event_queue_messages` | 256 | Ordered event delivery queue |
| `retained_output_bytes` | 16 MiB | Maximum requested capture per stdout/stderr stream |
| `buffered_value_bytes` | 16 MiB | One buffered file, append payload, or fixed-input fallback; shipped provider fallbacks also impose their own 16 MiB cap |
| `cached_handles` | 4,096 | Each local handle or event-route cache |
| `output_progress_timeout` | 30 s | Pending output with no delivery progress; quiet commands do not start this timer |
| `hard_cancel_drain_timeout` | 5 s | Local output waiting after the kill signal, independent of acknowledgment |
| `open_timeout` | 10 s | Waiting for a channel or an unauthenticated open |
| `shutdown_timeout` | 5 s | Server cleanup and writer shutdown phases |

Reserved control messages precede ordinary messages. Events have their own
queue and cannot consume response capacity. Admission does not queue provider
work. The reader may wait within a deadline to deliver one overload response.
The finite delivery queues are not a scheduler for new operations.

Dropped log, PTY, and stdio handles retain their I/O permit while owned
cleanup runs. A failed or timed-out cleanup retains that permit until the
connection is dropped. `PluginProvider::cleanup_error()` exposes this state;
unrelated channels stay open. Cleanup tasks and unresolved cleanup together
cannot exceed `active_io`. A stop acknowledgment establishes receipt of the
stop request. It does not establish remote termination or complete cleanup.

Retained output is disabled by default. Explicit capture is finite, with a
stable head and rolling tail. Deliberately omitted capture bytes increment
`omitted_bytes`; failed delivery sets `truncated` or returns a transport error.
Whole-value convenience APIs must fail when they cannot return the complete
value. A failed file write can leave partial destination changes.

Application RSS includes decoded messages, frame buffers, pipes, capture,
requests, caches, and owned tasks. Socket buffers consume additional kernel
memory. With explicit capture, the worst-case capture budget is twice
`active_io * retained_output_bytes`; applications must choose an affordable
configuration. Streaming without capture does not retain total stream output.

### Transport diagnostics extension

The optional `transport/diagnostics` request takes `{}` and returns the plugin's
current ownership counters. Older peers may return method-not-found. The Rust
client's `transport_diagnostics()` combines that response with its local counters.
Diagnostics contain counts only, with no operation tokens or credentials. They
use reserved request capacity.

Server counters cover active I/O permits, pending channel opens, exec, stdio,
PTY and log-stream registries, cached handles, and owned background tasks. Client
counters cover active I/O permits, pending opens, unauthenticated handshakes,
pending requests, cleanup tasks, unresolved cleanup, event routes and contexts.
`runtime_tasks` counts all live tasks in each Tokio runtime, including application
tasks and the diagnostic request itself. These snapshots are not atomic across
processes and do not establish remote termination. Stop-delivery counters report
logical requests, successful acknowledgments, and the longest acknowledged
delivery in microseconds, including retries for reserved capacity. A stop can
remain unacknowledged when its owning operation finishes first; provider
termination and output completion remain separate facts.

The listener also counts authenticated opens and transient accept backoffs.
`take_channel_setup_samples_us()` drains at most 4,096 recent setup measurements.
Each measurement starts before local admission and ends when the authenticated
channel is made available. It excludes caller scheduling and first-byte delivery.
The bounded buffer discards its oldest sample when full and increments
`setup_samples_lost`. These measurements do not retain output or channel tokens.

## 3. Message envelope

A restricted JSON-RPC 2.0: `jsonrpc` is always `"2.0"`, ids are unsigned
integers, and batches are not used.

Request (has `id` and `method`):

```json
{"jsonrpc":"2.0","id":7,"method":"sandbox/describe","params":{"sandbox_id":"sb-1"}}
```

Success response (has `id` and `result`):

```json
{"jsonrpc":"2.0","id":7,"result":{"status":{"...":"..."}}}
```

Error response (has `id` and `error`, see §7):

```json
{"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"...","data":{"...":"..."}}}
```

Notification (has `method`, no `id`; never answered):

```json
{"jsonrpc":"2.0","method":"exec/output","params":{"...":"..."}}
```

The host sends only requests; the plugin sends responses and the
notifications `host/event` and `host/log`. Unknown notifications must be
ignored. A request with an unknown method must be answered with error
code `-32601`.

## 4. Handshake

The host's first request must be `initialize`, naming the data transport
(§2.1):

```json
{"jsonrpc":"2.0","id":1,"method":"initialize",
 "params":{"protocol_version":2,
           "data_transport":{"socket_path":"/tmp/sandbox-driver-…/data.sock",
                             "max_frame_bytes":65536}}}
```

The plugin answers with its protocol version, identity, and capability
set:

```json
{
  "jsonrpc":"2.0","id":1,
  "result":{
    "protocol_version":2,
    "provider":{"kind":"host","version":"0.1.0"},
    "capabilities":{"...":"see §5"}
  }
}
```

Versioning is a single integer. If the versions differ, each side must
fail with a human-readable message naming both versions — never a decode
error. `data_transport` is required: a plugin that receives none must
refuse the handshake, and every channel the host names later opens
against the announced socket. `provider.kind` is the plugin's declared kind: lowercase ASCII
letters, digits, and interior hyphens, at most 64 bytes. A host that
launched the plugin from configuration names it by the configured kind;
the declared kind is informational and need not match (§11).

## 5. Capabilities

The capability set is negotiated once at `initialize` and is immutable
for the connection. Per-sandbox capability sets travel in every
`HandleInfo` (§8.1) and are authoritative for that sandbox. The shape
(all fields present; booleans default false; `pty`, `logs`, `snapshots`,
`volumes` are nullable objects):

```json
{
  "isolation": "none | container | vm",
  "lifecycle": {"pause":false,"archive":false,"fork":false,
                 "resize":false,"recover":false,"undelete":false,
                 "refresh_activity":false,
                 "timers":false,"labels":false,"update_network":false,
                 "snapshot_sandbox":false},
  "exec": {"live_streaming":false,"streams_separated":false,"stdin":false,
            "stop":false,"stdio_process":false,
            "stdin_stream":false,"environment":false},
  "fs": {"native":false,"upload":false,"download":false,"permissions":false},
  "search": {"supported":false,"native":false},
  "git": {"supported":false,"native":false},
  "services": {"supported":false,"native":false},
  "pty": null,
  "logs": null,
  "one_shot": null,
  "access": {"preview_urls":false,"signed_preview_urls":false,"ssh":false,
              "ssh_ttl":false,"ssh_revoke":false,
              "shell_command":false,"web_terminal":false,"vnc":false,"vpn":false},
  "network": {"allow_all":false,"block_all":false,"cidr_allow_list":false,
               "domain_allow_list":false,"outbound_proxy":false},
  "snapshots": {"from_image":false,"from_dockerfile":false,
                 "from_image_kinds":{"container":false,"virtual_machine":false},
                 "from_dockerfile_kinds":{"container":false,"virtual_machine":false},
                 "filesystem_from_sandbox":false,
                 "live_process_state_from_sandbox":false,
                 "build_logs":false,"activation":false},
  "volumes": null
}
```

Rules:

- **Capability honesty, both directions.** A declared capability must
  work; an undeclared operation must fail with the `unsupported` error
  kind (§7). Capabilities optimize failure timing; the error is still
  the enforcement.
- **The wire mask.** Native search/service passthrough and local shell
  commands do not cross the wire. A host forces `search.native`,
  `services.native`, and `access.shell_command` to false.
  `search.supported`, `git.supported`, and `services.supported` remain true when
  the complete facets can run through `exec/stream`; the host then selects the
  exec-derived implementations for search and services. `git.native`
  crosses as declared and selects the clone path: when true the host
  sends `git/clone` (§8.6) so the plugin runs its native clone; when
  false the host's exec-derived clone is the plugin's own implementation
  and runs host-side. Either way a plugin and an in-process provider
  select the same implementation, and the host derives every other git
  operation. Streamed stdin
  (`exec.stdin_stream`) and the effective environment
  (`exec.environment`) cross as declared.
- `one_shot` is a nullable object `{"build": false}`: non-null authorizes
  `one_shot/run`, and `build` authorizes the `build` image source.
- Older peers send `search`, `git`, and `services` groups as `{"native":false}`.
  Hosts read those groups as `supported:true` because `native:false` originally
  instructed the caller to use an exec-derived implementation. An explicitly
  sent `supported:false` means the complete facet is unavailable. An absent
  group also remains unsupported.
- The serialized `access.vpn` field is a compatibility tombstone. It is
  always false. VPN clients such as Tailscale are guest software managed
  through exec and are not a sandbox-driver capability.
- Version-1 peers can still send the legacy `lifecycle.checkpoint`,
  `snapshots.from_sandbox`, and `snapshots.include_memory` fields. Readers
  ignore them. New peers emit the normalized fields shown above.
- The aggregate snapshot source flags remain for version-1 compatibility.
  The `from_*_kinds` objects give exact container and virtual-machine
  support. If an exact flag is true, its aggregate flag must also be true.
- `capabilities.snapshots`/`volumes` being non-null is what authorizes
  the `snapshot/*` and `volume/*` methods (`snapshot/activate` and
  `snapshot/deactivate` additionally require `snapshots.activation`);
  `access.preview_urls` and `access.ssh` authorize the `access/*`
  methods.

## 6. Data conventions

- **Field names** are `snake_case`. Readers must ignore unknown object
  fields; new optional fields are the compatible evolution path.
- **Binary data** (file contents, command output, stdin) crosses on
  data channels (§2.1), never in JSON. The one base64 field that remains
  is the output sample inside an `exec` error report, suffixed `_b64`.
- **Durations** appear in two forms, fixed per field: integer
  milliseconds in fields suffixed `_ms`, and the structural form
  `{"secs":90,"nanos":0}` where a spec object embeds one (e.g.
  `timers.auto_stop_after_idle`, `exec` spec timeouts inside
  `sandbox/create` are not applicable — exec uses `timeout_ms`). Both
  forms are covered by encoding tests; neither may change within
  version 1.
- **Timestamps** are RFC 3339 strings in UTC (`"2026-08-31T20:00:00.250Z"`)
  where present (`created_at`, `updated_at`, `modified_at`, `expires_at`,
  `occurred_at`); they are informational. Readers also accept the
  structural form `{"secs_since_epoch":…,"nanos_since_epoch":…}` that
  peers wrote before this encoding.
- **Identifiers** (`sandbox_id`, `snapshot_id`, `volume_id`) are non-empty
  strings up to 256 bytes with no
  whitespace or control characters. They are opaque to the host and
  minted by the plugin; the host persists them to re-attach later.
- **State enums** are `snake_case` strings. A reader encountering an
  unknown state value must map it to `unknown`, not fail. Sandbox
  states: `creating starting running stopping stopped pausing paused
  resuming archiving archived restoring resizing forking snapshotting
  deleting deleted error unknown`. Snapshot states: `building active
  inactive error deleting unknown`. Volume states: `creating ready
  deleting deleted error unknown`.
- **Events and error kinds** likewise tolerate unknown values by
  ignoring the event or treating the kind as opaque.

## 7. Errors

Error codes: `-32600` malformed request or params, `-32601` unknown
method, `-32000` application failure. `-32600` describes what the host
sent; a plugin that cannot encode its own result answers `-32000` with
no `data`, since that is the plugin's failure. Application failures
carry `data`:

```json
{
  "code": -32000,
  "message": "capability lifecycle.pause is not supported by this provider",
  "data": {
    "report": {"kind":"unsupported","message":"…","retryable":false,"causes":[]},
    "detail": {"capability":"lifecycle.pause"}
  }
}
```

`report.kind` is the stable machine-readable classification:
`not_found`, `not_owned`, `read_only`, `unsupported`, `invalid_state`, `invalid_spec`, `timeout`,
`auth`, `rate_limited`, `overloaded`, `limit_exceeded`, `exec`, `git`, `provider`, `transport`, `incomplete`, `io`.
`report.causes` is a bounded rendered source chain. A receiver restores
these rendered causes as an opaque remote source chain. `detail` carries
kind-specific fields for faithful reconstruction:

| kind | detail fields |
| --- | --- |
| `unsupported` | `capability` (dotted path, e.g. `"exec.stdio_process"`) |
| `not_found` | `resource` (`sandbox`/`snapshot`/`volume`/`plugin`), `id` |
| `not_owned` | `resource`, `id` — the resource exists but lacks the labels a host-side ownership scope requires |
| `read_only` | `resource`, `id`, `action` — the handle observes a resource another process owns and refused the lifecycle `action` (an `Action` name such as `stop` or `delete`) |
| `invalid_spec` | `field`, `reason` |
| `invalid_state` | `current`, `action` |
| `timeout` | `operation`, `elapsed` |
| `auth` | `auth` object: `{provider, reason}` |
| `rate_limited` | `retry_after` (optional) |
| `overloaded` | `limit`, `not_started:true` |
| `limit_exceeded` | `limit`, `max_bytes` |
| `incomplete` | `operation`, `output_abandoned`, `stop_acknowledged`, `termination_confirmed`, `cleanup_confirmed` |
| `provider` | `provider` object: `{provider, code, message, retryable, detail}` |
| `exec` | `exec` object: `{label, termination, exit_code, stdout_b64, stderr_b64, duration_ms?}` (`duration_ms` is additive: senders may omit it, receivers must tolerate its absence) |
| `git` | `git` object: `{operation, kind, exec?, provider?}` — `kind` is one of `auth_rejected`, `remote_unavailable`, `ref_not_found`, `access_denied`, `target_exists`, `git_unavailable`, `unclassified`; a receiver reads a kind it does not know as `unclassified`. `exec` (same shape as the `exec` detail) is present when git ran inside the sandbox, `provider` (same shape as the `provider` detail) when a native operation ran it. `report.retryable` is true only for `remote_unavailable`. |
| `transport` | `transport_context` |
| `io` | `io_context` |

Raw command output appears only inside the `exec` detail and the `exec`
member of the `git` detail — never in `message` or `report`. Secret redaction is the host's responsibility.

## 8. Method catalog

All methods are host → plugin requests. `{}` denotes an empty result
object may be `null` — hosts must accept either for empty results.

### 8.1 Handles

Creation and attachment return a **HandleInfo**, everything a host needs
to operate a sandbox without further negotiation:

```json
{
  "status": {"id":"sb-1","name":"demo","state":"running","provider_state":"started",
              "error_reason":null,"resources":{"...":"…"},
              "sandbox_kind":"container","region":"eu","labels":{},
              "image":null,"snapshot":null,"network":null,"workspace_ownership":null,
              "created_at":null,"updated_at":null},
  "capabilities": {"...":"per-sandbox set, §5"},
  "working_directory": "/workspace",
  "runtime_directory": null
}
```

| method | params | result |
| --- | --- | --- |
| `sandbox/create` | `{spec,events?}` — see §8.2 | HandleInfo |
| `sandbox/attach` | `{sandbox_id,events?}` | HandleInfo |
| `sandbox/list` | `{filter:{labels:{…}}}` | `{sandboxes:[status…]}` |
| `sandbox/describe` | `{sandbox_id}` | `{status}` |
| `sandbox/platform_info` | `{sandbox_id}` | `{platform:{os,arch,version}}` |
| `sandbox/environment` | `{sandbox_id}` | `{environment:{…}}` (gated on `exec.environment`) |

### 8.2 The creation spec

`spec` is an unvalidated DTO; the plugin validates and answers
`invalid_spec` for anything it cannot honor:

```json
{
  "name": null,
  "source": {"image":{"reference":"ubuntu:24.04"}},
  "resources": {"cpu_cores":null,"memory_mb":null,"disk_mb":null,"gpus":null},
  "sandbox_kind": "container",
  "env": {}, "labels": {}, "user": null,
  "working_directory": null,
  "network": "provider_default",
  "volumes": [], 
  "timers": {"auto_stop_after_idle":null,"auto_pause_after_idle":null,
              "auto_archive_after_stop":null,"auto_delete_after_stop":null,"ttl":null},
  "ephemeral": false, "public": null, "region": null,
  "provider_config": null
}
```

`source` variants: `{"image":{"reference":…}}`,
`{"dockerfile":{"content":…}}`, `{"snapshot":{"name":…}}`,
`"host_directory"`. `network` variants: `"provider_default"`,
`"allow_all"`, `"block"`, `{"cidr_allow_list":{"cidrs":[…]}}`,
`{"domain_allow_list":{"domains":[…]}}`. `provider_config` is an opaque
JSON value the plugin defines and documents.

`sandbox_kind` is nullable and accepts `"container"` or
`"virtual_machine"`. When set, it is a required result. The plugin must
honor it or return `invalid_spec`; it must not silently substitute a
different kind. This field is separate from `capabilities.isolation`.
The latter describes the provider's security boundary. The public Rust
API uses a typed `SnapshotId` for a snapshot source, but protocol version
1 retains the original `source.snapshot.name` field shown above.

Timer semantics: a `null` timer defers to the provider's default. A
**zero duration** (`{"secs":0,"nanos":0}`) is the explicit "never" — it
disables the timer where the provider supports disabling; the plugin
translates per timer (Daytona: wire `0` for auto-stop, `-1` for
auto-delete). The same rule applies to `sandbox/set_timers`.

`sandbox/create`, `sandbox/attach`, and `sandbox/undelete` accept an
optional `events` object:

```json
{"route_id":"event-7","correlation_id":"fabro-run-42"}
```

`route_id` is host-generated, unique per connection, and used only to
route `host/event` notifications to the correct observer. This works
before a provider assigns a resource ID. `correlation_id` is an optional
opaque application value copied into each emitted `Event`. Plugins must
not interpret either value. They must not contain secrets.

### 8.3 Lifecycle

All take `{sandbox_id}` and return `{}` unless noted. Optional verbs are
capability-gated (§5); `sandbox/delete` must be idempotent (deleting an
unknown or already-deleting sandbox succeeds). `sandbox/delete` is a
provider-level operation by id: it needs no prior `sandbox/attach`, so a
sandbox that no handle can be built for is still removed, and it accepts
the optional `events` object of §8.2.

`sandbox/start`, `sandbox/stop`, `sandbox/delete`, `sandbox/pause`,
`sandbox/resume`, `sandbox/archive`, `sandbox/recover`,
`sandbox/refresh_activity`.

`sandbox/recover` is provider-assisted recovery of a live sandbox from
the `error` state. `sandbox/undelete` (gated on `lifecycle.undelete`)
restores a **deleted** sandbox within the provider's recovery window and
returns a fresh HandleInfo — it addresses the provider, not a live
handle, because a deleted sandbox cannot be attached.

| method | params | result |
| --- | --- | --- |
| `sandbox/fork` | `{sandbox_id, options:{name,include_memory}}` | HandleInfo |
| `sandbox/undelete` | `{sandbox_id,events?}` | HandleInfo |
| `sandbox/resize` | `{sandbox_id, resources}` | `{}` |
| `sandbox/snapshot` | `{sandbox_id, options:{name,include_memory}}` | `{snapshot_id}` |
| `sandbox/set_timers` | `{sandbox_id, timers}` | `{}` |
| `sandbox/set_labels` | `{sandbox_id, labels:{…}}` | `{}` (full replace) |
| `sandbox/update_network` | `{sandbox_id, network}` | `{}` |

The core API has no optional-memory fork. A v1 adapter sends
`include_memory: true`; a v1 request with `false` fails as `invalid_spec`.
The source and returned sandbox must both be Running, and filesystem,
memory, running processes, and process IDs must be preserved.

For snapshots, `include_memory: false` maps to `Filesystem` and `true`
maps to `LiveProcessState`. Daytona requires a Stopped source for the
filesystem mode and a Running source for the live-process mode.

`sandbox/checkpoint` and `sandbox/restore_checkpoint` are reserved v1
tombstones. A server returns JSON-RPC `method not found`; the public API
does not expose these Boxd-only operations.

### 8.4 Exec

The exec contract: `program` and `args` are an **argument vector**, run
directly with `execvp` semantics. `program` is resolved through the
command's `PATH` when it contains no slash; every element of `args`
reaches the process unchanged. No shell is involved: nothing is split,
globbed, or expanded. A plugin whose backend takes a shell string must
quote the vector so it stays literal. Buffered and streaming execution
must not differ in how the vector is run.

A host that wants shell semantics sends them explicitly, as
`{"program":"bash","args":["-c","<script>"],"env":{"BASH_ENV":""}}` — the
`ExecSpec::bash` helper on the Rust side. The exec-derived facets and the
bash probe (§12) are sent that way, so a plugin's sandbox must have `bash`
on `PATH` to serve them.

The exec spec DTO:

```json
{"program":"echo","args":["hi"],"timeout_ms":30000,"stop_grace_ms":5000,"working_dir":null,"env":{},"output_sanitization":"strip_ansi"}
```

`args` may be omitted and means `[]`. `stop_grace_ms` is optional and
additive: absent, stops are raw (the timeout kills, a `term` is one
signal); present, the plugin runs the TERM, grace, KILL ladder itself for
the timeout, a failing sink, and the host's `term`, while `kill` stays
immediate. A command the ladder ended for the timeout reports
`timed_out` whichever signal finally stopped it. A plugin built before
the field ignores it and keeps raw stops. Standard input is not in the spec:
a request whose `stdin` is true feeds the command from the channel's
`stdin` frames (§9), and a plugin whose provider takes only fixed stdin
collects those frames to their `eof` first.

`output_sanitization` is optional. Its values are `raw`, `strip_ansi`,
and `strip_all`; omission means `raw`. A plugin applies this policy to
buffered output and streaming notifications before capture accounting.
PTY and bidirectional stdio traffic always remains raw.

| method | params | result |
| --- | --- | --- |
| `exec/stream` | `{sandbox_id, exec_id, channel, spec, stdin, retained_output_limit}` | ExecStreamResult (§9) |
| `exec/stop` | `{exec_id, level}` | `{}` |
| `one_shot/run` | `{sandbox_id, exec_id, channel, spec, retained_output_limit}` | ExecStreamResult (§9.1) |
| `exec/stdio_open` | `{sandbox_id, process_id, channel, spec}` | `{}` |
| `exec/stdio_terminate` | `{process_id}` | `{}` |
| `exec/stdio_wait` | `{process_id}` | `{termination,exit_code,stderr_tail}` |
| `pty/open` | `{sandbox_id, pty_id, channel, options}` | `{}` |
| `pty/resize` | `{pty_id, size}` | `{}` |
| `pty/close` | `{pty_id}` | `{}` |
| `logs/follow` | `{sandbox_id, stream_id, channel, source}` | `{}` after the stream ends |
| `stream/cancel` | `{stream_id}` | `{}` |

There is no buffered exec method: a host that wants a buffered result
runs `exec/stream` and keeps the bytes itself.

ExecResult, the metadata half of a finished command:

```json
{"exit_code":0,"signal":null,"termination":"exited","duration_ms":12}
```

`signal` is the signal number that ended the process when the plugin
observed one — on any termination, a foreign `kill` or the plugin's own
stop ladder alike — else absent or `null`. `termination` ∈ `exited
timed_out cancelled killed unknown`. A timeout or a stop resolves the
call **normally** with the corresponding termination — it is not an
error. `cancelled` means the host's `term` (or a failing sink) ended the
command; `killed` means its `kill` did. Stdin arrives as `stdin` frames
and the host's `eof` closes it; a broken pipe while writing is not an
error.

`process_id`, `pty_id`, and `stream_id` are host-generated and unique
for the connection. A stdio process and a PTY each live on one channel
for their whole life: the host sends input as `stdin` frames (its `eof`
closes the process's stdin), the plugin sends output as `stdout` frames
(and a stdio process's stderr as `stderr` frames, which the host keeps as
the diagnostic tail) and its `eof` when the output ends. `pty/close` and
`exec/stdio_wait` end the channel. `logs/follow` writes `stdout` frames
on its channel and sends `eof` before its final response. Dropping the
host-side follow future sends `stream/cancel`.

### 8.5 Filesystem

Paths are sandbox-side strings; relative paths resolve against the
sandbox working directory.

| method | params | result |
| --- | --- | --- |
| `fs/read` | `{sandbox_id, path, channel, offset?, length?}` | `{}` after the bytes |
| `fs/write` | `{sandbox_id, path, channel, append?, content_length?}` | `{}` (creates parents) |
| `fs/delete` | `{sandbox_id, path, recursive}` | `{}` |
| `fs/exists` | `{sandbox_id, path}` | `{exists}` |
| `fs/metadata` | `{sandbox_id, path}` | `{metadata:{kind,size,mode,modified_at}}` |
| `fs/list_dir` | `{sandbox_id, path, depth}` | `{entries:[{path,kind,size}…]}` |
| `fs/create_dir` | `{sandbox_id, path}` | `{}` |
| `fs/rename` | `{sandbox_id, from, to}` | `{}` |
| `fs/set_permissions` | `{sandbox_id, path, mode}` | `{}` (mode is numeric POSIX) |

`kind` ∈ `file directory symlink other`. `fs/read` sends the file's
bytes as `stdout` frames on its channel, then `eof`, then the response;
it takes an optional byte `offset` (default `0`) and `length` (default:
to end of file), and reading at or past the end sends no bytes. A
missing file is the `not_found` error kind with resource `file`.
`fs/write` reads content from the channel's `stdin` frames through the
host's `eof`; it may write each chunk as it arrives. `append` (default
`false`) appends instead of truncating, creating the file when missing.
A plugin must create missing parent directories, and must not need a
shell in the sandbox to do it.

`content_length`, when present, is the exact number of file bytes in the
write channel, excluding frame headers. It lets a plugin start a
size-dependent upload, such as a tar entry, before it has received the
whole file. Too few bytes return an `io` error; too many return
`invalid_spec` for field `content_length`. The host must still send
`eof`. When the field is absent, the plugin accepts content through
`eof` and may buffer it to determine the length. A failed write may
leave a partial destination file.

Upload/download have no wire methods: the host composes them from local
I/O plus `fs/read`/`fs/write`. Frames bound every message, so a large
file crosses in pieces without paging by the host. Providers can stream
file content with bounded memory; providers whose native APIs require
complete byte arrays may still buffer it.

### 8.6 Git

Available when `git.supported` is true. The plugin runs the clone with
the sandbox's own git implementation — native (Daytona's toolbox),
derived (Host and Docker), or hybrid — exactly as it would in-process.
A host sends it when the sandbox declares `git.native`; for a sandbox
that does not, the host's exec-derived clone is the same implementation
the plugin would run, so the host runs it locally through `exec/stream`.
Both transports therefore select one implementation. Every other git
operation (fetch, status, add, add_all, commit, push, pull, branches,
checkout, rev_parse, is_ancestor, the diff views, log, blob sizes and
contents, config_set, untracked_files, and the ambient credential store)
is exec-derived on the host and has no wire method; the host places a sandbox's credential store under the
`runtime_directory` the plugin reported at create or attach.

| method | params | result |
| --- | --- | --- |
| `git/clone` | `{sandbox_id, url, target_path, options:{branch?, commit?, tag?, depth?, credentials?:{username,password}}}` | `{}` |

`target_path` resolves against the sandbox working directory when
relative. `commit`, when present, must be a full 40-hex SHA; the clone
is pinned to it whatever `depth` says, and with `branch` also present the
checkout ends attached to that branch at the pinned commit. `tag` names
a tag without its `refs/tags/` prefix and pins the clone the same way,
at the tagged commit; the plugin fetches it by its fully qualified ref so
a branch of the same name is never selected. `commit` and `tag` are
alternative pins and must not both be present. An unavailable commit or
tag fails with the provider's error; the plugin must not fall back to
the branch head. `credentials` are applied to this one network operation
and never written into the repository configuration. The method is
additive within version 2: a host that receives `-32601` from an older
plugin runs the exec-derived clone it ran before the method existed. A
plugin built before `tag` existed ignores the unknown field and clones
the branch head, so a host that pins tags must run a plugin at least as
new as itself.

### 8.7 Snapshots and volumes

Available only when the corresponding capability object is non-null.
Deletes must be idempotent, including while deletion is in progress.

| method | params | result |
| --- | --- | --- |
| `snapshot/create` | `{spec:{name,source,sandbox_kind,region,resources,provider_config},events?}` | `{snapshot_id}` |
| `snapshot/get` | `{snapshot_id}` | `{status:{id,name,state,sandbox_kind,regions,resources,error_reason,size_bytes,created_at}}` |
| `snapshot/list` | `{filter:{name}}` | `{snapshots:[status…]}` |
| `snapshot/delete` | `{snapshot_id,events?}` | `{}` |
| `snapshot/activate` | `{snapshot_id,events?}` | `{}` (gated on `snapshots.activation`) |
| `snapshot/deactivate` | `{snapshot_id,events?}` | `{}` (gated on `snapshots.activation`) |
| `snapshot/build_logs` | `{snapshot_id, stream_id, follow}` | `{}` after the stream ends |
| `volume/create` | `{spec:{name,size_mb},events?}` | `{volume_id}` |
| `volume/get` | `{volume_id}` | `{status:{id,name,state,error_reason,created_at}}` |
| `volume/list` | `{}` | `{volumes:[status…]}` |
| `volume/delete` | `{volume_id,events?}` | `{}` |

Snapshot `source` variants: `{"image":{"reference":…}}`,
`{"dockerfile":{"content":…}}`,
`{"sandbox":{"id":…,"include_memory":…}}`.
The sandbox source uses the same v1 mapping: `false` is `Filesystem` and
`true` is `LiveProcessState`.
`sandbox_kind` and `region` are optional additive fields. A snapshot made
from a sandbox inherits its kind and resources; a conflicting request is
`invalid_spec`. Daytona supports image sources for both sandbox kinds and
Dockerfile sources for containers only.

### 8.8 Access

| method | params | result |
| --- | --- | --- |
| `access/preview_url` | `{sandbox_id, port}` | `{preview:{url,headers,expires_at}}` |
| `access/signed_preview_url` | `{sandbox_id, port, expires_in_ms}` | `{preview}` |
| `access/preview_release` | `{sandbox_id, port}` | `{}` |
| `access/ssh_create` | `{sandbox_id, ttl_ms}` | `{access:{command,token,expires_at}}` |
| `access/ssh_revoke` | `{sandbox_id, token}` | `{}` |
| `access/web_terminal` | `{sandbox_id}` | `{url}` |
| `access/vnc` | `{sandbox_id}` | `{connection:{url,password}}` |

`access/preview_url` is how a host reaches a port a process inside the
sandbox listens on. The URL is one the *host's* machine can open: the
Host provider returns `http://127.0.0.1:<port>` (the sandbox is that
machine); the Docker provider opens a **port forward** — a listener on
the plugin's own loopback interface, each connection bridged into the
container by a process that connects to the port from inside — and
returns `http://127.0.0.1:<local port>`; Daytona returns its HTTPS
preview link with the headers it requires. A forward's listener accepts
before the container port does: a connection made before anything
listens inside closes with no data, so a host waiting for a server to
come up retries the request rather than the connect. Repeating
`access/preview_url` for the same port returns the same forward. The
bridge runs Bash (its `/dev/tcp` redirect) or `nc` inside the container;
the sandbox's first `access/preview_url` probes for one of them and
fails with a provider error when the image has neither, so an image
that cannot forward is reported at the request.

`access/preview_release` ends the host's use of the port's preview URL:
a provider holding a forward for it closes the forward; one whose URLs
hold nothing answers `{}`. Releasing a port never requested, or twice,
succeeds. `sandbox/stop` and `sandbox/delete` release every port of the
sandbox. The method is additive within version 2: a host treats
`-32601` from an older plugin as released. It uses reserved request
capacity, like the other close and cancel calls.

`access/ssh_create` returns a ready-to-run command. When `ttl_ms` is
absent, a provider may return stable access or use its default temporary
lifetime. When it is present, the provider must honor the requested TTL
or return `unsupported` for `access.ssh.ttl`. `token` and `expires_at`
are optional. `access/ssh_revoke` is available only when
`access.ssh.revoke` is true; a provider that declares it must return a
token that can be revoked.

### 8.9 Provider health

`provider/health` (params `{}`) reports whether the provider's backend
is reachable and its credential accepted, for host preflight and
diagnostics:

```json
{"health":{"status":"ok","message":null,"missing_permissions":[],"required_permissions":[]}}
```

`status` ∈ `ok unreachable unauthorized unknown` (readers map unknown
values to `unknown`). `required_permissions` names every credential
scope the provider's operations need, in the order an operator should
read them (e.g. Daytona API-key scopes), and `missing_permissions` the
ones the provider knows are absent, in the same order; both are empty
when the provider cannot enumerate them. `required_permissions` is
additive: senders may omit it and receivers read an absent list as
empty. An unhealthy
provider is a **successful** response with a non-`ok` status — errors
are reserved for failures of the check itself. Hosts must treat a
`-32601` reply (a plugin predating this method) as
`{"status":"unknown"}`.

### 8.10 Shutdown

`shutdown` (params `{}`) asks the plugin to exit. The plugin must stop
reading new requests, flush its reply, cancel active commands, and exit
promptly. A plugin must also exit when its stdin reaches EOF.

The Rust client gives the acknowledgment and process exit one shared
five-second deadline. If either fails or the deadline expires, the
client kills a child it spawned and returns an error. A client connected
to an externally owned process returns the error without killing that
process. Reaping a killed child must not extend the shutdown deadline.

## 9. Streaming exec

`exec/stream` runs one command with its bytes on a data channel:

```
host → {"id":7,"method":"exec/stream","params":{"sandbox_id":"sb-1","exec_id":"x1",
         "channel":{"channel_id":3,"token":"…"},"stdin":false,
         "spec":{"program":"cargo","args":["build"],"timeout_ms":null,…},"retained_output_limit":65536}}
plugin ⇢ connects to the data socket, frame open {"channel_id":3,"token":"…"}
plugin ⇢ frame stdout …    frame stderr …    (as the command produces them)
host → {"id":8,"method":"exec/stop","params":{"exec_id":"x1","level":"term"}}  (optional)
plugin → {"id":8,"result":{}}
host → {"id":9,"method":"exec/stop","params":{"exec_id":"x1","level":"kill"}}  (optional)
plugin → {"id":9,"result":{}}
plugin ⇢ frame eof
plugin → {"id":7,"result":{"result":{"exit_code":null,"termination":"killed",…},
           "streams_separated":true,"live_streaming":true,
           "stdout_capture":{"observed_bytes":…,"retained_bytes":…,"omitted_bytes":…,
                             "truncated":false},
           "stderr_capture":{…},
           "output_loss":{"dropped_frames":0,"dropped_bytes":0}}}
```

Rules:

- `exec_id` is **host-generated** and unique per connection, so a stop
  can be addressed before the `exec/stream` response exists.
- The plugin registers the exec's stop tokens before opening its data channel.
  The host waits for channel acceptance before forwarding stop tokens. This
  ordering also applies to one-shots and delivers cancellation that was
  requested before exec started. Registration is removed on completion or
  failure, including a failed channel connection.
- When `stdin` is true the host writes the command's input as `stdin`
  frames and its `eof` closes the input; a plugin whose provider offers
  fixed stdin only (`exec.stdin` without `exec.stdin_stream`) reads the
  frames to `eof` before it starts the command. A plugin whose provider
  offers neither must fail the request with `unsupported`.
- **Stops are signals, not a policy.** `level: "term"` sends SIGTERM to
  the command's process group once; the command keeps running until it
  exits or a `level: "kill"` sends SIGKILL. The host owns any escalation
  between the two. A plugin whose backend cannot deliver a signal ends
  the command on either level and reports the termination for the level
  it received. The plugin's own stops — `timeout_ms` elapsing, a failing
  output notification — have no host present to escalate, so they kill.
  Stops and the exec timeout remain active while output drains after the
  main process exits. A kill must not wait for a descendant's open pipe
  or a blocked output sink. Abandoned output sets the capture's
  `truncated` flag. The local hard-cancel drain deadline starts with the kill
  signal, including before channel open. At expiry, `incomplete` reports
  abandoned output, stop acknowledgment, confirmed termination, and cleanup
  as separate facts. Unknown facts are false. Only that local operation ends;
  an unconfirmed provider operation may still be running.
- Every output frame for an exec must be sent **before** its `eof`, and
  the `eof` before the `exec/stream` response, in the order the output
  was observed.
- **Honesty flags.** `live_streaming` is true only if output was
  delivered while the command ran (a plugin that buffers and replays
  must say false). `streams_separated` is true only if stdout and
  stderr are genuinely distinct (combined-output backends must say
  false and use `stdout`). Results must never claim a flag the
  `initialize` capabilities did not declare.
- **Retention.** With an explicit finite `retained_output_limit`, the host keeps a copy of the output
  (a stable head plus rolling tail, bounded by `retained_output_limit`)
  from the frames it reads; the result's bytes are the host's. The
  plugin's `stdout_capture`/`stderr_capture` report its own accounting
  such that `retained + omitted = observed` per stream, and `truncated`
  (default `false`) means bytes were lost *beyond* that accounting — the
  provider abandoned an unfinished drain — so the counts undercount the
  real output.
- **Loss.** `output_loss` (additive; both fields default to `0`) counts
  output the plugin's own transport lost before it reached the channel:
  `dropped_frames` records it could not read and `dropped_bytes` the
  encoded bytes they held. A plugin that reports a loss must also set
  `truncated` on the streams it cannot rule out, so a host that reads
  only the capture flags still learns the output is incomplete. A loss
  never fails the request; the result carries the command's exit status
  and the count. Daytona's encoded output decoder is the producer today.
- **Backpressure and isolation.** The channel's socket is the
  backpressure: a plugin that cannot write must stall the producing
  process, never drop output. Because every exec has its own channel, a
  slow consumer of one exec's output delays nothing else. The
  conformance suite's `concurrent_streams_do_not_starve_each_other`
  check is the acceptance test.
- `exec/stop` for an unknown or finished `exec_id` succeeds and does
  nothing.

### 9.1 One-shot containers

`one_shot/run` (gated on a non-null `one_shot` capability) runs an
ephemeral container beside a sandbox, in the sandbox's world: it shares
the sandbox's workspace at the sandbox's working directory and the
sandbox's network namespace, runs one command from its own image, and is
removed when the command ends. The request shape, channel use, stops,
retention, and result are those of `exec/stream`, without stdin. The spec:

```json
{"image":{"registry":{"reference":"alpine:3.20"}},
 "entrypoint":"sh","args":["-c","echo hi"],"env":{},
 "working_dir":null,"timeout":{"secs":60,"nanos":0},"output_sanitization":"raw"}
```

`image` is `{"registry":{"reference":…}}` or, gated on `one_shot.build`,
`{"build":{"context":…,"dockerfile":…,"tag":…,"reuse":…}}`: a Dockerfile
under `context` inside the sandbox, built to `tag`, reused without
building when `reuse` is true and an image carrying `tag` exists.
`working_dir` defaults to the sandbox's working directory. `timeout` is
the structural duration form, `null` for none.

A one-shot container belongs to its sandbox: the sandbox's `stop` and
`delete` end any of its one-shot containers still running, so a host
that went away mid-run leaves nothing the sandbox's own lifecycle does
not reach. The exit code is the container's own; a `term` is one SIGTERM
to the container's entrypoint, `kill` and the timeout SIGKILL it.

## 10. Events

The plugin emits `host/event` notifications for sandbox, snapshot, and
volume control-plane operations. Exec output, PTY bytes, file-transfer
chunks, and logs use their data channels and never use this event feed.

```json
{"method":"host/event","params":{
  "route_id":"event-7",
  "event":{
    "id":{"source_id":"9b2f…","sequence":4},
    "occurred_at":"2026-08-31T20:00:00Z",
    "provider":"daytona",
    "subject":{"type":"sandbox","id":"sb-1"},
    "operation_id":"58a1…",
    "correlation_id":"fabro-run-42",
    "type":"operation_failed",
    "action":"start",
    "duration":{"secs":30,"nanos":0},
    "error":{"kind":"timeout","message":"…","retryable":true,"causes":[]}
  }
}}
```

The event envelope fields are:

- `id`: `{source_id,sequence}`. `source_id` identifies one live event
  source. `sequence` starts at 1 and increases by one for that source.
- `occurred_at`: the observation time in the structural timestamp form
  from §6.
- `provider`: the provider kind.
- `subject`: `{"type":"provider"}` or a `sandbox`, `snapshot`, or
  `volume` subject. Resource subjects carry an optional `id` and `name`.
- `operation_id`: present on operation lifecycle events. It is stable
  from start through terminal outcome.
- `correlation_id`: the optional consumer value from the `events`
  request object.
- `type` and its body fields.

Event `type` values are:

- `operation_started` with `action`.
- `operation_progress` with `action` and `progress`. Progress has a
  stable `code`, an optional display `message`, and optional
  `completed`, `total`, and `unit` measurements. Consumers must branch
  on `code`, not `message`.
- `operation_completed` with `action` and `duration`.
- `operation_failed` with `action`, `duration`, and the structured error
  report from §7.
- `state_observed` with optional `previous` and required `current`
  resource state.
- `notice` with a stable `code` and display `message`.

For each accepted operation, the plugin must emit
`operation_started` and exactly one `operation_completed` or
`operation_failed`. They must have the same `operation_id`. The terminal
event must enter ordered event delivery before the operation response. A create can
start with a name-only subject. Its terminal event must include the
provider-assigned resource ID when one was assigned.

Event callbacks do not block the shared control response reader. Notifications
enter bounded, ordered event delivery; responses can arrive before callbacks
finish. A slow callback or a full queue explicitly fails this connection's
event subscription. The plugin signals queue failure with `host/event_failed`
and null params. The Rust client exposes `event_delivery_error()` for this
failure and for its own observer timeout or overflow. After failure, consumers
must treat the event history as incomplete. Unrelated control calls continue.
`route_id` echoes the request value when present; it is transport metadata,
not event identity. Applications that need durable persistence must wait on
their own observer's persistence boundary.

The protocol has no event replay or persistence API. The host decides
whether its observer persists events. `sandbox/describe`,
`snapshot/get`, and `volume/get` remain authoritative for current
resource state. A re-attach creates a new live event source. Unknown
event types and subject kinds must be ignored without closing the
connection.

`host/log` (`{level, message}`) is reserved: plugins should prefer
stderr for logs, and hosts may ignore `host/log`.

## 11. Discovery and trust (host-side conventions)

These bind hosts that launch plugins from configuration; the reference
implementation is `sandbox_driver_protocol::discovery`.

- **Naming convention.** A plugin binary is found either at an
  explicitly configured path or as `<prefix>-<kind>` on `PATH`, where
  `<prefix>` is chosen by the embedder. The reference plugins ship under
  the `sandbox-driver` prefix: `sandbox-driver-host`,
  `sandbox-driver-docker`, and `sandbox-driver-daytona`, one archive and
  one checksum each per target. There is no other fallback; unknown
  kinds are never launched (deny by default).
- **Checksum or dev, never neither.** Configuration pins the binary's
  SHA-256, verified before exec; a mismatch is a hard failure naming
  both hashes. Launching without a pin requires an explicit dev flag,
  and such launches are reported as unverified so operators can be
  warned.
- **Environment scrubbing.** The plugin starts from an empty
  environment plus exactly the variables its configuration declares or
  explicitly forwards. Provider credentials should be passed this way
  deliberately, never inherited.
- **The configuration names the plugin.** The configured kind is the
  host's name for whatever the executable serves; a declared
  `provider.kind` that differs from it is not an error. A host that has
  pinned the checksum has already decided which executable it trusts,
  and one executable may serve under several configured names.
- A plugin is inside the trust domain of every sandbox it drives; see
  the security section of the interface design document.

## 12. Conformance

A plugin is conformant when the `sandbox-driver-conformance` suite
passes against it through `PluginProvider` — the same battery every
in-process provider must pass, covering lifecycle, the bash probe,
exec semantics (literal argv, exit codes, env, binary safety, fixed and
streamed stdin, the effective environment, timeout, term and kill),
streaming honesty and isolation, retention accounting, filesystem round
trips including bounded reads and missing files, the git round trip
(whose clone is `git/clone`), one-shot containers, capability honesty in
both directions, label listing, event delivery, and
service/facet-capability consistency. The Host and Docker providers
run it over the wire in this repository's own tests.

## 13. Compatibility policy

Within version 2: changes must be additive (new methods, new optional
fields, new enum values that readers already tolerate). Anything else —
renaming fields, changing a pinned encoding, making an optional field
required, changing the frame format — requires incrementing
`protocol_version`, and the handshake's version check is the only
compatibility gate. Compatibility is verified behaviorally: readers must
decode era JSON written before any later additive field existed (every
wire struct's growable fields carry serde defaults), tolerate unknown
fields and enum values, and keep the pinned per-field encodings.
Full-shape golden pins are deliberately not used — they fail on additive
changes this section declares compatible.

Version 1 is not served or spoken by this implementation. Its
base64 methods (`exec/run`, `exec/stdio_input`, `exec/stdio_output`,
`exec/stdio_close_input`, `pty/input`, `pty/output`) and notifications
(`exec/output`, `logs/output`) do not exist in version 2, and a
version-1 `fs/read` or `fs/write` shape is a `-32600` here.

## 14. Deferred beyond version 2

Native search/service passthrough, local `shell_command`, and
`host/credentials` (per-call secret fetches from the host) remain
deferred. They are masked or absent per §5.

## 15. Shared-contract metadata discovery

The additive `sandbox_driver_protocol::metadata` module implements the
credential-free discovery step in [the shared contract](shared-contract.md).
It does not change the v2 handshake or make a v2 provider implement the new
contract. Existing `launch_plugin`, `PluginProvider`, and `serve_stdio` remain
the v2 entry points.

`SchemaDiscovery` starts nothing when constructed. An explicit `discover`
takes only a `PluginLocation` (entry name, kind, path, trust), never the
entry's config, arguments, environment or secrets. The path must be absolute;
there is no search of the caller's `PATH`. Discovery checks the checksum (or
explicit development trust) and only then runs the executable with exactly
`--describe-schema`. The child gets null stdin, `/` as its working directory,
and no runtime arguments, connection configuration, or configured, inherited,
or resolved secret environment. On Unix the probe PATH is `/usr/bin:/bin`; no
HOME or user PATH is forwarded. On Windows only SystemRoot is forwarded. The
executable writes one `ProviderSchemas` JSON object and exits successfully;
diagnostics belong on stderr and are logged, never put in an error. SDK
authors can use `metadata::write_schema` before constructing their provider or
loading runtime settings, then exit.

Default budgets are five seconds for the whole discovery call, 1 MiB of stdout,
64 KiB of stderr, four concurrent probes, and 64 cached binaries. A probe
beyond the concurrency budget waits within its caller's timeout; concurrent
discoveries of one binary share a single probe. Both pipes are drained
concurrently and bounded. On Unix the probe runs in its own process group,
which is killed when the probe returns, fails, times out or is cancelled, so
descendants do not outlive it.

The cache contains metadata only, keyed by the actual SHA-256 of the executable.
Every lookup rehashes the selected executable and checks that location's trust,
even on a cache hit. Changed bytes require rediscovery; failed probes are not
cached. Cache eviction (oldest first) permits a later probe. `DiscoveredPlugin`
retains the canonical executable path, digest, schemas, and whether a pin was
verified. Runtime launch must call `verify_unchanged` immediately before
starting the configured runtime; the catalog compares the runtime's descriptor
with the discovered schemas before any sandbox operation. Cached metadata never
supplies namespace or capability facts. Unlike legacy v2 kind aliasing, the
configured new-contract plugin kind must match discovery.

A missing, relative, non-executable or mismatched executable is
`invalid_config` naming the entry and the `path`, `sha256` or `plugin` field;
a mismatch reports both digests. A probe that fails, times out or prints
anything but one schemas object is `unavailable` with outcome `not_started`.

Probing executes trusted code, not sandboxed code. The executable is hashed and
then executed by path, so installation directories and executables must be
protected against concurrent modification. The checksum covers the executable
bytes, not interpreters, shared libraries, or dependencies.
