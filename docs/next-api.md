# sandbox-driver

> **Status: target, not yet the README.** This document describes the API
> that `sandbox_driver::contract` is building toward. It is the
> specification every lane of the sandbox refactor implements against, and
> it changes in the same PR as the `contract` module. The API an
> application can call today is described in [docs/design.md](design.md)
> and the current README; the implementation handoff for providers is
> [docs/shared-contract.md](shared-contract.md). This file replaces the
> README once the shared `Sandboxes` API, the plugin loader, and the Docker,
> Daytona and Host providers pass conformance together (the "providers
> ready" gate); its "Status" section then lists what is still not true.

One interface to any sandbox. Clients name an environment; providers own the sandboxes; the library owns everything in between.

```rust
// Shared by requests within the Fabro server process.
// Worker processes construct their own instance from resolved configuration.
let sandboxes = Sandboxes::try_new(
    settings.sandbox.providers.clone(), environments, &registry, credentials,
    CatalogOptions::default(),
)?;

// A run starts on the "cloud" environment.
let sandbox = sandboxes.builder("cloud-linux-xl").await?            // seeded from the environment's template
    .label("fabro.run", &run.id)
    .create()
    .await?;

sandbox.exec(ExecSpec::argv(["git", "clone", &repo, "."])).await?;
run.sandbox_ref = Some(sandbox.reference());         // {provider, namespace, id}, stored with the run

// Later, from the sandbox page in another request.
let sandbox = sandboxes.attach(&run.sandbox_ref).await?;
let preview = sandbox.ports().expose(3000, Expose::signed(Duration::from_secs(3600))).await?;

// Everything this run created, across every provider.
let leftovers = sandboxes.find(LabelFilter::eq("fabro.run", &run.id)).await?;
```

`"cloud"` is an **environment**: a named template on a **provider entry**. The entry says how to reach a backend; the environment says what to build on it. Many environments share one entry.

```toml
[providers.daytona]                      # a connection, named by the operator
plugin  = "daytona"                      # built in
config  = { api_url = "https://app.daytona.io/api", organization = "org_…" }
secrets = ["DAYTONA_API_KEY"]

[environments.cloud]
provider = "daytona"

[environments.cloud.spec]
source    = { dockerfile = "FROM buildpack-deps:noble\n" }   # or { image = "buildpack-deps:noble" }
resources = { cpu = 2, memory = "4GB" }
network   = { mode = "allow_all" }
timers    = { auto_stop = "60m" }
```

Fabro keeps provider entries in its server settings and environments in its database, edited in the UI. A workflow selects one with `[run.environment] id = "cloud"`, and says what happens to the sandbox when the run ends with `[run.lifecycle]`, which is Fabro's policy and not part of the environment. The values above are Fabro's seeded default for Daytona; a workflow that needs more, like Fabro's own code review, gives a longer Dockerfile that installs its tools on the same base. The Daytona provider builds the source into a Daytona snapshot named by its inputs and reuses it on later runs. The library itself ships no provider entries or environments.

Everything in the template rides along on `create()`. A caller that needs something different for one sandbox overrides it on the builder, and the provider validates the override against what it supports:

```rust
let sandbox = sandboxes.builder("cloud").await?.memory("8GB").create().await?;
```

Clients pass the environment name and never the provider's. Whether `cloud` points at the built-in Daytona provider, the built-in Docker one, or a plugin someone wrote last week, the code above is the same.

## What it is

- A **client API**: one `Sandboxes` value built once, a sandbox builder, and the `Sandbox` trait. That is all an embedding program sees.
- A **plugin contract**: twelve required calls over JSON lines on stdin and stdout, plus four optional capabilities a provider may declare. Any language. A plugin owns all state for its sandboxes; the library stores only ids.
- A **catalog** in two levels: provider entries (which plugin, its connection, its trust settings) and environments (a named spec template on an entry). The client decides where each level is stored and whether it changes at runtime; the library defines what an entry means.
- Everything **between them**: discovery, trust, supervision, restart, descriptor caching, identity, a fake for tests, and a conformance suite for plugin authors.

The library provisions and operates sandboxes. Scheduling, queueing, lease management and run-end policy belong to the caller.

## Concepts

| Term | Meaning |
| --- | --- |
| **Provider** | A backend that makes sandboxes. Built in (`docker`, `daytona`) or a plugin (`host`, and any third party's). |
| **Provider entry** | A named, configured connection to a provider: which plugin, its config, the secrets it needs, and for a plugin its path and checksum. Operator-owned. One entry serves many environments; two entries can use the same plugin (a production and a staging Daytona org). |
| **Environment** | A named spec template on a provider entry: source, resources, network, timers, labels, env, working directory. User-owned. What clients and workflows name. |
| **Plugin** | An executable implementing the contract, run as a supervised child process. `host` ships this way, installed separately on each execution machine, and so does every third-party provider. |
| **Built-in** | `docker` and `daytona`. Library code, compiled in, no child process. They implement the same internal trait a plugin is adapted to, so clients cannot tell the two apart. |
| **Namespace** | The stable, non-secret id a provider reports for where its sandboxes live: a Docker daemon, a Daytona organization, a machine. |
| **SandboxRef** | `{provider, namespace, id}`. The only identity. Serializable. `attach` resolves it by namespace. |
| **Durability** | What a provider promises about a sandbox outliving things: `process`, `machine`, or `remote`. |
| **Descriptor** | What a provider reports about itself: capabilities as data. Clients branch on it, never on a kind name. |

Kind names (`docker`, `daytona`, `lima`) appear in provider entries and nowhere else.

## Client API

`Sandboxes` is the entry point. It reads like a collection: attach one, build one, find many.

```rust
pub struct Sandboxes { /* private */ }

impl Sandboxes {
    /// Checks local structure only. Discovery, secrets and activation are lazy per entry.
    pub fn try_new(
        providers: impl IntoIterator<Item = (ProviderName, ProviderEntry)>,
        environments: Arc<dyn EnvironmentSource>,
        registry: &Registry,
        credentials: Arc<dyn SecretResolver>,
        options: CatalogOptions,
    ) -> Result<Self, Error>;
    /// Both levels from one file (the CLI, a bare Petri run).
    pub async fn from_config_file(
        path: &Path, registry: &Registry, credentials: Arc<dyn SecretResolver>,
        options: CatalogOptions,
    ) -> Result<Self, Error>;

    // Sandboxes
    pub async fn builder(&self, environment: &str) -> Result<SandboxBuilder, Error>;   // NoEnvironment if unknown
    pub async fn attach(&self, r: &SandboxRef) -> Result<Box<dyn Sandbox>, Error>;
    pub async fn find(&self, filter: LabelFilter) -> Result<Found, Error>;       // across every provider
    pub async fn destroy(&self, r: &SandboxRef) -> Result<(), Error>;

    // The catalog
    pub fn providers(&self) -> Vec<ProviderName>;
    pub fn provider(&self, name: &str) -> Result<ProviderHandle, Error>;
    pub async fn environments(&self) -> Result<Vec<EnvironmentName>, Error>;
    pub async fn environment(&self, name: &str) -> Result<Environment, Error>;

    pub async fn shutdown(self) -> Result<(), Error>;
}

impl SandboxBuilder {
    pub fn image(self, r: &str) -> Self;             // and dockerfile, env, working_dir, user, network,
    pub fn label(self, k: &str, v: &str) -> Self;    // cpu, memory, disk, timers, provider_config
    pub fn memory(self, v: &str) -> Self;
    pub fn spec(self, spec: SandboxSpec) -> Self;    // for callers that already hold a spec as data
    pub async fn create(self) -> Result<Box<dyn Sandbox>, Error>;
}

impl ProviderHandle {
    pub async fn describe(&self) -> Result<Descriptor, Error>;
    pub async fn health(&self) -> Result<Health, Error>;
}

/// What `find` returns: every match it could reach, and the providers it could not.
pub struct Found {
    pub sandboxes: Vec<SandboxSummary>,              // reference, name, state, labels
    pub unavailable: Vec<ProviderName>,              // every entry whose query failed
    pub failures: Vec<ProviderFailure>,              // entry plus structured error
}

#[async_trait]
pub trait Sandbox: Send + Sync {
    fn reference(&self) -> SandboxRef;
    fn working_directory(&self) -> &Path;
    fn env(&self) -> &BTreeMap<String, String>;      // the sandbox's ambient variables
    async fn status(&self) -> Result<Status, Error>;
    async fn start(&self) -> Result<(), Error>;
    async fn stop(&self) -> Result<(), Error>;
    async fn exec(&self, spec: ExecSpec, controls: ExecControls) -> Result<ExecOutcome, Error>;
    fn files(&self) -> &dyn Files;                   // get, put, list; stat/rm/mv/mkdir derived over exec
    fn ports(&self) -> &dyn Ports;                   // expose, release
    fn lifecycle(&self) -> Option<&dyn Lifecycle>;   // optional capability: pause, resume, archive, recover
    fn endpoints(&self) -> Option<&dyn Endpoints>;   // optional capability: ssh, shell command, vnc
    fn containers(&self) -> Option<&dyn Containers>; // optional capability: run a one-shot container in this sandbox's world
}
```

`Sandboxes` is a concrete type built once per process. Provider entries are given at construction. Environments are read from their source each time one is used, so a catalog that changes while the process runs, like Fabro's environment store, needs no rebuild. `Sandbox` is a trait because the value behind it is a built-in provider, a plugin child, or the fake, and the caller must not care which.

```rust
#[async_trait]
pub trait EnvironmentSource: Send + Sync {
    async fn names(&self) -> Result<Vec<EnvironmentName>, Error>;
    /// `Ok(None)` means the store has no environment with this name.
    async fn get(&self, name: &str) -> Result<Option<Environment>, Error>;
}
```

A static list and a config file are both `EnvironmentSource`s the library provides for you. A client whose environments are editable at runtime implements the trait over its own store. The trait is async and fallible because Fabro's store is a database: a storage failure is `Unavailable`, and only `Ok(None)` means the environment does not exist, so a flaky store is never read as a missing environment. `Environment::validate(&Descriptor)` is public so such a client can reject a bad edit when it is saved rather than when it is used.

`find` fans out to every provider entry and merges the results. A provider that does not answer is listed in `unavailable` rather than failing the whole query, so a sweep can act on what it found and come back for the rest.

Rules every method obeys:

1. **Wire-expressible.** Data in, data or an opaque `Sandbox` out. No process handles, file descriptors, closures, or client-local paths. This is what lets the plugin protocol carry a call to a child process and lets the fake serve the same API in memory.
2. **Identity is the ref.** Nothing else identifies a sandbox. Clients store refs and derive nothing. `attach` resolves only the saved provider-entry binding and checks its namespace before dispatch, so renaming or deleting the environment a sandbox was created from does not orphan the sandbox.
3. **Capabilities are data.** To know whether a sandbox can do something, read the descriptor or check an extension accessor for `Some`. Kind names are never a basis for behavior.

### Exec

`ExecSpec` is argv (no shell), env, working directory, timeout, and `tty`. `ExecControls` is a streamed stdin, an output sink, and two cancellation levels: `term` then `kill`. `ExecOutcome` reports `termination` (`exited`, `timed_out`, `terminated`, `killed`, `lost`), `exit_code`, `signal`, and output-loss counts. With `tty: true` the sink receives one stream and the controls gain `resize`. Dropping the future requests cancellation; it does not confirm that the command has stopped. The library owns bounded cleanup after the drop. `lost` means the execution outcome is unknown, not that the command or sandbox is known to be gone. Only a confirmed termination outcome or a successful sandbox `stop` establishes that the affected execution has ended.

### Files and ports

`Files` is `get(path, range)`, `put(path, bytes)` creating parents, and `list(path, depth)`. Everything else (`stat`, `remove`, `rename`, `mkdir`) is derived over exec by the library and works on every provider.

`Ports` is `expose(port, Expose { signed, ttl }) -> Exposure { url, headers, expires_at }` and `release(port)`. Some providers hold a real local forward until `release`.

### Errors

A closed set with stable codes, the same in Rust and on the wire:

`NoProvider`, `NoEnvironment`, `InvalidConfig { entry, path, message }`, `MissingSecret { entry, name }`, `NotFound`, `AlreadyExists { name, id }`, `NotOwned`, `NamespaceMismatch`, `Unavailable` (currently unreachable; does not establish that execution stopped or a resource was deleted), `Unauthenticated` (the backend rejected the configured credentials), `PermissionDenied` (valid credentials that do not permit this operation), `ResourceExhausted { retry_after_ms }` (a quota or rate limit refused admission), `Busy` (a prior instance is still going away or another mutating owner holds the fence), `Unsupported { capability }`, `Unhealthy { message }`, `Stale` (the plugin restarted; re-attach by ref), `InvalidSpec { path, message }`, `IncompleteCleanup { remaining }` (destroy or a failed create could not confirm cleanup; an empty list means the ids are unknown, not that nothing remains), `FenceLeaked`, `Transport`.

The three refusals are distinct from `Unavailable` so a client can tell bad credentials from a backend that is down, and none of them is a retry hint. `Unavailable`, `Stale` and `Transport` carry an `outcome` of `not_started` or `unknown`; it defaults to `unknown`, and `not_started` requires proof that nothing was admitted.

## Configuration

`try_new` takes both levels of the catalog from wherever the client keeps them. Provider entries are operator-owned and rarely change. Environments come from any `EnvironmentSource`: a fixed list, a file, or a live store whose edits are visible on the next use. A client with neither calls `from_config_file` with an explicit path (the application may select `~/.config/sandbox-driver/environments.toml` or `SANDBOX_DRIVER_CONFIG`), so tools on one machine can share a catalog. Names at both levels are chosen by the operator; the library ships none.

```toml
[providers.docker]
plugin = "docker"                 # built in, compiled into the caller
config = { host = "unix:///var/run/docker.sock" }

[providers.daytona]
plugin  = "daytona"
config  = { api_url = "https://app.daytona.io/api", organization = "org_…" }
secrets = ["DAYTONA_API_KEY"]     # read from the environment, never stored here

[providers.local]
plugin = "host"                   # not built in: a plugin installed on this machine, durability: process
path   = "~/.local/share/sandbox-driver/plugins/host/sandbox-driver-host"
sha256 = "3c1e…"

[providers.lima]
plugin = "lima"                   # not built in: resolved from the plugin directory, see "Using a third-party plugin"
path   = "~/.local/share/sandbox-driver/plugins/lima/sandbox-driver-lima"
sha256 = "9f2c…"                  # or dev = true during development

[environments.ci-linux]
provider = "docker"

[environments.ci-linux.spec]
source  = { image = "ghcr.io/lithoscomputer/ubuntu-24.04:slim-f8bbbfd81934" }   # Petri's pinned runner image
network = { mode = "block" }

[environments.cloud]
provider = "daytona"

[environments.cloud.spec]
source    = { dockerfile = "FROM buildpack-deps:noble\n" }
resources = { cpu = 2, memory = "4GB" }
timers    = { auto_stop = "60m" }

[environments.scratch]
provider = "local"

[environments.lima-dev]
provider = "lima"

[environments.lima-dev.spec]
source = { image = "ubuntu-lts" }
```

Per provider entry: `plugin`, `config` (whatever that provider's `config_schema` accepts, validated on first selected-entry activation so a typo fails with the field named; the library never reads its contents), `secrets`, and for a third-party plugin `path`, `sha256` or `dev`, `args`, `env`, `inherit_env`. A `sha256` pin together with `dev = true` is a decoding error, so a pin is never silently skipped.

Credentials and conveniences take different paths:

- **`secrets`** names the credentials the entry needs. The library resolves each name at entry activation through the explicitly supplied resolver (Fabro uses its vault), and an unresolvable name is a `MissingSecret` error naming the entry. Values reach a built-in as configuration and a plugin child as explicit environment. A plugin that accepts one of several credential forms says so in its `config_schema`; the library never guesses.
- **`inherit_env`** carries conveniences only. Every plugin child starts from an empty environment. An omitted list selects `PATH`, `HOME`, `TMPDIR` and `USER`; an explicit list replaces those defaults. Values are copied from the supplied snapshot and frozen for the life of the entry. An unset convenience name is simply not set. `inherit_env = []` yields exactly explicit `env` and resolved secrets, with no ambient inheritance, for operators who want nothing ambient. A value in `env` always wins over an inherited one.
- Built-ins read nothing from the ambient environment. Their only inputs are the entry's `config` and resolved `secrets`, which is what makes one process safe to run entries for different owners.

Per environment: `provider` (an entry name) and `spec`, a `SandboxSpec` template the sandbox builder starts from: `source`, `resources`, `network`, `timers`, `labels`, `env`, `working_directory`, `workspace_ownership`, `user`, `services` and `container` (a typed workload description, below), and `provider_config` for the few per-sandbox settings that have no provider-neutral meaning, validated against the provider's `spec_schema` when it declares one. `Environment` and `ProviderEntry` derive `Deserialize`, so a client's own settings types can embed them directly.

Sizes are written with a unit and read the way Docker and systemd read them: `4GB`, `4G`, `4GiB` and `4Gi` all mean 4096 MiB, and `cpu` is whole cores. The library parses once; providers receive integers and never see the string. A provider that allocates in coarser units rounds up and reports what it gave you in `status`.

That single parse happens when a `SandboxSpec` is decoded. `resources` accepts `cpu`, `memory` and `disk` with units, and `timers` accepts `auto_stop`, `auto_archive`, `auto_delete` and `ttl` as durations (`60m`, `1h 30m`, `500ms`; `0s` explicitly disables a timer). The value is stored and always serialized in the integer form, `cpu_cores`, `memory_mb`, `disk_mb` and `*_ms`, which is also accepted on input. The grammar is exported as `parse_size_mib` and `parse_duration_ms` for the builder's `memory("8GB")` and for UIs that validate before saving.

The `ci-linux` environment uses the image Petri runs process scopes in on Docker. Petri publishes these runners as `ghcr.io/lithoscomputer/ubuntu-<version>:<flavor>-<pin>`. Under this contract, clients select the workload source; choosing an outer runner or a Docker-in-Docker helper image is provider implementation detail. If Daytona needs an outer runner to host that workload, the Daytona provider provisions it and routes operations to the workload without a different recipe in Fabro or Petri.

Plugins are found in `~/.local/share/sandbox-driver/plugins/<name>/` or any directory listed in `plugin_dirs`. A `plugins.lock` records the checksum of every plugin the operator trusted. `lithos-sandbox plugin install <path-or-url>` writes both and adds a provider entry.

## Providers

Docker and Daytona are built in. Host is a plugin: installed separately on the machine that runs the work, reached through the same configured loader as any third-party plugin, and never linked into or bundled with Fabro, which a release check proves.

|  | host | docker | daytona |
| --- | --- | --- | --- |
| Namespace | `host:<machine-id>` | `docker:<daemon-id>` | `daytona:<org>` |
| Durability | `process` | `machine` | `remote` |
| Sources | host directory | image | image, dockerfile, snapshot |
| Reaches the client's machine | yes | yes, via gateway alias (local daemon only) | no |
| Mounts host paths | yes | local daemon only | no |
| Network policy | default only | allow all, block | allow all, block, CIDR list, domain list |
| Resources | ignored | cpu, memory | cpu, memory, disk |
| Optional capabilities | none | lifecycle (pause, resume), endpoints (shell command), services, containers | lifecycle (pause, resume on VMs), endpoints (ssh, vnc), services, containers |

Each of these facts comes from the provider's descriptor at runtime; the table summarizes them.

`host` runs commands as child processes in a directory the client designates, on the machine where the Host plugin is installed. One live plugin instance owns its sandboxes, their process groups and their lifecycle; a second instance cannot take them over from a path or a ref, and reattaching after the owner dies is not promised. Its sandboxes last as long as that instance. Use `docker` for local sandboxes that must outlive the caller.

## Using a third-party plugin

Three steps and no code. The example installs a Lima plugin someone else published.

**1. Install and trust it.** The CLI copies the executable into the plugin directory, records its checksum in `plugins.lock`, and adds a provider entry. During development, `--dev` skips the checksum.

```sh
lithos-sandbox plugin install ./sandbox-driver-lima
lithos-sandbox plugin install ./sandbox-driver-lima --dev
lithos-sandbox provider describe lima     # the plugin's descriptor: namespace, durability, config schema
```

**2. Give it an environment.** The entry's `config` is checked against the plugin's `config_schema`, so a typo fails at construction with the field named. If the plugin needed a credential, the entry would name it under `secrets` and construction would fail if it could not be resolved.

```toml
[providers.lima]
plugin = "lima"
path   = "~/.local/share/sandbox-driver/plugins/lima/sandbox-driver-lima"
sha256 = "9f2c…"
config = { vm_type = "vz" }

[environments.lima-dev]
provider = "lima"

[environments.lima-dev.spec]
source = { image = "ubuntu-lts" }
```

**3. Use it like any other environment.** Nothing in the calling program knows this one is a child process.

```rust
let sandbox = sandboxes.builder("lima-dev")?.create().await?;
```

```sh
lithos-sandbox create lima-dev
```

The child starts on the first call and is supervised from then on. If the executable is missing or its checksum has changed, construction reports which entry and why. If the plugin's own backend is unavailable, `provider("lima")?.health()` says so before any sandbox is attempted.

**In Fabro** the two levels live where they already do: the provider entry in server settings, the environment in the environments UI.

```toml
[server.sandbox.providers.lima]
plugin = "lima"
path   = "/usr/local/lib/sandbox-driver/plugins/sandbox-driver-lima"
sha256 = "9f2c…"
config = { vm_type = "vz" }
```

Then create an environment named `lima-dev` on provider `lima` in the UI, where the form shows the plugin's config fields from its schema. Workflows select it with `[run.environment] id = "lima-dev"`.

## Writing a plugin

A plugin is an executable. The library starts it, writes JSON lines to its stdin, and reads JSON lines from its stdout. Stderr is captured as plugin logs. Exit on stdin EOF.

**Before any launch, the library asks for your schemas.** `try_new` validates every entry's `config` without starting plugins: it runs your executable once as `--describe-schema`, a short-lived process given only the minimal platform environment and no config, secrets or backend access, which prints the `schemas` part of the descriptor (below) as one JSON object on stdout and exits. The result is cached by the verified executable digest, so a changed binary is probed and trusted again. The full descriptor, with the namespace, comes from `describe` once you are running with config.

The first request is `initialize`. It carries the protocol version, the entry's `config`, already validated against the `config_schema` your probe returned, and the path of the data socket described below, so a plugin reads its settings from the handshake and never from files it has to locate itself. Secrets named in the entry are in your environment under their own names. Your environment otherwise holds `PATH`, `HOME`, `TMPDIR`, `USER` and whatever the operator chose to forward; do not depend on anything else being set.

```json
{"id": 1, "method": "initialize", "params": {"protocol": 3, "config": {"kubeconfig": "/home/worker/.kube/config", "namespace": "livespec-factory"}, "data_socket": "/tmp/sandbox-driver-4821-x7/data.sock"}}
{"id": 1, "result": {"ok": true}}
```

**Control and data travel separately.** Every request and response is one JSON line on stdin and stdout, and no bulk bytes ever cross that pipe. A call that streams, meaning `exec`, `files.get`, `files.put`, a PTY, or a one-shot container, names a channel in its params. The plugin opens its own connection to the data socket, sends an `open` frame carrying that channel, and then moves raw bytes on that connection: `stdout` and `stderr` frames out, `stdin` frames in, and an `eof` when it is done, before it sends the control response. Frames are one kind byte, a four-byte big-endian length, and the payload.

```json
{"id": 7, "method": "exec", "params": {"sandbox": "sb-1a2b", "argv": ["make", "test"], "timeout_ms": 600000, "channel": {"id": "c9", "token": "…"}}}
```

```
on a new data connection:  open{"id":"c9","token":"…"}  stdout…  stderr…  eof
```

```json
{"id": 7, "result": {"termination": "exited", "exit_code": 0, "dropped_bytes": 0}}
{"id": 8, "method": "stop", "params": {"exec": "c9", "level": "term"}}
```

Each stream has its own connection, so a slow reader stalls only that stream, a large file transfer never delays an unrelated `status`, and `stop`, `cancel` and `health` are answered while a transfer is in flight because they have reserved capacity on the control channel. PTY `resize` is a control request too. The Rust SDK's `serve()` handles all of this; the Python example does it in about thirty lines, and a plugin author otherwise writes the twelve calls.

### Required calls

| Call | Returns |
| --- | --- |
| `describe` | the descriptor (below) |
| `health` | `{status: ok | unreachable | unauthorized, message, missing_permissions}` |
| `create(spec)` | `{id, name, working_directory, env, status}` |
| `attach(id)` | same as create |
| `status(id)` | `{reference, state, name, labels, image, snapshot, resources, network, services, web_url}`; `resources` is what you actually allocated; states include `unavailable` (unreachable, not known gone) and `lost` (established gone without a confirmed destroy); `services` maps each service name to the address the sandbox reaches it at |
| `start(id)`, `stop(id)` | `{}`; stop fences all workload execution, services and one-shot containers while keeping the workspace; start waits for declared service readiness and fails with `Busy` while a prior instance is still going away |
| `exec(sandbox, spec)` | streamed, then the outcome |
| `files.get(sandbox, path, range)`, `files.put(sandbox, path)`, `files.list(sandbox, path, depth)` | bytes stream, `{}`, entries |
| `ports.expose(sandbox, port, signed, ttl_ms)`, `ports.release(sandbox, port)` | `{url, headers, expires_at}`, `{}` |
| `list(labels)` | `[{id, labels, state}]` |
| `destroy(id)` | `{}`; not-found is success |

Errors are `{"id": 7, "error": {"code": "not_found", "message": "…"}}` using the closed code set above.

Three rules about what crosses the contract:

- **The name is a hint.** `spec.name` is what the caller would like the sandbox called. Use it as is, adapt it to your backend's rules, or generate your own, and return the name you used. If your backend enforces unique names and the hint collides, you may return `already_exists` with the existing id; you are not required to detect duplicates. Callers find sandboxes again by labels and by the id you returned, never by name.
- **Labels round-trip exactly.** Whatever keys and values the caller sets must come back unchanged from `status` and must match in `list` filters. If your backend cannot store a value as given, encode it reversibly; if it cannot store it at all, refuse with `invalid_spec` naming the label. Never truncate or drop one.
- Unavailable is not lost. When you cannot reach a sandbox but do not know it to be gone, report state `unavailable` and fail commands with the `unavailable` error. The library does not infer deletion or replace the sandbox because contact was lost. Report a known lifecycle state only when it can be established. The caller decides how its application responds to the uncertainty.

`stop` and `start` are a fence around the whole sandbox workload. Before draining execution, `stop(id)` blocks new exec and one-shot container admissions. It returns success only after workload commands, services, and one-shot containers have stopped and the workspace is intact. This includes execution admitted through another handle or provider instance. `start(id)` fails with `Busy` while a previous instance or stop is still draining; after stop then start, no execution from before the stop can remain. Providers coordinate this boundary across instances, or enforce one mutating owner and reject conflicting mutations with `Busy`. A timeout, lost connection, or unconfirmed cleanup is an error, never a successful fence.

### Workload lifecycle and uncertain outcomes

Services belong to the sandbox. `create` and `start` return success only when the workload can execute and every declared service readiness check has passed. `stop` stops the services as well as the workload; `start` restores them from the sandbox's saved spec and checks readiness again. Service addresses may change across a restart; `status.services` reports the current addresses.

`destroy` stops execution and removes services, one-shot containers, and other provider-managed resources belonging to the sandbox. It returns success only when that cleanup is confirmed; an already-absent sandbox is success once no associated managed resources remain. Failed creation attempts to clean up everything it provisioned. If cleanup cannot be confirmed, the error reports incomplete cleanup and any known resource IDs; the provider retains discovery information for remaining resources so the caller can reconcile them by labels or known IDs. A lost connection before IDs are returned is an unknown outcome, not proof that nothing was created.

Connection loss, a plugin crash, or `Error::Stale` does not establish whether an in-flight mutation took effect or whether its command stopped. The library never automatically replays a mutation whose outcome is uncertain. Restarting a plugin restores the transport only; reattaching a sandbox restores access only. Fabro or Petri decides whether to wait, inspect, stop, retry, replace, or fail its work using these primitives.

### The spec

`create` receives the environment's template merged with the caller's overrides, after the library has validated it and converted every size and duration, so you receive integers and never parse a string. Every field except `source` is optional; a field you do not support must be refused with `unsupported` naming it, never silently ignored.

```json
{
  "name": "petri-01jabc-l3",
  "source": {"image": "ghcr.io/lithoscomputer/ubuntu-24.04:slim-f8bbbfd81934"},
  "resources": {"cpu_cores": 2, "memory_mb": 4096, "disk_mb": 20480, "gpus": 0},
  "network": {"mode": "cidr_allow_list", "allow": ["10.0.0.0/8"]},
  "timers": {"auto_stop_ms": 3600000},
  "labels": {"fabro.run": "01JABC…", "petri.workspace": "01JABC…/build"},
  "env": {"CI": "1"},
  "working_directory": "/workspace",
  "user": "root",
  "services": {
    "postgres": {"image": "postgres:16", "env": {"POSTGRES_PASSWORD": "x"}, "ports": [5432],
                 "ready": {"command": ["pg_isready"], "interval_ms": 2000, "timeout_ms": 1000,
                           "retries": 30, "deadline_ms": 60000}}
  },
  "container": {"init": true, "capabilities_add": ["SYS_PTRACE"]},
  "provider_config": {}
}
```

- `source` is exactly one of `{"image": …}`, `{"dockerfile": …}` (the file's text), `{"snapshot": …}` (a provider-known id) or the bare string `"host_directory"`. Your descriptor's `sources` lists which you accept. A host directory has no image: the directory is `working_directory`, and `workspace_ownership` says whether it is `designated` (caller-owned, never removed by destroy; the default when a path is given) or `managed` (provider-owned, removed on confirmed destroy; a temporary directory when no path is given). Ownership is only valid with a host directory source.

`source` describes the workload the caller wants to execute. Sandbox `exec`, `working_directory`, ambient `env`, and file operations refer to that workload and its workspace, not an outer runner or helper container. A one-shot container uses its own requested image but shares the workload's workspace, network, and service reachability. Any outer runner, nested Docker setup, helper image, or backend-specific job recipe is the provider's responsibility; the caller supplies the same workload spec regardless of provider.

- `resources` are whole cores and MiB. Round up if your backend allocates in coarser units and report the result in `status`. A non-zero `gpus` you cannot provide is `unsupported`, never dropped.
- `network.mode` is `provider_default`, `allow_all`, `block`, `cidr_allow_list` or `domain_allow_list`; your descriptor's `network` lists which you honour. A mode you do not list is `unsupported`, except `provider_default`, which every provider honours whether listed or not, so a spec without `network` is accepted everywhere. The two allow-list modes carry their entries under one `allow` key, and an `allow` on any other mode is rejected at decode rather than dropped.
- `timers` are milliseconds. If you do not implement timers, refuse a non-empty `timers` with `unsupported`.
- `labels` and `env` are string maps and round-trip exactly (labels) or reach every process in the sandbox (env).
- `working_directory` and `user` are what commands run as and where; return the working directory you actually used in the create result.
- `services` is a workload beside the sandbox: named containers with an image, env, published ports, a registry credential name, and a readiness check executed inside the service container. The provider waits for readiness before `create` or `start` returns success. Every readiness check states its whole budget: `interval_ms`, a per-attempt `timeout_ms`, `retries` and an overall `deadline_ms`, all required and non-zero, so the bound is explicit rather than inferred; a timeout longer than the deadline, or an interval that leaves no room for a second attempt, is `invalid_spec`. Each service must be reachable from the sandbox workload and its one-shot containers; `status.services` reports the host and port used from that workload, because on Docker that is a network alias and in a pod it is localhost. Services follow the sandbox lifecycle and participate in its stop fence. Declare `services` in your capabilities or refuse a non-empty `services` with `unsupported`.
- `container` is the typed subset of container options a job can ask for: `init`, `privileged`, `capabilities_add`, `capabilities_drop`, `dns`, `extra_hosts`, `shm_size_mb`. Honour what you can; refuse the rest with `unsupported` naming the field.
- `provider_config` is yours, for the few settings with no provider-neutral meaning. Describe it with a `spec_schema` in your descriptor and the library validates overrides against it; otherwise it is passed through unread.

### The descriptor

```json
{
  "schemas": {
    "kind": "lima", "version": "0.3.0",
    "config_schema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": {"vm_type": {"type": "string"}}},
    "spec_schema": null,
    "secrets": []
  },
  "namespace": {"id": "lima:7f3a…", "scope": "machine"},
  "isolation": "vm",
  "durability": "machine",
  "access": "owner_process",
  "sources": ["image"],
  "locality": {"host_address": "192.168.5.2", "mounts_host_paths": true},
  "resources": ["cpu", "memory"],
  "network": ["allow_all"],
  "ports": {"exposable": "any"},
  "capabilities": {"lifecycle": [], "endpoints": [], "services": false, "containers": false,
                   "container_fields": [], "timers": ["auto_stop"]}
}
```

`schemas` is the credential-free part, and the only part the `--describe-schema` probe returns: `kind`, `version`, `config_schema` (a JSON Schema for the entry's `config`; use `oneOf` when you accept alternative forms), an optional `spec_schema` for the spec's `provider_config`, and `secrets`, the credential names you expect in your environment, so a catalog can check an entry names them all. None of it may need a backend.

`namespace.id` must be stable across restarts and must differ when the sandboxes would. The library refuses a descriptor without one. `isolation` is the workload's security boundary: `none`, `container` or `vm`. `access` is `owner_process` when only the live instance that created a sandbox can operate it, or `shared` when instances reaching the same namespace coordinate one stop fence. `capabilities.container_fields` lists exactly which `container` options you honour and `capabilities.timers` which timers you enforce, so a client knows in advance what you will refuse with `unsupported`. Every advertised list is a set; order and repetition carry no meaning. The protocol version is negotiated in `initialize`, not advertised here.

`durability` is a promise about restarts. If you declare `machine` or `remote`, `attach` and `list` must work after you are stopped and started again with the same entry, from whatever you chose to persist; the conformance suite kills and relaunches you to check. Where you keep that state is yours to decide. A `state_dir` key in your `config_schema` is the usual answer, since the catalog then carries it per entry and two entries never share a directory. `process` promises nothing across a restart and skips the check.

### Optional capabilities

Four groups of calls and fields that only some providers can offer. They are defined here, not by plugins: a provider declares which of them it implements in `capabilities`, and a client checks before relying on one. A plugin cannot add a fifth; features outside the contract are not reachable through the library, and a new group is added only when a second client and a second provider both need it.

- `lifecycle`: a list from `pause`, `resume`, `archive`, `recover`.
- `endpoints`: a list from `ssh` (`ssh(ttl_ms)`, `ssh_revoke(token)`), `shell_command`, `vnc`.
- `services`: `true` if `create` accepts the `services` and `container` fields of the spec.
- `containers`: `true` if you implement `run(sandbox, image | build, entrypoint, args, env, cwd)`, a one-shot container sharing the workload workspace, network, and service reachability, streamed like exec. `image` is a registry reference. `build` names a context directory in the workload workspace, resolved against the sandbox working directory, so the Dockerfile can copy files beside it; it also takes an optional Dockerfile path within the context, the tag the build produces, which is its cache key, and whether an image already carrying that tag may run without a build. Both forms are part of the capability. Unlike exec, a one-shot runs the image's own command: `entrypoint`, when given, replaces the image's entrypoint, and non-empty `args` replace the image's command, so arguments alone reach the image's entrypoint, as a GitHub Docker action expects. Its execution participates in the sandbox stop fence and its resources are removed when it finishes or the sandbox is destroyed.

Docker and Daytona implement all four. Host implements none: it runs commands as processes in a directory and has nothing to put a service in.

### Conformance

```sh
sandbox-driver conformance ./lima-plugin
```

Runs the contract suite against your binary: every required call, the error codes, stream ordering, cancellation, namespace stability across two launches, the stop/start fence against a long-running command, label round-trip with awkward values, restart recovery for `machine` and `remote` durability, and each optional capability you declare, including a `services` workload with a readiness check. The same suite runs in CI against the built-in providers and the fake. If it passes, every client of this library works with your plugin, with no change to the client.

The suite also verifies that exec and files address the requested workload rather than an outer runner; one-shot containers share its workspace and can reach its services; create and restart wait for readiness; and stop fences workload commands, services, and one-shot containers. Two provider instances must either participate in the same fence or reject conflicting mutations with `Busy`. Failure injection verifies that partial creation is cleaned up or reported as incomplete, and that transport loss or dropped callers never causes automatic replay or a false claim of confirmed termination.

### SDKs

`sandbox-driver-plugin` (Rust) gives you a trait to implement and a `serve()` that handles framing. For other languages the contract is small enough to implement directly; the Python example in `examples/plugins/` is under two hundred lines.

## Lifetimes

- `try_new` checks local provider-entry structure and freezes launch conveniences; it starts nothing and resolves no secrets. Entry errors are reported when that entry is selected.
- One backend is activated per immutable provider entry per catalog. Editing an environment touches no backend. Changing connection or launch settings requires a new catalog; shutdown explicitly retires the old catalog.
- Docker and Daytona run in the caller's process. Nothing is spawned for them, ever.
- Host is a plugin child like any other, installed separately on the execution machine. It keeps its process-group fence state in a temporary directory it owns, which lasts as long as its sandboxes do, and attempts bounded cleanup of them on stdin EOF; process loss alone is not proof that children died.
- A third-party plugin child starts on the first call to its entry, one per entry per process, and is supervised. On a crash it is restarted; handles from the old generation return `Error::Stale` and should be re-attached by ref.
- `shutdown()` ends supervised plugin children with bounded cleanup. Children also exit on stdin EOF. Neither shutdown nor a client or plugin crash proves that remote workload execution has stopped; sandbox durability and the explicit workload lifecycle guarantees determine what remains. Ending a client is not an implicit request to stop or destroy durable sandboxes.
- Sandboxes outlive the client according to the provider's declared durability, never more.
- A client made of several processes builds a `Sandboxes` in each one, and each supervises its own children. A parent hands a child process resolved provider entries and environments as data, never a settings file to re-read, with secrets passed as names for the child to resolve from the same store the parent used. Children of a worker end with the worker; sandboxes on durable providers are reattached by ref from whichever process needs them.

## Testing

```rust
let sandboxes = sandbox_driver_testing::contract::fake();
```

The fake lives in `sandbox-driver-testing`, which consumers add as a
dev-dependency. It implements the required in-memory lifecycle, identity, files,
and scripted streaming execution surface. Use `FakeProvider::script` to define
command responses and `FakeProvider::sandboxes` to retain access to its call log.
Unscripted commands fail; no command executes on the host. It advertises no
optional services, container, endpoint, lifecycle-extension or port capabilities.
The common conformance checks run against it; optional workload and provider
restart/failure scenarios remain integration gates, not claims made by the fake.

## CLI

`lithos-sandbox` is the command-line client of the library and uses the same catalog.

```
lithos-sandbox providers                     # provider entries from the catalog
lithos-sandbox provider describe daytona
lithos-sandbox provider health daytona
lithos-sandbox environments
lithos-sandbox create ci-linux --label job=1842
lithos-sandbox exec <ref> -- make test
lithos-sandbox shell <ref>
lithos-sandbox cp <ref>:/workspace/out ./out
lithos-sandbox find --label job=1842         # across every provider
lithos-sandbox destroy <ref>
lithos-sandbox plugin install ./lima-plugin
lithos-sandbox plugin list
sandbox-driver conformance ./lima-plugin
```

## Design rules

The rules the library holds itself and its clients to. Contributions are reviewed against them.

1. Clients never see a kind name except as data in a descriptor.
2. Every client-facing method is wire-expressible.
3. A provider reports its namespace, durability, and capabilities. The library trusts the descriptor and nothing else.
4. The required contract grows only when a second plugin and a second client both need the call. Vendor-only operations are extensions or stay out.
5. Built-ins get no special API. They implement the same internal trait a plugin is adapted to, and the conformance suite runs against them too.
6. The library owns config, discovery, trust, and supervision. Clients configure them.
7. Connection and trust settings live on provider entries; what to build lives on environments. Neither level repeats the other.
8. Bulk bytes never share a pipe with control messages. Streams get their own connections; stop and health are never queued behind data.

## Status

Protocol version 3. The crate is pre-1.0 and the public API may change before 1.0.

Prior art: Jupyter kernelspecs and `jupyter_client`, Kubernetes CRI and RuntimeClass, the OCI runtime spec, HashiCorp go-plugin, MCP servers.

### Current additive implementation

`contract::Sandboxes` requires an explicit `Registry`, `SecretResolver` and
`CatalogOptions`; no real provider is registered by core. `Registry::builtin`
registers an adapter, and `Registry::plugins` supplies the ordinary plugin
loader. Host cannot be registered as a built-in. Metadata discovery receives
only `PluginLocation`, never runtime configuration or secrets.

`builder`, `environment` and `environments` are async because their source may
be a database. A builder freezes the selected environment and provider binding.
Field setters override individual template values; map setters merge keys,
while `spec` explicitly replaces the whole template. Invalid size strings stay
as admission errors, even if a later setter supplies another value.

Schemas require draft-07, 2019-09 or 2020-12 and are compiled without external
retrieval, including when another dependency enables the engine's network
features. Config and spec options are checked separately. Error messages omit
instance values. Provider validation is still mandatory for opaque options.

Inventory defaults to eight concurrent entries and thirty seconds per entry,
configurable through `CatalogOptions`. Results and failures have deterministic
entry/identity ordering. Equal duplicate refs collapse; conflicting duplicate
refs or foreign refs invalidate that entry's batch. Different entry aliases
remain different refs. `unavailable` includes all failed entries and `failures`
retains the structured reason; neither implies absence or authorizes replay.

The common new-contract suite is
`sandbox_driver_conformance::contract::Conformance`. It accepts a catalog,
environment, and two command fixtures, and checks identity/inventory, binary
file ranges and preservation, streamed output, stop/start fencing and confirmed
cleanup. It reports its executed checks explicitly. Passing it alone is not
provider-ready certification for optional workloads, crash recovery or transport.
