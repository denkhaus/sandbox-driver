//! Shared declarations for the next sandbox API.
//!
//! This module is additive: it does not change the existing crate-root traits,
//! specs, errors, or protocol v2. Providers must explicitly implement the
//! guarantees in `docs/shared-contract.md` before advertising this contract.
//! Serialization here defines common data, not a negotiated wire protocol.

mod admission;
mod catalog;
mod config;
mod descriptor;
mod error;
mod execution;
mod identity;
mod runtime;
mod schema;
mod status;
mod units;
mod workload;

pub use catalog::{
    CatalogDiagnostic, CatalogOptions, Found, ProviderFailure, ProviderHandle, Registry,
    SandboxBuilder, Sandboxes, StaticEnvironments,
};
pub use config::{Environment, EnvironmentSource, PluginTrust, ProviderEntry};
pub use descriptor::{
    AccessScope, CapabilitySet, ContainerField, Descriptor, Durability, EndpointCapability,
    JsonSchema, LifecycleCapability, Locality, Namespace, NamespaceScope, NetworkMode, PortSupport,
    ProviderSchemas, ResourceCapability, SourceKind, TimerCapability,
};
pub use error::{Error, MutationOutcome, RemainingResource};
pub use execution::{
    ExecControls, ExecOutcome, ExecSpec, OutputSink, OutputStream, TerminalSize, Termination,
};
pub use identity::SandboxRef;
pub use runtime::{
    Activation, ByteStream, Containers, Endpoints, Expose, Exposure, FileEntry, FileRange, Files,
    Health, LabelFilter, Lifecycle, OneShotImage, OneShotSpec, PluginLocation, Ports,
    ProviderFactory, ProviderRuntime, Sandbox, SandboxSummary, SecretResolver, SecretValues,
    WorkloadAdmission,
};
pub use status::{SandboxState, ServiceAddress, Status};
pub use units::{ParseUnitError, parse_duration_ms, parse_size_mib};
pub use workload::{
    ContainerSpec, NetworkPolicy, ReadinessCheck, Resources, SandboxSpec, ServiceSpec, Source,
    Timers,
};

pub use crate::id::{EnvironmentName, NamespaceId, ProviderName, Sha256Digest};
