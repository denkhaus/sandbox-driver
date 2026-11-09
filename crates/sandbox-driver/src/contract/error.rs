use serde::{Deserialize, Serialize};

use super::{EnvironmentName, NamespaceId, ProviderName};

/// What is known about an unsuccessful mutation, never an automatic retry hint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    /// Admission was refused before any effect. Retrying is application policy.
    NotStarted,
    /// Effects, completion, termination, and cleanup have not been established.
    #[default]
    Unknown,
}

/// A known provider-managed resource requiring reconciliation.
/// `kind` also covers implementation resources such as helper networks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemainingResource {
    pub kind: String,
    pub id:   String,
}

/// Closed shared error codes. This is also the serializable error projection;
/// backend causes stay in provider diagnostics. Messages must be safe to
/// expose.
///
/// There is deliberately no blanket conversion from the legacy error: a
/// transport error or timeout cannot establish absence or successful fencing.
/// Unknown future codes must fail decoding, never become `NotFound`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum Error {
    #[error("provider entry {entry} is not configured")]
    NoProvider { entry: ProviderName },
    #[error("environment {name} is not configured")]
    NoEnvironment { name: EnvironmentName },
    #[error("invalid provider entry {entry} at {path}: {message}")]
    InvalidConfig {
        entry:   ProviderName,
        path:    String,
        message: String,
    },
    #[error("provider entry {entry} requires secret {name}")]
    MissingSecret { entry: ProviderName, name: String },
    /// Authoritative absence in the checked namespace, never failed contact.
    #[error("{resource} {id} was not found")]
    NotFound { resource: String, id: String },
    #[error("{name} already exists as {id}")]
    AlreadyExists { name: String, id: String },
    #[error("{id} is not owned by this consumer")]
    NotOwned { id: String },
    #[error("namespace mismatch: expected {expected}, observed {actual}")]
    NamespaceMismatch {
        expected: NamespaceId,
        actual:   NamespaceId,
    },
    #[error("provider unavailable: {message}")]
    Unavailable {
        message: String,
        #[serde(default)]
        outcome: MutationOutcome,
    },
    /// The backend rejected the configured credentials, for example because
    /// they are invalid or expired. This request was not admitted.
    #[error("credentials rejected: {message}")]
    Unauthenticated { message: String },
    /// The credentials are valid but do not permit this operation. This
    /// request was not admitted.
    #[error("permission denied: {message}")]
    PermissionDenied { message: String },
    /// A quota or rate limit refused this request before admission.
    /// `retry_after_ms` is the backend's hint when it gives one.
    #[error("resource exhausted: {message}")]
    ResourceExhausted {
        message:        String,
        #[serde(default)]
        retry_after_ms: Option<u64>,
    },
    /// This request was not admitted; another owner's work may still be live.
    #[error("sandbox busy: {message}")]
    Busy { message: String },
    #[error("unsupported capability: {capability}")]
    Unsupported { capability: String },
    #[error("workload unhealthy: {message}")]
    Unhealthy { message: String },
    #[error("handle belongs to an earlier provider generation")]
    Stale {
        #[serde(default)]
        outcome: MutationOutcome,
    },
    #[error("invalid spec at {path}: {message}")]
    InvalidSpec { path: String, message: String },
    /// An empty list means no IDs are known, not that cleanup succeeded.
    #[error("cleanup could not be confirmed: {message}")]
    IncompleteCleanup {
        message:   String,
        remaining: Vec<RemainingResource>,
    },
    #[error("workload termination could not be confirmed: {message}")]
    FenceLeaked { message: String },
    #[error("transport failed: {message}")]
    Transport {
        message: String,
        #[serde(default)]
        outcome: MutationOutcome,
    },
}

impl Error {
    pub(crate) fn invalid_spec(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidSpec {
            path:    path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn invalid_config(
        entry: &ProviderName,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::InvalidConfig {
            entry:   entry.clone(),
            path:    path.into(),
            message: message.into(),
        }
    }
}
