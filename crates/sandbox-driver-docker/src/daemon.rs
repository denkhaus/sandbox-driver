//! Shared daemon utilities: the provider kind, the mapping of API errors
//! onto provider errors, status predicates, and quoting for the commands
//! that run under `/bin/sh`.

use std::result::Result as StdResult;

use bollard::errors::Error as DockerApiError;
use sandbox_driver::{Error, ProviderError, ProviderKind, Result};

/// The POSIX shell every Linux image provides. The exec wrapper, the stop
/// request, and the container's init command run under it; the user's
/// program never does.
pub(crate) const POSIX_SH: &str = "/bin/sh";

pub(crate) fn docker_kind() -> ProviderKind {
    ProviderKind::try_new("docker").expect("static kind is valid")
}

pub(crate) fn docker_error(context: &str, error: DockerApiError) -> Error {
    let kind = docker_kind();
    let status_code = match &error {
        DockerApiError::DockerResponseServerError { status_code, .. } => Some(*status_code),
        _ => None,
    };
    let mut provider = ProviderError::with_source(kind, context, error);
    if let Some(status_code) = status_code {
        provider.code = Some(status_code.to_string());
        provider.retryable = status_code >= 500;
    }
    Error::Provider(provider)
}

/// The ID of a container the daemon listed. A listing without one is a
/// complete but unusable answer, not an unknown outcome.
pub(crate) fn listed_id(id: Option<String>, what: &str) -> Result<String> {
    id.ok_or_else(|| {
        Error::Provider(ProviderError::new(
            docker_kind(),
            format!("daemon listed a {what} without an ID"),
        ))
    })
}

pub(crate) fn is_not_found(error: &DockerApiError) -> bool {
    matches!(error, DockerApiError::DockerResponseServerError {
        status_code: 404,
        ..
    })
}

pub(crate) fn is_not_modified(error: &DockerApiError) -> bool {
    matches!(error, DockerApiError::DockerResponseServerError {
        status_code: 304,
        ..
    })
}

pub(crate) fn is_conflict(error: &DockerApiError) -> bool {
    matches!(error, DockerApiError::DockerResponseServerError {
        status_code: 409,
        ..
    })
}

pub(crate) fn tolerate_not_modified(
    outcome: StdResult<(), DockerApiError>,
    context: &str,
) -> Result<()> {
    match outcome {
        Ok(()) => Ok(()),
        Err(error) if is_not_modified(&error) || is_not_found(&error) => Ok(()),
        Err(error) => Err(docker_error(context, error)),
    }
}

pub(crate) fn shell_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for c in value.chars() {
        if c == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(c);
        }
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn docker_mapping_preserves_sources_and_retry_metadata() {
        let error = docker_error(
            "listing containers",
            DockerApiError::DockerResponseServerError {
                status_code: 503,
                message:     "daemon unavailable".to_owned(),
            },
        );
        let Error::Provider(provider) = error else {
            panic!("expected a provider error");
        };
        assert_eq!(provider.message, "listing containers");
        assert_eq!(provider.code.as_deref(), Some("503"));
        assert!(provider.retryable);
        assert_eq!(
            provider.source().expect("provider source").to_string(),
            "Docker responded with status code 503: daemon unavailable"
        );
    }
}
