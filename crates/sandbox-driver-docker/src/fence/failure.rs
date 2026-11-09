//! Ownership decisions travel alongside diagnostics, never inside them.

use std::error::Error as StdError;

use bollard::errors::Error as DockerApiError;
use sandbox_driver::{Error, ErrorReport, ProviderError, TransportError};
use serde_json::{Map, Value};

use crate::daemon::docker_kind;

/// Whether a failed operation has established that releasing its claim is safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Disposition {
    Release,
    Reconcile,
}

/// A private operation failure. Wrapping or combining its diagnostic cannot
/// erase the ownership decision made when the failure was observed.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(super) error:       Box<Error>,
    pub(super) disposition: Disposition,
}

pub(crate) type WorkResult<T> = Result<T, Failure>;

impl Failure {
    /// Termination was not established, even if all HTTP requests completed.
    pub(crate) fn reconcile(error: Error) -> Self {
        Self {
            error:       Box::new(error),
            disposition: Disposition::Reconcile,
        }
    }

    /// Combine ownership before rendering the two diagnostics. The renderer
    /// may choose either source without changing the claim's disposition.
    pub(crate) fn combine_with(
        self,
        other: Self,
        render: impl FnOnce(Error, Error) -> Error,
    ) -> Self {
        let disposition = if self.disposition == Disposition::Reconcile
            || other.disposition == Disposition::Reconcile
        {
            Disposition::Reconcile
        } else {
            Disposition::Release
        };
        Self {
            error: Box::new(render(*self.error, *other.error)),
            disposition,
        }
    }

    /// Preserve both failures, preferring a reconciliation diagnostic for
    /// callers that branch on the existing public error code.
    pub(crate) fn combine(self, other: Self) -> Self {
        let (first, second) = if self.disposition == Disposition::Release
            && other.disposition == Disposition::Reconcile
        {
            (other, self)
        } else {
            (self, other)
        };
        first.combine_with(second, |first, second| {
            let related = diagnostic(&second);
            let mut failure = match first {
                Error::Provider(failure) => failure,
                error => {
                    ProviderError::with_source(docker_kind(), "multiple operation failures", error)
                }
            };
            let mut detail = match failure.detail.take() {
                Some(Value::Object(detail)) => detail,
                Some(detail) => Map::from_iter([("original_detail".to_owned(), detail)]),
                None => Map::new(),
            };
            detail.insert("related_error".to_owned(), related);
            failure.detail = Some(Value::Object(detail));
            Error::Provider(failure)
        })
    }

    pub(crate) fn into_error(self) -> Error {
        *self.error
    }
}

/// Include structured provider detail as well as the shared report, so an
/// outer failure does not erase cleanup identities or earlier paired errors.
pub(super) fn diagnostic(error: &Error) -> Value {
    let mut report =
        serde_json::to_value(ErrorReport::from(error)).expect("error report serializes");
    if let Error::Provider(failure) = error {
        report["code"] = serde_json::json!(failure.code);
        report["detail"] = serde_json::json!(failure.detail);
    }
    report
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        let disposition = if outcome_unknown(&error) {
            Disposition::Reconcile
        } else {
            Disposition::Release
        };
        Self {
            error: Box::new(error),
            disposition,
        }
    }
}

/// Combine independently observed outcomes without short-circuiting either
/// failure. Successful observations do not erase a failure's disposition.
pub(crate) fn combine(first: WorkResult<()>, second: WorkResult<()>) -> WorkResult<()> {
    match (first, second) {
        (Err(first), Err(second)) => Err(first.combine(second)),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Classify an infrastructure error on entry to the fenced operation. An HTTP
/// response from a gateway is not proof of the upstream mutation's outcome.
pub(crate) fn outcome_unknown(error: &Error) -> bool {
    let mut cause: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = cause {
        if matches!(current.downcast_ref::<Error>(), Some(Error::Transport(_)))
            || current.downcast_ref::<TransportError>().is_some()
        {
            return true;
        }
        if let Some(error) = current.downcast_ref::<DockerApiError>() {
            return match error {
                DockerApiError::DockerResponseServerError { status_code, .. } => {
                    matches!(status_code, 502 | 504)
                }
                _ => true,
            };
        }
        cause = current.source();
    }
    false
}
