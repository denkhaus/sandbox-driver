use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::capabilities::Capability;
use crate::error::{Error, Result};
use crate::exec::{ExecControls, ExecStreamingResult};
use crate::sanitize::OutputSanitization;

/// Ephemeral containers run beside a sandbox, in its world.
///
/// A one-shot container shares the sandbox's workspace and its network
/// namespace, runs one command from an image of its own, and is gone when
/// that command ends. It is what a GitHub Actions `uses: docker://…` step
/// or a local Dockerfile action needs, and what a bare `docker run` on the
/// caller's machine cannot give a remote sandbox.
///
/// The contract mirrors [`crate::Exec::run_streaming`]: output reaches the
/// sink as it arrives, `term` and `kill` are raw signals to the container's
/// entrypoint, `spec.timeout` kills, and the result carries the exit code
/// and the honesty flags. Stdin is not offered. A provider's `stop` and
/// `delete` of the sandbox also end every one-shot container still running
/// for it, so a crashed caller leaves nothing behind that the sandbox's
/// own lifecycle does not reach.
///
/// Capability-gated on `one_shot`; [`crate::Sandbox::one_shot`] is `None`
/// where undeclared. Building from a Dockerfile in the workspace is the
/// separate `one_shot.build` capability.
#[async_trait]
pub trait OneShot: Send + Sync {
    /// Pulls or builds the image as needed, runs the container to
    /// completion, and removes it.
    async fn run(&self, spec: &OneShotSpec, controls: ExecControls) -> Result<ExecStreamingResult>;

    /// Evicts images whose reference carries `prefix`, keeping the
    /// `keep` newest by the daemon's creation time, and returns the
    /// evicted references. Fork feature (work#1440): the mint-time
    /// ceiling for cooked environment images — creation-time ordering
    /// on the daemon side bounds the population even across process
    /// restarts, which no in-process ledger can. Providers without
    /// image-listing access to their daemon return
    /// [`Error::Unsupported`]; callers treat that as a skipped sweep,
    /// never as a failure of the cook.
    async fn evict_lru_images(&self, prefix: &str, keep: usize) -> Result<Vec<String>> {
        let _ = (prefix, keep);
        Err(Error::unsupported(Capability::OneShot))
    }

    /// Whether the daemon already holds `reference`. Fork feature
    /// (work#1463): the environment-image cooks run one process per
    /// gear, so no in-process single-flight registry sees another
    /// process's cook — the daemon's image set is the shared truth. A
    /// presence hit lets a cook reuse the image without building.
    /// Providers without image-inspect access to their daemon return
    /// [`Error::Unsupported`]; callers treat that as "absent" and
    /// build (fail-open — a duplicate build, never a stuck cook).
    async fn image_present(&self, reference: &str) -> Result<bool> {
        let _ = reference;
        Err(Error::unsupported(Capability::OneShot))
    }

    /// The newest image reference carrying `prefix`, when the daemon
    /// holds one. Fork feature (work#1551): the environment cook's
    /// recipe digest runs `cargo chef prepare` in the newest existing
    /// cooked environment image — busybox (the cook's holder) has no
    /// cargo, while every cooked `petri-env-*` image carries cargo and
    /// cargo-chef (circular, but valid: prepare only parses manifests).
    /// A digest over the prepare output makes a source-only merge mint
    /// the SAME tag, which a whole-tree digest never did. Providers
    /// without image-listing access to their daemon return
    /// [`Error::Unsupported`]; the caller falls back to the whole-tree
    /// digest — a daemon with no cooked image has nothing to prepare in
    /// anyway (the bootstrap case).
    async fn newest_image(&self, prefix: &str) -> Result<Option<String>> {
        let _ = prefix;
        Err(Error::unsupported(Capability::OneShot))
    }
}

/// Where a one-shot container's image comes from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OneShotImage {
    /// A registry image, pulled when the provider's daemon lacks it.
    Registry { reference: String },
    /// Built from a Dockerfile under `context`, a path inside the sandbox
    /// resolved against its working directory. Capability-gated on
    /// `one_shot.build`.
    Build {
        context:    String,
        /// The Dockerfile within the context; `None` is the context's own
        /// `Dockerfile`.
        dockerfile: Option<String>,
        /// The tag the build produces, and the cache key.
        tag:        String,
        /// Whether an image already carrying `tag` may be reused without
        /// building.
        reuse:      bool,
    },
}

/// What one one-shot container runs.
///
/// `Debug` redacts the arguments and env values, as on [`crate::ExecSpec`].
#[derive(Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OneShotSpec {
    pub image:               OneShotImage,
    /// Override the image's entrypoint: one executable, arguments go in
    /// `args`.
    #[serde(default)]
    pub entrypoint:          Option<String>,
    /// Arguments to the entrypoint.
    #[serde(default)]
    pub args:                Vec<String>,
    #[serde(default)]
    pub env:                 BTreeMap<String, String>,
    /// The working directory inside the container, absolute. `None` is the
    /// sandbox's working directory, where the shared workspace is mounted.
    #[serde(default)]
    pub working_dir:         Option<String>,
    /// `None` waits forever; the provider kills at the deadline otherwise.
    #[serde(default)]
    pub timeout:             Option<Duration>,
    #[serde(default)]
    pub output_sanitization: OutputSanitization,
}

impl OneShotSpec {
    pub fn registry(reference: impl Into<String>) -> Self {
        Self::new(OneShotImage::Registry {
            reference: reference.into(),
        })
    }

    pub fn new(image: OneShotImage) -> Self {
        Self {
            image,
            entrypoint: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            working_dir: None,
            timeout: None,
            output_sanitization: OutputSanitization::Raw,
        }
    }

    #[must_use]
    pub fn entrypoint(mut self, entrypoint: impl Into<String>) -> Self {
        self.entrypoint = Some(entrypoint.into());
        self
    }

    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn working_dir(mut self, dir: impl Into<String>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl fmt::Debug for OneShotSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneShotSpec")
            .field("image", &self.image)
            .field("entrypoint", &self.entrypoint)
            .field("args", &"<redacted>")
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .field("working_dir", &self.working_dir)
            .field("timeout", &self.timeout)
            .field("output_sanitization", &self.output_sanitization)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_spec_debug_redacts_arguments_and_env_values() {
        let spec = OneShotSpec::registry("alpine:3.20")
            .args(["--token", "hunter2"])
            .env_var("API_TOKEN", "hunter2");
        let debug = format!("{spec:?}");
        assert!(!debug.contains("hunter2"), "debug: {debug}");
        assert!(debug.contains("API_TOKEN"), "keys stay visible: {debug}");
        assert!(
            debug.contains("alpine:3.20"),
            "the image stays visible: {debug}"
        );
    }

    #[test]
    fn one_shot_spec_round_trips_through_json() {
        let spec = OneShotSpec::new(OneShotImage::Build {
            context:    "action".to_owned(),
            dockerfile: None,
            tag:        "petri/action:abc".to_owned(),
            reuse:      true,
        })
        .working_dir("/workspace")
        .timeout(Duration::from_secs(5));
        let json = serde_json::to_string(&spec).expect("serializes");
        let back: OneShotSpec = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.image, spec.image);
        assert_eq!(back.working_dir.as_deref(), Some("/workspace"));
        assert_eq!(back.timeout, Some(Duration::from_secs(5)));
    }
}
