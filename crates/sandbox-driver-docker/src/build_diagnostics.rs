//! Failure diagnostics for the image-build path.
//!
//! Fork feature (work#1253, branch `fabro`): a failed build used to
//! surface as `docker error: building image` — [`ProviderError`] carries
//! the bollard cause in its `source` field, but nothing rendered the
//! chain, and the daemon's streamed build output was dropped on the
//! floor. These helpers keep a bounded tail of the daemon's build
//! output and render the walked cause chain, so a build failure names
//! where the stream broke and what the builder last said.

use std::collections::VecDeque;
use std::error::Error as StdError;

use bollard::errors::Error as DockerApiError;

/// How many daemon output lines a failure keeps at most.
const TAIL_LINES: usize = 20;
/// How many characters of daemon output a failure keeps at most.
const TAIL_CHARS: usize = 4 * 1024;

/// The bounded tail of a build's streamed output.
pub(crate) struct BuildOutputTail {
    lines: VecDeque<String>,
    chars: usize,
}

impl BuildOutputTail {
    pub(crate) fn new() -> Self {
        Self {
            lines: VecDeque::new(),
            chars: 0,
        }
    }

    /// Records one streamed line, dropping oldest lines to stay within
    /// the bounds.
    pub(crate) fn record(&mut self, line: impl Into<String>) {
        let line = line.into();
        self.chars += line.chars().count() + 1;
        self.lines.push_back(line);
        while self.lines.len() > TAIL_LINES || (self.chars > TAIL_CHARS && self.lines.len() > 1) {
            let Some(dropped) = self.lines.pop_front() else {
                break;
            };
            self.chars = self.chars.saturating_sub(dropped.chars().count() + 1);
        }
    }

    /// Renders the kept lines, or `None` when nothing was recorded.
    pub(crate) fn render(&self) -> Option<String> {
        if self.lines.is_empty() {
            return None;
        }
        Some(
            self.lines
                .iter()
                .cloned()
                .collect::<Vec<String>>()
                .join("\n"),
        )
    }
}

/// Renders an error together with every link of its cause chain, each
/// separated by ` — caused by: `.
pub(crate) fn render_cause_chain(error: &dyn StdError) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(link) = source {
        rendered.push_str(" — caused by: ");
        rendered.push_str(&link.to_string());
        source = link.source();
    }
    rendered
}

/// Renders a docker API failure with its cause chain AND the stream
/// detail bollard hides: `DockerStreamError`'s `#[error(...)]` renders
/// a fixed label while the parse failure rides an unrendered field
/// (bollard 0.18.1), so the chain walk alone stays blind to it.
pub(crate) fn render_api_failure(error: &DockerApiError) -> String {
    let mut rendered = render_cause_chain(error);
    if let DockerApiError::DockerStreamError { error } = error {
        rendered.push_str(" — stream detail: ");
        rendered.push_str(error);
    }
    rendered
}

/// Appends the rendered tail, when one exists, to a build failure
/// message so the builder's last words travel with the verdict.
pub(crate) fn with_output_tail(mut message: String, tail: &BuildOutputTail) -> String {
    if let Some(rendered) = tail.render() {
        message.push_str("\nbuild output tail:\n");
        message.push_str(&rendered);
    }
    message
}

#[cfg(test)]
mod tests {
    use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

    use super::*;

    /// A one-link chain with a cause, for chain-walking assertions.
    struct LinkError {
        message: String,
        source:  Option<Box<dyn StdError + Send + Sync>>,
    }

    impl LinkError {
        fn new(message: &str, source: Option<Box<dyn StdError + Send + Sync>>) -> Self {
            Self {
                message: message.to_owned(),
                source,
            }
        }
    }

    impl Debug for LinkError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            f.write_str(&self.message)
        }
    }

    impl Display for LinkError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            f.write_str(&self.message)
        }
    }

    impl StdError for LinkError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            self.source
                .as_ref()
                .map(|source| source.as_ref() as &(dyn StdError + 'static))
        }
    }

    #[test]
    fn the_cause_chain_renders_every_link() {
        let bottom = LinkError::new("connection refused by the socket", None);
        let middle = LinkError::new("hyper closed the request body", Some(Box::new(bottom)));
        let top = LinkError::new("streaming the build output failed", Some(Box::new(middle)));
        let rendered = render_cause_chain(&top);
        assert!(
            rendered.contains("streaming the build output failed"),
            "the top link must render: {rendered}"
        );
        assert!(
            rendered.contains("hyper closed the request body"),
            "the middle link must render: {rendered}"
        );
        assert!(
            rendered.contains("connection refused by the socket"),
            "the bottom link must render: {rendered}"
        );
        assert!(
            rendered.contains("caused by"),
            "the links must be separated by causes: {rendered}"
        );
    }

    #[test]
    fn the_tail_keeps_only_the_last_lines() {
        let mut tail = BuildOutputTail::new();
        for index in 0..(TAIL_LINES * 2) {
            tail.record(format!("line-{index}"));
        }
        let rendered = tail.render().expect("the tail renders");
        assert!(
            rendered.contains(&format!("line-{}", TAIL_LINES * 2 - 1)),
            "the newest line must survive: {rendered}"
        );
        assert!(
            !rendered.contains("line-0\n") && !rendered.ends_with("line-0"),
            "the oldest lines must be dropped: {rendered}"
        );
    }

    #[test]
    fn an_empty_tail_renders_nothing() {
        assert!(BuildOutputTail::new().render().is_none());
    }

    #[test]
    fn a_stream_error_unfolds_its_hidden_detail() {
        let error = DockerApiError::DockerStreamError {
            error: "failed to decode the build frame: eof while parsing a value".to_owned(),
        };
        let rendered = render_api_failure(&error);
        assert!(
            rendered.contains("Docker stream error"),
            "the label must render: {rendered}"
        );
        assert!(
            rendered.contains("eof while parsing a value"),
            "the hidden detail must render: {rendered}"
        );
    }
}
