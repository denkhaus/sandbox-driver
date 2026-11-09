//! Scripted, in-memory doubles for testing code that consumes
//! `sandbox-driver`.
//!
//! A consumer's unit tests need a [`Sandbox`](sandbox_driver::Sandbox)
//! that answers exec with results the test wrote, keeps files in memory,
//! returns canned search results, counts lifecycle calls, and fails on
//! demand — without a real provider behind it. [`ScriptedSandbox`] is
//! that double; [`ScriptedProvider`] hands them out through the
//! [`SandboxProvider`](sandbox_driver::SandboxProvider) trait so
//! provider-level code (create, attach, list, ownership scoping) is
//! testable too.
//!
//! These are doubles, not simulations: exec runs nothing, so the derived
//! facets that build on Bash return whatever results the test scripted.
//! Tests that need real files and processes use the Host provider on a
//! temporary directory instead.

/// In-memory provider for the additive shared sandbox contract.
pub mod contract;

mod exec;
mod fs;
mod provider;
mod sandbox;
mod search;

pub use exec::{ScriptedExec, ScriptedStdioProcess};
pub use fs::MemoryFs;
pub use provider::ScriptedProvider;
pub use sandbox::ScriptedSandbox;
pub use search::ScriptedSearch;
