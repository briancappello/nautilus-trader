//! Alpaca execution adapter for NautilusTrader v2 (in-wheel build).
//!
//! The live `ExecutionClient` must compile INTO the nautilus wheel (cross-cdylib boundary
//! — see the trading framework's `docs/milestone-5-alpaca-execution.md` §0), so this crate
//! is vendored into the fork's workspace and compiles against the fork's own nautilus crates
//! (HEAD, high-precision ON). It routes the strategy's `Order` outflow to the Alpaca Trading
//! API v2 instead of the backtest `SimulatedExchange`.
//!
//! **M5.0 is the skeleton**: the crate layout, an `impl ExecutionClient` satisfying the
//! trait's required accessors (order/connect methods on trait defaults), the
//! `ExecutionClientFactory`, the `AlpacaExecClientConfig` pyclass, and the pyo3 registry
//! registration so `add_exec_client("ALPACA", ...)` routes without `NotImplemented`. The
//! HTTP/WS clients, order behavior, the §5a validator, brackets, and reconciliation land in
//! M5.1–M5.5.

pub mod common;
pub mod config;

#[cfg(feature = "live")]
pub mod execution;
#[cfg(feature = "live")]
pub mod factories;
#[cfg(feature = "live")]
pub mod gatekeeper;
#[cfg(feature = "live")]
pub mod http;
#[cfg(feature = "live")]
pub mod rules;
#[cfg(feature = "live")]
pub mod session;
#[cfg(feature = "live")]
pub mod validator;
pub mod wash;
#[cfg(feature = "live")]
pub mod websocket;

pub use config::{AlpacaExecClientConfig, AlpacaMode};
#[cfg(feature = "live")]
pub use execution::AlpacaExecutionClient;
#[cfg(feature = "live")]
pub use factories::AlpacaExecutionClientFactory;

#[cfg(feature = "python")]
pub mod python;
