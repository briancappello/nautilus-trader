//! Shared constants for the Alpaca adapter.
//!
//! The `ALPACA` string is the single routing key: it is what
//! [`crate::factories::AlpacaExecutionClientFactory::name`] returns, the key the pyo3
//! factory extractor is registered under (`src/python/mod.rs`), and the venue/client
//! routing key used by `LiveNode.builder().add_exec_client(...)`. These three MUST agree.

use std::sync::LazyLock;

use nautilus_model::identifiers::Venue;

/// The Alpaca routing key (factory name / registered extractor key / venue).
pub const ALPACA: &str = "ALPACA";

/// The Alpaca venue identifier.
pub static ALPACA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::from(ALPACA));

/// Returns the Alpaca [`Venue`] (convenience accessor over [`ALPACA_VENUE`]).
#[must_use]
pub fn alpaca_venue() -> Venue {
    *ALPACA_VENUE
}

/// Paper-trading REST base URL (default; `ALPACA_MODE=paper`).
pub const PAPER_API_BASE_URL: &str = "https://paper-api.alpaca.markets";

/// Live (real-money) REST base URL (`ALPACA_MODE=live`).
pub const LIVE_API_BASE_URL: &str = "https://api.alpaca.markets";

/// The trade-updates WebSocket stream (paper). Live swaps the host.
pub const PAPER_WS_URL: &str = "wss://paper-api.alpaca.markets/stream";

/// The trade-updates WebSocket stream (live).
pub const LIVE_WS_URL: &str = "wss://api.alpaca.markets/stream";
