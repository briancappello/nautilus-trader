//! Configuration for the live Alpaca execution client.
//!
//! `mode` selects paper vs live base URLs (and credentials, resolved Python-side from
//! `.env` — see the framework's `docs/milestone-5-alpaca-execution.md` §6). The pyclass
//! constructor lives in [`crate::python::factories`]; this struct is the plain Rust config
//! the factory downcasts via [`ClientConfig`].

use std::any::Any;

use nautilus_common::factories::ClientConfig;

use crate::common::{
    LIVE_API_BASE_URL, LIVE_WS_URL, PAPER_API_BASE_URL, PAPER_WS_URL,
};

/// Trading mode: paper (default) or live (real money).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AlpacaMode {
    /// Paper trading against `paper-api.alpaca.markets`.
    #[default]
    Paper,
    /// Live (real-money) trading against `api.alpaca.markets`.
    Live,
}

impl AlpacaMode {
    /// Parses a mode string (`"paper"` / `"live"`, case-insensitive).
    ///
    /// # Errors
    ///
    /// Returns an error for any other value.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "live" => Ok(Self::Live),
            other => anyhow::bail!("Invalid ALPACA_MODE '{other}', expected 'paper' or 'live'"),
        }
    }

    /// The REST base URL for this mode (overridable in the config).
    #[must_use]
    pub const fn default_api_base_url(self) -> &'static str {
        match self {
            Self::Paper => PAPER_API_BASE_URL,
            Self::Live => LIVE_API_BASE_URL,
        }
    }

    /// The trade-updates WebSocket URL for this mode (overridable in the config).
    #[must_use]
    pub const fn default_ws_url(self) -> &'static str {
        match self {
            Self::Paper => PAPER_WS_URL,
            Self::Live => LIVE_WS_URL,
        }
    }
}

/// Configuration for the live Alpaca [`crate::execution::AlpacaExecutionClient`].
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.alpaca",
        from_py_object
    )
)]
pub struct AlpacaExecClientConfig {
    /// Paper (default) or live.
    pub mode: AlpacaMode,
    /// API key id (resolved Python-side from `ALPACA_API_KEY_{PAPER,LIVE}`).
    pub api_key: Option<String>,
    /// API secret (resolved Python-side from `ALPACA_API_SECRET_{PAPER,LIVE}`).
    pub api_secret: Option<String>,
    /// Optional REST base-URL override (defaults from `mode`).
    pub api_base_url: Option<String>,
    /// Optional trade-updates WS URL override (defaults from `mode`).
    pub ws_url: Option<String>,
    /// Optional explicit account id (otherwise derived as `ALPACA-001`).
    pub account_id: Option<String>,
}

impl Default for AlpacaExecClientConfig {
    fn default() -> Self {
        Self {
            mode: AlpacaMode::Paper,
            api_key: None,
            api_secret: None,
            api_base_url: None,
            ws_url: None,
            account_id: None,
        }
    }
}

impl AlpacaExecClientConfig {
    /// Creates a new [`AlpacaExecClientConfig`].
    #[must_use]
    pub fn new(
        mode: AlpacaMode,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_base_url: Option<String>,
        ws_url: Option<String>,
        account_id: Option<String>,
    ) -> Self {
        Self {
            mode,
            api_key,
            api_secret,
            api_base_url,
            ws_url,
            account_id,
        }
    }

    /// The effective REST base URL (override or mode default).
    #[must_use]
    pub fn resolved_api_base_url(&self) -> &str {
        self.api_base_url
            .as_deref()
            .unwrap_or_else(|| self.mode.default_api_base_url())
    }

    /// The effective trade-updates WS URL (override or mode default).
    #[must_use]
    pub fn resolved_ws_url(&self) -> &str {
        self.ws_url
            .as_deref()
            .unwrap_or_else(|| self.mode.default_ws_url())
    }
}

impl ClientConfig for AlpacaExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
