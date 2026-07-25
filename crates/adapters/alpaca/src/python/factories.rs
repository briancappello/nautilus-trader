//! Python bindings for the Alpaca factory + config `#[pyclass]`es.

use nautilus_model::identifiers::{AccountId, TraderId};
use pyo3::prelude::*;

use crate::{
    common::ALPACA,
    config::{AlpacaExecClientConfig, AlpacaMode},
    factories::AlpacaExecutionClientFactory,
};

#[pymethods]
impl AlpacaExecClientConfig {
    /// Configuration for the Alpaca execution client used with `LiveNode`.
    ///
    /// `mode` is `"paper"` (default) or `"live"`; credentials are resolved Python-side from
    /// `.env` (`ALPACA_API_KEY_{PAPER,LIVE}` / `ALPACA_API_SECRET_{PAPER,LIVE}`).
    #[new]
    #[pyo3(signature = (
        mode = "paper".to_string(),
        api_key = None,
        api_secret = None,
        api_base_url = None,
        ws_url = None,
        account_id = None,
    ))]
    fn py_new(
        mode: String,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_base_url: Option<String>,
        ws_url: Option<String>,
        account_id: Option<String>,
    ) -> PyResult<Self> {
        let mode = AlpacaMode::parse(&mode)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self::new(
            mode, api_key, api_secret, api_base_url, ws_url, account_id,
        ))
    }

    #[getter]
    #[pyo3(name = "mode")]
    fn py_mode(&self) -> &'static str {
        match self.mode {
            AlpacaMode::Paper => "paper",
            AlpacaMode::Live => "live",
        }
    }

    #[getter]
    #[pyo3(name = "api_base_url")]
    fn py_api_base_url(&self) -> String {
        self.resolved_api_base_url().to_string()
    }

    #[getter]
    #[pyo3(name = "ws_url")]
    fn py_ws_url(&self) -> String {
        self.resolved_ws_url().to_string()
    }

    fn __repr__(&self) -> String {
        // Never leak secrets in the repr.
        format!(
            "AlpacaExecClientConfig(mode={}, api_base_url={})",
            self.py_mode(),
            self.resolved_api_base_url(),
        )
    }
}

#[pymethods]
impl AlpacaExecutionClientFactory {
    /// Factory for creating Alpaca execution clients.
    #[new]
    fn py_new(trader_id: TraderId, account_id: AccountId) -> Self {
        Self::new(trader_id, account_id)
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        // MUST equal the registered extractor key (src/python/mod.rs) and
        // `ExecutionClientFactory::name`.
        ALPACA
    }
}
