//! Python bindings for the MarketStore factory + config `#[pyclass]`es.

use pyo3::prelude::*;

use crate::{
    common::{DEFAULT_GRPC_ENDPOINT, DEFAULT_VENUE, DEFAULT_WS_ENDPOINT},
    config::{InstrumentSpec, MarketStoreDataClientConfig, ReplayConfig},
    factories::MarketStoreDataClientFactory,
};

#[pymethods]
impl MarketStoreDataClientConfig {
    /// Configuration for MarketStore data clients used with `LiveNode`.
    ///
    /// `instruments` is a list of `(symbol, price_precision, size_precision, lot_size)`
    /// tuples describing the equity universe to serve.
    #[new]
    #[pyo3(signature = (
        instruments,
        grpc_endpoint = DEFAULT_GRPC_ENDPOINT.to_string(),
        ws_endpoint = DEFAULT_WS_ENDPOINT.to_string(),
        venue = DEFAULT_VENUE.to_string(),
        price_precision = 2,
        size_precision = 0,
        replay = None,
    ))]
    fn py_new(
        instruments: Vec<(String, u8, u8, u64)>,
        grpc_endpoint: String,
        ws_endpoint: String,
        venue: String,
        price_precision: u8,
        size_precision: u8,
        replay: Option<(String, String, i64)>,
    ) -> PyResult<Self> {
        let specs = instruments
            .into_iter()
            .map(|(symbol, pp, sp, lot)| InstrumentSpec::new(symbol, pp, sp, lot))
            .collect();
        let replay = replay.map(|(start, end, step)| ReplayConfig { start, end, step });
        Ok(Self::new(
            grpc_endpoint,
            ws_endpoint,
            venue,
            specs,
            price_precision,
            size_precision,
            replay,
        ))
    }

    #[getter]
    #[pyo3(name = "grpc_endpoint")]
    fn py_grpc_endpoint(&self) -> &str {
        &self.grpc_endpoint
    }

    #[getter]
    #[pyo3(name = "ws_endpoint")]
    fn py_ws_endpoint(&self) -> &str {
        &self.ws_endpoint
    }

    #[getter]
    #[pyo3(name = "venue")]
    fn py_venue(&self) -> &str {
        &self.venue
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
impl MarketStoreDataClientFactory {
    /// Factory for creating MarketStore data clients.
    #[new]
    fn py_new() -> Self {
        Self
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        // MUST equal the name the factory extractor is registered under (see python/mod.rs)
        // and what `DataClientFactory::name` returns.
        crate::common::MARKETSTORE
    }
}
