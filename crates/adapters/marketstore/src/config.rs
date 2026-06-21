//! Configuration for the live MarketStore data client.
//!
//! MarketStore carries no instrument metadata, so the universe + per-symbol precision
//! are described here and synthesized into `Equity` instruments on `connect` (see
//! [`crate::instruments`]). Mirrors the Python backtest conventions in
//! `trading/config/instruments.py` (USD, 2dp price / 0dp size for US equities).

use std::any::Any;

use nautilus_common::factories::ClientConfig;

/// A single equity to serve, with its display precision.
#[derive(Clone, Debug)]
pub struct InstrumentSpec {
    /// The ticker symbol (e.g. `"AAPL"`); the venue is the config's `venue`.
    pub symbol: String,
    /// Price display precision (US equities: 2).
    pub price_precision: u8,
    /// Size display precision (US equities: 0).
    pub size_precision: u8,
    /// Lot size (shares). Defaults to 1 when unset.
    pub lot_size: u64,
}

impl InstrumentSpec {
    /// Creates a new [`InstrumentSpec`].
    #[must_use]
    pub fn new(symbol: String, price_precision: u8, size_precision: u8, lot_size: u64) -> Self {
        Self {
            symbol,
            price_precision,
            size_precision,
            lot_size,
        }
    }
}

/// Configuration for the live MarketStore [`crate::data::MarketStoreDataClient`].
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.marketstore",
        from_py_object
    )
)]
pub struct MarketStoreDataClientConfig {
    /// The gRPC endpoint for historical `Query` (e.g. `"http://127.0.0.1:5995"`).
    pub grpc_endpoint: String,
    /// The synthetic venue baked into every `InstrumentId` (default `NASDAQ`).
    pub venue: String,
    /// The universe of equities to serve (instruments + subscribable symbols).
    pub instruments: Vec<InstrumentSpec>,
    /// Fallback price precision for symbols not in `instruments`.
    pub price_precision: u8,
    /// Fallback size precision for symbols not in `instruments`.
    pub size_precision: u8,
}

impl MarketStoreDataClientConfig {
    /// Creates a new [`MarketStoreDataClientConfig`].
    #[must_use]
    pub fn new(
        grpc_endpoint: String,
        venue: String,
        instruments: Vec<InstrumentSpec>,
        price_precision: u8,
        size_precision: u8,
    ) -> Self {
        Self {
            grpc_endpoint,
            venue,
            instruments,
            price_precision,
            size_precision,
        }
    }
}

impl ClientConfig for MarketStoreDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
