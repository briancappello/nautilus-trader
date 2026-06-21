//! Factory for creating live MarketStore data clients (the `LiveNode` entry point).
//!
//! Mirrors the databento/tardis pattern: the factory + config are `#[pyclass]`es exposed
//! to Python; `LiveNode.builder().add_data_client(name, factory, config)` downcasts the
//! config and constructs the `DataClient`.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    common::MARKETSTORE, config::MarketStoreDataClientConfig, data::MarketStoreDataClient,
};

// Re-export so the python module can attach #[pymethods] to the same type.
pub use crate::config::MarketStoreDataClientConfig as Config;

/// Factory for creating MarketStore data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.marketstore",
        from_py_object
    )
)]
pub struct MarketStoreDataClientFactory;

impl MarketStoreDataClientFactory {
    /// Creates a new [`MarketStoreDataClientFactory`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for MarketStoreDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for MarketStoreDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let ms_config = config
            .as_any()
            .downcast_ref::<MarketStoreDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for MarketStoreDataClientFactory. \
                     Expected MarketStoreDataClientConfig, was {config:?}"
                )
            })?;

        let client_id = ClientId::from(name);
        let client = MarketStoreDataClient::new(client_id, ms_config.clone())?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        MARKETSTORE
    }

    fn config_type(&self) -> &'static str {
        "MarketStoreDataClientConfig"
    }
}

// Silence unused on non-python builds (Any is used by the trait bound path).
const _: fn() = || {
    fn _assert_any<T: Any>() {}
    _assert_any::<MarketStoreDataClientConfig>();
};
