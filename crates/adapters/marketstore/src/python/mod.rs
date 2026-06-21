//! Python bindings from [PyO3](https://pyo3.rs).
//!
//! Exposes the MarketStore adapter under `nautilus_trader.core.nautilus_pyo3.marketstore`
//! (and, via the Python shim package, `nautilus_trader.adapters.marketstore`). Registers
//! the factory + config extractors with the global pyo3 registry so
//! `LiveNode.builder().add_data_client(...)` can route them.

#![allow(clippy::missing_errors_doc)]

pub mod factories;

#[cfg(feature = "live")]
use nautilus_common::factories::{ClientConfig, DataClientFactory};
#[cfg(feature = "live")]
use nautilus_core::python::to_pyvalue_err;
#[cfg(feature = "live")]
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

#[cfg(feature = "live")]
use crate::{
    common::MARKETSTORE,
    config::MarketStoreDataClientConfig,
    factories::MarketStoreDataClientFactory,
};

/// Returns the adapter package identifier (smoke value).
#[pyfunction]
fn marketstore_adapter_id() -> &'static str {
    "nautilus-marketstore"
}

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_marketstore_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<MarketStoreDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract MarketStoreDataClientFactory: {e}"
        ))),
    }
}

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_marketstore_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<MarketStoreDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract MarketStoreDataClientConfig: {e}"
        ))),
    }
}

/// The `marketstore` pyo3 submodule, registered into the wheel's pyo3 module
/// (`crates/pyo3/src/lib.rs`) as `nautilus_trader.core.nautilus_pyo3.marketstore`.
#[pymodule]
pub fn marketstore(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(marketstore_adapter_id, m)?)?;

    #[cfg(feature = "live")]
    {
        m.add_class::<MarketStoreDataClientConfig>()?;
        m.add_class::<MarketStoreDataClientFactory>()?;

        let registry = get_global_pyo3_registry();

        if let Err(e) = registry.register_factory_extractor(
            MARKETSTORE.to_string(),
            extract_marketstore_data_factory,
        ) {
            log::error!("Failed to register MarketStore factory extractor: {e}");
        }

        if let Err(e) = registry.register_config_extractor(
            "MarketStoreDataClientConfig".to_string(),
            extract_marketstore_data_config,
        ) {
            log::error!("Failed to register MarketStore config extractor: {e}");
        }
    }

    Ok(())
}
