//! Python bindings from [PyO3](https://pyo3.rs).
//!
//! Exposes the Alpaca adapter under `nautilus_trader.core.nautilus_pyo3.alpaca` (and, via the
//! Python shim package, `nautilus_trader.adapters.alpaca`). Registers the execution factory +
//! config extractors with the global pyo3 registry so
//! `LiveNode.builder().add_exec_client(...)` can route them.

#![allow(clippy::missing_errors_doc)]

pub mod factories;

#[cfg(feature = "live")]
use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
#[cfg(feature = "live")]
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
#[cfg(feature = "live")]
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

#[cfg(feature = "live")]
use crate::{
    common::ALPACA, config::AlpacaExecClientConfig, factories::AlpacaExecutionClientFactory,
};

/// Returns the adapter package identifier (smoke value).
#[pyfunction]
fn alpaca_adapter_id() -> &'static str {
    "nautilus-alpaca"
}

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_alpaca_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<AlpacaExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AlpacaExecutionClientFactory: {e}"
        ))),
    }
}

#[cfg(feature = "live")]
#[expect(clippy::needless_pass_by_value)]
fn extract_alpaca_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<AlpacaExecClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract AlpacaExecClientConfig: {e}"
        ))),
    }
}

/// The `alpaca` pyo3 submodule, registered into the wheel's pyo3 module
/// (`crates/pyo3/src/lib.rs`) as `nautilus_trader.core.nautilus_pyo3.alpaca`.
#[pymodule]
pub fn alpaca(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(alpaca_adapter_id, m)?)?;

    #[cfg(feature = "live")]
    {
        m.add(stringify!(ALPACA), ALPACA)?;
        m.add_class::<AlpacaExecClientConfig>()?;
        m.add_class::<AlpacaExecutionClientFactory>()?;

        let registry = get_global_pyo3_registry();

        if let Err(e) = registry
            .register_exec_factory_extractor(ALPACA.to_string(), extract_alpaca_exec_factory)
        {
            return Err(to_pyruntime_err(format!(
                "Failed to register Alpaca exec factory extractor: {e}"
            )));
        }

        if let Err(e) = registry.register_config_extractor(
            "AlpacaExecClientConfig".to_string(),
            extract_alpaca_exec_config,
        ) {
            return Err(to_pyruntime_err(format!(
                "Failed to register Alpaca exec config extractor: {e}"
            )));
        }
    }

    Ok(())
}
