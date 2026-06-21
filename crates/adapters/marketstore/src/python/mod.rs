//! Python bindings from [PyO3](https://pyo3.rs).
//!
//! Exposes the MarketStore adapter under `nautilus_trader.adapters.marketstore`. In this
//! first (vendoring) cut the module is intentionally minimal — it proves the in-wheel build
//! wiring and the import path. The `MarketStoreDataClientFactory` / `MarketStoreDataClientConfig`
//! `#[pyclass]`es are added with the live `DataClient` (Phase 2 DataClient step).

use pyo3::prelude::*;

/// Returns the adapter package identifier (smoke value to prove the module loads).
#[pyfunction]
fn marketstore_adapter_id() -> &'static str {
    "nautilus-marketstore"
}

/// The `marketstore` pyo3 submodule, registered into the wheel's pyo3 module
/// (`crates/pyo3/src/lib.rs`) as `nautilus_trader.adapters.marketstore`.
#[pymodule]
pub fn marketstore(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(marketstore_adapter_id, m)?)?;
    Ok(())
}
