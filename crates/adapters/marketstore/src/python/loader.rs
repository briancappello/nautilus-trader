//! Historical (bulk) loader bindings.
//!
//! These return the **wheel's own** `Bar` / `QuoteTick` / `TradeTick` objects. Because this
//! crate is compiled into the nautilus wheel, the objects constructed here are the exact
//! types the engine expects — `engine.add_data(...)` accepts them directly.
//!
//! That is the whole point of exposing the loader from here rather than from a separate
//! extension module: a `#[pyclass]` built in a foreign cdylib is a *different* type object
//! (see the cross-cdylib boundary notes in the adapter docs), so an out-of-wheel loader has
//! to hand back primitive columns and make Python rebuild every bar. Loading in-wheel skips
//! that entirely — decode goes straight to model objects, and nothing crosses a boundary.
//!
//! The gRPC calls are async; each binding drives them on a short-lived current-thread
//! runtime and releases the GIL while blocking, so bulk loads do not stall other threads.

use std::str::FromStr;

use nautilus_model::{
    data::{
        bar::{Bar, BarType},
        quote::QuoteTick,
        trade::TradeTick,
    },
    identifiers::InstrumentId,
};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::{
    common::DEFAULT_GRPC_ENDPOINT,
    grpc::MarketStoreGrpcClient,
    loader::{load_bars, load_quote_ticks, load_trade_ticks},
};

/// Runs `f` on a temporary current-thread runtime, releasing the GIL while it blocks.
fn block_on<F, T>(py: Python<'_>, f: F) -> PyResult<T>
where
    F: std::future::Future<Output = PyResult<T>> + Send,
    T: Send,
{
    py.detach(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
        runtime.block_on(f)
    })
}

async fn connect(endpoint: String) -> PyResult<MarketStoreGrpcClient> {
    MarketStoreGrpcClient::connect(endpoint)
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("connect: {e:?}")))
}

/// Lists all symbols known to MarketStore (universe discovery).
///
/// Returns bare symbol names. `timeframe` optionally filters to symbols that have data
/// for that bucket (e.g. `"1Min"`); `None` returns everything.
#[pyfunction]
#[pyo3(name = "list_symbols")]
#[pyo3(signature = (timeframe=None, endpoint=None))]
pub fn py_list_symbols(
    py: Python<'_>,
    timeframe: Option<&str>,
    endpoint: Option<&str>,
) -> PyResult<Vec<String>> {
    let endpoint = endpoint.unwrap_or(DEFAULT_GRPC_ENDPOINT).to_string();
    let timeframe = timeframe.map(str::to_string);

    block_on(py, async move {
        let client = connect(endpoint).await?;
        client
            .list_symbols(timeframe.as_deref())
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("list_symbols: {e:?}")))
    })
}

/// Loads historical OHLCV bars as wheel `Bar` objects, in ascending epoch order.
///
/// `ts_event` is the bar open; `ts_init` is the bar close (open + interval), which is the
/// timestamp the matching engine uses so there is no look-ahead. `limit` of `0` is
/// unlimited. Precisions are applied to every decoded bar (MarketStore carries no
/// instrument metadata).
#[pyfunction]
#[pyo3(name = "load_bars")]
#[pyo3(signature = (bar_type, start_ns=None, end_ns=None, limit=0, price_precision=2, size_precision=0, endpoint=None))]
#[expect(clippy::too_many_arguments)]
pub fn py_load_bars(
    py: Python<'_>,
    bar_type: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
    endpoint: Option<&str>,
) -> PyResult<Vec<Bar>> {
    let bar_type = BarType::from_str(bar_type)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid bar_type: {e}")))?;
    let endpoint = endpoint.unwrap_or(DEFAULT_GRPC_ENDPOINT).to_string();

    block_on(py, async move {
        let client = connect(endpoint).await?;
        load_bars(
            &client,
            bar_type,
            start_ns.map(Into::into),
            end_ns.map(Into::into),
            limit,
            price_precision,
            size_precision,
        )
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("load_bars: {e:?}")))
    })
}

/// Loads historical NBBO quotes as wheel `QuoteTick` objects, in ascending epoch order.
#[pyfunction]
#[pyo3(name = "load_quote_ticks")]
#[pyo3(signature = (instrument_id, start_ns=None, end_ns=None, limit=0, price_precision=2, size_precision=0, endpoint=None))]
#[expect(clippy::too_many_arguments)]
pub fn py_load_quote_ticks(
    py: Python<'_>,
    instrument_id: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
    endpoint: Option<&str>,
) -> PyResult<Vec<QuoteTick>> {
    let instrument_id = InstrumentId::from_str(instrument_id)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid instrument_id: {e}")))?;
    let endpoint = endpoint.unwrap_or(DEFAULT_GRPC_ENDPOINT).to_string();

    block_on(py, async move {
        let client = connect(endpoint).await?;
        load_quote_ticks(
            &client,
            instrument_id,
            start_ns.map(Into::into),
            end_ns.map(Into::into),
            limit,
            price_precision,
            size_precision,
        )
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("load_quote_ticks: {e:?}")))
    })
}

/// Loads historical trades as wheel `TradeTick` objects, in ascending epoch order.
#[pyfunction]
#[pyo3(name = "load_trade_ticks")]
#[pyo3(signature = (instrument_id, start_ns=None, end_ns=None, limit=0, price_precision=2, size_precision=0, endpoint=None))]
#[expect(clippy::too_many_arguments)]
pub fn py_load_trade_ticks(
    py: Python<'_>,
    instrument_id: &str,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
    endpoint: Option<&str>,
) -> PyResult<Vec<TradeTick>> {
    let instrument_id = InstrumentId::from_str(instrument_id)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid instrument_id: {e}")))?;
    let endpoint = endpoint.unwrap_or(DEFAULT_GRPC_ENDPOINT).to_string();

    block_on(py, async move {
        let client = connect(endpoint).await?;
        load_trade_ticks(
            &client,
            instrument_id,
            start_ns.map(Into::into),
            end_ns.map(Into::into),
            limit,
            price_precision,
            size_precision,
        )
        .await
        .map_err(|e| PyRuntimeError::new_err(format!("load_trade_ticks: {e:?}")))
    })
}
