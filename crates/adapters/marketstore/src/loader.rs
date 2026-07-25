//! Backtest loader: gRPC query -> decode -> `Vec<Bar>`.
//!
//! This is the primary backtest data path (see `docs/marketstore_v2_rewrite_plan.md`).
//! The Python runner maps the returned `Vec<Bar>` to `Vec<Data>` and calls
//! `engine.add_data(..., sort=true)` for a deterministic run. The same
//! gRPC + decode code backs the live client's `request_bars`.

use anyhow::Result;
use nautilus_core::UnixNanos;
use nautilus_model::data::bar::{Bar, BarType, get_bar_interval_ns};
use nautilus_model::data::{quote::QuoteTick, trade::TradeTick};
use nautilus_model::identifiers::InstrumentId;

use crate::decode::{
    RawBars, RawQuotes, RawTrades, decode_bars, decode_quotes, decode_raw_bars,
    decode_raw_quotes, decode_raw_trades, decode_trades,
};
use crate::grpc::MarketStoreGrpcClient;
use crate::symbology::{bar_type_to_tbk, quote_tbk, trade_tbk};

/// Loads historical bars as primitive column vectors (boundary-safe).
///
/// This is the path used across the PyO3 boundary: Rust does the gRPC query and
/// columnar decode; the caller builds model objects with its own constructors.
/// See [`crate::decode::RawBars`].
///
/// # Errors
///
/// Returns an error if the TBK cannot be derived, the query fails, or decoding fails.
pub async fn load_raw_bars(
    client: &MarketStoreGrpcClient,
    bar_type: BarType,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
) -> Result<RawBars> {
    let tbk = bar_type_to_tbk(&bar_type)?;
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    let ts_init_delta_ns = get_bar_interval_ns(&bar_type).as_u64();
    decode_raw_bars(&dataset, ts_init_delta_ns)
}

/// Loads historical bars for a single `bar_type` over `[start, end]`.
///
/// `price_precision` / `size_precision` are applied to every decoded bar (sourced
/// from the instrument; MarketStore carries no precision metadata). `limit` caps
/// rows (`0` = unlimited). Returned bars are in MarketStore's natural ascending
/// epoch order.
///
/// # Errors
///
/// Returns an error if the TBK cannot be derived, the query fails, or decoding fails.
pub async fn load_bars(
    client: &MarketStoreGrpcClient,
    bar_type: BarType,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
) -> Result<Vec<Bar>> {
    let tbk = bar_type_to_tbk(&bar_type)?;
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    // MarketStore timestamps bars at open; shift ts_init to the bar close so the
    // backtest matching engine aligns market state correctly (no look-ahead).
    let ts_init_delta_ns = get_bar_interval_ns(&bar_type).as_u64();
    decode_bars(
        &dataset,
        bar_type,
        price_precision,
        size_precision,
        ts_init_delta_ns,
    )
}

/// Loads historical trades for `instrument_id` over `[start, end]` as `Vec<TradeTick>`.
///
/// Uses the same gRPC `Query` + columnar decode as the live `request_trades`. `limit`
/// caps rows (`0` = unlimited). Ticks are point-in-time (`ts_init = ts_event`).
///
/// # Errors
///
/// Returns an error if the query or decoding fails.
pub async fn load_trade_ticks(
    client: &MarketStoreGrpcClient,
    instrument_id: InstrumentId,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
) -> Result<Vec<TradeTick>> {
    let tbk = trade_tbk(&instrument_id);
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    decode_trades(&dataset, instrument_id, price_precision, size_precision)
}

/// Loads historical NBBO quotes as primitive column vectors (boundary-safe).
///
/// The Python-facing path (mirrors [`load_raw_bars`]): Rust queries + decodes, the
/// caller builds `QuoteTick`s with the wheel's constructor. See [`crate::decode::RawQuotes`].
///
/// # Errors
///
/// Returns an error if the query or decoding fails.
pub async fn load_raw_quotes(
    client: &MarketStoreGrpcClient,
    instrument_id: InstrumentId,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
) -> Result<RawQuotes> {
    let tbk = quote_tbk(&instrument_id);
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    decode_raw_quotes(&dataset)
}

/// Loads historical trades as primitive column vectors (boundary-safe).
///
/// See [`load_raw_quotes`] / [`crate::decode::RawTrades`].
///
/// # Errors
///
/// Returns an error if the query or decoding fails.
pub async fn load_raw_trades(
    client: &MarketStoreGrpcClient,
    instrument_id: InstrumentId,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
) -> Result<RawTrades> {
    let tbk = trade_tbk(&instrument_id);
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    decode_raw_trades(&dataset)
}

/// Loads historical quotes for `instrument_id` over `[start, end]` as `Vec<QuoteTick>`.
///
/// # Errors
///
/// Returns an error if the query or decoding fails.
pub async fn load_quote_ticks(
    client: &MarketStoreGrpcClient,
    instrument_id: InstrumentId,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
    limit: i32,
    price_precision: u8,
    size_precision: u8,
) -> Result<Vec<QuoteTick>> {
    let tbk = quote_tbk(&instrument_id);
    let dataset = client.query_bars(&tbk, start, end, limit).await?;
    decode_quotes(&dataset, instrument_id, price_precision, size_precision)
}
