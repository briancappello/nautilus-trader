//! Columnar `NumpyMultiDataset` -> `Vec<Bar>` decode.
//!
//! MarketStore returns query results column-oriented: `NumpyDataset` carries
//! parallel `column_names`, `column_types` (numpy dtype strings like `"<i8"`,
//! `"<f4"`), and `column_data` (one little-endian byte blob per column, each
//! `length * sizeof(type)` bytes). For an `OHLCV` attrgroup the columns are
//! `Epoch` (i8 seconds), `Open/High/Low/Close` (f4), `Volume` (i8). Verified
//! against a live MarketStore (AAPL/1Min/OHLCV).
//!
//! We read raw bytes directly — no pandas, no Arrow. This is the performance
//! point of the Rust adapter.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use nautilus_core::UnixNanos;
use nautilus_model::data::bar::{Bar, BarType};
use nautilus_model::data::{quote::QuoteTick, trade::TradeTick};
use nautilus_model::enums::AggressorSide;
use nautilus_model::identifiers::{InstrumentId, TradeId};
use nautilus_model::types::{Price, Quantity};
use serde::Deserialize;

use crate::proto::NumpyMultiDataset;

const NANOS_PER_SEC: i64 = 1_000_000_000;

/// A single decoded column: name + raw little-endian bytes + numpy dtype string.
struct Column<'a> {
    name: &'a str,
    dtype: &'a str,
    data: &'a [u8],
}

/// Primitive, column-oriented decode of an OHLCV dataset.
///
/// This is the **boundary-safe** representation: plain `f64`/`u64` columns that
/// cross the PyO3 boundary as Python lists. Nautilus model objects (`Bar`,
/// `QuoteTick`) are NOT constructed here — they must be built with the consuming
/// process's own constructors, because a `#[pyclass]` compiled into our cdylib is
/// a *different* type object than the same class in the nautilus-trader wheel (type
/// identity is per-cdylib), so the wheel's engine rejects our objects. (Pins are now
/// unified, so this fails loud rather than silently zeroing fields under an ABI skew.)
/// Rust does the heavy decode; Python (using the wheel's types) builds the objects.
///
/// `ts_event` is the bar open epoch (ns); `ts_init = ts_event + ts_init_delta_ns`
/// (the bar close) so the matching engine aligns market state without look-ahead.
#[derive(Debug, Default, Clone)]
pub struct RawBars {
    pub ts_event: Vec<u64>,
    pub ts_init: Vec<u64>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub volume: Vec<f64>,
}

impl RawBars {
    pub fn len(&self) -> usize {
        self.ts_event.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ts_event.is_empty()
    }
}

/// Decodes an `OHLCV` `NumpyMultiDataset` into primitive column vectors.
///
/// See [`RawBars`] for why this returns primitives rather than `Bar` objects.
///
/// # Errors
///
/// Returns an error if required columns are missing, dtypes are unexpected, or
/// the byte buffers are inconsistent with the declared row count.
pub fn decode_raw_bars(
    dataset: &NumpyMultiDataset,
    ts_init_delta_ns: u64,
) -> Result<RawBars> {
    let ds = match &dataset.data {
        Some(ds) => ds,
        None => return Ok(RawBars::default()),
    };

    let n = ds.length as usize;
    if n == 0 {
        return Ok(RawBars::default());
    }

    check_columns_consistent(ds)?;
    let columns = build_columns(ds);
    let find = |want: &str| columns.iter().find(|c| c.name == want);

    let epoch = read_i64(find("Epoch").context("missing Epoch column")?, n)?;
    let open = read_f64(find("Open").context("missing Open column")?, n)?;
    let high = read_f64(find("High").context("missing High column")?, n)?;
    let low = read_f64(find("Low").context("missing Low column")?, n)?;
    let close = read_f64(find("Close").context("missing Close column")?, n)?;
    let volume = read_f64(find("Volume").context("missing Volume column")?, n)?;

    // Optional sub-second remainder column (not present in standard OHLCV).
    let nanos = match find("Nanoseconds") {
        Some(col) => Some(read_i64(col, n)?),
        None => None,
    };

    let mut ts_event = Vec::with_capacity(n);
    let mut ts_init = Vec::with_capacity(n);
    for i in 0..n {
        let frac = nanos.as_ref().map_or(0, |v| v[i]);
        let event = (epoch[i] * NANOS_PER_SEC + frac) as u64;
        ts_event.push(event);
        ts_init.push(event + ts_init_delta_ns);
    }

    Ok(RawBars {
        ts_event,
        ts_init,
        open,
        high,
        low,
        close,
        volume,
    })
}

/// Decodes an `OHLCV` `NumpyMultiDataset` into `Vec<Bar>` for in-process Rust use.
///
/// NOTE: do not pass the returned `Bar`s across the PyO3 boundary to the wheel's
/// engine — see [`RawBars`]. This exists for pure-Rust consumers and tests.
///
/// # Errors
///
/// Returns an error if decoding fails (see [`decode_raw_bars`]).
pub fn decode_bars(
    dataset: &NumpyMultiDataset,
    bar_type: BarType,
    price_precision: u8,
    size_precision: u8,
    ts_init_delta_ns: u64,
) -> Result<Vec<Bar>> {
    let raw = decode_raw_bars(dataset, ts_init_delta_ns)?;
    let mut bars = Vec::with_capacity(raw.len());
    for i in 0..raw.len() {
        bars.push(Bar::new(
            bar_type,
            Price::new(raw.open[i], price_precision),
            Price::new(raw.high[i], price_precision),
            Price::new(raw.low[i], price_precision),
            Price::new(raw.close[i], price_precision),
            Quantity::new(raw.volume[i], size_precision),
            UnixNanos::from(raw.ts_event[i]),
            UnixNanos::from(raw.ts_init[i]),
        ));
    }
    Ok(bars)
}

// ---------------------------------------------------------------------------
// TRADE decode (columnar + scalar)
// ---------------------------------------------------------------------------
//
// MarketStore `TRADE` attrgroup (see docs/marketstore_v2_rewrite_plan.md §3.1):
// on-disk columns `Epoch` (i64 sec), `Nanoseconds` (i32), `Price` (f64), `Size` (u64),
// and `TradeID` (string, once the massive writer is widened). No `Side` is stored, so
// `aggressor_side = NoAggressor`. `TradeId` falls back to the epoch-ns string when the
// `TradeID` column is absent. Trades are point-in-time: `ts_init = ts_event` (no shift).

/// Boundary-safe trade columns. Same rationale as [`RawBars`]: `TradeTick`s are
/// built Python-side. `trade_id` is carried as a string column (synthesized from
/// the timestamp when the `TradeID` column is absent). Point-in-time: `ts_init =
/// ts_event`.
#[derive(Debug, Default, Clone)]
pub struct RawTrades {
    pub ts_event: Vec<u64>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub trade_id: Vec<String>,
}

impl RawTrades {
    #[must_use]
    pub fn len(&self) -> usize {
        self.ts_event.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ts_event.is_empty()
    }
}

/// Decodes a `TRADE` `NumpyMultiDataset` into primitive column vectors.
///
/// See [`RawTrades`] for why this returns primitives rather than `TradeTick`s.
///
/// # Errors
///
/// Returns an error if required columns are missing or dtypes/lengths are inconsistent.
pub fn decode_raw_trades(dataset: &NumpyMultiDataset) -> Result<RawTrades> {
    let ds = match &dataset.data {
        Some(ds) => ds,
        None => return Ok(RawTrades::default()),
    };
    let n = ds.length as usize;
    if n == 0 {
        return Ok(RawTrades::default());
    }
    check_columns_consistent(ds)?;
    let columns = build_columns(ds);
    let find = |want: &str| columns.iter().find(|c| c.name == want);

    let epoch = read_i64(find("Epoch").context("missing Epoch column")?, n)?;
    let nanos = match find("Nanoseconds") {
        Some(col) => Some(read_i64(col, n)?),
        None => None,
    };
    let price = read_f64(find("Price").context("missing Price column")?, n)?;
    let size = read_f64(find("Size").context("missing Size column")?, n)?;
    // TradeID is a string column; decoded only if present (writer-widening pending).
    let trade_ids = match find("TradeID") {
        Some(col) => Some(read_strings(col, n)?),
        None => None,
    };

    let mut ts_event = Vec::with_capacity(n);
    let mut trade_id = Vec::with_capacity(n);
    for i in 0..n {
        let frac = nanos.as_ref().map_or(0, |v| v[i]);
        let event = (epoch[i] * NANOS_PER_SEC + frac) as u64;
        ts_event.push(event);
        trade_id.push(match &trade_ids {
            Some(ids) => ids[i].clone(),
            None => synth_trade_id(event, i),
        });
    }

    Ok(RawTrades {
        ts_event,
        price,
        size,
        trade_id,
    })
}

/// Decodes a `TRADE` `NumpyMultiDataset` into `Vec<TradeTick>` for in-process Rust use.
///
/// NOTE: do not pass the returned ticks across the PyO3 boundary to the wheel's engine
/// (see [`RawBars`]). For pure-Rust consumers (the in-wheel adapter, tests, the loader).
///
/// # Errors
///
/// Returns an error if required columns are missing or dtypes/lengths are inconsistent.
pub fn decode_trades(
    dataset: &NumpyMultiDataset,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> Result<Vec<TradeTick>> {
    let raw = decode_raw_trades(dataset)?;
    let mut ticks = Vec::with_capacity(raw.len());
    for i in 0..raw.len() {
        ticks.push(TradeTick::new(
            instrument_id,
            Price::new(raw.price[i], price_precision),
            Quantity::new(raw.size[i], size_precision),
            AggressorSide::NoAggressor,
            TradeId::new(&raw.trade_id[i]),
            UnixNanos::from(raw.ts_event[i]),
            UnixNanos::from(raw.ts_event[i]),
        ));
    }
    Ok(ticks)
}

/// Builds a `TradeTick` from a single decoded WS `TRADE` row.
///
/// # Errors
///
/// Returns an error if a required field is missing.
pub fn trade_from_ws_row(
    row: &HashMap<String, Value>,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> Result<TradeTick> {
    let num = |k: &str| {
        ws_field(row, k)
            .and_then(Value::as_num)
            .with_context(|| format!("missing/invalid {k} in WS trade row"))
    };
    let epoch = num("Epoch")?.as_i64();
    let frac = ws_field(row, "Nanoseconds").and_then(Value::as_num).map_or(0, |v| v.as_i64());
    let ts_event = (epoch * NANOS_PER_SEC + frac) as u64;
    let trade_id = match ws_field(row, "TradeID").and_then(Value::as_str) {
        Some(s) => TradeId::new(s),
        None => TradeId::new(synth_trade_id(ts_event, 0)),
    };
    Ok(TradeTick::new(
        instrument_id,
        Price::new(num("Price")?.as_f64(), price_precision),
        Quantity::new(num("Size")?.as_f64(), size_precision),
        AggressorSide::NoAggressor,
        trade_id,
        UnixNanos::from(ts_event),
        UnixNanos::from(ts_event),
    ))
}

/// Synthesizes a `TradeId` from the event timestamp (+ a row disambiguator) when the
/// `TradeID` column is not (yet) persisted. Stays within `TradeId`'s 36-char limit.
fn synth_trade_id(ts_event: u64, seq: usize) -> String {
    format!("{ts_event}-{seq}")
}

// ---------------------------------------------------------------------------
// QUOTE decode (columnar + scalar)
// ---------------------------------------------------------------------------
//
// MarketStore `QUOTE` attrgroup: `Epoch` (i64 sec), `Nanoseconds` (i32), `BidPrice`/
// `AskPrice` (f64), `BidSize`/`AskSize` (u64). Complete for `QuoteTick`. Point-in-time:
// `ts_init = ts_event`.

/// Boundary-safe NBBO quote columns. Same rationale as [`RawBars`]: `QuoteTick`
/// objects must be built on the Python side with the wheel's constructor, so the
/// PyO3 boundary carries these primitive columns instead. Quotes are point-in-time
/// (`ts_init = ts_event`), so only one timestamp column is needed.
#[derive(Debug, Default, Clone)]
pub struct RawQuotes {
    pub ts_event: Vec<u64>,
    pub bid_price: Vec<f64>,
    pub ask_price: Vec<f64>,
    pub bid_size: Vec<f64>,
    pub ask_size: Vec<f64>,
}

impl RawQuotes {
    #[must_use]
    pub fn len(&self) -> usize {
        self.ts_event.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ts_event.is_empty()
    }
}

/// Decodes a `QUOTE` `NumpyMultiDataset` into primitive NBBO column vectors.
///
/// See [`RawQuotes`] for why this returns primitives rather than `QuoteTick`s.
///
/// # Errors
///
/// Returns an error if required columns are missing or dtypes/lengths are inconsistent.
pub fn decode_raw_quotes(dataset: &NumpyMultiDataset) -> Result<RawQuotes> {
    let ds = match &dataset.data {
        Some(ds) => ds,
        None => return Ok(RawQuotes::default()),
    };
    let n = ds.length as usize;
    if n == 0 {
        return Ok(RawQuotes::default());
    }
    check_columns_consistent(ds)?;
    let columns = build_columns(ds);
    let find = |want: &str| columns.iter().find(|c| c.name == want);

    let epoch = read_i64(find("Epoch").context("missing Epoch column")?, n)?;
    let nanos = match find("Nanoseconds") {
        Some(col) => Some(read_i64(col, n)?),
        None => None,
    };
    let bid_price = read_f64(find("BidPrice").context("missing BidPrice column")?, n)?;
    let ask_price = read_f64(find("AskPrice").context("missing AskPrice column")?, n)?;
    let bid_size = read_f64(find("BidSize").context("missing BidSize column")?, n)?;
    let ask_size = read_f64(find("AskSize").context("missing AskSize column")?, n)?;

    let mut ts_event = Vec::with_capacity(n);
    for i in 0..n {
        let frac = nanos.as_ref().map_or(0, |v| v[i]);
        ts_event.push((epoch[i] * NANOS_PER_SEC + frac) as u64);
    }

    Ok(RawQuotes {
        ts_event,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
    })
}

/// Decodes a `QUOTE` `NumpyMultiDataset` into `Vec<QuoteTick>` for in-process Rust use.
///
/// NOTE: do not pass the returned ticks across the PyO3 boundary (see [`RawBars`]).
///
/// # Errors
///
/// Returns an error if required columns are missing or dtypes/lengths are inconsistent.
pub fn decode_quotes(
    dataset: &NumpyMultiDataset,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> Result<Vec<QuoteTick>> {
    let raw = decode_raw_quotes(dataset)?;
    let mut ticks = Vec::with_capacity(raw.len());
    for i in 0..raw.len() {
        ticks.push(QuoteTick::new(
            instrument_id,
            Price::new(raw.bid_price[i], price_precision),
            Price::new(raw.ask_price[i], price_precision),
            Quantity::new(raw.bid_size[i], size_precision),
            Quantity::new(raw.ask_size[i], size_precision),
            UnixNanos::from(raw.ts_event[i]),
            UnixNanos::from(raw.ts_event[i]),
        ));
    }
    Ok(ticks)
}

/// Builds a `QuoteTick` from a single decoded WS `QUOTE` row.
///
/// # Errors
///
/// Returns an error if a required field is missing.
pub fn quote_from_ws_row(
    row: &HashMap<String, Value>,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> Result<QuoteTick> {
    let num = |k: &str| {
        ws_field(row, k)
            .and_then(Value::as_num)
            .with_context(|| format!("missing/invalid {k} in WS quote row"))
    };
    let epoch = num("Epoch")?.as_i64();
    let frac = ws_field(row, "Nanoseconds").and_then(Value::as_num).map_or(0, |v| v.as_i64());
    let ts_event = (epoch * NANOS_PER_SEC + frac) as u64;
    Ok(QuoteTick::new(
        instrument_id,
        Price::new(num("BidPrice")?.as_f64(), price_precision),
        Price::new(num("AskPrice")?.as_f64(), price_precision),
        Quantity::new(num("BidSize")?.as_f64(), size_precision),
        Quantity::new(num("AskSize")?.as_f64(), size_precision),
        UnixNanos::from(ts_event),
        UnixNanos::from(ts_event),
    ))
}

// ---------------------------------------------------------------------------
// WebSocket scalar-row decode (live streaming)
// ---------------------------------------------------------------------------

/// A numeric msgpack scalar that may arrive as an integer or a float.
#[derive(Debug, Clone, Copy)]
pub struct Num(f64);

impl Num {
    fn as_f64(self) -> f64 {
        self.0
    }
    fn as_i64(self) -> i64 {
        self.0 as i64
    }
}

/// A single msgpack scalar from a WS `data` map: a number, a string, or a nested map.
///
/// MarketStore sends `Epoch` as an int, prices/sizes as float-or-int (per the stored
/// dtype), string columns such as `TradeID` as strings, and — in the live envelope —
/// the `payload` value as a nested map; this accepts any of them. (`serde(untagged)`
/// tries variants in order, so `Map` must precede scalar variants would be ambiguous —
/// it isn't here, msgpack maps only match `Map`.)
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Map(HashMap<String, Value>),
}

impl Value {
    /// Returns the value as a [`Num`] if it is numeric.
    pub fn as_num(&self) -> Option<Num> {
        match self {
            Value::Int(v) => Some(Num(*v as f64)),
            Value::Float(v) => Some(Num(*v)),
            _ => None,
        }
    }
    /// Returns the value as a string slice if it is a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// A single live data frame from the MarketStore `/ws` stream.
///
/// Wire format (msgpack, binary frame): `{ "key": "<TBK>", "data": <envelope> }`, where
/// `key` is the TBK (e.g. `AAPL/1Min/OHLCV`). The live server wraps the row in a typed
/// envelope — `data = { "msg_type": "bar"|"trade"|"quote", "payload": { <col>: <scalar> } }`
/// — with **lowercase** column keys (`open`, `close`, `epoch`, `volume`, …). Older/other
/// paths may send the flat form `data = { <Col>: <scalar> }` with capitalized keys. We
/// accept both: [`StreamPayload::row`] unwraps the `payload` envelope when present, and the
/// `*_from_ws_row` decoders look columns up case-insensitively.
#[derive(Debug, Clone, Deserialize)]
pub struct StreamPayload {
    pub key: String,
    pub data: HashMap<String, Value>,
}

impl StreamPayload {
    /// The flat column row: the inner `payload` map when the frame is enveloped
    /// (`{msg_type, payload}`), otherwise `data` itself. Returned as a reference so no
    /// allocation happens on the common (enveloped) path's borrow.
    #[must_use]
    pub fn row(&self) -> &HashMap<String, Value> {
        match self.data.get("payload") {
            Some(Value::Map(inner)) => inner,
            _ => &self.data,
        }
    }
}

/// The first WS reply may be an error map: `{ "error": "<msg>" }`.
#[derive(Debug, Clone, Deserialize)]
pub struct StreamError {
    pub error: String,
}

/// Looks up a WS-row column case-insensitively.
///
/// The historical/gRPC columns are capitalized (`Open`, `Epoch`, `BidPrice`), but the live
/// `/ws` envelope uses lowercase (`open`, `epoch`, `bidprice`). This tries the exact key
/// first (fast path), then falls back to an ASCII-case-insensitive scan, so one decoder
/// serves both wire dialects.
fn ws_field<'a>(row: &'a HashMap<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(v) = row.get(key) {
        return Some(v);
    }
    row.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v)
}

/// Builds a `Bar` from a single decoded WS `OHLCV` row.
///
/// `bar_type` is supplied by the caller (resolved from `key` via the active-subscription
/// map — MarketStore TBKs carry no venue). `ts_event` = `Epoch` (open); `ts_init =
/// ts_event + ts_init_delta_ns` (close), matching the historical/backtest convention so
/// backtest = live at the data layer.
///
/// # Errors
///
/// Returns an error if any required OHLCV field is missing from the row.
pub fn bar_from_ws_row(
    row: &HashMap<String, Value>,
    bar_type: BarType,
    price_precision: u8,
    size_precision: u8,
    ts_init_delta_ns: u64,
) -> Result<Bar> {
    let get = |k: &str| {
        ws_field(row, k)
            .and_then(Value::as_num)
            .with_context(|| format!("missing/invalid {k} in WS row"))
    };

    let epoch = get("Epoch")?.as_i64();
    let frac = ws_field(row, "Nanoseconds").and_then(Value::as_num).map_or(0, |v| v.as_i64());
    let ts_event = (epoch * NANOS_PER_SEC + frac) as u64;

    Ok(Bar::new(
        bar_type,
        Price::new(get("Open")?.as_f64(), price_precision),
        Price::new(get("High")?.as_f64(), price_precision),
        Price::new(get("Low")?.as_f64(), price_precision),
        Price::new(get("Close")?.as_f64(), price_precision),
        Quantity::new(get("Volume")?.as_f64(), size_precision),
        UnixNanos::from(ts_event),
        UnixNanos::from(ts_event + ts_init_delta_ns),
    ))
}

/// Validates that a `NumpyDataset`'s parallel column arrays are length-consistent.
fn check_columns_consistent(ds: &crate::proto::NumpyDataset) -> Result<()> {
    if ds.column_names.len() != ds.column_types.len()
        || ds.column_names.len() != ds.column_data.len()
    {
        bail!(
            "inconsistent NumpyDataset: {} names, {} types, {} data columns",
            ds.column_names.len(),
            ds.column_types.len(),
            ds.column_data.len(),
        );
    }
    Ok(())
}

/// Builds the borrowed `Column` views over a `NumpyDataset`'s parallel arrays.
fn build_columns(ds: &crate::proto::NumpyDataset) -> Vec<Column<'_>> {
    ds.column_names
        .iter()
        .zip(&ds.column_types)
        .zip(&ds.column_data)
        .map(|((name, dtype), data)| Column {
            name: name.as_str(),
            dtype: dtype.as_str(),
            data: data.as_slice(),
        })
        .collect()
}

/// Reads a fixed-width `string16` column (MarketStore's `[16]rune` = 64 bytes/elem,
/// little-endian UTF-32) into owned `String`s, trimming trailing NULs.
fn read_strings(col: &Column, n: usize) -> Result<Vec<String>> {
    const ELEM: usize = 64; // 16 runes * 4 bytes
    let normalized = normalize_dtype(col.dtype);
    if normalized != "string16" && !normalized.starts_with("u16") && !normalized.starts_with("s") {
        bail!("column {} has unexpected string dtype {}", col.name, col.dtype);
    }
    let expected = n * ELEM;
    if col.data.len() != expected {
        bail!(
            "string column {} expected {expected} bytes ({n} x {ELEM}), got {}",
            col.name,
            col.data.len(),
        );
    }
    let mut out = Vec::with_capacity(n);
    for chunk in col.data.chunks_exact(ELEM) {
        let mut s = String::with_capacity(16);
        for rune_bytes in chunk.chunks_exact(4) {
            let cp = u32::from_le_bytes([rune_bytes[0], rune_bytes[1], rune_bytes[2], rune_bytes[3]]);
            if cp == 0 {
                break;
            }
            if let Some(c) = char::from_u32(cp) {
                s.push(c);
            }
        }
        out.push(s);
    }
    Ok(out)
}

/// Reads a column as `i64`, accepting numpy int dtypes (`i8`/`i4`).
fn read_i64(col: &Column, n: usize) -> Result<Vec<i64>> {
    let normalized = normalize_dtype(col.dtype);
    match normalized.as_str() {
        "i8" => read_le::<8, _>(col, n, i64::from_le_bytes),
        "i4" => read_le::<4, _>(col, n, |b| i32::from_le_bytes(b) as i64),
        other => bail!("column {} has unexpected int dtype {other}", col.name),
    }
}

/// Reads a column as `f64`, accepting numpy float dtypes (`f4`/`f8`) and ints.
fn read_f64(col: &Column, n: usize) -> Result<Vec<f64>> {
    let normalized = normalize_dtype(col.dtype);
    match normalized.as_str() {
        "f4" => read_le::<4, _>(col, n, |b| f32::from_le_bytes(b) as f64),
        "f8" => read_le::<8, _>(col, n, f64::from_le_bytes),
        // Volume arrives as i8 in standard OHLCV; widen to f64 for Quantity.
        "i8" => read_le::<8, _>(col, n, |b| i64::from_le_bytes(b) as f64),
        "i4" => read_le::<4, _>(col, n, |b| i32::from_le_bytes(b) as f64),
        // TRADE/QUOTE Size columns are uint64 (Massive round-lot/share counts).
        "u8" => read_le::<8, _>(col, n, |b| u64::from_le_bytes(b) as f64),
        "u4" => read_le::<4, _>(col, n, |b| u32::from_le_bytes(b) as f64),
        other => bail!("column {} has unexpected numeric dtype {other}", col.name),
    }
}

/// Strips numpy byte-order/endianness prefixes (`<`, `>`, `=`, `|`) from a dtype.
fn normalize_dtype(dtype: &str) -> String {
    dtype
        .trim_start_matches(['<', '>', '=', '|'])
        .to_ascii_lowercase()
}

/// Reads `n` fixed-`WIDTH` little-endian elements from a column blob.
fn read_le<const WIDTH: usize, T>(
    col: &Column,
    n: usize,
    convert: impl Fn([u8; WIDTH]) -> T,
) -> Result<Vec<T>> {
    let expected = n * WIDTH;
    if col.data.len() != expected {
        bail!(
            "column {} expected {expected} bytes ({n} x {WIDTH}), got {}",
            col.name,
            col.data.len(),
        );
    }
    let mut out = Vec::with_capacity(n);
    for chunk in col.data.chunks_exact(WIDTH) {
        let mut buf = [0u8; WIDTH];
        buf.copy_from_slice(chunk);
        out.push(convert(buf));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::NumpyDataset;
    use std::str::FromStr;

    fn col_bytes_i64(vals: &[i64]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }
    fn col_bytes_f32(vals: &[f32]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn aapl_bar_type() -> BarType {
        BarType::from_str("AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL").unwrap()
    }

    #[test]
    fn decodes_ohlcv_columns() {
        // Mirrors the live wire format: Epoch i8, OHLC f4, Volume i8.
        let epochs = [1_781_827_020_i64, 1_781_827_080, 1_781_827_140];
        let ds = NumpyMultiDataset {
            data: Some(NumpyDataset {
                column_types: vec![
                    "<i8".into(),
                    "<f4".into(),
                    "<f4".into(),
                    "<f4".into(),
                    "<f4".into(),
                    "<i8".into(),
                ],
                column_names: vec![
                    "Epoch".into(),
                    "Open".into(),
                    "High".into(),
                    "Low".into(),
                    "Close".into(),
                    "Volume".into(),
                ],
                column_data: vec![
                    col_bytes_i64(&epochs),
                    col_bytes_f32(&[297.20, 297.29, 297.27]),
                    col_bytes_f32(&[297.21, 297.30, 297.27]),
                    col_bytes_f32(&[297.20, 297.21, 297.20]),
                    col_bytes_f32(&[297.21, 297.21, 297.23]),
                    col_bytes_i64(&[1063, 424, 699]),
                ],
                length: 3,
                data_shapes: vec![],
            }),
            start_index: Default::default(),
            lengths: Default::default(),
        };

        let bars = decode_bars(&ds, aapl_bar_type(), 2, 0, 60_000_000_000).unwrap();
        assert_eq!(bars.len(), 3);

        let first = &bars[0];
        assert_eq!(first.open.as_f64(), 297.20);
        assert_eq!(first.close.as_f64(), 297.21);
        assert_eq!(first.volume.as_f64(), 1063.0);
        assert_eq!(first.ts_event.as_u64(), 1_781_827_020 * 1_000_000_000);
        // ts_init = open + 1-minute duration (bar close), for correct execution timing.
        assert_eq!(
            first.ts_init.as_u64(),
            1_781_827_020 * 1_000_000_000 + 60_000_000_000
        );

        // Monotonic non-decreasing timestamps (replay determinism precondition).
        assert!(bars[0].ts_init <= bars[1].ts_init);
        assert!(bars[1].ts_init <= bars[2].ts_init);
    }

    fn col_bytes_f64(vals: &[f64]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }
    fn col_bytes_u64(vals: &[u64]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn aapl_instrument_id() -> InstrumentId {
        InstrumentId::from_str("AAPL.MARKETSTORE").unwrap()
    }

    #[test]
    fn decodes_ws_row_to_bar() {
        let mut row = HashMap::new();
        row.insert("Epoch".to_string(), Value::Int(1_781_827_020));
        row.insert("Open".to_string(), Value::Float(297.20));
        row.insert("High".to_string(), Value::Float(297.29));
        row.insert("Low".to_string(), Value::Float(297.20));
        row.insert("Close".to_string(), Value::Float(297.21));
        // Volume may arrive as an int on the wire.
        row.insert("Volume".to_string(), Value::Int(1063));

        let bar = bar_from_ws_row(&row, aapl_bar_type(), 2, 0, 60_000_000_000).unwrap();
        assert_eq!(bar.open.as_f64(), 297.20);
        assert_eq!(bar.close.as_f64(), 297.21);
        assert_eq!(bar.volume.as_f64(), 1063.0);
        assert_eq!(bar.ts_event.as_u64(), 1_781_827_020 * 1_000_000_000);
        assert_eq!(
            bar.ts_init.as_u64(),
            1_781_827_020 * 1_000_000_000 + 60_000_000_000
        );
    }

    #[test]
    fn ws_row_missing_field_errors() {
        let mut row = HashMap::new();
        row.insert("Epoch".to_string(), Value::Int(1));
        row.insert("Open".to_string(), Value::Float(1.0));
        // missing High/Low/Close/Volume
        assert!(bar_from_ws_row(&row, aapl_bar_type(), 2, 0, 60_000_000_000).is_err());
    }

    #[test]
    fn decodes_live_enveloped_lowercase_bar_row() {
        // The ACTUAL live wire format (verified against a running MarketStore /ws):
        // data = { msg_type: "bar", payload: { open, high, low, close, volume, epoch } }
        // with LOWERCASE keys. StreamPayload::row() must unwrap the envelope and
        // bar_from_ws_row must find the fields case-insensitively.
        let mut payload = HashMap::new();
        payload.insert("epoch".to_string(), Value::Int(1_783_712_700));
        payload.insert("open".to_string(), Value::Float(3.915));
        payload.insert("high".to_string(), Value::Float(3.915));
        payload.insert("low".to_string(), Value::Float(3.910));
        payload.insert("close".to_string(), Value::Float(3.9101));
        payload.insert("volume".to_string(), Value::Int(10174));
        payload.insert("symbol".to_string(), Value::Str("GRAB".into()));

        let mut data = HashMap::new();
        data.insert("msg_type".to_string(), Value::Str("bar".into()));
        data.insert("payload".to_string(), Value::Map(payload));

        let frame = StreamPayload {
            key: "GRAB.MARKETSTORE".to_string(),
            data,
        };
        // row() unwraps the {msg_type, payload} envelope.
        let bar = bar_from_ws_row(frame.row(), aapl_bar_type(), 4, 0, 60_000_000_000).unwrap();
        assert_eq!(bar.open.as_f64(), 3.915);
        assert_eq!(bar.close.as_f64(), 3.9101);
        assert_eq!(bar.volume.as_f64(), 10174.0);
        assert_eq!(bar.ts_event.as_u64(), 1_783_712_700 * 1_000_000_000);
    }

    #[test]
    fn stream_payload_row_passes_through_flat_form() {
        // The flat (non-enveloped) form: row() returns `data` unchanged.
        let mut data = HashMap::new();
        data.insert("Epoch".to_string(), Value::Int(1));
        data.insert("Open".to_string(), Value::Float(1.0));
        let frame = StreamPayload { key: "X".into(), data };
        assert!(frame.row().contains_key("Epoch"));
    }

    #[test]
    fn decodes_trade_columns() {
        // TRADE: Epoch i8, Nanoseconds i4, Price f8, Size u8 (no TradeID column yet).
        let ds = NumpyMultiDataset {
            data: Some(NumpyDataset {
                column_types: vec!["<i8".into(), "<i4".into(), "<f8".into(), "<u8".into()],
                column_names: vec![
                    "Epoch".into(),
                    "Nanoseconds".into(),
                    "Price".into(),
                    "Size".into(),
                ],
                column_data: vec![
                    col_bytes_i64(&[1_781_827_020, 1_781_827_020]),
                    vec![
                        500_000_000i32.to_le_bytes().to_vec(),
                        0i32.to_le_bytes().to_vec(),
                    ]
                    .concat(),
                    col_bytes_f64(&[297.21, 297.22]),
                    col_bytes_u64(&[100, 250]),
                ],
                length: 2,
                data_shapes: vec![],
            }),
            start_index: Default::default(),
            lengths: Default::default(),
        };
        let ticks = decode_trades(&ds, aapl_instrument_id(), 2, 0).unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].price.as_f64(), 297.21);
        assert_eq!(ticks[0].size.as_f64(), 100.0);
        assert_eq!(ticks[0].aggressor_side, AggressorSide::NoAggressor);
        // ts_event = epoch*1e9 + nanos; point-in-time so ts_init == ts_event.
        assert_eq!(ticks[0].ts_event.as_u64(), 1_781_827_020 * 1_000_000_000 + 500_000_000);
        assert_eq!(ticks[0].ts_init, ticks[0].ts_event);
        // Synthesized trade id (no TradeID column).
        assert!(!ticks[0].trade_id.to_string().is_empty());

        // The boundary-safe primitive columns (what crosses to Python).
        let raw = decode_raw_trades(&ds).unwrap();
        assert_eq!(raw.len(), 2);
        assert_eq!(raw.price, vec![297.21, 297.22]);
        assert_eq!(raw.size, vec![100.0, 250.0]);
        assert_eq!(
            raw.ts_event[0],
            1_781_827_020 * 1_000_000_000 + 500_000_000
        );
        // Trade ids synthesized (non-empty) when the column is absent.
        assert!(raw.trade_id.iter().all(|id| !id.is_empty()));
    }

    #[test]
    fn decodes_ws_row_to_trade_with_real_id() {
        let mut row = HashMap::new();
        row.insert("Epoch".to_string(), Value::Int(1_781_827_020));
        row.insert("Nanoseconds".to_string(), Value::Int(250_000_000));
        row.insert("Price".to_string(), Value::Float(297.21));
        row.insert("Size".to_string(), Value::Int(100));
        row.insert("TradeID".to_string(), Value::Str("X12345".into()));
        let t = trade_from_ws_row(&row, aapl_instrument_id(), 2, 0).unwrap();
        assert_eq!(t.price.as_f64(), 297.21);
        assert_eq!(t.size.as_f64(), 100.0);
        assert_eq!(t.trade_id.to_string(), "X12345");
        assert_eq!(t.ts_event.as_u64(), 1_781_827_020 * 1_000_000_000 + 250_000_000);
    }

    #[test]
    fn decodes_quote_columns() {
        let ds = NumpyMultiDataset {
            data: Some(NumpyDataset {
                column_types: vec![
                    "<i8".into(),
                    "<i4".into(),
                    "<f8".into(),
                    "<f8".into(),
                    "<u8".into(),
                    "<u8".into(),
                ],
                column_names: vec![
                    "Epoch".into(),
                    "Nanoseconds".into(),
                    "BidPrice".into(),
                    "AskPrice".into(),
                    "BidSize".into(),
                    "AskSize".into(),
                ],
                column_data: vec![
                    col_bytes_i64(&[1_781_827_020]),
                    0i32.to_le_bytes().to_vec(),
                    col_bytes_f64(&[297.20]),
                    col_bytes_f64(&[297.23]),
                    col_bytes_u64(&[3]),
                    col_bytes_u64(&[5]),
                ],
                length: 1,
                data_shapes: vec![],
            }),
            start_index: Default::default(),
            lengths: Default::default(),
        };
        let ticks = decode_quotes(&ds, aapl_instrument_id(), 2, 0).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].bid_price.as_f64(), 297.20);
        assert_eq!(ticks[0].ask_price.as_f64(), 297.23);
        assert_eq!(ticks[0].bid_size.as_f64(), 3.0);
        assert_eq!(ticks[0].ask_size.as_f64(), 5.0);
        assert_eq!(ticks[0].ts_init, ticks[0].ts_event);

        // The boundary-safe primitive columns (what crosses to Python).
        let raw = decode_raw_quotes(&ds).unwrap();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw.bid_price, vec![297.20]);
        assert_eq!(raw.ask_price, vec![297.23]);
        assert_eq!(raw.bid_size, vec![3.0]);
        assert_eq!(raw.ask_size, vec![5.0]);
        assert_eq!(raw.ts_event, vec![1_781_827_020 * 1_000_000_000]);
    }

    #[test]
    fn decodes_ws_row_to_quote() {
        let mut row = HashMap::new();
        row.insert("Epoch".to_string(), Value::Int(1_781_827_020));
        row.insert("BidPrice".to_string(), Value::Float(297.20));
        row.insert("AskPrice".to_string(), Value::Float(297.23));
        row.insert("BidSize".to_string(), Value::Int(3));
        row.insert("AskSize".to_string(), Value::Int(5));
        let q = quote_from_ws_row(&row, aapl_instrument_id(), 2, 0).unwrap();
        assert_eq!(q.bid_price.as_f64(), 297.20);
        assert_eq!(q.ask_price.as_f64(), 297.23);
        assert_eq!(q.ts_event.as_u64(), 1_781_827_020 * 1_000_000_000);
    }

    #[test]
    fn empty_dataset_yields_no_bars() {
        let ds = NumpyMultiDataset {
            data: None,
            start_index: Default::default(),
            lengths: Default::default(),
        };
        assert!(decode_bars(&ds, aapl_bar_type(), 2, 0, 60_000_000_000).unwrap().is_empty());
    }

    #[test]
    fn wrong_byte_length_errors() {
        let ds = NumpyMultiDataset {
            data: Some(NumpyDataset {
                column_types: vec!["<i8".into(); 6],
                column_names: vec![
                    "Epoch".into(),
                    "Open".into(),
                    "High".into(),
                    "Low".into(),
                    "Close".into(),
                    "Volume".into(),
                ],
                column_data: vec![vec![0u8; 3]; 6], // 3 bytes != 1 * 8
                length: 1,
                data_shapes: vec![],
            }),
            start_index: Default::default(),
            lengths: Default::default(),
        };
        assert!(decode_bars(&ds, aapl_bar_type(), 2, 0, 60_000_000_000).is_err());
    }
}
