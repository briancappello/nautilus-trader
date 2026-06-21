//! Synthesizes `Equity` instruments from config (MarketStore has no instrument metadata).
//!
//! Mirrors the Python backtest path (`trading/config/instruments.py::make_equity`): USD,
//! `price_increment = 10^-precision`, lot size in shares, `ts = 0`. The in-wheel build
//! constructs the wheel's own `InstrumentAny::Equity` directly (no cross-cdylib boundary),
//! which is then pushed onto the bus via `DataEvent::Instrument` on connect.

use nautilus_model::{
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{Equity, InstrumentAny},
    types::{Currency, Price, Quantity},
};

use crate::config::{InstrumentSpec, MarketStoreDataClientConfig};

/// Synthesizes an `InstrumentAny::Equity` for `spec` on the configured `venue`.
#[must_use]
pub fn equity_from_spec(spec: &InstrumentSpec, venue: &Venue) -> InstrumentAny {
    let symbol = Symbol::new(&spec.symbol);
    let instrument_id = InstrumentId::new(symbol, *venue);
    let price_increment = Price::new(10f64.powi(-(spec.price_precision as i32)), spec.price_precision);
    let lot_size = Quantity::new(spec.lot_size.max(1) as f64, 0);

    let equity = Equity::new(
        instrument_id,
        symbol,
        None, // isin
        Currency::USD(),
        spec.price_precision,
        price_increment,
        Some(lot_size),
        None, // max_quantity
        None, // min_quantity
        None, // max_price
        None, // min_price
        None, // margin_init
        None, // margin_maint
        None, // maker_fee
        None, // taker_fee
        None, // tick_scheme
        None, // info
        0.into(), // ts_event
        0.into(), // ts_init
    );
    InstrumentAny::Equity(equity)
}

/// Synthesizes the full instrument set described by `config`.
#[must_use]
pub fn instruments_from_config(config: &MarketStoreDataClientConfig) -> Vec<InstrumentAny> {
    let venue = Venue::new(&config.venue);
    config
        .instruments
        .iter()
        .map(|spec| equity_from_spec(spec, &venue))
        .collect()
}
