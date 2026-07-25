//! Translation between Nautilus model types and Alpaca wire representations, and parsing of
//! Alpaca account responses into Nautilus [`AccountState`].

use std::str::FromStr;

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{
        AccountType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
    },
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, VenueOrderId},
    reports::{OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Quantity},
};
use rust_decimal::Decimal;

use super::{
    error::Error,
    models::{AlpacaAccount, AlpacaOrder, AlpacaPosition},
};

/// Maps a Nautilus [`OrderSide`] to the Alpaca `side` string.
///
/// # Errors
///
/// Returns an error for [`OrderSide::NoOrderSide`].
pub fn order_side_to_alpaca(side: OrderSide) -> Result<&'static str, Error> {
    match side {
        OrderSide::Buy => Ok("buy"),
        OrderSide::Sell => Ok("sell"),
        OrderSide::NoOrderSide => Err(Error::Parse("order side must be Buy or Sell".into())),
    }
}

/// Maps a Nautilus [`OrderType`] to the Alpaca `type` string.
///
/// MVP supports market / limit / stop / stop-limit equities; other types are rejected.
///
/// # Errors
///
/// Returns an error for unsupported order types.
pub fn order_type_to_alpaca(order_type: OrderType) -> Result<&'static str, Error> {
    match order_type {
        OrderType::Market => Ok("market"),
        OrderType::Limit => Ok("limit"),
        OrderType::StopMarket => Ok("stop"),
        OrderType::StopLimit => Ok("stop_limit"),
        // Alpaca has a single `trailing_stop` type; market vs limit is distinguished by the
        // presence of a limit price (handled at request-build time).
        OrderType::TrailingStopMarket | OrderType::TrailingStopLimit => Ok("trailing_stop"),
        // Alpaca has no market-to-limit type; it is submitted as a market order (the
        // limit-on-fill conversion is Alpaca-internal).
        OrderType::MarketToLimit => Ok("market"),
        other => Err(Error::Parse(format!(
            "unsupported order type for Alpaca: {other:?}"
        ))),
    }
}

/// Maps a Nautilus [`TimeInForce`] to the Alpaca `time_in_force` string.
///
/// Supports DAY and GTC only. Alpaca rejects GTD for US equities (422
/// `order_time_in_force provided not supported for us_equity trading`), and IOC/FOK/auction
/// TIFs are out of scope — all are rejected upstream by the validator before reaching here.
///
/// # Errors
///
/// Returns an error for unsupported TIFs.
pub fn time_in_force_to_alpaca(tif: TimeInForce) -> Result<&'static str, Error> {
    match tif {
        TimeInForce::Day => Ok("day"),
        TimeInForce::Gtc => Ok("gtc"),
        other => Err(Error::Parse(format!(
            "unsupported time-in-force for Alpaca: {other:?}"
        ))),
    }
}

/// Parses a decimal string from the wire, attributing the field name on error.
fn parse_decimal(field: &str, raw: &str) -> Result<Decimal, Error> {
    Decimal::from_str(raw.trim())
        .map_err(|e| Error::Parse(format!("account field '{field}'='{raw}': {e}")))
}

/// Parses an [`AlpacaAccount`] into a Nautilus cash [`AccountState`].
///
/// Alpaca equity accounts are single-currency cash accounts; the balance is reported as
/// `total = equity`, `free = cash`, `locked = total − free` (clamped at zero). The account
/// currency string (e.g. `"USD"`) resolves to a fiat [`Currency`].
///
/// # Errors
///
/// Returns an error if a monetary field or the currency code fails to parse.
pub fn parse_account_state(
    account: &AlpacaAccount,
    account_id: AccountId,
    is_reported: bool,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> Result<AccountState, Error> {
    let currency = Currency::from_str(account.currency.trim())
        .map_err(|e| Error::Parse(format!("unknown account currency '{}': {e}", account.currency)))?;

    let equity = parse_decimal("equity", &account.equity)?;
    let cash = parse_decimal("cash", &account.cash)?;

    let total = Money::from_decimal(equity, currency)
        .map_err(|e| Error::Parse(format!("equity money: {e}")))?;
    let free = Money::from_decimal(cash, currency)
        .map_err(|e| Error::Parse(format!("cash money: {e}")))?;
    // Locked is the non-cash portion of equity (market value tied up in positions). Clamp to
    // zero: a momentarily negative spread (short market value) must not produce a negative lock.
    let locked_dec = (equity - cash).max(Decimal::ZERO);
    let locked = Money::from_decimal(locked_dec, currency)
        .map_err(|e| Error::Parse(format!("locked money: {e}")))?;

    let balance = AccountBalance::new(total, locked, free);

    Ok(AccountState::new(
        account_id,
        AccountType::Cash,
        vec![balance],
        Vec::new(),
        is_reported,
        UUID4::new(),
        ts_event,
        ts_init,
        Some(currency),
    ))
}

/// Maps an Alpaca order-status string to a Nautilus [`OrderStatus`].
#[must_use]
pub fn order_status_from_alpaca(status: &str) -> OrderStatus {
    match status {
        "new" | "accepted" | "pending_new" | "accepted_for_bidding" | "held" => {
            OrderStatus::Accepted
        }
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "pending_cancel" => OrderStatus::Canceled,
        "expired" | "done_for_day" => OrderStatus::Expired,
        "rejected" | "suspended" => OrderStatus::Rejected,
        "replaced" | "pending_replace" => OrderStatus::PendingUpdate,
        _ => OrderStatus::Accepted,
    }
}

fn side_from_alpaca(side: Option<&str>) -> OrderSide {
    match side {
        Some("sell") => OrderSide::Sell,
        _ => OrderSide::Buy,
    }
}

/// Builds an [`OrderStatusReport`] from an Alpaca order (for reconciliation, M5.5).
///
/// Only the fields the engine needs for reconciliation are populated; order type / TIF default to
/// LIMIT/GTC when absent on the wire (the subset modeled). Returns `None` if the symbol is missing.
#[must_use]
pub fn parse_order_status_report(
    order: &AlpacaOrder,
    account_id: AccountId,
    ts: UnixNanos,
) -> Option<OrderStatusReport> {
    let symbol = order.symbol.as_deref()?;
    let instrument_id = InstrumentId::new(Symbol::new(symbol), crate::common::alpaca_venue());

    let filled = order
        .filled_qty
        .as_deref()
        .and_then(|s| Decimal::from_str(s.trim()).ok())
        .unwrap_or(Decimal::ZERO);

    let status = order_status_from_alpaca(order.status.as_deref().unwrap_or("accepted"));
    let client_order_id = order.client_order_id.as_deref().map(ClientOrderId::new);
    let venue_order_id = VenueOrderId::new(&order.id);

    // Quantity/filled precision 0 (whole-share equities, MVP).
    let quantity = Quantity::from_decimal_dp(filled.max(Decimal::ONE), 0).ok()?;
    let filled_qty = Quantity::from_decimal_dp(filled, 0).ok()?;

    Some(OrderStatusReport::new(
        account_id,
        instrument_id,
        client_order_id,
        venue_order_id,
        side_from_alpaca(order.side.as_deref()),
        OrderType::Limit,
        TimeInForce::Gtc,
        status,
        quantity,
        filled_qty,
        ts,
        ts,
        ts,
        None,
    ))
}

/// Builds a [`PositionStatusReport`] from an Alpaca position (for reconciliation, M5.5).
#[must_use]
pub fn parse_position_status_report(
    position: &AlpacaPosition,
    account_id: AccountId,
    ts: UnixNanos,
) -> Option<PositionStatusReport> {
    let instrument_id =
        InstrumentId::new(Symbol::new(&position.symbol), crate::common::alpaca_venue());
    let qty_abs = Decimal::from_str(position.qty.trim()).ok()?.abs();
    let quantity = Quantity::from_decimal_dp(qty_abs, 0).ok()?;
    let side = match position.side.as_str() {
        "short" => PositionSide::Short,
        _ => PositionSide::Long,
    };
    let avg_px = Decimal::from_str(position.avg_entry_price.trim()).ok();

    Some(PositionStatusReport::new(
        account_id,
        instrument_id,
        side.as_specified(),
        quantity,
        ts,
        ts,
        None,
        None,
        avg_px,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::models::AlpacaAccount;

    fn sample_account() -> AlpacaAccount {
        AlpacaAccount {
            id: "acct-1".into(),
            currency: "USD".into(),
            cash: "100000".into(),
            equity: "100000".into(),
            long_market_value: "0".into(),
            short_market_value: "0".into(),
            buying_power: "400000".into(),
            trading_blocked: false,
            account_blocked: false,
        }
    }

    #[test]
    fn side_and_type_mapping() {
        assert_eq!(order_side_to_alpaca(OrderSide::Buy).unwrap(), "buy");
        assert_eq!(order_side_to_alpaca(OrderSide::Sell).unwrap(), "sell");
        assert!(order_side_to_alpaca(OrderSide::NoOrderSide).is_err());

        assert_eq!(order_type_to_alpaca(OrderType::Market).unwrap(), "market");
        assert_eq!(order_type_to_alpaca(OrderType::Limit).unwrap(), "limit");
        assert_eq!(order_type_to_alpaca(OrderType::StopMarket).unwrap(), "stop");
        assert_eq!(
            order_type_to_alpaca(OrderType::StopLimit).unwrap(),
            "stop_limit"
        );
        // Trailing stops (market + limit) both map to Alpaca's single `trailing_stop` type.
        assert_eq!(
            order_type_to_alpaca(OrderType::TrailingStopMarket).unwrap(),
            "trailing_stop"
        );
        assert_eq!(
            order_type_to_alpaca(OrderType::TrailingStopLimit).unwrap(),
            "trailing_stop"
        );
        // Market-to-limit submits as a plain market order (Alpaca has no MTL type).
        assert_eq!(
            order_type_to_alpaca(OrderType::MarketToLimit).unwrap(),
            "market"
        );
        // Still unsupported.
        assert!(order_type_to_alpaca(OrderType::MarketIfTouched).is_err());
    }

    #[test]
    fn tif_mapping() {
        assert_eq!(time_in_force_to_alpaca(TimeInForce::Day).unwrap(), "day");
        assert_eq!(time_in_force_to_alpaca(TimeInForce::Gtc).unwrap(), "gtc");
        // Alpaca does not support GTD/IOC/FOK for US equities.
        assert!(time_in_force_to_alpaca(TimeInForce::Gtd).is_err());
        assert!(time_in_force_to_alpaca(TimeInForce::Ioc).is_err());
        assert!(time_in_force_to_alpaca(TimeInForce::Fok).is_err());
    }

    #[test]
    fn account_state_flat_cash() {
        let acct = sample_account();
        let state = parse_account_state(
            &acct,
            AccountId::from("ALPACA-001"),
            true,
            UnixNanos::default(),
            UnixNanos::default(),
        )
        .unwrap();
        assert_eq!(state.balances.len(), 1);
        let bal = &state.balances[0];
        assert_eq!(bal.total.as_f64(), 100_000.0);
        assert_eq!(bal.free.as_f64(), 100_000.0);
        assert_eq!(bal.locked.as_f64(), 0.0);
        assert_eq!(state.base_currency, Some(Currency::USD()));
    }

    #[test]
    fn account_state_with_positions_locks_market_value() {
        let mut acct = sample_account();
        // Equity 100k, cash 70k → 30k locked in positions.
        acct.cash = "70000".into();
        acct.equity = "100000".into();
        let state = parse_account_state(
            &acct,
            AccountId::from("ALPACA-001"),
            true,
            UnixNanos::default(),
            UnixNanos::default(),
        )
        .unwrap();
        let bal = &state.balances[0];
        assert_eq!(bal.total.as_f64(), 100_000.0);
        assert_eq!(bal.free.as_f64(), 70_000.0);
        assert_eq!(bal.locked.as_f64(), 30_000.0);
    }

    #[test]
    fn order_status_mapping() {
        assert_eq!(order_status_from_alpaca("new"), OrderStatus::Accepted);
        assert_eq!(order_status_from_alpaca("accepted"), OrderStatus::Accepted);
        assert_eq!(order_status_from_alpaca("filled"), OrderStatus::Filled);
        assert_eq!(
            order_status_from_alpaca("partially_filled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(order_status_from_alpaca("canceled"), OrderStatus::Canceled);
        assert_eq!(order_status_from_alpaca("rejected"), OrderStatus::Rejected);
        assert_eq!(order_status_from_alpaca("expired"), OrderStatus::Expired);
    }

    #[test]
    fn order_status_report_from_wire() {
        let order = crate::http::models::AlpacaOrder {
            id: "v-1".into(),
            client_order_id: Some("c-1".into()),
            symbol: Some("AAPL".into()),
            side: Some("buy".into()),
            status: Some("accepted".into()),
            filled_qty: Some("0".into()),
            filled_avg_price: None,
        };
        let r = parse_order_status_report(&order, AccountId::from("ALPACA-001"), UnixNanos::default())
            .unwrap();
        assert_eq!(r.order_status, OrderStatus::Accepted);
        assert_eq!(r.order_side, OrderSide::Buy);
        assert_eq!(r.filled_qty.as_f64(), 0.0);
        assert_eq!(r.venue_order_id.to_string(), "v-1");
    }

    #[test]
    fn position_status_report_from_wire() {
        let pos = crate::http::models::AlpacaPosition {
            symbol: "MSFT".into(),
            qty: "-5".into(),
            side: "short".into(),
            avg_entry_price: "412.50".into(),
            market_value: "-2062.50".into(),
        };
        let r = parse_position_status_report(&pos, AccountId::from("ALPACA-001"), UnixNanos::default())
            .unwrap();
        assert_eq!(r.quantity.as_f64(), 5.0);
        assert_eq!(
            r.position_side,
            nautilus_model::enums::PositionSideSpecified::Short
        );
    }
}
