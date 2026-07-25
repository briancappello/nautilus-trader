//! Serde models for the Alpaca Trading API v2 wire schemas.
//!
//! Field sets mirror the live `/v2/account`, `/v2/positions`, and `/v2/orders` JSON (verified
//! against a paper account and `alpaca-py`'s request/response models). Only the fields the
//! adapter consumes are modeled; unknown fields are ignored by serde defaults.

use serde::{Deserialize, Serialize};

/// The `/v2/account` response (subset). Monetary fields are strings on the wire.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaAccount {
    /// Account id (UUID).
    pub id: String,
    /// Account currency, e.g. `"USD"`.
    pub currency: String,
    /// Total cash balance.
    pub cash: String,
    /// Total account equity (cash + long market value − short market value).
    pub equity: String,
    /// Long market value of held positions.
    #[serde(default)]
    pub long_market_value: String,
    /// Short market value of held positions.
    #[serde(default)]
    pub short_market_value: String,
    /// Available buying power.
    #[serde(default)]
    pub buying_power: String,
    /// Whether trading is blocked on this account.
    #[serde(default)]
    pub trading_blocked: bool,
    /// Whether the account is fully blocked.
    #[serde(default)]
    pub account_blocked: bool,
}

/// A `/v2/positions` entry (subset).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaPosition {
    /// The asset symbol, e.g. `"AAPL"`.
    pub symbol: String,
    /// Signed quantity (`qty` is unsigned; `side` gives direction).
    pub qty: String,
    /// `"long"` or `"short"`.
    pub side: String,
    /// Average entry price.
    pub avg_entry_price: String,
    /// Current market value (signed).
    #[serde(default)]
    pub market_value: String,
}

/// The `/v2/orders` POST request body. Optional fields are omitted when `None`.
///
/// Mirrors `alpaca-py`'s `OrderRequest`: `type`/`time_in_force`/`order_class` are lowercase
/// enum strings; `qty`/`limit_price`/`stop_price` are decimal strings.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateOrderRequest {
    /// The asset symbol.
    pub symbol: String,
    /// Quantity in shares (whole-share equities for MVP).
    pub qty: String,
    /// `"buy"` or `"sell"`.
    pub side: String,
    /// `"market"`, `"limit"`, `"stop"`, `"stop_limit"`.
    #[serde(rename = "type")]
    pub order_type: String,
    /// `"day"`, `"gtc"`, etc.
    pub time_in_force: String,
    /// Required for limit / stop-limit orders.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
    /// Required for stop / stop-limit orders.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<String>,
    /// `"simple"`, `"bracket"`, `"oco"`, `"oto"` (omitted = simple).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_class: Option<String>,
    /// Trailing-stop offset as a percent (mutually exclusive with `trail_price`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_percent: Option<String>,
    /// Trailing-stop offset as an absolute price (mutually exclusive with `trail_percent`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_price: Option<String>,
    /// RFC-3339 expiry timestamp; required by Alpaca for GTD orders, omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Whether the order may execute in extended hours (limit + day/gtc only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_hours: Option<bool>,
    /// Client-assigned order id (Nautilus `ClientOrderId`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    /// Bracket/OTO take-profit leg.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<TakeProfit>,
    /// Bracket/OTO/OCO stop-loss leg.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<StopLoss>,
}

/// A bracket/OTO take-profit leg.
#[derive(Clone, Debug, Serialize)]
pub struct TakeProfit {
    /// The limit price to exit a profitable trade.
    pub limit_price: String,
}

/// A bracket/OTO/OCO stop-loss leg.
#[derive(Clone, Debug, Serialize)]
pub struct StopLoss {
    /// The trigger price for the stop.
    pub stop_price: String,
    /// Optional limit price (stop-limit exit); omitted = stop-market exit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
}

/// The `PATCH /v2/orders/{id}` request body (order replacement).
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReplaceOrderRequest {
    /// New quantity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    /// New limit price (for limit / stop-limit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<String>,
    /// New stop price (for stop / stop-limit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<String>,
}

/// The `/v2/orders` response (subset) — the venue's view of a submitted order.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaOrder {
    /// Venue order id (UUID).
    pub id: String,
    /// The client order id we assigned, if echoed back.
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// The asset symbol.
    #[serde(default)]
    pub symbol: Option<String>,
    /// `"buy"` or `"sell"`.
    #[serde(default)]
    pub side: Option<String>,
    /// Order status, e.g. `"new"`, `"accepted"`, `"filled"`, `"rejected"`.
    #[serde(default)]
    pub status: Option<String>,
    /// Filled quantity so far.
    #[serde(default)]
    pub filled_qty: Option<String>,
    /// Average fill price, if any.
    #[serde(default)]
    pub filled_avg_price: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request() -> CreateOrderRequest {
        CreateOrderRequest {
            symbol: "SOXS".into(),
            qty: "1".into(),
            side: "buy".into(),
            order_type: "market".into(),
            time_in_force: "gtc".into(),
            ..Default::default()
        }
    }

    #[test]
    fn omits_none_trail_and_expiry_fields() {
        let json = serde_json::to_value(base_request()).unwrap();
        assert!(json.get("trail_percent").is_none());
        assert!(json.get("trail_price").is_none());
        assert!(json.get("expires_at").is_none());
        // `type` rename still holds.
        assert_eq!(json["type"], "market");
    }

    #[test]
    fn serializes_trailing_and_gtd_fields_with_wire_names() {
        let req = CreateOrderRequest {
            order_type: "trailing_stop".into(),
            time_in_force: "gtd".into(),
            trail_percent: Some("1".into()),
            expires_at: Some("2026-07-14T20:00:00+00:00".into()),
            ..base_request()
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["trail_percent"], "1");
        assert_eq!(json["expires_at"], "2026-07-14T20:00:00+00:00");
        assert_eq!(json["type"], "trailing_stop");
        assert_eq!(json["time_in_force"], "gtd");
    }

    #[test]
    fn trail_price_serializes() {
        let req = CreateOrderRequest {
            order_type: "trailing_stop".into(),
            trail_price: Some("0.05".into()),
            ..base_request()
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["trail_price"], "0.05");
        assert!(json.get("trail_percent").is_none());
    }
}
