//! Serde models for the Alpaca trade-updates WebSocket stream.
//!
//! Envelope: `{"stream":"trade_updates","data":{...}}`. The `data` is an [`AlpacaTradeUpdate`]
//! carrying the lifecycle `event`, the (possibly partial) `order`, and — on fill events —
//! top-level `price`/`qty`/`execution_id`. Verified against the live paper stream and
//! `alpaca-py`'s `TradeUpdate` model.

use serde::Deserialize;

/// A top-level message from the `/stream` WebSocket.
///
/// Control messages use `stream` ∈ {`authorization`, `listening`}; trade events use
/// `stream = "trade_updates"`. Only the fields the adapter consumes are modeled.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsEnvelope {
    /// The stream name (`"trade_updates"`, `"authorization"`, `"listening"`).
    pub stream: String,
    /// The payload (shape depends on `stream`).
    #[serde(default)]
    pub data: serde_json::Value,
}

/// The authorization control payload (`stream = "authorization"`).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaAuthData {
    /// `"authorized"` on success, `"unauthorized"` otherwise.
    pub status: String,
}

/// A `trade_updates` event payload.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaTradeUpdate {
    /// The lifecycle event: `new`, `accepted`, `fill`, `partial_fill`, `canceled`, `expired`,
    /// `rejected`, `pending_new`, `done_for_day`, `replaced`, etc.
    pub event: String,
    /// A per-fill execution id (present on fill / partial_fill).
    #[serde(default)]
    pub execution_id: Option<String>,
    /// The order this event concerns.
    pub order: AlpacaWsOrder,
    /// Last fill price (fill / partial_fill).
    #[serde(default)]
    pub price: Option<String>,
    /// Last fill quantity (fill / partial_fill).
    #[serde(default)]
    pub qty: Option<String>,
    /// Net position quantity after this event.
    #[serde(default)]
    pub position_qty: Option<String>,
    /// Event timestamp (RFC3339).
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// The nested `order` object on a trade update (subset).
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaWsOrder {
    /// Venue order id (UUID).
    pub id: String,
    /// The client order id we assigned (echoed back).
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// The asset symbol.
    #[serde(default)]
    pub symbol: Option<String>,
    /// `"buy"` or `"sell"`.
    #[serde(default)]
    pub side: Option<String>,
    /// Cumulative filled quantity.
    #[serde(default)]
    pub filled_qty: Option<String>,
    /// Cumulative average fill price.
    #[serde(default)]
    pub filled_avg_price: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILL_MSG: &str = r#"{
        "stream":"trade_updates",
        "data":{
            "event":"fill",
            "execution_id":"exec-123",
            "price":"189.50",
            "qty":"1",
            "position_qty":"1",
            "timestamp":"2026-06-24T14:30:00Z",
            "order":{
                "id":"363d4ae7-8b88-4a5f-821e-2ab6032682ba",
                "client_order_id":"ws-ef6db1f446",
                "symbol":"AAPL",
                "side":"buy",
                "filled_qty":"1",
                "filled_avg_price":"189.50"
            }
        }
    }"#;

    #[test]
    fn parses_fill_envelope() {
        let env: AlpacaWsEnvelope = serde_json::from_str(FILL_MSG).unwrap();
        assert_eq!(env.stream, "trade_updates");
        let upd: AlpacaTradeUpdate = serde_json::from_value(env.data).unwrap();
        assert_eq!(upd.event, "fill");
        assert_eq!(upd.execution_id.as_deref(), Some("exec-123"));
        assert_eq!(upd.price.as_deref(), Some("189.50"));
        assert_eq!(upd.qty.as_deref(), Some("1"));
        assert_eq!(upd.order.client_order_id.as_deref(), Some("ws-ef6db1f446"));
        assert_eq!(upd.order.side.as_deref(), Some("buy"));
    }

    #[test]
    fn parses_authorization_envelope() {
        let raw = r#"{"stream":"authorization","data":{"status":"authorized"}}"#;
        let env: AlpacaWsEnvelope = serde_json::from_str(raw).unwrap();
        assert_eq!(env.stream, "authorization");
        let auth: AlpacaAuthData = serde_json::from_value(env.data).unwrap();
        assert_eq!(auth.status, "authorized");
    }

    #[test]
    fn parses_accepted_without_fill_fields() {
        let raw = r#"{"stream":"trade_updates","data":{"event":"accepted",
            "order":{"id":"abc","client_order_id":"c1","symbol":"MSFT","side":"buy"}}}"#;
        let env: AlpacaWsEnvelope = serde_json::from_str(raw).unwrap();
        let upd: AlpacaTradeUpdate = serde_json::from_value(env.data).unwrap();
        assert_eq!(upd.event, "accepted");
        assert!(upd.price.is_none());
        assert!(upd.qty.is_none());
        assert!(upd.execution_id.is_none());
    }
}
