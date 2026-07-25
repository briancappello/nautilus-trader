//! MarketStore JSON-RPC ingestion control (`/rpc`).
//!
//! MarketStore's tick streams (`{symbol}/1Sec/QUOTE`, `.../1Sec/TRADE`) are created with
//! `dynamic_ticks: true`, so they start **empty**: the server only begins pulling a
//! symbol's quotes/trades from the upstream (Massive) feed once told to. That is a
//! separate action from the WS `/ws` subscribe (which only *delivers* an already-flowing
//! stream). So a live quote/trade subscription is two steps:
//!
//! 1. **RPC `DataService.Subscribe`** (here) — turn on upstream ingestion for a symbol.
//! 2. **WS `subscribe`** ([`crate::ws`]) — receive the resulting stream.
//!
//! Bars (`OHLCV`) are always ingested, so they need step 1 only for ticks.
//!
//! The RPC is JSON-RPC 2.0 over HTTP POST at `http://<host>:5993/rpc`. The server (Go)
//! matches struct fields by **PascalCase** name — `Symbol`, `DataTypes`, `Action` —
//! verified against a running server (snake_case is silently rejected as "required").

use serde_json::json;

/// Which tick stream to turn on for a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickKind {
    Quotes,
    Trades,
}

impl TickKind {
    /// The server's `DataTypes` token (`"quotes"` / `"trades"`).
    #[must_use]
    pub const fn as_data_type(self) -> &'static str {
        match self {
            TickKind::Quotes => "quotes",
            TickKind::Trades => "trades",
        }
    }
}

/// Derives the JSON-RPC endpoint from the configured `/ws` endpoint.
///
/// `ws://host:5993/ws` -> `http://host:5993/rpc` (and `wss` -> `https`). The RPC and WS
/// share host:port; only the scheme and path differ.
#[must_use]
pub fn rpc_endpoint_from_ws(ws_endpoint: &str) -> String {
    let base = ws_endpoint
        .trim_end_matches("/ws/replay")
        .trim_end_matches("/ws")
        .trim_end_matches('/');
    let base = base
        .strip_prefix("ws://")
        .map(|rest| format!("http://{rest}"))
        .or_else(|| base.strip_prefix("wss://").map(|rest| format!("https://{rest}")))
        .unwrap_or_else(|| base.to_string());
    format!("{base}/rpc")
}

/// Builds the JSON-RPC 2.0 request body for `DataService.Subscribe` (PascalCase fields).
fn subscribe_body(symbol: &str, kind: TickKind) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "DataService.Subscribe",
        "params": {
            "Symbol": symbol,
            "DataTypes": [kind.as_data_type()],
            "Action": "subscribe",
        },
    })
}

/// Turns on upstream ingestion for `symbol`'s `kind` stream (step 1 above).
///
/// A successful RPC means "accepted" — the server returns its active desired set
/// (`{"result": {"Active": {"AAPL": ["quotes"]}}}`); data begins once upstream ticks land.
/// Errors are surfaced so the caller can log them, but a failure here shouldn't wedge the
/// WS session (bars still flow; ticks simply stay empty).
///
/// # Errors
///
/// Returns an error if the HTTP request fails or the server returns a JSON-RPC `error`.
pub async fn subscribe_ticks(
    rpc_endpoint: &str,
    symbol: &str,
    kind: TickKind,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let resp = client
        .post(rpc_endpoint)
        .json(&subscribe_body(symbol, kind))
        .send()
        .await?;
    let value: serde_json::Value = resp.json().await?;
    if let Some(err) = value.get("error").filter(|e| !e.is_null()) {
        anyhow::bail!("MarketStore RPC Subscribe error for {symbol} {kind:?}: {err}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_rpc_endpoint_from_ws() {
        assert_eq!(rpc_endpoint_from_ws("ws://127.0.0.1:5993/ws"), "http://127.0.0.1:5993/rpc");
        assert_eq!(
            rpc_endpoint_from_ws("ws://host:5993/ws/replay"),
            "http://host:5993/rpc"
        );
        assert_eq!(rpc_endpoint_from_ws("wss://h:5993/ws"), "https://h:5993/rpc");
    }

    #[test]
    fn subscribe_body_uses_pascal_case() {
        let body = subscribe_body("AAPL", TickKind::Quotes);
        let params = &body["params"];
        assert_eq!(params["Symbol"], "AAPL");
        assert_eq!(params["DataTypes"][0], "quotes");
        assert_eq!(params["Action"], "subscribe");
    }

    #[test]
    fn tick_kind_tokens() {
        assert_eq!(TickKind::Quotes.as_data_type(), "quotes");
        assert_eq!(TickKind::Trades.as_data_type(), "trades");
    }
}
