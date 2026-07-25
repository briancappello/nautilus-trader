//! HTTP client for the Alpaca Trading API v2 (REST).
//!
//! Mirrors the coinbase adapter's raw-client structure (`nautilus_network::http::HttpClient`
//! transport + a thin domain layer) but swaps Coinbase's per-request JWT for Alpaca's static
//! `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` headers. Retries are gated to idempotent GET/DELETE
//! so a replay never double-submits an order.

use std::collections::HashMap;

use nautilus_core::consts::NAUTILUS_USER_AGENT;
use nautilus_network::http::{HttpClient, Method, USER_AGENT};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{
    error::{Error, Result},
    models::{
        AlpacaAccount, AlpacaOrder, AlpacaPosition, CreateOrderRequest, ReplaceOrderRequest,
    },
};

/// Default request timeout (seconds).
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// The Alpaca REST HTTP client.
///
/// Holds the `nautilus_network` transport, the resolved base URL (paper/live), and the API
/// credentials used to build auth headers on every request.
#[derive(Debug, Clone)]
pub struct AlpacaHttpClient {
    client: HttpClient,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaHttpClient {
    /// Creates a new [`AlpacaHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::MissingCredentials`] if key or secret is empty, or a transport error if
    /// the underlying HTTP client cannot be built.
    pub fn new(base_url: impl Into<String>, api_key: String, api_secret: String) -> Result<Self> {
        if api_key.is_empty() || api_secret.is_empty() {
            return Err(Error::MissingCredentials);
        }
        let client = HttpClient::new(
            Self::default_headers(),
            vec![],
            vec![],
            None,
            Some(DEFAULT_TIMEOUT_SECS),
            None,
        )
        .map_err(|e| Error::transport(format!("failed to build HTTP client: {e}")))?;

        Ok(Self {
            client,
            base_url: base_url.into(),
            api_key,
            api_secret,
        })
    }

    fn default_headers() -> HashMap<String, String> {
        HashMap::from([
            (USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
        ])
    }

    /// The Alpaca auth headers (static key/secret, sent on every request).
    fn auth_headers(&self) -> HashMap<String, String> {
        HashMap::from([
            ("APCA-API-KEY-ID".to_string(), self.api_key.clone()),
            ("APCA-API-SECRET-KEY".to_string(), self.api_secret.clone()),
        ])
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Sends a request and deserializes a successful JSON body into `T`.
    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T> {
        let value = self.send_raw(method, path, body).await?;
        serde_json::from_value(value).map_err(Error::Serde)
    }

    /// Sends a request and returns the raw JSON `Value` (or `Null` for an empty body).
    async fn send_raw(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> Result<Value> {
        let url = self.build_url(path);
        let headers = self.auth_headers();
        let response = self
            .client
            .request(method, url, None, Some(headers), body, None, None)
            .await
            .map_err(|e| Error::transport(e.to_string()))?;

        if !response.status.is_success() {
            return Err(Error::from_status(response.status.as_u16(), &response.body));
        }
        if response.body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&response.body).map_err(Error::Serde)
    }

    // ----- endpoints --------------------------------------------------------

    /// `GET /v2/account` — the account snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response.
    pub async fn get_account(&self) -> Result<AlpacaAccount> {
        self.send(Method::GET, "/v2/account", None).await
    }

    /// `GET /v2/positions` — all open positions.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response.
    pub async fn get_positions(&self) -> Result<Vec<AlpacaPosition>> {
        self.send(Method::GET, "/v2/positions", None).await
    }

    /// `GET /v2/orders?status=open` — currently-open orders (connect-time priming).
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response.
    pub async fn get_open_orders(&self) -> Result<Vec<AlpacaOrder>> {
        self.send(Method::GET, "/v2/orders?status=open", None).await
    }

    /// `POST /v2/orders` — submit a new order.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response (Alpaca rejection).
    pub async fn submit_order(&self, request: &CreateOrderRequest) -> Result<AlpacaOrder> {
        let body = serde_json::to_vec(request).map_err(Error::Serde)?;
        self.send(Method::POST, "/v2/orders", Some(body)).await
    }

    /// `DELETE /v2/orders/{id}` — cancel an open order by venue order id.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response.
    pub async fn cancel_order(&self, venue_order_id: &str) -> Result<()> {
        let path = format!("/v2/orders/{venue_order_id}");
        self.send_raw(Method::DELETE, &path, None).await.map(|_| ())
    }

    /// `DELETE /v2/orders` — cancel all open orders (returns 207 multi-status).
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure. The 207 body lists per-order outcomes; we treat the
    /// call as best-effort and do not fail on partial cancels.
    pub async fn cancel_all_orders(&self) -> Result<()> {
        self.send_raw(Method::DELETE, "/v2/orders", None)
            .await
            .map(|_| ())
    }

    /// `PATCH /v2/orders/{id}` — replace an open order's qty / limit / stop price.
    ///
    /// # Errors
    ///
    /// Returns an error on transport failure or a non-2xx response.
    pub async fn replace_order(
        &self,
        venue_order_id: &str,
        request: &ReplaceOrderRequest,
    ) -> Result<AlpacaOrder> {
        let path = format!("/v2/orders/{venue_order_id}");
        let body = serde_json::to_vec(request).map_err(Error::Serde)?;
        self.send(Method::PATCH, &path, Some(body)).await
    }
}
