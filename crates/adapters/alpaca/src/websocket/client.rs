//! The Alpaca trade-updates WebSocket client.
//!
//! Far simpler than a multi-channel market-data feed: there is exactly one stream
//! (`trade_updates`) and one subscription. On [`connect`](AlpacaWebSocketClient::connect) the
//! client opens the socket (with the network layer's auto-reconnect/backoff), authenticates with
//! the API key/secret, subscribes to `trade_updates`, and spawns a task that parses each inbound
//! frame into an [`AlpacaTradeUpdate`] forwarded over an unbounded channel. The execution client
//! takes that receiver via [`take_out_rx`](AlpacaWebSocketClient::take_out_rx) and maps updates
//! to order events.

use nautilus_network::websocket::{WebSocketClient, WebSocketConfig, channel_message_handler};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::models::{AlpacaAuthData, AlpacaTradeUpdate, AlpacaWsEnvelope};

/// A parsed message handed to the execution client.
#[derive(Clone, Debug)]
pub enum AlpacaWsMessage {
    /// A trade-update lifecycle event.
    TradeUpdate(Box<AlpacaTradeUpdate>),
    /// The connection was re-established (the exec client should refresh account state).
    Reconnected,
}

/// Heartbeat interval (seconds) for transport keep-alive.
const WS_HEARTBEAT_SECS: u64 = 10;
/// Reconnect backoff bounds (milliseconds).
const RECONNECT_DELAY_INITIAL_MS: u64 = 1_000;
const RECONNECT_DELAY_MAX_MS: u64 = 30_000;
const RECONNECT_TIMEOUT_MS: u64 = 60_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;
const RECONNECT_JITTER_MS: u64 = 250;

/// The Alpaca trade-updates WebSocket client.
#[derive(Debug)]
pub struct AlpacaWebSocketClient {
    url: String,
    api_key: String,
    api_secret: String,
    inner: Option<WebSocketClient>,
    out_rx: Option<mpsc::UnboundedReceiver<AlpacaWsMessage>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AlpacaWebSocketClient {
    /// Creates a new (unconnected) client.
    #[must_use]
    pub fn new(url: impl Into<String>, api_key: String, api_secret: String) -> Self {
        Self {
            url: url.into(),
            api_key,
            api_secret,
            inner: None,
            out_rx: None,
            task_handle: None,
        }
    }

    /// `true` if the socket is connected.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.as_ref().is_some_and(WebSocketClient::is_active)
    }

    /// Opens the socket, authenticates, subscribes to `trade_updates`, and starts the parse task.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or the auth/subscribe sends fail.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_active() {
            log::warn!("Alpaca WS already connected");
            return Ok(());
        }

        let (message_handler, mut raw_rx) = channel_message_handler();
        let config = WebSocketConfig {
            url: self.url.clone(),
            headers: vec![],
            heartbeat: Some(WS_HEARTBEAT_SECS),
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(RECONNECT_TIMEOUT_MS),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL_MS),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX_MS),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER_MS),
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            backend: Default::default(),
            proxy_url: None,
        };

        let client = WebSocketClient::connect(config, Some(message_handler), None, None, vec![], None)
            .await
            .map_err(|e| anyhow::anyhow!("Alpaca WS connect failed: {e}"))?;

        // Authenticate + subscribe. Alpaca's `/stream` accepts the auth/listen control frames as
        // plain JSON text; we send and let the parse task confirm the authorization reply.
        let auth = serde_json::json!({
            "action": "auth",
            "key": self.api_key,
            "secret": self.api_secret,
        })
        .to_string();
        client
            .send_text(auth, None)
            .await
            .map_err(|e| anyhow::anyhow!("Alpaca WS auth send failed: {e}"))?;

        let listen = serde_json::json!({
            "action": "listen",
            "data": {"streams": ["trade_updates"]},
        })
        .to_string();
        client
            .send_text(listen, None)
            .await
            .map_err(|e| anyhow::anyhow!("Alpaca WS listen send failed: {e}"))?;

        let (out_tx, out_rx) = mpsc::unbounded_channel::<AlpacaWsMessage>();

        let handle = nautilus_common::live::get_runtime().spawn(async move {
            while let Some(msg) = raw_rx.recv().await {
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                        Ok(t) => t,
                        Err(_) => continue,
                    },
                    Message::Close(_) => {
                        log::info!("Alpaca WS received close frame");
                        continue;
                    }
                    _ => continue,
                };

                let envelope: AlpacaWsEnvelope = match serde_json::from_str(&text) {
                    Ok(e) => e,
                    Err(e) => {
                        log::debug!("Alpaca WS: dropping unparseable frame: {e}");
                        continue;
                    }
                };

                match envelope.stream.as_str() {
                    "trade_updates" => {
                        match serde_json::from_value::<AlpacaTradeUpdate>(envelope.data) {
                            Ok(update) => {
                                if out_tx
                                    .send(AlpacaWsMessage::TradeUpdate(Box::new(update)))
                                    .is_err()
                                {
                                    break; // receiver dropped
                                }
                            }
                            Err(e) => log::warn!("Alpaca WS: bad trade_update payload: {e}"),
                        }
                    }
                    "authorization" => {
                        match serde_json::from_value::<AlpacaAuthData>(envelope.data) {
                            Ok(a) if a.status == "authorized" => {
                                log::info!("Alpaca WS authorized");
                            }
                            Ok(a) => log::error!("Alpaca WS authorization failed: {}", a.status),
                            Err(e) => log::warn!("Alpaca WS: bad authorization payload: {e}"),
                        }
                    }
                    "listening" => log::info!("Alpaca WS listening on trade_updates"),
                    other => log::debug!("Alpaca WS: ignoring stream '{other}'"),
                }
            }
            log::info!("Alpaca WS parse task stopped");
        });

        self.inner = Some(client);
        self.out_rx = Some(out_rx);
        self.task_handle = Some(handle);
        log::info!("Alpaca WS connected: {}", self.url);
        Ok(())
    }

    /// Takes the parsed-message receiver, leaving `None` behind.
    pub fn take_out_rx(&mut self) -> Option<mpsc::UnboundedReceiver<AlpacaWsMessage>> {
        self.out_rx.take()
    }

    /// Closes the socket and aborts the parse task.
    pub async fn disconnect(&mut self) {
        if let Some(client) = &self.inner {
            client.disconnect().await;
        }
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        self.inner = None;
        log::info!("Alpaca WS disconnected");
    }
}
