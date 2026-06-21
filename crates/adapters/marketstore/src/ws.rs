//! MarketStore WebSocket streaming session (live bars).
//!
//! MarketStore's only push transport is `/ws` (msgpack). The wire protocol (fixed
//! server-side in `frontend/stream/stream.go`):
//! - **Subscribe:** a binary msgpack frame `{ "action": "subscribe", "tbks": [<TBK>, ...] }`.
//! - **First reply:** a msgpack map — `{ "error": "<msg>" }` on failure, else a confirmation.
//! - **Data frames:** binary msgpack `{ "key": "<TBK>", "data": { <col>: <scalar>, ... } }`.
//! - **Heartbeat:** the server pings ~every 54s and expects a pong within 60s.
//!
//! This session connects, subscribes the configured TBKs, decodes each `OHLCV` row into
//! a wheel `Bar` (via the shared [`crate::decode::bar_from_ws_row`]), and pushes it onto
//! the engine's data-event channel as `DataEvent::Data(Data::Bar(_))`. It reconnects with
//! exponential backoff and is cancellable via a [`CancellationToken`].

use std::{collections::HashMap, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nautilus_common::messages::DataEvent;
use nautilus_model::{data::Data, data::bar::BarType};
use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::{connect_async, tungstenite};
use tokio_util::sync::CancellationToken;

use crate::decode::{StreamError, StreamPayload, bar_from_ws_row};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Per-TBK subscription metadata: the resolved `BarType` (the TBK carries no venue), the
/// display precisions to apply when decoding rows, and the bar interval used to shift
/// `ts_init` to the bar close.
#[derive(Debug, Clone)]
pub struct TbkSubscription {
    pub bar_type: BarType,
    pub price_precision: u8,
    pub size_precision: u8,
    pub ts_init_delta_ns: u64,
}

/// A replay window for the `/ws/replay` endpoint (off-hours live simulation).
#[derive(Debug, Clone)]
pub struct ReplayWindow {
    pub start: String,
    pub end: String,
    pub step: i64,
}

/// The live subscribe message (binary msgpack frame): `{action, tbks}`.
#[derive(Debug, Serialize)]
struct SubscribeMessage {
    action: &'static str,
    tbks: Vec<String>,
}

/// The replay subscribe message: `{action, tbks, start, end, step}`.
#[derive(Debug, Serialize)]
struct ReplaySubscribeMessage {
    action: &'static str,
    tbks: Vec<String>,
    start: String,
    end: String,
    step: i64,
}

/// Spawns the WebSocket streaming session as a background task on the live runtime.
///
/// The task runs until `cancel` is triggered. `subscriptions` maps TBK -> metadata; the
/// task subscribes every TBK and routes incoming rows back to their `BarType`. Each decoded
/// bar is sent on `data_sender` as `DataEvent::Data`.
///
/// Each subscription carries its own `ts_init_delta_ns` (bar interval) so `ts_init` lands
/// on the bar close (no look-ahead), matching the historical decode. Returns immediately;
/// connection happens inside the task (with retry).
pub fn spawn_ws_session(
    ws_url: String,
    subscriptions: HashMap<String, TbkSubscription>,
    replay: Option<ReplayWindow>,
    data_sender: UnboundedSender<DataEvent>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    nautilus_common::live::runtime::get_runtime().spawn(async move {
        run_session_loop(ws_url, subscriptions, replay, data_sender, cancel).await;
    })
}

/// The reconnect loop: connect → run one session → on drop/error back off and retry,
/// until cancelled.
async fn run_session_loop(
    ws_url: String,
    subscriptions: HashMap<String, TbkSubscription>,
    replay: Option<ReplayWindow>,
    data_sender: UnboundedSender<DataEvent>,
    cancel: CancellationToken,
) {
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        match run_one_session(&ws_url, &subscriptions, replay.as_ref(), &data_sender, &cancel)
            .await
        {
            Ok(SessionEnd::Cancelled | SessionEnd::ReplayComplete) => {
                // Clean shutdown — do not reconnect (replay is one-shot; cancel is final).
                return;
            }
            Err(e) => {
                if cancel.is_cancelled() {
                    return;
                }
                // Replay connections are one-shot; reconnecting would re-replay. Only
                // live sessions reconnect.
                if replay.is_some() {
                    log::warn!("MarketStore replay session error: {e}");
                    return;
                }
                log::warn!("MarketStore WS session ended ({e}); reconnecting in {backoff:?}");
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// How a single session ended (clean cases; errors propagate via `Err`).
enum SessionEnd {
    /// The `CancellationToken` was triggered.
    Cancelled,
    /// The replay server sent `{action: "end"}`.
    ReplayComplete,
}

/// One connection lifecycle: connect, subscribe, receive until error/close/cancel.
async fn run_one_session(
    ws_url: &str,
    subscriptions: &HashMap<String, TbkSubscription>,
    replay: Option<&ReplayWindow>,
    data_sender: &UnboundedSender<DataEvent>,
    cancel: &CancellationToken,
) -> anyhow::Result<SessionEnd> {
    let (ws_stream, _resp) = connect_async(ws_url).await?;
    let (mut writer, mut reader) = ws_stream.split();

    // Subscribe to all configured TBKs in one frame. Replay carries start/end/step.
    let tbks: Vec<String> = subscriptions.keys().cloned().collect();
    let frame = match replay {
        Some(window) => rmp_serde::to_vec_named(&ReplaySubscribeMessage {
            action: "subscribe",
            tbks: tbks.clone(),
            start: window.start.clone(),
            end: window.end.clone(),
            step: window.step,
        })?,
        None => rmp_serde::to_vec_named(&SubscribeMessage {
            action: "subscribe",
            tbks: tbks.clone(),
        })?,
    };
    writer.send(tungstenite::Message::Binary(frame.into())).await?;
    log::info!(
        "MarketStore {} subscribed: {tbks:?}",
        if replay.is_some() { "replay" } else { "WS" }
    );

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                let _ = writer.send(tungstenite::Message::Close(None)).await;
                return Ok(SessionEnd::Cancelled);
            }
            msg = reader.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Binary(data))) => {
                        if handle_binary(&data, subscriptions, data_sender) == FrameOutcome::ReplayEnd {
                            log::info!("MarketStore replay complete");
                            let _ = writer.send(tungstenite::Message::Close(None)).await;
                            return Ok(SessionEnd::ReplayComplete);
                        }
                    }
                    Some(Ok(tungstenite::Message::Ping(payload))) => {
                        // Respond to keep the connection alive (server pongWait = 60s).
                        let _ = writer.send(tungstenite::Message::Pong(payload)).await;
                    }
                    Some(Ok(tungstenite::Message::Close(frame))) => {
                        // Replay closes the socket after `end`; treat as completion.
                        if replay.is_some() {
                            return Ok(SessionEnd::ReplayComplete);
                        }
                        anyhow::bail!("server closed connection: {frame:?}");
                    }
                    Some(Ok(_)) => { /* ignore Text/Pong/Frame */ }
                    Some(Err(e)) => anyhow::bail!("WS read error: {e}"),
                    None => {
                        if replay.is_some() {
                            return Ok(SessionEnd::ReplayComplete);
                        }
                        anyhow::bail!("WS stream ended");
                    }
                }
            }
        }
    }
}

/// Outcome of decoding one frame.
#[derive(Debug, PartialEq, Eq)]
enum FrameOutcome {
    /// A data row, error, or control frame was handled; keep reading.
    Continue,
    /// The replay server signalled `{action: "end"}` — end the session.
    ReplayEnd,
}

/// A control frame: `{action: "subscribed" | "end"}`.
#[derive(Debug, serde::Deserialize)]
struct ControlMessage {
    action: String,
}

/// Decodes one binary msgpack frame: a data row (`{key,data}`), an error map, a control
/// frame (`subscribed`/`end`), or an unknown frame (ignored).
fn handle_binary(
    data: &[u8],
    subscriptions: &HashMap<String, TbkSubscription>,
    data_sender: &UnboundedSender<DataEvent>,
) -> FrameOutcome {
    // Try a data row first (the common case).
    if let Ok(payload) = rmp_serde::from_slice::<StreamPayload>(data) {
        let Some(sub) = subscriptions.get(&payload.key) else {
            log::debug!("WS row for unsubscribed key {}", payload.key);
            return FrameOutcome::Continue;
        };
        match bar_from_ws_row(
            &payload.data,
            sub.bar_type,
            sub.price_precision,
            sub.size_precision,
            sub.ts_init_delta_ns,
        ) {
            Ok(bar) => {
                if let Err(e) = data_sender.send(DataEvent::Data(Data::Bar(bar))) {
                    log::error!("Failed to send live bar: {e}");
                }
            }
            Err(e) => log::warn!("Failed to decode WS bar for {}: {e}", payload.key),
        }
        return FrameOutcome::Continue;
    }

    // Control frame: subscribe confirmation or replay end.
    if let Ok(ctrl) = rmp_serde::from_slice::<ControlMessage>(data) {
        if ctrl.action == "end" {
            return FrameOutcome::ReplayEnd;
        }
        // "subscribed" or other control: ignore.
        return FrameOutcome::Continue;
    }

    // Otherwise it may be an error map.
    if let Ok(err) = rmp_serde::from_slice::<StreamError>(data) {
        log::error!("MarketStore WS error: {}", err.error);
    }
    FrameOutcome::Continue
}
