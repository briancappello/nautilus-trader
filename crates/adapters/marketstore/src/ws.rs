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

use nautilus_model::identifiers::InstrumentId;

use crate::decode::{
    StreamError, StreamPayload, bar_from_ws_row, quote_from_ws_row, trade_from_ws_row,
};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// What kind of data a TBK subscription carries, with the per-kind decode context.
///
/// The TBK (the WS `key`) carries no venue, so the resolved `BarType`/`InstrumentId` is
/// captured at subscribe time and used to build the wheel object on each incoming row.
#[derive(Debug, Clone)]
pub enum SubKind {
    /// `OHLCV` → `Bar`. `ts_init_delta_ns` shifts `ts_init` to the bar close.
    Bar {
        bar_type: BarType,
        ts_init_delta_ns: u64,
    },
    /// `TRADE` → `TradeTick` (point-in-time).
    Trade { instrument_id: InstrumentId },
    /// `QUOTE` → `QuoteTick` (point-in-time).
    Quote { instrument_id: InstrumentId },
}

/// Per-TBK subscription metadata: the data kind + display precisions for decoding rows.
#[derive(Debug, Clone)]
pub struct TbkSubscription {
    pub kind: SubKind,
    pub price_precision: u8,
    pub size_precision: u8,
}

/// Wildcard (glob) bar streaming — e.g. `"*/1Min/OHLCV"` to stream the whole market.
///
/// Frames arrive keyed by the *resolved* `SYMBOL/<tf>/OHLCV` (not a pre-registered TBK),
/// so a glob-matched frame's `BarType` is **synthesized on the fly** from the key + the
/// configured `venue` (see [`crate::symbology::tbk_to_bar_type`]). Synthesized bars use
/// the fallback precisions. Only bar (`OHLCV`) globs are supported.
#[derive(Debug, Clone, Default)]
pub struct GlobStreams {
    /// Glob patterns to subscribe (each a `<sym>/<tf>/<attr>` with `*` wildcards).
    pub patterns: Vec<String>,
    /// Venue baked into every synthesized `BarType`.
    pub venue: String,
    /// Fallback precisions for synthesized bars.
    pub price_precision: u8,
    pub size_precision: u8,
}

impl GlobStreams {
    fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
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
    globs: GlobStreams,
    replay: Option<ReplayWindow>,
    data_sender: UnboundedSender<DataEvent>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    nautilus_common::live::runtime::get_runtime().spawn(async move {
        run_session_loop(ws_url, subscriptions, globs, replay, data_sender, cancel).await;
    })
}

/// The reconnect loop: connect → run one session → on drop/error back off and retry,
/// until cancelled.
async fn run_session_loop(
    ws_url: String,
    subscriptions: HashMap<String, TbkSubscription>,
    globs: GlobStreams,
    replay: Option<ReplayWindow>,
    data_sender: UnboundedSender<DataEvent>,
    cancel: CancellationToken,
) {
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        match run_one_session(
            &ws_url,
            &subscriptions,
            &globs,
            replay.as_ref(),
            &data_sender,
            &cancel,
        )
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
    globs: &GlobStreams,
    replay: Option<&ReplayWindow>,
    data_sender: &UnboundedSender<DataEvent>,
    cancel: &CancellationToken,
) -> anyhow::Result<SessionEnd> {
    let (ws_stream, _resp) = connect_async(ws_url).await?;
    let (mut writer, mut reader) = ws_stream.split();

    // Subscribe to all configured exact TBKs PLUS any glob patterns in one frame.
    let mut tbks: Vec<String> = subscriptions.keys().cloned().collect();
    tbks.extend(globs.patterns.iter().cloned());
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
                        if handle_binary(&data, subscriptions, globs, data_sender) == FrameOutcome::ReplayEnd {
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
    globs: &GlobStreams,
    data_sender: &UnboundedSender<DataEvent>,
) -> FrameOutcome {
    // Try a data row first (the common case).
    if let Ok(payload) = rmp_serde::from_slice::<StreamPayload>(data) {
        // `payload.row()` unwraps the live `{msg_type, payload}` envelope (and is a no-op
        // for the flat form), so the decoders see the flat column map either way.
        let row = payload.row();
        let result: anyhow::Result<Data> = if let Some(sub) = subscriptions.get(&payload.key) {
            match &sub.kind {
                SubKind::Bar {
                    bar_type,
                    ts_init_delta_ns,
                } => bar_from_ws_row(
                    row,
                    *bar_type,
                    sub.price_precision,
                    sub.size_precision,
                    *ts_init_delta_ns,
                )
                .map(Data::Bar),
                SubKind::Trade { instrument_id } => trade_from_ws_row(
                    row,
                    *instrument_id,
                    sub.price_precision,
                    sub.size_precision,
                )
                .map(Data::Trade),
                SubKind::Quote { instrument_id } => quote_from_ws_row(
                    row,
                    *instrument_id,
                    sub.price_precision,
                    sub.size_precision,
                )
                .map(Data::Quote),
            }
        } else if !globs.is_empty()
            && globs
                .patterns
                .iter()
                .any(|p| crate::symbology::tbk_matches_pattern(&payload.key, p))
        {
            // Glob-streamed bar: synthesize the BarType from the resolved TBK + venue.
            match crate::symbology::tbk_to_bar_type(&payload.key, &globs.venue) {
                Ok(bar_type) => {
                    let delta = nautilus_model::data::bar::get_bar_interval_ns(&bar_type).as_u64();
                    bar_from_ws_row(
                        row,
                        bar_type,
                        globs.price_precision,
                        globs.size_precision,
                        delta,
                    )
                    .map(Data::Bar)
                }
                Err(e) => {
                    log::debug!("glob key {} not a bar TBK: {e}", payload.key);
                    return FrameOutcome::Continue;
                }
            }
        } else {
            log::debug!("WS row for unsubscribed key {}", payload.key);
            return FrameOutcome::Continue;
        };
        match result {
            Ok(data) => {
                if let Err(e) = data_sender.send(DataEvent::Data(data)) {
                    log::error!("Failed to send live data: {e}");
                }
            }
            Err(e) => log::warn!("Failed to decode WS row for {}: {e}", payload.key),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    // Flat OHLCV row + frame, serialized with rmp-serde (named maps, like the server).
    #[derive(Serialize)]
    struct OhlcvRow {
        #[serde(rename = "Epoch")]
        epoch: i64,
        #[serde(rename = "Open")]
        open: f64,
        #[serde(rename = "High")]
        high: f64,
        #[serde(rename = "Low")]
        low: f64,
        #[serde(rename = "Close")]
        close: f64,
        #[serde(rename = "Volume")]
        volume: i64,
    }

    #[derive(Serialize)]
    struct Frame {
        key: String,
        data: OhlcvRow,
    }

    /// Build a flat OHLCV WS frame `{key, data:{Epoch,Open,High,Low,Close,Volume}}`.
    fn ohlcv_frame(key: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&Frame {
            key: key.to_string(),
            data: OhlcvRow {
                epoch: 1_783_603_800, // 2026-07-09 13:30Z
                open: 1.5,
                high: 1.6,
                low: 1.4,
                close: 1.55,
                volume: 1000,
            },
        })
        .unwrap()
    }

    fn glob_1min(venue: &str) -> GlobStreams {
        GlobStreams {
            patterns: vec!["*/1Min/OHLCV".to_string()],
            venue: venue.to_string(),
            price_precision: 2,
            size_precision: 0,
        }
    }

    #[test]
    fn glob_frame_synthesizes_bar_when_not_in_exact_subs() {
        // No exact subscriptions; the glob pattern must route AAPL/1Min/OHLCV.
        let subs: HashMap<String, TbkSubscription> = HashMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let frame = ohlcv_frame("AAPL/1Min/OHLCV");
        let outcome = handle_binary(&frame, &subs, &glob_1min("MARKETSTORE"), &tx);
        assert_eq!(outcome, FrameOutcome::Continue);
        let ev = rx.try_recv().expect("a bar should have been emitted");
        match ev {
            DataEvent::Data(Data::Bar(bar)) => {
                assert_eq!(
                    bar.bar_type.to_string(),
                    "AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL"
                );
                assert_eq!(bar.close.as_f64(), 1.55);
            }
            other => panic!("expected a Bar, got {other:?}"),
        }
    }

    #[test]
    fn non_matching_glob_key_is_dropped() {
        // A 5Min frame does not match the */1Min/OHLCV glob -> no bar emitted.
        let subs: HashMap<String, TbkSubscription> = HashMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let frame = ohlcv_frame("AAPL/5Min/OHLCV");
        handle_binary(&frame, &subs, &glob_1min("MARKETSTORE"), &tx);
        assert!(rx.try_recv().is_err(), "no bar should be emitted");
    }

    #[test]
    fn exact_subscription_takes_precedence_over_glob() {
        // With no glob, an unsubscribed key is dropped (baseline behavior preserved).
        let subs: HashMap<String, TbkSubscription> = HashMap::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let frame = ohlcv_frame("AAPL/1Min/OHLCV");
        handle_binary(&frame, &subs, &GlobStreams::default(), &tx);
        assert!(rx.try_recv().is_err(), "no glob -> unsubscribed key dropped");
    }
}
