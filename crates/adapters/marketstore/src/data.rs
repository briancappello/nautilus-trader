//! The live MarketStore `DataClient` (in-wheel).
//!
//! Compiled INTO the nautilus wheel so it shares the engine's ABI (cross-cdylib boundary
//! — see `docs/marketstore_v2_rewrite_plan.md` §5 #14). It therefore builds the wheel's own
//! `Bar`/`InstrumentAny` directly and pushes them onto the engine's data-event channel.
//!
//! This first cut implements the request/response (historical) path + instrument provision;
//! WebSocket live streaming (`subscribe_bars`) lands in Phase 3.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, InstrumentsResponse, RequestBars, RequestInstruments,
            SubscribeBars, UnsubscribeBars,
        },
    },
};
use nautilus_core::{
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::bar::{BarType, get_bar_interval_ns},
    identifiers::{ClientId, Venue},
    instruments::InstrumentAny,
};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::MarketStoreDataClientConfig,
    grpc::MarketStoreGrpcClient,
    instruments::instruments_from_config,
    loader::load_bars,
    symbology::bar_type_to_tbk,
    ws::{ReplayWindow, TbkSubscription, spawn_ws_session},
};

/// Derives the `/ws/replay` endpoint from the configured live `/ws` endpoint.
///
/// `ws://host:5993/ws` -> `ws://host:5993/ws/replay`. If the endpoint already targets
/// `/ws/replay` it is returned unchanged.
fn replay_endpoint(ws_endpoint: &str) -> String {
    if ws_endpoint.ends_with("/ws/replay") {
        ws_endpoint.to_string()
    } else if let Some(base) = ws_endpoint.strip_suffix("/ws") {
        format!("{base}/ws/replay")
    } else {
        // Fall back to appending; covers custom endpoints.
        format!("{}/replay", ws_endpoint.trim_end_matches('/'))
    }
}

/// The live MarketStore data client.
#[derive(Debug)]
pub struct MarketStoreDataClient {
    client_id: ClientId,
    venue: Venue,
    config: MarketStoreDataClientConfig,
    instruments: Vec<InstrumentAny>,
    is_connected: AtomicBool,
    data_sender: UnboundedSender<DataEvent>,
    clock: &'static AtomicTime,
    /// Active bar subscriptions keyed by TBK (the WS `key`); rebuilt into a WS session
    /// whenever the set changes (MarketStore has no per-TBK unsubscribe).
    subscriptions: HashMap<String, TbkSubscription>,
    /// Cancels the running WS session task (if any).
    ws_cancel: Option<CancellationToken>,
    /// Handle to the running WS session task.
    ws_task: Option<JoinHandle<()>>,
}

impl MarketStoreDataClient {
    /// Creates a new [`MarketStoreDataClient`] from a resolved config.
    ///
    /// # Errors
    ///
    /// Returns an error if the data-event sender is unavailable (not on a live runtime).
    pub fn new(client_id: ClientId, config: MarketStoreDataClientConfig) -> anyhow::Result<Self> {
        let venue = Venue::new(&config.venue);
        let instruments = instruments_from_config(&config);
        let data_sender = get_data_event_sender();
        let clock = get_atomic_clock_realtime();
        Ok(Self {
            client_id,
            venue,
            config,
            instruments,
            is_connected: AtomicBool::new(false),
            data_sender,
            clock,
            subscriptions: HashMap::new(),
            ws_cancel: None,
            ws_task: None,
        })
    }

    /// (Re)starts the WS streaming session covering the current subscription set.
    ///
    /// MarketStore takes the full TBK list at subscribe time and has no granular
    /// unsubscribe, so any change tears down and reopens the session with the new set.
    fn restart_ws_session(&mut self) {
        // Cancel any existing session.
        if let Some(token) = self.ws_cancel.take() {
            token.cancel();
        }
        self.ws_task = None;

        if self.subscriptions.is_empty() {
            return;
        }

        // Replay mode connects to /ws/replay with a window; live uses the configured /ws.
        let (ws_url, replay) = match &self.config.replay {
            Some(r) => (
                replay_endpoint(&self.config.ws_endpoint),
                Some(ReplayWindow {
                    start: r.start.clone(),
                    end: r.end.clone(),
                    step: r.step,
                }),
            ),
            None => (self.config.ws_endpoint.clone(), None),
        };

        let cancel = CancellationToken::new();
        let handle = spawn_ws_session(
            ws_url,
            self.subscriptions.clone(),
            replay,
            self.data_sender.clone(),
            cancel.clone(),
        );
        self.ws_cancel = Some(cancel);
        self.ws_task = Some(handle);
    }

    /// Resolves `(price_precision, size_precision)` for a `BarType`'s symbol.
    fn precision_for_bar_type(&self, bar_type: &BarType) -> (u8, u8) {
        self.precision_for(bar_type.instrument_id().symbol.as_str())
    }

    /// Resolves the display precision `(price, size)` for `symbol`, falling back to the
    /// config defaults for symbols not explicitly specified.
    fn precision_for(&self, symbol: &str) -> (u8, u8) {
        self.config
            .instruments
            .iter()
            .find(|s| s.symbol == symbol)
            .map_or(
                (self.config.price_precision, self.config.size_precision),
                |s| (s.price_precision, s.size_precision),
            )
    }
}

#[async_trait(?Send)]
impl DataClient for MarketStoreDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        // MarketStore is venue-agnostic; the configured venue is baked into each
        // synthesized `InstrumentId`, so report multi-venue (None) like tardis/bybit.
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(token) = self.ws_cancel.take() {
            token.cancel();
        }
        self.ws_task = None;
        self.is_connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        // Bootstrap: push the synthesized instruments onto the bus so strategies/scanners
        // can look them up. Live bar streaming starts on the first `subscribe_bars`
        // (MarketStore takes the TBK set at subscribe time).
        for instrument in &self.instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::error!("Failed to send instrument on connect: {e}");
            }
        }
        self.is_connected.store(true, Ordering::SeqCst);
        // If subscriptions were registered before connect, (re)start the session now.
        if !self.subscriptions.is_empty() {
            self.restart_ws_session();
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(token) = self.ws_cancel.take() {
            token.cancel();
        }
        self.ws_task = None;
        self.is_connected.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd.bar_type;
        let tbk = bar_type_to_tbk(&bar_type)?;
        let (price_precision, size_precision) = self.precision_for_bar_type(&bar_type);
        self.subscriptions.insert(
            tbk,
            TbkSubscription {
                bar_type,
                price_precision,
                size_precision,
                ts_init_delta_ns: get_bar_interval_ns(&bar_type).as_u64(),
            },
        );
        // (Re)start the session with the new superset (no granular subscribe over WS).
        if self.is_connected() {
            self.restart_ws_session();
        }
        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let tbk = bar_type_to_tbk(&cmd.bar_type)?;
        self.subscriptions.remove(&tbk);
        if self.is_connected() {
            self.restart_ws_session();
        }
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let sender = self.data_sender.clone();
        let endpoint = self.config.grpc_endpoint.clone();
        let bar_type = request.bar_type;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let params = request.params;
        let clock = self.clock;
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let limit = request.limit.map_or(0, |n| n.get() as i32);
        let (price_precision, size_precision) =
            self.precision_for(bar_type.instrument_id().symbol.as_str());

        get_runtime().spawn(async move {
            let client = match MarketStoreGrpcClient::connect(endpoint).await {
                Ok(c) => c,
                Err(e) => {
                    log::error!("MarketStore connect failed for bars request: {e:?}");
                    return;
                }
            };
            match load_bars(
                &client,
                bar_type,
                start_nanos,
                end_nanos,
                limit,
                price_precision,
                size_precision,
            )
            .await
            {
                Ok(bars) => {
                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        client_id,
                        bar_type,
                        bars,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));
                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send bars response: {e}");
                    }
                }
                Err(e) => log::error!("MarketStore bars request failed: {e:?}"),
            }
        });

        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let response = DataResponse::Instruments(InstrumentsResponse::new(
            request_id,
            client_id,
            self.venue,
            self.instruments.clone(),
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        ));
        if let Err(e) = self.data_sender.send(DataEvent::Response(response)) {
            log::error!("Failed to send instruments response: {e}");
        }
        Ok(())
    }
}
