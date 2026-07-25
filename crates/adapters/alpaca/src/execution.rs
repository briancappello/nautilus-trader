//! The Alpaca execution client (M5.1: REST order path + connect-time state priming).
//!
//! Implements `ExecutionClient` for Alpaca paper/live, routing the strategy's `Order` outflow to
//! the Alpaca Trading API v2. Structured on the coinbase adapter: an [`ExecutionClientCore`] for
//! identity + cache, an [`ExecutionEventEmitter`] for lifecycle events, an [`AlpacaHttpClient`]
//! for REST, and a pending-task set for spawned async work (the sync `submit_order`/`cancel_order`
//! commands spawn their network calls onto the global runtime).
//!
//! The [`crate::gatekeeper`] risk safeguard runs synchronously inside `submit_order`, before any
//! network I/O: a denied order emits `OrderDenied` and never reaches the wire.
//!
//! Still to come: trade-updates WS + fills (M5.2), the §5a protocol validator (M5.3), brackets
//! (M5.4), and full reconciliation reports (M5.5).

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateFillReportsBuilder, GenerateOrderStatusReports, GenerateOrderStatusReportsBuilder,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder, ModifyOrder,
        SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    MUTEX_POISONED, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::{Account, AccountAny},
    enums::{LiquiditySide, OmsType},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    orders::Order,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::{
    common::ALPACA_VENUE,
    config::AlpacaExecClientConfig,
    gatekeeper::{self, AccountInfo, GatekeeperLimits, OrderInfo},
    http::{
        AlpacaHttpClient,
        models::CreateOrderRequest,
        parse::{order_side_to_alpaca, order_type_to_alpaca, time_in_force_to_alpaca},
    },
    session::UsEquityCalendar,
    validator::{OrderShape, OrderValidator, QUEUE_FOR_OPEN_TAG, Validity},
    websocket::{AlpacaWebSocketClient, AlpacaWsMessage, models::AlpacaTradeUpdate},
};

/// Timeout (seconds) waiting for the engine to register the primed account.
const ACCOUNT_REGISTERED_TIMEOUT_SECS: f64 = 30.0;

/// Live execution client for Alpaca (paper + live).
#[derive(Debug)]
pub struct AlpacaExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: AlpacaExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: AlpacaHttpClient,
    ws_client: AlpacaWebSocketClient,
    validator: OrderValidator<UsEquityCalendar>,
    limits: GatekeeperLimits,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
    ws_task: Option<JoinHandle<()>>,
}

impl AlpacaExecutionClient {
    /// Creates a new [`AlpacaExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if credentials are missing or the HTTP client / gatekeeper config fails
    /// to build.
    pub fn new(core: ExecutionClientCore, config: AlpacaExecClientConfig) -> anyhow::Result<Self> {
        let api_key = config
            .api_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Alpaca API key not configured"))?;
        let api_secret = config
            .api_secret
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Alpaca API secret not configured"))?;

        let http_client = AlpacaHttpClient::new(
            config.resolved_api_base_url().to_string(),
            api_key.clone(),
            api_secret.clone(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to build Alpaca HTTP client: {e}"))?;

        let ws_client = AlpacaWebSocketClient::new(
            config.resolved_ws_url().to_string(),
            api_key,
            api_secret,
        );

        let limits = GatekeeperLimits::from_env()
            .map_err(|e| anyhow::anyhow!("Invalid ALPACA_GATE_* configuration: {e}"))?;
        if limits.is_empty() {
            log::warn!("Alpaca gatekeeper: no ALPACA_GATE_* limits configured (safeguard inactive)");
        } else {
            log::info!("Alpaca gatekeeper active: {limits:?}");
        }

        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            None,
        );

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            ws_client,
            validator: OrderValidator::new(UsEquityCalendar),
            limits,
            pending_tasks: Mutex::new(Vec::new()),
            ws_task: None,
        })
    }

    /// Spawns async work onto the global runtime, logging failures.
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = get_runtime();
        let handle = runtime.spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        });
        let mut tasks = self.pending_tasks.lock().expect(MUTEX_POISONED);
        tasks.retain(|h| !h.is_finished());
        tasks.push(handle);
    }

    /// Polls the cache until the account is registered (the engine consumed the primed state).
    async fn await_account_registered(&self, timeout_secs: f64) -> anyhow::Result<()> {
        let account_id = self.core.account_id;
        if self.core.cache().account(&account_id).is_some() {
            return Ok(());
        }
        let start = Instant::now();
        let timeout = Duration::from_secs_f64(timeout_secs);
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if self.core.cache().account(&account_id).is_some() {
                log::info!("Account {account_id} registered");
                return Ok(());
            }
            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "Timeout waiting for account {account_id} registration after {timeout_secs}s"
                );
            }
        }
    }

    /// Reads the (Alpaca-primed) cache for the equity + position state the gatekeeper needs.
    ///
    /// Equity is the account's total balance in its base currency (the value `connect` primed and
    /// the WS keeps current). Per-symbol net qty / avg price and total gross exposure are summed
    /// from open positions at cost basis. Returns `equity: None` if the account is not yet primed,
    /// which the gatekeeper treats fail-closed.
    fn account_info_for(&self, instrument_id: InstrumentId) -> AccountInfo {
        let cache = self.core.cache();

        let equity = cache
            .account_owned(&self.core.account_id)
            .and_then(|acct| account_equity(&acct));

        let mut symbol_net_qty = Decimal::ZERO;
        let mut symbol_avg_px = Decimal::ZERO;
        let mut total_gross_exposure = Decimal::ZERO;

        let symbol = instrument_id.symbol;
        for pos in cache.positions_open(Some(&ALPACA_VENUE), None, None, None, None) {
            // Positions expose signed_qty / avg_px_open as f64; convert at the boundary so all
            // gatekeeper valuation stays in exact Decimal arithmetic.
            let signed_qty = f64_to_decimal(pos.signed_qty);
            let avg_px = f64_to_decimal(pos.avg_px_open);
            total_gross_exposure += (signed_qty * avg_px).abs();
            if pos.symbol() == symbol {
                symbol_net_qty = signed_qty;
                symbol_avg_px = avg_px;
            }
        }

        AccountInfo {
            equity,
            symbol_net_qty,
            symbol_avg_px,
            total_gross_exposure,
        }
    }

    /// Resolves a decision-time reference price for `instrument_id` from the cache.
    ///
    /// Used to value price-less orders (market / market-to-limit / trailing) for the
    /// gatekeeper's notional / %-of-equity rules when the order carries no price of its own.
    /// Prefers the latest quote mid, then the latest trade price. Returns `None` if the cache
    /// has neither yet (the gatekeeper then fail-closes value-based rules, as before).
    fn reference_price_for(&self, instrument_id: InstrumentId) -> Option<Price> {
        let cache = self.core.cache();
        if let Some(quote) = cache.quote(&instrument_id) {
            let bid = quote.bid_price.as_f64();
            let ask = quote.ask_price.as_f64();
            if bid > 0.0 && ask > 0.0 {
                return Some(Price::new((bid + ask) / 2.0, quote.bid_price.precision));
            }
        }
        cache.trade(&instrument_id).map(|t| t.price)
    }

    /// Locally checks whether Alpaca would 403 `order` as a wash trade / short-block, using the
    /// current position and open orders on the same symbol from the cache. Returns `Some(reason)`
    /// to reject before any network call. See [`crate::wash`].
    fn wash_reason_for(&self, order: &nautilus_model::orders::OrderAny) -> Option<String> {
        use crate::wash::{OpenOrderView, PendingOrder, check_wash_trade};
        let cache = self.core.cache();
        let instrument_id = order.instrument_id();

        // Signed net position quantity for this symbol.
        let mut net_qty = Decimal::ZERO;
        for pos in cache.positions_open(Some(&ALPACA_VENUE), Some(&instrument_id), None, None, None) {
            net_qty += f64_to_decimal(pos.signed_qty);
        }

        // Open orders on the same symbol (excluding this one), as leaves-qty views.
        let open: Vec<OpenOrderView> = cache
            .orders_open(Some(&ALPACA_VENUE), Some(&instrument_id), None, None, None)
            .into_iter()
            .filter(|o| o.client_order_id() != order.client_order_id())
            .map(|o| OpenOrderView {
                is_buy: o.order_side() == nautilus_model::enums::OrderSide::Buy,
                leaves_qty: f64_to_decimal(o.leaves_qty().as_f64()),
                limit_price: o.price().map(|p| f64_to_decimal(p.as_f64())),
            })
            .collect();

        let pending = PendingOrder {
            is_buy: order.order_side() == nautilus_model::enums::OrderSide::Buy,
            quantity: f64_to_decimal(order.quantity().as_f64()),
            limit_price: order.price().map(|p| f64_to_decimal(p.as_f64())),
        };
        check_wash_trade(&pending, net_qty, &open)
    }
}

impl AlpacaExecutionClient {
    /// Builds an Alpaca `order_class=bracket` request from the entry leg + TP/SL prices.
    fn build_bracket_request(
        &self,
        entry: &nautilus_model::orders::OrderAny,
        tp_px: &str,
        sl_px: &str,
    ) -> anyhow::Result<CreateOrderRequest> {
        let side = order_side_to_alpaca(entry.order_side())
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        let order_type = order_type_to_alpaca(entry.order_type())
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        let tif = time_in_force_to_alpaca(entry.time_in_force())
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        Ok(CreateOrderRequest {
            symbol: entry.instrument_id().symbol.to_string(),
            qty: format_qty(entry.quantity().as_f64()),
            side,
            order_type,
            time_in_force: tif,
            limit_price: entry.price().map(|p| p.to_string()),
            stop_price: None,
            order_class: Some("bracket".to_string()),
            // Bracket entries are simple market/limit — no trailing/GTD legs here.
            trail_percent: None,
            trail_price: None,
            expires_at: None,
            // Brackets are Regular-hours only (validator enforced), so never extended hours.
            extended_hours: None,
            client_order_id: Some(entry.client_order_id().to_string()),
            take_profit: Some(crate::http::models::TakeProfit {
                limit_price: tp_px.to_string(),
            }),
            stop_loss: Some(crate::http::models::StopLoss {
                stop_price: sl_px.to_string(),
                limit_price: None,
            }),
        })
    }

    /// Holds a session-rejected bracket until the next regular open, then submits it.
    fn queue_bracket_until_open(
        &self,
        entry: nautilus_model::orders::OrderAny,
        tp_px: String,
        sl_px: String,
        release: UnixNanos,
    ) {
        let request = match self.build_bracket_request(&entry, &tp_px, &sl_px) {
            Ok(r) => r,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&entry, &format!("queued bracket build failed: {e}"));
                return;
            }
        };
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = entry.strategy_id();
        let instrument_id = entry.instrument_id();
        let client_order_id = entry.client_order_id();
        let delay_ns = release.as_u64().saturating_sub(clock.get_time_ns().as_u64());

        self.spawn_task("queued_bracket", async move {
            tokio::time::sleep(std::time::Duration::from_nanos(delay_ns)).await;
            emitter.emit_order_submitted(&entry);
            match http_client.submit_order(&request).await {
                Ok(resp) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_accepted(&entry, VenueOrderId::new(&resp.id), ts);
                }
                Err(e) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("queued-bracket-rejected: {e}"),
                        ts,
                        false,
                    );
                }
            }
            Ok(())
        });
    }

    /// Holds a session-rejected, `queue_for_open`-tagged order in memory until `release`, then
    /// re-validates and submits it. **Non-durable** (§5a): a process restart drops held orders;
    /// reconciliation (M5.5) re-syncs broker state and the strategy re-decides.
    fn queue_until_open(&self, order: nautilus_model::orders::OrderAny, release: UnixNanos) {
        // Build the request now (translation already known-valid; only the session was wrong).
        let side = match order_side_to_alpaca(order.order_side()) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };
        let order_type = match order_type_to_alpaca(order.order_type()) {
            Ok(t) => t.to_string(),
            Err(_) => return,
        };
        let tif = match time_in_force_to_alpaca(order.time_in_force()) {
            Ok(t) => t.to_string(),
            Err(_) => return,
        };
        let (trail_percent, trail_price) = trailing_params(&order);
        let mut request = CreateOrderRequest {
            symbol: order.instrument_id().symbol.to_string(),
            qty: format_qty(order.quantity().as_f64()),
            side,
            order_type,
            time_in_force: tif,
            limit_price: order.price().map(|p| p.to_string()),
            stop_price: order.trigger_price().map(|p| p.to_string()),
            order_class: None,
            trail_percent,
            trail_price,
            expires_at: expires_at_rfc3339(&order),
            extended_hours: None,
            client_order_id: Some(order.client_order_id().to_string()),
            take_profit: None,
            stop_loss: None,
        };
        // EH-eligible limit orders released at the pre-market open need extended_hours=true.
        let eh_at_release = order.order_type()
            == nautilus_model::enums::OrderType::Limit
            && order.contingency_type().is_none();
        if eh_at_release {
            // Pre-market release → extended_hours; regular-open release leaves it off. The
            // validator chose pre-market only for EH-eligible limits, so this is consistent.
            request.extended_hours = Some(true);
        }

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();
        let client_order_id = order.client_order_id();
        let now = clock.get_time_ns().as_u64();
        let delay_ns = release.as_u64().saturating_sub(now);

        self.spawn_task("queued_submit", async move {
            tokio::time::sleep(std::time::Duration::from_nanos(delay_ns)).await;
            emitter.emit_order_submitted(&order);
            match http_client.submit_order(&request).await {
                Ok(resp) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_accepted(&order, VenueOrderId::new(&resp.id), ts);
                }
                Err(e) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("queued-submit-rejected: {e}"),
                        ts,
                        false,
                    );
                }
            }
            Ok(())
        });
    }
}

/// Extracts account equity (total balance) in the account's base currency, else USD.
fn account_equity(account: &AccountAny) -> Option<Money> {
    let currency = account.base_currency().or_else(|| Some(Currency::USD()));
    account.balance_total(currency)
}

/// Converts a position-derived `f64` into a [`Decimal`] for exact gatekeeper math. A non-finite
/// value (should never occur for a valued position) collapses to zero rather than panicking.
fn f64_to_decimal(value: f64) -> Decimal {
    Decimal::try_from(value).unwrap_or(Decimal::ZERO)
}

#[async_trait(?Send)]
impl ExecutionClient for AlpacaExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *ALPACA_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        // Account state is primed in `connect` and (M5.2) refreshed over the trade-updates WS;
        // this trait hook is unused for Alpaca.
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        log::info!(
            "Started: client_id={}, account_id={}, mode={:?}",
            self.core.client_id,
            self.core.account_id,
            self.config.mode,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.core.set_stopped();
        self.core.set_disconnected();
        {
            let mut tasks = self.pending_tasks.lock().expect(MUTEX_POISONED);
            for handle in tasks.drain(..) {
                handle.abort();
            }
        }
        if let Some(handle) = self.ws_task.take() {
            handle.abort();
        }
        log::info!("Stopped: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }

        // Prime account state from /v2/account so the engine registers the account (and the
        // gatekeeper has equity to evaluate %-rules against).
        let account = self
            .http_client
            .get_account()
            .await
            .map_err(|e| anyhow::anyhow!("failed to prime Alpaca account: {e}"))?;
        if account.trading_blocked || account.account_blocked {
            anyhow::bail!(
                "Alpaca account {} is blocked (trading_blocked={}, account_blocked={})",
                account.id,
                account.trading_blocked,
                account.account_blocked,
            );
        }
        let ts = self.clock.get_time_ns();
        let state = crate::http::parse::parse_account_state(
            &account,
            self.core.account_id,
            true,
            ts,
            ts,
        )
        .map_err(|e| anyhow::anyhow!("failed to parse Alpaca account state: {e}"))?;
        self.emitter.send_account_state(state);

        // Divergence logging: read open orders + positions and warn (full reconciliation is
        // M5.5; this is the lightweight restart-safety precursor).
        match self.http_client.get_open_orders().await {
            Ok(orders) if !orders.is_empty() => {
                log::warn!(
                    "Alpaca connect: {} open order(s) already resting at the venue; \
                     reconciliation (M5.5) will sync these",
                    orders.len()
                );
            }
            Ok(_) => {}
            Err(e) => log::warn!("Alpaca connect: could not read open orders: {e}"),
        }
        match self.http_client.get_positions().await {
            Ok(positions) if !positions.is_empty() => {
                log::warn!(
                    "Alpaca connect: {} existing position(s) at the venue; \
                     reconciliation (M5.5) will sync these",
                    positions.len()
                );
            }
            Ok(_) => {}
            Err(e) => log::warn!("Alpaca connect: could not read positions: {e}"),
        }

        self.await_account_registered(ACCOUNT_REGISTERED_TIMEOUT_SECS)
            .await?;

        // Trade-updates WS (M5.2): open, then spawn a task mapping updates to fill/status
        // reports. The WS task cannot touch the engine cache (CacheView is !Send), so it emits
        // FillReport / OrderStatusReport through the reconciler path rather than order events.
        self.ws_client.connect().await?;
        if let Some(mut rx) = self.ws_client.take_out_rx() {
            let emitter = self.emitter.clone();
            let http_client = self.http_client.clone();
            let clock = self.clock;
            let account_id = self.core.account_id;
            let handle = get_runtime().spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        AlpacaWsMessage::TradeUpdate(update) => {
                            handle_trade_update(&update, &emitter, account_id, clock);
                        }
                        AlpacaWsMessage::Reconnected => {
                            if let Ok(account) = http_client.get_account().await {
                                let ts = clock.get_time_ns();
                                match crate::http::parse::parse_account_state(
                                    &account, account_id, true, ts, ts,
                                ) {
                                    Ok(state) => emitter.send_account_state(state),
                                    Err(e) => log::warn!(
                                        "Alpaca WS reconnect: account refresh parse failed: {e}"
                                    ),
                                }
                            }
                        }
                    }
                }
                log::info!("Alpaca trade-updates consume task stopped");
            });
            self.ws_task = Some(handle);
        }

        self.core.set_connected();
        log::info!("Connected: client_id={}", self.core.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if !self.core.is_connected() {
            return Ok(());
        }
        self.ws_client.disconnect().await;
        if let Some(handle) = self.ws_task.take() {
            handle.abort();
        }
        self.core.set_disconnected();
        log::info!("Disconnected: client_id={}", self.core.client_id);
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;
        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let instrument_id = order.instrument_id();

        // ---- Gatekeeper: the risk safeguard, synchronous, pre-network. ----
        // A price-less order (market / market-to-limit / trailing) carries no price of its own,
        // so value it at a cached decision-time price (quote mid, else last trade) — otherwise
        // value-based risk rules fail-closed and deny every market-type order.
        // Reducing-exposure exits bypass valuation regardless.
        let reference_price = self.reference_price_for(instrument_id);
        let order_info = OrderInfo {
            side: order.order_side(),
            order_type: order.order_type(),
            quantity: order.quantity(),
            limit_price: order.price(),
            reference_price,
        };
        let account_info = self.account_info_for(instrument_id);
        if let gatekeeper::Decision::Deny { reason } =
            gatekeeper::check(&order_info, &account_info, &self.limits)
        {
            log::warn!(
                "Gatekeeper DENIED order {}: {reason}",
                order.client_order_id()
            );
            self.emitter
                .emit_order_denied(&order, &format!("gatekeeper: {reason}"));
            return Ok(());
        }

        // ---- Wash-trade / short-block guard: reject locally what Alpaca would 403. ----
        if let Some(reason) = self.wash_reason_for(&order) {
            log::warn!("Wash-guard DENIED order {}: {reason}", order.client_order_id());
            self.emitter
                .emit_order_denied(&order, &format!("wash-guard: {reason}"));
            return Ok(());
        }

        // ---- §5a validator: never submit a shape Alpaca would reject. ----
        // A plain order reports `Some(ContingencyType::NoContingency)` (not `None`), so
        // `is_some()` alone would misclassify EVERY order as complex (bracket/OCO/OTO) and
        // reject it after-hours. Only a *real* contingency (OCO/OTO/OUO) makes it complex.
        let has_real_contingency = matches!(
            order.contingency_type(),
            Some(ct) if ct != nautilus_model::enums::ContingencyType::NoContingency
        );
        let (trail_percent, trail_price) = trailing_params(&order);
        let shape = OrderShape {
            order_type: order.order_type(),
            time_in_force: order.time_in_force(),
            is_complex: order.is_emulated() || has_real_contingency,
            limit_price: order.price().map(|p| p.as_f64()),
            stop_price: order.trigger_price().map(|p| p.as_f64()),
            trail_price: trail_price.as_ref().and_then(|s| s.parse::<f64>().ok()),
        };
        let now = self.clock.get_time_ns();
        let extended_hours = match self.validator.validate(&shape, now) {
            Validity::Ok { extended_hours } => extended_hours,
            Validity::StructureReject { reason } => {
                log::warn!("Validator STRUCTURE-rejected {}: {reason}", order.client_order_id());
                self.emitter
                    .emit_order_rejected(&order, &format!("validator: {reason}"), now, false);
                return Ok(());
            }
            Validity::SessionReject { reason } => {
                let queueable = order
                    .tags()
                    .is_some_and(|tags| tags.iter().any(|t| t.as_str() == QUEUE_FOR_OPEN_TAG));
                if queueable {
                    let release = self.validator.earliest_valid_open(&shape, now);
                    log::info!(
                        "Validator session-rejected {} but '{QUEUE_FOR_OPEN_TAG}'-tagged; \
                         holding until {} ({reason})",
                        order.client_order_id(),
                        release,
                    );
                    self.queue_until_open(order, release);
                    return Ok(());
                }
                log::warn!("Validator SESSION-rejected {}: {reason}", order.client_order_id());
                self.emitter
                    .emit_order_rejected(&order, &format!("validator: {reason}"), now, false);
                return Ok(());
            }
        };

        // ---- Build the Alpaca request from primitives. ----
        let side = match order_side_to_alpaca(order.order_side()) {
            Ok(s) => s,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("invalid order side: {e}"));
                return Ok(());
            }
        };
        let order_type = match order_type_to_alpaca(order.order_type()) {
            Ok(t) => t,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("unsupported order type: {e}"));
                return Ok(());
            }
        };
        let tif = match time_in_force_to_alpaca(order.time_in_force()) {
            Ok(t) => t,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("unsupported time-in-force: {e}"));
                return Ok(());
            }
        };

        // Alpaca derives a trailing stop's trigger from trail_percent/trail_price and REJECTS
        // an explicit stop_price on it — so send stop_price only for stop / stop-limit types.
        let is_trailing = matches!(
            order.order_type(),
            nautilus_model::enums::OrderType::TrailingStopMarket
                | nautilus_model::enums::OrderType::TrailingStopLimit
        );
        let stop_price = if is_trailing {
            None
        } else {
            order.trigger_price().map(|p| p.to_string())
        };
        let request = CreateOrderRequest {
            symbol: instrument_id.symbol.to_string(),
            qty: format_qty(order.quantity().as_f64()),
            side: side.to_string(),
            order_type: order_type.to_string(),
            time_in_force: tif.to_string(),
            limit_price: order.price().map(|p| p.to_string()),
            stop_price,
            order_class: None,
            trail_percent,
            trail_price,
            expires_at: expires_at_rfc3339(&order),
            // Set internally by the validator (§5a) — never on the strategy⇄adapter seam.
            extended_hours: if extended_hours { Some(true) } else { None },
            client_order_id: Some(order.client_order_id().to_string()),
            take_profit: None,
            stop_loss: None,
        };

        log::debug!("OrderSubmitted client_order_id={}", order.client_order_id());
        self.emitter.emit_order_submitted(&order);

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = order.strategy_id();
        let client_order_id = order.client_order_id();

        self.spawn_task("submit_order", async move {
            match http_client.submit_order(&request).await {
                Ok(resp) => {
                    let ts_event = clock.get_time_ns();
                    let venue_order_id = VenueOrderId::new(&resp.id);
                    emitter.emit_order_accepted(&order, venue_order_id, ts_event);
                }
                Err(e) => {
                    let ts_event = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("submit-order-rejected: {e}"),
                        ts_event,
                        false,
                    );
                    return Err(anyhow::anyhow!("submit order failed: {e}"));
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let Some(venue_order_id) = cmd.venue_order_id else {
            log::warn!(
                "Cancel for {} requires a venue_order_id; ignoring",
                cmd.client_order_id
            );
            return Ok(());
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;

        self.spawn_task("cancel_order", async move {
            if let Err(e) = http_client.cancel_order(venue_order_id.as_str()).await {
                let ts_event = clock.get_time_ns();
                emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(venue_order_id),
                    &format!("cancel-order-rejected: {e}"),
                    ts_event,
                );
                return Err(anyhow::anyhow!("cancel order failed: {e}"));
            }
            Ok(())
        });

        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        // Resolve the legs from the cache. A breakout bracket is entry (no parent) + take-profit
        // (limit child) + stop-loss (stop child), all sharing the order_list_id.
        let cache = self.core.cache();
        let mut entry = None;
        let mut take_profit_px = None;
        let mut stop_loss_px = None;
        for coid in &cmd.order_list.client_order_ids {
            let Some(order) = cache.order_owned(coid) else {
                continue;
            };
            if order.parent_order_id().is_none() {
                entry = Some(order);
            } else if order.order_type() == nautilus_model::enums::OrderType::Limit {
                take_profit_px = order.price().map(|p| p.to_string());
            } else {
                // Stop / stop-limit child → the stop-loss leg.
                stop_loss_px = order.trigger_price().map(|p| p.to_string());
            }
        }
        drop(cache);

        let Some(entry) = entry else {
            anyhow::bail!("bracket order list {} has no entry leg", cmd.order_list.id);
        };
        let (Some(tp_px), Some(sl_px)) = (take_profit_px, stop_loss_px) else {
            // Without both protective legs it is not a valid Alpaca bracket. Deny the entry.
            self.emitter.emit_order_denied(
                &entry,
                "bracket requires both take-profit and stop-loss legs",
            );
            return Ok(());
        };

        let instrument_id = entry.instrument_id();

        // Gatekeeper (the entry increases exposure).
        let reference_price = self.reference_price_for(instrument_id);
        let order_info = OrderInfo {
            side: entry.order_side(),
            order_type: entry.order_type(),
            quantity: entry.quantity(),
            limit_price: entry.price(),
            reference_price,
        };
        let account_info = self.account_info_for(instrument_id);
        if let gatekeeper::Decision::Deny { reason } =
            gatekeeper::check(&order_info, &account_info, &self.limits)
        {
            self.emitter
                .emit_order_denied(&entry, &format!("gatekeeper: {reason}"));
            return Ok(());
        }

        // Validator: brackets require Regular hours (complex order).
        let shape = OrderShape {
            order_type: entry.order_type(),
            time_in_force: entry.time_in_force(),
            is_complex: true,
            limit_price: entry.price().map(|p| p.as_f64()),
            stop_price: entry.trigger_price().map(|p| p.as_f64()),
            trail_price: None,
        };
        let now = self.clock.get_time_ns();
        match self.validator.validate(&shape, now) {
            Validity::Ok { .. } => {}
            Validity::StructureReject { reason } | Validity::SessionReject { reason } => {
                // queue_for_open also applies to brackets (release at next regular open).
                let queueable = entry
                    .tags()
                    .is_some_and(|tags| tags.iter().any(|t| t.as_str() == QUEUE_FOR_OPEN_TAG));
                if queueable && matches!(self.validator.validate(&shape, now), Validity::SessionReject { .. }) {
                    let release = self.validator.earliest_valid_open(&shape, now);
                    log::info!("Bracket {} held until {release} (queue_for_open)", entry.client_order_id());
                    // Re-submit as a bracket at release.
                    self.queue_bracket_until_open(entry, tp_px, sl_px, release);
                    return Ok(());
                }
                self.emitter
                    .emit_order_rejected(&entry, &format!("validator: {reason}"), now, false);
                return Ok(());
            }
        }

        let request = match self.build_bracket_request(&entry, &tp_px, &sl_px) {
            Ok(r) => r,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&entry, &format!("bracket build failed: {e}"));
                return Ok(());
            }
        };

        self.emitter.emit_order_submitted(&entry);
        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = entry.strategy_id();
        let client_order_id = entry.client_order_id();

        self.spawn_task("submit_bracket", async move {
            match http_client.submit_order(&request).await {
                Ok(resp) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_accepted(&entry, VenueOrderId::new(&resp.id), ts);
                }
                Err(e) => {
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        &format!("submit-bracket-rejected: {e}"),
                        ts,
                        false,
                    );
                    return Err(anyhow::anyhow!("submit bracket failed: {e}"));
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let Some(venue_order_id) = cmd.venue_order_id else {
            log::warn!(
                "Modify for {} requires a venue_order_id; ignoring",
                cmd.client_order_id
            );
            return Ok(());
        };

        let request = crate::http::models::ReplaceOrderRequest {
            qty: cmd.quantity.map(|q| format_qty(q.as_f64())),
            limit_price: cmd.price.map(|p| p.to_string()),
            stop_price: cmd.trigger_price.map(|p| p.to_string()),
        };

        let http_client = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;

        self.spawn_task("modify_order", async move {
            if let Err(e) = http_client
                .replace_order(venue_order_id.as_str(), &request)
                .await
            {
                let ts = clock.get_time_ns();
                emitter.emit_order_modify_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(venue_order_id),
                    &format!("modify-order-rejected: {e}"),
                    ts,
                );
                return Err(anyhow::anyhow!("modify order failed: {e}"));
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_all_orders(&self, _cmd: CancelAllOrders) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        self.spawn_task("cancel_all_orders", async move {
            http_client
                .cancel_all_orders()
                .await
                .map_err(|e| anyhow::anyhow!("cancel all orders failed: {e}"))
        });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        // Alpaca has no batch-cancel endpoint; issue individual DELETEs per resolved venue id.
        let cache = self.core.cache();
        let venue_ids: Vec<String> = cmd
            .cancels
            .iter()
            .filter_map(|c| {
                c.venue_order_id
                    .map(|v| v.to_string())
                    .or_else(|| cache.order_owned(&c.client_order_id).and_then(|o| o.venue_order_id().map(|v| v.to_string())))
            })
            .collect();
        drop(cache);

        let http_client = self.http_client.clone();
        self.spawn_task("batch_cancel_orders", async move {
            for vid in venue_ids {
                if let Err(e) = http_client.cancel_order(&vid).await {
                    log::warn!("batch cancel: {vid} failed: {e}");
                }
            }
            Ok(())
        });
        Ok(())
    }

    // ----- M5.5 reconciliation report generators ----------------------------

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        // Alpaca's /v2/orders?status=open lists resting orders; we report those (the engine
        // reconciles them on restart). `open_only=false` would want closed orders too, which a
        // richer status-query endpoint covers in a follow-up.
        let orders = self
            .http_client
            .get_open_orders()
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch open orders: {e}"))?;
        let ts = self.clock.get_time_ns();
        let account_id = self.core.account_id;
        Ok(orders
            .iter()
            .filter(|o| {
                cmd.instrument_id.is_none_or(|want| {
                    o.symbol
                        .as_deref()
                        .is_some_and(|s| InstrumentId::new(nautilus_model::identifiers::Symbol::new(s), *ALPACA_VENUE) == want)
                })
            })
            .filter_map(|o| crate::http::parse::parse_order_status_report(o, account_id, ts))
            .collect())
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        // Fills are delivered live over the trade-updates WS (M5.2); a historical fill-report
        // backfill endpoint (account activities) is a follow-up. Empty for now.
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let positions = self
            .http_client
            .get_positions()
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch positions: {e}"))?;
        let ts = self.clock.get_time_ns();
        let account_id = self.core.account_id;
        Ok(positions
            .iter()
            .filter(|p| {
                cmd.instrument_id.is_none_or(|want| {
                    InstrumentId::new(nautilus_model::identifiers::Symbol::new(&p.symbol), *ALPACA_VENUE) == want
                })
            })
            .filter_map(|p| crate::http::parse::parse_position_status_report(p, account_id, ts))
            .collect())
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        log::info!("Generating Alpaca ExecutionMassStatus (lookback_mins={lookback_mins:?})");
        let ts_now = self.clock.get_time_ns();
        let start = lookback_mins
            .map(|m| UnixNanos::from(ts_now.as_u64().saturating_sub(m * 60 * 1_000_000_000)));

        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(true)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .build()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let (order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_fill_reports(fill_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        log::info!(
            "Alpaca mass status: {} orders, {} fills, {} positions",
            order_reports.len(),
            fill_reports.len(),
            position_reports.len(),
        );

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *ALPACA_VENUE,
            ts_now,
            None,
        );
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);
        Ok(Some(mass_status))
    }
}

/// US-equity price precision (Alpaca equities quote to 2dp ≥ $1; sub-$1 to 4dp — see §5a).
const EQUITY_PRICE_PRECISION: u8 = 2;
/// Sub-dollar price precision.
const EQUITY_SUBDOLLAR_PRICE_PRECISION: u8 = 4;

/// Maps an Alpaca trade-update to the reconciler-fed events.
///
/// Fill / partial-fill events synthesize a [`FillReport`] (the WS task cannot read the engine
/// cache to build an order-event, so fills flow through the reconciler path, mirroring coinbase).
/// Other lifecycle events are logged; the formal order-status reconciliation lands in M5.5.
fn handle_trade_update(
    update: &AlpacaTradeUpdate,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    clock: &AtomicTime,
) {
    match update.event.as_str() {
        "fill" | "partial_fill" => {
            if let Some(report) = build_fill_report(update, account_id, clock) {
                emitter.send_fill_report(report);
            }
        }
        // These are surfaced through the M5.5 order-status reconciler; for M5.2 we log them so
        // the lifecycle is observable without yet wiring OrderStatusReport generation.
        other => log::debug!(
            "Alpaca trade-update '{other}' for client_order_id={:?} (status reporting in M5.5)",
            update.order.client_order_id,
        ),
    }
}

/// Builds a [`FillReport`] from a fill / partial-fill trade update, or `None` if it lacks the
/// required price/qty fields or they fail to parse.
fn build_fill_report(
    update: &AlpacaTradeUpdate,
    account_id: AccountId,
    clock: &AtomicTime,
) -> Option<nautilus_model::reports::FillReport> {
    use std::str::FromStr;

    use nautilus_model::{enums::OrderSide, identifiers::PositionId, types::Money};
    use rust_decimal::Decimal;

    let symbol = update.order.symbol.as_deref()?;
    let instrument_id = InstrumentId::new(
        nautilus_model::identifiers::Symbol::new(symbol),
        *ALPACA_VENUE,
    );

    let last_px_dec = Decimal::from_str(update.price.as_deref()?.trim()).ok()?;
    let last_qty_dec = Decimal::from_str(update.qty.as_deref()?.trim()).ok()?;

    // Price precision: 2dp for ≥ $1, 4dp for sub-dollar (Alpaca's sub-penny rule, §5a).
    let price_precision = if last_px_dec >= Decimal::ONE {
        EQUITY_PRICE_PRECISION
    } else {
        EQUITY_SUBDOLLAR_PRICE_PRECISION
    };
    let last_px = Price::from_decimal_dp(last_px_dec, price_precision).ok()?;
    // Equity quantities are whole shares for MVP (precision 0).
    let last_qty = Quantity::from_decimal_dp(last_qty_dec, 0).ok()?;

    let side = match update.order.side.as_deref() {
        Some("buy") => OrderSide::Buy,
        Some("sell") => OrderSide::Sell,
        _ => return None,
    };

    // Alpaca charges no per-fill commission on equities; report zero USD.
    let commission = Money::new(0.0, Currency::USD());

    let trade_id = TradeId::new(
        update
            .execution_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", update.order.id, clock.get_time_ns())),
    );
    let venue_order_id = VenueOrderId::new(&update.order.id);
    let client_order_id = update
        .order
        .client_order_id
        .as_deref()
        .map(ClientOrderId::new);

    let ts = clock.get_time_ns();
    Some(nautilus_model::reports::FillReport::new(
        account_id,
        instrument_id,
        venue_order_id,
        trade_id,
        side,
        last_qty,
        last_px,
        commission,
        LiquiditySide::Taker,
        client_order_id,
        None::<PositionId>,
        ts,
        ts,
        None,
    ))
}

/// Formats a share quantity for the Alpaca `qty` field (whole shares for MVP, trimmed decimals).
fn format_qty(qty: f64) -> String {
    if qty.fract() == 0.0 {
        format!("{}", qty as i64)
    } else {
        // Trim trailing zeros from a fractional qty (fractional equities, future use).
        let s = format!("{qty:.9}");
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// Resolves the Alpaca `(trail_percent, trail_price)` pair from a trailing-stop order.
///
/// A trailing offset is either a percent (`BasisPoints` → `trail_percent`, in whole percent)
/// or an absolute price move (`Price`/`Ticks` → `trail_price`). Non-trailing orders yield
/// `(None, None)`. The two are mutually exclusive on the Alpaca wire.
fn trailing_params(order: &nautilus_model::orders::OrderAny) -> (Option<String>, Option<String>) {
    use nautilus_model::enums::TrailingOffsetType;
    let Some(offset) = order.trailing_offset() else {
        return (None, None);
    };
    match order.trailing_offset_type() {
        Some(TrailingOffsetType::BasisPoints) => {
            // Nautilus basis-points offset → Alpaca trail_percent (percent). 1% = 100 bps.
            let pct = offset / rust_decimal::Decimal::from(100);
            (Some(pct.to_string()), None)
        }
        Some(TrailingOffsetType::Price | TrailingOffsetType::Ticks) => {
            (None, Some(offset.to_string()))
        }
        _ => (None, None),
    }
}

/// Formats a GTD order's `expire_time` as an RFC-3339 string for Alpaca's `expires_at`.
///
/// Returns `None` when the order carries no expiry (non-GTD orders omit the field).
fn expires_at_rfc3339(order: &nautilus_model::orders::OrderAny) -> Option<String> {
    let expire = order.expire_time()?;
    Some(chrono::DateTime::from_timestamp_nanos(expire.as_i64()).to_rfc3339())
}
