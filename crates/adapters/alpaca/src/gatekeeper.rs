//! Pre-trade risk gatekeeper — a deliberately simple, auditable "last line of defense".
//!
//! This is **not** the §5a protocol validator (which answers "would Alpaca's server reject
//! this order shape?"). The gatekeeper answers a different question: **"do *our* risk limits
//! allow this order?"** — max shares, max dollar value, max % of account exposure. It is the
//! final safeguard the order crosses inside [`crate::execution::AlpacaExecutionClient::submit_order`]
//! before any network I/O, sitting *downstream* of the smarter pre-trade risk engine that runs
//! at the strategy/runner layer with fuller information. Where that engine is rich and
//! context-aware, this is intentionally mostly-dumb and easy to audit: a pure function over
//! primitives.
//!
//! # Design contract
//!
//! - **Pure / no I/O.** [`check`] takes already-extracted primitives ([`OrderInfo`] +
//!   [`AccountInfo`]) and a [`GatekeeperLimits`]; it touches no cache, clock, or socket. The
//!   adapter does the cache reads and hands primitives in. This makes every rule trivially
//!   table-testable.
//! - **Entries/increases only.** Exposure rules (`max_position_pct`, `max_gross_exposure_pct`)
//!   apply only to orders that *increase* absolute exposure for their symbol. An order that
//!   reduces |net position| (an exit/trim) skips them — a risk limit must never trap you in a
//!   position you need to close. The absolute per-order caps (`max_order_shares`,
//!   `max_order_notional`) still apply to everything, since a fat-finger exit is still a
//!   fat-finger.
//! - **Limit entries by default.** A MARKET order that increases exposure is denied unless
//!   `ALPACA_GATE_ALLOW_MARKET_ORDERS=true` is set. This is a *risk policy*, not an Alpaca
//!   protocol rule (Alpaca accepts market orders in RTH), so it lives here, not in §5a. Limit
//!   entries carry their own price, so notional/% math is exact — no quote lookup, no
//!   reference-price guessing; an *allowed* market entry can only be bounded by the absolute
//!   share cap (it has no price to value). MARKET *exits* are always allowed.
//! - **Fail-closed.** If any %-rule is configured but account equity is unknown (e.g. `connect`
//!   has not primed `/v2/account` yet), every entry is denied. A safeguard that silently waves
//!   orders through when it can't evaluate its own rules is not a safeguard.
//! - **Deny + log.** A breach yields [`Decision::Deny`] with a human-readable reason; the
//!   adapter emits `OrderDenied` (no network call). The gatekeeper never clamps/resizes.
//!
//! All limits are optional and sourced from `ALPACA_GATE_*` env vars via
//! [`GatekeeperLimits::from_env`]; an unset var disables that rule.

use nautilus_model::{
    enums::{OrderSide, OrderType},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

/// The `ALPACA_GATE_*` env var names (single source of truth, also used in error messages).
const ENV_MAX_ORDER_SHARES: &str = "ALPACA_GATE_MAX_ORDER_SHARES";
const ENV_MAX_ORDER_NOTIONAL: &str = "ALPACA_GATE_MAX_ORDER_NOTIONAL";
const ENV_MAX_POSITION_PCT: &str = "ALPACA_GATE_MAX_POSITION_PCT";
const ENV_MAX_GROSS_EXPOSURE_PCT: &str = "ALPACA_GATE_MAX_GROSS_EXPOSURE_PCT";
const ENV_ALLOW_MARKET_ORDERS: &str = "ALPACA_GATE_ALLOW_MARKET_ORDERS";

/// Risk limits for the gatekeeper. The numeric fields are optional; `None` disables that rule.
///
/// Percentage fields are stored internally as a **fraction** in `(0, 1]` (e.g. `0.25` = 25%).
/// The `ALPACA_GATE_*_PCT` env vars, by contrast, are entered as a **percent number in
/// `(0, 100]`** (`25` = 25%) and converted to the fraction by [`GatekeeperLimits::from_env`].
///
/// The [`Default`] is the conservative guardrail posture: no numeric caps, and
/// `allow_market_orders = false` (market entries denied). The market-entry ban is therefore the
/// one rule active even when no numeric limits are configured.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GatekeeperLimits {
    /// Max shares (quantity) on any single order. Applies to entries *and* exits.
    pub max_order_shares: Option<f64>,
    /// Max notional ($ = qty × limit_price) on any single order. Applies to entries *and* exits.
    pub max_order_notional: Option<f64>,
    /// Max fraction of account equity any single symbol's position may reach after an entry.
    pub max_position_pct: Option<f64>,
    /// Max fraction of account equity total gross exposure may reach after an entry.
    pub max_gross_exposure_pct: Option<f64>,
    /// Whether MARKET orders are permitted for *entries*. Default `false` (deny market entries —
    /// the safe guardrail default). Market *exits* are always allowed regardless of this flag.
    /// Set `ALPACA_GATE_ALLOW_MARKET_ORDERS=true` to opt in.
    pub allow_market_orders: bool,
}

impl GatekeeperLimits {
    /// `true` if no numeric rule is configured AND market entries are allowed — i.e. the
    /// gatekeeper would never deny anything (a true pass-through).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.max_order_shares.is_none()
            && self.max_order_notional.is_none()
            && self.max_position_pct.is_none()
            && self.max_gross_exposure_pct.is_none()
            && self.allow_market_orders
    }

    /// `true` if any configured rule needs account equity (the %-rules).
    #[must_use]
    pub fn requires_equity(&self) -> bool {
        self.max_position_pct.is_some() || self.max_gross_exposure_pct.is_some()
    }

    /// Builds limits from the `ALPACA_GATE_*` environment variables.
    ///
    /// The numeric vars are optional; an unset var leaves its rule `None` (disabled). The `_PCT`
    /// vars are a **percent number in `(0, 100]`** — i.e. the literal percentage, no `%` suffix:
    /// `25` means 25% (fraction `0.25`), `0.25` means 0.25% (fraction `0.0025`), `100` means 100%
    /// (fraction `1.0`). The value is divided by 100 to get the fraction used internally.
    ///
    /// `ALPACA_GATE_ALLOW_MARKET_ORDERS` is a boolean (`true`/`1`/`yes`/`on`, case-insensitive);
    /// unset or anything else means `false` (market entries denied).
    ///
    /// # Errors
    ///
    /// Returns an error if a set var fails to parse as a positive finite number, or if a `_PCT`
    /// var resolves outside `(0, 100]`.
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            max_order_shares: parse_positive_env(ENV_MAX_ORDER_SHARES)?,
            max_order_notional: parse_positive_env(ENV_MAX_ORDER_NOTIONAL)?,
            max_position_pct: parse_pct_env(ENV_MAX_POSITION_PCT)?,
            max_gross_exposure_pct: parse_pct_env(ENV_MAX_GROSS_EXPOSURE_PCT)?,
            allow_market_orders: parse_bool_env(ENV_ALLOW_MARKET_ORDERS),
        })
    }
}

/// Reads `name`; parses a positive, finite f64 or returns `None` if unset/empty.
fn parse_positive_env(name: &str) -> anyhow::Result<Option<f64>> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    let value: f64 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("{name}='{raw}' is not a number"))?;
    if !value.is_finite() || value <= 0.0 {
        anyhow::bail!("{name}='{raw}' must be a positive finite number");
    }
    Ok(Some(value))
}

/// Reads `name`; parses a percent number in `(0, 100]` into a fraction in `(0, 1]`, or `None`
/// if unset/empty. The value is the literal percentage (`25` → 0.25, `0.25` → 0.0025).
fn parse_pct_env(name: &str) -> anyhow::Result<Option<f64>> {
    let Some(raw) = read_env(name) else {
        return Ok(None);
    };
    let percent: f64 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{name}='{raw}' is not a number"))?;
    if !percent.is_finite() || percent <= 0.0 || percent > 100.0 {
        anyhow::bail!(
            "{name}='{raw}' must be a percent number in (0, 100] (e.g. 25 = 25%, 0.25 = 0.25%)"
        );
    }
    Ok(Some(percent / 100.0))
}

/// Reads an env var, treating unset *or* whitespace-only as absent.
fn read_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

/// Reads a boolean env var. `true`/`1`/`yes`/`on` (case-insensitive) → `true`; anything else
/// (incl. unset) → `false`.
fn parse_bool_env(name: &str) -> bool {
    read_env(name).is_some_and(|raw| {
        matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        )
    })
}

/// The order under evaluation, reduced to the typed values the gatekeeper needs.
///
/// Extracted from the resolved `OrderAny` in `submit_order`. Prices and quantity are the
/// model's fixed-point [`Price`]/[`Quantity`] newtypes; all internal valuation is performed in
/// [`Decimal`] (exact, no float rounding) since there is no typed `Price × Quantity → Money`
/// operation in the model (notional needs a currency only the instrument/account knows).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OrderInfo {
    /// BUY or SELL.
    pub side: OrderSide,
    /// The order type (only non-`Market` orders may be a priced entry; `Market` entries are
    /// denied by default and otherwise valued via [`OrderInfo::reference_price`]).
    pub order_type: OrderType,
    /// Order quantity in shares.
    pub quantity: Quantity,
    /// Limit price, if the order carries one (`None` for market orders).
    pub limit_price: Option<Price>,
    /// Decision-time / indicative price captured at instantiation. Used to value an *allowed*
    /// `MARKET` entry against the notional / %-of-equity rules when there is no `limit_price`.
    pub reference_price: Option<Price>,
}

impl OrderInfo {
    /// The signed quantity (in shares) this order contributes to the net position (BUY +, SELL −).
    fn signed_qty(&self) -> Decimal {
        let qty = self.quantity.as_decimal();
        match self.side {
            OrderSide::Buy => qty,
            OrderSide::Sell => -qty,
            OrderSide::NoOrderSide => Decimal::ZERO,
        }
    }

    /// The price to value this order against the notional / %-of-equity rules: the limit price if
    /// present, else the decision-time reference price. `None` for an unpriced market order.
    fn valuation_price(&self) -> Option<Price> {
        self.limit_price.or(self.reference_price)
    }
}

/// Account state the gatekeeper needs, valued from the (Alpaca-primed) cache.
///
/// `equity` is `None` until `connect` has primed `/v2/account`. Existing positions are valued
/// at **cost basis** (`avg_px_open × signed_qty`) — deterministic and self-contained, slightly
/// stale vs. market, which is acceptable for a generous safeguard (limits should be set with
/// headroom, not as exact tripwires).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AccountInfo {
    /// Account equity as [`Money`] (carries its currency), or `None` if not yet primed.
    pub equity: Option<Money>,
    /// Signed net quantity currently held for *this order's symbol* (long +, short −), in shares.
    pub symbol_net_qty: Decimal,
    /// Average entry price of the current position for this symbol (`0` if flat).
    pub symbol_avg_px: Decimal,
    /// Total gross exposure across *all* symbols at cost basis (Σ |signed_qty × avg_px|), ≥ 0.
    pub total_gross_exposure: Decimal,
}

/// The gatekeeper's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// The order is within all configured limits and may proceed.
    Allow,
    /// The order breaches a limit (or cannot be evaluated fail-closed); `reason` is log/event text.
    Deny { reason: String },
}

impl Decision {
    /// `true` if this is [`Decision::Allow`].
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Evaluates `order` against `limits` given current `account` state. Pure; no I/O.
///
/// See the module docs for the full contract. Order of checks (first breach wins):
/// 1. Absolute per-order caps (`max_order_shares`, `max_order_notional`) — apply to all orders.
/// 2. If the order is an *entry* (increases |exposure|):
///    a. MARKET entries are denied unless explicitly allowed.
///    b. Fail-closed: if a %-rule is configured but `equity` is `None`, deny.
///    c. Resolve a valuation price (limit price, else reference price); if a notional or %-rule is configured but neither price exists, deny fail-closed.
///    d. `max_position_pct`: resulting symbol position value vs equity.
///    e. `max_gross_exposure_pct`: resulting total gross exposure vs equity.
///
/// All monetary/exposure arithmetic is performed in [`Decimal`] (exact). Limit values parsed
/// from the `ALPACA_GATE_*` env vars are `f64`; they are converted to [`Decimal`] for comparison.
#[must_use]
pub fn check(order: &OrderInfo, account: &AccountInfo, limits: &GatekeeperLimits) -> Decision {
    if limits.is_empty() {
        return Decision::Allow;
    }

    let order_qty = order.quantity.as_decimal();

    // (1) Absolute per-order caps — entries and exits alike.
    if let Some(max_shares) = limits.max_order_shares
        && order_qty > to_decimal(max_shares)
    {
        return deny(format!(
            "order quantity {order_qty} > {ENV_MAX_ORDER_SHARES} ({max_shares})",
        ));
    }
    // The notional cap can only be evaluated when a price is available. Prefer the limit price,
    // fall back to the decision-time reference price (lets it bite oversized market entries too).
    if let Some(max_notional) = limits.max_order_notional
        && let Some(valuation_px) = order.valuation_price()
    {
        let notional = order_qty * valuation_px.as_decimal();
        if notional > to_decimal(max_notional) {
            return deny(format!(
                "order notional {notional} > {ENV_MAX_ORDER_NOTIONAL} ({max_notional})",
            ));
        }
    }

    // Exposure rules only gate orders that INCREASE absolute exposure for the symbol.
    if !is_increasing(order, account.symbol_net_qty) {
        return Decision::Allow;
    }

    // (2a) Market-entry policy. A market order that increases exposure is denied unless market
    // orders are explicitly allowed (ALPACA_GATE_ALLOW_MARKET_ORDERS=true). Market *exits* never
    // reach here (the increasing-only short-circuit above lets them through).
    if order.order_type == OrderType::Market && !limits.allow_market_orders {
        return deny(format!(
            "market orders not permitted for entries (set {ENV_ALLOW_MARKET_ORDERS}=true to \
             allow; limit entries otherwise)"
        ));
    }

    // (2b) Fail-closed: %-rules require equity. If unknown, deny every entry.
    if limits.requires_equity() && account.equity.is_none() {
        return deny(
            "account equity not yet primed; denying entry (fail-closed) until connect \
             completes account sync"
                .to_string(),
        );
    }

    // (2c) Resolve the valuation price for the notional / %-of-equity rules. A limit entry
    // carries its own price; an allowed market entry is valued at its reference price. If any
    // value-based rule is configured but we have no price to value against, deny fail-closed
    // (rather than waving the entry through bounded only by the share cap).
    let needs_valuation = limits.max_order_notional.is_some()
        || limits.max_position_pct.is_some()
        || limits.max_gross_exposure_pct.is_some();
    let entry_px = match order.valuation_price() {
        Some(px) => px.as_decimal(),
        None => {
            if needs_valuation {
                return deny(format!(
                    "entry has no limit or reference price to value against risk limits \
                     (set a reference price on MARKET orders, or use a LIMIT entry); rules \
                     requiring valuation: {ENV_MAX_ORDER_NOTIONAL} / {ENV_MAX_POSITION_PCT} / \
                     {ENV_MAX_GROSS_EXPOSURE_PCT}"
                ));
            }
            // No value-based rule configured (only the share cap, already checked): allow.
            return Decision::Allow;
        }
    };

    // (2d) Per-symbol position cap: value of the resulting position vs equity.
    if let Some(max_pos_pct) = limits.max_position_pct {
        let equity = account.equity.expect("equity present (checked above)");
        // Resulting net qty after this entry, valued at the entry price (a conservative,
        // self-contained proxy — the new shares dominate an increasing position).
        let resulting_net = account.symbol_net_qty + order.signed_qty();
        let resulting_value = resulting_net.abs() * entry_px;
        if let Some(d) = check_pct(
            resulting_value,
            equity,
            max_pos_pct,
            ENV_MAX_POSITION_PCT,
            "resulting position value",
        ) {
            return d;
        }
    }

    // (2e) Gross exposure cap: total gross exposure after this entry vs equity.
    if let Some(max_gross_pct) = limits.max_gross_exposure_pct {
        let equity = account.equity.expect("equity present (checked above)");
        // Replace this symbol's current contribution with its post-entry contribution.
        let current_symbol_gross = (account.symbol_net_qty * account.symbol_avg_px).abs();
        let resulting_symbol_gross =
            (account.symbol_net_qty + order.signed_qty()).abs() * entry_px;
        let resulting_gross =
            account.total_gross_exposure - current_symbol_gross + resulting_symbol_gross;
        if let Some(d) = check_pct(
            resulting_gross,
            equity,
            max_gross_pct,
            ENV_MAX_GROSS_EXPOSURE_PCT,
            "resulting gross exposure",
        ) {
            return d;
        }
    }

    Decision::Allow
}

/// Converts an `f64` env-derived limit into a [`Decimal`] for exact comparison. The env values
/// are validated finite and positive at parse time, so the conversion cannot fail in practice;
/// a defensive fallback keeps the function total.
fn to_decimal(value: f64) -> Decimal {
    Decimal::try_from(value).unwrap_or(Decimal::MAX)
}

/// Returns `Some(Deny)` if `value / equity` exceeds `max_pct`; else `None`. All comparison is in
/// [`Decimal`]; `equity` is taken as its decimal amount (currency is not cross-checked — the
/// venue is single-currency and positions are valued at cost basis).
fn check_pct(
    value: Decimal,
    equity: Money,
    max_pct: f64,
    env_name: &str,
    label: &str,
) -> Option<Decision> {
    let equity_dec = equity.as_decimal();
    // Non-positive equity can't satisfy any positive %-cap on a positive exposure: fail-closed.
    if equity_dec <= Decimal::ZERO {
        return Some(Decision::Deny {
            reason: format!(
                "account equity {equity} is non-positive; denying entry (fail-closed) for {env_name}",
            ),
        });
    }
    let fraction = value / equity_dec;
    if fraction > to_decimal(max_pct) {
        return Some(Decision::Deny {
            reason: format!(
                "{label} {value} is {:.1}% of equity {equity}, exceeds {env_name} ({:.1}%)",
                fraction * Decimal::ONE_HUNDRED,
                max_pct * 100.0,
            ),
        });
    }
    None
}

/// `true` if `order` increases the absolute net position for its symbol (an entry/increase).
///
/// An order increases exposure when it pushes the net quantity *further from zero*. A BUY on a
/// long (or flat) position increases; a SELL on a short (or flat) increases; orders that move
/// toward or through zero (and net smaller |position|) are reductions/exits. An order that
/// flips the sign and overshoots (e.g. SELL 10 against +3) both reduces past flat *and* opens
/// the other side — we treat any order whose post-trade |net| > pre-trade |net| as increasing.
fn is_increasing(order: &OrderInfo, symbol_net_qty: Decimal) -> bool {
    let resulting = symbol_net_qty + order.signed_qty();
    resulting.abs() > symbol_net_qty.abs()
}

/// Convenience constructor for a denial.
fn deny(reason: String) -> Decision {
    Decision::Deny { reason }
}

#[cfg(test)]
mod tests {
    use nautilus_model::types::Currency;
    use rstest::rstest;

    use super::*;

    // ----- builders ---------------------------------------------------------
    //
    // Builders accept `f64` for ergonomics and convert to the model's typed `Price`/`Quantity`
    // (and `Money` for equity) at the boundary, mirroring how the adapter feeds the gatekeeper.

    const TEST_PRECISION: u8 = 2;

    fn qty(value: f64) -> Quantity {
        Quantity::new(value, 0)
    }

    fn px(value: f64) -> Price {
        Price::new(value, TEST_PRECISION)
    }

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    fn order(
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        limit_price: Option<f64>,
        reference_price: Option<f64>,
    ) -> OrderInfo {
        OrderInfo {
            side,
            order_type,
            quantity: qty(quantity),
            limit_price: limit_price.map(px),
            reference_price: reference_price.map(px),
        }
    }

    fn buy_limit(quantity: f64, limit_px: f64) -> OrderInfo {
        order(OrderSide::Buy, OrderType::Limit, quantity, Some(limit_px), None)
    }

    fn buy_market(quantity: f64) -> OrderInfo {
        order(OrderSide::Buy, OrderType::Market, quantity, None, None)
    }

    /// A BUY market order carrying a decision-time reference price.
    fn buy_market_ref(quantity: f64, reference_px: f64) -> OrderInfo {
        order(OrderSide::Buy, OrderType::Market, quantity, None, Some(reference_px))
    }

    fn sell_limit(quantity: f64, limit_px: f64) -> OrderInfo {
        order(OrderSide::Sell, OrderType::Limit, quantity, Some(limit_px), None)
    }

    fn sell_market(quantity: f64) -> OrderInfo {
        order(OrderSide::Sell, OrderType::Market, quantity, None, None)
    }

    fn usd(amount: f64) -> Money {
        Money::new(amount, Currency::USD())
    }

    /// Flat account with known equity and no exposure.
    fn flat(equity: f64) -> AccountInfo {
        AccountInfo {
            equity: Some(usd(equity)),
            symbol_net_qty: Decimal::ZERO,
            symbol_avg_px: Decimal::ZERO,
            total_gross_exposure: Decimal::ZERO,
        }
    }

    /// Limits with market entries allowed (the explicit opt-in) and no numeric caps.
    fn allow_market() -> GatekeeperLimits {
        GatekeeperLimits {
            allow_market_orders: true,
            ..Default::default()
        }
    }

    // ----- empty / pass-through --------------------------------------------

    #[test]
    fn default_denies_market_entry_but_allows_limits() {
        // The default posture: no numeric caps, but market entries are denied (not is_empty).
        let limits = GatekeeperLimits::default();
        assert!(!limits.is_empty());
        assert!(matches!(
            check(&buy_market(10.0), &flat(1_000_000.0), &limits),
            Decision::Deny { .. }
        ));
        assert!(check(&buy_limit(1_000_000.0, 999.0), &flat(1.0), &limits).is_allowed());
    }

    #[test]
    fn allow_market_with_no_caps_is_true_passthrough() {
        let limits = allow_market();
        assert!(limits.is_empty());
        assert!(check(&buy_market(1_000_000.0), &flat(1.0), &limits).is_allowed());
        assert!(check(&buy_limit(1_000_000.0, 999.0), &flat(1.0), &limits).is_allowed());
    }

    #[test]
    fn market_entry_allowed_when_opted_in_but_share_cap_still_applies() {
        let limits = GatekeeperLimits {
            max_order_shares: Some(100.0),
            allow_market_orders: true,
            ..Default::default()
        };
        // Market entry within the share cap → allowed.
        assert!(check(&buy_market(50.0), &flat(1_000_000.0), &limits).is_allowed());
        // Market entry over the share cap → still denied by the absolute cap.
        assert!(matches!(
            check(&buy_market(150.0), &flat(1_000_000.0), &limits),
            Decision::Deny { .. }
        ));
    }

    // ----- market entry valuation via reference_price ----------------------

    #[test]
    fn market_entry_notional_valued_via_reference_price() {
        // Notional cap configured, market entries allowed. The market order has no limit price,
        // so it is valued at its reference price.
        let limits = GatekeeperLimits {
            max_order_notional: Some(50_000.0),
            allow_market_orders: true,
            ..Default::default()
        };
        // 100 × 600 = 60,000 > 50,000 → deny (valued at reference price).
        assert!(matches!(
            check(&buy_market_ref(100.0, 600.0), &flat(10_000_000.0), &limits),
            Decision::Deny { .. }
        ));
        // 100 × 500 = 50,000 (boundary) → allow.
        assert!(check(&buy_market_ref(100.0, 500.0), &flat(10_000_000.0), &limits).is_allowed());
    }

    #[test]
    fn market_entry_position_pct_valued_via_reference_price() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.25), // 25%
            allow_market_orders: true,
            ..Default::default()
        };
        // Flat, equity 100k. Market BUY 300 @ ref 100 = 30k = 30% > 25% → deny.
        assert!(matches!(
            check(&buy_market_ref(300.0, 100.0), &flat(100_000.0), &limits),
            Decision::Deny { .. }
        ));
        // 250 @ 100 = 25k = 25% (boundary) → allow.
        assert!(check(&buy_market_ref(250.0, 100.0), &flat(100_000.0), &limits).is_allowed());
    }

    #[test]
    fn allowed_market_entry_without_reference_price_denies_when_notional_configured() {
        // The new strict, fail-closed path: market entries allowed and a notional rule set, but
        // the order carries neither a limit nor a reference price → cannot value → deny.
        let limits = GatekeeperLimits {
            max_order_notional: Some(50_000.0),
            allow_market_orders: true,
            ..Default::default()
        };
        let d = check(&buy_market(10.0), &flat(1_000_000.0), &limits);
        match d {
            Decision::Deny { reason } => assert!(reason.contains("reference price")),
            Decision::Allow => panic!("expected fail-closed deny (no price to value)"),
        }
    }

    #[test]
    fn allowed_market_entry_without_reference_price_allows_when_only_share_cap() {
        // No value-based rule (only the share cap): an allowed market entry with no price is fine.
        let limits = GatekeeperLimits {
            max_order_shares: Some(100.0),
            allow_market_orders: true,
            ..Default::default()
        };
        assert!(check(&buy_market(50.0), &flat(1_000_000.0), &limits).is_allowed());
    }

    #[test]
    fn reference_price_ignored_when_limit_price_present() {
        // A LIMIT order is valued at its limit price; any reference price is not consulted.
        let limits = GatekeeperLimits {
            max_order_notional: Some(50_000.0),
            ..Default::default()
        };
        let order = OrderInfo {
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: qty(100.0),
            limit_price: Some(px(400.0)),     // 100 × 400 = 40k ≤ 50k → allow
            reference_price: Some(px(600.0)), // would be 60k > 50k, but must be ignored
        };
        assert!(check(&order, &flat(10_000_000.0), &limits).is_allowed());
    }

    // ----- max_order_shares (applies to entries AND exits) ------------------

    #[test]
    fn max_order_shares_denies_oversized_entry() {
        let limits = GatekeeperLimits {
            max_order_shares: Some(100.0),
            ..Default::default()
        };
        assert!(matches!(
            check(&buy_limit(101.0, 10.0), &flat(1_000_000.0), &limits),
            Decision::Deny { .. }
        ));
        assert!(check(&buy_limit(100.0, 10.0), &flat(1_000_000.0), &limits).is_allowed());
    }

    #[test]
    fn max_order_shares_applies_to_exits_too() {
        let limits = GatekeeperLimits {
            max_order_shares: Some(100.0),
            ..Default::default()
        };
        // Long 500; a SELL 200 is an exit, but the absolute share cap still bites (fat-finger).
        let acct = AccountInfo {
            symbol_net_qty: dec(500.0),
            symbol_avg_px: dec(10.0),
            total_gross_exposure: dec(5_000.0),
            ..flat(1_000_000.0)
        };
        assert!(matches!(
            check(&sell_limit(200.0, 10.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    // ----- max_order_notional ----------------------------------------------

    #[test]
    fn max_order_notional_denies_by_dollar_value() {
        let limits = GatekeeperLimits {
            max_order_notional: Some(50_000.0),
            ..Default::default()
        };
        // 100 × 600 = 60,000 > 50,000.
        assert!(matches!(
            check(&buy_limit(100.0, 600.0), &flat(10_000_000.0), &limits),
            Decision::Deny { .. }
        ));
        // 100 × 500 = 50,000 (boundary, allowed).
        assert!(check(&buy_limit(100.0, 500.0), &flat(10_000_000.0), &limits).is_allowed());
    }

    // ----- market entry ban -------------------------------------------------

    #[test]
    fn market_entry_is_denied() {
        let limits = GatekeeperLimits {
            max_order_shares: Some(1_000.0),
            ..Default::default()
        };
        let d = check(&buy_market(10.0), &flat(1_000_000.0), &limits);
        match d {
            Decision::Deny { reason } => assert!(reason.contains("market")),
            Decision::Allow => panic!("expected market entry to be denied"),
        }
    }

    #[test]
    fn market_exit_is_allowed() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.10),
            ..Default::default()
        };
        // Long 100; SELL 50 market is an exit (reduces |net|) → market ban does not apply.
        let acct = AccountInfo {
            equity: Some(usd(1_000_000.0)),
            symbol_net_qty: dec(100.0),
            symbol_avg_px: dec(10.0),
            total_gross_exposure: dec(1_000.0),
        };
        assert!(check(&sell_market(50.0), &acct, &limits).is_allowed());
    }

    #[test]
    fn market_order_opening_short_is_an_entry_and_denied() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.50),
            ..Default::default()
        };
        // Flat → SELL market opens a short (increases |net|) → denied as a market entry.
        let d = check(&sell_market(10.0), &flat(1_000_000.0), &limits);
        assert!(matches!(d, Decision::Deny { .. }));
    }

    // ----- entry vs exit detection -----------------------------------------

    #[rstest]
    #[case(0.0, OrderSide::Buy, 10.0, true)] // flat → long: entry
    #[case(0.0, OrderSide::Sell, 10.0, true)] // flat → short: entry
    #[case(100.0, OrderSide::Buy, 10.0, true)] // long → more long: entry
    #[case(100.0, OrderSide::Sell, 10.0, false)] // long → less long: exit
    #[case(-100.0, OrderSide::Sell, 10.0, true)] // short → more short: entry
    #[case(-100.0, OrderSide::Buy, 10.0, false)] // short → less short: exit
    #[case(3.0, OrderSide::Sell, 10.0, true)] // long 3 → short 7: |net| grows: entry
    #[case(100.0, OrderSide::Sell, 100.0, false)] // long 100 → flat: exit
    fn increasing_detection(
        #[case] net: f64,
        #[case] side: OrderSide,
        #[case] quantity: f64,
        #[case] expect_increasing: bool,
    ) {
        let o = order(side, OrderType::Limit, quantity, Some(10.0), None);
        assert_eq!(is_increasing(&o, dec(net)), expect_increasing);
    }

    #[test]
    fn exit_skips_exposure_rules() {
        // A tight position cap that the current position already exceeds; an exit must still pass.
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.01),
            max_gross_exposure_pct: Some(0.01),
            ..Default::default()
        };
        let acct = AccountInfo {
            equity: Some(usd(10_000.0)),
            symbol_net_qty: dec(1_000.0),
            symbol_avg_px: dec(50.0), // position worth 50k vs 10k equity — over, but existing
            total_gross_exposure: dec(50_000.0),
        };
        // SELL to reduce: exit → exposure rules skipped → allowed.
        assert!(check(&sell_limit(100.0, 50.0), &acct, &limits).is_allowed());
    }

    // ----- fail-closed on unknown equity -----------------------------------

    #[test]
    fn pct_rule_with_unknown_equity_denies_entry() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.10),
            ..Default::default()
        };
        let acct = AccountInfo {
            equity: None,
            ..flat(0.0)
        };
        assert!(matches!(
            check(&buy_limit(1.0, 1.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    #[test]
    fn unknown_equity_still_allows_exit() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.10),
            ..Default::default()
        };
        let acct = AccountInfo {
            equity: None,
            symbol_net_qty: dec(100.0),
            symbol_avg_px: dec(10.0),
            total_gross_exposure: dec(1_000.0),
        };
        // Exit short-circuits before the equity check.
        assert!(check(&sell_limit(50.0, 10.0), &acct, &limits).is_allowed());
    }

    #[test]
    fn absolute_caps_work_without_equity() {
        // Only absolute caps configured → equity never needed, even for an entry.
        let limits = GatekeeperLimits {
            max_order_shares: Some(100.0),
            max_order_notional: Some(10_000.0),
            ..Default::default()
        };
        let acct = AccountInfo {
            equity: None,
            ..flat(0.0)
        };
        assert!(check(&buy_limit(50.0, 10.0), &acct, &limits).is_allowed());
        assert!(matches!(
            check(&buy_limit(150.0, 10.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    // ----- max_position_pct -------------------------------------------------

    #[test]
    fn position_pct_denies_when_resulting_position_too_large() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.25), // 25%
            ..Default::default()
        };
        // Flat, equity 100k. BUY 300 @ 100 = 30k = 30% > 25% → deny.
        assert!(matches!(
            check(&buy_limit(300.0, 100.0), &flat(100_000.0), &limits),
            Decision::Deny { .. }
        ));
        // BUY 250 @ 100 = 25k = 25% (boundary) → allow.
        assert!(check(&buy_limit(250.0, 100.0), &flat(100_000.0), &limits).is_allowed());
    }

    #[test]
    fn position_pct_accounts_for_existing_position() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.25),
            ..Default::default()
        };
        // Already long 200 @ 100 (20k). BUY 100 more @ 100 → resulting 300 × 100 = 30k = 30% → deny.
        let acct = AccountInfo {
            equity: Some(usd(100_000.0)),
            symbol_net_qty: dec(200.0),
            symbol_avg_px: dec(100.0),
            total_gross_exposure: dec(20_000.0),
        };
        assert!(matches!(
            check(&buy_limit(100.0, 100.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    // ----- max_gross_exposure_pct ------------------------------------------

    #[test]
    fn gross_exposure_pct_sums_across_symbols() {
        let limits = GatekeeperLimits {
            max_gross_exposure_pct: Some(0.50), // 50%
            ..Default::default()
        };
        // Equity 100k, existing gross 40k (all in OTHER symbols, this symbol flat).
        // BUY 200 @ 100 = 20k new → resulting gross 60k = 60% > 50% → deny.
        let acct = AccountInfo {
            equity: Some(usd(100_000.0)),
            symbol_net_qty: Decimal::ZERO,
            symbol_avg_px: Decimal::ZERO,
            total_gross_exposure: dec(40_000.0),
        };
        assert!(matches!(
            check(&buy_limit(200.0, 100.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    #[test]
    fn gross_exposure_replaces_this_symbols_contribution() {
        let limits = GatekeeperLimits {
            max_gross_exposure_pct: Some(0.50),
            ..Default::default()
        };
        // Equity 100k. Gross 40k total, of which THIS symbol is 10k (100 @ 100).
        // BUY 100 more @ 100 → this symbol becomes 200 × 100 = 20k; gross = 40k - 10k + 20k = 50k
        // = 50% (boundary) → allow.
        let acct = AccountInfo {
            equity: Some(usd(100_000.0)),
            symbol_net_qty: dec(100.0),
            symbol_avg_px: dec(100.0),
            total_gross_exposure: dec(40_000.0),
        };
        assert!(check(&buy_limit(100.0, 100.0), &acct, &limits).is_allowed());
    }

    #[test]
    fn non_positive_equity_denies_pct_entry() {
        let limits = GatekeeperLimits {
            max_position_pct: Some(0.25),
            ..Default::default()
        };
        let acct = AccountInfo {
            equity: Some(usd(0.0)),
            ..flat(0.0)
        };
        assert!(matches!(
            check(&buy_limit(1.0, 1.0), &acct, &limits),
            Decision::Deny { .. }
        ));
    }

    // ----- multiple rules: first breach wins, order of checks --------------

    #[test]
    fn share_cap_checked_before_market_entry_ban() {
        // An oversized market entry trips the share cap (an absolute cap) first.
        let limits = GatekeeperLimits {
            max_order_shares: Some(5.0),
            ..Default::default()
        };
        match check(&buy_market(10.0), &flat(1_000_000.0), &limits) {
            Decision::Deny { reason } => assert!(reason.contains(ENV_MAX_ORDER_SHARES)),
            Decision::Allow => panic!("expected deny"),
        }
    }

    // ----- env parsing ------------------------------------------------------

    #[test]
    fn pct_env_parsing_treats_value_as_percent_number() {
        // A percent number in (0, 100]: 25 → 0.25, 0.25 → 0.0025, 100 → 1.0, 2 → 0.02.
        for (raw, expected) in [("25", 0.25), ("0.25", 0.0025), ("100", 1.0), ("2", 0.02)] {
            // SAFETY: single-threaded test; var removed immediately after read.
            unsafe { std::env::set_var("ALPACA_GATE_MAX_POSITION_PCT", raw) };
            let got = parse_pct_env("ALPACA_GATE_MAX_POSITION_PCT").unwrap();
            unsafe { std::env::remove_var("ALPACA_GATE_MAX_POSITION_PCT") };
            assert_eq!(got, Some(expected), "raw={raw}");
        }
    }

    #[test]
    fn pct_env_rejects_out_of_range_and_garbage() {
        // "0"/"-5"/"abc" invalid; "150" and "100.1" exceed 100%.
        for raw in ["0", "-5", "abc", "150", "100.1"] {
            unsafe { std::env::set_var("ALPACA_GATE_MAX_GROSS_EXPOSURE_PCT", raw) };
            let got = parse_pct_env("ALPACA_GATE_MAX_GROSS_EXPOSURE_PCT");
            unsafe { std::env::remove_var("ALPACA_GATE_MAX_GROSS_EXPOSURE_PCT") };
            assert!(got.is_err(), "raw={raw} should be rejected");
        }
    }

    #[test]
    fn positive_env_unset_is_none() {
        unsafe { std::env::remove_var("ALPACA_GATE_MAX_ORDER_SHARES") };
        assert_eq!(parse_positive_env("ALPACA_GATE_MAX_ORDER_SHARES").unwrap(), None);
    }
}
