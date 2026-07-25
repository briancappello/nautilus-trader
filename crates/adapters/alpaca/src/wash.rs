//! Local pre-rejection of orders Alpaca would 403 as wash trades / short-blocks.
//!
//! Alpaca (US equities, no shorting-through-pending) rejects two order shapes that depend on
//! *live account state* — the current position and your own open orders on the symbol:
//!
//! 1. **Short / insufficient-quantity block** (`code 40310000`): a SELL is refused when the
//!    already-reserved open-sell quantity plus this order would exceed the long position
//!    (`open_sell_qty + new_qty > position_net_qty`) — it would open/extend a short while long
//!    exposure is committed. Symmetric for a BUY against a short position.
//! 2. **Wash-trade self-cross**: a *limit* order priced to cross one of your own resting
//!    opposite-side *limit* orders (a SELL limit at/below a resting BUY limit, or a BUY limit
//!    at/above a resting SELL limit) — it would trade against yourself.
//!
//! Enforcing these locally turns a wasted network round-trip + 403 into an immediate, clearly
//! reasoned `OrderDenied`. This module is pure (no I/O): the caller supplies the order facts and
//! a snapshot of open orders from the cache.

use rust_decimal::Decimal;

/// A minimal view of an open order on the same symbol, for wash/short detection.
#[derive(Clone, Copy, Debug)]
pub struct OpenOrderView {
    /// `true` if this open order is a BUY, `false` if a SELL.
    pub is_buy: bool,
    /// Remaining (unfilled) quantity still working / reserving shares.
    pub leaves_qty: Decimal,
    /// Limit price, if this is a priced (limit / stop-limit) order.
    pub limit_price: Option<Decimal>,
}

/// The order under evaluation, reduced to the facts the checks need.
#[derive(Clone, Copy, Debug)]
pub struct PendingOrder {
    /// `true` if BUY, `false` if SELL.
    pub is_buy: bool,
    /// Order quantity.
    pub quantity: Decimal,
    /// Limit price, if priced (used for the wash self-cross check).
    pub limit_price: Option<Decimal>,
}

/// Checks whether Alpaca would reject `order` as a wash trade / short-block, given the current
/// signed position net quantity and the open orders on the same symbol.
///
/// Returns `Some(reason)` to reject locally (mirroring Alpaca's message), or `None` to allow.
#[must_use]
pub fn check_wash_trade(
    order: &PendingOrder,
    position_net_qty: Decimal,
    open_orders: &[OpenOrderView],
) -> Option<String> {
    // (1) Short / insufficient-quantity block. A SELL may only dispose of shares actually held
    //     and not already reserved by other open SELLs. If selling more than that, Alpaca refuses
    //     (it would open a short while long — "cannot open a short sell while a long buy order is
    //     open" / "insufficient qty available").
    if !order.is_buy {
        // Only long positions can back a SELL here. (Reducing an existing short is a BUY.)
        let long_qty = position_net_qty.max(Decimal::ZERO);
        let reserved_sells: Decimal = open_orders
            .iter()
            .filter(|o| !o.is_buy)
            .map(|o| o.leaves_qty)
            .sum();
        let available = long_qty - reserved_sells;
        if order.quantity > available {
            return Some(format!(
                "would sell {} share(s) but only {} available (position {}, {} reserved by open \
                 sells) — Alpaca rejects this as an uncovered short / insufficient qty",
                order.quantity, available, position_net_qty, reserved_sells,
            ));
        }
    } else {
        // Symmetric BUY-against-short block (covers a short with more than it holds → would flip
        // long while a short-reducing order rests). Rare for our long-only strategies but exact.
        let short_qty = (-position_net_qty).max(Decimal::ZERO);
        if short_qty > Decimal::ZERO {
            let reserved_buys: Decimal = open_orders
                .iter()
                .filter(|o| o.is_buy)
                .map(|o| o.leaves_qty)
                .sum();
            let available = short_qty - reserved_buys;
            if order.quantity > available {
                return Some(format!(
                    "would buy {} share(s) to cover a {} short but only {} available ({} reserved \
                     by open buys) — Alpaca rejects this as flipping long while short",
                    order.quantity, short_qty, available, reserved_buys,
                ));
            }
        }
    }

    // (2) Wash-trade self-cross. A priced order that would cross one of your own resting priced
    //     opposite-side orders trades against yourself. SELL limit <= resting BUY limit, or
    //     BUY limit >= resting SELL limit.
    if let Some(px) = order.limit_price {
        for other in open_orders.iter().filter(|o| o.is_buy != order.is_buy) {
            let Some(other_px) = other.limit_price else {
                continue;
            };
            let crosses = if order.is_buy {
                px >= other_px // our buy at/above a resting sell
            } else {
                px <= other_px // our sell at/below a resting buy
            };
            if crosses {
                return Some(format!(
                    "limit {px} would cross own resting {} limit {other_px} — Alpaca rejects this \
                     as a potential wash trade (use complex/limit/stop-limit)",
                    if other.is_buy { "buy" } else { "sell" },
                ));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    fn sell(qty: &str, limit: Option<&str>) -> PendingOrder {
        PendingOrder {
            is_buy: false,
            quantity: dec(qty),
            limit_price: limit.map(dec),
        }
    }
    fn buy(qty: &str, limit: Option<&str>) -> PendingOrder {
        PendingOrder {
            is_buy: true,
            quantity: dec(qty),
            limit_price: limit.map(dec),
        }
    }
    fn open_sell(qty: &str, limit: Option<&str>) -> OpenOrderView {
        OpenOrderView {
            is_buy: false,
            leaves_qty: dec(qty),
            limit_price: limit.map(dec),
        }
    }
    fn open_buy(qty: &str, limit: Option<&str>) -> OpenOrderView {
        OpenOrderView {
            is_buy: true,
            leaves_qty: dec(qty),
            limit_price: limit.map(dec),
        }
    }

    #[test]
    fn sell_within_position_is_allowed() {
        // Hold 2, sell 1, nothing reserved → OK.
        assert!(check_wash_trade(&sell("1", None), dec("2"), &[]).is_none());
    }

    #[test]
    fn sell_exceeding_position_is_blocked() {
        // Hold 0, sell 1 → would short.
        assert!(check_wash_trade(&sell("1", None), dec("0"), &[]).is_some());
    }

    #[test]
    fn sell_exceeding_available_after_reservation_is_blocked() {
        // Hold 2, one open sell reserves 2 → 0 available, sell 1 blocked.
        let reason = check_wash_trade(&sell("1", None), dec("2"), &[open_sell("2", None)]);
        assert!(reason.is_some());
        assert!(reason.unwrap().contains("only 0 available"));
    }

    #[test]
    fn sell_using_remaining_available_is_allowed() {
        // Hold 3, one open sell reserves 1 → 2 available, sell 2 OK.
        assert!(
            check_wash_trade(&sell("2", None), dec("3"), &[open_sell("1", None)]).is_none()
        );
    }

    #[test]
    fn sell_limit_crossing_resting_buy_is_wash() {
        // Hold 5 (so short-block passes), sell limit 3.00 <= resting buy 3.00 → wash.
        let reason =
            check_wash_trade(&sell("1", Some("3.00")), dec("5"), &[open_buy("1", Some("3.00"))]);
        assert!(reason.is_some());
        assert!(reason.unwrap().contains("wash trade"));
    }

    #[test]
    fn sell_limit_above_resting_buy_not_wash() {
        // Sell limit 3.01 > resting buy 3.00 → no self-cross (short-block already satisfied).
        assert!(
            check_wash_trade(&sell("1", Some("3.01")), dec("5"), &[open_buy("1", Some("3.00"))])
                .is_none()
        );
    }

    #[test]
    fn buy_limit_crossing_resting_sell_is_wash() {
        // Buy limit 4.00 >= resting sell 4.00 → wash (no short position, so short-block N/A).
        let reason =
            check_wash_trade(&buy("1", Some("4.00")), dec("0"), &[open_sell("1", Some("4.00"))]);
        assert!(reason.is_some());
    }

    #[test]
    fn plain_buy_entry_is_allowed() {
        // A normal opening BUY with no opposing resting order → allowed.
        assert!(check_wash_trade(&buy("1", Some("4.00")), dec("0"), &[]).is_none());
    }
}
