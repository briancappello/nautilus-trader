//! Alpaca order-validity rule matrix (§5a) — pure const data, no clock, no I/O.
//!
//! Encodes "what order shapes does Alpaca accept in which session" so the validator can reject
//! locally anything the server would reject. Sourced from Alpaca's *Orders at Alpaca* docs,
//! cross-checked against `alpaca-py`. Scope is whole-share US equities, DAY/GTC TIF (the only
//! TIFs our strategies emit); IOC/FOK/OPG/CLS are out of scope and rejected if seen.

use nautilus_model::enums::{OrderType, TimeInForce};

use crate::session::Session;

/// Whether a *simple* `(order_type, tif)` order is accepted in `session`, and if so whether it
/// requires the `extended_hours` flag.
///
/// Returns:
/// - `Some(true)`  → valid, set `extended_hours=true`.
/// - `Some(false)` → valid, `extended_hours` false/omitted.
/// - `None`        → not valid in this session (session-reject).
#[must_use]
pub fn simple_validity(
    order_type: OrderType,
    tif: TimeInForce,
    session: Session,
) -> Option<bool> {
    // Only DAY / GTC are in scope. Alpaca rejects GTD (and IOC/FOK/auction) for US equities.
    if !matches!(tif, TimeInForce::Day | TimeInForce::Gtc) {
        return None;
    }

    match session {
        Session::Regular => match order_type {
            // Supported simple types in RTH, no extended-hours flag. Alpaca accepts
            // market, limit, stop, stop-limit, trailing-stop, and (as a market) MTL.
            OrderType::Market
            | OrderType::Limit
            | OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
            | OrderType::MarketToLimit => Some(false),
            _ => None,
        },
        Session::PreMarket | Session::AfterHours | Session::Overnight => match order_type {
            // Extended hours: ONLY limit orders, and they must carry extended_hours=true.
            // (Alpaca does not accept market/stop/trailing in extended hours.)
            OrderType::Limit => Some(true),
            _ => None,
        },
        Session::Closed => None,
    }
}

/// Whether a complex order (`order_class ∈ {bracket, oco, oto}`) is accepted in `session`.
///
/// Brackets/OCO/OTO require Regular hours (extended hours unsupported) and DAY/GTC TIF. This is
/// decisive for the breakout strategy: its bracket entries are not submittable outside RTH.
#[must_use]
pub fn complex_allowed(tif: TimeInForce, session: Session) -> bool {
    matches!(tif, TimeInForce::Day | TimeInForce::Gtc) && session == Session::Regular
}

/// Validates Alpaca's sub-penny price rule.
///
/// Limit/stop prices ≥ $1.00 may have at most 2 decimals; prices < $1.00 at most 4 decimals
/// (else Alpaca rejects with `code 42210000`). Returns `Err(reason)` on violation.
///
/// # Errors
///
/// Returns an error string describing the sub-penny violation.
pub fn check_subpenny(price: f64) -> Result<(), String> {
    if !price.is_finite() || price <= 0.0 {
        return Err(format!("price {price} must be positive and finite"));
    }
    let max_dp = if price >= 1.0 { 2 } else { 4 };
    // Count decimal places by scaling and checking for a clean integer.
    let scaled = price * 10f64.powi(max_dp);
    if (scaled.round() - scaled).abs() > 1e-9 {
        return Err(format!(
            "price {price} violates sub-penny rule (max {max_dp} decimals for price {} $1.00)",
            if price >= 1.0 { "≥" } else { "<" }
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_accepts_all_types_no_eh() {
        for ot in [
            OrderType::Market,
            OrderType::Limit,
            OrderType::StopMarket,
            OrderType::StopLimit,
            OrderType::TrailingStopMarket,
            OrderType::TrailingStopLimit,
            OrderType::MarketToLimit,
        ] {
            assert_eq!(
                simple_validity(ot, TimeInForce::Day, Session::Regular),
                Some(false),
                "{ot:?} should be valid in Regular with no EH flag"
            );
        }
    }

    #[test]
    fn gtd_out_of_scope_for_equities() {
        // Alpaca rejects GTD for US equities (422), so it must be out of scope here.
        assert_eq!(
            simple_validity(OrderType::Limit, TimeInForce::Gtd, Session::Regular),
            None
        );
    }

    #[test]
    fn trailing_and_mtl_rejected_in_extended_hours() {
        // Alpaca accepts trailing-stop / market-to-limit only in Regular hours.
        for session in [Session::PreMarket, Session::AfterHours] {
            for ot in [
                OrderType::TrailingStopMarket,
                OrderType::MarketToLimit,
            ] {
                assert_eq!(
                    simple_validity(ot, TimeInForce::Day, session),
                    None,
                    "{ot:?} should be rejected in {session:?}"
                );
            }
        }
    }

    #[test]
    fn extended_hours_only_limit_with_eh_flag() {
        for session in [Session::PreMarket, Session::AfterHours, Session::Overnight] {
            assert_eq!(
                simple_validity(OrderType::Limit, TimeInForce::Day, session),
                Some(true),
                "limit should be valid in {session:?} with EH flag"
            );
            for ot in [OrderType::Market, OrderType::StopMarket, OrderType::StopLimit] {
                assert_eq!(
                    simple_validity(ot, TimeInForce::Day, session),
                    None,
                    "{ot:?} should be rejected in {session:?}"
                );
            }
        }
    }

    #[test]
    fn closed_rejects_everything() {
        for ot in [OrderType::Market, OrderType::Limit] {
            assert_eq!(
                simple_validity(ot, TimeInForce::Gtc, Session::Closed),
                None
            );
        }
    }

    #[test]
    fn unsupported_tif_rejected() {
        assert_eq!(
            simple_validity(OrderType::Limit, TimeInForce::Ioc, Session::Regular),
            None
        );
    }

    #[test]
    fn complex_only_in_regular() {
        assert!(complex_allowed(TimeInForce::Day, Session::Regular));
        assert!(complex_allowed(TimeInForce::Gtc, Session::Regular));
        for s in [
            Session::PreMarket,
            Session::AfterHours,
            Session::Overnight,
            Session::Closed,
        ] {
            assert!(!complex_allowed(TimeInForce::Day, s), "complex in {s:?}");
        }
        assert!(!complex_allowed(TimeInForce::Ioc, Session::Regular));
    }

    #[test]
    fn subpenny_rules() {
        // ≥ $1: max 2dp.
        assert!(check_subpenny(189.50).is_ok());
        assert!(check_subpenny(189.123).is_err());
        // < $1: max 4dp.
        assert!(check_subpenny(0.1234).is_ok());
        assert!(check_subpenny(0.12345).is_err());
        // boundary $1.00.
        assert!(check_subpenny(1.00).is_ok());
        assert!(check_subpenny(0.0).is_err());
    }
}
