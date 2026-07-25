//! The §5a client-side order validator — "never submit an order Alpaca would reject".
//!
//! Pure logic joining `(order shape, session, asset)` → [`Validity`], sitting in `submit_order`
//! after the [`crate::gatekeeper`] (risk) and before the network. Where the gatekeeper answers
//! "do our risk limits allow this?", the validator answers "would Alpaca's server accept this
//! shape right now?". It decides the `extended_hours` flag **internally** (never on the seam) and
//! distinguishes two reject classes for the `queue_for_open` policy:
//!
//! - **Session-reject** — right shape, wrong session (e.g. MARKET after-hours, bracket in EH).
//!   Default reject-local; an order tagged `queue_for_open` is instead *held* to the next valid
//!   open.
//! - **Structure-reject** — no session makes it valid (sub-penny price, unsupported type/TIF).
//!   Always rejected immediately; the tag never rescues it.

use nautilus_core::UnixNanos;
use nautilus_model::enums::{OrderType, TimeInForce};

use crate::{
    rules::{check_subpenny, complex_allowed, simple_validity},
    session::{MarketCalendar, Session},
};

/// The recognized order tag that opts an order into hold-until-open on a session-reject.
pub const QUEUE_FOR_OPEN_TAG: &str = "queue_for_open";

/// The shape of the order under validation (extracted primitives).
#[derive(Clone, Copy, Debug)]
pub struct OrderShape {
    /// The order type.
    pub order_type: OrderType,
    /// The time-in-force.
    pub time_in_force: TimeInForce,
    /// `true` if this is a complex (bracket/oco/oto) order.
    pub is_complex: bool,
    /// Limit price, if present (for the sub-penny check).
    pub limit_price: Option<f64>,
    /// Stop/trigger price, if present (for the sub-penny check).
    pub stop_price: Option<f64>,
    /// Trailing offset expressed as an absolute price, if present (sub-penny check). A
    /// percent-based trailing offset is not a price and is not checked here.
    pub trail_price: Option<f64>,
}

/// The validator's verdict.
#[derive(Clone, Debug, PartialEq)]
pub enum Validity {
    /// Valid now; submit with the given `extended_hours` flag.
    Ok {
        /// Whether to set `extended_hours=true` on the payload.
        extended_hours: bool,
    },
    /// Right shape, wrong session — reject now, or hold if `queue_for_open`-tagged.
    SessionReject {
        /// Human-readable reason.
        reason: String,
    },
    /// Structurally invalid — always reject; the queue tag cannot rescue it.
    StructureReject {
        /// Human-readable reason.
        reason: String,
    },
}

/// What `(order_type, tif)` simple shapes are accepted right now, plus complex-order allowance.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderCapabilities {
    /// The session these capabilities describe.
    pub session: Session,
    /// Accepted simple `(order_type, tif)` pairs right now (Nautilus terms; never exposes the
    /// `extended_hours` flag).
    pub simple: Vec<(OrderType, TimeInForce)>,
    /// Whether bracket/OCO/OTO are allowed right now.
    pub complex_allowed: bool,
}

/// The order validator over a [`MarketCalendar`].
#[derive(Debug)]
pub struct OrderValidator<C: MarketCalendar> {
    calendar: C,
}

impl<C: MarketCalendar> OrderValidator<C> {
    /// Creates a validator over `calendar`.
    pub const fn new(calendar: C) -> Self {
        Self { calendar }
    }

    /// The session in effect at `at`.
    pub fn session_at(&self, at: UnixNanos) -> Session {
        self.calendar.session_at(at)
    }

    /// Validates `shape` at instant `at`. Sub-penny / type / TIF problems are structure-rejects;
    /// session mismatches are session-rejects. On success, returns the internal `extended_hours`.
    #[must_use]
    pub fn validate(&self, shape: &OrderShape, at: UnixNanos) -> Validity {
        // Structure-rejects first (no session makes these valid).
        if let Some(px) = shape.limit_price
            && let Err(reason) = check_subpenny(px)
        {
            return Validity::StructureReject { reason };
        }
        if let Some(px) = shape.stop_price
            && let Err(reason) = check_subpenny(px)
        {
            return Validity::StructureReject { reason };
        }
        if let Some(px) = shape.trail_price
            && let Err(reason) = check_subpenny(px)
        {
            return Validity::StructureReject { reason };
        }
        // Alpaca does NOT support GTD for US equities (returns 422 even with expires_at);
        // only DAY/GTC are valid. Reject anything else fast, locally, with a clear reason.
        if !matches!(shape.time_in_force, TimeInForce::Day | TimeInForce::Gtc) {
            return Validity::StructureReject {
                reason: format!(
                    "time-in-force {:?} is out of scope for US equities (only DAY/GTC; \
                     Alpaca rejects GTD/IOC/FOK/auction TIFs)",
                    shape.time_in_force
                ),
            };
        }

        let session = self.calendar.session_at(at);

        if shape.is_complex {
            return if complex_allowed(shape.time_in_force, session) {
                Validity::Ok {
                    extended_hours: false,
                }
            } else {
                Validity::SessionReject {
                    reason: format!(
                        "bracket/OCO/OTO orders require Regular hours (current session: {session:?})"
                    ),
                }
            };
        }

        match simple_validity(shape.order_type, shape.time_in_force, session) {
            Some(extended_hours) => Validity::Ok { extended_hours },
            None => Validity::SessionReject {
                reason: format!(
                    "{:?} {:?} not accepted in session {session:?}",
                    shape.order_type, shape.time_in_force
                ),
            },
        }
    }

    /// The accepted order capabilities at `at` (read-only; for dry-runs/tests).
    #[must_use]
    pub fn order_capabilities(&self, at: UnixNanos) -> OrderCapabilities {
        let session = self.calendar.session_at(at);
        let mut simple = Vec::new();
        for ot in [
            OrderType::Market,
            OrderType::Limit,
            OrderType::StopMarket,
            OrderType::StopLimit,
        ] {
            for tif in [TimeInForce::Day, TimeInForce::Gtc] {
                if simple_validity(ot, tif, session).is_some() {
                    simple.push((ot, tif));
                }
            }
        }
        OrderCapabilities {
            session,
            simple,
            complex_allowed: complex_allowed(TimeInForce::Day, session),
        }
    }

    /// The earliest valid session-open `UnixNanos` to release a held (`queue_for_open`) order.
    ///
    /// EH-eligible simple orders (limit + day/gtc) release at the next pre-market open (04:00 ET);
    /// everything else (market/stop/stop-limit, complex) releases at the next Regular open
    /// (09:30 ET).
    #[must_use]
    pub fn earliest_valid_open(&self, shape: &OrderShape, at: UnixNanos) -> UnixNanos {
        let eh_eligible = !shape.is_complex
            && shape.order_type == OrderType::Limit
            && matches!(shape.time_in_force, TimeInForce::Day | TimeInForce::Gtc);
        if eh_eligible {
            self.calendar.next_premarket_open(at)
        } else {
            self.calendar.next_regular_open(at)
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone};
    use chrono_tz::US::Eastern;

    use super::*;
    use crate::session::UsEquityCalendar;

    fn et(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> UnixNanos {
        let naive = NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap();
        let dt = Eastern.from_local_datetime(&naive).earliest().unwrap();
        UnixNanos::from(dt.timestamp_nanos_opt().unwrap() as u64)
    }

    fn validator() -> OrderValidator<UsEquityCalendar> {
        OrderValidator::new(UsEquityCalendar)
    }

    fn simple(ot: OrderType, tif: TimeInForce, limit: Option<f64>) -> OrderShape {
        OrderShape {
            order_type: ot,
            time_in_force: tif,
            is_complex: false,
            limit_price: limit,
            stop_price: None,
            trail_price: None,
        }
    }

    // Regular-hours timestamp (Wed 2026-06-24 11:00 ET) and an after-hours one (17:00 ET).
    fn regular() -> UnixNanos {
        et(2026, 6, 24, 11, 0)
    }
    fn after_hours() -> UnixNanos {
        et(2026, 6, 24, 17, 0)
    }
    fn premarket() -> UnixNanos {
        et(2026, 6, 24, 5, 0)
    }

    #[test]
    fn market_in_regular_ok_no_eh() {
        let v = validator().validate(&simple(OrderType::Market, TimeInForce::Day, None), regular());
        assert_eq!(
            v,
            Validity::Ok {
                extended_hours: false
            }
        );
    }

    #[test]
    fn market_after_hours_session_reject() {
        let v = validator().validate(
            &simple(OrderType::Market, TimeInForce::Day, None),
            after_hours(),
        );
        assert!(matches!(v, Validity::SessionReject { .. }));
    }

    #[test]
    fn limit_premarket_sets_extended_hours() {
        let v = validator().validate(
            &simple(OrderType::Limit, TimeInForce::Day, Some(189.50)),
            premarket(),
        );
        assert_eq!(
            v,
            Validity::Ok {
                extended_hours: true
            }
        );
    }

    #[test]
    fn limit_after_hours_sets_extended_hours() {
        let v = validator().validate(
            &simple(OrderType::Limit, TimeInForce::Gtc, Some(189.50)),
            after_hours(),
        );
        assert_eq!(
            v,
            Validity::Ok {
                extended_hours: true
            }
        );
    }

    #[test]
    fn subpenny_is_structure_reject() {
        // Wrong-decimals price is a structure reject regardless of session.
        let v = validator().validate(
            &simple(OrderType::Limit, TimeInForce::Day, Some(189.123)),
            regular(),
        );
        assert!(matches!(v, Validity::StructureReject { .. }));
    }

    #[test]
    fn bracket_in_regular_ok_in_eh_session_reject() {
        let bracket = OrderShape {
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            is_complex: true,
            limit_price: Some(189.50),
            stop_price: None,
            trail_price: None,
        };
        assert_eq!(
            validator().validate(&bracket, regular()),
            Validity::Ok {
                extended_hours: false
            }
        );
        assert!(matches!(
            validator().validate(&bracket, after_hours()),
            Validity::SessionReject { .. }
        ));
    }

    #[test]
    fn unsupported_tif_structure_reject() {
        let v = validator().validate(
            &simple(OrderType::Limit, TimeInForce::Ioc, Some(189.50)),
            regular(),
        );
        assert!(matches!(v, Validity::StructureReject { .. }));
    }

    #[test]
    fn capabilities_per_session() {
        let caps_reg = validator().order_capabilities(regular());
        assert_eq!(caps_reg.session, Session::Regular);
        assert_eq!(caps_reg.simple.len(), 8); // 4 types × 2 TIFs
        assert!(caps_reg.complex_allowed);

        let caps_eh = validator().order_capabilities(after_hours());
        assert_eq!(caps_eh.session, Session::AfterHours);
        // Only limit × {day,gtc}.
        assert_eq!(caps_eh.simple.len(), 2);
        assert!(caps_eh.simple.iter().all(|(ot, _)| *ot == OrderType::Limit));
        assert!(!caps_eh.complex_allowed);
    }

    #[test]
    fn earliest_open_eh_eligible_vs_not() {
        // After-hours Wed 17:00. EH-eligible limit → next pre-market (Thu 04:00).
        let limit = simple(OrderType::Limit, TimeInForce::Day, Some(189.50));
        assert_eq!(
            validator().earliest_valid_open(&limit, after_hours()),
            et(2026, 6, 25, 4, 0)
        );
        // Market → next regular open (Thu 09:30).
        let market = simple(OrderType::Market, TimeInForce::Day, None);
        assert_eq!(
            validator().earliest_valid_open(&market, after_hours()),
            et(2026, 6, 25, 9, 30)
        );
    }
}
