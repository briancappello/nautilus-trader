//! Market-session clock for the §5a validator.
//!
//! The single place wall-clock → market session lives. A [`MarketCalendar`] trait abstracts the
//! calendar source so it is swappable (the plan defers a venue-agnostic primitive; see
//! `docs/milestone-5-alpaca-execution.md` §5b). The MVP ships [`UsEquityCalendar`], an
//! **owned, auditable** US-equity implementation: the ET session windows below plus a vetted
//! holiday + early-close table. Inlined on purpose — this is the execution hot path of a "never
//! submit an invalid order" gate, so the table must be under our control.
//!
//! Session windows (ET):
//! - Regular  09:30–16:00 Mon–Fri
//! - Pre      04:00–09:30 Mon–Fri
//! - After    16:00–20:00 Mon–Fri
//! - Overnight 20:00–04:00 (Sun evening → Fri morning)
//! - Closed otherwise, and all day on holidays.
//!
//! On an **early-close** day (e.g. day after Thanksgiving, Christmas Eve) Regular ends at 13:00
//! and After-hours runs 13:00–17:00 (Alpaca's early-close extended window), per Nasdaq/NYSE.

use chrono::{Datelike, NaiveDate, NaiveTime, TimeZone, Weekday};
use chrono_tz::US::Eastern;
use nautilus_core::UnixNanos;

/// A US-equity market session at a point in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Session {
    /// Regular trading hours (09:30–16:00 ET, or 09:30–13:00 on early-close days).
    Regular,
    /// Pre-market (04:00–09:30 ET).
    PreMarket,
    /// After-hours (16:00–20:00 ET, or 13:00–17:00 on early-close days).
    AfterHours,
    /// Overnight (20:00–04:00 ET, Sun–Fri).
    Overnight,
    /// Market closed (weekend, holiday, or outside all windows).
    Closed,
}

impl Session {
    /// `true` if this is an extended-hours (non-Regular, non-Closed) session.
    #[must_use]
    pub const fn is_extended_hours(self) -> bool {
        matches!(self, Self::PreMarket | Self::AfterHours | Self::Overnight)
    }
}

/// A swappable market-calendar source. Pure (no I/O); a calendar answers session questions for an
/// instant and the next session open.
pub trait MarketCalendar: std::fmt::Debug + Send + Sync {
    /// The session in effect at `ts`.
    fn session_at(&self, ts: UnixNanos) -> Session;

    /// The next Regular open (09:30 ET on the next trading day at/after `ts`), as `UnixNanos`.
    fn next_regular_open(&self, ts: UnixNanos) -> UnixNanos;

    /// The next pre-market open (04:00 ET on the next trading day at/after `ts`), as `UnixNanos`.
    fn next_premarket_open(&self, ts: UnixNanos) -> UnixNanos;
}

/// An owned, inlined US-equity calendar (ET windows + holiday/early-close table).
#[derive(Clone, Copy, Debug, Default)]
pub struct UsEquityCalendar;

// Session boundary times (ET).
const PRE_OPEN: (u32, u32) = (4, 0); // 04:00
const REGULAR_OPEN: (u32, u32) = (9, 30); // 09:30
const REGULAR_CLOSE: (u32, u32) = (16, 0); // 16:00
const EARLY_CLOSE: (u32, u32) = (13, 0); // 13:00
const AFTER_CLOSE: (u32, u32) = (20, 0); // 20:00
const EARLY_AFTER_CLOSE: (u32, u32) = (17, 0); // 17:00 on early-close days

fn time(hm: (u32, u32)) -> NaiveTime {
    NaiveTime::from_hms_opt(hm.0, hm.1, 0).expect("valid HH:MM")
}

impl UsEquityCalendar {
    /// `true` if `date` is a full-day market holiday (no trading).
    ///
    /// Covers fixed-rule federal market holidays with the standard weekend-observance shift
    /// (Saturday → observed Friday, Sunday → observed Monday), plus the floating holidays
    /// (MLK, Presidents', Memorial, Thanksgiving) and Good Friday. Audited for 2024–2030; extend
    /// `good_friday` / re-verify before the horizon lapses (§5b risk note).
    #[must_use]
    pub fn is_holiday(date: NaiveDate) -> bool {
        let (y, m, wd) = (date.year(), date.month(), date.weekday());

        // New Year's Day (Jan 1, observed).
        if Self::observed(date, 1, 1) {
            return true;
        }
        // Juneteenth (Jun 19, observed) — federal market holiday since 2022.
        if y >= 2022 && Self::observed(date, 6, 19) {
            return true;
        }
        // Independence Day (Jul 4, observed).
        if Self::observed(date, 7, 4) {
            return true;
        }
        // Christmas (Dec 25, observed).
        if Self::observed(date, 12, 25) {
            return true;
        }
        // MLK Day — 3rd Monday of January.
        if m == 1 && wd == Weekday::Mon && Self::nth_weekday_of_month(date) == 3 {
            return true;
        }
        // Presidents' Day — 3rd Monday of February.
        if m == 2 && wd == Weekday::Mon && Self::nth_weekday_of_month(date) == 3 {
            return true;
        }
        // Memorial Day — last Monday of May.
        if m == 5 && wd == Weekday::Mon && Self::is_last_weekday_of_month(date) {
            return true;
        }
        // Labor Day — 1st Monday of September.
        if m == 9 && wd == Weekday::Mon && Self::nth_weekday_of_month(date) == 1 {
            return true;
        }
        // Thanksgiving — 4th Thursday of November.
        if m == 11 && wd == Weekday::Thu && Self::nth_weekday_of_month(date) == 4 {
            return true;
        }
        // Good Friday (markets closed; not a federal holiday) — table-driven.
        if Self::is_good_friday(date) {
            return true;
        }
        false
    }

    /// `true` if `date` is an early-close day (Regular ends 13:00 ET).
    ///
    /// The recurring early closes: July 3 (when a weekday and Jul-4 isn't shifted onto it), the
    /// Friday after Thanksgiving, and Christmas Eve (Dec 24, when a weekday). These are the
    /// standard NYSE/Nasdaq 1pm closes.
    #[must_use]
    pub fn is_early_close(date: NaiveDate) -> bool {
        if Self::is_holiday(date) || Self::is_weekend(date) {
            return false;
        }
        let (m, d, wd) = (date.month(), date.day(), date.weekday());

        // Friday after Thanksgiving (4th Thursday of Nov + 1 day).
        if m == 11
            && wd == Weekday::Fri
            && let Some(prev) = date.pred_opt()
            && prev.weekday() == Weekday::Thu
            && Self::nth_weekday_of_month(prev) == 4
        {
            return true;
        }
        // July 3 (when a weekday and itself not the observed Jul-4 holiday).
        if m == 7 && d == 3 && !Self::is_weekend(date) {
            return true;
        }
        // Christmas Eve, Dec 24 (when a weekday).
        if m == 12 && d == 24 && !Self::is_weekend(date) {
            return true;
        }
        false
    }

    fn is_weekend(date: NaiveDate) -> bool {
        matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
    }

    /// `true` if `date` is the observed market holiday for the fixed `(month, day)`, applying the
    /// Sat→Fri / Sun→Mon weekend shift.
    fn observed(date: NaiveDate, month: u32, day: u32) -> bool {
        let Some(actual) = NaiveDate::from_ymd_opt(date.year(), month, day) else {
            return false;
        };
        let observed = match actual.weekday() {
            Weekday::Sat => actual.pred_opt().unwrap_or(actual), // Friday
            Weekday::Sun => actual.succ_opt().unwrap_or(actual), // Monday
            _ => actual,
        };
        date == observed
    }

    /// Which occurrence of its weekday within the month `date` is (1-based): e.g. 3 for the 3rd
    /// Monday.
    fn nth_weekday_of_month(date: NaiveDate) -> u32 {
        (date.day() - 1) / 7 + 1
    }

    /// `true` if `date` is the last occurrence of its weekday in the month.
    fn is_last_weekday_of_month(date: NaiveDate) -> bool {
        date.checked_add_days(chrono::Days::new(7))
            .is_none_or(|next| next.month() != date.month())
    }

    /// Good Friday dates (markets closed). Table-driven, audited 2024–2030.
    fn is_good_friday(date: NaiveDate) -> bool {
        const GOOD_FRIDAYS: &[(i32, u32, u32)] = &[
            (2024, 3, 29),
            (2025, 4, 18),
            (2026, 4, 3),
            (2027, 3, 26),
            (2028, 4, 14),
            (2029, 3, 30),
            (2030, 4, 19),
        ];
        GOOD_FRIDAYS
            .iter()
            .any(|&(y, m, d)| date.year() == y && date.month() == m && date.day() == d)
    }

    /// Converts `ts` to an ET date + time-of-day, then classifies the session.
    fn classify(ts: UnixNanos) -> Session {
        let dt_utc = chrono::DateTime::from_timestamp_nanos(ts.as_i64());
        let et = dt_utc.with_timezone(&Eastern);
        let date = et.date_naive();
        let tod = et.time();

        if Self::is_weekend(date) {
            // Overnight (Sun 20:00 → Mon 04:00) is the only "open" window touching a weekend day;
            // Saturday and Sunday-before-20:00 are Closed. Sunday ≥ 20:00 ET is Overnight.
            if date.weekday() == Weekday::Sun && tod >= time(AFTER_CLOSE) {
                return Session::Overnight;
            }
            return Session::Closed;
        }
        if Self::is_holiday(date) {
            return Session::Closed;
        }

        let early = Self::is_early_close(date);
        let regular_close = if early { EARLY_CLOSE } else { REGULAR_CLOSE };
        let after_close = if early {
            EARLY_AFTER_CLOSE
        } else {
            AFTER_CLOSE
        };

        if tod >= time(REGULAR_OPEN) && tod < time(regular_close) {
            Session::Regular
        } else if tod >= time(PRE_OPEN) && tod < time(REGULAR_OPEN) {
            Session::PreMarket
        } else if tod >= time(regular_close) && tod < time(after_close) {
            Session::AfterHours
        } else if tod >= time(after_close) {
            // Evening overnight (after the after-hours window, before midnight). Friday evening
            // has no overnight session (weekend), so Friday ≥ after_close is Closed.
            if date.weekday() == Weekday::Fri {
                Session::Closed
            } else {
                Session::Overnight
            }
        } else {
            // Before 04:00: still the prior evening's overnight session (Tue–Fri pre-dawn).
            Session::Overnight
        }
    }

    /// The `UnixNanos` of `time_hm` ET on `date` (DST-correct).
    fn et_instant(date: NaiveDate, time_hm: (u32, u32)) -> UnixNanos {
        let naive = date.and_time(time(time_hm));
        // `from_local_datetime` can be ambiguous around DST; `.earliest()` is the conventional
        // pick and these boundary times (04:00/09:30) never fall in the 02:00 DST gap.
        let dt = Eastern
            .from_local_datetime(&naive)
            .earliest()
            .expect("valid ET local datetime");
        UnixNanos::from(dt.timestamp_nanos_opt().expect("in-range timestamp") as u64)
    }

    /// The next trading day at/after `from` whose given `at` time is strictly after `ts`.
    fn next_open(ts: UnixNanos, at: (u32, u32)) -> UnixNanos {
        let dt_utc = chrono::DateTime::from_timestamp_nanos(ts.as_i64());
        let mut date = dt_utc.with_timezone(&Eastern).date_naive();
        for _ in 0..14 {
            if !Self::is_weekend(date) && !Self::is_holiday(date) {
                let open = Self::et_instant(date, at);
                if open.as_u64() > ts.as_u64() {
                    return open;
                }
            }
            date = date.succ_opt().expect("date in range");
        }
        // Fallback (should never hit within a 2-week window): just return ts.
        ts
    }
}

impl MarketCalendar for UsEquityCalendar {
    fn session_at(&self, ts: UnixNanos) -> Session {
        Self::classify(ts)
    }

    fn next_regular_open(&self, ts: UnixNanos) -> UnixNanos {
        Self::next_open(ts, REGULAR_OPEN)
    }

    fn next_premarket_open(&self, ts: UnixNanos) -> UnixNanos {
        Self::next_open(ts, PRE_OPEN)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    /// Builds a `UnixNanos` from an ET wall-clock datetime (for readable test cases).
    fn et(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> UnixNanos {
        let naive = NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, 0)
            .unwrap();
        let dt = Eastern.from_local_datetime(&naive).earliest().unwrap();
        UnixNanos::from(dt.timestamp_nanos_opt().unwrap() as u64)
    }

    #[test]
    fn regular_session_weekday_midday() {
        // Wed 2026-06-24 11:00 ET → Regular.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 11, 0)),
            Session::Regular
        );
    }

    #[test]
    fn premarket_and_afterhours() {
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 5, 0)),
            Session::PreMarket
        );
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 17, 0)),
            Session::AfterHours
        );
    }

    #[test]
    fn boundaries_open_and_close() {
        // 09:30 exactly → Regular; 16:00 exactly → AfterHours (regular is half-open [open, close)).
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 9, 30)),
            Session::Regular
        );
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 16, 0)),
            Session::AfterHours
        );
        // 04:00 exactly → PreMarket; 03:59 → Overnight.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 4, 0)),
            Session::PreMarket
        );
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 3, 59)),
            Session::Overnight
        );
    }

    #[test]
    fn overnight_and_weekend() {
        // Wed 21:00 ET → Overnight.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 24, 21, 0)),
            Session::Overnight
        );
        // Fri 21:00 ET → Closed (no weekend overnight).
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 26, 21, 0)),
            Session::Closed
        );
        // Saturday midday → Closed.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 27, 12, 0)),
            Session::Closed
        );
        // Sunday 21:00 ET → Overnight (week's first session).
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 6, 28, 21, 0)),
            Session::Overnight
        );
    }

    #[test]
    fn holidays_are_closed() {
        // Christmas 2026 (Dec 25 is a Friday) → Closed.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 12, 25, 11, 0)),
            Session::Closed
        );
        // Independence Day 2026: Jul 4 is Saturday → observed Friday Jul 3. So Jul 3 is the
        // holiday (closed), NOT an early close.
        assert!(UsEquityCalendar::is_holiday(
            NaiveDate::from_ymd_opt(2026, 7, 3).unwrap()
        ));
        // New Year's Day 2026 (Jan 1, Thursday) → Closed.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 1, 1, 11, 0)),
            Session::Closed
        );
        // MLK 2026 = 3rd Monday Jan = Jan 19.
        assert!(UsEquityCalendar::is_holiday(
            NaiveDate::from_ymd_opt(2026, 1, 19).unwrap()
        ));
        // Thanksgiving 2026 = 4th Thursday Nov = Nov 26.
        assert!(UsEquityCalendar::is_holiday(
            NaiveDate::from_ymd_opt(2026, 11, 26).unwrap()
        ));
        // Good Friday 2026 = Apr 3.
        assert!(UsEquityCalendar::is_holiday(
            NaiveDate::from_ymd_opt(2026, 4, 3).unwrap()
        ));
    }

    #[test]
    fn early_close_day_shifts_windows() {
        // Friday after Thanksgiving 2026 = Nov 27. Regular ends 13:00; 14:00 → AfterHours.
        let nov27 = NaiveDate::from_ymd_opt(2026, 11, 27).unwrap();
        assert!(UsEquityCalendar::is_early_close(nov27));
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 11, 27, 12, 30)),
            Session::Regular
        );
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 11, 27, 14, 0)),
            Session::AfterHours
        );
        // Nov 27 is a FRIDAY early close; early after-hours ends 17:00, and Friday evening has no
        // overnight session → 17:00 is Closed. (On a weekday early close it would be Overnight.)
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 11, 27, 17, 0)),
            Session::Closed
        );
        // 16:30 (within early after-hours 13:00–17:00) is still AfterHours.
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 11, 27, 16, 30)),
            Session::AfterHours
        );
    }

    #[test]
    fn next_regular_open_skips_weekend_and_holiday() {
        // From Fri 2026-06-26 18:00 ET, next regular open is Mon 2026-06-29 09:30 ET.
        let from = et(2026, 6, 26, 18, 0);
        let open = UsEquityCalendar.next_regular_open(from);
        let expected = et(2026, 6, 29, 9, 30);
        assert_eq!(open, expected);
    }

    #[test]
    fn next_premarket_open_is_4am() {
        // From Wed 2026-06-24 21:00 ET, next pre-market open is Thu 04:00 ET.
        let from = et(2026, 6, 24, 21, 0);
        let open = UsEquityCalendar.next_premarket_open(from);
        let expected = et(2026, 6, 25, 4, 0);
        assert_eq!(open, expected);
    }

    #[test]
    fn dst_spring_forward_regular_open() {
        // 2026 DST begins Sun Mar 8. Mon Mar 9 09:30 ET → Regular (offset -04:00).
        assert_eq!(
            UsEquityCalendar.session_at(et(2026, 3, 9, 9, 30)),
            Session::Regular
        );
    }
}
