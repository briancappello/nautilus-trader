//! `BarType` <-> MarketStore TBK string conversion.
//!
//! MarketStore addresses time-series by a "TBK": `<symbol>/<timeframe>/<attrgroup>`.
//! For bars the attrgroup is `OHLCV` and the timeframe encodes the bar step, e.g.
//! `AAPL/1Min/OHLCV`, `SPY/5Min/OHLCV`, `MSFT/1D/OHLCV`. MarketStore is venue-less:
//! the venue lives only in the Nautilus `InstrumentId`, never in the TBK.

use std::str::FromStr;

use anyhow::{Result, bail};
use nautilus_model::data::bar::BarType;
use nautilus_model::enums::BarAggregation;

/// Builds the MarketStore TBK destination string for a bars query/subscription.
///
/// Example: `AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL` -> `AAPL/1Min/OHLCV`.
pub fn bar_type_to_tbk(bar_type: &BarType) -> Result<String> {
    let symbol = bar_type.instrument_id().symbol;
    let spec = bar_type.spec();
    let step = spec.step.get();
    let suffix = aggregation_suffix(spec.aggregation)?;
    Ok(format!("{symbol}/{step}{suffix}/OHLCV"))
}

/// Reverse of [`bar_type_to_tbk`]: parse a MarketStore bar TBK + a venue into a
/// `BarType`. Used by the **glob** streaming path, where incoming frames are keyed by
/// the resolved `SYMBOL/<step><unit>/OHLCV` (not a pre-registered TBK), so the adapter
/// must synthesize the `BarType` on the fly.
///
/// Example: `("AAPL/1Min/OHLCV", "MARKETSTORE")` -> `AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL`.
/// Only `OHLCV` (bar) TBKs are accepted.
pub fn tbk_to_bar_type(tbk: &str, venue: &str) -> Result<BarType> {
    let parts: Vec<&str> = tbk.split('/').collect();
    let [symbol, timeframe, attrgroup] = parts.as_slice() else {
        bail!("malformed TBK: {tbk}");
    };
    if *attrgroup != "OHLCV" {
        bail!("not a bar TBK (attrgroup {attrgroup}): {tbk}");
    }
    // Split "1Min" -> step "1", unit "Min".
    let split = timeframe
        .find(|c: char| c.is_alphabetic())
        .ok_or_else(|| anyhow::anyhow!("no unit in timeframe {timeframe}"))?;
    let (step, unit) = timeframe.split_at(split);
    let aggregation = match unit {
        "Sec" => "SECOND",
        "Min" => "MINUTE",
        "H" => "HOUR",
        "D" => "DAY",
        other => bail!("unsupported TBK timeframe unit: {other}"),
    };
    let step: u32 = step.parse().map_err(|_| anyhow::anyhow!("bad step {step}"))?;
    let s = format!("{symbol}.{venue}-{step}-{aggregation}-LAST-EXTERNAL");
    BarType::from_str(&s).map_err(|e| anyhow::anyhow!("bar type {s}: {e}"))
}

/// Whether `tbk` matches a MarketStore glob pattern like `*/1Min/OHLCV`.
///
/// Patterns are `<sympart>/<tf>/<attr>` where any segment may be `*` (matches any).
/// This mirrors the server's glob so the adapter can route glob-streamed frames.
#[must_use]
pub fn tbk_matches_pattern(tbk: &str, pattern: &str) -> bool {
    let t: Vec<&str> = tbk.split('/').collect();
    let p: Vec<&str> = pattern.split('/').collect();
    if t.len() != p.len() {
        return false;
    }
    t.iter().zip(p.iter()).all(|(seg, pat)| *pat == "*" || pat == seg)
}

/// Builds the MarketStore TBK for an instrument's trade stream: `{symbol}/1Sec/TRADE`.
///
/// MarketStore stores tick-level trades in the `TRADE` attrgroup at a 1-second bucket
/// (see `docs/marketstore_v2_rewrite_plan.md` §3.1).
#[must_use]
pub fn trade_tbk(instrument_id: &nautilus_model::identifiers::InstrumentId) -> String {
    format!("{}/1Sec/TRADE", instrument_id.symbol)
}

/// Builds the MarketStore TBK for an instrument's quote stream: `{symbol}/1Sec/QUOTE`.
///
/// NBBO quotes live in the `QUOTE` attrgroup at a 1-second bucket. (The server writer's
/// historical `1Min/QUOTE` is a bug being fixed to `1Sec`; the adapter targets `1Sec`.)
#[must_use]
pub fn quote_tbk(instrument_id: &nautilus_model::identifiers::InstrumentId) -> String {
    format!("{}/1Sec/QUOTE", instrument_id.symbol)
}

/// Maps a Nautilus time-based [`BarAggregation`] to a MarketStore timeframe suffix.
fn aggregation_suffix(aggregation: BarAggregation) -> Result<&'static str> {
    Ok(match aggregation {
        BarAggregation::Second => "Sec",
        BarAggregation::Minute => "Min",
        BarAggregation::Hour => "H",
        BarAggregation::Day => "D",
        other => bail!("unsupported bar aggregation for MarketStore: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn minute_bar_to_tbk() {
        let bt = BarType::from_str("AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL").unwrap();
        assert_eq!(bar_type_to_tbk(&bt).unwrap(), "AAPL/1Min/OHLCV");
    }

    #[test]
    fn multi_step_and_units() {
        let cases = [
            ("SPY.MARKETSTORE-5-MINUTE-LAST-EXTERNAL", "SPY/5Min/OHLCV"),
            ("MSFT.MARKETSTORE-1-HOUR-LAST-EXTERNAL", "MSFT/1H/OHLCV"),
            ("QQQ.MARKETSTORE-1-DAY-LAST-EXTERNAL", "QQQ/1D/OHLCV"),
            ("X.MARKETSTORE-30-SECOND-LAST-EXTERNAL", "X/30Sec/OHLCV"),
        ];
        for (input, expected) in cases {
            let bt = BarType::from_str(input).unwrap();
            assert_eq!(bar_type_to_tbk(&bt).unwrap(), expected, "for {input}");
        }
    }

    #[test]
    fn rejects_non_time_aggregation() {
        let bt = BarType::from_str("AAPL.MARKETSTORE-100-TICK-LAST-EXTERNAL").unwrap();
        assert!(bar_type_to_tbk(&bt).is_err());
    }

    #[test]
    fn trade_and_quote_tbks() {
        use nautilus_model::identifiers::InstrumentId;
        let id = InstrumentId::from_str("AAPL.NASDAQ").unwrap();
        assert_eq!(trade_tbk(&id), "AAPL/1Sec/TRADE");
        assert_eq!(quote_tbk(&id), "AAPL/1Sec/QUOTE");
    }

    #[test]
    fn tbk_to_bar_type_roundtrips() {
        // reverse of bar_type_to_tbk, with a venue supplied.
        let bt = tbk_to_bar_type("AAPL/1Min/OHLCV", "MARKETSTORE").unwrap();
        assert_eq!(bar_type_to_tbk(&bt).unwrap(), "AAPL/1Min/OHLCV");
        assert_eq!(
            bt,
            BarType::from_str("AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL").unwrap()
        );
        // multi-step + units
        for (tbk, unit_check) in [
            ("SPY/5Min/OHLCV", "5-MINUTE"),
            ("MSFT/1H/OHLCV", "1-HOUR"),
            ("QQQ/1D/OHLCV", "1-DAY"),
        ] {
            let bt = tbk_to_bar_type(tbk, "NASDAQ").unwrap();
            assert!(bt.to_string().contains(unit_check), "{tbk}");
            assert_eq!(bar_type_to_tbk(&bt).unwrap(), tbk);
        }
        // non-bar attrgroup rejected
        assert!(tbk_to_bar_type("AAPL/1Sec/QUOTE", "NASDAQ").is_err());
    }

    #[test]
    fn glob_pattern_matching() {
        assert!(tbk_matches_pattern("AAPL/1Min/OHLCV", "*/1Min/OHLCV"));
        assert!(tbk_matches_pattern("ZZZ/1Min/OHLCV", "*/1Min/OHLCV"));
        assert!(!tbk_matches_pattern("AAPL/5Min/OHLCV", "*/1Min/OHLCV"));
        assert!(!tbk_matches_pattern("AAPL/1Sec/QUOTE", "*/1Min/OHLCV"));
        assert!(tbk_matches_pattern("AAPL/1Min/OHLCV", "*/*/*"));
    }
}
