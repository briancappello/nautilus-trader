//! Integration tests against a live MarketStore gRPC endpoint (port 5995).
//!
//! Ignored by default; run with a MarketStore running:
//!   cargo test -p nautilus-marketstore --test integration_grpc -- --ignored
//!
//! Uses AAPL/1Min — a liquid symbol confirmed present in the dev MarketStore.

use std::str::FromStr;

use nautilus_marketstore::MarketStoreGrpcClient;
use nautilus_marketstore::common::DEFAULT_GRPC_ENDPOINT;
use nautilus_marketstore::load_bars;
use nautilus_model::data::bar::BarType;

fn aapl_1min() -> BarType {
    BarType::from_str("AAPL.MARKETSTORE-1-MINUTE-LAST-EXTERNAL").unwrap()
}

#[tokio::test]
#[ignore = "requires a running MarketStore on :5995"]
async fn loads_aapl_bars_in_ascending_order() {
    let client = MarketStoreGrpcClient::connect(DEFAULT_GRPC_ENDPOINT)
        .await
        .expect("connect to MarketStore");

    let bars = load_bars(&client, aapl_1min(), None, None, 500, 2, 0)
        .await
        .expect("load bars");

    assert!(!bars.is_empty(), "expected AAPL 1Min bars");
    assert!(bars.len() <= 500, "limit should cap row count");

    // OHLC sanity + monotonic non-decreasing timestamps (replay precondition).
    for w in bars.windows(2) {
        assert!(w[0].ts_init <= w[1].ts_init, "bars must be time-ordered");
    }
    for b in &bars {
        assert!(b.high >= b.low, "high >= low");
        assert!(b.open.as_f64() > 0.0 && b.close.as_f64() > 0.0);
    }

    let first = &bars[0];
    println!(
        "AAPL first bar: O={} H={} L={} C={} V={} ts={}",
        first.open, first.high, first.low, first.close, first.volume, first.ts_event,
    );
}

#[tokio::test]
#[ignore = "requires a running MarketStore on :5995"]
async fn limit_from_start_is_deterministic_across_calls() {
    let client = MarketStoreGrpcClient::connect(DEFAULT_GRPC_ENDPOINT)
        .await
        .expect("connect");

    let a = load_bars(&client, aapl_1min(), None, None, 100, 2, 0)
        .await
        .unwrap();
    let b = load_bars(&client, aapl_1min(), None, None, 100, 2, 0)
        .await
        .unwrap();

    assert_eq!(a.len(), b.len());
    assert_eq!(a, b, "identical queries must return identical bars");
}
