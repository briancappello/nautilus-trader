//! M0 smoke test: the generated proto compiles and request types construct.

use nautilus_marketstore::proto;

#[test]
fn multi_query_request_constructs() {
    let req = proto::MultiQueryRequest {
        requests: vec![proto::QueryRequest {
            destination: "AAPL/1Min/OHLCV".to_string(),
            ..Default::default()
        }],
    };
    assert_eq!(req.requests.len(), 1);
    assert_eq!(req.requests[0].destination, "AAPL/1Min/OHLCV");
}
