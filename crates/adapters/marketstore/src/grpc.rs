//! Tonic gRPC client wrapper around the generated `MarketstoreClient`.
//!
//! Used for historical / backtest bulk loads via the unary `Query` RPC, which
//! returns column-oriented `NumpyMultiDataset` results (decoded in [`crate::decode`]).
//! MarketStore serves gRPC on port 5995.

use anyhow::{Context, Result};
use nautilus_core::UnixNanos;
use tonic::transport::Channel;

use crate::proto::marketstore_client::MarketstoreClient;
use crate::proto::{
    ListSymbolsRequest, MultiQueryRequest, NumpyMultiDataset, QueryRequest,
    list_symbols_request::Format,
};

const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Thin async wrapper over the generated MarketStore gRPC client.
#[derive(Clone)]
pub struct MarketStoreGrpcClient {
    inner: MarketstoreClient<Channel>,
}

impl MarketStoreGrpcClient {
    /// Connects to a MarketStore gRPC endpoint (e.g. `http://127.0.0.1:5995`).
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint is malformed or the connection fails.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())
            .with_context(|| format!("invalid MarketStore gRPC endpoint: {endpoint}"))?
            // MarketStore can return very large columnar payloads.
            .connect()
            .await
            .with_context(|| format!("failed to connect to MarketStore at {endpoint}"))?;
        Ok(Self {
            inner: MarketstoreClient::new(channel)
                .max_decoding_message_size(usize::MAX),
        })
    }

    /// Runs a single `Query` for the given TBK `destination` and time range.
    ///
    /// `start`/`end` are inclusive bounds; pass `None` for an open bound. `limit`
    /// caps the returned row count (`0` = unlimited).
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the response carries no result.
    pub async fn query_bars(
        &self,
        destination: &str,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        limit: i32,
    ) -> Result<NumpyMultiDataset> {
        let (epoch_start, epoch_start_nanos) = split_epoch(start);
        let (epoch_end, epoch_end_nanos) = split_epoch(end);

        let request = MultiQueryRequest {
            requests: vec![QueryRequest {
                destination: destination.to_string(),
                epoch_start,
                epoch_start_nanos,
                epoch_end,
                epoch_end_nanos,
                limit_record_count: limit,
                ..Default::default()
            }],
        };

        let mut client = self.inner.clone();
        let response = client
            .query(request)
            .await
            .with_context(|| format!("MarketStore Query failed for {destination}"))?
            .into_inner();

        let result = response
            .responses
            .into_iter()
            .next()
            .and_then(|r| r.result)
            .with_context(|| format!("empty MarketStore response for {destination}"))?;

        Ok(result)
    }

    /// Lists all symbols known to MarketStore (the `ListSymbols` RPC).
    ///
    /// Returns bare symbol names (e.g. `["AAPL", "AMZN", ...]`). `timeframe`
    /// optionally filters to symbols that have data for that bucket (e.g. `"1Min"`);
    /// pass `None`/empty for no filter. This is the universe-discovery entry point.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn list_symbols(&self, timeframe: Option<&str>) -> Result<Vec<String>> {
        let request = ListSymbolsRequest {
            format: Format::Symbol as i32,
            timeframe: timeframe.unwrap_or_default().to_string(),
            date: String::new(),
        };

        let mut client = self.inner.clone();
        let response = client
            .list_symbols(request)
            .await
            .context("MarketStore ListSymbols failed")?
            .into_inner();

        Ok(response.results)
    }
}

/// Splits a `UnixNanos` into `(epoch_seconds, nanos_remainder)` for the proto.
/// Returns `(0, 0)` for `None` (an open bound MarketStore interprets as unbounded).
fn split_epoch(ts: Option<UnixNanos>) -> (i64, i64) {
    match ts {
        Some(ts) => {
            let nanos = ts.as_u64() as i64;
            (nanos / NANOS_PER_SEC, nanos % NANOS_PER_SEC)
        }
        None => (0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_epoch_decomposes_nanos() {
        let ts = UnixNanos::from(1_781_827_020_123_456_789u64);
        assert_eq!(split_epoch(Some(ts)), (1_781_827_020, 123_456_789));
        assert_eq!(split_epoch(None), (0, 0));
    }
}
