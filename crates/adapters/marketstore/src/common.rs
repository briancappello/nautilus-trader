//! Constants and shared config for the MarketStore adapter.

/// NautilusTrader client identifier for this adapter.
pub const MARKETSTORE_CLIENT_ID: &str = "MARKETSTORE";

/// Canonical adapter name (factory registration key / default client name).
pub const MARKETSTORE: &str = "MARKETSTORE";

/// Default synthetic venue for MarketStore-sourced instruments (venue-less source).
pub const DEFAULT_VENUE: &str = "MARKETSTORE";

/// Default gRPC endpoint for a local MarketStore.
///
/// MarketStore serves gRPC on port 5995 (HTTP/msgpack-rpc is on 5993).
pub const DEFAULT_GRPC_ENDPOINT: &str = "http://127.0.0.1:5995";

/// Default WebSocket streaming endpoint for a local MarketStore (`/ws` on 5993).
pub const DEFAULT_WS_ENDPOINT: &str = "ws://127.0.0.1:5993/ws";
