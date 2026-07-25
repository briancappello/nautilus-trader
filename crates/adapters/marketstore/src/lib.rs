//! MarketStore data adapter for NautilusTrader v2 (in-wheel build).
//!
//! The live `DataClient` must compile INTO the nautilus wheel (cross-cdylib boundary —
//! see `docs/marketstore_v2_rewrite_plan.md` §5 #14), so this crate lives in the fork's
//! workspace and compiles against the fork's own nautilus crates.
//!
//! **Self-contained.** Everything this crate needs — the proto and the engine-agnostic
//! modules (`common`, `decode`, `grpc`, `symbology`, `loader`) — is vendored here. It
//! builds from a bare clone of this repository alone and makes no assumption about any
//! sibling checkout on the filesystem. An earlier revision `#[path]`-included those
//! modules from a private sibling repo, which meant the crate could not build on a
//! machine that lacked it, could not be upstreamed, and broke in every git worktree.
//!
//! Downstream consumers that need the same decode/gRPC logic should depend on this
//! crate rather than keeping a parallel copy.

/// Generated tonic client + message types from the vendored `proto/marketstore.proto`.
///
/// The proto package is `proto`; tonic-build emits `$OUT_DIR/proto.rs`.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

// --- Engine-agnostic modules (transport + decode, no live-client deps) ------
pub mod common;
pub mod decode;
pub mod grpc;
pub mod loader;
pub mod symbology;

pub use decode::RawBars;
pub use grpc::MarketStoreGrpcClient;
pub use loader::{load_bars, load_raw_bars};

// --- In-wheel-only modules (live DataClient glue) ---------------------------
pub mod config;
pub mod instruments;

#[cfg(feature = "live")]
pub mod data;
#[cfg(feature = "live")]
pub mod factories;
#[cfg(feature = "live")]
pub mod rpc;
#[cfg(feature = "live")]
pub mod ws;

pub use config::{InstrumentSpec, MarketStoreDataClientConfig};
#[cfg(feature = "live")]
pub use data::MarketStoreDataClient;
#[cfg(feature = "live")]
pub use factories::MarketStoreDataClientFactory;

#[cfg(feature = "python")]
pub mod python;
