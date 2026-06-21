//! MarketStore data adapter for NautilusTrader v2 (in-wheel build).
//!
//! This crate is the **in-wheel** counterpart of the standalone `nautilus-marketstore`
//! crate in the trading repo (`../trading/crates/marketstore`). The live `DataClient`
//! must compile INTO the nautilus wheel (cross-cdylib boundary — see
//! `docs/marketstore_v2_rewrite_plan.md` §5 #14), so this crate is vendored into the
//! fork's workspace and compiles against the fork's own nautilus crates (HEAD,
//! high-precision ON).
//!
//! **Single source of truth.** The engine-agnostic modules (`common`, `decode`, `grpc`,
//! `symbology`, `loader`) and the proto are NOT duplicated — they are `#[path]`-included
//! from the trading repo so there is exactly one copy of the decode/gRPC logic. They use
//! only stable nautilus + tonic APIs, so the same source compiles in both build contexts
//! (standalone: crates.io 0.58 + tonic 0.14; in-wheel: fork HEAD + tonic 0.13).
//!
//! The **in-wheel-only** modules (`config`, `instruments`, `factories`, `data`, `python`)
//! live here — they are the live `impl DataClient` glue that only makes sense inside the
//! wheel.

/// Generated tonic client + message types from the trading repo's vendored proto.
///
/// The proto package is `proto`; tonic-build emits `$OUT_DIR/proto.rs`.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

// --- Engine-agnostic modules (shared source from the trading repo) ----------
#[path = "../../../../../trading/crates/marketstore/src/common.rs"]
pub mod common;
#[path = "../../../../../trading/crates/marketstore/src/decode.rs"]
pub mod decode;
#[path = "../../../../../trading/crates/marketstore/src/grpc.rs"]
pub mod grpc;
#[path = "../../../../../trading/crates/marketstore/src/symbology.rs"]
pub mod symbology;
#[path = "../../../../../trading/crates/marketstore/src/loader.rs"]
pub mod loader;

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

pub use config::{InstrumentSpec, MarketStoreDataClientConfig};
#[cfg(feature = "live")]
pub use data::MarketStoreDataClient;
#[cfg(feature = "live")]
pub use factories::MarketStoreDataClientFactory;

#[cfg(feature = "python")]
pub mod python;
