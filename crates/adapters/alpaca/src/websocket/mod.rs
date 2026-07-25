//! Trade-updates WebSocket layer for the Alpaca adapter (M5.2).
//!
//! - [`client`]: the [`AlpacaWebSocketClient`] (connect/auth/subscribe + parse task).
//! - [`models`]: serde wire structs for the `trade_updates` envelope and payload.

pub mod client;
pub mod models;

pub use client::{AlpacaWebSocketClient, AlpacaWsMessage};
