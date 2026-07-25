//! HTTP (REST) layer for the Alpaca Trading API v2.
//!
//! - [`client`]: the [`AlpacaHttpClient`] transport (auth, endpoints).
//! - [`models`]: serde wire structs for account / positions / orders.
//! - [`parse`]: Nautilus ⇄ Alpaca enum translation and account-state parsing.
//! - [`error`]: the HTTP error type.

pub mod client;
pub mod error;
pub mod models;
pub mod parse;

pub use client::AlpacaHttpClient;
pub use error::{Error, Result};
