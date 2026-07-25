//! Error types for the Alpaca HTTP client.

use bytes::Bytes;

/// A result alias for Alpaca HTTP operations.
pub type Result<T> = std::result::Result<T, Error>;

/// An error from the Alpaca Trading API HTTP layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No API credentials were configured.
    #[error("Alpaca credentials not configured (set ALPACA_API_KEY_*/ALPACA_API_SECRET_*)")]
    MissingCredentials,

    /// The transport (network) failed.
    #[error("HTTP transport error: {0}")]
    Transport(String),

    /// The Alpaca API returned a non-2xx status with a body.
    #[error("Alpaca API error (status {status}): {body}")]
    Api {
        /// The HTTP status code.
        status: u16,
        /// The (possibly JSON) error body, as text.
        body: String,
    },

    /// A JSON (de)serialization error.
    #[error("JSON error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A value from the wire could not be parsed into a model type.
    #[error("parse error: {0}")]
    Parse(String),
}

impl Error {
    /// Builds an [`Error::Api`] from a status code and raw body bytes.
    #[must_use]
    pub fn from_status(status: u16, body: &Bytes) -> Self {
        Self::Api {
            status,
            body: String::from_utf8_lossy(body).into_owned(),
        }
    }

    /// Wraps a transport-layer error.
    #[must_use]
    pub fn transport(msg: impl Into<String>) -> Self {
        Self::Transport(msg.into())
    }

    /// `true` if this looks like a transient error worth retrying (idempotent calls only).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Api { status, .. } => *status == 429 || *status >= 500,
            _ => false,
        }
    }
}
