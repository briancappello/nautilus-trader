//! Factory for creating live Alpaca execution clients (the `LiveNode` entry point).
//!
//! Mirrors the coinbase pattern: the factory + config are `#[pyclass]`es exposed to Python;
//! `LiveNode.builder().add_exec_client(name, factory, config)` downcasts the config and
//! constructs the `ExecutionClient`. The pyo3 registry routes the factory by its `name()`
//! (= [`ALPACA`]); see `src/python/mod.rs`.

use std::any::Any;

use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    common::{ALPACA, ALPACA_VENUE},
    config::AlpacaExecClientConfig,
    execution::AlpacaExecutionClient,
};

/// Factory for creating Alpaca execution clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.alpaca",
        from_py_object
    )
)]
pub struct AlpacaExecutionClientFactory {
    trader_id: TraderId,
    account_id: AccountId,
}

impl AlpacaExecutionClientFactory {
    /// Creates a new [`AlpacaExecutionClientFactory`].
    #[must_use]
    pub const fn new(trader_id: TraderId, account_id: AccountId) -> Self {
        Self {
            trader_id,
            account_id,
        }
    }
}

impl ExecutionClientFactory for AlpacaExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let alpaca_config = config
            .as_any()
            .downcast_ref::<AlpacaExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AlpacaExecutionClientFactory. \
                     Expected AlpacaExecClientConfig, was {config:?}"
                )
            })?
            .clone();

        // US-equity cash account; Alpaca exposes no hedge mode, so OMS is Netting.
        let core = ExecutionClientCore::new(
            self.trader_id,
            ClientId::from(name),
            *ALPACA_VENUE,
            OmsType::Netting,
            self.account_id,
            AccountType::Cash,
            None,
            cache,
        );

        let client = AlpacaExecutionClient::new(core, alpaca_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ALPACA
    }

    fn config_type(&self) -> &'static str {
        "AlpacaExecClientConfig"
    }
}

// Silence unused `Any` on non-python builds (used via the ClientConfig downcast path).
const _: fn() = || {
    fn _assert_any<T: Any>() {}
    _assert_any::<AlpacaExecClientConfig>();
};
