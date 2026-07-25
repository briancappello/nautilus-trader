# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
MarketStore market data integration adapter.

The data client + factory are implemented in Rust and compiled INTO this wheel (the
live ``DataClient`` must share the engine's ABI — see the trading framework's
``docs/marketstore_v2_rewrite_plan.md`` §5 #14). This subpackage re-exports the
pyo3 factory/config classes so downstream code can simply import from
``nautilus_trader.adapters.marketstore``.

The historical bulk loaders (``load_bars``, ``load_quote_ticks``,
``load_trade_ticks``) are exposed here for the same reason. Being in-wheel, they
return the engine's own model objects, so ``engine.add_data(...)`` accepts the
result directly — an out-of-wheel loader would have to return primitive columns
and make Python rebuild every object.
"""

from nautilus_trader.core.nautilus_pyo3.marketstore import MarketStoreDataClientConfig
from nautilus_trader.core.nautilus_pyo3.marketstore import MarketStoreDataClientFactory
from nautilus_trader.core.nautilus_pyo3.marketstore import list_symbols
from nautilus_trader.core.nautilus_pyo3.marketstore import load_bars
from nautilus_trader.core.nautilus_pyo3.marketstore import load_quote_ticks
from nautilus_trader.core.nautilus_pyo3.marketstore import load_trade_ticks


__all__ = [
    "MarketStoreDataClientConfig",
    "MarketStoreDataClientFactory",
    "list_symbols",
    "load_bars",
    "load_quote_ticks",
    "load_trade_ticks",
]
