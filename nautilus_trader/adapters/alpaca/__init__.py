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
Alpaca execution integration adapter.

The execution client + factory are implemented in Rust and compiled INTO this wheel (the
live ``ExecutionClient`` must share the engine's ABI — see the trading framework's
``docs/milestone-5-alpaca-execution.md`` §0). This subpackage re-exports the pyo3
factory/config classes so downstream code can simply import from
``nautilus_trader.adapters.alpaca``.
"""

from nautilus_trader.core.nautilus_pyo3.alpaca import AlpacaExecClientConfig
from nautilus_trader.core.nautilus_pyo3.alpaca import AlpacaExecutionClientFactory


__all__ = [
    "AlpacaExecClientConfig",
    "AlpacaExecutionClientFactory",
]
