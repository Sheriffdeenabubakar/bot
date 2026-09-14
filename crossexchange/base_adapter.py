"""
crossexchange/base_adapter.py
================================
The shared contract every venue adapter (Binance, OKX, Bybit — and the
read-only Bitget view) implements. This is NOT a generic abstraction that
replaces the proven Bitget WebSocket lifecycle; Bitget's own code
(signal_analyzer.OrderFlowManager / OrderFlowAnalyzer) is untouched. This
module only defines the shape the CONSOLIDATION layer consumes, so three new
independent adapters can be built to the same contract while each still
speaks its exchange's native WebSocket protocol internally.

Every adapter:
  * owns its OWN WebSocket connection lifecycle (connect, subscribe,
    heartbeat, reconnect, resubscribe, gap-detect, resync, rebuild) —
    entirely independent of the other three venues.
  * exposes normalized, USD-notional book/trade data per canonical_symbol.
  * never lets a stale/invalid book keep contributing (see VenueWSState).
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .venue_state import VenueWSState


@dataclass
class NormalizedBookSnapshot:
    """One venue's CURRENT book for one canonical_symbol, already converted
    to USD notional and bucketed relative to that venue's own mid, in bps."""

    venue: str
    canonical_symbol: str
    ts: float
    venue_mid: float
    best_bid: float
    best_ask: float
    # bucket_index (int, bucket_size defined by normalization.DEFAULT_BUCKET_SIZE_BPS) -> usd notional
    bids_bps: Dict[int, float] = field(default_factory=dict)
    asks_bps: Dict[int, float] = field(default_factory=dict)
    book_valid: bool = False
    sequence_meta: dict = field(default_factory=dict)


@dataclass
class NormalizedTradeFlow:
    """One venue's trade flow for one canonical_symbol over a rolling window,
    already converted to USD notional."""

    venue: str
    canonical_symbol: str
    window_start: float
    window_end: float
    buy_notional_usd: float = 0.0
    sell_notional_usd: float = 0.0
    trade_count: int = 0
    book_valid: bool = True  # trade validity tracks WS health, not book validity per se

    @property
    def delta_usd(self) -> float:
        return self.buy_notional_usd - self.sell_notional_usd

    @property
    def total_notional_usd(self) -> float:
        return self.buy_notional_usd + self.sell_notional_usd


class VenueOrderFlowAdapter(abc.ABC):
    """Abstract contract for an independent, venue-owned WebSocket order-flow
    adapter. Binance/OKX/Bybit adapters subclass this. It intentionally does
    NOT wrap or alter signal_analyzer.OrderFlowManager (Bitget) — a separate
    thin read-only view class does that instead (see shadow_runner.py)."""

    venue_name: str = "unknown"

    def __init__(self):
        self._states: Dict[str, VenueWSState] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Lifecycle — implemented per-venue, respecting its native protocol.
    # ------------------------------------------------------------------
    @abc.abstractmethod
    async def start(self, canonical_symbols: List[str]) -> None:
        """Begin this venue's independent WS lifecycle for the given symbols."""
        raise NotImplementedError

    @abc.abstractmethod
    async def stop(self) -> None:
        """Tear down this venue's connection(s). Must never be called as a
        side effect of another venue's failure."""
        raise NotImplementedError

    @abc.abstractmethod
    async def ensure_symbols(self, canonical_symbols: List[str]) -> None:
        """Add/refresh subscriptions for additional symbols at runtime."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Read-only accessors consumed by the consolidation layer.
    # ------------------------------------------------------------------
    def get_state(self, canonical_symbol: str) -> Optional[VenueWSState]:
        return self._states.get(canonical_symbol.upper())

    def all_states(self) -> Dict[str, VenueWSState]:
        return dict(self._states)

    @abc.abstractmethod
    def get_book_snapshot(self, canonical_symbol: str) -> Optional[NormalizedBookSnapshot]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_trade_flow(self, canonical_symbol: str, *, lookback_seconds: float = 60.0) -> Optional[NormalizedTradeFlow]:
        raise NotImplementedError

    # ------------------------------------------------------------------
    def _state_for(self, canonical_symbol: str) -> VenueWSState:
        key = canonical_symbol.upper()
        state = self._states.get(key)
        if state is None:
            state = VenueWSState(venue=self.venue_name, canonical_symbol=key)
            self._states[key] = state
        return state

    def health_report(self) -> dict:
        return {
            "venue": self.venue_name,
            "symbols_tracked": len(self._states),
            "eligible_count": sum(1 for s in self._states.values() if s.is_eligible_for_consolidation()),
            "states": {sym: st.to_dict() for sym, st in self._states.items()},
            "generated_at": time.time(),
        }
