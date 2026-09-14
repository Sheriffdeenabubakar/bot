"""
crossexchange/venues/bitget_view.py
======================================
A thin, READ-ONLY adapter that exposes the ALREADY-RUNNING, proven Bitget
OrderFlowManager/OrderFlowAnalyzer (signal_analyzer.py) through the same
VenueOrderFlowAdapter contract the new Binance/OKX/Bybit adapters implement.

THIS FILE DOES NOT MODIFY, WRAP, OR REPLACE ANY BITGET WEBSOCKET LOGIC.
It never calls anything Bitget doesn't already expose publicly
(get_orderflow_manager(), manager.ensure_symbols(), and simple attribute
reads off the analyzer instances the manager already owns). If Bitget's
manager isn't enabled/running, this view degrades to "no data" for every
symbol rather than starting a second Bitget connection.

EAGER STATE MIRRORING (fixes the 'venue book not currently valid/eligible'
exclusion that previously kept Bitget out of the consolidated book):
  The consolidation layer checks VenueWSState.is_eligible_for_consolidation()
  BEFORE it ever calls get_book_snapshot()/get_trade_flow(). A purely lazy
  view that only touches its state inside those accessors can therefore
  NEVER transition its state to eligible on its own (chicken-and-egg).
  get_state() now refreshes the mirror cheaply from the live manager's
  public attributes (symbol_meta / groups / analyzer book buffers) on every
  read: no network calls, no second connection, fully fail-safe.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

from ..base_adapter import NormalizedBookSnapshot, NormalizedTradeFlow, VenueOrderFlowAdapter
from ..canonical_symbols import from_canonical, to_canonical
from .. import normalization as norm
from .. import observability as obs

logger = logging.getLogger(__name__)


class BitgetOrderFlowView(VenueOrderFlowAdapter):
    """Read-only view over the existing, live Bitget OrderFlowManager."""

    venue_name = "bitget"

    def __init__(self):
        super().__init__()
        self._manager = None
        self._started = False

    # ------------------------------------------------------------------
    def _get_manager(self):
        if self._manager is not None:
            return self._manager
        try:
            # Imported lazily so this module never forces signal_analyzer to
            # load before it's ready, and never creates a second manager —
            # get_orderflow_manager() is a process-wide singleton accessor
            # already used by the existing, proven code path.
            from signal_analyzer import get_orderflow_manager
            self._manager = get_orderflow_manager()
        except Exception as exc:
            logger.warning("BitgetOrderFlowView: could not reach the live OrderFlowManager: %s", exc)
            self._manager = None
        return self._manager

    async def start(self, canonical_symbols: List[str]) -> None:
        manager = self._get_manager()
        for sym in canonical_symbols:
            self._state_for(sym)  # pre-create so health reports show it immediately
        if manager is None:
            for sym in canonical_symbols:
                self._state_for(sym).mark_disconnected("bitget orderflow manager unavailable")
            return
        self._started = True
        # Piggyback on the SAME manager/universe the existing Bitget path
        # already subscribes — this never opens a second Bitget connection,
        # it just makes sure the symbols we want a shadow view for are among
        # the ones the proven manager is already tracking.
        try:
            native_symbols = [from_canonical("bitget", s) for s in canonical_symbols]
            native_symbols = [s for s in native_symbols if s]
            if native_symbols:
                await manager.ensure_symbols(native_symbols)
        except Exception as exc:
            obs.log_venue_event("bitget", "*", "ensure_symbols_failed", error=str(exc))
        obs.log_venue_event("bitget", "*", "view_started", symbols=len(canonical_symbols))

    async def stop(self) -> None:
        # Never touch the live manager's lifecycle — it is owned by the
        # existing production path and must keep running regardless of the
        # shadow layer's own start/stop cycle.
        self._started = False
        obs.log_venue_event("bitget", "*", "view_stopped")

    async def ensure_symbols(self, canonical_symbols: List[str]) -> None:
        manager = self._get_manager()
        if manager is None:
            return
        native_symbols = [from_canonical("bitget", s) for s in canonical_symbols]
        native_symbols = [s for s in native_symbols if s]
        if native_symbols:
            await manager.ensure_symbols(native_symbols)

    # ------------------------------------------------------------------
    # Eager state mirroring — THE eligibility fix
    # ------------------------------------------------------------------
    def get_state(self, canonical_symbol: str):
        """Return the venue state AFTER refreshing it from the live manager.

        Called by the consolidation layer before eligibility is checked, so
        the mirror must be brought up to date here — not lazily inside
        get_book_snapshot() (which is only invoked when already eligible).
        Fail-safe: any mirror failure returns the state as-is.
        """
        state = self._state_for(canonical_symbol)
        try:
            self._mirror_state(canonical_symbol, state)
        except Exception as exc:
            logger.debug("BitgetOrderFlowView mirror failed (non-fatal): %s", exc)
        return state

    def _mirror_state(self, canonical_symbol: str, state) -> None:
        """Mirror the live Bitget manager's public health into `state`.

        Reads only public attributes the existing code already maintains:
          manager.symbol_meta[symbol] : {subscribed, group_id,
                                         last_message_ms, last_book_ms,
                                         last_trade_ms, ...}
          manager.groups[gid]["connected"]
          analyzer.bids / analyzer.asks / analyzer.needs_resync
        """
        canonical_symbol = (canonical_symbol or "").upper()
        analyzer = self._get_analyzer(canonical_symbol)
        if analyzer is None:
            # Manager missing or symbol not tracked: leave the state as
            # start()/ensure_symbols() marked it (disconnected) — never fake
            # eligibility for a symbol the proven path isn't streaming.
            return

        manager = self._manager  # set by _get_analyzer's manager lookup
        native_symbol = from_canonical("bitget", canonical_symbol)

        meta: Dict = {}
        connected = False
        try:
            meta = (getattr(manager, "symbol_meta", None) or {}).get(native_symbol) or {}
            gid = meta.get("group_id")
            if gid is not None:
                group = (getattr(manager, "groups", None) or {}).get(gid) or {}
                connected = bool(group.get("connected"))
        except Exception:
            meta = {}

        subscribed = bool(meta.get("subscribed")) if meta else False

        # Most recent message timestamp across book/trade/message channels.
        candidates = []
        for key in ("last_message_ms", "last_book_ms", "last_trade_ms"):
            val = meta.get(key) if isinstance(meta, dict) else None
            try:
                if val:
                    candidates.append(float(val) / 1000.0)
            except (TypeError, ValueError):
                pass
        last_ts = max(candidates) if candidates else 0.0

        needs_resync = bool(getattr(analyzer, "needs_resync", False))
        bids = getattr(analyzer, "bids", None)
        asks = getattr(analyzer, "asks", None)
        book_present = bool(bids) and bool(asks)

        # --- transport / subscription flags ------------------------------
        if connected and subscribed:
            if not state.ws_connected:
                state.mark_connected()
            else:
                state.ws_transport_healthy = True
                state.reconnecting = False
            if not state.ws_subscribed:
                state.ws_subscribed = True
                state.resubscription_required = False

        # --- freshness ----------------------------------------------------
        # Always overwrite with the LIVE manager's true last-message time so a
        # dead/stalled Bitget stream is excluded by the staleness gate even on
        # the very first mirror after connect (mark_connected stamps 'now').
        if last_ts:
            state.last_message_ts = last_ts
        elif not state.last_message_ts:
            state.last_message_ts = time.time()

        # --- book validity -------------------------------------------------
        if book_present and not needs_resync and connected and subscribed:
            if not (state.book_initialized and state.book_valid):
                state.mark_book_ready()
                obs.log_venue_event("bitget", canonical_symbol, "book_valid",
                                   basis="live_manager_mirror")
        elif needs_resync and state.book_valid:
            # Mirrors Bitget's own book-invalidation-on-gap behavior: once
            # needs_resync is set, the live book is stale until the existing
            # manager completes its own resubscribe/resync.
            state.mark_gap("bitget analyzer requested resync")

    # ------------------------------------------------------------------
    def _get_analyzer(self, canonical_symbol: str):
        manager = self._get_manager()
        if manager is None:
            return None
        native_symbol = from_canonical("bitget", canonical_symbol)
        if not native_symbol:
            return None
        return manager.analyzers.get(native_symbol)

    def get_book_snapshot(self, canonical_symbol: str) -> Optional[NormalizedBookSnapshot]:
        state = self._state_for(canonical_symbol)
        # Refresh the mirror so eligibility below reflects the live manager.
        try:
            self._mirror_state(canonical_symbol, state)
        except Exception:
            pass

        analyzer = self._get_analyzer(canonical_symbol)
        if analyzer is None:
            state.mark_disconnected("no bitget analyzer for symbol")
            return None

        needs_resync = bool(getattr(analyzer, "needs_resync", False))
        bids = getattr(analyzer, "bids", None)
        asks = getattr(analyzer, "asks", None)
        if not bids or not asks:
            state.book_initialized = False
            state.book_valid = False
            return None

        try:
            best_bid = float(next(iter(bids.keys())))
            best_ask = float(next(iter(asks.keys())))
        except (StopIteration, TypeError, ValueError):
            state.book_valid = False
            return None

        mid = norm.venue_mid(best_bid, best_ask)
        if mid is None:
            state.book_valid = False
            return None

        state.ws_connected = True
        state.ws_subscribed = True
        state.ws_transport_healthy = True
        state.book_initialized = True
        if not state.last_message_ts:
            state.last_message_ts = time.time()

        if needs_resync:
            # Mirrors Bitget's own book-invalidation-on-gap behavior:
            # once needs_resync is set, the live book is stale until the
            # existing manager completes its own resubscribe/resync.
            state.mark_gap("bitget analyzer requested resync")
            return None

        state.mark_book_ready()

        depth_levels = 50
        bid_levels = list(bids.items())[:depth_levels]
        ask_levels = list(asks.items())[:depth_levels]
        bids_bps = norm.build_bps_depth(bid_levels, mid, multiplier=1.0)
        asks_bps = norm.build_bps_depth(ask_levels, mid, multiplier=1.0)

        return NormalizedBookSnapshot(
            venue=self.venue_name,
            canonical_symbol=canonical_symbol.upper(),
            ts=time.time(),
            venue_mid=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_bps=bids_bps,
            asks_bps=asks_bps,
            book_valid=True,
            sequence_meta={"seq": getattr(analyzer, "seq", None), "ws_diag": dict(getattr(analyzer, "ws_diag", {}) or {})},
        )

    def get_trade_flow(self, canonical_symbol: str, *, lookback_seconds: float = 60.0) -> Optional[NormalizedTradeFlow]:
        analyzer = self._get_analyzer(canonical_symbol)
        if analyzer is None:
            return None
        trade_history = getattr(analyzer, "trade_history", None)
        if not trade_history:
            return None

        now = time.time()
        cutoff_ms = (now - lookback_seconds) * 1000.0
        buy_notional = 0.0
        sell_notional = 0.0
        count = 0
        # trade_history entries are (ts_ms, price, size, side); Bitget already
        # stores notional-equivalent (price*size) semantics — multiplier=1.0.
        for ts_ms, price, size, side in list(trade_history):
            if ts_ms is not None and ts_ms < cutoff_ms:
                continue
            usd = norm.notional_usd(price, size, 1.0)
            if side == "buy":
                buy_notional += usd
            elif side == "sell":
                sell_notional += usd
            else:
                continue
            count += 1

        return NormalizedTradeFlow(
            venue=self.venue_name,
            canonical_symbol=canonical_symbol.upper(),
            window_start=now - lookback_seconds,
            window_end=now,
            buy_notional_usd=buy_notional,
            sell_notional_usd=sell_notional,
            trade_count=count,
            book_valid=not bool(getattr(analyzer, "needs_resync", False)),
        )
