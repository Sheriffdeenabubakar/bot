"""
crossexchange/venues/binance_adapter.py
=========================================
Binance USDⓈ-M Futures WebSocket order-flow adapter for the cross-exchange layer.

DEPTH BASIS — three selectable modes (cx_binance_depth_mode):

  "auto" (default)
      Full-depth book via the OFFICIAL Binance sync algorithm:
        1. subscribe to <symbol>@depth@100ms (diff stream) and buffer events
        2. fetch GET /fapi/v1/depth?symbol=<s>&limit=<N> (REST snapshot)
        3. drop buffered events with u <= lastUpdateId; the first applied
           event must satisfy U <= lastUpdateId <= u; afterwards every event
           must chain: pu == previous u (futures semantics). A broken chain
           invalidates the book and re-runs the snapshot sync.
      If REST is unavailable in the runtime region (HTTP 451 legal block /
      unreachable — e.g. a VPN exiting in a restricted country), the adapter
      detects it after a few consecutive failures and AUTOMATICALLY switches
      to the pure-WS partial-depth basis so the venue keeps contributing.

  "diff"
      REST + diff only. No fallback: if REST is blocked the venue stays
      unsynced (explicit user choice).

  "partial"
      Pure-WS top-20 book: <symbol>@depth20@100ms pushes the COMPLETE top-20
      book every 100ms. No REST, no sequence reconciliation — immune to
      HTTP 451 geo-blocking.

TRADES — <symbol>@aggTrade with an automatic raw-<symbol>@trade fallback:
  Some regions that get HTTP 451 on REST also receive zero @aggTrade frames
  (books and raw trades still flow). Each shard starts on @aggTrade (lower
  volume) and, if zero trade frames arrive within 20s of connect while books
  are flowing, switches to the raw @trade stream (identical p/q/m/T
  semantics).
  `m`: True  => buyer was the maker => taker was the SELLER (SELL trade).
       False => buyer was the taker (BUY trade).

CONNECTION SHARDING (fixes the "keepalive ping timeout" churn):
  Binance allows at most 200 streams per combined connection. Streams are
  partitioned across multiple connections (default 100 streams each,
  configurable), each with an independent reconnect loop (backoff + jitter)
  and a receive watchdog so a silently-dead socket can never wedge a shard.

Verified live payloads (combined-stream envelope {"stream": ..., "data": ...}):
  @depth20@100ms -> {"e":"depthUpdate","s":"BTCUSDT","b":[[px,qty],...],"a":[...]}
  @depth@100ms   -> {"e":"depthUpdate","s":"BTCUSDT","U":...,"u":...,"pu":...,
                     "b":[[px,qty],...],"a":[...]}   (diffs, qty 0 => remove)
  @aggTrade/@trade -> {"e":"aggTrade"/"trade","s":"BTCUSDT","p":px,"q":qty,
                       "T":ts_ms,"m":makerFlag}
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import deque
from typing import Dict, List, Optional, Set

import websockets

try:
    from ..venue_state import VenueWSState
    from ..base_adapter import VenueOrderFlowAdapter, NormalizedBookSnapshot, NormalizedTradeFlow
    from .. import normalization as norm
    from .. import canonical_symbols as csym
    from .. import observability as obs
    from ..cx_config import CROSSEXCHANGE_CONFIG
except (ImportError, ValueError):
    from crossexchange.venue_state import VenueWSState
    from crossexchange.base_adapter import VenueOrderFlowAdapter, NormalizedBookSnapshot, NormalizedTradeFlow
    from crossexchange import normalization as norm
    from crossexchange import canonical_symbols as csym
    from crossexchange import observability as obs
    from crossexchange.cx_config import CROSSEXCHANGE_CONFIG


# Binance hard limit is 200 streams per combined connection; we stay well under it.
DEFAULT_STREAMS_PER_CONNECTION = 100
# Partial-depth levels Binance offers: 5, 10, 20. 20 is the deepest partial feed.
PARTIAL_DEPTH_LEVELS = 20
# Valid futures /fapi/v1/depth limits for snapping the configured value.
VALID_REST_DEPTH_LIMITS = (5, 10, 20, 50, 100, 500, 1000)
# Consecutive REST failures (in auto mode) before switching to the partial basis.
REST_BLOCK_SWITCH_THRESHOLD = 5
# Global pacing for REST snapshot calls (weight-based; futures cap 2400/min).
REST_MIN_INTERVAL_SECONDS = 0.55
REST_TIMEOUT_SECONDS = 10.0
# Per-symbol resync backoff: 2s -> 60s cap, so REST failures never hot-loop.
RESYNC_INITIAL_BACKOFF = 2.0
RESYNC_MAX_BACKOFF = 60.0
RESYNC_MAX_ATTEMPTS = 5


def _snap_depth_limit(requested: int) -> int:
    """Snap a configured REST depth limit to the nearest valid Binance value."""
    try:
        req = int(requested)
    except (TypeError, ValueError):
        req = 100
    for lim in VALID_REST_DEPTH_LIMITS:
        if req <= lim:
            return lim
    return VALID_REST_DEPTH_LIMITS[-1]


class BinanceOrderFlowAdapter(VenueOrderFlowAdapter):
    """
    Binance USDⓈ-M Futures Order-Flow Adapter (sharded connections,
    full-depth diff sync with geo-block fallback).

    Public contract is unchanged: start / stop / ensure_symbols /
    get_state / get_book_snapshot / get_trade_flow.
    """

    venue_name: str = "binance"

    def __init__(self):
        super().__init__()
        self._tracked_canonical_symbols: Set[str] = set()
        self._symbol_data: Dict[str, dict] = {}
        self._shard_tasks: List[asyncio.Task] = []
        self._sockets: Set[object] = set()
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()

        # Depth-basis mode ("auto" | "diff" | "partial")
        self._depth_mode = str(CROSSEXCHANGE_CONFIG.get("cx_binance_depth_mode", "auto") or "auto").lower()
        if self._depth_mode not in ("auto", "diff", "partial"):
            self._depth_mode = "auto"
        # In auto mode: starts on the full-depth diff basis; flipped to True
        # after repeated REST failures, which rebuilds shards on partial depth.
        self._rest_blocked: bool = False
        # Venue-wide WS connect pacing. Binance caps connection
        # ATTEMPTS per IP (~300/5min); un-paced shard reconnects trip
        # it and every following opening handshake times out.
        self._connect_pacing_ts: float = 0.0
        self._rest_consecutive_failures: int = 0
        self._rest_pacing_ts: float = 0.0

        # Per-shard aggTrade->rawTrade fallback state.
        self._shard_raw_trade: Dict[int, bool] = {}
        # Venue-wide preference: once ANY shard detects region-level
        # aggTrade silence, all shards connect with raw @trade immediately
        # (no per-shard rediscovery, no wasted connection cycles).
        self._raw_trades_preferred: bool = False
        # Global count of processed trade events (aggTrade-silence detector).
        self._trade_events_count: int = 0

    # ------------------------------------------------------------------
    def _init_symbol_data(self, canonical_symbol: str) -> dict:
        """Initialize state and internal buffers for a canonical symbol."""
        canonical_symbol = canonical_symbol.upper()
        # Guarantee VenueWSState exists in self._states
        _ = self._state_for(canonical_symbol)

        if canonical_symbol not in self._symbol_data:
            self._symbol_data[canonical_symbol] = {
                "bids": {},          # price (float) -> quantity (float)
                "asks": {},          # price (float) -> quantity (float)
                "last_update_id": 0,
                "trades": deque(),   # tuples: (timestamp_sec, notional_usd, is_buy)
                # diff-sync machinery
                "synced": False,
                "buffer": deque(maxlen=2000),   # buffered depthUpdate diffs while unsynced
                "resync_in_progress": False,
                "resync_attempts": 0,
                "next_resync_ts": 0.0,
            }
        return self._symbol_data[canonical_symbol]

    # ------------------------------------------------------------------
    # Depth-basis helpers
    # ------------------------------------------------------------------
    def _effective_partial_basis(self) -> bool:
        """True when shards should stream top-20 partial depth instead of diffs."""
        if self._depth_mode == "partial":
            return True
        if self._depth_mode == "auto":
            return self._rest_blocked
        return False  # "diff" — REST + diff only, no fallback

    def _streams_for_symbol(self, canonical_symbol: str, raw_trade: bool = False) -> List[str]:
        native = csym.from_canonical(self.venue_name, canonical_symbol)
        if not native:
            return []
        n_low = native.lower()
        trade_stream = f"{n_low}@trade" if raw_trade else f"{n_low}@aggTrade"
        if self._effective_partial_basis():
            book_stream = f"{n_low}@depth{PARTIAL_DEPTH_LEVELS}@100ms"
        else:
            book_stream = f"{n_low}@depth@100ms"
        return [book_stream, trade_stream]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self, canonical_symbols: List[str]) -> None:
        """Spawn the shard supervisors; returns immediately."""
        await self.ensure_symbols(canonical_symbols)
        self._stop_event.clear()
        if not self._shard_tasks:
            per_conn = self._streams_per_connection()
            max_shards = 64  # generous pool; idle shards sleep-poll
            self._shard_tasks = [
                asyncio.create_task(self._shard_loop(i, per_conn))
                for i in range(max_shards)
            ]
            obs.log_venue_event(self.venue_name, "*", "adapter_started",
                                symbols=len(self._tracked_canonical_symbols),
                                streams_per_connection=per_conn,
                                depth_mode=self._depth_mode,
                                rest_depth_limit=_snap_depth_limit(
                                    CROSSEXCHANGE_CONFIG.get("cx_binance_rest_depth_limit", 100)))

    async def stop(self) -> None:
        """Tear down all shard connections and background tasks cleanly."""
        self._stop_event.set()

        for ws in list(self._sockets):
            try:
                await ws.close()
            except Exception:
                pass
        self._sockets.clear()

        for task in self._shard_tasks:
            if not task.done():
                task.cancel()
        for task in self._shard_tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._shard_tasks = []

        async with self._lock:
            for sym in list(self._tracked_canonical_symbols):
                state = self._state_for(sym)
                state.mark_disconnected("Adapter stopped")
        obs.log_venue_event(self.venue_name, "*", "adapter_stopped")

    async def ensure_symbols(self, canonical_symbols: List[str]) -> None:
        """Add/refresh subscriptions for additional symbols at runtime.

        Adding new symbols re-partitions the streams: every open socket is
        closed so each shard loop reconnects with the updated partition.
        """
        added = False
        async with self._lock:
            for sym in canonical_symbols:
                if not sym:
                    continue
                sym_upper = sym.upper()
                if sym_upper not in self._tracked_canonical_symbols:
                    self._tracked_canonical_symbols.add(sym_upper)
                    self._init_symbol_data(sym_upper)
                    added = True

        if added and self._sockets:
            for ws in list(self._sockets):
                try:
                    await ws.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Stream partitioning
    # ------------------------------------------------------------------
    def _streams_per_connection(self) -> int:
        try:
            val = int(CROSSEXCHANGE_CONFIG.get("cx_binance_streams_per_connection", DEFAULT_STREAMS_PER_CONNECTION) or DEFAULT_STREAMS_PER_CONNECTION)
        except (TypeError, ValueError):
            val = DEFAULT_STREAMS_PER_CONNECTION
        return max(10, min(val, 150))  # hard ceiling below Binance's 200 limit

    def _partition_symbols(self, per_conn_streams: int) -> List[List[str]]:
        """Split sorted tracked symbols into shards of <= per_conn_streams streams.

        2 streams per symbol (book + trades), so symbols_per_shard is
        per_conn_streams // 2.
        """
        symbols_per_shard = max(1, per_conn_streams // 2)
        symbols = sorted(self._tracked_canonical_symbols)
        return [symbols[i:i + symbols_per_shard] for i in range(0, len(symbols), symbols_per_shard)]

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------
    def _on_message(self, raw_msg: str) -> None:
        """Parse incoming WebSocket text message and dispatch to event handler."""
        try:
            msg = json.loads(raw_msg)
        except Exception:
            return

        # Combined stream envelope: {"stream": "...", "data": {...}}
        if isinstance(msg, dict) and isinstance(msg.get("data"), dict):
            data = msg["data"]
            stream = str(msg.get("stream") or "")
        else:
            data = msg
            stream = ""

        if not isinstance(data, dict):
            return

        event_type = data.get("e")
        if event_type in ("aggTrade", "trade") or stream.endswith(("@aggTrade", "@trade")):
            self._handle_trade_event(data)
        elif stream.endswith(f"@depth{PARTIAL_DEPTH_LEVELS}@100ms"):
            self._handle_partial_depth(data, stream)
        elif stream.endswith("@depth@100ms") or event_type == "depthUpdate":
            # Diff stream frame (or envelope without a stream name).
            if self._effective_partial_basis() and not stream.endswith("@depth@100ms"):
                # No stream name and we're on the partial basis: partial frame.
                self._handle_partial_depth(data, stream)
            else:
                self._handle_depth_diff(data)

    def _handle_partial_depth(self, data: dict, stream: str) -> None:
        """Apply a partial depth frame as a WHOLESALE top-of-book replacement.

        Each @depth20@100ms message carries the complete top-20 book, so the
        local book is rebuilt from scratch every frame — no sequence
        reconciliation, no REST snapshot, nothing to drift out of sync.
        """
        native_sym = data.get("s") or data.get("ps")
        if not native_sym and stream:
            head = stream.rsplit("/", 1)[-1]
            native_sym = head.split("@", 1)[0]
        if not native_sym:
            return

        canonical_symbol = csym.to_canonical(self.venue_name, str(native_sym))
        if not canonical_symbol or canonical_symbol not in self._tracked_canonical_symbols:
            return

        bids_raw = data.get("b") or data.get("bids")
        asks_raw = data.get("a") or data.get("asks")
        if not bids_raw or not asks_raw:
            return

        sym_data = self._init_symbol_data(canonical_symbol)
        state = self._state_for(canonical_symbol)

        new_bids: Dict[float, float] = {}
        new_asks: Dict[float, float] = {}
        for p_str, q_str in bids_raw:
            try:
                p, q = float(p_str), float(q_str)
                if q > 0:
                    new_bids[p] = q
            except (ValueError, TypeError, IndexError):
                continue
        for p_str, q_str in asks_raw:
            try:
                p, q = float(p_str), float(q_str)
                if q > 0:
                    new_asks[p] = q
            except (ValueError, TypeError, IndexError):
                continue

        if not new_bids or not new_asks:
            return

        sym_data["bids"] = new_bids
        sym_data["asks"] = new_asks
        sym_data["last_update_id"] = int(data.get("u") or data.get("lastUpdateId") or 0)
        sym_data["synced"] = True  # every partial is a complete, self-consistent book

        was_ready = state.book_initialized and state.book_valid
        state.mark_message()
        if not was_ready:
            state.mark_book_ready()
            obs.log_venue_event(self.venue_name, canonical_symbol, "book_valid",
                                basis="partial_depth_ws", levels=len(new_bids))

    def _handle_depth_diff(self, data: dict) -> None:
        """Handle a @depth@100ms diff event (official Binance futures sync).

        While unsynced: buffer the event and (re)start a REST snapshot sync.
        While synced: every event must chain (pu == previous u); a broken
        chain invalidates the book and re-runs the snapshot sync.
        """
        native_sym = data.get("s") or data.get("ps")
        if not native_sym:
            return
        canonical_symbol = csym.to_canonical(self.venue_name, str(native_sym))
        if not canonical_symbol or canonical_symbol not in self._tracked_canonical_symbols:
            return

        sym_data = self._init_symbol_data(canonical_symbol)
        state = self._state_for(canonical_symbol)

        try:
            U = int(data["U"])
            u = int(data["u"])
        except (KeyError, TypeError, ValueError):
            return
        pu = data.get("pu")
        pu = int(pu) if pu is not None else None
        bids_raw = data.get("b")
        asks_raw = data.get("a")

        if not sym_data["synced"]:
            sym_data["buffer"].append((U, u, pu, bids_raw, asks_raw))
            self._maybe_start_resync(canonical_symbol)
            return

        # Futures diff chain: pu of this event must equal u of the previous one.
        if pu is not None and pu != sym_data["last_update_id"]:
            sym_data["synced"] = False
            state.mark_gap(
                f"diff chain broken: pu={pu} != last_update_id={sym_data['last_update_id']}"
            )
            obs.log_venue_event(self.venue_name, canonical_symbol, "sequence_gap",
                                pu=pu, expected=sym_data["last_update_id"])
            sym_data["buffer"].append((U, u, pu, bids_raw, asks_raw))
            self._maybe_start_resync(canonical_symbol)
            return

        self._apply_diff_levels(sym_data, bids_raw, asks_raw)
        sym_data["last_update_id"] = u
        state.mark_message()

    def _apply_diff_levels(self, sym_data: dict, bids_raw, asks_raw) -> None:
        """Apply one diff's level updates in place (qty 0 => remove level)."""
        if bids_raw:
            bids = sym_data["bids"]
            for p_str, q_str in bids_raw:
                try:
                    p, q = float(p_str), float(q_str)
                except (ValueError, TypeError, IndexError):
                    continue
                if q <= 0:
                    bids.pop(p, None)
                else:
                    bids[p] = q
        if asks_raw:
            asks = sym_data["asks"]
            for p_str, q_str in asks_raw:
                try:
                    p, q = float(p_str), float(q_str)
                except (ValueError, TypeError, IndexError):
                    continue
                if q <= 0:
                    asks.pop(p, None)
                else:
                    asks[p] = q

    # ------------------------------------------------------------------
    # REST snapshot sync (diff mode)
    # ------------------------------------------------------------------
    def _maybe_start_resync(self, canonical_symbol: str) -> None:
        """Start a REST snapshot sync for a symbol (with backoff brake)."""
        sym_data = self._symbol_data.get(canonical_symbol)
        if not sym_data or sym_data["resync_in_progress"]:
            return
        if time.time() < sym_data["next_resync_ts"]:
            return
        sym_data["resync_in_progress"] = True
        asyncio.create_task(self._resync_symbol(canonical_symbol))

    async def _resync_symbol(self, canonical_symbol: str) -> None:
        """Fetch a REST snapshot and replay buffered diffs per Binance's
        official local-orderbook algorithm (futures variant)."""
        sym_data = self._symbol_data.get(canonical_symbol)
        state = self._state_for(canonical_symbol)
        try:
            if self._effective_partial_basis():
                return  # partial basis never needs REST

            attempt = sym_data["resync_attempts"] + 1
            sym_data["resync_attempts"] = attempt
            if attempt > 1:
                delay = min(RESYNC_MAX_BACKOFF, RESYNC_INITIAL_BACKOFF * (2 ** min(attempt, 5)))
                sym_data["next_resync_ts"] = time.time() + delay

            snapshot = await self._fetch_rest_snapshot(canonical_symbol)
            if snapshot is None:
                obs.log_venue_event(self.venue_name, canonical_symbol,
                                    "resync_failed", attempt=attempt)
                return

            last_id, bids, asks = snapshot

            # Drain the buffered diffs per the official algorithm:
            #   - drop events with u <= lastUpdateId
            #   - first applied event must satisfy U <= lastUpdateId <= u
            #   - afterwards each event must chain via pu
            buffer = sym_data["buffer"]
            while buffer and buffer[0][1] <= last_id:
                buffer.popleft()

            if buffer and not (buffer[0][0] <= last_id <= buffer[0][1]):
                # Snapshot is stale relative to the buffer — refetch shortly
                # (small retry brake; REST pacing already limits the rate).
                obs.log_venue_event(self.venue_name, canonical_symbol,
                                    "resync_stale_snapshot", buffer_head=(buffer[0][0], buffer[0][1]),
                                    last_id=last_id)
                sym_data["next_resync_ts"] = time.time() + 1.0
                sym_data["resync_attempts"] += 1
                return

            sym_data["bids"] = dict(bids)
            sym_data["asks"] = dict(asks)
            sym_data["last_update_id"] = last_id
            sym_data["synced"] = True

            # Apply any buffered events that chain from the snapshot.
            while buffer:
                U, u, pu, b_raw, a_raw = buffer.popleft()
                if pu is not None and pu != sym_data["last_update_id"]:
                    sym_data["synced"] = False
                    break
                self._apply_diff_levels(sym_data, b_raw, a_raw)
                sym_data["last_update_id"] = u

            if sym_data["synced"]:
                sym_data["resync_attempts"] = 0
                sym_data["next_resync_ts"] = 0.0
                was_ready = state.book_initialized and state.book_valid
                state.mark_message()
                if not was_ready:
                    state.mark_book_ready()
                    obs.log_venue_event(self.venue_name, canonical_symbol, "book_valid",
                                        basis="rest_snapshot_diff_replay", last_id=last_id)
        finally:
            sym_data["resync_in_progress"] = False

    async def _fetch_rest_snapshot(self, canonical_symbol: str):
        """Paced GET /fapi/v1/depth. Returns (lastUpdateId, bids, asks) or None.

        Also implements the auto-mode geo-block detection: REST failures are
        counted globally, and after REST_BLOCK_SWITCH_THRESHOLD consecutive
        failures the adapter switches the whole venue to the pure-WS
        partial-depth basis so Binance keeps contributing without REST.
        """
        if self._depth_mode == "partial":
            return None

        native = csym.from_canonical(self.venue_name, canonical_symbol)
        if not native:
            return None
        limit = _snap_depth_limit(CROSSEXCHANGE_CONFIG.get("cx_binance_rest_depth_limit", 100))
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={native}&limit={limit}"

        # Pacing + HTTP happen inside _blocking_rest_snapshot, in a worker
        # thread (never blocking the adapter's event loop).
        try:
            payload = await asyncio.to_thread(self._blocking_rest_snapshot, url)
        except Exception as exc:
            self._on_rest_failure(f"exception: {str(exc)[:120]}")
            return None

        if not isinstance(payload, dict) or "lastUpdateId" not in payload:
            self._on_rest_failure("malformed snapshot payload")
            return None

        self._rest_consecutive_failures = 0
        return int(payload["lastUpdateId"]), payload.get("bids") or [], payload.get("asks") or []

    def _blocking_rest_snapshot(self, url: str) -> dict:
        """Blocking, paced HTTP GET (runs in a worker thread via to_thread)."""
        # Global pacing (weight-based; futures cap is 2400 request-weight/min).
        now = time.time()
        wait = self._rest_pacing_ts + REST_MIN_INTERVAL_SECONDS - now
        if wait > 0:
            time.sleep(wait)
        self._rest_pacing_ts = time.time()
        return self._http_get_json(url)

    def _http_get_json(self, url: str) -> dict:
        """Blocking HTTP GET returning parsed JSON (raises on HTTP >= 400).

        Uses `requests` when available, urllib otherwise; runs in a worker
        thread via asyncio.to_thread (caller must be async).
        """
        try:
            import requests
            resp = requests.get(url, timeout=REST_TIMEOUT_SECONDS)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            return resp.json()
        except ImportError:
            import urllib.request
            req = urllib.request.Request(url, headers={"User-Agent": "cx-binance-adapter/1.0"})
            with urllib.request.urlopen(req, timeout=REST_TIMEOUT_SECONDS) as resp:
                return json.loads(resp.read().decode())

    def _on_rest_failure(self, reason: str) -> None:
        self._rest_consecutive_failures += 1
        obs.log_venue_event(self.venue_name, "*", "rest_snapshot_failed",
                            consecutive=self._rest_consecutive_failures, reason=reason[:160])
        if (
            self._depth_mode == "auto"
            and not self._rest_blocked
            and self._rest_consecutive_failures >= REST_BLOCK_SWITCH_THRESHOLD
        ):
            self._rest_blocked = True
            obs.log_venue_event(
                self.venue_name, "*", "rest_blocked_switching_to_partial_depth",
                note="REST unavailable in this region (451/blocked); Binance basis is now the pure-WS top-20 partial depth stream",
            )
            # Force all shards to rebuild on partial-depth streams.
            _task = asyncio.create_task(self._close_all_sockets())

    async def _close_all_sockets(self) -> None:
        for ws in list(self._sockets):
            try:
                await ws.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------
    def _handle_trade_event(self, data: dict) -> None:
        """Process a trade event (@aggTrade or raw @trade — identical fields)."""
        native_sym = data.get("s")
        if not native_sym:
            return
        canonical_symbol = csym.to_canonical(self.venue_name, native_sym)
        if not canonical_symbol or canonical_symbol not in self._tracked_canonical_symbols:
            return

        sym_data = self._init_symbol_data(canonical_symbol)
        state = self._state_for(canonical_symbol)

        try:
            price = float(data["p"])
            qty = float(data["q"])
            trade_ts = float(data.get("T", time.time() * 1000.0)) / 1000.0
            is_buyer_maker = bool(data.get("m", False))
            # m=True means buyer was maker -> seller was taker (SELL trade).
            is_buy = not is_buyer_maker

            notional = norm.notional_usd(price, qty, multiplier=1.0)

            sym_data["trades"].append((trade_ts, notional, is_buy))
            state.last_trade_ts = trade_ts
            state.mark_message()
            self._trade_events_count += 1

            # Maintain rolling buffer window up to 300s
            cutoff = time.time() - 300.0
            trades = sym_data["trades"]
            while trades and trades[0][0] < cutoff:
                trades.popleft()
        except (KeyError, ValueError, TypeError):
            pass

    # ------------------------------------------------------------------
    # Connection runner (one per shard, independent reconnect loops)
    # ------------------------------------------------------------------
    async def _paced_connect_delay(self) -> None:
        """Guarantee a minimum interval between connect() attempts
        across ALL shards (shared venue-level pacing clock)."""
        interval = float(CROSSEXCHANGE_CONFIG.get(
            "cx_binance_connect_min_interval_seconds", 6.0))
        now = time.time()
        wait = self._connect_pacing_ts - now
        self._connect_pacing_ts = max(now, self._connect_pacing_ts) + interval
        if wait > 0:
            await asyncio.sleep(wait + random.uniform(0.0, 1.0))

    async def _shard_loop(self, shard_index: int, per_conn_streams: int) -> None:
        """Own one partition of the symbol universe; reconnect with backoff."""
        initial_backoff = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_initial_backoff_seconds", 1.0))
        max_backoff = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_max_backoff_seconds", 30.0))
        jitter = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_jitter_seconds", 0.5))
        backoff = initial_backoff

        while not self._stop_event.is_set():
            shard_symbols: Optional[List[str]] = None
            try:
                partition = self._partition_symbols(per_conn_streams)
                if shard_index < len(partition):
                    shard_symbols = partition[shard_index]
            except Exception:
                shard_symbols = None

            if not shard_symbols:
                # Nothing assigned to this shard (yet) — idle-wait quietly.
                await asyncio.sleep(2.0)
                continue

            use_raw_trade = bool(self._shard_raw_trade.get(shard_index, False)) or self._raw_trades_preferred
            stream_parts: List[str] = []
            for sym in shard_symbols:
                stream_parts.extend(self._streams_for_symbol(sym, raw_trade=use_raw_trade))
            stream_parts = [s for s in stream_parts if s]
            if not stream_parts:
                await asyncio.sleep(2.0)
                continue

            url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_parts)}"
            partial_basis = self._effective_partial_basis()

            for sym in shard_symbols:
                self._state_for(sym).mark_reconnect_attempt()

            obs.log_venue_event(self.venue_name, "*", "shard_connecting",
                                shard=shard_index, symbols=len(shard_symbols),
                                streams=len(stream_parts),
                                depth_basis="partial20" if partial_basis else "diff_rest")

            ws = None
            await self._paced_connect_delay()
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=15,
                    open_timeout=15.0,
                    close_timeout=5,
                    max_size=2 ** 24,
                ) as _ws:
                    ws = _ws
                    self._sockets.add(_ws)
                    backoff = initial_backoff

                    for sym in shard_symbols:
                        state = self._state_for(sym)
                        state.mark_connected()
                        state.mark_subscribed()
                    trade_count_at_connect = self._trade_events_count
                    connected_ts = time.time()
                    obs.log_venue_event(self.venue_name, "*", "shard_connected",
                                        shard=shard_index, symbols=len(shard_symbols),
                                        trade_stream="raw@trade" if use_raw_trade else "aggTrade",
                                        depth_basis="partial20" if partial_basis else "diff_rest")

                    # Receive with a watchdog: a silently-dead connection
                    # (no frames, no close) must not wedge the shard forever.
                    while not self._stop_event.is_set():
                        try:
                            raw_msg = await asyncio.wait_for(_ws.recv(), timeout=45.0)
                        except asyncio.TimeoutError:
                            # depth + trades across a whole shard are never
                            # silent for 45s; treat as dead transport.
                            obs.log_venue_event(self.venue_name, "*", "shard_stale",
                                                shard=shard_index, reason="no frames for 45s")
                            raise ConnectionError("shard receive watchdog fired")

                        # aggTrade-silence detector: in legally-restricted
                        # regions Binance may serve books + raw trades but
                        # zero @aggTrade frames. After 20s with books flowing
                        # but no trade events, switch this shard to raw @trade.
                        if (
                            not use_raw_trade
                            and time.time() - connected_ts > 20.0
                            and self._trade_events_count == trade_count_at_connect
                        ):
                            self._shard_raw_trade[shard_index] = True
                            self._raw_trades_preferred = True
                            obs.log_venue_event(self.venue_name, "*",
                                                "aggtrade_silent_switching_to_raw_trade",
                                                shard=shard_index, symbols=len(shard_symbols))
                            raise ConnectionError("aggTrade silent — switching shard to raw @trade")

                        self._on_message(raw_msg)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                obs.log_venue_event(self.venue_name, "*", "shard_disconnected",
                                    shard=shard_index, reason=str(exc)[:200])
                handshake_timeout = "timed out during opening handshake" in str(exc)
            finally:
                if ws is not None:
                    self._sockets.discard(ws)
                for sym in shard_symbols:
                    self._state_for(sym).mark_disconnected()

            if self._stop_event.is_set():
                break

            if handshake_timeout:
                # Opening-handshake timeout = per-IP connection-budget
                # exhaustion signature. Retrying fast deepens the lockout;
                # back off far beyond the normal cap, jittered so the
                # shards never retry in sync.
                hs_cap = float(CROSSEXCHANGE_CONFIG.get(
                    "cx_binance_handshake_backoff_cap_seconds", 300.0))
                backoff = min(hs_cap, max(45.0, backoff * 3.0))
                actual_backoff = backoff + random.uniform(0.0, 15.0)
            else:
                actual_backoff = max(0.1, min(max_backoff, backoff) + random.uniform(0.0, jitter))
                backoff = min(max_backoff, backoff * 2.0)
            # obs.log_venue_event(self.venue_name, "*", "shard_reconnect_waiting",
            #                     shard=shard_index, backoff_seconds=round(actual_backoff, 2))
            await asyncio.sleep(actual_backoff)

    # ------------------------------------------------------------------
    # Data Accessors
    # ------------------------------------------------------------------
    def get_book_snapshot(self, canonical_symbol: str) -> Optional[NormalizedBookSnapshot]:
        """
        Constructs and returns NormalizedBookSnapshot for the given canonical symbol.
        Returns None if book is uninitialized, invalid, stale, or crossed.
        """
        canonical_symbol = canonical_symbol.upper()
        state = self._state_for(canonical_symbol)

        stale_after = CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0)
        if not state.is_eligible_for_consolidation(stale_after_seconds=stale_after):
            return None

        sym_data = self._symbol_data.get(canonical_symbol)
        if not sym_data:
            return None

        # Diff basis: an un-synced book (REST snapshot pending / chain broken)
        # must never contribute.
        if not sym_data.get("synced") and not self._effective_partial_basis():
            return None

        bids = sym_data["bids"]
        asks = sym_data["asks"]

        if not bids or not asks:
            return None

        best_bid = max(bids.keys())
        best_ask = min(asks.keys())

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            state.book_valid = False
            return None

        mid = norm.venue_mid(best_bid, best_ask)
        if mid is None or mid <= 0:
            return None

        bucket_size = CROSSEXCHANGE_CONFIG.get("cx_bucket_size_bps", 5)
        max_bps_range = CROSSEXCHANGE_CONFIG.get("cx_max_bps_range", 100)

        bids_bps = norm.build_bps_depth(
            bids.items(),
            mid,
            multiplier=1.0,
            bucket_size_bps=bucket_size,
            max_bps_range=max_bps_range,
        )
        asks_bps = norm.build_bps_depth(
            asks.items(),
            mid,
            multiplier=1.0,
            bucket_size_bps=bucket_size,
            max_bps_range=max_bps_range,
        )

        return NormalizedBookSnapshot(
            venue=self.venue_name,
            canonical_symbol=canonical_symbol,
            ts=time.time(),
            venue_mid=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_bps=bids_bps,
            asks_bps=asks_bps,
            book_valid=True,
            sequence_meta={"last_update_id": sym_data["last_update_id"]},
        )

    def get_trade_flow(
        self,
        canonical_symbol: str,
        *,
        lookback_seconds: float = 60.0
    ) -> Optional[NormalizedTradeFlow]:
        """
        Calculates buyer/seller USD trade notionals within rolling lookback window.
        Returns None if connection is inactive or no trades are recorded in window.
        """
        canonical_symbol = canonical_symbol.upper()
        state = self._state_for(canonical_symbol)

        if not state.ws_connected or not state.ws_subscribed:
            return None

        sym_data = self._symbol_data.get(canonical_symbol)
        if not sym_data:
            return None

        trades = sym_data["trades"]

        now = time.time()
        window_start = now - lookback_seconds

        buy_notional = 0.0
        sell_notional = 0.0
        trade_count = 0

        for t_ts, notional, is_buy in trades:
            if t_ts >= window_start:
                trade_count += 1
                if is_buy:
                    buy_notional += notional
                else:
                    sell_notional += notional

        if trade_count == 0:
            return None

        return NormalizedTradeFlow(
            venue=self.venue_name,
            canonical_symbol=canonical_symbol,
            window_start=window_start,
            window_end=now,
            buy_notional_usd=buy_notional,
            sell_notional_usd=sell_notional,
            trade_count=trade_count,
            book_valid=state.book_valid,
        )
