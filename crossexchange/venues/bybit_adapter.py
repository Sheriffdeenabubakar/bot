"""
crossexchange/venues/bybit_adapter.py
=========================================
Bybit V5 Linear-Perpetual WebSocket order-flow adapter for the cross-exchange layer.

Fixes applied after live-log forensics (the adapter previously connected on the
3rd attempt and then went SILENT forever right after ws_connected):

  1. SUBSCRIBE RATE LIMITING — Bybit allows max 5 subscribe/unsubscribe
     operations per second. The old build fired 51 subscribe requests in
     ~2.5s (10 args each, 0.05s pacing), blowing straight through the limit,
     after which Bybit stops answering — the socket wedges with no error,
     no ACK, no data. Subscribes are now paced at 4 ops/s with per-send
     timeouts, and every ACK/error is logged.

  2. CONNECTION SHARDING — 254 symbols x 2 topics on a single socket is
     both a throughput and reliability hazard. Symbols are now partitioned
     across multiple connections (default 50 symbols per connection,
     configurable via cx_bybit_symbols_per_connection), each with an
     independent reconnect loop, ping loop and watchdog.

  3. RECEIVE WATCHDOG — Bybit's failure mode is a SILENT connection (no
     frames, no close). The old `async for` loop would then hang forever.
     Every receive is now wrapped in a 60s timeout: a silent socket is
     force-reconnected instead of wedging the venue.

Protocol (unchanged, per Bybit V5 docs):
  wss://stream.bybit.com/v5/public/linear (USDT & USDC linear perpetuals).
  - Orderbook topic: `orderbook.{depth}.{symbol}` (e.g. `orderbook.50.BTCUSDT`)
    -> snapshot message (type="snapshot") establishes the book; subsequent
    type="delta" messages are applied with strict u-sequence verification
    (gap => unsubscribe/resubscribe to force a fresh snapshot).
  - Trades topic: `publicTrade.{symbol}`.
  - Subscribe payload: {"op": "subscribe", "args": [...]} with <= 10 args per
    request (Bybit recommended batch size).
  - Heartbeat: client sends {"op": "ping"} every ~15s; server answers pong.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import deque
from typing import Dict, List, Optional, Set

import websockets
from sortedcontainers import SortedDict

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


BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

DEFAULT_SYMBOLS_PER_CONNECTION = 50
SUBSCRIBE_ARGS_PER_REQUEST = 10       # Bybit recommended batch size
SUBSCRIBE_PACING_SECONDS = 0.30       # ~3.3 ops/s, safely under Bybit's 5/s limit
MAX_SHARD_TASKS = 32                  # generous pool; idle shards sleep-poll


def _ws_is_open(ws) -> bool:
    """websockets compatibility shim: works on both the legacy
    WebSocketClientProtocol API (<=12, which exposes `.open`) and the new
    asyncio ClientConnection API (>=13/14, which exposes `.state`)."""
    if ws is None:
        return False
    open_attr = getattr(ws, "open", None)
    if isinstance(open_attr, bool):
        return open_attr
    state = getattr(ws, "state", None)
    name = getattr(state, "name", str(state))
    return str(name).upper() == "OPEN"


class BybitOrderFlowAdapter(VenueOrderFlowAdapter):
    """Bybit V5 Linear-Perpetual WebSocket order-flow adapter (sharded)."""

    venue_name = "bybit"

    def __init__(self):
        super().__init__()
        self._symbols: Set[str] = set()
        self._books: Dict[str, dict] = {}
        # Stores trade flow tuples: (timestamp_seconds, side, price, qty, notional_usd)
        self._trades: Dict[str, deque] = {}
        self._running: bool = False

        # Sharded connection management
        self._shard_sockets: Dict[int, object] = {}
        self._shard_locks: Dict[int, asyncio.Lock] = {}
        self._shard_tasks: List[asyncio.Task] = []
        self._ping_tasks: Dict[int, asyncio.Task] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self, canonical_symbols: List[str]) -> None:
        """Begin Bybit's independent WebSocket lifecycle for the given symbols."""
        if self._running:
            await self.ensure_symbols(canonical_symbols)
            return

        self._running = True
        for cs in canonical_symbols:
            cs_upper = cs.strip().upper()
            self._symbols.add(cs_upper)
            self._state_for(cs_upper)

        obs.log_venue_event(
            self.venue_name,
            "*",
            "start_requested",
            symbol_count=len(canonical_symbols),
            symbols_per_connection=self._symbols_per_connection(),
        )
        for i in range(MAX_SHARD_TASKS):
            self._shard_locks[i] = asyncio.Lock()
            self._shard_tasks.append(asyncio.create_task(self._run_shard(i)))

    async def stop(self) -> None:
        """Tear down all shard connections and background tasks."""
        if not self._running:
            return

        self._running = False
        obs.log_venue_event(self.venue_name, "*", "stop_requested")

        for ws in list(self._shard_sockets.values()):
            try:
                await ws.close()
            except Exception:
                pass
        self._shard_sockets.clear()

        for task in self._shard_tasks:
            if not task.done():
                task.cancel()
        for task in self._ping_tasks.values():
            if not task.done():
                task.cancel()

        for task in self._shard_tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._shard_tasks = []
        self._ping_tasks = {}

        for st in self._states.values():
            st.mark_disconnected(reason="stopped")

        obs.log_venue_event(self.venue_name, "*", "stopped")

    async def ensure_symbols(self, canonical_symbols: List[str]) -> None:
        """Dynamically add/subscribe additional symbols at runtime."""
        new_syms: List[str] = []
        for cs in canonical_symbols:
            cs_upper = cs.strip().upper()
            if cs_upper not in self._symbols:
                self._symbols.add(cs_upper)
                self._state_for(cs_upper)
                new_syms.append(cs_upper)

        if not new_syms:
            return

        # Force every shard to reconnect with the new partition; the shard
        # that ends up owning each new symbol will subscribe it on connect.
        for ws in list(self._shard_sockets.values()):
            try:
                await ws.close()
            except Exception:
                pass
        obs.log_venue_event(self.venue_name, "*", "ensure_symbols_repartition",
                            new_symbols=len(new_syms))

    # ------------------------------------------------------------------
    # Data Accessors
    # ------------------------------------------------------------------
    def get_book_snapshot(self, canonical_symbol: str) -> Optional[NormalizedBookSnapshot]:
        """Return normalized book snapshot for canonical_symbol, or None if invalid/stale."""
        cs_upper = canonical_symbol.strip().upper()
        state = self.get_state(cs_upper)
        stale_after = CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0)

        if not state or not state.is_eligible_for_consolidation(stale_after_seconds=stale_after):
            return None

        book = self._books.get(cs_upper)
        if not book:
            return None

        bids: SortedDict = book.get("bids")
        asks: SortedDict = book.get("asks")

        if not bids or not asks:
            return None

        try:
            best_bid = float(bids.keys()[-1])
            best_ask = float(asks.keys()[0])
        except (IndexError, TypeError, ValueError):
            return None

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            return None

        mid = norm.venue_mid(best_bid, best_ask)
        if not mid or mid <= 0:
            return None

        bucket_size = CROSSEXCHANGE_CONFIG.get("cx_bucket_size_bps", 5)
        max_bps = CROSSEXCHANGE_CONFIG.get("cx_max_bps_range", 100)

        bids_bps = norm.build_bps_depth(
            bids.items(),
            mid,
            multiplier=1.0,
            bucket_size_bps=bucket_size,
            max_bps_range=max_bps,
        )

        asks_bps = norm.build_bps_depth(
            asks.items(),
            mid,
            multiplier=1.0,
            bucket_size_bps=bucket_size,
            max_bps_range=max_bps,
        )

        return NormalizedBookSnapshot(
            venue=self.venue_name,
            canonical_symbol=state.canonical_symbol,
            ts=time.time(),
            venue_mid=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_bps=bids_bps,
            asks_bps=asks_bps,
            book_valid=True,
            sequence_meta=dict(state.sequence_state),
        )

    def get_trade_flow(self, canonical_symbol: str, *, lookback_seconds: float = 60.0) -> Optional[NormalizedTradeFlow]:
        """Return normalized trade flow for canonical_symbol over lookback_seconds, or None if transport unhealthy."""
        cs_upper = canonical_symbol.strip().upper()
        state = self.get_state(cs_upper)
        stale_after = CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0)

        if not state or not state.ws_connected or not state.ws_subscribed:
            return None

        now = time.time()
        if stale_after and state.last_message_ts > 0 and (now - state.last_message_ts) > stale_after:
            return None

        window_start = now - max(lookback_seconds, 1.0)
        deque_trades = self._trades.get(cs_upper)

        buy_notional = 0.0
        sell_notional = 0.0
        trade_count = 0

        if deque_trades:
            for ts_sec, side, _p, _v, notional in deque_trades:
                if ts_sec >= window_start:
                    trade_count += 1
                    if side == "Buy":
                        buy_notional += notional
                    elif side == "Sell":
                        sell_notional += notional

        return NormalizedTradeFlow(
            venue=self.venue_name,
            canonical_symbol=state.canonical_symbol,
            window_start=window_start,
            window_end=now,
            buy_notional_usd=buy_notional,
            sell_notional_usd=sell_notional,
            trade_count=trade_count,
            book_valid=state.book_valid,
        )

    # ------------------------------------------------------------------
    # Sharding
    # ------------------------------------------------------------------
    def _symbols_per_connection(self) -> int:
        try:
            val = int(CROSSEXCHANGE_CONFIG.get("cx_bybit_symbols_per_connection", DEFAULT_SYMBOLS_PER_CONNECTION) or DEFAULT_SYMBOLS_PER_CONNECTION)
        except (TypeError, ValueError):
            val = DEFAULT_SYMBOLS_PER_CONNECTION
        return max(5, min(val, 100))

    def _partition_symbols(self) -> List[List[str]]:
        """Deterministic contiguous partition of the sorted symbol universe."""
        per_shard = self._symbols_per_connection()
        symbols = sorted(self._symbols)
        return [symbols[i:i + per_shard] for i in range(0, len(symbols), per_shard)]

    def _shard_index_for_symbol(self, canonical_symbol: str) -> int:
        cs_upper = (canonical_symbol or "").strip().upper()
        for idx, part in enumerate(self._partition_symbols()):
            if cs_upper in part:
                return idx
        return -1

    # ------------------------------------------------------------------
    # Connection & subscription management (per shard)
    # ------------------------------------------------------------------
    async def _send_json_shard(self, shard_index: int, payload: dict) -> None:
        """Send a JSON payload on one shard's socket, guarded by its lock."""
        ws = self._shard_sockets.get(shard_index)
        if not _ws_is_open(ws):
            raise ConnectionError(f"shard {shard_index} socket not open")
        lock = self._shard_locks.get(shard_index)
        if lock is None:
            lock = asyncio.Lock()
            self._shard_locks[shard_index] = lock
        async with lock:
            await asyncio.wait_for(ws.send(json.dumps(payload)), timeout=10.0)

    async def _subscribe_topics_on_shard(self, shard_index: int, topics: List[str]) -> int:
        """Batch topics in chunks of 10, PACED under Bybit's 5 ops/sec limit.

        Returns the number of subscribe requests sent. Every send is wrapped
        in a timeout so a wedged socket surfaces as an error instead of an
        eternal hang.
        """
        if not topics:
            return 0
        sent = 0
        for i in range(0, len(topics), SUBSCRIBE_ARGS_PER_REQUEST):
            chunk = topics[i:i + SUBSCRIBE_ARGS_PER_REQUEST]
            payload = {"op": "subscribe", "args": chunk}
            await self._send_json_shard(shard_index, payload)
            sent += 1
            if i + SUBSCRIBE_ARGS_PER_REQUEST < len(topics):
                await asyncio.sleep(SUBSCRIBE_PACING_SECONDS)
        return sent

    async def _run_shard(self, shard_index: int) -> None:
        """Own one partition of the symbol universe; reconnect with backoff."""
        initial_backoff = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_initial_backoff_seconds", 1.0))
        max_backoff = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_max_backoff_seconds", 30.0))
        jitter = float(CROSSEXCHANGE_CONFIG.get("cx_reconnect_jitter_seconds", 0.5))
        backoff = initial_backoff
        attempt = 0

        while self._running:
            partition = self._partition_symbols()
            shard_symbols = partition[shard_index] if shard_index < len(partition) else None

            if not shard_symbols:
                # Nothing assigned to this shard (yet) — idle-wait quietly.
                await asyncio.sleep(2.0)
                continue

            attempt += 1
            for cs in shard_symbols:
                self._state_for(cs).mark_reconnect_attempt()

            obs.log_venue_event(self.venue_name, "*", "shard_connecting",
                                shard=shard_index, attempt=attempt, symbols=len(shard_symbols))

            ws = None
            ping_task: Optional[asyncio.Task] = None
            try:
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=None,     # Bybit uses app-level JSON ping
                    ping_timeout=None,
                    open_timeout=15.0,
                    close_timeout=5.0,
                    max_size=2 ** 24,
                ) as _ws:
                    ws = _ws
                    self._shard_sockets[shard_index] = _ws
                    backoff = initial_backoff
                    attempt = 0

                    for cs in shard_symbols:
                        st = self._state_for(cs)
                        st.mark_connected()

                    obs.log_venue_event(self.venue_name, "*", "shard_connected",
                                        shard=shard_index, url=BYBIT_WS_URL)

                    # Subscribe to orderbook and trade topics for this shard,
                    # paced under the 5 ops/sec subscribe limit.
                    topics = self._build_sub_topics(shard_symbols)
                    try:
                        reqs = await self._subscribe_topics_on_shard(shard_index, topics)
                    except Exception as exc:
                        obs.log_venue_event(self.venue_name, "*", "subscribe_failed",
                                            shard=shard_index, error=str(exc)[:200])
                        raise

                    for cs in shard_symbols:
                        st = self._state_for(cs)
                        st.mark_subscribed()

                    obs.log_venue_event(self.venue_name, "*", "shard_subscribed",
                                        shard=shard_index, topics=len(topics), requests=reqs)

                    # App-level keepalive for this shard's socket
                    ping_task = asyncio.create_task(self._ping_loop(shard_index))

                    # RECEIVE WATCHDOG: a silently-dead Bybit connection (no
                    # frames, no close frame — the failure observed live)
                    # must never wedge the shard forever. 60s with zero
                    # messages across a whole shard means dead transport.
                    while self._running:
                        try:
                            msg = await asyncio.wait_for(_ws.recv(), timeout=60.0)
                        except asyncio.TimeoutError:
                            obs.log_venue_event(self.venue_name, "*", "shard_stale",
                                                shard=shard_index, reason="no frames for 60s")
                            raise ConnectionError("shard receive watchdog fired")
                        await self._handle_message(msg)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                obs.log_venue_event(self.venue_name, "*", "shard_disconnected",
                                    shard=shard_index, reason=str(exc)[:200])
            finally:
                if ping_task is not None and not ping_task.done():
                    ping_task.cancel()
                self._ping_tasks.pop(shard_index, None)
                if ws is not None:
                    self._shard_sockets.pop(shard_index, None)
                for cs in shard_symbols:
                    self._state_for(cs).mark_disconnected(reason="connection_closed")

            if not self._running:
                break

            backoff = min(max_backoff, initial_backoff * (2 ** attempt)) + random.uniform(0, jitter)
            attempt += 1
            # obs.log_venue_event(
            #     self.venue_name,
            #     "*",
            #     "shard_reconnect_waiting",
            #     shard=shard_index,
            #     backoff_seconds=round(backoff, 2),
            #     attempt=attempt,
            # )
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break

    async def _ping_loop(self, shard_index: int) -> None:
        """Periodically send JSON ping keepalives on one shard's socket."""
        interval = float(CROSSEXCHANGE_CONFIG.get("cx_heartbeat_interval_seconds", 15.0))
        while self._running:
            try:
                await asyncio.sleep(interval)
                await self._send_json_shard(shard_index, {"op": "ping"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                obs.log_venue_event(self.venue_name, "*", "ping_error", shard=shard_index, error=str(e)[:200])
                break

    def _build_sub_topics(self, canonical_symbols: List[str]) -> List[str]:
        """Convert canonical symbols to Bybit native orderbook and publicTrade topics."""
        topics: List[str] = []
        depth = CROSSEXCHANGE_CONFIG.get("cx_book_depth_levels", 50)
        for csym_str in canonical_symbols:
            native = csym.from_canonical(self.venue_name, csym_str)
            if native:
                topics.append(f"orderbook.{depth}.{native}")
                topics.append(f"publicTrade.{native}")
        return topics

    # ------------------------------------------------------------------
    # Message handling (unchanged protocol logic)
    # ------------------------------------------------------------------
    async def _handle_message(self, msg_raw) -> None:
        """Parse raw incoming WebSocket frame."""
        try:
            data = json.loads(msg_raw)
        except Exception:
            return

        if not isinstance(data, dict):
            return

        op = data.get("op")
        ret_msg = str(data.get("ret_msg", "")).lower()

        # Handle Ping / Pong response
        if op == "pong" or "pong" in ret_msg or (op == "ping" and data.get("success")):
            for st in self._states.values():
                st.mark_heartbeat()
            return

        # Handle Subscribe ACK — log failures loudly but never crash.
        if op in ("subscribe", "unsubscribe"):
            success = data.get("success")
            if not success:
                obs.log_venue_event(
                    self.venue_name,
                    "*",
                    f"{op}_ack_failed",
                    ret_msg=str(data.get("ret_msg"))[:200],
                    shard_hint=len(str(data.get("conn_id") or "")),
                )
            return

        topic = data.get("topic", "")
        if not topic:
            return

        if topic.startswith("orderbook."):
            await self._handle_orderbook_message(topic, data)
        elif topic.startswith("publicTrade."):
            self._handle_trade_message(topic, data)

    async def _handle_orderbook_message(self, topic: str, msg: dict) -> None:
        """Process orderbook snapshot and delta payloads with u sequence verification."""
        parts = topic.split(".")
        if len(parts) < 3:
            return

        native_symbol = parts[-1]
        canonical_symbol = csym.to_canonical(self.venue_name, native_symbol)
        if not canonical_symbol:
            return

        state = self._state_for(canonical_symbol)
        msg_type = msg.get("type")
        book_data = msg.get("data", {})
        if not isinstance(book_data, dict):
            return

        u = book_data.get("u")
        seq = book_data.get("seq")

        if canonical_symbol not in self._books:
            self._books[canonical_symbol] = {
                "bids": SortedDict(),
                "asks": SortedDict(),
                "last_u": None,
                "last_seq": None,
            }

        book = self._books[canonical_symbol]

        if msg_type == "snapshot":
            bids = SortedDict()
            asks = SortedDict()

            for p_str, v_str in book_data.get("b", []):
                try:
                    p, v = float(p_str), float(v_str)
                    if v > 0:
                        bids[p] = v
                except (ValueError, TypeError):
                    continue

            for p_str, v_str in book_data.get("a", []):
                try:
                    p, v = float(p_str), float(v_str)
                    if v > 0:
                        asks[p] = v
                except (ValueError, TypeError):
                    continue

            book["bids"] = bids
            book["asks"] = asks
            book["last_u"] = u
            book["last_seq"] = seq

            state.sequence_state["last_u"] = u
            state.sequence_state["last_seq"] = seq

            was_recovering = state.recovery_in_progress
            state.mark_book_ready()
            state.mark_message()

            if was_recovering:
                obs.log_venue_event(self.venue_name, canonical_symbol, "recovery_completed", u=u, seq=seq)
            else:
                obs.log_venue_event(self.venue_name, canonical_symbol, "book_valid", u=u, seq=seq)

        elif msg_type == "delta":
            if not state.book_initialized or not state.book_valid:
                return

            last_u = book.get("last_u")

            # Check for server reset signal (u == 1)
            if u == 1:
                state.mark_gap("Received u=1 snapshot reset signal")
                obs.log_venue_event(
                    self.venue_name,
                    canonical_symbol,
                    "sequence_gap",
                    reason="u=1_reset",
                    prev_u=last_u,
                    new_u=u,
                )
                await self._trigger_symbol_resync(canonical_symbol)
                return

            # Check update ID sequencing
            if last_u is not None:
                if u <= last_u:
                    if u == last_u:
                        return  # Duplicate frame, ignore
                    else:
                        state.mark_gap(f"Out-of-order updateId: prev_u={last_u}, msg_u={u}")
                        obs.log_venue_event(
                            self.venue_name,
                            canonical_symbol,
                            "sequence_gap",
                            reason="out_of_order",
                            prev_u=last_u,
                            new_u=u,
                        )
                        await self._trigger_symbol_resync(canonical_symbol)
                        return
                elif u != last_u + 1:
                    state.mark_gap(f"Sequence gap: prev_u={last_u}, msg_u={u}")
                    obs.log_venue_event(
                        self.venue_name,
                        canonical_symbol,
                        "sequence_gap",
                        reason="gap",
                        prev_u=last_u,
                        new_u=u,
                    )
                    await self._trigger_symbol_resync(canonical_symbol)
                    return

            bids = book["bids"]
            asks = book["asks"]

            for p_str, v_str in book_data.get("b", []):
                try:
                    p, v = float(p_str), float(v_str)
                    if v <= 0:
                        bids.pop(p, None)
                    else:
                        bids[p] = v
                except (ValueError, TypeError):
                    continue

            for p_str, v_str in book_data.get("a", []):
                try:
                    p, v = float(p_str), float(v_str)
                    if v <= 0:
                        asks.pop(p, None)
                    else:
                        asks[p] = v
                except (ValueError, TypeError):
                    continue

            book["last_u"] = u
            book["last_seq"] = seq
            state.sequence_state["last_u"] = u
            state.sequence_state["last_seq"] = seq
            state.mark_message()

    async def _trigger_symbol_resync(self, canonical_symbol: str) -> None:
        """Resync a single symbol by unsubscribing and resubscribing (on the
        shard that owns it) to trigger a fresh snapshot."""
        state = self._state_for(canonical_symbol)
        state.mark_recovery_started()
        obs.log_venue_event(self.venue_name, canonical_symbol, "recovery_started")

        native = csym.from_canonical(self.venue_name, canonical_symbol)
        if not native:
            return
        depth = CROSSEXCHANGE_CONFIG.get("cx_book_depth_levels", 50)
        topic = f"orderbook.{depth}.{native}"

        shard_index = self._shard_index_for_symbol(canonical_symbol)
        if shard_index < 0:
            return
        try:
            await self._send_json_shard(shard_index, {"op": "unsubscribe", "args": [topic]})
            await asyncio.sleep(0.1)
            await self._send_json_shard(shard_index, {"op": "subscribe", "args": [topic]})
        except Exception as exc:
            # Shard socket is gone; the shard loop will resubscribe
            # everything (including this symbol) when it reconnects.
            obs.log_venue_event(self.venue_name, canonical_symbol,
                                "resync_send_failed", error=str(exc)[:200])

    def _handle_trade_message(self, topic: str, msg: dict) -> None:
        """Ingest real-time trade messages into a rolling deque."""
        parts = topic.split(".")
        if len(parts) < 2:
            return

        native_symbol = parts[-1]
        canonical_symbol = csym.to_canonical(self.venue_name, native_symbol)
        if not canonical_symbol:
            return

        state = self._state_for(canonical_symbol)
        trades_data = msg.get("data", [])
        if not isinstance(trades_data, list):
            return

        if canonical_symbol not in self._trades:
            self._trades[canonical_symbol] = deque()

        deque_trades = self._trades[canonical_symbol]
        now = time.time()
        max_lookback = 600.0  # Keep up to 10 minutes of trade flow

        for tr in trades_data:
            if not isinstance(tr, dict):
                continue
            try:
                ts_ms = tr.get("T")
                ts_sec = float(ts_ms) / 1000.0 if ts_ms else now
                side = str(tr.get("S", "")).strip()
                p = float(tr.get("p", 0))
                v = float(tr.get("v", 0))
                if p <= 0 or v <= 0:
                    continue

                notional = norm.notional_usd(p, v, multiplier=1.0)
                deque_trades.append((ts_sec, side, p, v, notional))
                state.last_trade_ts = ts_sec
                state.mark_message()
            except (ValueError, TypeError):
                continue

        cutoff = now - max_lookback
        while deque_trades and deque_trades[0][0] < cutoff:
            deque_trades.popleft()
