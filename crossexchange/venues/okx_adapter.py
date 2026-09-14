"""
crossexchange/venues/okx_adapter.py
======================================
OKX Perpetual Swap WebSocket Order-Flow Adapter for cross-exchange consolidation.

Protocol & Architecture Details:
--------------------------------
1. Contract Multiplier (ctVal):
   - OKX perpetual swap order book and trade `sz` fields are expressed in NUMBER OF CONTRACTS,
     not raw base-asset units.
   - At startup and when discovering new symbols, this adapter fetches instrument details from
     REST endpoint GET /api/v5/public/instruments?instType=SWAP and calls
     `norm.CONTRACT_SPECS.set_multiplier('okx', canonical_symbol, float(ctVal))` before treating any
     book or trade numerical data as valid.

2. Order Book Stream & Sequence / Checksum Validation:
   - Channel: "books" (400 depth levels pushed as snapshot + incremental updates).
   - Subscribe payload: {"op": "subscribe", "args": [{"channel": "books", "instId": "BTC-USDT-SWAP"}]}
   - Sequence ID tracking:
     - Initial message action="snapshot" has prevSeqId = -1 and a new seqId.
     - Incremental messages action="update" contain prevSeqId and seqId.
     - Sequence gap is detected if update's prevSeqId != last seen seqId.
   - Checksum Validation Algorithm:
     - OKX computes a CRC32 checksum over the top 25 bid levels (sorted price descending)
       and top 25 ask levels (sorted price ascending).
     - Up to 25 levels from bids and asks are interleaved in price:size pairs separated by colons:
       "bids[0].px:bids[0].sz:asks[0].px:asks[0].sz:bids[1].px:bids[1].sz:asks[1].px:asks[1].sz:..."
     - The raw CRC32 unsigned 32-bit checksum is calculated over this string.
     - If sequence gap or checksum failure occurs, mark_gap() is called, book_valid becomes False,
       and an unsubscribe/resubscribe sequence is initiated for that symbol to force a fresh snapshot.

3. Keepalive & Reconnection:
   - Websocket endpoint: wss://ws.okx.com:8443/ws/v5/public
   - Text frame keepalive: Client sends string 'ping' every ~15-20s, OKX returns string 'pong'.
   - Disconnect handling: Exponential backoff with jitter derived from CROSSEXCHANGE_CONFIG.
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import random
import time
import zlib
from collections import deque
from typing import Dict, List, Optional, Set

import websockets
from sortedcontainers import SortedDict

try:
    import requests
except ImportError:
    requests = None

try:
    from .venue_state import VenueWSState
    from .base_adapter import VenueOrderFlowAdapter, NormalizedBookSnapshot, NormalizedTradeFlow
    from . import normalization as norm
    from . import canonical_symbols as csym
    from . import observability as obs
    from .cx_config import CROSSEXCHANGE_CONFIG
except (ImportError, ValueError):
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


class OkxOrderFlowAdapter(VenueOrderFlowAdapter):
    venue_name = "okx"

    def __init__(self):
        super().__init__()
        self._symbols: List[str] = []
        # canonical_symbol -> SortedDict({float_price: (q_float, sz_str, px_str)})
        self._bids: Dict[str, SortedDict] = {}
        self._asks: Dict[str, SortedDict] = {}
        # canonical_symbol -> deque([{'ts': float, 'side': str, 'usd': float}])
        self._trades: Dict[str, deque] = {}

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # Primary public endpoint (port 443 — reachable on restricted networks);
        # :8443 exists as an alternative but is blocked on some egress paths.
        self._ws_url = "wss://ws.okx.com/ws/v5/public"
        self._running = False
        self._main_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._recovery_in_flight: Set[str] = set()
        # Exponential resubscribe brake (2s -> 60s cap) so repeated gap/checksum
        # failures never hot-loop the connection.
        self._resub_attempts: Dict[str, int] = {}
        self._next_resubscribe_ts: Dict[str, float] = {}
        # Instruments that actually exist on OKX (from /api/v5/public/instruments).
        # None = not fetched yet (subscribe-all fallback); set = filter source
        # of truth that eliminates the 60018 "instId doesn't exist" error spam
        # for symbols OKX simply does not list (stock perps, renamed coins...).
        self._valid_instids: Optional[Set[str]] = None

    # ------------------------------------------------------------------
    # Lifecycle Methods
    # ------------------------------------------------------------------
    async def start(self, canonical_symbols: List[str]) -> None:
        """Begin OKX independent WS lifecycle.

        Fetches ctVal for all symbols via REST first, then spins up WS connection
        and keepalive tasks asynchronously via asyncio.create_task. Returns immediately.
        """
        if self._running:
            return
        self._running = True

        for sym in canonical_symbols:
            sym_upper = sym.upper()
            if sym_upper not in self._symbols:
                self._symbols.append(sym_upper)
                self._bids[sym_upper] = SortedDict()
                self._asks[sym_upper] = SortedDict()
                self._trades[sym_upper] = deque(maxlen=2000)
                st = self._state_for(sym_upper)
                st.mark_connecting()

        # Fetch contract multipliers (and the valid-instrument list) before
        # establishing WS feeds
        await self._fetch_contract_multipliers(self._symbols)
        listed, unlisted = self._okx_subscribable_symbols(self._symbols)
        if unlisted:
            obs.log_venue_event("okx", "*", "symbols_not_on_okx", count=len(unlisted),
                                sample=unlisted[:20])

        # Launch non-blocking background tasks
        self._main_task = asyncio.create_task(self._ws_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def stop(self) -> None:
        """Tear down OKX WebSocket connection and background loops."""
        self._running = False

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()

        if self._main_task and not self._main_task.done():
            self._main_task.cancel()

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        for sym in self._symbols:
            st = self._state_for(sym)
            st.mark_disconnected(reason="adapter_stopped")

        obs.log_venue_event("okx", "*", "adapter_stopped")

    async def ensure_symbols(self, canonical_symbols: List[str]) -> None:
        """Add/refresh subscriptions for additional canonical symbols dynamically."""
        new_symbols = []
        for sym in canonical_symbols:
            sym_upper = sym.upper()
            if sym_upper not in self._symbols:
                new_symbols.append(sym_upper)
                self._symbols.append(sym_upper)
                self._bids[sym_upper] = SortedDict()
                self._asks[sym_upper] = SortedDict()
                self._trades[sym_upper] = deque(maxlen=2000)
                st = self._state_for(sym_upper)
                st.mark_connecting()

        if not new_symbols:
            return

        await self._fetch_contract_multipliers(new_symbols)

        if _ws_is_open(self._ws):
            await self._subscribe_symbols(new_symbols)
        else:
            listed, unlisted = self._okx_subscribable_symbols(new_symbols)
            for sym in unlisted:
                self._state_for(sym).mark_disconnected(reason="instrument not listed on OKX")

    # ------------------------------------------------------------------
    # Data Accessors
    # ------------------------------------------------------------------
    def get_book_snapshot(self, canonical_symbol: str) -> Optional[NormalizedBookSnapshot]:
        """Returns a normalized USD book snapshot, or None if book is invalid/not ready."""
        sym = canonical_symbol.upper()
        st = self.get_state(sym)
        stale_after = CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0)

        if not st or not st.is_eligible_for_consolidation(stale_after_seconds=stale_after):
            return None

        bids_sd = self._bids.get(sym)
        asks_sd = self._asks.get(sym)
        if not bids_sd or not asks_sd:
            return None

        # SortedDict keys view supports positional indexing; bids are stored
        # ascending, so the best bid is the LAST key, the best ask the FIRST.
        if not len(bids_sd) or not len(asks_sd):
            return None
        best_bid_p = bids_sd.keys()[-1]
        best_ask_p = asks_sd.keys()[0]

        if best_bid_p is None or best_ask_p is None:
            return None

        best_bid = float(best_bid_p)
        best_ask = float(best_ask_p)

        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            return None

        mid = norm.venue_mid(best_bid, best_ask)
        if not mid:
            return None

        multiplier = norm.CONTRACT_SPECS.get_multiplier("okx", sym)
        bucket_size_bps = CROSSEXCHANGE_CONFIG.get("cx_bucket_size_bps", 5)
        max_bps_range = CROSSEXCHANGE_CONFIG.get("cx_max_bps_range", 100)

        raw_bids_levels = [(p, val[0]) for p, val in bids_sd.items()]
        raw_asks_levels = [(p, val[0]) for p, val in asks_sd.items()]

        bids_bps = norm.build_bps_depth(
            raw_bids_levels,
            mid,
            multiplier=multiplier,
            bucket_size_bps=bucket_size_bps,
            max_bps_range=max_bps_range,
        )
        asks_bps = norm.build_bps_depth(
            raw_asks_levels,
            mid,
            multiplier=multiplier,
            bucket_size_bps=bucket_size_bps,
            max_bps_range=max_bps_range,
        )

        return NormalizedBookSnapshot(
            venue="okx",
            canonical_symbol=sym,
            ts=st.last_message_ts or time.time(),
            venue_mid=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_bps=bids_bps,
            asks_bps=asks_bps,
            book_valid=True,
            sequence_meta=dict(st.sequence_state),
        )

    def get_trade_flow(
        self, canonical_symbol: str, *, lookback_seconds: float = 60.0
    ) -> Optional[NormalizedTradeFlow]:
        """Returns normalized rolling trade flow, or None if stream is disconnected/unsubscribed."""
        sym = canonical_symbol.upper()
        st = self.get_state(sym)
        if not st or not st.ws_connected or not st.ws_subscribed:
            return None

        trade_q = self._trades.get(sym)
        if trade_q is None:
            return None

        now = time.time()
        window_start = now - lookback_seconds
        window_end = now

        buy_usd = 0.0
        sell_usd = 0.0
        trade_count = 0

        for tr in trade_q:
            if tr["ts"] >= window_start:
                trade_count += 1
                if tr["side"] == "buy":
                    buy_usd += tr["usd"]
                elif tr["side"] == "sell":
                    sell_usd += tr["usd"]

        return NormalizedTradeFlow(
            venue="okx",
            canonical_symbol=sym,
            window_start=window_start,
            window_end=window_end,
            buy_notional_usd=buy_usd,
            sell_notional_usd=sell_usd,
            trade_count=trade_count,
            book_valid=st.book_valid,
        )

    # ------------------------------------------------------------------
    # REST Specification Fetching
    # ------------------------------------------------------------------
    async def _fetch_contract_multipliers(self, symbols: List[str]) -> None:
        """Populates norm.CONTRACT_SPECS by fetching ctVal from OKX REST API."""
        url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        headers = {"User-Agent": "OKXOrderFlowAdapter/1.0"}

        try:
            def _get():
                if requests is not None:
                    resp = requests.get(url, headers=headers, timeout=10.0)
                    if resp.status_code == 200:
                        return resp.json()
                    return None
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=10.0) as resp:
                        return json.loads(resp.read().decode())

            payload = await asyncio.to_thread(_get)
            if payload and payload.get("code") == "0":
                inst_map = {item.get("instId"): item for item in payload.get("data", [])}
                self._valid_instids = set(inst_map.keys())
                obs.log_venue_event("okx", "*", "instruments_fetched",
                                    instruments=len(self._valid_instids))
                for sym in symbols:
                    native = csym.from_canonical("okx", sym)
                    if native and native in inst_map:
                        item = inst_map[native]
                        ct_val_str = item.get("ctVal")
                        if ct_val_str:
                            try:
                                val = float(ct_val_str)
                                norm.CONTRACT_SPECS.set_multiplier("okx", sym, val)
                                obs.log_venue_event(
                                    "okx",
                                    sym,
                                    "contract_multiplier_set",
                                    multiplier=val,
                                    ctValCcy=item.get("ctValCcy"),
                                )
                            except ValueError:
                                obs.log_venue_event(
                                    "okx",
                                    sym,
                                    "contract_multiplier_error",
                                    error=f"Invalid ctVal: {ct_val_str}",
                                )
                    else:
                        obs.log_venue_event("okx", sym, "contract_multiplier_missing", native_symbol=native)
            else:
                obs.log_venue_event("okx", "*", "rest_instruments_error", code=payload.get("code") if payload else "HTTP_ERROR")
        except Exception as exc:
            obs.log_venue_event("okx", "*", "rest_instruments_exception", error=str(exc))

    # ------------------------------------------------------------------
    # WS Communication & Subscriptions
    # ------------------------------------------------------------------
    def _okx_subscribable_symbols(self, symbols: List[str]) -> tuple:
        """Split symbols into (listed, unlisted) using the fetched instrument set.

        Unlisted instruments previously produced a 60018 error per channel per
        symbol on every subscribe (140 errors in one session) and wasted
        subscribe capacity. When the instruments list is unavailable (REST
        failed), fall back to subscribing everything, exactly like before.
        """
        if self._valid_instids is None:
            return list(symbols), []
        listed, unlisted = [], []
        for sym in symbols:
            native = csym.from_canonical("okx", sym)
            if native and native in self._valid_instids:
                listed.append(sym)
            else:
                unlisted.append(sym)
        return listed, unlisted

    async def _subscribe_symbols(self, symbols: List[str]) -> None:
        if not _ws_is_open(self._ws) or not symbols:
            return

        listed, unlisted = self._okx_subscribable_symbols(symbols)
        for sym in unlisted:
            st = self._state_for(sym)
            st.mark_disconnected(reason="instrument not listed on OKX")
        if unlisted:
            obs.log_venue_event("okx", "*", "symbols_not_on_okx", count=len(unlisted),
                                sample=unlisted[:20])
        if not listed:
            return

        # Build args for listed instruments only, then send in paced batches
        # (OKX rejects oversized/over-fast subscribe bursts).
        args = []
        for sym in listed:
            native = csym.from_canonical("okx", sym)
            if native:
                args.append({"channel": "books", "instId": native})
                args.append({"channel": "trades", "instId": native})

        batch_size = 50
        try:
            for i in range(0, len(args), batch_size):
                batch = args[i:i + batch_size]
                msg = {"op": "subscribe", "args": batch}
                await self._ws.send(json.dumps(msg))
                if i + batch_size < len(args):
                    await asyncio.sleep(0.15)
            obs.log_venue_event("okx", "*", "subscribe_sent", count=len(listed),
                                skipped=len(unlisted), requests=(len(args) + batch_size - 1) // batch_size)
        except Exception as exc:
            obs.log_venue_event("okx", "*", "subscribe_failed", error=str(exc))

    async def _resubscribe_symbol(self, canonical_symbol: str) -> None:
        """Force resubscription of a single symbol to trigger a fresh snapshot."""
        if canonical_symbol in self._recovery_in_flight:
            return
        if time.time() < self._next_resubscribe_ts.get(canonical_symbol, 0.0):
            return

        self._recovery_in_flight.add(canonical_symbol)
        try:
            self._resub_attempts[canonical_symbol] = self._resub_attempts.get(canonical_symbol, 0) + 1
            delay = min(60.0, 2.0 * (2 ** min(self._resub_attempts[canonical_symbol], 5)))
            self._next_resubscribe_ts[canonical_symbol] = time.time() + delay
            native = csym.from_canonical("okx", canonical_symbol)
            if not native:
                return

            st = self._state_for(canonical_symbol)
            st.mark_recovery_started()
            obs.log_venue_event("okx", canonical_symbol, "recovery_started", reason=st.last_error)

            self._bids[canonical_symbol].clear()
            self._asks[canonical_symbol].clear()

            if _ws_is_open(self._ws):
                unsub_msg = {"op": "unsubscribe", "args": [{"channel": "books", "instId": native}]}
                sub_msg = {"op": "subscribe", "args": [{"channel": "books", "instId": native}]}
                try:
                    await self._ws.send(json.dumps(unsub_msg))
                    await asyncio.sleep(0.1)
                    await self._ws.send(json.dumps(sub_msg))
                    obs.log_venue_event("okx", canonical_symbol, "resubscribe_sent", native_symbol=native)
                except Exception as exc:
                    obs.log_venue_event("okx", canonical_symbol, "resubscribe_failed", error=str(exc))
        finally:
            self._recovery_in_flight.discard(canonical_symbol)

    # ------------------------------------------------------------------
    # WS Loops & Messaging
    # ------------------------------------------------------------------
    async def _ws_loop(self) -> None:
        init_backoff = CROSSEXCHANGE_CONFIG.get("cx_reconnect_initial_backoff_seconds", 1.0)
        max_backoff = CROSSEXCHANGE_CONFIG.get("cx_reconnect_max_backoff_seconds", 30.0)
        jitter = CROSSEXCHANGE_CONFIG.get("cx_reconnect_jitter_seconds", 0.5)

        attempt = 0
        while self._running:
            try:
                for sym in self._symbols:
                    st = self._state_for(sym)
                    if attempt > 0:
                        st.mark_reconnect_attempt()
                    else:
                        st.mark_connecting()

                obs.log_venue_event("okx", "*", "ws_connecting", attempt=attempt, url=self._ws_url)

                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,
                    ping_timeout=None,
                    open_timeout=15.0,
                    close_timeout=5.0,
                ) as ws:
                    self._ws = ws
                    attempt = 0

                    for sym in self._symbols:
                        st = self._state_for(sym)
                        st.mark_connected()

                    obs.log_venue_event("okx", "*", "ws_connected")
                    await self._subscribe_symbols(self._symbols)

                    # RECEIVE WATCHDOG: a silently-dead connection (no frames,
                    # no close frame) must not wedge the venue forever. The
                    # books/trades channels are high-frequency; 60s of total
                    # silence across ALL subscribed symbols means dead
                    # transport -> force reconnect.
                    while self._running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        except asyncio.TimeoutError:
                            obs.log_venue_event("okx", "*", "ws_stale", reason="no frames for 60s")
                            raise ConnectionError("okx receive watchdog fired")
                        self._on_ws_message(message)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                obs.log_venue_event("okx", "*", "ws_error", error=str(exc))
            finally:
                self._ws = None
                for sym in self._symbols:
                    st = self._state_for(sym)
                    st.mark_disconnected(reason="connection_closed")
                obs.log_venue_event("okx", "*", "ws_disconnected")

            if not self._running:
                break

            backoff = min(max_backoff, init_backoff * (2 ** attempt)) + random.uniform(0, jitter)
            attempt += 1
            obs.log_venue_event("okx", "*", "reconnect_started", attempt=attempt, backoff_seconds=round(backoff, 2))
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break

    async def _ping_loop(self) -> None:
        interval = CROSSEXCHANGE_CONFIG.get("cx_heartbeat_interval_seconds", 15.0)
        while self._running:
            try:
                await asyncio.sleep(interval)
                if _ws_is_open(self._ws):
                    await self._ws.send("ping")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                obs.log_venue_event("okx", "*", "ping_error", error=str(exc))

    def _on_ws_message(self, message: str) -> None:
        if message == "pong":
            for sym in self._symbols:
                st = self._state_for(sym)
                st.mark_heartbeat()
            return

        try:
            data = json.loads(message)
        except Exception:
            return

        event = data.get("event")
        if event == "subscribe":
            arg = data.get("arg", {})
            inst_id = arg.get("instId")
            sym = csym.to_canonical("okx", inst_id) if inst_id else None
            if sym:
                st = self._state_for(sym)
                st.mark_subscribed()
                obs.log_venue_event("okx", sym, "resubscribed", channel=arg.get("channel"))
            return
        elif event == "error":
            obs.log_venue_event("okx", "*", "ws_event_error", code=data.get("code"), msg=data.get("msg"))
            return

        arg = data.get("arg")
        if not arg or not isinstance(arg, dict):
            return

        channel = arg.get("channel")
        inst_id = arg.get("instId")
        if not inst_id:
            return

        canonical_symbol = csym.to_canonical("okx", inst_id)
        if not canonical_symbol or canonical_symbol not in self._bids:
            return

        if channel == "books":
            self._process_books_msg(canonical_symbol, data)
        elif channel == "trades":
            self._process_trades_msg(canonical_symbol, data)

    # ------------------------------------------------------------------
    # Message Processing & Integrity Checks
    # ------------------------------------------------------------------
    def _process_books_msg(self, canonical_symbol: str, msg: dict) -> None:
        data_list = msg.get("data")
        if not data_list or not isinstance(data_list, list):
            return

        data = data_list[0]
        action = msg.get("action")
        st = self._state_for(canonical_symbol)
        st.mark_message()

        raw_checksum = data.get("checksum")
        seq_id = int(data.get("seqId", -1))
        prev_seq_id = int(data.get("prevSeqId", -1))

        bids_data = data.get("bids", [])
        asks_data = data.get("asks", [])

        if action == "snapshot":
            self._bids[canonical_symbol].clear()
            self._asks[canonical_symbol].clear()

            for level in bids_data:
                px_str, sz_str = level[0], level[1]
                p, q = float(px_str), float(sz_str)
                if q > 0:
                    self._bids[canonical_symbol][p] = (q, sz_str, px_str)

            for level in asks_data:
                px_str, sz_str = level[0], level[1]
                p, q = float(px_str), float(sz_str)
                if q > 0:
                    self._asks[canonical_symbol][p] = (q, sz_str, px_str)

            st.sequence_state["seqId"] = seq_id

            # OKX populates `checksum` only on tbt channels; the plain `books`
            # feed sends checksum=0, which must be read as "not provided" and
            # skipped — never as a mismatch. seqId/prevSeqId continuity remains
            # the active integrity check in that case.
            if raw_checksum and not self._validate_checksum(canonical_symbol, raw_checksum):
                st.diag["checksum_failures"] += 1
                st.mark_gap(reason="checksum_failure_snapshot")
                obs.log_venue_event("okx", canonical_symbol, "checksum_failure", action="snapshot", seqId=seq_id)
                asyncio.create_task(self._resubscribe_symbol(canonical_symbol))
                return

            # Fresh snapshot applied cleanly: recovery done, brake reset.
            self._resub_attempts[canonical_symbol] = 0
            self._next_resubscribe_ts[canonical_symbol] = 0.0
            st.mark_book_ready()
            obs.log_venue_event("okx", canonical_symbol, "book_valid", action="snapshot", seqId=seq_id)

        elif action == "update":
            if not st.book_valid or not st.book_initialized or st.recovery_in_progress:
                return

            last_seq_id = st.sequence_state.get("seqId")

            if last_seq_id is not None and prev_seq_id != last_seq_id:
                st.diag["sequence_gaps"] += 1
                st.mark_gap(reason=f"sequence_gap: expected prevSeqId={last_seq_id}, got {prev_seq_id}")
                obs.log_venue_event(
                    "okx",
                    canonical_symbol,
                    "sequence_gap",
                    prevSeqId=prev_seq_id,
                    lastSeqId=last_seq_id,
                    seqId=seq_id,
                )
                asyncio.create_task(self._resubscribe_symbol(canonical_symbol))
                return

            for level in bids_data:
                px_str, sz_str = level[0], level[1]
                p, q = float(px_str), float(sz_str)
                if q == 0:
                    self._bids[canonical_symbol].pop(p, None)
                else:
                    self._bids[canonical_symbol][p] = (q, sz_str, px_str)

            for level in asks_data:
                px_str, sz_str = level[0], level[1]
                p, q = float(px_str), float(sz_str)
                if q == 0:
                    self._asks[canonical_symbol].pop(p, None)
                else:
                    self._asks[canonical_symbol][p] = (q, sz_str, px_str)

            st.sequence_state["seqId"] = seq_id

            if raw_checksum and not self._validate_checksum(canonical_symbol, raw_checksum):
                st.diag["checksum_failures"] += 1
                st.mark_gap(reason="checksum_failure_update")
                obs.log_venue_event("okx", canonical_symbol, "checksum_failure", action="update", seqId=seq_id)
                asyncio.create_task(self._resubscribe_symbol(canonical_symbol))
                return

    def _process_trades_msg(self, canonical_symbol: str, msg: dict) -> None:
        data_list = msg.get("data")
        if not data_list or not isinstance(data_list, list):
            return

        st = self._state_for(canonical_symbol)
        st.mark_message()

        multiplier = norm.CONTRACT_SPECS.get_multiplier("okx", canonical_symbol)

        for tr in data_list:
            try:
                px = float(tr["px"])
                sz = float(tr["sz"])
                side = str(tr.get("side", "")).lower()
                ts_ms = float(tr.get("ts", time.time() * 1000.0))
                ts = ts_ms / 1000.0
                notional = norm.notional_usd(px, sz, multiplier)

                self._trades[canonical_symbol].append({
                    "ts": ts,
                    "side": side,
                    "usd": notional,
                })
                st.last_trade_ts = ts
            except (ValueError, KeyError, TypeError):
                continue

        cutoff = time.time() - 600.0
        q = self._trades[canonical_symbol]
        while q and q[0]["ts"] < cutoff:
            q.popleft()

    # ------------------------------------------------------------------
    # CRC32 Checksum Algorithm
    # ------------------------------------------------------------------
    def _compute_checksum(self, canonical_symbol: str) -> int:
        bids_dict = self._bids.get(canonical_symbol)
        asks_dict = self._asks.get(canonical_symbol)
        if not bids_dict and not asks_dict:
            return 0

        # Top 25 bids (highest price first)
        top_bids = [v for k, v in reversed(bids_dict.items())][:25] if bids_dict else []
        # Top 25 asks (lowest price first)
        top_asks = [v for k, v in asks_dict.items()][:25] if asks_dict else []

        parts = []
        max_len = max(len(top_bids), len(top_asks))
        for i in range(max_len):
            if i < len(top_bids):
                # val is (q_float, sz_str, px_str)
                parts.append(f"{top_bids[i][2]}:{top_bids[i][1]}")
            if i < len(top_asks):
                parts.append(f"{top_asks[i][2]}:{top_asks[i][1]}")

        crc_str = ":".join(parts)
        raw_crc = zlib.crc32(crc_str.encode("utf-8"))
        # Return signed 32-bit int representation
        return ctypes.c_int32(raw_crc).value

    def _validate_checksum(self, canonical_symbol: str, expected_checksum: int) -> bool:
        computed = self._compute_checksum(canonical_symbol)
        return (computed & 0xFFFFFFFF) == (int(expected_checksum) & 0xFFFFFFFF)
