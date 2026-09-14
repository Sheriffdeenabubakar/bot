"""
crossexchange/shadow_runner.py
=================================
Orchestrates all four venue adapters (Bitget read-only view + Binance + OKX
+ Bybit) and the ConsolidationEngine, in SHADOW MODE (spec section 19):

    Production (UNCHANGED):
        Bitget WS -> existing Bitget logic -> existing signal -> live execution

    Shadow (NEW, additive, this module):
        Bitget WS ─┐
        Binance WS ┤
        OKX WS     ┤-> normalization -> consolidation -> warm/coverage/confirm -> shadow signal
        Bybit WS   ┘

This module NEVER feeds into live execution. It only records how the
cross-exchange shadow decision compares to the existing Bitget-only decision,
to `live_cross_exchange_shadow_events.jsonl`.

Runs on its OWN dedicated background thread + asyncio event loop — mirroring
the existing, proven `_ORDERFLOW_BACKGROUND_THREAD` pattern already used for
the Bitget-only OrderFlowManager in signal_analyzer.py — so a crash, hang, or
reconnect storm in this layer can NEVER block or interfere with the main
trading loop or the Bitget WebSocket thread.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import Dict, List, Optional

from . import observability as obs
from .base_adapter import VenueOrderFlowAdapter
from .canonical_symbols import to_canonical
from .consolidation import ConsolidationEngine
from .cx_config import CROSSEXCHANGE_CONFIG

logger = logging.getLogger(__name__)


def _build_adapters() -> Dict[str, VenueOrderFlowAdapter]:
    adapters: Dict[str, VenueOrderFlowAdapter] = {}

    if CROSSEXCHANGE_CONFIG.get("enable_bitget_view", True):
        from .venues.bitget_view import BitgetOrderFlowView
        adapters["bitget"] = BitgetOrderFlowView()

    if CROSSEXCHANGE_CONFIG.get("enable_binance_ws", True):
        try:
            from .venues.binance_adapter import BinanceOrderFlowAdapter
            adapters["binance"] = BinanceOrderFlowAdapter()
        except Exception as exc:
            logger.warning("crossexchange: Binance adapter unavailable: %s", exc)

    if CROSSEXCHANGE_CONFIG.get("enable_okx_ws", True):
        try:
            from .venues.okx_adapter import OkxOrderFlowAdapter
            adapters["okx"] = OkxOrderFlowAdapter()
        except Exception as exc:
            logger.warning("crossexchange: OKX adapter unavailable: %s", exc)

    if CROSSEXCHANGE_CONFIG.get("enable_bybit_ws", True):
        try:
            from .venues.bybit_adapter import BybitOrderFlowAdapter
            adapters["bybit"] = BybitOrderFlowAdapter()
        except Exception as exc:
            logger.warning("crossexchange: Bybit adapter unavailable: %s", exc)

    return adapters


class CrossExchangeShadowManager:
    """Process-wide cross-exchange shadow orchestrator."""

    def __init__(self):
        self.adapters: Dict[str, VenueOrderFlowAdapter] = {}
        # Global venue-outage watchdog state (all venues down detection)
        self._outage = {"all_down_since": None, "last_alert_ts": 0.0}
        # Partial-degradation watchdog state (>=2 venues down, not all)
        self._degraded = {"since": None, "down": [], "last_alert_ts": 0.0}
        self._started_ts = 0.0  # startup grace for the outage watchdogs
        self.engine = ConsolidationEngine(CROSSEXCHANGE_CONFIG)
        self.canonical_symbols: List[str] = []
        self._eval_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_summary: dict = {}
        # v8.1.2: dedicated single-worker executor for the eval
        # sweep. The sweep is fully synchronous; running it ON
        # the event loop pegged the loop 10-30s+ per pass and
        # starved every WS handshake (Oracle/Colab opening-
        # handshake-timeout root cause). Connections now own the
        # loop; compute is delegated, exactly like the Bitget
        # OrderFlowManager background-thread pattern.
        self._eval_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="cx-eval")

    async def start(self, native_bitget_symbols: List[str]):
        if self._running:
            return
        self.adapters = _build_adapters()
        self.canonical_symbols = sorted({
            s for s in (to_canonical("bitget", sym) for sym in native_bitget_symbols) if s
        })
        max_symbols = int(CROSSEXCHANGE_CONFIG.get("cx_max_symbols", 25) or 25)
        if max_symbols > 0:
            self.canonical_symbols = self.canonical_symbols[:max_symbols]

        obs.log_consolidated_event(
            "*", "shadow_manager_starting",
            venues=list(self.adapters.keys()), symbol_count=len(self.canonical_symbols),
        )

        for venue, adapter in self.adapters.items():
            try:
                await adapter.start(self.canonical_symbols)
            except Exception as exc:
                # A failure starting ONE venue must never prevent the others
                # from starting or running.
                obs.log_venue_event(venue, "*", "start_failed", error=str(exc))
                logger.exception("crossexchange: %s adapter failed to start", venue)

        self._started_ts = time.time()
        self._running = True
        self._eval_task = asyncio.create_task(self._eval_loop())

    async def stop(self):
        self._running = False
        if self._eval_task is not None:
            self._eval_task.cancel()
            try:
                await self._eval_task
            except (asyncio.CancelledError, Exception):
                pass
            self._eval_task = None
        for venue, adapter in self.adapters.items():
            try:
                await adapter.stop()
            except Exception:
                logger.exception("crossexchange: %s adapter failed to stop cleanly", venue)
        try:
            self._eval_executor.shutdown(wait=False)
        except Exception:
            pass
        obs.log_consolidated_event("*", "shadow_manager_stopped")

    async def ensure_symbols(self, native_bitget_symbols: List[str]):
        new_canon = sorted({
            s for s in (to_canonical("bitget", sym) for sym in native_bitget_symbols) if s
        })
        max_symbols = int(CROSSEXCHANGE_CONFIG.get("cx_max_symbols", 25) or 25)
        merged = sorted(set(self.canonical_symbols) | set(new_canon))
        if max_symbols > 0:
            merged = merged[:max_symbols]
        self.canonical_symbols = merged
        for venue, adapter in self.adapters.items():
            try:
                await adapter.ensure_symbols(self.canonical_symbols)
            except Exception:
                logger.exception("crossexchange: %s ensure_symbols failed", venue)

    # ------------------------------------------------------------------
    async def _eval_loop(self):
        interval = float(CROSSEXCHANGE_CONFIG.get("cx_shadow_eval_interval_seconds", 10.0))
        while self._running:
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(self._eval_executor,
                                            self._evaluate_all_symbols)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("crossexchange: evaluation cycle failed")
            await asyncio.sleep(max(1.0, interval))

    def _evaluate_all_symbols(self):
        """Fully synchronous by design — runs on the dedicated
        cx-eval executor thread, NEVER on the event loop. Keep
        this method await-free; anything async belongs on the
        loop."""
        summary_rows = []
        venue_health = {venue: adapter.health_report() for venue, adapter in self.adapters.items()}

        # ── Global venue-outage watchdog ─────────────────────────────
        # A venue is DOWN when it tracks symbols but NONE are currently
        # eligible (fresh connected data). When EVERY enabled venue is down
        # at once, that is an environment-level outage (network/runtime),
        # not per-venue flapping — make it LOUD but rare: one alert line
        # every 5 minutes, plus a persistent marker in the venue health file.
        try:
            enabled = [
                v for v, a in self.adapters.items()
                if v == "bitget" or bool(CROSSEXCHANGE_CONFIG.get(f"cx_enable_{v}_ws", True))
            ]
            down = []
            for v in enabled:
                hr = venue_health.get(v) or {}
                tracked = int(hr.get("symbols_tracked") or 0)
                eligible = int(hr.get("eligible_count") or 0)
                if tracked > 0 and eligible == 0:
                    down.append(v)
            now = time.time()
            # Startup grace: books take up to ~2 min to warm after layer
            # start; alerting during that window is a false positive.
            in_grace = bool(self._started_ts) and (now - self._started_ts) < 180.0
            if enabled and len(down) == len(enabled):
                if self._outage["all_down_since"] is None:
                    self._outage["all_down_since"] = now
                dur = now - self._outage["all_down_since"]
                venue_health["outage"] = {
                    "all_venues_down": True,
                    "since": self._outage["all_down_since"],
                    "duration_seconds": round(dur, 1),
                    "down_venues": down,
                }
                if not in_grace and now - self._outage["last_alert_ts"] >= 300.0:
                    self._outage["last_alert_ts"] = now
                    obs.log_consolidated_event(
                        "*", "ALL_VENUES_DOWN",
                        duration_seconds=round(dur, 1), venues=down,
                    )
            else:
                if self._outage["all_down_since"] is not None and not in_grace:
                    obs.log_consolidated_event(
                        "*", "venues_recovered",
                        outage_seconds=round(now - self._outage["all_down_since"], 1),
                        still_down=down,
                    )
                self._outage["all_down_since"] = None
                self._outage["last_alert_ts"] = 0.0

            # ── Partial-degradation watchdog ──────────────────────────
            # >=2 venues (but not all) with zero eligible books for a
            # sustained period: the consolidated basis is running crippled
            # and deserves the same loud, rare alerts as a full outage.
            if len(down) >= 2 and len(down) < len(enabled):
                if self._degraded["since"] is None or self._degraded["down"] != down:
                    self._degraded["since"] = now
                    self._degraded["down"] = list(down)
                dur_d = now - self._degraded["since"]
                if not in_grace and now - self._degraded["last_alert_ts"] >= 300.0:
                    self._degraded["last_alert_ts"] = now
                    obs.log_consolidated_event(
                        "*", "VENUES_DEGRADED",
                        duration_seconds=round(dur_d, 1), down=down,
                        up=[v for v in enabled if v not in down],
                    )
            else:
                if self._degraded["since"] is not None and not in_grace:
                    obs.log_consolidated_event(
                        "*", "venues_degraded_recovered",
                        degraded_seconds=round(now - self._degraded["since"], 1),
                        down=down,
                    )
                self._degraded = {"since": None, "down": [], "last_alert_ts": 0.0}

            if down:
                venue_health["outage"] = {
                    "all_venues_down": len(enabled) > 0 and len(down) == len(enabled),
                    "down_venues": down,
                    "degraded_since": self._degraded["since"],
                }
        except Exception:
            logger.exception("crossexchange: outage watchdog failed (non-fatal)")

        obs.write_venue_health(venue_health)

        # Consolidated-coverage accounting (per cycle): how many symbols
        # have multi-venue, bitget-only, or no fresh venue coverage at all.
        coverage_stats = {"multi_venue": 0, "bitget_only": 0, "single_venue": 0, "no_venue": 0}
        fresh_age_acc: Dict[str, list] = {v: [] for v in self.adapters}

        for canonical_symbol in self.canonical_symbols:
            book_snapshots = {}
            trade_flows = {}
            eligibility = {}

            for venue, adapter in self.adapters.items():
                state = adapter.get_state(canonical_symbol)
                eligible = bool(state and state.is_eligible_for_consolidation(
                    stale_after_seconds=float(CROSSEXCHANGE_CONFIG.get("cx_stale_after_seconds", 15.0))
                ))
                eligibility[venue] = eligible
                if eligible and state is not None:
                    try:
                        fresh_age_acc[venue].append(
                            max(0.0, time.time() - float(state.last_message_ts or 0.0))
                        )
                    except Exception:
                        pass
                try:
                    book_snapshots[venue] = adapter.get_book_snapshot(canonical_symbol) if eligible else None
                except Exception:
                    book_snapshots[venue] = None
                try:
                    trade_flows[venue] = adapter.get_trade_flow(
                        canonical_symbol,
                        lookback_seconds=float(CROSSEXCHANGE_CONFIG.get("cx_confirmation_trade_lookback_seconds", 60.0)),
                    ) if eligible else None
                except Exception:
                    trade_flows[venue] = None

            n_eligible = sum(1 for e in eligibility.values() if e)
            if n_eligible >= 2:
                coverage_stats["multi_venue"] += 1
            elif n_eligible == 1:
                if eligibility.get("bitget"):
                    coverage_stats["bitget_only"] += 1
                else:
                    coverage_stats["single_venue"] += 1
            else:
                coverage_stats["no_venue"] += 1

            consolidated = self.engine.consolidate(
                track_history=True,
                canonical_symbol=canonical_symbol,
                book_snapshots=book_snapshots,
                trade_flows=trade_flows,
                venue_eligibility=eligibility,
            )

            bitget_only_decision = self._bitget_only_reference_decision(canonical_symbol)
            agreement = (
                bitget_only_decision == consolidated.consolidated_signal
                if bitget_only_decision is not None or consolidated.consolidated_signal is not None
                else True
            )

            row = consolidated.to_dict()
            row["bitget_only_decision"] = bitget_only_decision
            row["shadow_vs_bitget_only_agree"] = agreement
            summary_rows.append(row)

            if consolidated.consolidated_signal or bitget_only_decision:
                obs.record_shadow_decision(row)

        for _v, _ages in fresh_age_acc.items():
            _hr = venue_health.get(_v)
            if isinstance(_hr, dict) and _ages:
                _hr["fresh_count"] = len(_ages)
                _hr["avg_fresh_age_s"] = round(sum(_ages) / len(_ages), 2)
                _hr["max_fresh_age_s"] = round(max(_ages), 2)
        venue_health["consolidated"] = dict(coverage_stats)
        obs.write_venue_health(venue_health)

        self._last_summary = {
            "symbols_evaluated": len(summary_rows),
            "signals": [r for r in summary_rows if r.get("consolidated_signal")],
            "disagreements": [r for r in summary_rows if not r.get("shadow_vs_bitget_only_agree")],
        }
        obs.write_shadow_summary(self._last_summary)

    def _bitget_only_reference_decision(self, canonical_symbol: str) -> Optional[str]:
        """Latest Bitget-only reference decision for this symbol, read from a
        thread-safe cache populated by the MAIN thread's confirmation path.

        v8: this deliberately no longer calls analyzer.compute_metrics().
        The old cross-thread call (a) dragged every eval cycle out to
        30-60s, which starved the Bitget mirror past its 15s freshness gate
        so Bitget kept dropping out of consolidation, and (b) deadlocked the
        whole cross-exchange loop against locks held by the main thread
        (observed live: loop frozen with all venue reconnects stuck).
        The cache is written only by the main thread; this read is always
        safe. Entries older than 10 minutes are treated as stale."""
        try:
            from .canonical_symbols import from_canonical
            from signal_analyzer import get_orderflow_manager

            native_symbol = from_canonical("bitget", canonical_symbol)
            if not native_symbol:
                return None
            manager = get_orderflow_manager()
            if manager is None:
                return None
            snapshot = manager.get_reference_signals_snapshot()
            entry = snapshot.get(native_symbol) if isinstance(snapshot, dict) else None
            if not isinstance(entry, dict):
                return None
            if time.time() - float(entry.get("ts") or 0.0) > 600.0:
                return None
            return entry.get("signal")
        except Exception:
            return None

    def health_report(self) -> dict:
        return {
            "running": self._running,
            "venues": list(self.adapters.keys()),
            "symbol_count": len(self.canonical_symbols),
            "venue_health": {v: a.health_report() for v, a in self.adapters.items()},
            "last_summary": self._last_summary,
        }


# ---------------------------------------------------------------------------
# Dedicated background-thread lifecycle (mirrors signal_analyzer.py's own
# _ORDERFLOW_BACKGROUND_THREAD / _ORDERFLOW_BACKGROUND_LOOP pattern exactly,
# but as a FULLY SEPARATE thread+loop so it can never share fate with, or
# block, the Bitget-only background orderflow thread).
# ---------------------------------------------------------------------------
_CX_MANAGER: Optional[CrossExchangeShadowManager] = None
_CX_LOOP: Optional[asyncio.AbstractEventLoop] = None
_CX_THREAD: Optional[threading.Thread] = None
_CX_READY = threading.Event()

# ── Loop-liveness supervision ──────────────────────────────────────────
# The cross-exchange layer runs on its OWN asyncio loop in its own thread.
# If that loop ever stalls permanently (observed live: all venue reconnect
# coroutines froze mid-connect forever while the main bot kept running),
# nothing on that loop can detect or fix it. So:
#   * a heartbeat coroutine on the cx loop stamps a timestamp every 5s
#   * a supervisor DAEMON THREAD (outside the cx loop, outside asyncio)
#     checks the stamp; if it goes stale the supervisor rebuilds the whole
#     layer: fresh thread, fresh loop, fresh adapters, same symbols.
_CX_HEARTBEAT = {"ts": 0.0}
_CX_GENERATION = 0
_CX_SUPERVISOR_THREAD: Optional[threading.Thread] = None
_CX_SUPERVISOR_LOCK = threading.Lock()
_CX_LAST_START_SYMBOLS: List[str] = []


def cx_layer_generation() -> int:
    """Monotonic generation of the running cross-exchange layer. Bumped on
    every (re)start; lets cached providers detect they are stale."""
    return _CX_GENERATION


def get_shadow_manager() -> CrossExchangeShadowManager:
    global _CX_MANAGER
    if _CX_MANAGER is None:
        _CX_MANAGER = CrossExchangeShadowManager()
    return _CX_MANAGER


async def _cx_heartbeat_loop():
    """Liveness heartbeat for the cx loop — one timestamp every 5s."""
    while True:
        _CX_HEARTBEAT["ts"] = time.time()
        await asyncio.sleep(5.0)


def _thread_main():
    global _CX_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _CX_LOOP = loop
    _CX_HEARTBEAT["ts"] = time.time()
    try:
        loop.create_task(_cx_heartbeat_loop())
    except Exception:
        logger.exception("crossexchange: heartbeat task failed to schedule")
    _CX_READY.set()
    try:
        loop.run_forever()
    finally:
        loop.close()


def _cx_supervisor_loop():
    """Daemon watchdog OUTSIDE the cx event loop: if the cx loop's heartbeat
    goes stale (>180s) the loop is frozen and can never recover itself —
    rebuild the entire layer (max one rebuild per 5 minutes)."""
    global _CX_GENERATION
    last_restart_ts = 0.0
    while True:
        time.sleep(30.0)
        try:
            if not CROSSEXCHANGE_CONFIG.get("enable_cross_exchange_shadow", False):
                continue
            if _CX_THREAD is None or not _CX_THREAD.is_alive():
                continue
            hb = float(_CX_HEARTBEAT.get("ts") or 0.0)
            if hb <= 0.0:
                continue
            age = time.time() - hb
            if age > 180.0:
                obs.log_consolidated_event(
                    "*", "cx_loop_stalled", heartbeat_age_seconds=round(age, 1)
                )
                if time.time() - last_restart_ts >= 300.0:
                    last_restart_ts = time.time()
                    try:
                        _restart_cx_layer(stall_seconds=age)
                    except Exception:
                        logger.exception("crossexchange: layer restart failed")
        except Exception:
            logger.exception("crossexchange: supervisor cycle error (non-fatal)")


def _restart_cx_layer(stall_seconds: float) -> None:
    """Abandon a frozen cx thread/loop (daemon — it cannot be un-frozen) and
    build a fresh layer with the same symbol set."""
    global _CX_MANAGER, _CX_LOOP, _CX_THREAD, _CX_GENERATION
    with _CX_SUPERVISOR_LOCK:
        obs.log_consolidated_event(
            "*", "cx_layer_restart_triggered", stall_seconds=round(stall_seconds, 1)
        )
        # Best-effort flag so old loops exit if they ever resume.
        try:
            if _CX_MANAGER is not None:
                _CX_MANAGER._running = False
        except Exception:
            pass
        _CX_MANAGER = None
        _CX_LOOP = None
        _CX_THREAD = None
        # Drop the cached live provider so it re-binds to the new manager.
        try:
            from .live_provider import reset_live_provider
            reset_live_provider()
        except Exception:
            pass
        result = start_cross_exchange_shadow_background(list(_CX_LAST_START_SYMBOLS))
        obs.log_consolidated_event(
            "*",
            "cx_layer_restart_result",
            enabled=bool(result.get("enabled")) if isinstance(result, dict) else False,
            reason=str(result.get("reason")) if isinstance(result, dict) else str(result),
        )


def start_cross_exchange_shadow_background(native_bitget_symbols: List[str]) -> dict:
    """Synchronous, non-blocking entry point safe to call from main.py's
    synchronous startup section. No-op if the master feature flag is off."""
    global _CX_THREAD, _CX_GENERATION, _CX_SUPERVISOR_THREAD, _CX_LAST_START_SYMBOLS
    if not CROSSEXCHANGE_CONFIG.get("enable_cross_exchange_shadow", False):
        return {"enabled": False, "reason": "cross-exchange shadow disabled (enable_cross_exchange_shadow=False)"}

    _CX_LAST_START_SYMBOLS = list(native_bitget_symbols or [])
    _CX_GENERATION += 1

    if _CX_SUPERVISOR_THREAD is None or not _CX_SUPERVISOR_THREAD.is_alive():
        _CX_SUPERVISOR_THREAD = threading.Thread(
            target=_cx_supervisor_loop, name="crossexchange-supervisor", daemon=True
        )
        _CX_SUPERVISOR_THREAD.start()

    if _CX_THREAD is None or not _CX_THREAD.is_alive():
        _CX_READY.clear()
        _CX_THREAD = threading.Thread(target=_thread_main, name="crossexchange-shadow", daemon=True)
        _CX_THREAD.start()
        _CX_READY.wait(timeout=10.0)

    if _CX_LOOP is None:
        return {"enabled": False, "reason": "cross-exchange background loop failed to start"}

    timeout = float(CROSSEXCHANGE_CONFIG.get("cx_background_command_timeout_seconds", 15.0))
    try:
        future = asyncio.run_coroutine_threadsafe(
            get_shadow_manager().start(native_bitget_symbols), _CX_LOOP
        )
        future.result(timeout=timeout)
        return {"enabled": True, "symbol_count": len(native_bitget_symbols)}
    except Exception as exc:
        logger.exception("crossexchange: failed to start shadow manager")
        return {"enabled": False, "reason": f"start failed: {exc}"}


def stop_cross_exchange_shadow_background() -> None:
    global _CX_LOOP, _CX_THREAD
    if _CX_LOOP is None:
        return
    timeout = float(CROSSEXCHANGE_CONFIG.get("cx_background_command_timeout_seconds", 15.0))
    try:
        future = asyncio.run_coroutine_threadsafe(get_shadow_manager().stop(), _CX_LOOP)
        future.result(timeout=timeout)
    except Exception:
        logger.exception("crossexchange: error stopping shadow manager")
    try:
        _CX_LOOP.call_soon_threadsafe(_CX_LOOP.stop)
    except Exception:
        pass
    if _CX_THREAD is not None:
        _CX_THREAD.join(timeout=5.0)
    _CX_LOOP = None
    _CX_THREAD = None


async def _health_report_coro() -> dict:
    return get_shadow_manager().health_report()


def get_cross_exchange_health_report() -> dict:
    if not CROSSEXCHANGE_CONFIG.get("enable_cross_exchange_shadow", False):
        return {"enabled": False}
    if _CX_LOOP is None:
        return {"enabled": True, "running": False, "reason": "background loop not started"}
    timeout = float(CROSSEXCHANGE_CONFIG.get("cx_background_command_timeout_seconds", 15.0))
    try:
        future = asyncio.run_coroutine_threadsafe(_health_report_coro(), _CX_LOOP)
        return future.result(timeout=timeout)
    except Exception as exc:
        return {"enabled": True, "running": True, "error": str(exc)}
