"""
Background maintenance for OrderFlowManager.

Moves expensive snapshot compute off the WebSocket receive loop and maintains
a continuously refreshed per-symbol snapshot cache that signal generation reads
without waiting on the manager.

Architecture (fixes the timeout root cause):
  - Buffer CLONING happens on the manager's event loop (serialized with
    handle_message) -> no torn reads, no lock needed.
  - Heavy COMPUTE (metrics/footprint/absorption/liquidity/queue) happens in a
    bounded ThreadPool over the private clone -> never blocks the WS loop.
  - prune() runs periodically on the loop, NOT per snapshot request.
  - Cache is keyed by (symbol, trigger_type) so a sweep request never reads a
    breakout-window snapshot.

Usage (started automatically by start_orderflow_background_manager):
  from orderflow_maintenance import start_orderflow_maintenance, get_cached_snapshot
"""

import time
import asyncio
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import namedtuple

logger = logging.getLogger(__name__)

# (symbol, trigger_type) -> {'ts': float, 'snapshot': dict}
_ORDERFLOW_SNAPSHOT_CACHE = {}

MaintenanceHandle = namedtuple('MaintenanceHandle', ['stop'])


async def _run_sync_on_loop(fn, *args, **kwargs):
    """Run a sync callable on the event loop (serialized with WS handlers)."""
    return fn(*args, **kwargs)


def get_cached_snapshot(symbol, trigger_type="breakout"):
    """Return the latest cached snapshot for (symbol, trigger_type) or None.

    Returned object: {'ts': float, 'snapshot': dict} or None
    """
    return _ORDERFLOW_SNAPSHOT_CACHE.get((symbol, trigger_type))


async def _refresh_wave(
    manager,
    *,
    refresh_triggers,
    lookback_seconds,
    event_lookback_seconds,
    key_levels,
):
    """One cache refresh wave on the WS loop, yielding so recv is not starved."""
    updated = {}
    symbols = list(getattr(manager, "analyzers", {}).keys())
    for idx, sym in enumerate(symbols):
        for trig in refresh_triggers:
            try:
                snap = await manager.snapshot(
                    sym,
                    trigger_type=trig,
                    lookback_seconds=lookback_seconds,
                    event_lookback_seconds=event_lookback_seconds,
                    key_levels=key_levels,
                )
                if isinstance(snap, dict):
                    updated[(sym, trig)] = {"ts": time.time(), "snapshot": snap}
            except Exception as exc:
                logger.debug("maintenance snapshot %s/%s failed: %s", sym, trig, exc)
        if idx % 4 == 3:
            await asyncio.sleep(0)
    return updated


def start_orderflow_maintenance(
    manager,
    *,
    loop,
    snapshot_interval=1.0,
    prune_interval=30.0,
    worker_threads=4,
    batch_size=200,
    refresh_triggers=("breakout", "sweep"),
    lookback_seconds=None,
    event_lookback_seconds=None,
    key_levels=None,
):
    """Start background snapshot refresh + periodic prune.

    - snapshot_interval: seconds between refresh waves
    - prune_interval: seconds between prune() calls (run on the loop)
    - worker_threads: unused here; manager owns the compute executor
    - refresh_triggers: trigger types to refresh per symbol each wave
    - batch_size: kept for API compatibility

    Returns a handle with .stop() to terminate the background thread.
    """
    stop_event = threading.Event()
    snapshot_interval = max(2.0, float(snapshot_interval or 5.0))
    wave_timeout = max(60.0, snapshot_interval * 20)

    def _worker():
        logger.info(
            "OrderFlow maintenance starting: snapshot_interval=%s prune_interval=%s triggers=%s",
            snapshot_interval, prune_interval, refresh_triggers,
        )
        last_prune = 0.0
        try:
            while not stop_event.is_set():
                wave_start = time.time()
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        _refresh_wave(
                            manager,
                            refresh_triggers=refresh_triggers,
                            lookback_seconds=lookback_seconds,
                            event_lookback_seconds=event_lookback_seconds,
                            key_levels=key_levels,
                        ),
                        loop,
                    )
                    updated = fut.result(timeout=wave_timeout)
                    if isinstance(updated, dict):
                        _ORDERFLOW_SNAPSHOT_CACHE.update(updated)
                except Exception:
                    logger.exception("Unhandled exception in snapshot refresh wave")

                now = time.time()
                if prune_interval and (now - last_prune) >= prune_interval:
                    try:
                        fut = asyncio.run_coroutine_threadsafe(_run_sync_on_loop(manager.prune), loop)
                        fut.result(timeout=10.0)
                        last_prune = now
                    except Exception as e:
                        logger.debug("maintenance prune failed: %s", e)

                elapsed = time.time() - wave_start
                stop_event.wait(max(0.0, snapshot_interval - elapsed))
        finally:
            logger.info("OrderFlow maintenance thread exiting")

    thread = threading.Thread(target=_worker, name="orderflow-maintenance", daemon=True)
    thread.start()

    def _stop():
        stop_event.set()
        thread.join(timeout=5.0)

    return MaintenanceHandle(stop=_stop)