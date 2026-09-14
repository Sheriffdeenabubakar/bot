"""
Live Bitget orderflow validation harness (public WS only, no keys, no trading).

Runs OrderFlowManager over a real symbol universe, promotes a rotating subset
to the focus tier so the incremental `books` path is exercised, and dumps
periodic diagnostics.
"""
import asyncio, json, logging, os, sys, time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "2base"))

import signal_analyzer as sa

DURATION = float(os.getenv("VALIDATION_MINUTES", "30")) * 60
UNIVERSE_SIZE = int(os.getenv("VALIDATION_SYMBOLS", "60"))
REPORT = "/home/ubuntu/bot/live_orderflow_validation.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("validation")


def universe(manager, n):
    valid = manager._fetch_valid_usdt_futures_inst_ids() or set()
    majors = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
              "ADAUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT"]
    picked = [s for s in majors if s in valid]
    picked += sorted(valid - set(picked))[: max(0, n - len(picked))]
    return picked[:n]


def diag_totals(manager):
    total = Counter()
    reasons = Counter()
    for a in manager.analyzers.values():
        for k, v in (a.ws_diag or {}).items():
            if isinstance(v, (int, float)):
                total[k] += int(v or 0)
            elif v:
                reasons[f"{k}={v}"] += 1
    out = dict(total)
    if reasons:
        out["reasons"] = dict(reasons)
    return out


def group_shape(manager):
    shape = []
    for gid, g in sorted(manager.groups.items()):
        shape.append({
            "gid": gid,
            "channel": manager._group_book_channel(g),
            "symbols": len(g.get("symbols") or ()),
            "connected": bool(g.get("connected")),
            "pending_release": sorted(g.get("pending_release") or ()),
        })
    return shape


def book_state(manager):
    depths, seq_ok, focus_rows = [], 0, []
    for sym in sorted(manager._focus_symbols):
        a = manager.analyzers.get(sym)
        if not a:
            continue
        focus_rows.append({
            "symbol": sym,
            "bid_levels": len(a.bids),
            "ask_levels": len(a.asks),
            "seq": a.seq,
            "needs_resync": bool(getattr(a, "needs_resync", False)),
            "updates": a.ws_diag.get("updates"),
            "gaps": a.ws_diag.get("sequence_gaps"),
            "resyncs": a.ws_diag.get("resync_requests"),
            "dupes": a.ws_diag.get("duplicate_trades"),
            "trades": len(a.trade_history),
        })
    for sym, a in manager.analyzers.items():
        depths.append(len(a.bids))
        if a.seq:
            seq_ok += 1
    return focus_rows, (sum(depths) / len(depths) if depths else 0.0), seq_ok


async def main():
    sa.SIGNAL_CONFIG["enable_websocket_orderflow_confirmation"] = True
    sa.SIGNAL_CONFIG["enable_continuous_orderflow_manager"] = True
    manager = sa.OrderFlowManager()

    symbols = await asyncio.to_thread(universe, manager, UNIVERSE_SIZE)
    log.info("universe: %d symbols, wide=%s focus=%s per-conn wide=%d focus=%d",
             len(symbols), manager._book_channel(), manager._focus_book_channel(),
             manager._symbols_per_connection(manager._book_channel()),
             manager._symbols_per_connection(manager._focus_book_channel()))

    await manager.ensure_symbols(symbols)

    started = time.time()
    timeline = []
    tick = 0
    try:
        while time.time() - started < DURATION:
            await asyncio.sleep(60)
            tick += 1
            # Rotate what the "signal layer" reads -> drives focus promotion.
            focus_batch = symbols[(tick * 4) % len(symbols):][:4] or symbols[:4]
            for sym in focus_batch:
                try:
                    await manager.snapshot(sym, trigger_type="breakout")
                except Exception as exc:
                    log.warning("snapshot %s failed: %s", sym, exc)
            manager.prune()
            await manager.ensure_symbols(symbols)

            focus_rows, avg_depth, seq_ok = book_state(manager)
            totals = diag_totals(manager)
            entry = {
                "minute": tick,
                "elapsed_s": round(time.time() - started, 1),
                "totals": totals,
                "groups": group_shape(manager),
                "focus": focus_rows,
                "avg_bid_levels": round(avg_depth, 1),
                "analyzers_with_seq": seq_ok,
                "analyzer_count": len(manager.analyzers),
            }
            timeline.append(entry)
            log.info("t+%dm totals=%s focus=%d groups=%s avg_bid_levels=%.1f",
                     tick, totals, len(manager._focus_symbols),
                     Counter(g["channel"] for g in entry["groups"]), avg_depth)
            with open(REPORT, "w") as fh:
                json.dump({"symbols": symbols, "timeline": timeline}, fh, indent=2, default=str)
    finally:
        try:
            health = manager.health_report(symbols, persist=False)
        except Exception as exc:
            health = {"error": str(exc)}
        with open(REPORT, "w") as fh:
            json.dump({"symbols": symbols, "timeline": timeline, "health": health},
                      fh, indent=2, default=str)
        await manager.shutdown()
        log.info("report written to %s", REPORT)


asyncio.run(main())
