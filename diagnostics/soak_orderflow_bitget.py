"""Live Bitget soak for OrderFlowManager. Does not place trades."""
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

os.environ["ENABLE_LIVE_TRADING"] = "False"

import bitget_dns_fallback
bitget_dns_fallback.install()

from scanner import scan_coins
from signal_analyzer import (
    get_orderflow_manager,
    prepare_orderflow_for_symbols,
    get_orderflow_health_report,
    shutdown_orderflow_manager,
    SIGNAL_CONFIG,
)
import asyncio

OUT_DIR = Path(__file__).resolve().parent
SNAPSHOT_PATH = OUT_DIR / "orderflow_soak_health.json"
SUMMARY_PATH = OUT_DIR / "orderflow_soak_summary.json"


def _slim(health):
    return {
        "updated_at": health.get("updated_at"),
        "prepared": health.get("prepared"),
        "warm_count": health.get("warm_count"),
        "not_ready_count": health.get("not_ready_count"),
        "subscribed_count": health.get("subscribed_count"),
        "coverage_ok_count": health.get("coverage_ok_count"),
        "active_groups": health.get("active_groups"),
        "duplicate_symbol_assignments": health.get("duplicate_symbol_assignments"),
        "failure_counts": health.get("failure_counts"),
        "stale_trade_count": health.get("stale_trade_count"),
        "stale_book_count": health.get("stale_book_count"),
        "avg_latest_trade_age_seconds": health.get("avg_latest_trade_age_seconds"),
        "avg_latest_book_age_seconds": health.get("avg_latest_book_age_seconds"),
        "sample_not_ready": health.get("sample_not_ready"),
        "group_count": len(health.get("groups") or []),
    }


async def main():
    duration_s = float(os.getenv("OF_SOAK_SECONDS", "2100"))
    poll_s = float(os.getenv("OF_SOAK_POLL_SECONDS", "60"))
    SIGNAL_CONFIG["enable_websocket_orderflow_confirmation"] = True
    SIGNAL_CONFIG["enable_continuous_orderflow_manager"] = True
    SIGNAL_CONFIG["enable_orderflow_background_thread"] = True

    coins = scan_coins()
    symbols = list(dict.fromkeys(coin["symbol"] for coin in coins))
    print(f"scanner symbols={len(symbols)} soak={duration_s:.0f}s", flush=True)
    warmup = await prepare_orderflow_for_symbols(symbols, wait_for_warmup=True)
    print(f"warmup={json.dumps(warmup, default=str)}", flush=True)

    started = time.time()
    history = []
    while True:
        health = await get_orderflow_health_report(symbols)
        slim = _slim(health if isinstance(health, dict) else {})
        slim["elapsed_seconds"] = round(time.time() - started, 1)
        history.append(slim)
        SNAPSHOT_PATH.write_text(json.dumps(health, indent=2, default=str), encoding="utf-8")
        SUMMARY_PATH.write_text(json.dumps({"history": history}, indent=2, default=str), encoding="utf-8")
        print(
            "SOAK "
            f"t={slim['elapsed_seconds']:.0f}s warm={slim.get('warm_count')}/{slim.get('prepared')} "
            f"sub={slim.get('subscribed_count')} cov_ok={slim.get('coverage_ok_count')} "
            f"groups={slim.get('active_groups')} dups={slim.get('duplicate_symbol_assignments')} "
            f"fail={slim.get('failure_counts')}",
            flush=True,
        )
        if time.time() - started >= duration_s:
            break
        await asyncio.sleep(poll_s)

    mgr = get_orderflow_manager()
    mgr.prune()
    final = await get_orderflow_health_report(symbols)
    SUMMARY_PATH.write_text(
        json.dumps({"history": history, "final": _slim(final if isinstance(final, dict) else {})}, indent=2, default=str),
        encoding="utf-8",
    )
    await shutdown_orderflow_manager()
    print("soak complete", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
