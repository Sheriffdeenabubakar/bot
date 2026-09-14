"""
crossexchange/daemon.py
========================
Standalone daemon process for the cross-exchange orderflow engine.
Runs in its own OS process (with its own Python interpreter and GIL),
decoupling WebSocket network I/O and multi-venue consolidation from
main bot execution loops.

Publishes atomic snapshots to shared memory (/dev/shm/2base_cx/ or local IPC cache).
"""

import sys
import os
import time
import json
import logging
import asyncio
from pathlib import Path

# Ensure repo root is on sys.path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crossexchange.cx_config import CROSSEXCHANGE_CONFIG
from crossexchange.shadow_runner import CrossExchangeShadowManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CX_DAEMON] %(message)s",
)
logger = logging.getLogger("crossexchange.daemon")

CACHE_DIR = Path("/dev/shm/2base_cx") if Path("/dev/shm").exists() else Path("/tmp/2base_cx")


def publish_ipc_state(manager: CrossExchangeShadowManager):
    """Periodically publish snapshot state to fast local IPC RAM directory."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        summary = manager.health_report()

        with open(CACHE_DIR / "health.json.tmp", "w") as f:
            json.dump(summary, f)
        (CACHE_DIR / "health.json.tmp").replace(CACHE_DIR / "health.json")
    except Exception as e:
        logger.warning(f"Failed to publish IPC state: {e}")


async def main():
    logger.info("Starting CrossExchange Daemon process...")

    # Load symbol universe dynamically
    symbols = []
    try:
        from config import SYMBOLS
        symbols = list(SYMBOLS)
    except Exception as e:
        logger.warning(f"Could not load SYMBOLS from config: {e}")

    if not symbols:
        # Fallback default symbol list if config import is unavailable
        symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
            "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "PEPEUSDT", "NEARUSDT"
        ]

    logger.info(f"Loaded {len(symbols)} native symbols into CrossExchange daemon")

    manager = CrossExchangeShadowManager()
    await manager.start(symbols)
    logger.info("CrossExchange manager started successfully in daemon process")

    try:
        while True:
            publish_ipc_state(manager)
            await asyncio.sleep(2.0)
    except asyncio.CancelledError:
        logger.info("Daemon loop cancelled")
    finally:
        await manager.stop()
        logger.info("CrossExchange daemon stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon interrupted by user")
