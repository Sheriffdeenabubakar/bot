"""Short live probe of Bitget public WS trade/books5 frames."""
import asyncio
import json
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bitget_dns_fallback
bitget_dns_fallback.install()

import websockets

WS_URL = "wss://ws.bitget.com/v2/ws/public"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]


async def main():
    counts = {"trade": 0, "books5": 0, "subscribe": 0, "error": 0, "other": 0}
    samples = {"trade": [], "books5": [], "error": []}
    deadline = time.time() + 25
    async with websockets.connect(WS_URL, ping_interval=None, ping_timeout=None, open_timeout=20) as ws:
        args = []
        for symbol in SYMBOLS:
            args.append({"instType": "USDT-FUTURES", "channel": "trade", "instId": symbol})
            args.append({"instType": "USDT-FUTURES", "channel": "books5", "instId": symbol})
        await ws.send(json.dumps({"op": "subscribe", "args": args}))
        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue
            if raw in ("ping", "pong"):
                if raw == "ping":
                    await ws.send("pong")
                continue
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if not isinstance(msg, dict):
                continue
            evt = msg.get("event")
            if evt == "subscribe":
                counts["subscribe"] += 1
                continue
            if evt == "error":
                counts["error"] += 1
                if len(samples["error"]) < 5:
                    samples["error"].append(msg)
                continue
            channel = str((msg.get("arg") or {}).get("channel") or "")
            if channel in counts:
                counts[channel] += 1
                if len(samples[channel]) < 3:
                    samples[channel].append(
                        {
                            "keys": sorted(msg.keys()),
                            "action": msg.get("action"),
                            "arg": msg.get("arg"),
                            "data0_keys": sorted((msg.get("data") or [{}])[0].keys())
                            if isinstance(msg.get("data"), list) and msg.get("data") and isinstance(msg["data"][0], dict)
                            else type(msg.get("data")).__name__,
                            "data0": (msg.get("data") or [None])[0] if isinstance(msg.get("data"), list) else msg.get("data"),
                        }
                    )
            else:
                counts["other"] += 1
    print(json.dumps({"counts": counts, "samples": samples}, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
