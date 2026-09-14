# fix_orderflow.py — run once from anywhere; pass the bot folder as arg 1
import sys, os, py_compile

ROOT = sys.argv[1] if len(sys.argv) > 1 else r"G:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot"

# locate signal_analyzer.py anywhere under the folder
target = None
for r, d, files in os.walk(ROOT):
    if "signal_analyzer.py" in files:
        target = os.path.join(r, "signal_analyzer.py")
        break
assert target, f"signal_analyzer.py not found under {ROOT}"
print("Patching:", target)
p = target
src = open(p, encoding="utf-8").read()

def rep(old, new, label):
    global src
    n = src.count(old)
    assert n == 1, f"[{label}] matches={n}"
    src = src.replace(old, new)
    print("OK", label)

# 1) connection-cap headroom + new liveness config
rep('    "of_max_connections": 25,',
    '    "of_max_connections": 40,\n    "of_ws_stale_reconnect_seconds": 30,\n    "of_group_reap_seconds": 120,',
    "cap+config")

# 2) reset reconnect backoff after a successful subscribe
rep('''                self.logger.info(
                    "OrderFlowManager group %s subscribed %d symbols (%s channel)",
                    group_id,
                    len(symbols),
                    self._book_channel(),
                )''',
    '''                self.logger.info(
                    "OrderFlowManager group %s subscribed %d symbols (%s channel)",
                    group_id,
                    len(symbols),
                    self._book_channel(),
                )
                reconnect_attempt = 0''',
    "reset_attempt")

# 3) liveness-driven reconnect: if no frame (incl. pong) arrives within 30s, reconnect
rep('''                next_ping_at = time.time() + ping_interval_s
                while True:
                    if time.time() >= next_ping_at:
                        await ws.send("ping")
                        next_ping_at = time.time() + ping_interval_s
                    try:
                        msg = await asyncio.wait_for(
                            ws.recv(),
                            timeout=recv_poll_s,
                        )
                    except asyncio.TimeoutError:
                        continue

                    if msg == "ping":
                        try:
                            await ws.send("pong")
                        except Exception:
                            pass
                        continue
                    if msg == "pong":
                        continue''',
    '''                stale_reconnect_s = max(
                    5.0,
                    _safe_float(SIGNAL_CONFIG.get("of_ws_stale_reconnect_seconds"), 30.0) or 30.0,
                )
                last_recv_at = time.time()
                next_ping_at = time.time() + ping_interval_s
                while True:
                    if time.time() >= next_ping_at:
                        try:
                            await ws.send("ping")
                        except Exception:
                            raise RuntimeError(f"group {group_id} ping send failed")
                        next_ping_at = time.time() + ping_interval_s
                    try:
                        msg = await asyncio.wait_for(
                            ws.recv(),
                            timeout=recv_poll_s,
                        )
                    except asyncio.TimeoutError:
                        if (time.time() - last_recv_at) >= stale_reconnect_s:
                            raise RuntimeError(
                                f"group {group_id} stale: no frames in {time.time() - last_recv_at:.1f}s; reconnecting"
                            )
                        continue
                    last_recv_at = time.time()
                    if msg == "ping":
                        try:
                            await ws.send("pong")
                        except Exception:
                            pass
                        continue
                    if msg == "pong":
                        continue''',
    "liveness")

# 4) stale-group reaper: free slots from silently-dead groups so the cap stops filling
rep('''    def prune(self):
        # Dead-group sweep (orphans that finished while symbols were still flagged)
        for gid, g in list(self.groups.items()):
            if g.get("task_done"):
                for s in g.get("symbols", []):
                    self.symbol_to_group.pop(s, None)
                    meta = self.symbol_meta.get(s)
                    if isinstance(meta, dict):
                        meta["group_id"] = None
                self.groups.pop(gid, None)

        self._reconcile_stale_subscription_state()''',
    '''    def prune(self):
        # Dead-group sweep (orphans that finished while symbols were still flagged)
        for gid, g in list(self.groups.items()):
            if g.get("task_done"):
                for s in g.get("symbols", []):
                    self.symbol_to_group.pop(s, None)
                    meta = self.symbol_meta.get(s)
                    if isinstance(meta, dict):
                        meta["group_id"] = None
                self.groups.pop(gid, None)

        # Stale-group reaper: reclaim slots from groups whose every symbol has been
        # silent well past the liveness window (reconnect has already failed).
        try:
            reap_s = max(30.0, _safe_float(SIGNAL_CONFIG.get("of_group_reap_seconds"), 120.0) or 120.0)
        except Exception:
            reap_s = 120.0
        reap_deadline_ms = int(time.time() * 1000) - int(reap_s * 1000)
        for gid, g in list(self.groups.items()):
            if g.get("task_done"):
                continue
            created_ms = _safe_int(g.get("created_at_ms"), None)
            if created_ms is None or created_ms > reap_deadline_ms:
                continue
            syms = list(g.get("symbols", set()) or set())
            if not syms:
                continue
            all_stale = True
            for s in syms:
                m = self.symbol_meta.get(s) or {}
                last = _safe_int(m.get("last_message_ms"), None)
                if last is not None and last >= reap_deadline_ms:
                    all_stale = False
                    break
            if all_stale:
                task = g.get("task")
                if task is not None and not task.done():
                    task.cancel()
                g["task_done"] = True
                self.logger.warning(
                    "OrderFlowManager group %s reaped after %.0fs of silence across %d symbols; slot freed",
                    gid, reap_s, len(syms),
                )

        self._reconcile_stale_subscription_state()''',
    "reaper")

open(p, "w", encoding="utf-8").write(src)
py_compile.compile(p, doraise=True)
print("PATCHED + COMPILED OK:", p)