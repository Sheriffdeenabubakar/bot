import time

from signal_analyzer import (
    OrderFlowManager,
    OrderFlowAnalyzer,
    SIGNAL_CONFIG,
    MIN_SNAPSHOTS_REQUIRED,
)


def test_low_trade_notional():
    """Test B — Warm WS + healthy book + low trade notional -> thin_tape_notional failure"""
    mgr = OrderFlowManager()
    symbol = "TESTB"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # enough book snapshots
    for i in range(MIN_SNAPSHOTS_REQUIRED + 2):
        analyzer.book_history.append((now_ms - 1000 * i, {}))
    # trades present but notional below threshold
    analyzer.trade_history.append((now_ms - 1000, 0.1, 1.0, "buy"))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms - 5000, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol, lookback_seconds=60, event_lookback_seconds=60, required_stream_age_seconds=1)
    assert cov["ws_available"] is True
    assert cov["ws_warm"] is True
    assert cov["coverage_sufficient"] is False
    assert any("thin_tape_notional" in f for f in cov.get("trade_failures", []))


def test_missing_book_stream():
    """Test E — Missing book stream should mark book failures and coverage insufficient"""
    mgr = OrderFlowManager()
    symbol = "TESTE"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # no book history, some trades
    analyzer.trade_history.append((now_ms - 1000, 100.0, 1.0, "buy"))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms - 2000, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol, lookback_seconds=60, event_lookback_seconds=60, required_stream_age_seconds=1)
    assert cov["ws_available"] is True
    assert cov["coverage_sufficient"] is False
    assert any("missing_book_stream" in f or "stale_book_stream" in f or "book_coverage" in f for f in cov.get("book_failures", []))


def test_symbol_failure_inside_group():
    """Test H — Only stale symbol enters symbol-level recovery; healthy symbols remain connected"""
    mgr = OrderFlowManager()
    group_id = "G1"
    symbols = ["S1", "S2"]
    mgr.groups[group_id] = {"symbols": symbols, "task_done": False}

    now_ms = int(time.time() * 1000)
    # S1 stale
    a1 = OrderFlowAnalyzer("S1")
    a1.book_history.append((now_ms - 100000, {}))
    mgr.analyzers["S1"] = a1
    mgr.symbol_meta["S1"] = {"last_message_ms": now_ms - 100000, "subscribed": True}

    # S2 healthy
    a2 = OrderFlowAnalyzer("S2")
    a2.book_history.append((now_ms, {}))
    a2.trade_history.append((now_ms, 100.0, 1.0, "buy"))
    mgr.analyzers["S2"] = a2
    mgr.symbol_meta["S2"] = {"last_message_ms": now_ms, "subscribed": True}

    # Record gap for only S1
    mgr._record_gap_for_symbols(["S1"])

    # Ensure S2 still present and considered healthy (ws_available True)
    cov_s2 = mgr._coverage("S2")
    assert cov_s2["ws_available"] is True
    # G1 group should still exist and not be fully restarted (task_done False)
    assert group_id in mgr.groups
    assert mgr.groups[group_id]["task_done"] is False


def test_group_connection_failure_preserves_history():
    """Test I — Group disconnect records gaps, preserves symbol buffers"""
    mgr = OrderFlowManager()
    group_id = "G2"
    symbols = ["G2A", "G2B"]
    mgr.groups[group_id] = {"symbols": symbols, "task_done": False}

    now_ms = int(time.time() * 1000)
    for s in symbols:
        a = OrderFlowAnalyzer(s)
        for i in range(3):
            a.trade_history.append((now_ms - 1000 * (i + 1), 10.0, 1.0, "buy"))
            a.book_history.append((now_ms - 1000 * (i + 1), {}))
        mgr.analyzers[s] = a
        mgr.symbol_meta[s] = {"last_message_ms": now_ms - 500, "subscribed": True}

    # simulate group-level reconnect by recording gaps for all symbols
    mgr._record_gap_for_symbols(symbols)

    for s in symbols:
        gaps = mgr.symbol_gaps.get(s)
        assert gaps and len(gaps) >= 1
        # history preserved
        assert len(mgr.analyzers[s].trade_history) >= 3
        assert len(mgr.analyzers[s].book_history) >= 3


def test_thin_market_no_reconnect():
    """Test J — Thin market activity should not trigger reconnect; ws remains available, coverage insufficient"""
    mgr = OrderFlowManager()
    symbol = "TESTJ"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # healthy book but no trades
    for i in range(MIN_SNAPSHOTS_REQUIRED + 1):
        analyzer.book_history.append((now_ms - 1000 * i, {}))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms - 2000, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol)
    assert cov["ws_available"] is True
    assert cov["coverage_sufficient"] is False
    # no symbol_gaps should be created by coverage check alone
    assert not mgr.symbol_gaps.get(symbol)


def test_gap_intersects_lookback_blocks_confirmation():
    """Test K — Reconnect gap intersects required lookback -> coverage failure includes reconnect_gap_intersects_required_window"""
    mgr = OrderFlowManager()
    symbol = "TESTK"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # add history
    for i in range(5):
        analyzer.trade_history.append((now_ms - 1000 * (i + 1), 10.0, 1.0, "buy"))
        analyzer.book_history.append((now_ms - 1000 * (i + 1), {}))

    mgr.analyzers[symbol] = analyzer
    # ensure gap-failure config is enabled
    SIGNAL_CONFIG["of_fail_closed_on_gap"] = True
    # record a gap that intersects the event_cutoff for a 60s lookback
    start = now_ms - 30000
    end = now_ms - 10000
    mgr.symbol_gaps[symbol].append((start, end))
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms - 60000, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol, lookback_seconds=60, event_lookback_seconds=60)
    assert cov["coverage_sufficient"] is False
    # gap_hits should be recorded for this symbol
    assert cov.get("gap_hits") and len(cov.get("gap_hits")) >= 1


class _AliveTask:
    def done(self):
        return False

    def cancel(self):
        self.cancelled = True


def test_reconcile_does_not_drop_quiet_symbol_on_live_group():
    mgr = OrderFlowManager()
    symbol = "THINUSDT"
    now_ms = int(time.time() * 1000)
    mgr.analyzers[symbol] = OrderFlowAnalyzer(symbol)
    mgr.groups[1] = {
        "symbols": {symbol},
        "task": _AliveTask(),
        "task_done": False,
        "connected": True,
        "created_at_ms": now_ms - 600000,
    }
    mgr.symbol_to_group[symbol] = 1
    mgr.symbol_meta[symbol] = {
        "subscribed": True,
        "group_id": 1,
        "last_message_ms": now_ms - 120000,
        "stream_start_ms": now_ms - 600000,
    }
    mgr._reconcile_stale_subscription_state()
    assert mgr.symbol_to_group.get(symbol) == 1
    assert mgr.symbol_meta[symbol]["subscribed"] is True
    cov = mgr._coverage(symbol)
    assert cov["ws_available"] is True
    assert "symbol_not_subscribed" not in (cov.get("ws_failures") or [])


def test_dedupe_cancels_duplicate_group():
    mgr = OrderFlowManager()
    now_ms = int(time.time() * 1000)
    older = _AliveTask()
    newer = _AliveTask()
    mgr.groups[1] = {"symbols": {"BTCUSDT", "ETHUSDT"}, "task": older, "task_done": False, "created_at_ms": now_ms - 1000}
    mgr.groups[2] = {"symbols": {"BTCUSDT"}, "task": newer, "task_done": False, "created_at_ms": now_ms}
    mgr.symbol_to_group["BTCUSDT"] = 2
    overlaps = mgr._dedupe_overlapping_groups()
    assert overlaps >= 1
    assert "BTCUSDT" in mgr.groups[1]["symbols"]
    assert mgr.groups[2].get("task_done") is True


def test_trade_message_without_action_is_ingested():
    import asyncio

    analyzer = OrderFlowAnalyzer("BTCUSDT")
    payload = {
        "arg": {"instType": "USDT-FUTURES", "channel": "trade", "instId": "BTCUSDT"},
        "data": [{"ts": "1000", "price": "100", "size": "2", "side": "buy", "tradeId": "1"}],
    }
    asyncio.run(analyzer.handle_message(payload))
    assert len(analyzer.trade_history) == 1
    assert analyzer.trade_history[0][2] == 2.0


def test_assigned_group_stays_available_during_reconnect():
    mgr = OrderFlowManager()
    symbol = "BTCUSDT"
    now_ms = int(time.time() * 1000)
    analyzer = OrderFlowAnalyzer(symbol)
    for i in range(5):
        analyzer.book_history.append((now_ms - 1000 * i, 1.0, 1.0))
        analyzer.trade_history.append((now_ms - 1000 * i, 100.0, 1.0, "buy"))
    mgr.analyzers[symbol] = analyzer
    mgr.groups[1] = {
        "symbols": {symbol},
        "task": _AliveTask(),
        "task_done": False,
        "connected": False,
        "created_at_ms": now_ms - 120000,
    }
    mgr.symbol_to_group[symbol] = 1
    mgr.symbol_meta[symbol] = {
        "subscribed": False,
        "group_id": 1,
        "stream_start_ms": now_ms - 120000,
        "last_message_ms": now_ms - 2000,
    }
    cov = mgr._coverage(symbol, required_stream_age_seconds=60)
    assert cov["ws_available"] is True
    assert cov["ws_warm"] is True
    assert "symbol_not_subscribed" not in (cov.get("ws_failures") or [])


def test_books5_pseq_zero_does_not_resync():
    import asyncio

    analyzer = OrderFlowAnalyzer("BTCUSDT")
    snapshot = {
        "action": "snapshot",
        "arg": {"channel": "books5", "instId": "BTCUSDT"},
        "data": [{
            "ts": "1",
            "seq": 10,
            "pseq": 0,
            "bids": [["100", "1"]],
            "asks": [["101", "1"]],
        }],
    }
    update = {
        "action": "update",
        "arg": {"channel": "books5", "instId": "BTCUSDT"},
        "data": [{
            "ts": "2",
            "seq": 11,
            "pseq": 0,
            "bids": [["100.5", "2"]],
            "asks": [["101.5", "2"]],
        }],
    }
    asyncio.run(analyzer.handle_message(snapshot))
    asyncio.run(analyzer.handle_message(update))
    assert analyzer.needs_resync is False
    assert len(analyzer.book_history) >= 2
