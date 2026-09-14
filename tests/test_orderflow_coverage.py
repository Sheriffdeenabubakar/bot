import time

from signal_analyzer import (
    OrderFlowManager,
    OrderFlowAnalyzer,
    SIGNAL_CONFIG,
    MIN_SNAPSHOTS_REQUIRED,
)


def setup_module(module):
    # Make warmup short for tests
    SIGNAL_CONFIG["of_min_stream_age_seconds"] = 1
    SIGNAL_CONFIG["of_min_book_snapshots"] = MIN_SNAPSHOTS_REQUIRED
    SIGNAL_CONFIG["of_min_trades_per_snapshot"] = 8
    SIGNAL_CONFIG["of_min_trade_notional"] = 50.0
    SIGNAL_CONFIG["of_max_data_staleness_seconds"] = 10.0
    SIGNAL_CONFIG["of_fail_closed_on_thin_tape"] = True


def test_warm_ws_book_healthy_thin_tape():
    """Test A — Warm WS + healthy book + thin tape => coverage_sufficient False but ws_available/ws_warm True"""
    mgr = OrderFlowManager()
    symbol = "TESTA"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # populate book history with enough snapshots
    for i in range(MIN_SNAPSHOTS_REQUIRED + 1):
        analyzer.book_history.append((now_ms - 1000 * i, {}))
    # populate trades but fewer than min_trades and low notional
    analyzer.trade_history.append((now_ms - 1000, 1.0, 1.0, "buy"))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms - 2000, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol, lookback_seconds=60, event_lookback_seconds=60, required_stream_age_seconds=1)
    assert cov["ws_available"] is True
    assert cov["ws_warm"] is True
    assert cov["coverage_sufficient"] is False
    assert any("thin_tape_trades" in f for f in cov.get("trade_failures", [])) or any(
        "thin_tape_notional" in f for f in cov.get("trade_failures", [])
    )


def test_stream_not_warm():
    """Test F — Stream has not reached warmup => ws_warm False, ws_available True"""
    mgr = OrderFlowManager()
    symbol = "TESTF"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # book and trades present but stream_start is now (not warmed)
    analyzer.book_history.append((now_ms, {}))
    analyzer.trade_history.append((now_ms, 100.0, 1.0, "buy"))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"stream_start_ms": now_ms, "subscribed": True, "last_message_ms": now_ms}

    cov = mgr._coverage(symbol, lookback_seconds=60, event_lookback_seconds=60, required_stream_age_seconds=5)
    assert cov["ws_available"] is True
    assert cov["ws_warm"] is False
    assert any("stream_not_warm" in f for f in cov.get("ws_failures", []))


def test_record_gap_preserves_history():
    """Test G — Symbol reconnect should preserve historical buffers and record a gap"""
    mgr = OrderFlowManager()
    symbol = "TESTG"
    analyzer = OrderFlowAnalyzer(symbol)
    now_ms = int(time.time() * 1000)

    # add some history
    for i in range(5):
        analyzer.trade_history.append((now_ms - 1000 * (i + 1), 10.0, 1.0, "buy"))
        analyzer.book_history.append((now_ms - 1000 * (i + 1), {}))

    mgr.analyzers[symbol] = analyzer
    mgr.symbol_meta[symbol] = {"last_message_ms": now_ms - 500, "subscribed": True}

    trades_before = list(analyzer.trade_history)
    books_before = list(analyzer.book_history)

    mgr._record_gap_for_symbols([symbol])

    # buffers should be preserved
    assert list(analyzer.trade_history) == trades_before
    assert list(analyzer.book_history) == books_before

    # gap should be recorded
    gaps = mgr.symbol_gaps.get(symbol)
    assert gaps and len(gaps) >= 1
