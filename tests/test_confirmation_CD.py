from collections import deque

from signal_analyzer import OrderFlowAnalyzer, _evaluate_path_orderflow_confirmation


def test_pressure_is_complementary():
    analyzer = OrderFlowAnalyzer('TEST')
    analyzer.trade_history = deque([
        (1, 100.0, 60.0, 'buy'),
        (2, 100.0, 40.0, 'sell'),
    ])
    analyzer.book_history = deque([
        (1, 90.0, 10.0),
        (2, 80.0, 20.0),
        (3, 70.0, 30.0),
    ])

    metrics = analyzer.compute_metrics()
    assert abs(metrics['buy_pressure'] + metrics['sell_pressure'] - 100.0) < 1e-9
    assert abs(metrics['buy_pressure'] - 60.0) < 1e-9
    assert abs(metrics['sell_pressure'] - 40.0) < 1e-9


def test_imbalance_passes():
    """Test C — Warm WS + sufficient coverage + imbalance passes"""
    mode_profile = {"orderflow_gate_pressure_min": 60, "orderflow_gate_imbalance_min": 0.03}
    fresh_of = {"buy_pressure": 60.0, "sell_pressure": 40.0, "imbalance": 0.0548, "imbalance_slope_per_snapshot": 0.0}
    res = _evaluate_path_orderflow_confirmation(trigger_type='breakout', direction='BUY', fresh_of=fresh_of, mode_profile=mode_profile, price_place=2)
    # imbalance meets threshold
    assert float(res.get('imbalance', 0.0)) >= 0.03
    # votes or score should reflect supportive imbalance
    assert res.get('of_votes', 0) >= 1


def test_pressure_fails_confirmation():
    """Test D — Warm WS + sufficient coverage + pressure fails"""
    mode_profile = {"orderflow_gate_pressure_min": 60, "orderflow_gate_imbalance_min": 0.03}
    fresh_of = {"buy_pressure": 51.0, "sell_pressure": 49.0, "imbalance": 0.02, "imbalance_slope_per_snapshot": 0.0}
    res = _evaluate_path_orderflow_confirmation(trigger_type='breakout', direction='BUY', fresh_of=fresh_of, mode_profile=mode_profile, price_place=2)
    # pressure below threshold -> of_votes should be low or zero
    assert res.get('of_votes', 0) <= 1
    # if imbalance below threshold, no strong support
    assert abs(float(res.get('imbalance', 0.0))) < 0.03
