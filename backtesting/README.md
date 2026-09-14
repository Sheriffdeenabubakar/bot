# Fresh Backtesting Baseline

This folder is a clean restart after removing the legacy backtest files.
It is intentionally small and opinionated.

## Design rules
- Research mode must use historical data only.
- Strategy logic must be pure and side-effect free.
- Entries happen on the next bar open after a signal.
- Stops and targets are evaluated with later bar high and low.
- Fees and slippage are always included.
- Results must be segmented by symbol, regime, session, and direction.

## What this baseline gives us
- A generic event-driven engine in `engine.py`.
- A strict interface for strategy output.
- Path-dependent exits with stop loss, take profit, and time stop.
- Cost modeling without touching live exchange code.

## What this baseline does not do
- It does not call Bitget.
- It does not use the old wall-clock signal cache.
- It does not place real trades.
- It does not assume the current `signal_analyzer.py` is safe for research mode.

## Recommended integration path
1. Build a research-only adapter that converts historical features into `SignalDecision`.
2. Keep funding and depth disabled until those features are stored historically at signal time.
3. Add experiment tags to each signal so results can be grouped by rule set.
4. Run walk-forward tests before tuning thresholds.

## Minimal example
```python
import pandas as pd

from backtesting.engine import BarBacktestEngine, BacktestConfig, SignalDecision


class ExampleStrategy:
    def generate(self, history: pd.DataFrame):
        last = history.iloc[-1]
        if last["close"] > last["open"]:
            return SignalDecision(
                direction="BUY",
                confidence=60.0,
                stop_loss=float(last["close"]) * 0.99,
                take_profit=float(last["close"]) * 1.015,
                max_holding_bars=12,
                metadata={"rule_set": "example"},
            )
        return None


config = BacktestConfig(
    initial_equity=1000.0,
    trade_notional=100.0,
    fee_rate=0.0006,
    slippage_bps=2.0,
    warmup_bars=50,
)

engine = BarBacktestEngine(config)
results = engine.run(df, ExampleStrategy())
print(results.metrics)
```

## Next step for this project
The next real step is not threshold tuning.
It is building a pure adapter around the current strategy so we can test the structure without live-only leakage.
