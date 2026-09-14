from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Optional, Protocol

import pandas as pd


Direction = Literal["BUY", "SELL"]
ExitReason = Literal["stop_loss", "take_profit", "time_exit", "end_of_data"]
IntrabarPolicy = Literal["worst_case", "best_case", "stop_first", "target_first"]


@dataclass(slots=True)
class SignalDecision:
    direction: Direction
    confidence: float
    stop_loss: float
    take_profit: float
    max_holding_bars: int = 12
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BacktestConfig:
    initial_equity: float = 1000.0
    trade_notional: float = 100.0
    fee_rate: float = 0.0006
    slippage_bps: float = 2.0
    warmup_bars: int = 50
    cooldown_bars: int = 0
    intrabar_policy: IntrabarPolicy = "worst_case"


@dataclass(slots=True)
class TradeRecord:
    direction: Direction
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    quantity: float
    gross_pnl: float
    net_pnl: float
    gross_return_pct: float
    net_return_pct: float
    risk_multiple: float
    bars_held: int
    exit_reason: ExitReason
    confidence: float
    fees_paid: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BacktestResult:
    trades: list[TradeRecord]
    equity_curve: pd.DataFrame
    metrics: dict[str, float]


class StrategyProtocol(Protocol):
    def generate(self, history: pd.DataFrame) -> Optional[SignalDecision]:
        ...


@dataclass(slots=True)
class _PendingEntry:
    signal: SignalDecision
    requested_at: pd.Timestamp
    metadata: dict[str, Any]


@dataclass(slots=True)
class _OpenPosition:
    direction: Direction
    entry_time: pd.Timestamp
    entry_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    entry_bar: int
    max_holding_bars: int
    confidence: float
    metadata: dict[str, Any]


class BarBacktestEngine:
    """
    Generic event-driven backtester for OHLCV bar data.

    Rules:
    - Strategy sees history through the current bar close.
    - Orders are entered on the next bar open.
    - Exits are decided from later bar high/low/close.
    - Costs are modeled on both entry and exit.
    """

    REQUIRED_COLUMNS = ("open", "high", "low", "close")

    def __init__(self, config: BacktestConfig):
        self.config = config

    def run(self, df: pd.DataFrame, strategy: StrategyProtocol) -> BacktestResult:
        data = self._prepare_frame(df)
        equity = float(self.config.initial_equity)
        equity_points: list[dict[str, float | pd.Timestamp]] = []
        trades: list[TradeRecord] = []

        position: Optional[_OpenPosition] = None
        pending_entry: Optional[_PendingEntry] = None
        cooldown_until_bar = -1

        for bar_index in range(len(data)):
            timestamp = data.index[bar_index]
            bar = data.iloc[bar_index]

            if pending_entry is not None and position is None:
                position = self._open_position(
                    signal=pending_entry.signal,
                    bar=bar,
                    timestamp=timestamp,
                    bar_index=bar_index,
                )
                pending_entry = None

            if position is not None:
                exit_reason, exit_price = self._scan_exit(position, bar, bar_index)
                if exit_reason is not None and exit_price is not None:
                    trade = self._close_position(
                        position=position,
                        exit_time=timestamp,
                        exit_price=exit_price,
                        exit_reason=exit_reason,
                        bar_index=bar_index,
                    )
                    trades.append(trade)
                    equity += trade.net_pnl
                    position = None
                    cooldown_until_bar = bar_index + self.config.cooldown_bars

            equity_points.append({"time": timestamp, "equity": equity})

            can_generate_signal = (
                bar_index >= self.config.warmup_bars
                and bar_index < len(data) - 1
                and position is None
                and pending_entry is None
                and bar_index >= cooldown_until_bar
            )
            if can_generate_signal:
                history = data.iloc[: bar_index + 1].copy()
                signal = strategy.generate(history)
                if signal is not None:
                    self._validate_signal(signal)
                    pending_entry = _PendingEntry(
                        signal=signal,
                        requested_at=timestamp,
                        metadata=dict(signal.metadata),
                    )

        if position is not None:
            last_time = data.index[-1]
            last_close = float(data.iloc[-1]["close"])
            trade = self._close_position(
                position=position,
                exit_time=last_time,
                exit_price=self._apply_exit_slippage(last_close, position.direction),
                exit_reason="end_of_data",
                bar_index=len(data) - 1,
            )
            trades.append(trade)
            equity += trade.net_pnl
            equity_points[-1]["equity"] = equity

        equity_curve = pd.DataFrame(equity_points).set_index("time")
        metrics = self._calculate_metrics(trades, equity_curve)
        return BacktestResult(trades=trades, equity_curve=equity_curve, metrics=metrics)

    def _prepare_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [column for column in self.REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"Missing required OHLC columns: {missing}")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex.")

        data = df.copy().sort_index()
        for column in self.REQUIRED_COLUMNS:
            data[column] = pd.to_numeric(data[column], errors="raise")
        return data

    def _validate_signal(self, signal: SignalDecision) -> None:
        if signal.direction not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported direction: {signal.direction}")
        if signal.stop_loss <= 0 or signal.take_profit <= 0:
            raise ValueError("Stop loss and take profit must be positive.")
        if signal.max_holding_bars <= 0:
            raise ValueError("max_holding_bars must be positive.")

    def _open_position(
        self,
        signal: SignalDecision,
        bar: pd.Series,
        timestamp: pd.Timestamp,
        bar_index: int,
    ) -> _OpenPosition:
        entry_price = self._apply_entry_slippage(float(bar["open"]), signal.direction)
        quantity = self.config.trade_notional / entry_price
        return _OpenPosition(
            direction=signal.direction,
            entry_time=timestamp,
            entry_price=entry_price,
            stop_loss=float(signal.stop_loss),
            take_profit=float(signal.take_profit),
            quantity=quantity,
            entry_bar=bar_index,
            max_holding_bars=signal.max_holding_bars,
            confidence=float(signal.confidence),
            metadata=dict(signal.metadata),
        )

    def _scan_exit(
        self,
        position: _OpenPosition,
        bar: pd.Series,
        bar_index: int,
    ) -> tuple[Optional[ExitReason], Optional[float]]:
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        bars_held = (bar_index - position.entry_bar) + 1

        if position.direction == "BUY":
            stop_hit = low <= position.stop_loss
            target_hit = high >= position.take_profit
        else:
            stop_hit = high >= position.stop_loss
            target_hit = low <= position.take_profit

        if stop_hit and target_hit:
            return self._resolve_intrabar_collision(position)
        if stop_hit:
            return "stop_loss", self._apply_exit_slippage(position.stop_loss, position.direction)
        if target_hit:
            return "take_profit", self._apply_exit_slippage(position.take_profit, position.direction)
        if bars_held >= position.max_holding_bars:
            return "time_exit", self._apply_exit_slippage(close, position.direction)
        return None, None

    def _resolve_intrabar_collision(self, position: _OpenPosition) -> tuple[ExitReason, float]:
        policy = self.config.intrabar_policy
        if policy == "best_case":
            return "take_profit", self._apply_exit_slippage(position.take_profit, position.direction)
        if policy == "target_first":
            return "take_profit", self._apply_exit_slippage(position.take_profit, position.direction)
        return "stop_loss", self._apply_exit_slippage(position.stop_loss, position.direction)

    def _close_position(
        self,
        position: _OpenPosition,
        exit_time: pd.Timestamp,
        exit_price: float,
        exit_reason: ExitReason,
        bar_index: int,
    ) -> TradeRecord:
        quantity = position.quantity
        entry_notional = position.entry_price * quantity
        exit_notional = exit_price * quantity
        fees_paid = (entry_notional + exit_notional) * self.config.fee_rate

        if position.direction == "BUY":
            gross_pnl = (exit_price - position.entry_price) * quantity
            gross_return_pct = ((exit_price / position.entry_price) - 1.0) * 100.0
            initial_risk = max(position.entry_price - position.stop_loss, 1e-9)
            risk_multiple = (exit_price - position.entry_price) / initial_risk
        else:
            gross_pnl = (position.entry_price - exit_price) * quantity
            gross_return_pct = (gross_pnl / entry_notional) * 100.0
            initial_risk = max(position.stop_loss - position.entry_price, 1e-9)
            risk_multiple = (position.entry_price - exit_price) / initial_risk

        net_pnl = gross_pnl - fees_paid
        net_return_pct = (net_pnl / entry_notional) * 100.0
        bars_held = (bar_index - position.entry_bar) + 1

        return TradeRecord(
            direction=position.direction,
            entry_time=position.entry_time,
            exit_time=exit_time,
            entry_price=round(position.entry_price, 10),
            exit_price=round(exit_price, 10),
            quantity=round(quantity, 10),
            gross_pnl=round(gross_pnl, 10),
            net_pnl=round(net_pnl, 10),
            gross_return_pct=round(gross_return_pct, 10),
            net_return_pct=round(net_return_pct, 10),
            risk_multiple=round(risk_multiple, 10),
            bars_held=bars_held,
            exit_reason=exit_reason,
            confidence=position.confidence,
            fees_paid=round(fees_paid, 10),
            metadata=dict(position.metadata),
        )

    def _apply_entry_slippage(self, price: float, direction: Direction) -> float:
        slippage = self.config.slippage_bps / 10_000.0
        if direction == "BUY":
            return price * (1.0 + slippage)
        return price * (1.0 - slippage)

    def _apply_exit_slippage(self, price: float, direction: Direction) -> float:
        slippage = self.config.slippage_bps / 10_000.0
        if direction == "BUY":
            return price * (1.0 - slippage)
        return price * (1.0 + slippage)

    def _calculate_metrics(
        self,
        trades: list[TradeRecord],
        equity_curve: pd.DataFrame,
    ) -> dict[str, float]:
        if not trades:
            return {
                "trade_count": 0.0,
                "win_rate_pct": 0.0,
                "profit_factor": 0.0,
                "expectancy_r": 0.0,
                "avg_net_return_pct": 0.0,
                "avg_bars_held": 0.0,
                "max_drawdown_pct": 0.0,
                "ending_equity": float(self.config.initial_equity),
                "total_return_pct": 0.0,
            }

        trade_frame = pd.DataFrame([asdict(trade) for trade in trades])
        wins = trade_frame[trade_frame["net_pnl"] > 0]
        losses = trade_frame[trade_frame["net_pnl"] <= 0]
        gross_profit = float(wins["net_pnl"].sum()) if not wins.empty else 0.0
        gross_loss = abs(float(losses["net_pnl"].sum())) if not losses.empty else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        running_peak = equity_curve["equity"].cummax()
        drawdown = (equity_curve["equity"] - running_peak) / running_peak
        ending_equity = float(equity_curve["equity"].iloc[-1])

        return {
            "trade_count": float(len(trades)),
            "win_rate_pct": float((trade_frame["net_pnl"] > 0).mean() * 100.0),
            "profit_factor": float(profit_factor),
            "expectancy_r": float(trade_frame["risk_multiple"].mean()),
            "avg_net_return_pct": float(trade_frame["net_return_pct"].mean()),
            "avg_bars_held": float(trade_frame["bars_held"].mean()),
            "max_drawdown_pct": float(drawdown.min() * 100.0),
            "ending_equity": ending_equity,
            "total_return_pct": float(((ending_equity / self.config.initial_equity) - 1.0) * 100.0),
        }
