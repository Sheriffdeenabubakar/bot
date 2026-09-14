"""
Signal pattern analysis tool.
Analyzes which price-action patterns and confluence factors
are correlated with wins vs losses for a specific symbol.
"""

import os
import sys
import asyncio
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load .env file
load_dotenv()

from signal_analyzer import generate_trade_signal, fetch_and_prepare_data
from bitget_client import get_candlestick_data
from config import SIGNAL_CONFIG

logger = __import__('logging').getLogger(__name__)


async def analyze_signal_patterns(symbol, lookback_days=30, limit=200):
    """
    Analyzes which signal patterns/factors correlate with wins vs losses.
    """
    print(f"\n{'='*70}")
    print(f"SIGNAL PATTERN ANALYSIS: {symbol}")
    print('='*70)

    # Fetch data
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=lookback_days)

    df = await fetch_and_prepare_data(symbol, '15m', limit)
    if df is None:
        print(f"Failed to fetch data for {symbol}")
        return

    print(f"Analyzing {len(df)} candles from {df.index[0]} to {df.index[-1]}\n")

    # Collect signals and track them
    signals_with_outcomes = []

    for i in range(20, len(df), 10):  # Check every 10 candles
        hist_df = df.iloc[:i+1].copy()
        current_price = float(df.iloc[i]['close'])

        signal, error = await generate_trade_signal(symbol, df=hist_df, current_price=current_price)

        if signal and signal.get('direction') in ['BUY', 'SELL']:
            # Look ahead 10 candles to see if trade was profitable
            if i + 10 < len(df):
                future_price = float(df.iloc[i+10]['close'])
                entry = float(signal['entry_price'])

                if signal['direction'] == 'BUY':
                    pnl_pct = ((future_price - entry) / entry) * 100
                    win = pnl_pct > 0
                else:  # SELL
                    pnl_pct = ((entry - future_price) / entry) * 100
                    win = pnl_pct > 0

                rationale = signal.get('rationale', [])

                signals_with_outcomes.append({
                    'time': df.index[i],
                    'direction': signal['direction'],
                    'confidence': signal.get('confidence_score', 0),
                    'entry_price': entry,
                    'future_price': future_price,
                    'pnl_pct': pnl_pct,
                    'win': win,
                    'rationale_text': ' | '.join(rationale),
                    # Pattern detection from rationale text
                    'liquidity_sweep': any('sweep' in str(r).lower() for r in rationale),
                    'wyckoff': any('wyckoff' in str(r).lower() for r in rationale),
                    'breakout': any('breakout' in str(r).lower() for r in rationale),
                    'retest': any('retest' in str(r).lower() or 'rejection' in str(r).lower() for r in rationale),
                    'volume_spike': any('volume' in str(r).lower() for r in rationale),
                    'candlestick': any('candlestick' in str(r).lower() or 'pin bar' in str(r).lower() or 'engulfing' in str(r).lower() or 'hammer' in str(r).lower() for r in rationale),
                    'divergence': any('divergence' in str(r).lower() for r in rationale),
                    'fibonacci': any('fibonacci' in str(r).lower() for r in rationale),
                    'vwap': any('vwap' in str(r).lower() for r in rationale),
                })

    if not signals_with_outcomes:
        print("No signals generated. Strategy is too conservative for this symbol.")
        return

    results_df = pd.DataFrame(signals_with_outcomes)

    # Overall statistics
    total_signals = len(results_df)
    winning_signals = results_df['win'].sum()
    win_rate = (winning_signals / total_signals) * 100 if total_signals > 0 else 0
    avg_win = results_df[results_df['win']]['pnl_pct'].mean() if winning_signals > 0 else 0
    avg_loss = results_df[~results_df['win']]['pnl_pct'].mean() if (total_signals - winning_signals) > 0 else 0

    print(f"Total Signals:     {total_signals}")
    print(f"Winning Signals:   {winning_signals}")
    print(f"Losing Signals:    {total_signals - winning_signals}")
    print(f"Win Rate:          {win_rate:.1f}%")
    print(f"Avg Win:           {avg_win:+.2f}%")
    print(f"Avg Loss:          {avg_loss:+.2f}%")
    print(f"Avg Confidence:    {results_df['confidence'].mean():.1f}/100")

    # Pattern analysis
    print(f"\n{'='*70}")
    print("PATTERN WIN RATES (Which factors correlate with wins?)")
    print('='*70)

    patterns = ['liquidity_sweep', 'wyckoff', 'breakout', 'retest', 'volume_spike', 'candlestick', 'divergence', 'fibonacci', 'vwap']
    pattern_stats = []

    for pattern in patterns:
        pattern_signals = results_df[results_df[pattern] == True]
        if len(pattern_signals) > 0:
            pattern_win_rate = (pattern_signals['win'].sum() / len(pattern_signals)) * 100
            pattern_stats.append({
                'Pattern': pattern.replace('_', ' ').title(),
                'Frequency': f"{len(pattern_signals)}/{total_signals}",
                'Win Rate': f"{pattern_win_rate:.1f}%",
                'Avg P&L': f"{pattern_signals['pnl_pct'].mean():+.2f}%"
            })

    pattern_df = pd.DataFrame(pattern_stats)
    print(pattern_df.to_string(index=False))

    # Confidence analysis
    print(f"\n{'='*70}")
    print("CONFIDENCE SCORE ANALYSIS")
    print('='*70)

    confidence_bins = [0, 50, 60, 70, 80, 100]
    confidence_labels = ['<50', '50-60', '60-70', '70-80', '80+']

    results_df['conf_bin'] = pd.cut(results_df['confidence'], bins=confidence_bins, labels=confidence_labels)

    conf_stats = []
    for conf_range in confidence_labels:
        conf_signals = results_df[results_df['conf_bin'] == conf_range]
        if len(conf_signals) > 0:
            conf_win_rate = (conf_signals['win'].sum() / len(conf_signals)) * 100
            conf_stats.append({
                'Confidence': conf_range,
                'Count': len(conf_signals),
                'Win Rate': f"{conf_win_rate:.1f}%",
                'Avg P&L': f"{conf_signals['pnl_pct'].mean():+.2f}%"
            })

    conf_df = pd.DataFrame(conf_stats)
    print(conf_df.to_string(index=False))

    # Direction analysis
    print(f"\n{'='*70}")
    print("BUY vs SELL PERFORMANCE")
    print('='*70)

    for direction in ['BUY', 'SELL']:
        dir_signals = results_df[results_df['direction'] == direction]
        if len(dir_signals) > 0:
            dir_win_rate = (dir_signals['win'].sum() / len(dir_signals)) * 100
            print(f"{direction}: {len(dir_signals)} signals, {dir_win_rate:.1f}% win rate, {dir_signals['pnl_pct'].mean():+.2f}% avg P&L")

    # Export detailed results
    export_file = f"signal_analysis_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    results_df.to_csv(export_file, index=False)
    print(f"\nDetailed results exported to {export_file}")

    # Recommendations
    print(f"\n{'='*70}")
    print("RECOMMENDATIONS FOR IMPROVING WIN RATE")
    print('='*70)

    # Find best-performing pattern
    if pattern_stats:
        best_pattern = max(pattern_stats, key=lambda x: float(x['Win Rate'].rstrip('%')))
        print(f"✓ Best pattern: {best_pattern['Pattern']} ({best_pattern['Win Rate']})")

    # Find worst-performing pattern
    if pattern_stats:
        worst_pattern = min(pattern_stats, key=lambda x: float(x['Win Rate'].rstrip('%')))
        print(f"✗ Worst pattern: {worst_pattern['Pattern']} ({worst_pattern['Win Rate']})")

    # Confidence threshold
    high_conf = results_df[results_df['confidence'] >= 75]
    if len(high_conf) > 0:
        high_conf_wr = (high_conf['win'].sum() / len(high_conf)) * 100
        print(f"→ Setting min confidence to 75+ would give {high_conf_wr:.1f}% win rate ({len(high_conf)} signals)")

    print('='*70 + "\n")


if __name__ == '__main__':
    # Analyze each symbol
    symbols = ['ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'BNBUSDT']

    for symbol in symbols:
        try:
            asyncio.run(analyze_signal_patterns(symbol, lookback_days=30, limit=200))
        except Exception as e:
            print(f"Error analyzing {symbol}: {e}\n")
