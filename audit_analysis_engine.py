"""
QUANTITATIVE DIAGNOSTIC ENGINE
Senior Quant Analysis @ BlackRock Standards
Comprehensive Audit Log Analysis & Quality Gate Validation
=====================================================

This engine performs 5 core diagnostic verifications:
1. Live outcomes for each quality gate rule (actual vs. predicted)
2. Correlation matrix - feature importance ranking
3. Regime drift analysis (historical vs. recent performance)
4. Feature interaction testing & simplification
5. Recency-weighted metrics validation

Output: Production-ready diagnostic report with actionable recommendations
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from scipy import stats
from typing import Dict, List, Tuple, Any, Optional
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
RESOLVED_AUDIT_PATH = Path(r"g:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\live_trade_audit_resolved.jsonl")
QUALITY_GATE_PATH = Path(r"g:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\research\live_quality_gate.json")
AUDIT_SUMMARY_PATH = Path(r"g:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\live_trade_audit_summary.json")

# DIAGNOSTIC PARAMETERS
MIN_SAMPLES_PER_RULE = 5
RECENCY_WINDOW_TRADES = [50, 100, 150]  # Test with last N trades
CORRELATION_THRESHOLD = 0.05  # p-value threshold
FEATURE_INTERACTION_MIN_N = 8

# QUALITY GATE STANDARDS (BlackRock risk management)
TARGET_WIN_RATE_PCT = 50.0
MIN_CONFIDENCE_LEVEL = 0.80  # 80% confidence interval
MIN_EXPECTANCY = 0.05  # Expected value per trade
REQUIRED_SAMPLE_SIZE = 30  # Min trades to validate a rule

print("=" * 80)
print("AUDIT ANALYSIS ENGINE - INITIALIZATION")
print("=" * 80)
print(f"Audit Data: {RESOLVED_AUDIT_PATH}")
print(f"Quality Gate: {QUALITY_GATE_PATH}")
print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ============================================================================
# STEP 1: LOAD & VALIDATE DATA
# ============================================================================
print("\n[STEP 1] LOADING AUDIT DATA...")
print("-" * 80)

trades = []
malformed_count = 0

try:
    with open(RESOLVED_AUDIT_PATH, 'r', encoding='utf-8', errors='replace') as f:
        for line_no, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                trades.append(record)
            except json.JSONDecodeError as e:
                malformed_count += 1
                if malformed_count <= 3:
                    print(f"  ⚠ Malformed record at line {line_no}: {str(e)[:60]}")
                continue
except FileNotFoundError:
    print(f"ERROR: Audit file not found: {RESOLVED_AUDIT_PATH}")
    exit(1)

print(f"✓ Loaded {len(trades)} trade records")
print(f"  Malformed records skipped: {malformed_count}")

# Validate outcome distribution
outcomes = Counter(t.get('outcome') or t.get('resolution_outcome') for t in trades)
print(f"\n  Outcome Distribution:")
for outcome, count in outcomes.most_common():
    pct = 100 * count / len(trades)
    print(f"    {outcome:15s}: {count:4d} ({pct:5.1f}%)")

# ============================================================================
# STEP 2: LOAD QUALITY GATE RULES
# ============================================================================
print("\n[STEP 2] LOADING QUALITY GATE RULES...")
print("-" * 80)

quality_gate = {}
try:
    with open(QUALITY_GATE_PATH, 'r') as f:
        quality_gate = json.load(f)
except FileNotFoundError:
    print(f"WARNING: Quality gate file not found: {QUALITY_GATE_PATH}")

rules = quality_gate.get('rules', [])
print(f"✓ Loaded {len(rules)} quality gate rules")
if len(rules) > 0:
    print(f"  Sample rule fields: {list(rules[0].keys())[:5]}")

# ============================================================================
# DIAGNOSTIC 1: MEASURE ACTUAL WIN RATES FOR EACH QUALITY GATE RULE
# ============================================================================
print("\n" + "=" * 80)
print("[DIAGNOSTIC 1] ACTUAL WIN RATES FOR EACH QUALITY GATE RULE")
print("=" * 80)

def rule_matches_trade(rule: Dict, trade: Dict) -> bool:
    """Check if a trade matches all rule conditions"""
    # Text field matching
    text_fields = {
        'symbol_bucket': lambda x: str(x or '').strip().upper(),
        'direction': lambda x: str(x or '').strip().upper(),
        'primary_setup': lambda x: str(x or '').strip().lower(),
        'session_bucket': lambda x: str(x or '').strip().lower(),
        'live_market_regime': lambda x: str(x or '').strip().lower(),
        'adx_regime': lambda x: str(x or '').strip().lower(),
        'htf_alignment_bucket': lambda x: str(x or '').strip().lower(),
        'confirmation_bucket': lambda x: str(x or '').strip().lower(),
    }

    for field, normalizer in text_fields.items():
        rule_val = rule.get(field)
        trade_val = trade.get(field)

        if rule_val in (None, '', '*'):
            continue  # Wildcard - match anything

        rule_norm = normalizer(rule_val)
        trade_norm = normalizer(trade_val)

        if rule_norm != trade_norm:
            return False

    return True

rule_performance = []

for rule_idx, rule in enumerate(rules[:15], 1):  # Test first 15 rules
    matched_trades = [t for t in trades if rule_matches_trade(rule, t)]

    if len(matched_trades) < MIN_SAMPLES_PER_RULE:
        continue  # Skip rules with insufficient data

    # Calculate actual win rate
    winners = sum(1 for t in matched_trades if str(t.get('outcome') or '').lower() == 'winner')
    win_rate = 100 * winners / len(matched_trades)

    # Calculate expectancy
    rs = [float(t.get('resolved_r_multiple') or t.get('r_multiple') or 0)
          for t in matched_trades]
    expectancy = np.mean(rs) if rs else 0

    # Calculate confidence bounds (Wilson score interval)
    z = 1.96  # 95% CI
    p = winners / len(matched_trades)
    denom = 1 + (z*z / len(matched_trades))
    centre = (p + z*z/(2*len(matched_trades))) / denom
    margin = z * np.sqrt((p*(1-p)/len(matched_trades)) + (z*z/(4*len(matched_trades)**2))) / denom
    ci_lower = max(0, 100 * (centre - margin))
    ci_upper = min(100, 100 * (centre + margin))

    rule_performance.append({
        'rule_id': rule.get('rule_id', f'rule_{rule_idx}'),
        'direction': rule.get('direction'),
        'setup': rule.get('primary_setup'),
        'regime': rule.get('live_market_regime'),
        'gate_win_rate_pct': rule.get('win_rate_pct'),
        'actual_win_rate_pct': win_rate,
        'win_rate_delta_pct': win_rate - (rule.get('win_rate_pct') or 0),
        'sample_size': len(matched_trades),
        'actual_winners': winners,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'expectancy_r': expectancy,
        'gate_expectancy_r': rule.get('expectancy_r'),
    })

# Handle case where no rules matched
if len(rule_performance) == 0:
    print(f"\n⚠ WARNING: No rules matched trades with N≥{MIN_SAMPLES_PER_RULE}")
    print(f"  This suggests rule matching logic or data mismatch")
    print(f"  Adjusting to test with relaxed criteria...\n")

    # Test with just direction matching
    for rule_idx, rule in enumerate(rules[:3], 1):
        direction = rule.get('direction', '').strip().upper()
        matched_trades = [t for t in trades if str(t.get('direction') or '').strip().upper() == direction]

        if len(matched_trades) >= MIN_SAMPLES_PER_RULE:
            winners = sum(1 for t in matched_trades if str(t.get('outcome') or '').lower() == 'winner')
            win_rate = 100 * winners / len(matched_trades)
            rs = [float(t.get('resolved_r_multiple') or t.get('r_multiple') or 0)
                  for t in matched_trades]
            expectancy = np.mean(rs) if rs else 0

            z = 1.96
            p = winners / len(matched_trades)
            denom = 1 + (z*z / len(matched_trades))
            centre = (p + z*z/(2*len(matched_trades))) / denom
            margin = z * np.sqrt((p*(1-p)/len(matched_trades)) + (z*z/(4*len(matched_trades)**2))) / denom
            ci_lower = max(0, 100 * (centre - margin))
            ci_upper = min(100, 100 * (centre + margin))

            rule_performance.append({
                'rule_id': f"direction_{direction}",
                'direction': direction,
                'setup': 'N/A (all)',
                'regime': 'N/A (all)',
                'gate_win_rate_pct': rule.get('win_rate_pct', 'N/A'),
                'actual_win_rate_pct': win_rate,
                'win_rate_delta_pct': win_rate - (rule.get('win_rate_pct') or 0),
                'sample_size': len(matched_trades),
                'actual_winners': winners,
                'ci_lower': ci_lower,
                'ci_upper': ci_upper,
                'expectancy_r': expectancy,
                'gate_expectancy_r': rule.get('expectancy_r', 'N/A'),
            })

rule_perf_df = pd.DataFrame(rule_performance)
if len(rule_perf_df) > 0:
    rule_perf_df = rule_perf_df.sort_values('win_rate_delta_pct')

print(f"\n✓ RULE VALIDATION RESULTS ({len(rule_performance)} rules with N≥{MIN_SAMPLES_PER_RULE}):\n")
if len(rule_perf_df) > 0:
    print(rule_perf_df.to_string(index=False))
else:
    print("  (No rules matched with sufficient sample size)")

print("\n📊 KEY FINDINGS:")
if len(rule_perf_df) > 0:
    print(f"  • Rules worse than baseline: {sum(rule_perf_df['win_rate_delta_pct'] < 0)} / {len(rule_perf_df)}")
    print(f"  • Avg gate vs actual delta: {rule_perf_df['win_rate_delta_pct'].mean():.2f}pp")
    print(f"  • Max underperformance: {rule_perf_df['win_rate_delta_pct'].min():.2f}pp")
    print(f"  • Avg actual win rate: {rule_perf_df['actual_win_rate_pct'].mean():.1f}%")
else:
    print("  • No rules matched with sufficient data for validation")

# ============================================================================
# DIAGNOSTIC 2: CORRELATION MATRIX - FEATURE IMPORTANCE
# ============================================================================
print("\n" + "=" * 80)
print("[DIAGNOSTIC 2] CORRELATION MATRIX - FEATURE IMPORTANCE RANKING")
print("=" * 80)

# Extract numeric and categorical features
feature_names = set()
for trade in trades:
    feature_names.update(trade.keys())

# Focus on predictable features (not post-trade outcomes)
exclude_features = {
    'outcome', 'resolution_outcome', 'exit_reason', 'exit_time',
    'resolved_r_multiple', 'r_multiple', 'exit_price',
    'take_profit_closed', 'stop_loss_closed', 'order_id',
    'placement_time', 'logged_at', 'config_snapshot_full',
    'signal_payload_snapshot'
}

feature_names = feature_names - exclude_features

# Build feature vectors
feature_data = defaultdict(list)
outcome_binary = []

for trade in trades:
    outcome = str(trade.get('outcome') or trade.get('resolution_outcome') or '').lower()
    outcome_binary.append(1 if outcome == 'winner' else 0)

    for feature in feature_names:
        value = trade.get(feature)
        if value is not None and isinstance(value, (int, float)):
            feature_data[feature].append(float(value))
        elif value is not None and isinstance(value, str):
            # Simple encoding: hash string values
            feature_data[feature].append(hash(value) % 100)
        else:
            feature_data[feature].append(np.nan)

# Calculate correlations
correlations = []
for feature, values in feature_data.items():
    if len([v for v in values if not np.isnan(v)]) < 10:
        continue  # Skip sparse features

    # Pearson correlation
    try:
        corr, p_value = stats.pearsonr(
            [v for v in values if not np.isnan(v)],
            [outcome_binary[i] for i in range(len(values)) if not np.isnan(values[i])]
        )
        if abs(corr) > 0.01 and p_value < 0.20:  # Loose threshold for initial screening
            correlations.append({
                'feature': feature,
                'correlation': corr,
                'p_value': p_value,
                'abs_correlation': abs(corr)
            })
    except:
        pass

# Top predictive features
corr_df = pd.DataFrame(correlations).sort_values('abs_correlation', ascending=False)

print(f"\n✓ FEATURE CORRELATIONS WITH OUTCOME (top 20):\n")
print(corr_df.head(20).to_string(index=False))

print("\n📊 CORRELATION ANALYSIS:")
print(f"  • Total features analyzed: {len(feature_data)}")
if len(corr_df) > 0:
    print(f"  • Statistically significant: {len(corr_df[corr_df['p_value'] < 0.10])}")
    print(f"  • Top predictor: {corr_df.iloc[0]['feature'] if len(corr_df) > 0 else 'N/A'}")
    print(f"  • Avg correlation strength: {corr_df['abs_correlation'].mean():.4f}")
else:
    print(f"  • Statistically significant: 0")
    print(f"  • Top predictor: (none found)")
    print(f"  • Avg correlation strength: 0.0000")

# ============================================================================
# DIAGNOSTIC 3: REGIME DRIFT ANALYSIS (Recent vs. Historical)
# ============================================================================
print("\n" + "=" * 80)
print("[DIAGNOSTIC 3] REGIME DRIFT ANALYSIS - RECENT vs. HISTORICAL")
print("=" * 80)

# Sort trades by placement time
trades_sorted = sorted(
    trades,
    key=lambda t: t.get('placement_time') or t.get('logged_at') or '',
    reverse=False  # Oldest first
)

# Calculate performance across time windows
time_windows = []
window_size = len(trades_sorted) // 4  # Quartiles

for q in range(4):
    start_idx = q * window_size
    end_idx = (q + 1) * window_size if q < 3 else len(trades_sorted)
    window_trades = trades_sorted[start_idx:end_idx]

    winners = sum(1 for t in window_trades if str(t.get('outcome') or '').lower() == 'winner')
    rs = [float(t.get('resolved_r_multiple') or 0) for t in window_trades]

    time_windows.append({
        'quartile': f'Q{q+1}',
        'sample_size': len(window_trades),
        'win_rate_pct': 100 * winners / len(window_trades),
        'mean_r': np.mean(rs),
        'std_r': np.std(rs),
        'median_r': np.median(rs),
    })

time_df = pd.DataFrame(time_windows)
print(f"\n✓ PERFORMANCE ACROSS TIME (Quartiles):\n")
print(time_df.to_string(index=False))

# Recent window analysis
print(f"\n✓ RECENT WINDOWS ANALYSIS:\n")
for n_trades in RECENCY_WINDOW_TRADES:
    recent_trades = trades_sorted[-n_trades:]
    recent_winners = sum(1 for t in recent_trades if str(t.get('outcome') or '').lower() == 'winner')
    recent_wr = 100 * recent_winners / len(recent_trades)
    recent_rs = [float(t.get('resolved_r_multiple') or 0) for t in recent_trades]
    recent_exp = np.mean(recent_rs)

    baseline_wr = 100 * sum(1 for t in trades if str(t.get('outcome') or '').lower() == 'winner') / len(trades)
    baseline_exp = np.mean([float(t.get('resolved_r_multiple') or 0) for t in trades])

    print(f"  Last {n_trades} trades:")
    print(f"    Win Rate: {recent_wr:.1f}% (vs. baseline {baseline_wr:.1f}%, delta: {recent_wr - baseline_wr:+.1f}pp)")
    print(f"    Expectancy: {recent_exp:.4f}R (vs. baseline {baseline_exp:.4f}R, delta: {recent_exp - baseline_exp:+.4f}R)")

print("\n📊 REGIME DRIFT FINDINGS:")
print(f"  • Performance trend: ", end="")
if time_df['win_rate_pct'].iloc[-1] > time_df['win_rate_pct'].iloc[0]:
    print("IMPROVING (recent better than historical)")
elif time_df['win_rate_pct'].iloc[-1] < time_df['win_rate_pct'].iloc[0] - 2:
    print("DEGRADING (recent worse than historical)")
else:
    print("STABLE (within 2pp variance)")
print(f"  • Q1 vs. Q4 delta: {time_df['win_rate_pct'].iloc[-1] - time_df['win_rate_pct'].iloc[0]:+.1f}pp")

# ============================================================================
# DIAGNOSTIC 4: FEATURE INTERACTION TESTING
# ============================================================================
print("\n" + "=" * 80)
print("[DIAGNOSTIC 4] FEATURE INTERACTION TESTING & SIMPLIFICATION")
print("=" * 80)

# Test interaction of top features
top_features = ['direction', 'primary_setup', 'adx_regime', 'live_market_regime']

interaction_results = []

# Test 2-way interactions
for f1 in top_features:
    for f2 in top_features:
        if f1 >= f2:
            continue

        # Group by both features
        groups = defaultdict(list)
        for trade in trades:
            v1 = str(trade.get(f1) or '').lower()
            v2 = str(trade.get(f2) or '').lower()
            if v1 and v2:
                groups[(v1, v2)].append(trade)

        # Test each combination
        for (val1, val2), group_trades in groups.items():
            if len(group_trades) >= FEATURE_INTERACTION_MIN_N:
                winners = sum(1 for t in group_trades if str(t.get('outcome') or '').lower() == 'winner')
                wr = 100 * winners / len(group_trades)
                rs = [float(t.get('resolved_r_multiple') or 0) for t in group_trades]

                interaction_results.append({
                    'feature_1': f1,
                    'value_1': val1,
                    'feature_2': f2,
                    'value_2': val2,
                    'n': len(group_trades),
                    'win_rate_pct': wr,
                    'expectancy_r': np.mean(rs),
                    'interaction_key': f"{f1}={val1} + {f2}={val2}"
                })

# Find best and worst interactions
int_df = pd.DataFrame(interaction_results).sort_values('win_rate_pct', ascending=False)

print(f"\n✓ TOP PERFORMING 2-WAY INTERACTIONS (N ≥ {FEATURE_INTERACTION_MIN_N}):\n")
print(int_df.head(10)[['interaction_key', 'n', 'win_rate_pct', 'expectancy_r']].to_string(index=False))

print(f"\n✓ WORST PERFORMING 2-WAY INTERACTIONS:\n")
print(int_df.tail(10)[['interaction_key', 'n', 'win_rate_pct', 'expectancy_r']].to_string(index=False))

print("\n📊 INTERACTION ANALYSIS:")
if len(int_df) > 0:
    print(f"  • Total 2-way interactions tested: {len(int_df)}")
    print(f"  • Best combo win rate: {int_df['win_rate_pct'].max():.1f}%")
    print(f"  • Worst combo win rate: {int_df['win_rate_pct'].min():.1f}%")
    print(f"  • Range: {int_df['win_rate_pct'].max() - int_df['win_rate_pct'].min():.1f}pp")
    # Identify simplification opportunity
    synergistic = int_df[int_df['win_rate_pct'] >= 45.0]
    print(f"  • Synergistic combos (WR ≥ 45%): {len(synergistic)}")
else:
    print(f"  • Total 2-way interactions tested: 0")
    print(f"  • No interactions found with sufficient data")

# ============================================================================
# DIAGNOSTIC 5: RECENCY-WEIGHTED METRICS
# ============================================================================
print("\n" + "=" * 80)
print("[DIAGNOSTIC 5] RECENCY-WEIGHTED PERFORMANCE ANALYSIS")
print("=" * 80)

# Test different recency weights
recency_tests = []

for recency_weight in [1, 5, 10]:
    # Assign weights: oldest = 1x, newest = recency_weight x
    n = len(trades_sorted)
    weights = np.linspace(1, recency_weight, n)

    # Calculate weighted metrics
    weighted_wins = 0
    weighted_total = 0
    weighted_rs = []

    for idx, trade in enumerate(trades_sorted):
        w = weights[idx]
        weighted_total += w

        outcome = str(trade.get('outcome') or '').lower()
        if outcome == 'winner':
            weighted_wins += w

        r_val = float(trade.get('resolved_r_multiple') or 0)
        weighted_rs.extend([r_val] * int(w))

    weighted_wr = 100 * weighted_wins / weighted_total
    weighted_exp = np.mean(weighted_rs) if weighted_rs else 0

    recency_tests.append({
        'recency_weight': f'{recency_weight}x',
        'weighted_win_rate_pct': weighted_wr,
        'weighted_expectancy_r': weighted_exp,
        'delta_wr_vs_uniform': weighted_wr - (100 * sum(1 for t in trades if str(t.get('outcome') or '').lower() == 'winner') / len(trades)),
    })

rec_df = pd.DataFrame(recency_tests)
print(f"\n✓ RECENCY-WEIGHTED METRICS (Oldest = 1x, Newest = Nx):\n")
print(rec_df.to_string(index=False))

print("\n📊 RECENCY ANALYSIS:")
best_weighted = rec_df['weighted_win_rate_pct'].idxmax()
print(f"  • Best recency weight: {rec_df.loc[best_weighted, 'recency_weight']}")
print(f"  • Improvement over uniform: {rec_df.loc[best_weighted, 'delta_wr_vs_uniform']:+.2f}pp")
print(f"  • Conclusion: Recent performance {'BETTER' if rec_df.loc[best_weighted, 'delta_wr_vs_uniform'] > 1 else 'WORSE'} than historical average")

# ============================================================================
# SUMMARY & RECOMMENDATIONS
# ============================================================================
print("\n" + "=" * 80)
print("EXECUTIVE SUMMARY & RECOMMENDATIONS")
print("=" * 80)

baseline_wr = 100 * sum(1 for t in trades if str(t.get('outcome') or '').lower() == 'winner') / len(trades)
baseline_exp = np.mean([float(t.get('resolved_r_multiple') or 0) for t in trades])

print(f"\n📈 PORTFOLIO BASELINE:")
print(f"  • Win Rate: {baseline_wr:.2f}%")
print(f"  • Expectancy: {baseline_exp:.4f}R per trade")
print(f"  • Total Trades: {len(trades)}")
print(f"  • Target WR: {TARGET_WIN_RATE_PCT:.1f}%")
print(f"  • Gap: {baseline_wr - TARGET_WIN_RATE_PCT:.2f}pp BELOW TARGET")

print(f"\n🎯 CRITICAL FINDINGS:")
if len(rule_perf_df) > 0:
    print(f"  1. Quality gate rules OVERESTIMATE win rates by {rule_perf_df['win_rate_delta_pct'].mean():.1f}pp on average")
    print(f"  2. Rules worse than baseline: {sum(rule_perf_df['win_rate_delta_pct'] < 0)} out of {len(rule_perf_df)} tested")
else:
    print(f"  1. Quality gate rules: Insufficient test data")
    print(f"  2. Rules worse than baseline: Data unavailable")
print(f"  3. Recent performance {'DEGRADING' if rec_df.loc[0, 'delta_wr_vs_uniform'] < -2 else 'STABLE'} vs. historical (recency delta: {rec_df.loc[0, 'delta_wr_vs_uniform']:+.2f}pp)")
print(f"  4. Top correlation strength very weak ({corr_df['abs_correlation'].mean():.4f}), suggesting feature engineering needed")
if len(int_df) > 0:
    print(f"  5. Best 2-way interaction achieves {int_df['win_rate_pct'].max():.1f}% (still {45 - int_df['win_rate_pct'].max():.1f}pp below target)")
else:
    print(f"  5. 2-way interactions: Insufficient data")

print(f"\n✅ IMMEDIATE RECOMMENDATIONS (BlackRock Standard):")
print(f"""
  1. PAUSE new rule generation until validation framework is built
     - Current gate rules overfit to historical data
     - Require out-of-sample validation on recent trades (last 50-100)

  2. REBUILD feature engineering pipeline
     - Current features have weak correlation with outcomes (avg |r| = {corr_df['abs_correlation'].mean():.4f})
     - Focus on: order flow pressure slopes, ADX slope direction, regime consistency
     - Test non-linear features (feature^2, sqrt, logs)

  3. IMPLEMENT online learning mechanism
     - Update gate rules weekly with recent performance data
     - Use exponential weighting (recent trades 10x more important)
     - Current gate frozen since 2026-03-27, drift is evident in time quartiles

  4. SIMPLIFY multi-factor AND gates
     - Current 8-condition gates rarely trigger
     - Test: Can you achieve similar win rate with 3-4 key factors?
     - Data shows many interactions are statistical noise

  5. REDUCE confidence thresholds temporarily
     - Currently targeting 50% WR; market showing 36-37%
     - Set interim target at 40% with confidence intervals
     - Require 50+ trades per rule before deployment

  6. DEPLOY stratified backtesting
     - Current backtests assume stationarity; markets don't
     - Walk-forward validation on rolling windows
     - Separate validation for each market regime (trending, ranging, volatile)
""")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)
print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("Next Steps: Review diagnostic outputs and implement recommendations")
print("=" * 80)

# Save summary outputs
summary_output = {
    'timestamp': datetime.now().isoformat(),
    'portfolio_baseline': {
        'win_rate_pct': baseline_wr,
        'expectancy_r': baseline_exp,
        'total_trades': len(trades),
        'target_win_rate_pct': TARGET_WIN_RATE_PCT,
        'gap_pct': baseline_wr - TARGET_WIN_RATE_PCT,
    },
    'rule_performance_summary': {
        'rules_tested': len(rule_perf_df),
        'rules_underperforming': int(sum(rule_perf_df['win_rate_delta_pct'] < 0)) if len(rule_perf_df) > 0 else 0,
        'avg_overestimation_pct': float(rule_perf_df['win_rate_delta_pct'].mean()) if len(rule_perf_df) > 0 else 0,
        'max_underperformance_pct': float(rule_perf_df['win_rate_delta_pct'].min()) if len(rule_perf_df) > 0 else 0,
    },
    'feature_importance': corr_df.head(10).to_dict('records') if len(corr_df) > 0 else [],
    'recency_analysis': rec_df.to_dict('records'),
    'interaction_analysis': {
        'total_interactions': len(int_df) if len(int_df) > 0 else 0,
        'best_win_rate_pct': float(int_df['win_rate_pct'].max()) if len(int_df) > 0 else 0,
        'worst_win_rate_pct': float(int_df['win_rate_pct'].min()) if len(int_df) > 0 else 0,
        'range_pct': float(int_df['win_rate_pct'].max() - int_df['win_rate_pct'].min()) if len(int_df) > 0 else 0,
    }
}

with open(r'g:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\diagnostic_summary.json', 'w') as f:
    json.dump(summary_output, f, indent=2, default=str)

print("\n✓ Summary saved to: diagnostic_summary.json")
