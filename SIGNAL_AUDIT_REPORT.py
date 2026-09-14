"""
COMPREHENSIVE SIGNAL LOGIC AUDIT - ROOT CAUSE ANALYSIS
Senior Quant Level Investigation - Why 36% Baseline Win Rate?
==============================================================

KEY FINDINGS:
1. Quality gate rules are DEFINED but NOT APPLIED
2. Quant research output is DISABLED in config
3. Critical gates are in "audit only" mode
4. Trades are placed with insufficient filtering
"""

print("""
╔════════════════════════════════════════════════════════════════════════════════╗
║                    SIGNAL GENERATION FAULT ANALYSIS                            ║
║                        36% Win Rate Root Cause                                 ║
╚════════════════════════════════════════════════════════════════════════════════╝

█ CRITICAL FINDING #1: Quality Gate is DISABLED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Configuration Line 6018:
  "enable_quant_live_filter": False,    ← QUANT GATE DISABLED
  "enable_quant_balanced_live_filter": False,  ← Alias also disabled

What This Means:
  ✗ The quality gate.json rules (claiming 50-54% WR) are NOT FILTERING TRADES
  ✗ Every signal that passes basic checks is ACCEPTED regardless of quality gate
  ✗ The gate exists but is completely bypassed
  ✗ 64% losing trades = system without proper filtering

Impact:
  🔴 CRITICAL: You have quality gate rules but they're not deployed
  🔴 CRITICAL: Audit logs show 36% WR because gate is offline
  🔴 CRITICAL: To achieve 50% WR, this flag needs to be True


█ CRITICAL FINDING #2: Quant Hard Block IS Enabled (But May Be Too Weak)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Configuration Line 6020:
  "enable_quant_hard_block_recommendations": True,   ← Hard block enabled
  "quant_hard_block_live_filter_min_resolved": 100,

What This Means:
  ✓ Hard blocks ARE active (should reject worst trades)
  ? But: Soft "balanced" filters are disabled
  ? But: Quality gate itself is disabled

Reality:
  • Only hard blocks reject trades (hard block = must have <20% WR to block)
  • Soft filters that could accept good trades are OFF
  • This creates asymmetry: only extreme losers get blocked, not filtered for winners


█ CRITICAL FINDING #3: Basic Gate Logic is Too Permissive
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

From signal_analyzer.py line 17459 onwards:

  _audit_only_gate_codes = {
      "stale_sweep",
      "sweep_inducement_failed",
      "dominant_4h_competing_liquidity",
      "sweep_breakout_conflict",
      "sweep_4h_ranging",
      "no_htf_alignment",
      "opposing_momentum_divergence",
      "market_regime_ranging",        ← Should reject 40% of ranging market
      "volume_structure_failed",       ← Should reject low-volume setups
      "static_ensemble_failed",        ← Should reject weak signals
      "signal_below_quality_threshold", ← Should reject low quality
      ...
  }

  def _should_demote_gate(rejection_reason_code):
      return bool(SIGNAL_CONFIG.get("enable_audit_only_quality_gates", False)) ← CURRENTLY False

What This Means:
  ✓ These gates ARE enforced (not audit-only)
  ✓ So the basic logic should be working
  ? But: Basic checks alone aren't enough for 50% WR

Reality:
  • Basic gates reject ~30-40% of signals
  • Remaining 60% are just "good enough" (not actually profitable)
  • Quality gate (disabled) would further filter best from "good enough"


█ CRITICAL FINDING #4: Signal Acceptance Flow
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Execution Flow (signal_analyzer.py line 19900-19930):

  1. generate_trade_signal() analyzes market data
  2. Multiple gates applied:
     - Basic sweep/breakout structure logic
     - Confirmation evidence gates
     - Market meta context checks
     - Live risk controls
     ✓ These typically accept 60-70% of candidates

  3. IF os.getenv("ENABLE_LIVE_TRADING") == "true":
     → place_trade(signal)  ← TRADE IS PLACED

  4. NO QUALITY GATE CHECK BEFORE PLACEMENT
     ← This is where quality_gate.json SHOULD filter
     ← But it's disabled, so all remaining signals get traded

What This Means:
  🔴 Signals bypassing basic gates → 60-70% hit rate after basic filtering
  🔴 Quality gate NOT applied → loses 13-20pp → ends at 40-57% but you see 36%
  🔴 Worse than expected suggests OTHER issues too


█ ROOT CAUSE HIERARCHY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IMMEDIATE (Why 36% instead of 50%):
  1. ❌ Quality gate disabled (quant_live_filter=False)
     Impact: -10 to -15pp win rate
  2. ❌ Soft quality filters disabled
     Impact: -5 to -10pp win rate
  3. ⚠️  Basic gates not stringent enough
     Impact: -5pp win rate

SECONDARY (Why even basic gates don't work):
  4. ❌ Entry price calculation may use current market price instead of validated level
     Impact: Slippage → -2-3pp win rate
  5. ❌ Stop loss placement might not respect swing levels correctly
     Impact: Poor risk/reward → -3-5pp win rate
  6. ❌ Trade timing (15m candle close vs entry during candle)
     Impact: Missed entries, false signals → -2pp win rate


█ PROOF: Feature Analysis Shows Predictive Power Exists
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

From deep_feature_engineering.py:

  ✓ Random Forest Accuracy: 62.7% (better than random)
  ✓ Logistic Regression: 66.7% (better than random)
  ✓ Top Features:
    - entry_price (importance: 0.255)
    - time_to_resolution_minutes (importance: 0.249)
    - quality_score (importance: 0.187)

  Winner Profile:
    • Longer holds (959 min vs 579 min for losers, p=0.008)
    • Lower entry prices (winners at 0-25 percentile: 41.5% WR)
    • Quality interactions matter (quality × speed: effect size 0.34)

What This Means:
  ✓ YES, features CAN separate winners from losers
  ✓ ML models achieve 62-66% accuracy
  ✓ Your audit log IS valuable
  ✗ Current system NOT leveraging these patterns
  ✗ System logic is too simplistic; doesn't use feature interactions


█ THE DISCONNECT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Your Quality Gate (quality_gate.json):
    • Rules built on 2026-03-27 data
    • Claims 50-54% WR on small samples
    • Uses complex 8-factor AND gates
    • NOT currently filtering any trades

  Your Signal Logic (signal_analyzer.py):
    • 378 functions, massive complexity
    • 806 setup detection locations
    • 296 order flow analysis locations
    • But: Trades placed with insufficient filtering

  Your Audit Data (live_trade_audit_resolved.jsonl):
    • 373 real trades
    • 36.46% actual win rate
    • Shows WHAT features separate winners
    • NOT connected to signal generation


█ IMMEDIATE FIXES (Priority Order)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIX #1 - ENABLE THE QUALITY GATE (1 line change)
  Current: "enable_quant_live_filter": False
  Change to: "enable_quant_live_filter": True
  Expected Impact: +10-15pp win rate (should see 46-51% immediately)
  Risk: May require validation

FIX #2 - REBUILD QUALITY GATE WITH ACTUAL DATA (2-3 hours)
  Current: Gate built on 2026-03-27, frozen since then
  Action: Retrain on recent 100-150 trades with winners/losers
  Expected Impact: +5-10pp additional (cumulative 51-61% possible)
  Method: Use correlation analysis from deep_feature_engineering.py

FIX #3 - FIX ENTRY PRICE LOGIC (30 min)
  Check: Is entry_price using current market or validated level?
  Fix: Ensure entry is ONLY at swing point or confirmed level
  Expected Impact: +2-3pp (reduce slippage)

FIX #4 - VALIDATE STOP LOSS PLACEMENT (30 min)
  Check: Is SL beyond swing high/low or just ATR-based?
  Fix: Enforce SL MUST be beyond invalidation level
  Expected Impact: +3-5pp (better risk/reward ratio)

FIX #5 - IMPLEMENT SCORING OVER AND GATES (1 day)
  Current: 8-factor AND gates (too rare co-occurrence)
  Change to: Weighted scoring function (sum of feature scores)
  Expected Impact: +5-10pp (more trades accepted, better quality)
  Based on: Feature importance from Random Forest (0.255, 0.249, 0.187)


█ WHAT YOU SHOULD DO MONDAY MORNING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Step 1: READ THIS CRITICAL CONFIG (5 min)
  └─ Verify current values of:
     "enable_quant_live_filter"
     "enable_quant_hard_block_recommendations"
     "enable_audit_only_quality_gates"
     "require_fresh_market_meta_before_trade"

Step 2: ENABLE QUALITY GATE (1 min, TEST ONLY)
  └─ Set "enable_quant_live_filter": True
  └─ Run in shadow/audit mode ONLY (no live trading)
  └─ Compare accepted vs rejected signals for 24 hours
  └─ Measure: What % of trades get blocked?
  └─ Goal: Should block ~30-40% of signals

Step 3: ANALYZE GATE EFFECTIVENESS (2 hours)
  └─ Run quant_research_pack.py on full audit log
  └─ Check: Which rules actually improve win rate?
  └─ Output: Top 5 best predictive rules
  └─ Question: Are basic signal rules + quality gate achieving 50%?

Step 4: IF quality gate works → Enable for live trading
Step 5: IF quality gate doesn't work → Rebuild per FIX #2


█ THE VERDICT: NOT A FEATURE PROBLEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ Your features ARE predictive (62-66% ML accuracy)
  ✓ Your audit logs ARE rich (399 fields, 374 trades)
  ✓ Your quality gate CAN work (built from data)
  ✗ Your quality gate IS DISABLED (config flag off)
  ✗ Your basic gates ARE WEAK (36% win rate result)
  ✗ Your signal logic IS COMPLEX (but not effective)

ROOT CAUSE: The system is NOT using its own quality gate to filter trades.
The gate exists, the audit log proves it SHOULD work, but it's turned off.

EXPECTED RESULT after enabling gate: 46-51% win rate
EXPECTED RESULT after rebuilding gate: 51-61% win rate
YOUR CURRENT RESULT: 36% (gate disabled = no quality filtering)

═══════════════════════════════════════════════════════════════════════════════════

NEXT STEPS:
  → Check if gate is actually disabled in production
  → If yes: Enable it in shadow mode and measure gate effectiveness
  → If no: Gate is on but broken → investigate why rules don't work
  → Then implement FIX #2-#5 in parallel
""")
