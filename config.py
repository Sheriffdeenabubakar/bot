import os
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# --- Bitget API Configuration ---
BITGET_API_KEY    = os.getenv("BITGET_API_KEY",    "YOUR_BITGET_API_KEY")
BITGET_SECRET_KEY = os.getenv("BITGET_SECRET_KEY", "YOUR_BITGET_SECRET_KEY")
BITGET_PASSPHRASE = os.getenv("BITGET_PASSPHRASE", "YOUR_BITGET_PASSPHRASE")

BITGET_API_URL = os.getenv("BITGET_API_URL", "https://api.bitget.com")

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Scanner ---
# Minimum 24h volume in USDT for a symbol to be eligible for analysis.
MIN_VOLUME_THRESHOLD   = float(os.getenv("MIN_VOLUME_THRESHOLD", 500_000))
SCANNER_INTERVAL_SECONDS = 60  # seconds between scanner cycles

# --- Signal Analyzer Configuration ---
# These values override DEFAULT_SIGNAL_CONFIG in signal_analyzer.py.
# Merge: SIGNAL_CONFIG = {**DEFAULT_SIGNAL_CONFIG, **SIGNAL_CONFIG}
# Keys not listed here fall back to defaults in DEFAULT_SIGNAL_CONFIG.
SIGNAL_CONFIG = {

    # ── Data fetching ─────────────────────────────────────────────────────
    "candlestick_interval":  "15m",
    "candlestick_limit":     200,    # 200 x 15m = ~50 hours; MA direction now replaces deep structure direction
    "structure_15m_candle_limit": 200,  # 15m swing/liquidity window aligned with live decision data
    "operational_15m_candle_limit": 200, # working 15m operational window for sweep trigger + local indicators
    "directional_15m_candle_limit": 200, # dedicated 15m direction-context window; separate from sweep/trigger window
    "operational_update_candles": 10,    # subsequent operational refreshes fetch only the latest tail and merge into cache
    "ltf_levels_ttl_s":      10800,  # cache TTL for 15m structural swing levels (3h)
    "fresh_data_limit":      100,    # fallback small-window fetches outside the main operational caches
    "htf_candle_limit":      200,    # 1H/4H trend fetch — needs enough candles for swing structure classification
    "htf_deep_candle_limit": 200,    # 4H swing/liquidity extraction uses the same 200-candle MA-era window
    "htf_operational_candle_limit": 200, # 1H/4H operational window for MA/MACD/ADX telemetry
    "htf_min_required_candles": 100, # minimum 1H/4H candles required for HTF MA direction
    "htf_operational_update_candles": 10, # subsequent 1H/4H operational refreshes fetch only the latest tail and merge
    "htf_1h_deep_candle_limit": 200,
    "htf_1h_cache_ttl_s":    0,  # no deep 1H structure cache in MA-direction mode
    "min_required_candles":  200,   # must be >= any indicator lookback
    "time_span_hours":       48,    # expect ~4 days of data (400 × 15m) — was 12h

    # ── Indicators ────────────────────────────────────────────────────────
    "ma_short_period":                20,
    "ma_long_period":                 50,
    "rsi_period":                     14,
    "rsi_threshold_oversold":         35,
    "rsi_threshold_overbought":       65,
    "macd_fast_period":               12,
    "macd_slow_period":               26,
    "macd_signal_period":             9,
    "enable_htf_ma_confirmation_gate": False,
    "htf_ma_confirmation_timeframe":   "4H",
    "htf_ma_slope_lookback_candles":   5,
    "htf_ma_require_price_side":       True,
    "htf_ma_require_stack":            True,
    "htf_ma_require_fast_slope":       True,
    "htf_ma_require_slow_slope":       True,
    "htf_ma_price_deadband_pct":       0.001,
    "htf_ma_slope_deadband_pct":       0.0002,
    "htf_macd_confirmation_mode":      "audit_only",
    "bb_period":                      20,
    "bb_std_dev":                     2.0,
    "volume_ma_period":               20,
    "volume_confirmation_multiplier": 1.1,
    "atr_period":                     14,
    "structure_bos_atr_multiple":     0.5,   # minimum displacement for a BOS to qualify (× ATR)
    "adx_period":                     14,
    "adx_threshold":                  25,

    # ── Swing / structure ─────────────────────────────────────────────────
    "swing_point_window":                    3,    # minimum fractal window — enforced via swing_point_window_min
    "swing_point_window_min":                3,
    "market_structure_tolerance_atr_multiple": 0.5,
    "min_confluence_points":                 5,
    "structure_lookback_15m":                200,  # ~2.1 days for 15m direction context

    # ── Swing detection — persistence-based confirmation ─────────────────
    # A swing is structural only if: fractal geometry (window≥5) AND
    # subsequent price confirms the level AND minimum displacement from prior swing.
    # swing_confirm_pct:   subsequent price must trade this % beyond the pivot
    # swing_price_pct_min: minimum displacement between consecutive confirmed swings
    "swing_confirm_pct":   0.003,   # 0.3% — subsequent price confirmation threshold
    "swing_price_pct_min": 0.003,   # 0.3% — min displacement from prior confirmed swing
    "sweep_filter_mitigated_levels": True,  # drop levels already swept/mitigated
    "sweep_unswept_atr_tolerance":   0.0,   # ATR tolerance for unswept check (0 = strict)

    # ── Equal highs/lows ──────────────────────────────────────────────────
    "eqh_eql_lookback":   80,    # ~20h on 15m — was 20 (5h), now uses full history depth
    "eqh_eql_tolerance":  0.002,

    # ── Order blocks / displacement ───────────────────────────────────────
    "ob_lookback_candles":              60,    # ~15h on 15m — was 20 (5h)
    "displacement_threshold":           0.005,
    "displacement_threshold_atr_multiple": 0.10,
    "displacement_ratio":               0.1,
    "ob_entry_buffer_atr_multiple":     1.0,
    "ob_allow_mitigated":               True,
    "ob_invalidation_atr":              0.0,


    # Max candles since breakout for flip to still be valid.
    # Beyond this, trapped traders may already be stopped out.

    # ── Sweep detection ───────────────────────────────────────────────────
    "sweep_swing_min_age_candles": 30,  # only consider established sweep levels
    "sweep_lookback_candles":      8,   # ~2 hours on 15m
    "sweep_volume_multiplier":     1.2, # institutional-level volume required
    "min_sweep_wick_pct":          0.30,# rejection wick >= 30% of candle range
    "sweep_two_candle_lookback":   3,   # Pattern B: candles back to look for prior body break
    "sweep_two_candle_lookback":   3,   # prior candles to scan for two-candle sweep pattern
    "sweep_use_15m_levels":        True,  # use 15m swing levels for sweeps
    "sweep_require_4h_confluence": False,  # 15m level must be near a 4H level
    "sweep_4h_confluence_atr":     0.25,  # proximity in ATR multiples

    # ── Sweep staleness / proximity ───────────────────────────────────────
    # BOTH age AND drift must exceed their thresholds to hard-reject a stale sweep.
    "max_sweep_age_candles":  10,    # 5 before 5 candles = 75 min on 15m
    "max_sweep_drift_atr":    1.0,  # max ATR drift from swept level
    "max_sweep_entry_atr":    1.0,  # max ATR from swept level at entry
    "confirmation_aware_entry_proximity": False,
    "max_sweep_confirmed_entry_atr": 1.5,
    "max_sweep_absolute_entry_atr": 2.0,
    "confirmed_entry_body_extension_cap_atr": 0.25,
    "sweep_entry_buffer_atr": 0.1,
    "sweep_require_bos_confirmation": False,

    # ── Breakout detection ────────────────────────────────────────────────
    "bo_scan_candles":        30,
    "bo_volume_multiplier":   1.05,
    "bo_buffer_atr_multiple": 0.15,
    "bo_min_body_ratio":      0.18,
    "breakout_min_level_quality_score": 0.5,
    "breakout_min_level_touches": 3,
    "breakout_retest_tolerance_atr": 0.35,
    "breakout_retest_min_pullback_atr": 0.05,
    "breakout_retest_max_overshoot_atr": 0.55,
    "breakout_retest_max_volume_ratio": 1.25,
    "breakout_allow_first_candle_retest": False,
    "breakout_touch_tolerance_atr": 0.25,
    "enable_order_block_context": False,
    "order_block_zone_tolerance_atr": 0.50, # 0.20 before
    "order_block_context_score": 2, # 8 before
    "max_breakout_entry_atr": 1.0,  # max ATR from retest zone at breakout entry
    "max_breakout_confirmed_entry_atr": 1.75,
    "max_breakout_absolute_entry_atr": 2.25,
    "breakout_entry_buffer_atr": 0.05,
    "breakout_swing_min_age_candles": 10,
    "breakout_retest_max_age_candles": 10,

    # ── S/R retest ────────────────────────────────────────────────────────
    "sr_retest_zone_buffer_atr_multiple": 0.5,
    "entry_zone_tolerance_percent":       0.005,

    # ── ADX / regime ──────────────────────────────────────────────────────
    "adx_slope_exhaustion_thresh": -1.5,  # per-bar ADX decline for exhaustion downgrade
    "stateless_entry_paths": ["sweep", "breakout"],
    "path_adx_confirmation_timeframe": "15m",
    "path_adx_confirmation_candle_limit": 100,
    "path_adx_audit_timeframes": ["3m", "5m", "15m", "1H"],
    "path_adx_slope_lookback_candles": 10,
    "sweep_adx_prior_strength_min": 22.0,
    "sweep_adx_current_min": 16.0,
    "sweep_adx_delta_max": -0.25,
    "breakout_adx_current_min": 20.0,
    "breakout_adx_delta_min": 0.15,
    "breakout_adx_expansion_positive_ratio_min": 0.60,
    "breakout_adx_retest_cooldown_max_drop": 1.25,
    "breakout_adx_retest_cooldown_positive_ratio_min": 0.45,

    # -- Live-only decisioning --------------------------------------------
    # Research-era bucket/session/context filters are disabled.
    # Live decisions should come from current structure, timing, order-flow,
    # divergence, and live audit feedback instead of old research buckets.
    "enable_bucket_filter": False,
    "blocked_symbol_buckets": [],
    "blocked_symbol_session_buckets": [],
    "enable_precision_sweep_filter": False,
    "precision_sweep_regime_source": "live",
    "precision_sweep_allowed_market_regimes": [],
    "research_regime_quantile_window": 300,
    "precision_sweep_hard_block_rules": [],
    "precision_sweep_allow_any_rules": [],
    "enable_precision_breakout_filter": False,
    "precision_breakout_regime_source": "live",
    "precision_breakout_allowed_market_regimes": [],
    "precision_breakout_hard_block_rules": [],
    "precision_breakout_allow_any_rules": [],
    "enable_contextual_static_ensemble_soft_bypass": False,
    "sweep_static_ensemble_soft_bypass_rules": [],
    "breakout_static_ensemble_soft_bypass_rules": [],
    "enable_sweep_anchor_divergence_layer": True,
    "sweep_anchor_divergence_bonus_multiplier": 0.12,
    "sweep_anchor_divergence_bonus_cap": 12,
    "sweep_anchor_divergence_strong_score": 55,
    "sweep_anchor_divergence_min_score": 55,
    "sweep_anchor_divergence_fresh_threshold_penalty": 0,
    "sweep_anchor_divergence_min_rsi_delta": 3.0,
    "sweep_anchor_divergence_threshold_candidates": [45, 50, 55, 60, 70],
    "live_liquidity_cache_seconds": 300,
    "enable_post_path_rejected_shadow_tracking": True,
    "live_path_quarantine_min_trades": 8,
    "live_path_quarantine_expectancy_r": -0.2,
    "live_path_quarantine_win_rate_pct": 35.0,
    "live_path_quarantine_hard_block": False,
    "live_symbol_quality_min_trades": 4,
    "live_symbol_quality_expectancy_r": -0.25,
    "live_symbol_quality_max_failure_rate": 0.35,
    "live_context_hard_block_rules": [],
    "live_config_evidence_min_samples": 3,
    "live_config_evidence_positive_expectancy_r": 0.0,
    "live_config_evidence_positive_win_rate_pct": 55.0,
    "live_swing_evidence_min_samples": 3,
    "enable_live_audit_dynamic_rules": False,
    "enable_quant_refresh": False,
    "enable_quant_live_filter": False, # make it false when you have gotten enough trades. for now enable hard block.
    "enable_quant_balanced_live_filter": False,
    "quant_live_filter_preferred_lane": "precision",
    "quant_live_filter_min_resolved": 100,
    "quant_live_filter_require_all_resolved_basis": True,
    "quant_live_filter_max_stale_resolved_delta": 12,
    "quant_live_filter_max_stale_resolved_pct": 0.05,
    "quant_precision_live_filter_target_wr_pct": 50.0,
    "quant_precision_live_filter_min_retained_pct": 0.0,
    "quant_practical_live_filter_target_wr_pct": 45.0,
    "quant_practical_live_filter_min_retained_pct": 40.0,
    "quant_balanced_live_filter_min_retained_pct": 60.0,
    "enable_quant_hard_block_recommendations": False,
    "enable_quant_filter_blocklists_as_hard_blocks": False,
    "enable_live_symbol_quality_hard_block": False,
    "quant_hard_block_live_filter_min_resolved": 100,
    "enable_ensemble_hard_gate": False,
    "enable_static_quality_hard_gate": False,
    "ensemble_min_votes_hard_floor": 5,
    "require_fresh_market_meta_before_trade": True,
    "enable_audit_only_quality_gates": False,
    "breakout_require_htf_alignment": False,
    "block_ranging_regime": True,
    "enable_websocket_orderflow_confirmation": True,
    "enable_continuous_orderflow_manager": True,
    "enable_orderflow_background_thread": True,
    "scanner_run_once_on_startup": True,
    "enable_periodic_symbol_rescan": False,
    "periodic_symbol_rescan_seconds": 21600,
    "main_loop_yield_seconds": 0.0,
    "main_loop_error_retry_seconds": 10.0,
    "of_allow_point_in_time_fallback": False,
    "of_warmup_seconds": 60,
    "of_incremental_warmup_seconds": 60,
    "of_warmup_max_wait_seconds": 400,
    "of_warmup_ready_ratio": 0.95,
    "of_warmup_poll_seconds": 2.0,
    "of_min_stream_age_seconds": 60,
    "of_book_stale_seconds": 30.0,
    "of_subscription_reconcile_seconds": 30.0,
    "of_disconnect_mark_unavailable_after_seconds": 45.0,
    "of_snapshot_timeout_seconds": 10.0, #put below
    "of_snapshot_timeout_seconds": 10.0,            # safeguard only (was 5.0)
    "of_snapshot_refresh_interval_seconds": 5.0,   # do not starve the WS receive loop
    "of_ws_stale_reconnect_seconds": 90.0,
    "of_snapshot_cache_max_age_seconds": 2.0,      # max age for cache-first reads
    "of_snapshot_worker_threads": 8,               # threads for offloaded compute
    "of_prune_interval_seconds": 30.0,
    "of_background_command_timeout_seconds": 15.0,
    "of_trade_retention_seconds": 900,
    "of_book_retention_seconds": 600,
    "of_event_retention_seconds": 900,
    "of_target_channels_per_connection": 40,
    "of_snapshot_worker_threads": 8,
    "of_book_history_maxlen": 2000,
    "of_trade_history_maxlen": 8000,
    "of_max_connections": 50,
    "of_book_channel": "books5",
    "of_focus_book_channel": "books5",
    "of_enable_focus_book_upgrade": True,
    "of_focus_symbols_max": 40,
    "of_focus_min_dwell_seconds": 300,
    "of_focus_idle_seconds": 900,
    "of_book_channel_weights": {"books1": 1, "books5": 1, "books15": 2, "books": 6},
    "of_subscribe_book_channel": True,
    "of_trade_dedupe_window": 20000,
    "of_subscribe_payload_max_bytes": 3800,
    "of_subscribe_messages_per_second": 5.0,
    "of_ws_ping_interval_seconds": 20.0,
    "of_ws_recv_poll_seconds": 1.0,
    "of_breakout_lookback_seconds": 180,
    "of_breakout_event_lookback_seconds": 180,
    "of_sweep_lookback_seconds": 180,
    "of_sweep_event_lookback_seconds": 300,
    "of_fail_closed_on_gap": False,
    "of_fail_closed_on_thin_tape": True,
    "of_max_data_staleness_seconds": 180.0, # from 45.0
    "of_between_symbol_yield_seconds": 0.05,
    "of_min_trades_per_snapshot": 4, # 8 before
    "of_min_trade_notional": 25.0, # 50 before
    "of_min_book_snapshots": 3,
    "of_depth_snapshot_levels": 15,
    "of_trade_history_maxlen": 8000,
    "of_book_history_maxlen": 2000,
    "require_candlestick_orderflow_confirmation": False,
    "enable_live_confirmation_evidence_gate": False,
    "enable_path_adx_hard_gate": False,
    "enable_simplified_live_entry_gate": True,
    "require_live_ws_orderflow_for_entry": True,
    "live_ws_directional_pressure_min": 52.0,
    "live_ws_pressure_edge_min": 2.0,
    "live_ws_imbalance_min_abs": 0.03,
    "live_ws_cvd_strong_opposition_abs": 2500.0,
    "live_ws_sweep_require_event_evidence": False,
    "live_ws_sweep_absorption_min_strength": 0.40,
    "live_ws_sweep_delta_divergence_min_strength": 0.15,
    "live_ws_pressure_slope_min": 0.0005,
    "live_ws_imbalance_slope_min": 0.00005,
    "last_candle_confirm_min_body_ratio": 0.35,
    "last_candle_confirm_min_body_atr": 0.10,
    "last_candle_confirm_require_close_location": True,
    "live_confirmation_min_momentum_divergence_strength_sweep": 40.0,
    "live_confirmation_min_momentum_divergence_strength_breakout": 25.0,
    "live_confirmation_min_sweep_anchor_divergence_score": 68.0,
    "live_confirmation_min_orderflow_pressure_slope": 0.001,
    "live_confirmation_min_orderflow_imbalance_slope": 0.0001,
    "live_audit_dynamic_tighten_min_resolved": 2,
    "live_audit_dynamic_tighten_max_expectancy_r": -1.0,
    "live_audit_dynamic_tighten_max_rules": 12,
    "live_audit_dynamic_swing_tighten_min_resolved": 4,
    "live_audit_dynamic_swing_tighten_max_expectancy_r": -1.0,
    "live_audit_dynamic_swing_tighten_max_rules": 5,
    "live_audit_dynamic_rescue_min_shadow_resolved": 25,
    "live_audit_dynamic_rescue_min_core_resolved": 25,
    "live_audit_dynamic_rescue_min_shadow_win_rate_pct": 45.0,
    "live_audit_dynamic_rescue_min_core_win_rate_pct": 45.0,
    "live_audit_dynamic_rescue_min_shadow_expectancy_r": 0.25,
    "live_audit_dynamic_rescue_min_core_expectancy_r": 0.25,
    "live_audit_dynamic_rescue_max_rules": 6,
    "live_audit_dynamic_swing_rescue_min_shadow_resolved": 60,
    "live_audit_dynamic_swing_rescue_min_core_resolved": 60,
    "live_audit_dynamic_swing_rescue_min_shadow_win_rate_pct": 45.0,
    "live_audit_dynamic_swing_rescue_min_core_win_rate_pct": 45.0,
    "live_audit_dynamic_swing_rescue_min_shadow_expectancy_r": 0.25,
    "live_audit_dynamic_swing_rescue_min_core_expectancy_r": 0.25,
    "live_audit_dynamic_swing_rescue_max_rules": 2,
    "enable_exchange_exact_trade_reconciliation": True,
    "live_trade_audit_candle_fallback_enabled": False,
    "live_trade_reconciliation_lookback_hours": 96,
    "live_trade_reconciliation_fill_limit": 100,
    "live_trade_reconciliation_flat_pnl_tolerance_quote": 0.01,
    "live_trade_reconciliation_exit_match_tolerance_risk_fraction": 0.2,
    "enable_balance_auto_resize": False,
    "balance_auto_resize_utilization": 0.98,
    "live_rejected_shadow_refresh_every_cycles": 2,
    "live_rejected_shadow_refresh_max_symbols": 40,
    "public_api_timeout_seconds": 10.0,
    "signed_api_timeout_seconds": 12.0,
    "candle_fetch_timeout_seconds": 15.0,
    "async_session_total_timeout_seconds": 15.0,
    "async_session_connect_timeout_seconds": 10.0,
    "websocket_open_timeout_seconds": 60.0,
    "websocket_recv_timeout_seconds": 60.0,
    "api_request_retry_attempts": 3,
    "api_request_retry_backoff_seconds": 1.0,
    "order_detail_fill_retry_attempts": 5,
    "order_detail_fill_retry_sleep_seconds": 1.0,
    "history_cache_min_completion_ratio": 0.85,
    "htf_4h_cache_ttl_s": 0,
    "enable_htf_level_cache": False,

    # ── Fibonacci ─────────────────────────────────────────────────────────
    "fib_tolerance": 0.02,

    # ── Order flow ────────────────────────────────────────────────────────
    # Adversarial flow veto: fires only when BOTH conditions are met.
    # pressure < 35% (opposing > 65%) AND imbalance strongly opposing.
    "adversarial_flow_pressure_max":  35,
    "adversarial_flow_imbalance_min": 0.35,
    "orderflow_miss_penalty":         8,
    "enable_orderflow_final_score_gate": False,
    "enable_path_specific_orderflow_hard_gate": True,
    "include_raw_signal_snapshots_in_trade_audit": False,
    "trade_metrics_realized_r_cap": 5.0,

    # ── SL/TP ─────────────────────────────────────────────────────────────
    "atr_sl_multiplier":   1.5, #changed from 2.0
    "fallback_sl_percent": 0.015,   # 1.5% fallback SL when no structural reference
    "fallback_buffer_percent": 0.005,
    "max_risk_percent":    0.030,    # max SL distance from entry for sweep and breakout paths

    # Sweep SL: anchored beyond the swept level plus ATR/bps buffer.
    # 0.3× ATR covers the typical re-test range without being excessively wide.
    # Breakout SL: use structure invalidation, but add ATR padding and enforce
    # a minimum risk floor so fresh retests do not get stopped out too tightly.
    "sl_breakout_atr_buffer": 0.6,
    "sl_breakout_min_risk_atr": 1.5,

    "sl_sweep_atr_buffer": 0.6,

    "tp1_rr_multiplier": 1.5,   # partial exit at 1.5R
    "tp2_rr_multiplier": 2.0,   # remainder to 3R with trailing

    # Trailing stop is percentage-activated from fill by default.
    # If trailing_activation_at_tp1=True, TP1 becomes the activation trigger.
    "trailing_activation_at_tp1": False,  # percentage-based activation at 3% from fill

    # ── HTF sweep trend alignment ─────────────────────────────────────────
    # True:  two-tier sweep HTF check.
    #   With-trend (4H agrees with reversal direction): skip exhaustion, +8/+12 bonus.
    #   Counter-trend (4H opposes reversal): exhaustion becomes hard requirement.
    #   Ranging 4H: existing soft-penalty behaviour unchanged.
    # False: original exhaustion-only HTF check applies to all sweeps.
    "sweep_htf_trend_alignment": False,

    # False: original hard reject on all exhausted breakout clusters.

    # ── Candlestick patterns ──────────────────────────────────────────────
    "pin_bar_body_ratio": 0.1,
    "pin_bar_wick_ratio": 0.5,

    # ── Signal caching ────────────────────────────────────────────────────
    "signal_cache_timeout": 72000,   # seconds before cached signal expires

    # Touch and scan window relaxed — sweep wicks overshoot, reversal needs time.
    # Body ratio 0.35 — candle must show directional intent, not a doji.
    # Volume kept — rejection candle with volume at the level IS the proof of
    # absorption. Proximity re-check only says price is NEAR the level — the
    # 5m is the only gate confirming it TOUCHED and REVERSED from it.

    # ── 15m touch-and-reverse confirmation (sweep OR path 2) ─────────────────
    # Structural same-timeframe check using the 15m data already in hand.
    # 15m candles are larger and more complex, so tolerances are wider.
    "conf_15m_touch_atr":    1.0,   # touch tolerance in ATR multiples
    "conf_15m_body_ratio":   0.30,  # lower body ratio — 15m candles are noisier
    "conf_15m_scan_candles": 4,     # 4 × 15m = 60min window

    # ── Path enable flags ─────────────────────────────────────────────────
    "enable_sweep_path":    True,    # sweep remains the primary reversal path
    "enable_breakout_path": True,    # enabled with selective precision overlay




    # ── Structural lookbacks — scaled to 400-candle history ──────────────
    # All lookbacks previously anchored to 200-candle limit. Now scaled to
    # use the full 4-day 15m history available.
    "eq_lookback_candles":      150,   # equal high/low cluster scan — was 50 (~12h), now ~37h
    "div_sweep_lookback":       150,   # divergence sweep anchor search — was 40 (~10h), now ~37h
    "div_retest_lookback":      30,    # divergence retest anchor search — was 8 (~2h), now ~7h

    # ── Unified DOL Assessment ────────────────────────────────────────────
    # dol_score = S1(competing×0.45) + S2(weakness×0.30) + S3(prior_sweeps×0.25)
    "dol_reject_threshold":     65.0,  # dol_score >= this → hard reject
    "dol_warn_threshold":       40.0,  # dol_score >= this → marginal inducement warning
    "dol_min_origin_zscore":    1.0,   # z-score below this = weak institutional origin
    "dol_min_mass_floor":       0.25,  # liquidity mass below this = insufficient anchor
    "dol_prior_sweep_lookback": 150,   # candles to scan for prior sweeps of same level (~37h on 15m)
    "dol_prior_sweep_lookback_long": 1000,  # long lookback for prior sweep history only
    "dol_s3_ttl_s": 14400,  # cache TTL for long S3 sweep history (4h)
    "dol_decay_halflife":       20.0,  # candles for recency weight to halve
    "inducement_reject_ratio":  1.4,   # S1 legacy ratio — mapped to s1_score internally
    "inducement_warn_ratio":    1.0,   # S1 legacy warn ratio

    # ── DOL scan radius — realised volatility based ───────────────────────
    # radius = dol_rv_k × σ_n × swept_level  (clamped to [min_pct, max_pct])
    # σ_n = rolling std of log returns over dol_rv_window candles.
    # Self-calibrates: calm symbol → narrow radius, volatile → wide radius.
    "dol_rv_window":  20,    # candles for realised vol calculation
    "dol_rv_k":       3.0,   # multiplier: 3 sigma covers typical reachable distance
    "dol_rv_min_pct": 0.01,  # floor: always scan at least 1% of swept level
    "dol_rv_max_pct": 0.06,  # ceiling: never scan more than 6% of swept level


    # ── 15m touch-and-reverse confirmation ───────────────────────────────
    "conf_15m_touch_atr":    1.0,
    "conf_15m_body_ratio":   0.30,
    "conf_15m_scan_candles": 4,

    # ── Swing point detection ─────────────────────────────────────────────
    "swing_atr_significance_multiple": 0.5,
    "swing_same_side_extension_atr":   1.2,
    "swing_same_side_min_separation":  5,
    "swing_trailing_edge_window":      5,

    # ── Equal highs/lows detection ────────────────────────────────────────
    "eq_tolerance_atr_multiple": 0.15,

    # ── Symbol precision defaults (overridden per-symbol via API) ─────────
    "price_place":     4,
    "volume_place":    3,
    "min_trade_num":   0.001,
    "use_exchange_min_order_size": True,
    "size_multiplier": 0.001,

    # ── Funding rate ──────────────────────────────────────────────────────

    # --- 4H structure window overrides ---
    "htf_deep_candle_limit": 200,
    "structure_candles_4h":  200,
    "structure_lookback_4h": 100,
    "structure_use_full_history_4h": False,
    "structure_use_full_history_1h": False,
    "structure_require_major_break": True,
}

# --- Trade Execution Configuration ---
TRADE_CONFIG = {
    # NOTE: this key is read for reference only — main.py and signal_analyzer.py
    # gate live trading directly via os.getenv("ENABLE_LIVE_TRADING", "False").
    # Keep this default in sync: False = paper mode, True = live execution.
    "enable_live_trading": os.getenv("ENABLE_LIVE_TRADING", "False").lower() == "true",
    "risk_per_trade_percent":       0.01,
    "trade_amount_type":            "fixed",
    "fixed_trade_amount":           10,
    "percent_balance_trade_amount": 0.01,
    "max_open_positions":           1,
    "symbol_to_trade":              "BTCUSDT_UMCBL",
    "signal_check_interval_seconds": 10,
    "leverage":                     20,

    # Trailing stop — activation is now controlled by trailing_activation_at_tp1
    # in SIGNAL_CONFIG. trailing_activation_percent is kept as a fallback only.
    "use_trailing_stop":            False,
    "trailing_activation_percent":  3.0,   # fallback only (used when trailing_activation_at_tp1=False)
    "trailing_callback_percent":    0.8,   # how far price can retrace before stop triggers

    "MAIN_LOOP_SLEEP_SECONDS": 10,
}

# --- File Paths ---
SIGNALS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'active_signals.json')

# --- Quant Research Configuration ---
QUANT_RESEARCH_CONFIG = {
    "output_dir": os.getenv(
        "QUANT_OUTPUT_DIR",
        os.path.join(os.path.dirname(__file__), "quant_outputs"),
    ),
    "default_universe_source": os.getenv("QUANT_UNIVERSE_SOURCE", "bitget"),
    "default_15m_limit": int(os.getenv("QUANT_15M_LIMIT", 3600)),
    "default_warmup_candles": int(os.getenv("QUANT_WARMUP_CANDLES", 720)),
    "default_step": int(os.getenv("QUANT_STEP", 3)),
    "default_lookahead_candles": int(os.getenv("QUANT_LOOKAHEAD_CANDLES", 24)),
    "default_max_symbols": int(os.getenv("QUANT_MAX_SYMBOLS", 0)),
    "default_max_concurrency": int(os.getenv("QUANT_MAX_CONCURRENCY", 6)),
    "default_variants": os.getenv(
        "QUANT_VARIANTS",
        "baseline,no_dol,no_major_break,no_15m_gate",
    ),
    "execution_entry_delay_bars": int(os.getenv("QUANT_EXEC_ENTRY_DELAY_BARS", 0)),
    "execution_entry_spread_bps": float(os.getenv("QUANT_EXEC_ENTRY_SPREAD_BPS", 2.0)),
    "execution_entry_slippage_bps_mean": float(os.getenv("QUANT_EXEC_ENTRY_SLIPPAGE_BPS_MEAN", 1.5)),
    "execution_entry_slippage_bps_std": float(os.getenv("QUANT_EXEC_ENTRY_SLIPPAGE_BPS_STD", 2.0)),
    "execution_exit_spread_bps": float(os.getenv("QUANT_EXEC_EXIT_SPREAD_BPS", 2.0)),
    "execution_exit_slippage_bps_mean": float(os.getenv("QUANT_EXEC_EXIT_SLIPPAGE_BPS_MEAN", 1.5)),
    "execution_exit_slippage_bps_std": float(os.getenv("QUANT_EXEC_EXIT_SLIPPAGE_BPS_STD", 2.0)),
    "execution_taker_fee_bps": float(os.getenv("QUANT_EXEC_TAKER_FEE_BPS", 6.0)),
    "execution_default_funding_rate_per_8h": float(os.getenv("QUANT_EXEC_FUNDING_RATE_PER_8H", 0.0001)),
    "execution_missed_fill_probability": float(os.getenv("QUANT_EXEC_MISSED_FILL_PROBABILITY", 0.0)),
    "execution_partial_fill_probability": float(os.getenv("QUANT_EXEC_PARTIAL_FILL_PROBABILITY", 0.0)),
    "execution_partial_fill_min_fraction": float(os.getenv("QUANT_EXEC_PARTIAL_FILL_MIN_FRACTION", 0.5)),
    "execution_tp1_exit_fraction": float(os.getenv("QUANT_EXEC_TP1_EXIT_FRACTION", 0.0)),
    "execution_seed": int(os.getenv("QUANT_EXEC_SEED", 20260320)),
    "execution_intrabar_path_mode": os.getenv("QUANT_EXEC_INTRABAR_PATH_MODE", "conservative"),
    "portfolio_starting_equity": float(os.getenv("QUANT_PORTFOLIO_STARTING_EQUITY", 1000.0)),
    "portfolio_risk_per_trade_percent": float(os.getenv("QUANT_PORTFOLIO_RISK_PER_TRADE_PERCENT", 0.01)),
    "portfolio_max_open_positions": int(os.getenv("QUANT_PORTFOLIO_MAX_OPEN_POSITIONS", TRADE_CONFIG.get("max_open_positions", 1))),
    "portfolio_max_total_risk_percent": float(os.getenv("QUANT_PORTFOLIO_MAX_TOTAL_RISK_PERCENT", 0.03)),
    "portfolio_max_positions_per_cluster": int(os.getenv("QUANT_PORTFOLIO_MAX_POSITIONS_PER_CLUSTER", 1)),
    "portfolio_correlation_lookback_bars": int(os.getenv("QUANT_PORTFOLIO_CORRELATION_LOOKBACK_BARS", 240)),
    "portfolio_correlation_threshold": float(os.getenv("QUANT_PORTFOLIO_CORRELATION_THRESHOLD", 0.75)),
    "split_train_frac": float(os.getenv("QUANT_SPLIT_TRAIN_FRAC", 0.60)),
    "split_validate_frac": float(os.getenv("QUANT_SPLIT_VALIDATE_FRAC", 0.20)),
    "split_test_frac": float(os.getenv("QUANT_SPLIT_TEST_FRAC", 0.20)),
    "walkforward_folds": int(os.getenv("QUANT_WALKFORWARD_FOLDS", 3)),
    "monte_carlo_runs": int(os.getenv("QUANT_MONTE_CARLO_RUNS", 250)),
    "monte_carlo_slippage_noise_bps": float(os.getenv("QUANT_MONTE_CARLO_SLIPPAGE_NOISE_BPS", 1.5)),
}
