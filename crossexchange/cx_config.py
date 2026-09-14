"""
crossexchange/cx_config.py
=============================
Fully additive configuration namespace for the cross-exchange shadow layer.
Deliberately kept SEPARATE from config.SIGNAL_CONFIG so nothing about the
existing, proven configuration parsing / env handling is touched.

Everything defaults to OFF (enable_cross_exchange_shadow=False) so dropping
this package into the bot changes ZERO live behavior until explicitly
enabled.
"""

import os


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return float(val)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    val = os.getenv(name)
    if val is None or not val.strip():
        return default
    return val.strip().lower()


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


CROSSEXCHANGE_CONFIG = {
    # Master switch. Off by default. Flip via env CX_ENABLE_SHADOW=true or edit here.
    # Master switch for the whole cross-exchange layer (live signal basis +
    # shadow telemetry). ON by default per owner decision (2026-09-07): the
    # consolidated book must affect signal generation straight away.
    # Set CX_ENABLE_SHADOW=false to turn the layer off entirely; set
    # CX_LIVE_SIGNAL_MODE=false to keep it recording-only (no signal impact).
    "enable_cross_exchange_shadow": _env_bool("CX_ENABLE_SHADOW", True),

    # Per-venue switches (only matter if the master switch above is on).
    "enable_bitget_view": _env_bool("CX_ENABLE_BITGET_VIEW", True),
    "enable_binance_ws": _env_bool("CX_ENABLE_BINANCE_WS", True),
    "enable_okx_ws": _env_bool("CX_ENABLE_OKX_WS", True),
    "enable_bybit_ws": _env_bool("CX_ENABLE_BYBIT_WS", True),

    "cx_min_contributing_venues": _env_int("CX_MIN_CONTRIBUTING_VENUES", 2),

    # Symbol universe for the shadow layer (canonical symbols). Empty list =
    # derive from whatever the scanner currently feeds the Bitget manager.
    "cx_symbol_universe": [],
    "cx_max_symbols": _env_int("CX_MAX_SYMBOLS", 500),

    # Depth channel sizing.
    "cx_book_depth_levels": _env_int("CX_BOOK_DEPTH_LEVELS", 50),

    # Binance depth basis:
    #   "auto"    — full book via REST snapshot + @depth@100ms diff replay
    #               (the official Binance sync algorithm); automatically
    #               falls back to the pure-WS top-20 partial-depth basis if
    #               REST is blocked (HTTP 451 / unreachable) in the runtime
    #               region.
    #   "diff"    — REST + diff only (no fallback; venue stays unsynced if
    #               REST is unavailable).
    #   "partial" — pure-WS top-20 partial depth only, never touches REST.
    "cx_binance_depth_mode": _env_str("CX_BINANCE_DEPTH_MODE", "partial"),
    # REST snapshot depth for diff mode. Snapped to the nearest valid
    # Binance futures limit (5/10/20/50/100/500/1000). Higher = deeper book
    # but slower initial sync (request weight is paced at ~2/s).
    "cx_binance_rest_depth_limit": _env_int("CX_BINANCE_REST_DEPTH_LIMIT", 100),
    "cx_bucket_size_bps": _env_int("CX_BUCKET_SIZE_BPS", 5),
    "cx_max_bps_range": _env_int("CX_MAX_BPS_RANGE", 100),

    # Reconnection behavior (independent per venue).
    "cx_reconnect_initial_backoff_seconds": _env_float("CX_RECONNECT_INITIAL_BACKOFF", 1.0),
    "cx_reconnect_max_backoff_seconds": _env_float("CX_RECONNECT_MAX_BACKOFF", 30.0),
    "cx_reconnect_jitter_seconds": _env_float("CX_RECONNECT_JITTER", 0.5),
    "cx_stale_after_seconds": _env_float("CX_STALE_AFTER_SECONDS", 15.0),
    "cx_heartbeat_interval_seconds": _env_float("CX_HEARTBEAT_INTERVAL_SECONDS", 15.0),

    # Consolidated warmness / coverage / confirmation thresholds — all
    # calculated ONLY from consolidated valid data, never a per-venue gate.
    "cx_warm_min_seconds": _env_float("CX_WARM_MIN_SECONDS", 20.0),
    "cx_warm_min_trades": _env_int("CX_WARM_MIN_TRADES", 5),
    "cx_coverage_min_depth_usd": _env_float("CX_COVERAGE_MIN_DEPTH_USD", 25_000.0),
    "cx_coverage_bps_window": _env_int("CX_COVERAGE_BPS_WINDOW", 50),
    "cx_confirmation_min_imbalance": _env_float("CX_CONFIRMATION_MIN_IMBALANCE", 0.12),
    "cx_confirmation_min_aggression_ratio": _env_float("CX_CONFIRMATION_MIN_AGGRESSION_RATIO", 0.20),
    "cx_confirmation_trade_lookback_seconds": _env_float("CX_CONFIRMATION_TRADE_LOOKBACK_SECONDS", 60.0),

    # ── LIVE signal mode ────────────────────────────────────────────────
    # When True (default), the consolidated multi-venue book is the LIVE basis
    # for warmness / coverage / confirmation in signal generation, and the
    # confirmation metrics (pressure, imbalance, CVD, aggression) handed to the
    # entry gates are the consolidated ones. Set CX_LIVE_SIGNAL_MODE=false to
    # revert to shadow-only (recording, no signal impact).
    "cx_live_signal_mode": _env_bool("CX_LIVE_SIGNAL_MODE", True),
    # Cap for the dynamic venue-count scaling of absolute-USD thresholds
    # (min trade notional, min trades, coverage depth, CVD opposition).
    # With N contributing venues these USD thresholds are multiplied by
    # min(N, cap) so the richer book does not silently loosen the gates.
    "cx_notional_threshold_scale_cap": _env_float("CX_NOTIONAL_THRESHOLD_SCALE_CAP", 4.0),

    # Shadow diffing cadence.
    "cx_shadow_eval_interval_seconds": _env_float("CX_SHADOW_EVAL_INTERVAL_SECONDS", 10.0),

    # Background command timeout (mirrors of_background_command_timeout_seconds pattern).
    "cx_background_command_timeout_seconds": _env_float("CX_BACKGROUND_COMMAND_TIMEOUT_SECONDS", 15.0),
}
