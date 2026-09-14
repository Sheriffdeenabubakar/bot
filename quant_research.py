import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import sys
import time
from collections import Counter
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from bitget_client import get_futures_tickers
from config import QUANT_RESEARCH_CONFIG, SIGNAL_CONFIG, TRADE_CONFIG
from scanner import scan_coins
from signal_analyzer import (
    SIGNAL_MODE,
    SIGNAL_MODE_PROFILES,
    _check_signal_confirmation,
    _get_filtered_swing_levels_from_df,
    calculate_all_indicators,
    calculate_atr_only,
    calculate_sl_tp,
    check_htf_alignment,
    check_sweep_inducement,
    check_effort_result_divergence,
    classify_market_structure,
    confirm_with_candlesticks,
    detect_consolidation_break,
    detect_breakout_candle,
    detect_internal_bar_breakout,
    detect_market_regime,
    detect_momentum_divergence,
    detect_recent_failed_breakouts,
    detect_liquidity_sweep,
    detect_wyckoff_pattern,
    fetch_data_async,
    get_symbol_config,
    identify_equal_highs_lows,
    identify_swing_points,
    validate_retest,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def safe_log_text(value):
    text = str(value)
    return text.encode("ascii", "backslashreplace").decode("ascii")

REASON_4H_RANGING = "4h_ranging"
REASON_15M_RANGING = "15m_ranging_gate"
REASON_15M_MISSED = "15m_aligned_missed"
REASON_NO_LEVELS = "no_eligible_levels"
REASON_NO_SWEEP = "no_sweep_trigger"
REASON_NO_BREAKOUT = "no_breakout_trigger"
REASON_BREAKOUT_STRUCTURE = "breakout_structure_misaligned"
REASON_BREAKOUT_RETEST = "breakout_retest_failed"
REASON_INVARIANT = "invariant_violation"
REASON_DOL = "dol_reject"
REASON_DATA = "insufficient_data"
REASON_NOT_ENTERED = "not_entered"
REASON_OPEN_AT_DATA_END = "open_at_data_end"
REASON_BUCKET_FILTER = "bucket_filter_blocked"
REASON_PRECISION_FILTER = "precision_filter_blocked"
REASON_PARITY_ENSEMBLE = "parity_static_ensemble_failed"
REASON_PARITY_DATA = "parity_data_unavailable"

SHADOW_TRIGGER_TYPES = ("sweep", "breakout")

MAJOR_SYMBOLS = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "LINK", "AVAX", "DOT", "LTC", "TON", "SUI",
}
EQUITY_INDEXED_SYMBOLS = {
    "AAPL", "AMZN", "ASML", "AVGO", "BABA", "COIN", "COST", "CSCO", "GE", "GOOGL", "HOOD", "IBM", "INTC",
    "JD", "LLY", "MCD", "META", "MSFT", "MRVL", "MSTR", "NVDA", "OXY", "PLTR", "QQQ", "RDDT", "SPY",
    "TSLA", "TSM", "UNH", "WMT", "XOM",
}
METAL_SYMBOLS = {"XAU", "XAG", "XAUT", "XPD", "XPT", "PAXG", "COPPER"}
INDEX_STYLE_SYMBOLS = {"QQQ", "SPY", "EWJ", "EWY"}


@dataclass(frozen=True)
class ExecutionModel:
    entry_delay_bars: int
    entry_spread_bps: float
    entry_slippage_bps_mean: float
    entry_slippage_bps_std: float
    exit_spread_bps: float
    exit_slippage_bps_mean: float
    exit_slippage_bps_std: float
    taker_fee_bps: float
    default_funding_rate_per_8h: float
    missed_fill_probability: float
    partial_fill_probability: float
    partial_fill_min_fraction: float
    tp1_exit_fraction: float
    execution_seed: int
    intrabar_path_mode: str


@dataclass(frozen=True)
class PortfolioModel:
    starting_equity: float
    risk_per_trade_percent: float
    max_open_positions: int
    max_total_risk_percent: float
    max_positions_per_cluster: int
    correlation_lookback_bars: int
    correlation_threshold: float
    train_frac: float
    validate_frac: float
    test_frac: float
    walkforward_folds: int
    monte_carlo_runs: int
    monte_carlo_slippage_noise_bps: float


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    use_dol: bool = True
    enforce_15m_gate: bool = True
    require_major_break: bool = True


VARIANT_MAP = {
    "baseline": Variant("baseline", "Live-equivalent sweep replay."),
    "no_dol": Variant("no_dol", "DOL disabled.", use_dol=False),
    "no_major_break": Variant(
        "no_major_break",
        "Major-break latch disabled.",
        require_major_break=False,
    ),
    "no_15m_gate": Variant(
        "no_15m_gate",
        "15m ranging/alignment gate disabled.",
        enforce_15m_gate=False,
    ),
}


def _merge_override_layers(*layers):
    merged = {}
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        for key, value in layer.items():
            merged[str(key)] = value
    return merged


def _load_json_mapping(path, label):
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must contain a top-level JSON object.")
    return payload


def _extract_override_sections(payload):
    signal_overrides = payload.get("signal_config_overrides", payload.get("signal", {}))
    trade_overrides = payload.get("trade_config_overrides", payload.get("trade", {}))
    research_overrides = payload.get("research_config_overrides", payload.get("research", {}))
    if signal_overrides is None:
        signal_overrides = {}
    if trade_overrides is None:
        trade_overrides = {}
    if research_overrides is None:
        research_overrides = {}
    if not isinstance(signal_overrides, dict):
        raise ValueError("signal_config_overrides must be a JSON object.")
    if not isinstance(trade_overrides, dict):
        raise ValueError("trade_config_overrides must be a JSON object.")
    if not isinstance(research_overrides, dict):
        raise ValueError("research_config_overrides must be a JSON object.")
    meta = {
        key: value
        for key, value in payload.items()
        if key not in {
            "signal_config_overrides",
            "trade_config_overrides",
            "research_config_overrides",
            "signal",
            "trade",
            "research",
            "profiles",
        }
    }
    return signal_overrides, trade_overrides, research_overrides, meta


def resolve_runtime_overrides(args):
    profile_meta = {}
    profile_path = str(getattr(args, "profile_file", "") or "").strip()
    profile_name = str(getattr(args, "profile_name", "") or "").strip()

    profile_signal = {}
    profile_trade = {}
    profile_research = {}

    if profile_path:
        profile_payload = _load_json_mapping(profile_path, "profile file")
        if "profiles" in profile_payload:
            profiles = profile_payload.get("profiles")
            if not isinstance(profiles, dict):
                raise ValueError("profiles must be a JSON object keyed by profile name.")
            if not profile_name:
                raise ValueError("profile-name is required when profile-file contains a profiles object.")
            selected = profiles.get(profile_name)
            if not isinstance(selected, dict):
                raise ValueError(f"profile '{profile_name}' was not found in {profile_path}.")
            profile_signal, profile_trade, profile_research, profile_meta = _extract_override_sections(selected)
        else:
            if profile_name:
                raise ValueError("profile-name was provided but profile-file does not contain a profiles object.")
            profile_signal, profile_trade, profile_research, profile_meta = _extract_override_sections(profile_payload)

    signal_file = str(getattr(args, "signal_config_overrides_file", "") or "").strip()
    trade_file = str(getattr(args, "trade_config_overrides_file", "") or "").strip()
    research_file = str(getattr(args, "research_config_overrides_file", "") or "").strip()

    signal_file_overrides = _load_json_mapping(signal_file, "signal override file") if signal_file else {}
    trade_file_overrides = _load_json_mapping(trade_file, "trade override file") if trade_file else {}
    research_file_overrides = _load_json_mapping(research_file, "research override file") if research_file else {}

    signal_overrides = _merge_override_layers(profile_signal, signal_file_overrides)
    trade_overrides = _merge_override_layers(profile_trade, trade_file_overrides)
    research_overrides = _merge_override_layers(profile_research, research_file_overrides)

    return {
        "profile_file": profile_path,
        "profile_name": profile_name,
        "profile_meta": profile_meta,
        "signal_overrides": signal_overrides,
        "trade_overrides": trade_overrides,
        "research_overrides": research_overrides,
        "signal_config_overrides_file": signal_file,
        "trade_config_overrides_file": trade_file,
        "research_config_overrides_file": research_file,
    }


@contextmanager
def temporary_dict_overrides(target, overrides):
    if not overrides:
        yield
        return
    sentinel = object()
    original = {key: target.get(key, sentinel) for key in overrides}
    target.update(overrides)
    try:
        yield
    finally:
        for key, value in original.items():
            if value is sentinel:
                target.pop(key, None)
            else:
                target[key] = value


@contextmanager
def temporary_signal_overrides(overrides):
    with temporary_dict_overrides(SIGNAL_CONFIG, overrides):
        yield


@contextmanager
def temporary_runtime_config_overrides(signal_overrides=None, trade_overrides=None, research_overrides=None):
    with ExitStack() as stack:
        stack.enter_context(temporary_dict_overrides(SIGNAL_CONFIG, signal_overrides or {}))
        stack.enter_context(temporary_dict_overrides(TRADE_CONFIG, trade_overrides or {}))
        stack.enter_context(temporary_dict_overrides(QUANT_RESEARCH_CONFIG, research_overrides or {}))
        yield


def populate_arg_defaults(args):
    if not str(getattr(args, "universe_source", "") or "").strip():
        args.universe_source = QUANT_RESEARCH_CONFIG.get("default_universe_source", "bitget")
    if args.limit is None:
        args.limit = int(QUANT_RESEARCH_CONFIG.get("default_15m_limit", 1200))
    if args.warmup is None:
        args.warmup = int(QUANT_RESEARCH_CONFIG.get("default_warmup_candles", 720))
    if args.step is None:
        args.step = int(QUANT_RESEARCH_CONFIG.get("default_step", 3))
    if args.lookahead is None:
        args.lookahead = int(QUANT_RESEARCH_CONFIG.get("default_lookahead_candles", 24))
    if args.max_symbols is None:
        args.max_symbols = int(QUANT_RESEARCH_CONFIG.get("default_max_symbols", 0))
    if args.max_concurrency is None:
        args.max_concurrency = int(QUANT_RESEARCH_CONFIG.get("default_max_concurrency", 6))
    if args.operational_15m_limit is None:
        args.operational_15m_limit = int(SIGNAL_CONFIG.get("operational_15m_candle_limit", 300))
    if args.directional_15m_limit is None:
        args.directional_15m_limit = int(
            SIGNAL_CONFIG.get(
                "directional_15m_candle_limit",
                SIGNAL_CONFIG.get("operational_15m_candle_limit", 300),
            )
        )
    if not str(getattr(args, "structure_15m_limits", "") or "").strip():
        args.structure_15m_limits = str(SIGNAL_CONFIG.get("structure_15m_candle_limit", 1000))
    if not str(getattr(args, "variants", "") or "").strip():
        args.variants = QUANT_RESEARCH_CONFIG.get(
            "default_variants",
            "baseline,no_dol,no_major_break,no_15m_gate",
        )
    if not str(getattr(args, "output_dir", "") or "").strip():
        args.output_dir = QUANT_RESEARCH_CONFIG.get("output_dir")
    return args


def config_snapshot(mapping):
    return {str(key): mapping[key] for key in sorted(mapping)}


def stable_object_hash(payload):
    normalized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def write_json_file(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def parse_args():
    parser = argparse.ArgumentParser(description="Quant-style sweep replay runner.")
    parser.add_argument(
        "--universe-source",
        choices=("bitget", "scanner"),
        default="",
    )
    parser.add_argument("--symbols", default="")
    parser.add_argument("--symbols-file", default="", help="Optional newline- or comma-delimited symbols file for research runs.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--warmup", type=int, default=None)
    parser.add_argument("--step", type=int, default=None)
    parser.add_argument("--lookahead", type=int, default=None)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--max-concurrency", type=int, default=None)
    parser.add_argument(
        "--operational-15m-limit",
        type=int,
        default=None,
        help="Research-only 15m operational window used for sweep/trigger context.",
    )
    parser.add_argument(
        "--directional-15m-limit",
        type=int,
        default=None,
        help="Research-only 15m directional window used for the 15m structure gate.",
    )
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument(
        "--structure-15m-limits",
        default="",
        help="Comma-separated deep 15m structure windows to test for swing-level extraction.",
    )
    parser.add_argument("--variants", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--profile-file", default="", help="Optional JSON profile file defining signal/trade/research overrides.")
    parser.add_argument("--profile-name", default="", help="Optional profile key to select when profile-file contains a profiles map.")
    parser.add_argument("--signal-config-overrides-file", default="", help="Optional JSON file of SIGNAL_CONFIG overrides.")
    parser.add_argument("--trade-config-overrides-file", default="", help="Optional JSON file of TRADE_CONFIG overrides.")
    parser.add_argument("--research-config-overrides-file", default="", help="Optional JSON file of QUANT_RESEARCH_CONFIG overrides.")
    return parser.parse_args()


def get_variants(raw_names):
    variants = []
    for raw_name in raw_names.split(","):
        name = raw_name.strip()
        if not name:
            continue
        if name not in VARIANT_MAP:
            raise ValueError(f"Unknown variant: {name}")
        variants.append(VARIANT_MAP[name])
    if not variants:
        raise ValueError("At least one variant is required.")
    return variants


def get_structure_15m_limits(raw_limits):
    limits = []
    for raw in str(raw_limits).split(","):
        raw = raw.strip()
        if not raw:
            continue
        value = int(raw)
        if value < 200:
            raise ValueError("Each structure 15m limit must be at least 200 candles.")
        limits.append(value)
    if not limits:
        raise ValueError("At least one structure 15m limit is required.")
    return list(dict.fromkeys(limits))


def get_universe(args):
    requested_symbols = []
    if str(getattr(args, "symbols_file", "")).strip():
        with open(str(args.symbols_file).strip(), "r", encoding="utf-8") as handle:
            raw = handle.read()
        requested_symbols = list(dict.fromkeys(s.strip().upper() for s in raw.replace("\n", ",").split(",") if s.strip()))
    elif args.symbols.strip():
        requested_symbols = list(dict.fromkeys(s.strip().upper() for s in args.symbols.split(",") if s.strip()))

    if requested_symbols:
        if args.universe_source == "scanner":
            ranked = sorted(scan_coins(), key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)
        elif args.universe_source == "bitget":
            ranked = sorted(get_futures_tickers() or [], key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)
        else:
            ranked = []

        if ranked:
            metadata = {}
            for item in ranked:
                symbol = item.get("symbol")
                if not symbol or symbol not in requested_symbols:
                    continue
                metadata[symbol] = {
                    "symbol": symbol,
                    "quote_volume": float(item.get("quoteVolume", 0) or 0),
                    "symbol_bucket": classify_symbol_bucket(symbol),
                }
            for symbol in requested_symbols:
                metadata.setdefault(
                    symbol,
                    {
                        "symbol": symbol,
                        "quote_volume": np.nan,
                        "symbol_bucket": classify_symbol_bucket(symbol),
                    },
                )
            return requested_symbols, metadata

        metadata = {
            symbol: {
                "symbol": symbol,
                "quote_volume": np.nan,
                "symbol_bucket": classify_symbol_bucket(symbol),
            }
            for symbol in requested_symbols
        }
        return requested_symbols, metadata

    if args.universe_source == "scanner":
        ranked = sorted(scan_coins(), key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)
    else:
        ranked = sorted(get_futures_tickers() or [], key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)

    symbols = []
    metadata = {}
    for item in ranked:
        symbol = str(item.get("symbol") or "").upper().strip()
        if not symbol or symbol in metadata:
            continue
        symbols.append(symbol)
        metadata[symbol] = {
            "symbol": symbol,
            "quote_volume": float(item.get("quoteVolume", 0) or 0),
        }
    return symbols, metadata


def close_time_df(df):
    result = df.copy()
    result.index = result.index + pd.Timedelta(minutes=15)
    result.index.name = "close_time"
    return result


def resample_ohlcv(df, rule):
    return (
        df.resample(rule, label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close", "volume"])
    )


def get_atr_col(df):
    atr_period = SIGNAL_CONFIG.get("atr_period", 14)
    for col in (f"ATRr_{atr_period}", f"ATR_{atr_period}", "ATR"):
        if col in df.columns and not df[col].dropna().empty:
            return col
    return None


def classify_bucket(value, low_threshold, high_threshold):
    if np.isnan(value):
        return "unknown"
    if value <= low_threshold:
        return "low"
    if value >= high_threshold:
        return "high"
    return "medium"


def base_symbol(symbol):
    text = str(symbol or "").upper().replace("_UMCBL", "")
    return text[:-4] if text.endswith("USDT") else text


def classify_symbol_bucket(symbol):
    root = base_symbol(symbol)
    if root in INDEX_STYLE_SYMBOLS:
        return "index"
    if root in EQUITY_INDEXED_SYMBOLS:
        return "equity"
    if root in METAL_SYMBOLS:
        return "metal"
    if root in MAJOR_SYMBOLS:
        return "major"
    return "alt"


def classify_session(ts):
    hour = pd.Timestamp(ts).hour
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 13:
        return "europe"
    if 13 <= hour < 21:
        return "us"
    return "late"


def classify_trend_regime(structure_4h, adx_regime):
    if structure_4h == "ranging":
        return "ranging"
    if adx_regime == "high":
        return "trending"
    if adx_regime == "low":
        return "weak_trend"
    return "transition"


def _normalize_precision_rule(raw):
    if not isinstance(raw, dict):
        return {}
    normalized = {}
    for key, value in raw.items():
        norm_key = str(key or "").strip().lower()
        norm_value = str(value or "").strip().lower()
        if norm_key and norm_value:
            normalized[norm_key] = norm_value
    return normalized


def _format_precision_rule(rule):
    normalized = _normalize_precision_rule(rule)
    if not normalized:
        return ""
    return " & ".join(f"{key}={value}" for key, value in normalized.items())


def _precision_rule_matches(context, raw_rule):
    rule = _normalize_precision_rule(raw_rule)
    if not rule:
        return False
    for key, expected in rule.items():
        actual = str(context.get(key) or "").strip().lower()
        if actual != expected:
            return False
    return True


def _evaluate_precision_filter_rejection(
    trigger_label,
    enabled,
    regime_source,
    allowed_regimes,
    hard_block_rules,
    allow_any_rules,
    precision_context,
    trend_regime,
    live_regime,
):
    if not enabled:
        return None, None

    source = str(regime_source or "live").strip().lower()
    allowed = [str(value).strip() for value in allowed_regimes if str(value).strip()]
    regime_for_precision = trend_regime if source == "research_parity" else live_regime
    if allowed and regime_for_precision not in allowed:
        return (
            REASON_PRECISION_FILTER,
            f"Precision {trigger_label.lower()} filter blocked regime {regime_for_precision} "
            f"(source={source}, allowed={allowed}).",
        )

    for raw_rule in hard_block_rules:
        if _precision_rule_matches(precision_context, raw_rule):
            return (
                REASON_PRECISION_FILTER,
                f"Precision {trigger_label.lower()} filter blocked context rule {_format_precision_rule(raw_rule)}.",
            )

    normalized_allow_rules = [_normalize_precision_rule(raw_rule) for raw_rule in allow_any_rules]
    normalized_allow_rules = [rule for rule in normalized_allow_rules if rule]
    if normalized_allow_rules and not any(_precision_rule_matches(precision_context, rule) for rule in normalized_allow_rules):
        return (
            REASON_PRECISION_FILTER,
            f"Precision {trigger_label.lower()} filter blocked context: none of the allowed rules matched "
            f"({'; '.join(_format_precision_rule(rule) for rule in normalized_allow_rules)}).",
        )

    return None, None


def evaluate_live_filter_rejection(
    trigger_type,
    symbol_bucket,
    session_bucket,
    trend_regime,
    live_regime,
    structure_4h,
    structure_15m,
    vol_regime,
    liquidity_bucket,
):
    if trigger_type == "sweep" and SIGNAL_CONFIG.get("enable_bucket_filter", False):
        blocked_symbol_buckets = {
            str(value).strip().lower()
            for value in SIGNAL_CONFIG.get("blocked_symbol_buckets", [])
            if str(value).strip()
        }
        blocked_symbol_session = set()
        for raw in SIGNAL_CONFIG.get("blocked_symbol_session_buckets", []):
            text = str(raw).strip().lower()
            if ":" not in text:
                continue
            bucket, session = text.split(":", 1)
            if bucket and session:
                blocked_symbol_session.add((bucket.strip(), session.strip()))

        symbol_bucket_key = str(symbol_bucket or "").strip().lower()
        session_bucket_key = str(session_bucket or "").strip().lower()
        if symbol_bucket_key in blocked_symbol_buckets:
            return (
                REASON_BUCKET_FILTER,
                f"Bucket filter blocked symbol bucket {symbol_bucket_key}.",
            )
        if (symbol_bucket_key, session_bucket_key) in blocked_symbol_session:
            return (
                REASON_BUCKET_FILTER,
                f"Bucket filter blocked {symbol_bucket_key}:{session_bucket_key}.",
            )

    precision_context = {
        "symbol_bucket": symbol_bucket,
        "session_bucket": session_bucket,
        "trend_regime": trend_regime,
        "live_regime": live_regime,
        "structure_4h": structure_4h,
        "structure_15m": structure_15m,
        "vol_regime": vol_regime,
        "liquidity_bucket": liquidity_bucket,
    }
    if trigger_type == "sweep":
        filter_reason = _evaluate_precision_filter_rejection(
            "Sweep",
            SIGNAL_CONFIG.get("enable_precision_sweep_filter", False),
            SIGNAL_CONFIG.get("precision_sweep_regime_source", "live"),
            SIGNAL_CONFIG.get("precision_sweep_allowed_market_regimes", []) or [],
            SIGNAL_CONFIG.get("precision_sweep_hard_block_rules", []) or [],
            SIGNAL_CONFIG.get("precision_sweep_allow_any_rules", []) or [],
            precision_context,
            trend_regime,
            live_regime,
        )
        if filter_reason[0]:
            return filter_reason
    elif trigger_type == "breakout":
        filter_reason = _evaluate_precision_filter_rejection(
            "Breakout",
            SIGNAL_CONFIG.get("enable_precision_breakout_filter", False),
            SIGNAL_CONFIG.get("precision_breakout_regime_source", "live"),
            SIGNAL_CONFIG.get("precision_breakout_allowed_market_regimes", []) or [],
            SIGNAL_CONFIG.get("precision_breakout_hard_block_rules", []) or [],
            SIGNAL_CONFIG.get("precision_breakout_allow_any_rules", []) or [],
            precision_context,
            trend_regime,
            live_regime,
        )
        if filter_reason[0]:
            return filter_reason

    return None, None


def _evaluate_contextual_soft_bypass(context, trigger_type, *, enabled_key, rules_key_suffix, label):
    if not bool(SIGNAL_CONFIG.get(enabled_key, False)):
        return False, ""

    trigger_token = str(trigger_type or "").strip().lower()
    if trigger_token not in ("sweep", "breakout"):
        return False, ""

    rules_key = f"{trigger_token}_{rules_key_suffix}"
    for raw_rule in SIGNAL_CONFIG.get(rules_key, []) or []:
        if _precision_rule_matches(context, raw_rule):
            return True, f"{label} soft-bypassed via {_format_precision_rule(raw_rule)}"
    return False, ""



def evaluate_contextual_static_ensemble_soft_bypass(context, trigger_type):
    return _evaluate_contextual_soft_bypass(
        context,
        trigger_type,
        enabled_key="enable_contextual_static_ensemble_soft_bypass",
        rules_key_suffix="static_ensemble_soft_bypass_rules",
        label="Static ensemble",
    )


def build_liquidity_thresholds(metadata_map):
    volumes = [float(meta.get("quote_volume", np.nan)) for meta in metadata_map.values() if pd.notna(meta.get("quote_volume", np.nan))]
    if not volumes:
        return (np.nan, np.nan)
    return tuple(float(v) for v in pd.Series(volumes).quantile([0.33, 0.67]).tolist())


def classify_liquidity_bucket(quote_volume, thresholds):
    low, high = thresholds
    return classify_bucket(float(quote_volume) if pd.notna(quote_volume) else np.nan, low, high)


def stable_seed(*parts):
    digest = hashlib.sha256("||".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % (2 ** 32)


def build_execution_model():
    return ExecutionModel(
        entry_delay_bars=int(QUANT_RESEARCH_CONFIG.get("execution_entry_delay_bars", 0)),
        entry_spread_bps=float(QUANT_RESEARCH_CONFIG.get("execution_entry_spread_bps", 2.0)),
        entry_slippage_bps_mean=float(QUANT_RESEARCH_CONFIG.get("execution_entry_slippage_bps_mean", 1.5)),
        entry_slippage_bps_std=float(QUANT_RESEARCH_CONFIG.get("execution_entry_slippage_bps_std", 2.0)),
        exit_spread_bps=float(QUANT_RESEARCH_CONFIG.get("execution_exit_spread_bps", 2.0)),
        exit_slippage_bps_mean=float(QUANT_RESEARCH_CONFIG.get("execution_exit_slippage_bps_mean", 1.5)),
        exit_slippage_bps_std=float(QUANT_RESEARCH_CONFIG.get("execution_exit_slippage_bps_std", 2.0)),
        taker_fee_bps=float(QUANT_RESEARCH_CONFIG.get("execution_taker_fee_bps", 6.0)),
        default_funding_rate_per_8h=float(QUANT_RESEARCH_CONFIG.get("execution_default_funding_rate_per_8h", 0.0001)),
        missed_fill_probability=float(QUANT_RESEARCH_CONFIG.get("execution_missed_fill_probability", 0.0)),
        partial_fill_probability=float(QUANT_RESEARCH_CONFIG.get("execution_partial_fill_probability", 0.0)),
        partial_fill_min_fraction=float(QUANT_RESEARCH_CONFIG.get("execution_partial_fill_min_fraction", 0.5)),
        tp1_exit_fraction=float(QUANT_RESEARCH_CONFIG.get("execution_tp1_exit_fraction", 0.0)),
        execution_seed=int(QUANT_RESEARCH_CONFIG.get("execution_seed", 20260320)),
        intrabar_path_mode=str(QUANT_RESEARCH_CONFIG.get("execution_intrabar_path_mode", "conservative")),
    )


def build_portfolio_model():
    return PortfolioModel(
        starting_equity=float(QUANT_RESEARCH_CONFIG.get("portfolio_starting_equity", 1000.0)),
        risk_per_trade_percent=float(QUANT_RESEARCH_CONFIG.get("portfolio_risk_per_trade_percent", 0.01)),
        max_open_positions=int(QUANT_RESEARCH_CONFIG.get("portfolio_max_open_positions", TRADE_CONFIG.get("max_open_positions", 1))),
        max_total_risk_percent=float(QUANT_RESEARCH_CONFIG.get("portfolio_max_total_risk_percent", 0.03)),
        max_positions_per_cluster=int(QUANT_RESEARCH_CONFIG.get("portfolio_max_positions_per_cluster", 1)),
        correlation_lookback_bars=int(QUANT_RESEARCH_CONFIG.get("portfolio_correlation_lookback_bars", 240)),
        correlation_threshold=float(QUANT_RESEARCH_CONFIG.get("portfolio_correlation_threshold", 0.75)),
        train_frac=float(QUANT_RESEARCH_CONFIG.get("split_train_frac", 0.60)),
        validate_frac=float(QUANT_RESEARCH_CONFIG.get("split_validate_frac", 0.20)),
        test_frac=float(QUANT_RESEARCH_CONFIG.get("split_test_frac", 0.20)),
        walkforward_folds=int(QUANT_RESEARCH_CONFIG.get("walkforward_folds", 3)),
        monte_carlo_runs=int(QUANT_RESEARCH_CONFIG.get("monte_carlo_runs", 250)),
        monte_carlo_slippage_noise_bps=float(QUANT_RESEARCH_CONFIG.get("monte_carlo_slippage_noise_bps", 1.5)),
    )


def build_context(df_15m):
    close_15m = close_time_df(df_15m)
    ind_15m = calculate_all_indicators(close_15m.copy())
    atr_col = get_atr_col(ind_15m)
    atr_pct = ((ind_15m[atr_col] / ind_15m["close"]) if atr_col else pd.Series(0.0, index=ind_15m.index)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    adx = ind_15m["adx"].replace([np.inf, -np.inf], np.nan).fillna(0.0) if "adx" in ind_15m.columns else pd.Series(0.0, index=ind_15m.index)
    vol_low, vol_high = atr_pct.quantile([0.33, 0.67]).tolist()
    adx_low, adx_high = adx.quantile([0.33, 0.67]).tolist()
    return {
        "close_15m": close_15m,
        "ind_15m": ind_15m,
        "atr_pct": atr_pct,
        "adx": adx,
        "vol_thresholds": (float(vol_low), float(vol_high)),
        "adx_thresholds": (float(adx_low), float(adx_high)),
    }


def build_structure_df(prefix_df):
    work = prefix_df.copy()
    work = identify_swing_points(work)
    return work


def get_structure(prefix_df, lookback):
    if prefix_df is None or prefix_df.empty:
        return "ranging", None, None, {}
    return classify_market_structure(prefix_df.copy(), lookback=lookback)


def _same_window(left, right):
    return len(left) == len(right) and left.index.equals(right.index)


def prepare_state_context(op_prefix_15m, direction_prefix_15m, level_prefix_15m, prefix_4h, require_major_break=True, structure_15m_limit=None):
    overrides = {"structure_require_major_break": require_major_break}
    if structure_15m_limit is not None:
        overrides["structure_15m_candle_limit"] = int(structure_15m_limit)

    with temporary_signal_overrides(overrides):
        df_15m = identify_equal_highs_lows(build_structure_df(op_prefix_15m))
        if _same_window(direction_prefix_15m, op_prefix_15m):
            df_15m_direction = df_15m
        else:
            df_15m_direction = build_structure_df(direction_prefix_15m)
        if _same_window(level_prefix_15m, op_prefix_15m):
            df_15m_levels = df_15m
        elif _same_window(level_prefix_15m, direction_prefix_15m):
            df_15m_levels = df_15m_direction
        else:
            df_15m_levels = build_structure_df(level_prefix_15m)
        df_4h = build_structure_df(calculate_atr_only(prefix_4h.copy()))

        structure_15m, _, _, detail_15m = get_structure(df_15m_direction, SIGNAL_CONFIG.get("structure_lookback_15m", 300))
        structure_4h, _, _, detail_4h = get_structure(df_4h, SIGNAL_CONFIG.get("structure_lookback_4h", 100))

        atr_col = get_atr_col(df_15m)
        atr_value = float(df_15m[atr_col].iloc[-1]) if atr_col and not pd.isna(df_15m[atr_col].iloc[-1]) else float(df_15m["close"].iloc[-1]) * 0.005
        highs_15m, lows_15m = _get_filtered_swing_levels_from_df(df_15m_levels)
        highs_4h, lows_4h = _get_filtered_swing_levels_from_df(df_4h)

        return {
            "structure_15m": structure_15m,
            "structure_4h": structure_4h,
            "detail_15m": detail_15m or {},
            "detail_4h": detail_4h or {},
            "eligible_high_count": len(highs_15m),
            "eligible_low_count": len(lows_15m),
            "operational_15m_window": len(df_15m),
            "direction_15m_window": len(df_15m_direction),
            "level_15m_window": len(df_15m_levels),
            "atr_value": atr_value,
            "current_price": float(df_15m["close"].iloc[-1]),
            "_df_15m": df_15m,
            "_df_15m_levels": df_15m_levels,
            "_df_4h": df_4h,
            "_highs_15m": highs_15m,
            "_lows_15m": lows_15m,
            "_highs_4h": highs_4h,
            "_lows_4h": lows_4h,
            "_sweep_cache": None,
            "_dol_cache": None,
            "_breakout_cache": None,
        }

def materialize_state(context):
    return {
        "structure_15m": context["structure_15m"],
        "structure_4h": context["structure_4h"],
        "detail_15m": context.get("detail_15m") or {},
        "detail_4h": context.get("detail_4h") or {},
        "eligible_high_count": int(context.get("eligible_high_count", 0)),
        "eligible_low_count": int(context.get("eligible_low_count", 0)),
        "operational_15m_window": int(context.get("operational_15m_window", 0)),
        "direction_15m_window": int(context.get("direction_15m_window", 0)),
        "level_15m_window": int(context.get("level_15m_window", 0)),
        "atr_value": float(context.get("atr_value", 0.0)),
        "current_price": float(context.get("current_price", 0.0)),
        "expected_direction": None,
        "expected_sweep_type": None,
        "sweep_type": None,
        "swept_level": None,
        "sweep_candle_idx": None,
        "invariant_ok": True,
        "no_signal_reason": None,
        "dol_reject": False,
        "dol_score": None,
        "dol_reason": None,
        "trigger_type": None,
        "breakout_idx": None,
        "breakout_level": None,
        "breakout_direction": None,
        "breakout_retest_valid": None,
        "breakout_retest_reason": None,
        "breakout_er_score": None,
    }


def ensure_sweep_state(context, symbol):
    if context.get("_sweep_cache") is not None:
        return context["_sweep_cache"]

    structure_4h = context["structure_4h"]
    highs_15m = context["_highs_15m"]
    lows_15m = context["_lows_15m"]
    state = {
        "expected_direction": None,
        "expected_sweep_type": None,
        "sweep_type": None,
        "swept_level": None,
        "sweep_candle_idx": None,
        "invariant_ok": True,
        "candidate_detected": False,
        "ready": False,
        "no_signal_reason": None,
    }
    if structure_4h not in ("bullish", "bearish"):
        state["no_signal_reason"] = REASON_4H_RANGING
        context["_sweep_cache"] = state
        return state

    if structure_4h == "bearish":
        sweep_zones = {"highs": highs_15m, "lows": []}
        state["expected_direction"] = "SELL"
        state["expected_sweep_type"] = "bearish_sweep"
        if not highs_15m:
            state["no_signal_reason"] = REASON_NO_LEVELS
            context["_sweep_cache"] = state
            return state
    else:
        sweep_zones = {"highs": [], "lows": lows_15m}
        state["expected_direction"] = "BUY"
        state["expected_sweep_type"] = "bullish_sweep"
        if not lows_15m:
            state["no_signal_reason"] = REASON_NO_LEVELS
            context["_sweep_cache"] = state
            return state

    sweep_type, swept_level, sweep_candle_idx = detect_liquidity_sweep(context["_df_15m"], sweep_zones, symbol)
    state["sweep_type"] = sweep_type
    state["swept_level"] = float(swept_level) if swept_level is not None else None
    state["sweep_candle_idx"] = int(sweep_candle_idx) if sweep_candle_idx is not None else None
    state["candidate_detected"] = bool(sweep_type)
    if sweep_type is None:
        state["no_signal_reason"] = REASON_NO_SWEEP
    elif sweep_type != state["expected_sweep_type"]:
        state["invariant_ok"] = False
        state["no_signal_reason"] = REASON_INVARIANT
    state["ready"] = bool(state["candidate_detected"] and state["invariant_ok"])
    context["_sweep_cache"] = state
    return state


def ensure_dol_state(context, sweep_state):
    if context.get("_dol_cache") is not None:
        return context["_dol_cache"]

    state = {"dol_reject": False, "dol_score": None, "dol_reason": None}
    if not sweep_state.get("sweep_type") or not sweep_state.get("invariant_ok", True):
        context["_dol_cache"] = state
        return state

    dol_reject, dol_score, dol_reason, _ = check_sweep_inducement(
        context["_df_15m"],
        sweep_state["sweep_type"],
        float(sweep_state["swept_level"]),
        context["atr_value"],
        htf_levels={
            "highs": context["_highs_15m"],
            "lows": context["_lows_15m"],
            "htf_highs": context["_highs_4h"],
            "htf_lows": context["_lows_4h"],
        },
        df_htf=context["_df_4h"],
        df_s3=context["_df_15m_levels"],
        sweep_candle_idx=sweep_state["sweep_candle_idx"],
    )
    state["dol_reject"] = bool(dol_reject)
    state["dol_score"] = float(dol_score) if dol_score is not None else None
    state["dol_reason"] = dol_reason
    context["_dol_cache"] = state
    return state


def ensure_breakout_state(context):
    if context.get("_breakout_cache") is not None:
        return context["_breakout_cache"]

    df_15m = context["_df_15m"]
    structure_15m = context["structure_15m"]
    state = {
        "trigger_type": "breakout",
        "breakout_idx": None,
        "breakout_level": None,
        "breakout_direction": None,
        "structure_alignment_ok": True,
        "retest_valid": False,
        "retest_reason": None,
        "er_score": None,
        "candidate_detected": False,
        "ready": False,
        "no_signal_reason": None,
    }

    breakout_idx, breakout_level, breakout_direction = detect_breakout_candle(df_15m)
    state["breakout_idx"] = int(breakout_idx) if breakout_idx is not None else None
    state["breakout_level"] = float(breakout_level) if breakout_level is not None else None
    state["breakout_direction"] = breakout_direction
    state["candidate_detected"] = bool(breakout_direction)

    if not breakout_direction:
        state["no_signal_reason"] = REASON_NO_BREAKOUT
        context["_breakout_cache"] = state
        return state

    if structure_15m == "bearish" and breakout_direction == "BUY":
        state["structure_alignment_ok"] = False
        state["no_signal_reason"] = REASON_BREAKOUT_STRUCTURE
    elif structure_15m == "bullish" and breakout_direction == "SELL":
        state["structure_alignment_ok"] = False
        state["no_signal_reason"] = REASON_BREAKOUT_STRUCTURE

    if breakout_idx is not None and breakout_level is not None:
        retest_valid, retest_reason = validate_retest(
            df_15m,
            int(breakout_idx),
            float(breakout_level),
            breakout_direction,
        )
        state["retest_valid"] = bool(retest_valid)
        state["retest_reason"] = retest_reason
        if not retest_valid and state["no_signal_reason"] is None:
            state["no_signal_reason"] = REASON_BREAKOUT_RETEST

        breakout_slice = df_15m.iloc[int(breakout_idx): min(int(breakout_idx) + 4, len(df_15m))]
        if not breakout_slice.empty:
            state["er_score"] = float(check_effort_result_divergence(breakout_slice, breakout_direction))

    state["ready"] = bool(
        state["candidate_detected"]
        and state["structure_alignment_ok"]
        and state["retest_valid"]
    )
    context["_breakout_cache"] = state
    return state



def _adx_strength_label(adx_value):
    adx_value = float(adx_value) if pd.notna(adx_value) else 0.0
    strong_threshold = float(SIGNAL_CONFIG.get("adx_trending_strong", 35))
    weak_threshold = float(SIGNAL_CONFIG.get("adx_trending_weak", 25))
    if adx_value >= strong_threshold:
        return "strong"
    if adx_value >= weak_threshold:
        return "weak"
    return "ranging"


def _classify_historical_tf_trend(df, fallback="ranging", lookback=None):
    if df is None or df.empty:
        return str(fallback or "ranging")
    try:
        df_struct = identify_swing_points(df.copy())
        structure, _, _, _ = classify_market_structure(
            df_struct,
            lookback=int(lookback or SIGNAL_CONFIG.get("structure_lookback_1h", 30)),
        )
        return str(structure or fallback or "ranging")
    except Exception:
        return str(fallback or "ranging")


def build_historical_htf_context(prefix_1h, prefix_4h, structure_4h):
    htf_data = {}
    if prefix_4h is not None and not prefix_4h.empty:
        last_4h = prefix_4h.iloc[-1]
        htf_data["4H"] = {
            "rsi": float(last_4h.get("rsi", 50.0) or 50.0),
            "adx": float(last_4h.get("adx", 0.0) or 0.0),
            "trend": str(structure_4h or "ranging"),
            "strength": _adx_strength_label(last_4h.get("adx", 0.0)),
        }
    if prefix_1h is not None and not prefix_1h.empty:
        last_1h = prefix_1h.iloc[-1]
        htf_data["1H"] = {
            "rsi": float(last_1h.get("rsi", 50.0) or 50.0),
            "adx": float(last_1h.get("adx", 0.0) or 0.0),
            "trend": _classify_historical_tf_trend(prefix_1h, fallback="ranging"),
            "strength": _adx_strength_label(last_1h.get("adx", 0.0)),
        }
    return htf_data



def evaluate_historical_path_parity(
    context,
    trade_state,
    signal_ts,
    close_price,
    prefix_1h,
    prefix_4h,
    symbol_config,
    live_regime,
):
    trigger_type = str(trade_state.get("trigger_type") or "sweep")
    direction = str(trade_state.get("expected_direction") or trade_state.get("direction") or "")
    if direction not in ("BUY", "SELL"):
        return {
            "accepted": False,
            "reason_code": REASON_PARITY_DATA,
            "reason_text": "Invalid direction for parity evaluation.",
            "voters": {},
            "total_votes": 0,
            "ensemble_min": 0,
            "ensemble_passed": False,
            "entry_result": {
                "confirmed": False,
                "entry_price": np.nan,
                "entry_time": pd.NaT,
                "reason": "Invalid direction",
                "reason_code": REASON_PARITY_DATA,
            },
        }

    mode_profile = SIGNAL_MODE_PROFILES[SIGNAL_MODE]
    df_15m = context["_df_15m"]
    breakout_state = ensure_breakout_state(context)
    htf_data = build_historical_htf_context(prefix_1h, prefix_4h, context.get("structure_4h"))

    htf_trigger_type = "sweep" if trigger_type == "sweep" else "breakout"
    htf_check_direction = direction
    htf_score = float(check_htf_alignment(htf_data, htf_check_direction, trigger_type=htf_trigger_type) or 0.0)

    wyckoff_pattern, wyckoff_conf, phase_info = detect_wyckoff_pattern(
        df_15m,
        higher_tf_data=htf_data,
        is_bullish=(direction == "BUY"),
    )
    candle_pattern = confirm_with_candlesticks(df_15m, is_bullish=(direction == "BUY"))
    recent_confirmed, recent_confirmation_score = _check_signal_confirmation(df_15m, direction)

    consolidation_break = None
    try:
        consolidation_break, _, _ = detect_consolidation_break(
            df_15m,
            is_bullish=(direction == "BUY"),
            symbol_config=symbol_config,
        )
    except Exception:
        consolidation_break = None

    ibo_type = None
    if trigger_type == "breakout" and breakout_state.get("breakout_idx") is not None:
        try:
            ibo_type, _ = detect_internal_bar_breakout(
                df_15m,
                is_bullish=(direction == "BUY"),
                anchor_idx=breakout_state.get("breakout_idx"),
            )
        except Exception:
            ibo_type = None

    div_type, div_strength = (None, 0)
    if trigger_type != "sweep":
        try:
            div_type, div_strength = detect_momentum_divergence(df_15m, is_bullish=(direction == "BUY"))
        except Exception:
            div_type, div_strength = (None, 0)

    failed_structure_support = False
    try:
        failed_structure_support, _ = detect_recent_failed_breakouts(
            df_15m,
            direction,
            lookback=96,
            post_window=10,
            reject_threshold=mode_profile["failed_structure_reject_threshold"],
        )
    except Exception:
        failed_structure_support = False

    regime = str(live_regime or "ranging")
    if trigger_type == "sweep":
        wyckoff_phase_c = (
            phase_info.get("Phase C (Spring)", 0) >= 25
            or phase_info.get("Phase C (UTAD)", 0) >= 25
        ) if wyckoff_pattern else False
        voters = {
            "liquidity_sweep": 4,
            "agreeing_breakout": 2 if (breakout_state.get("breakout_direction") and breakout_state.get("breakout_direction") == direction) else 0,
            "wyckoff_phase_c": 5 if wyckoff_phase_c else (3 if wyckoff_pattern else 0),
            "htf_exhaustion": 3 if htf_score >= 1.5 else (1 if htf_score >= 0.5 else 0),
            "order_flow": 0,
            "consolidation": 1 if consolidation_break else 0,
            "candle": 2 if candle_pattern else 0,
            "divergence": 0,
            "failed_structure": 2 if failed_structure_support else 0,
            "trend": 1 if "trending" in regime else 0,
        }
        ensemble_min = int(mode_profile["ensemble_min_static_sweep"])
    else:
        wyckoff_sos = (
            phase_info.get("Phase E (Markup)", 0) >= 15
            or phase_info.get("Phase D (SOS)", 0) >= 15
            or phase_info.get("Phase E (Markdown)", 0) >= 15
            or phase_info.get("Phase D (SOW)", 0) >= 15
        ) if wyckoff_pattern else False
        er_score = float(trade_state.get("breakout_er_score") or trade_state.get("er_score") or 0.0)
        voters = {
            "liquidity_sweep": 0,
            "breakout_retest": (5 if er_score > 0.3 else (3 if er_score >= -0.1 else 2)) if breakout_state.get("breakout_direction") else 0,
            "wyckoff_sos": 4 if wyckoff_sos else (2 if wyckoff_pattern else 0),
            "htf_alignment": 3 if htf_score >= 1.5 else (2 if htf_score >= 0.5 else 0),
            "order_flow": 0,
            "consolidation": 3 if consolidation_break else 0,
            "ibo": 2 if ibo_type else 0,
            "candle": 1 if candle_pattern else 0,
            "divergence": 2 if div_type else 0,
            "trend": 3 if regime == "trending_strong" else (2 if regime == "trending_weak" else 0),
        }
        ensemble_min = int(mode_profile["ensemble_min_static_breakout"])

    parity_context = {
        "symbol_bucket": context.get("symbol_bucket"),
        "session_bucket": context.get("session_bucket"),
        "trend_regime": context.get("trend_regime"),
        "live_regime": live_regime,
        "structure_4h": context.get("structure_4h"),
        "structure_15m": context.get("structure_15m"),
        "vol_regime": context.get("vol_regime"),
        "liquidity_bucket": context.get("liquidity_bucket"),
    }
    total_votes = int(sum(voters.values()))
    ensemble_soft_bypassed = False
    ensemble_soft_bypass_reason = ""
    ensemble_passed = bool(total_votes >= ensemble_min)
    if not ensemble_passed:
        ensemble_soft_bypassed, ensemble_soft_bypass_reason = evaluate_contextual_static_ensemble_soft_bypass(
            parity_context,
            trigger_type,
        )
        ensemble_passed = bool(ensemble_soft_bypassed)

    entry_result = {
        "confirmed": False,
        "entry_price": np.nan,
        "entry_time": pd.NaT,
        "reason": "Static ensemble rejected before entry replay",
        "reason_code": REASON_PARITY_ENSEMBLE,
    }
    accepted = False
    reason_code = REASON_PARITY_ENSEMBLE
    reason_text = (
        f"Static ensemble too low ({total_votes} votes, required {ensemble_min}, path={trigger_type})."
    )
    if ensemble_passed:
        if ensemble_soft_bypassed:
            reason_text = (
                f"{ensemble_soft_bypass_reason} "
                f"({total_votes} votes, required {ensemble_min}, path={trigger_type})."
            )
        entry_result = {
            "confirmed": True,
            "entry_price": float(close_price) if close_price is not None else np.nan,
            "entry_time": pd.Timestamp(signal_ts),
            "reason": "Entry replay uses decision-candle close after static parity acceptance",
            "reason_code": "entry_replay_ready",
        }
        accepted = True
        reason_code = "shadow_signal"
        reason_text = (
            f"Historical parity accepted ({trigger_type}) - "
            f"{entry_result.get('reason')}"
        )

    return {
        "accepted": accepted,
        "reason_code": reason_code,
        "reason_text": reason_text,
        "voters": voters,
        "total_votes": total_votes,
        "ensemble_min": ensemble_min,
        "ensemble_passed": ensemble_passed,
        "ensemble_soft_bypassed": ensemble_soft_bypassed,
        "ensemble_soft_bypass_reason": ensemble_soft_bypass_reason if ensemble_soft_bypassed else "",
        "htf_score": round(float(htf_score), 4),
        "htf_trigger_type": htf_trigger_type,
        "htf_check_direction": htf_check_direction,
        "wyckoff_pattern": wyckoff_pattern,
        "wyckoff_conf": float(wyckoff_conf or 0.0),
        "phase_info": phase_info or {},
        "candle_pattern": candle_pattern,
        "recent_confirmation_passed": bool(recent_confirmed),
        "recent_confirmation_score": round(float(recent_confirmation_score or 0.0), 4),
        "consolidation_break": consolidation_break,
        "ibo_type": ibo_type,
        "divergence_type": div_type,
        "divergence_strength": int(div_strength or 0),
        "entry_result": entry_result,
    }


def safe_div(numerator, denominator):
    denominator = float(denominator or 0.0)
    return float(numerator) / denominator if denominator else 0.0


def round_price(value, price_place):
    return round(float(value), int(price_place))


def round_size(size, symbol_config):
    step = float(symbol_config.get("size_multiplier", 0.001) or 0.001)
    volume_place = int(symbol_config.get("volume_place", 3))
    if step <= 0:
        return round(float(size), volume_place)
    floored = math.floor(max(0.0, float(size)) / step) * step
    return round(floored, volume_place)


def sample_slippage_bps(rng, mean_bps, std_bps):
    return max(0.0, float(rng.normal(float(mean_bps), float(std_bps))))


def entry_fill_price(base_price, direction, execution_model, price_place, rng):
    spread_bps = float(execution_model.entry_spread_bps) / 2.0
    slip_bps = sample_slippage_bps(rng, execution_model.entry_slippage_bps_mean, execution_model.entry_slippage_bps_std)
    total_bps = spread_bps + slip_bps
    if direction == "BUY":
        return round_price(base_price * (1.0 + total_bps / 10000.0), price_place), total_bps
    return round_price(base_price * (1.0 - total_bps / 10000.0), price_place), total_bps


def exit_fill_price(level_price, direction, execution_model, price_place, rng):
    spread_bps = float(execution_model.exit_spread_bps) / 2.0
    slip_bps = sample_slippage_bps(rng, execution_model.exit_slippage_bps_mean, execution_model.exit_slippage_bps_std)
    total_bps = spread_bps + slip_bps
    if direction == "BUY":
        return round_price(level_price * (1.0 - total_bps / 10000.0), price_place), total_bps
    return round_price(level_price * (1.0 + total_bps / 10000.0), price_place), total_bps


def get_live_trailing_settings():
    use_trailing = bool(SIGNAL_CONFIG.get("use_trailing_stop", TRADE_CONFIG.get("use_trailing_stop", True)))
    activation_pct = float(SIGNAL_CONFIG.get("trailing_activation_percent", TRADE_CONFIG.get("trailing_activation_percent", 3.5)))
    callback_pct = float(SIGNAL_CONFIG.get("trailing_callback_percent", TRADE_CONFIG.get("trailing_callback_percent", 0.8)))
    activation_at_tp1 = bool(SIGNAL_CONFIG.get("trailing_activation_at_tp1", False))
    return use_trailing, activation_pct, callback_pct, activation_at_tp1


def build_intrabar_points(row, direction, mode):
    open_price = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    close = float(row["close"])
    mode = str(mode or "conservative").lower()
    if mode == "optimistic":
        return [open_price, high, low, close] if direction == "BUY" else [open_price, low, high, close]
    if mode == "ohlc":
        return [open_price, high, low, close] if close >= open_price else [open_price, low, high, close]
    return [open_price, low, high, close] if direction == "BUY" else [open_price, high, low, close]


def summarize_leg_pnl(direction, entry_fill, exit_fill, qty_fraction):
    if direction == "BUY":
        return (float(exit_fill) - float(entry_fill)) * float(qty_fraction)
    return (float(entry_fill) - float(exit_fill)) * float(qty_fraction)


def simulate_trade_path(
    symbol,
    df_15m,
    start_pos,
    state,
    lookahead,
    symbol_config,
    execution_model,
    entry_time_override=None,
    entry_price_override=None,
):
    direction = state.get("expected_direction") or state.get("direction")
    trigger_type = str(state.get("trigger_type") or "sweep")
    price_place = int(symbol_config.get("price_place", 4))
    if direction not in ("BUY", "SELL"):
        return {
            "trade_status": REASON_NOT_ENTERED,
            "entry_reason": "invalid_direction",
            "entered": False,
            "filled_fraction": 0.0,
            "first_touch": REASON_NOT_ENTERED,
            "tp1_hit_any": False,
            "tp2_hit_any": False,
            "sl_hit_any": False,
            "ambiguous_touch": False,
            "bars_to_first_touch": None,
            "mfe_r": 0.0,
            "mae_r": 0.0,
        }
    rng = np.random.default_rng(stable_seed(execution_model.execution_seed, symbol, start_pos, direction))
    if entry_time_override is not None:
        entry_anchor_pos = int(df_15m.index.searchsorted(pd.Timestamp(entry_time_override), side="right"))
        entry_pos = int(entry_anchor_pos) + max(0, int(execution_model.entry_delay_bars))
    else:
        entry_pos = int(start_pos) + 1 + max(0, int(execution_model.entry_delay_bars))
    if entry_pos >= len(df_15m):
        return {
            "trade_status": REASON_NOT_ENTERED,
            "entry_reason": "entry_window_exhausted",
            "entered": False,
            "filled_fraction": 0.0,
            "first_touch": REASON_NOT_ENTERED,
            "tp1_hit_any": False,
            "tp2_hit_any": False,
            "sl_hit_any": False,
            "ambiguous_touch": False,
            "bars_to_first_touch": None,
            "mfe_r": 0.0,
            "mae_r": 0.0,
        }

    if rng.random() < float(execution_model.missed_fill_probability):
        return {
            "trade_status": REASON_NOT_ENTERED,
            "entry_reason": "missed_fill",
            "entered": False,
            "filled_fraction": 0.0,
            "first_touch": REASON_NOT_ENTERED,
            "tp1_hit_any": False,
            "tp2_hit_any": False,
            "sl_hit_any": False,
            "ambiguous_touch": False,
            "bars_to_first_touch": None,
            "mfe_r": 0.0,
            "mae_r": 0.0,
        }

    actual_entry_time = pd.Timestamp(entry_time_override) if entry_time_override is not None else pd.Timestamp(df_15m.index[entry_pos])
    base_entry = (
        float(entry_price_override)
        if entry_price_override is not None and pd.notna(entry_price_override)
        else float(df_15m["open"].iloc[entry_pos])
    )
    filled_fraction = 1.0
    if rng.random() < float(execution_model.partial_fill_probability):
        filled_fraction = float(rng.uniform(float(execution_model.partial_fill_min_fraction), 1.0))

    entry_fill, entry_total_bps = entry_fill_price(base_entry, direction, execution_model, price_place, rng)
    calc_df = df_15m.iloc[:entry_pos + 1].copy()
    atr_col = get_atr_col(calc_df)
    atr_value = float(calc_df[atr_col].iloc[-1]) if atr_col and not pd.isna(calc_df[atr_col].iloc[-1]) else float(calc_df["close"].iloc[-1]) * 0.005
    sl, tp1, tp2, trailing_sl = calculate_sl_tp(
        entry_fill,
        direction,
        atr_value,
        calc_df,
        price_place,
        swept_level=state.get("swept_level"),
        trigger_type=trigger_type,
    )
    if sl is None or tp1 is None or tp2 is None:
        return {
            "trade_status": REASON_NOT_ENTERED,
            "entry_reason": "invalid_geometry",
            "entered": False,
            "filled_fraction": 0.0,
            "first_touch": REASON_NOT_ENTERED,
            "tp1_hit_any": False,
            "tp2_hit_any": False,
            "sl_hit_any": False,
            "ambiguous_touch": False,
            "bars_to_first_touch": None,
            "mfe_r": 0.0,
            "mae_r": 0.0,
        }

    risk_per_unit = abs(float(entry_fill) - float(sl))
    if risk_per_unit <= 0:
        return {
            "trade_status": REASON_NOT_ENTERED,
            "entry_reason": "invalid_risk",
            "entered": False,
            "filled_fraction": 0.0,
            "first_touch": REASON_NOT_ENTERED,
            "tp1_hit_any": False,
            "tp2_hit_any": False,
            "sl_hit_any": False,
            "ambiguous_touch": False,
            "bars_to_first_touch": None,
            "mfe_r": 0.0,
            "mae_r": 0.0,
        }

    use_trailing, activation_pct, callback_pct, activation_at_tp1 = get_live_trailing_settings()
    trailing_active = False
    trailing_activated = False
    trail_stop = None
    best_price = float(entry_fill)
    activation_price = float(tp1) if activation_at_tp1 else (
        float(entry_fill) * (1.0 + activation_pct / 100.0) if direction == "BUY" else float(entry_fill) * (1.0 - activation_pct / 100.0)
    )
    activation_price = round_price(activation_price, price_place)
    tp1_exit_fraction = min(max(0.0, float(execution_model.tp1_exit_fraction)), filled_fraction)
    tp1_done = False
    remaining_fraction = float(filled_fraction)
    entry_fee_per_unit = float(entry_fill) * float(execution_model.taker_fee_bps) / 10000.0 * filled_fraction
    exit_fee_per_unit = 0.0
    funding_per_unit = 0.0
    gross_pnl_per_unit = 0.0
    realized_fraction = 0.0
    weighted_exit_price = 0.0
    exit_reason = REASON_OPEN_AT_DATA_END
    exit_time = df_15m.index[min(len(df_15m) - 1, entry_pos)]
    bars_to_first_touch = None
    first_touch = "none"
    tp1_hit_any = False
    tp2_hit_any = False
    sl_hit_any = False
    trailing_exit = False
    partial_exit = False
    legs = []
    mfe_r = 0.0
    mae_r = 0.0
    future = df_15m.iloc[entry_pos:min(len(df_15m), entry_pos + max(1, int(lookahead)))]

    def current_stop_price():
        if direction == "BUY":
            return max(float(sl), float(trail_stop) if trailing_active and trail_stop is not None else -np.inf)
        return min(float(sl), float(trail_stop) if trailing_active and trail_stop is not None else np.inf)

    def funding_cost(entry_px, qty_fraction, holding_hours):
        notional = float(entry_px) * float(qty_fraction)
        return abs(float(execution_model.default_funding_rate_per_8h)) * notional * max(0.0, float(holding_hours)) / 8.0

    def realize_fraction(level_price, qty_fraction, reason, bar_offset):
        nonlocal remaining_fraction, exit_fee_per_unit, funding_per_unit, gross_pnl_per_unit
        nonlocal realized_fraction, weighted_exit_price, exit_reason, exit_time, first_touch, bars_to_first_touch
        if qty_fraction <= 0 or remaining_fraction <= 0:
            return False
        qty_fraction = min(float(qty_fraction), float(remaining_fraction))
        exit_fill, exit_bps = exit_fill_price(level_price, direction, execution_model, price_place, rng)
        exit_fee = float(exit_fill) * float(execution_model.taker_fee_bps) / 10000.0 * qty_fraction
        hold_bars = max(1, int(bar_offset))
        hold_hours = hold_bars * 0.25
        funding = funding_cost(entry_fill, qty_fraction, hold_hours)
        gross = summarize_leg_pnl(direction, entry_fill, exit_fill, qty_fraction)
        legs.append(
            {
                "reason": reason,
                "qty_fraction": round(float(qty_fraction), 6),
                "exit_price": float(exit_fill),
                "exit_bps": round(float(exit_bps), 4),
                "holding_bars": hold_bars,
                "holding_hours": round(float(hold_hours), 4),
            }
        )
        gross_pnl_per_unit += gross
        exit_fee_per_unit += exit_fee
        funding_per_unit += funding
        weighted_exit_price += float(exit_fill) * float(qty_fraction)
        realized_fraction += float(qty_fraction)
        remaining_fraction -= float(qty_fraction)
        exit_reason = reason
        exit_time = current_bar_time
        if first_touch == "none":
            first_touch = reason
            bars_to_first_touch = int(bar_offset)
        return True

    done = False
    for bar_offset, (current_bar_time, row) in enumerate(future.iterrows(), start=1):
        high = float(row["high"])
        low = float(row["low"])
        if direction == "BUY":
            mfe_r = max(mfe_r, max(0.0, high - float(entry_fill)) / risk_per_unit)
            mae_r = max(mae_r, max(0.0, float(entry_fill) - low) / risk_per_unit)
        else:
            mfe_r = max(mfe_r, max(0.0, float(entry_fill) - low) / risk_per_unit)
            mae_r = max(mae_r, max(0.0, high - float(entry_fill)) / risk_per_unit)

        for seg_start, seg_end in zip(build_intrabar_points(row, direction, execution_model.intrabar_path_mode), build_intrabar_points(row, direction, execution_model.intrabar_path_mode)[1:]):
            seg_start = float(seg_start)
            seg_end = float(seg_end)
            move_up = seg_end >= seg_start
            cursor = seg_start
            while not done:
                events = []
                stop_price = current_stop_price()
                if direction == "BUY":
                    if move_up:
                        if not tp1_done and tp1_exit_fraction > 0 and cursor < float(tp1) <= seg_end:
                            events.append(("tp1", float(tp1)))
                        if use_trailing and not trailing_active and cursor < float(activation_price) <= seg_end:
                            events.append(("activate", float(activation_price)))
                        if remaining_fraction > 0 and cursor < float(tp2) <= seg_end:
                            events.append(("tp2", float(tp2)))
                    else:
                        if remaining_fraction > 0 and seg_end <= stop_price < cursor:
                            events.append(("stop", float(stop_price)))
                else:
                    if move_up:
                        if remaining_fraction > 0 and cursor < stop_price <= seg_end:
                            events.append(("stop", float(stop_price)))
                    else:
                        if not tp1_done and tp1_exit_fraction > 0 and seg_end <= float(tp1) < cursor:
                            events.append(("tp1", float(tp1)))
                        if use_trailing and not trailing_active and seg_end <= float(activation_price) < cursor:
                            events.append(("activate", float(activation_price)))
                        if remaining_fraction > 0 and seg_end <= float(tp2) < cursor:
                            events.append(("tp2", float(tp2)))

                if not events:
                    if trailing_active:
                        if direction == "BUY" and move_up:
                            best_price = max(float(best_price), float(seg_end))
                            trail_stop = round_price(best_price * (1.0 - callback_pct / 100.0), price_place)
                        elif direction == "SELL" and not move_up:
                            best_price = min(float(best_price), float(seg_end))
                            trail_stop = round_price(best_price * (1.0 + callback_pct / 100.0), price_place)
                    break

                next_event = min(events, key=lambda item: item[1]) if move_up else max(events, key=lambda item: item[1])
                event_name, event_price = next_event
                cursor = float(event_price)
                if event_name == "tp1":
                    partial_exit = True
                    tp1_hit_any = True
                    tp1_done = True
                    realize_fraction(float(tp1), tp1_exit_fraction, "tp1", bar_offset)
                    if activation_at_tp1 and use_trailing and remaining_fraction > 0 and not trailing_active:
                        trailing_active = True
                        trailing_activated = True
                        best_price = float(tp1) if direction == "BUY" else float(tp1)
                        trail_stop = round_price(best_price * (1.0 - callback_pct / 100.0), price_place) if direction == "BUY" else round_price(best_price * (1.0 + callback_pct / 100.0), price_place)
                elif event_name == "activate":
                    trailing_active = True
                    trailing_activated = True
                    best_price = float(event_price)
                    trail_stop = round_price(best_price * (1.0 - callback_pct / 100.0), price_place) if direction == "BUY" else round_price(best_price * (1.0 + callback_pct / 100.0), price_place)
                elif event_name == "tp2":
                    tp1_hit_any = True
                    tp2_hit_any = True
                    realize_fraction(float(tp2), remaining_fraction, "tp2", bar_offset)
                    done = True
                else:
                    sl_hit_any = True
                    trailing_exit = trailing_active and trail_stop is not None and (
                        (direction == "BUY" and float(stop_price) > float(sl)) or
                        (direction == "SELL" and float(stop_price) < float(sl))
                    )
                    realize_fraction(float(stop_price), remaining_fraction, "trailing_stop" if trailing_exit else "sl", bar_offset)
                    done = True

                if remaining_fraction <= 1e-9:
                    done = True
                if done:
                    break

        if done:
            break

    unresolved_on_data_end = remaining_fraction > 1e-9
    mark_close = float(future["close"].iloc[-1]) if not future.empty else float(df_15m["close"].iloc[entry_pos])
    avg_exit_price = safe_div(weighted_exit_price, realized_fraction) if realized_fraction > 0 else np.nan
    net_pnl_per_unit = gross_pnl_per_unit - entry_fee_per_unit - exit_fee_per_unit - funding_per_unit
    gross_r = safe_div(gross_pnl_per_unit, risk_per_unit) if realized_fraction > 0 else np.nan
    net_r = safe_div(net_pnl_per_unit, risk_per_unit) if realized_fraction > 0 else np.nan
    if direction == "BUY":
        unrealized_mtm_r = safe_div(mark_close - float(entry_fill), risk_per_unit)
    else:
        unrealized_mtm_r = safe_div(float(entry_fill) - mark_close, risk_per_unit)
    if unresolved_on_data_end:
        return {
            "trade_status": REASON_OPEN_AT_DATA_END,
            "entry_reason": "filled",
            "entered": True,
            "resolved": False,
            "open_at_data_end": True,
            "filled_fraction": round(float(filled_fraction), 6),
            "entry_index": int(entry_pos),
            "entry_time": actual_entry_time,
            "entry_price": round(float(entry_fill), 8),
            "signal_reference_price": round(float(df_15m["close"].iloc[start_pos]), 8),
            "stop_loss": float(sl),
            "take_profit_1": float(tp1),
            "take_profit_2": float(tp2),
            "trailing_stop_loss": float(trailing_sl),
            "trailing_activation_price": float(activation_price) if use_trailing else np.nan,
            "trailing_activated": bool(trailing_activated),
            "trailing_exit": bool(trailing_exit),
            "partial_exit": bool(partial_exit),
            "partial_exit_fraction": round(float(tp1_exit_fraction), 6),
            "exit_time": pd.NaT,
            "exit_price": np.nan,
            "exit_reason": REASON_OPEN_AT_DATA_END,
            "holding_bars": int(max(1, len(future))),
            "holding_hours": round(float(max(0.25, len(future) * 0.25)), 4),
            "risk_per_unit": round(float(risk_per_unit), 8),
            "effective_risk_per_unit": round(float(risk_per_unit * filled_fraction), 8),
            "gross_pnl_per_unit": round(float(gross_pnl_per_unit), 8),
            "net_pnl_per_unit": round(float(net_pnl_per_unit), 8),
            "gross_r": round(float(gross_r), 4) if pd.notna(gross_r) else np.nan,
            "net_r": round(float(net_r), 4) if pd.notna(net_r) else np.nan,
            "entry_fee_per_unit": round(float(entry_fee_per_unit), 8),
            "exit_fee_per_unit": round(float(exit_fee_per_unit), 8),
            "funding_per_unit": round(float(funding_per_unit), 8),
            "entry_execution_bps": round(float(entry_total_bps), 4),
            "first_touch": first_touch,
            "tp1_hit_any": bool(tp1_hit_any),
            "tp2_hit_any": bool(tp2_hit_any),
            "sl_hit_any": bool(sl_hit_any),
            "ambiguous_touch": False,
            "bars_to_first_touch": bars_to_first_touch,
            "mfe_r": round(float(mfe_r), 4),
            "mae_r": round(float(mae_r), 4),
            "legs_json": json.dumps(legs, default=str),
            "realized_fraction": round(float(realized_fraction), 6),
            "remaining_fraction": round(float(remaining_fraction), 6),
            "mark_price_at_data_end": round(float(mark_close), 8),
            "unrealized_mtm_r": round(float(unrealized_mtm_r), 4),
        }
    return {
        "trade_status": "entered",
        "entry_reason": "filled",
        "entered": True,
        "resolved": True,
        "open_at_data_end": False,
        "filled_fraction": round(float(filled_fraction), 6),
        "entry_index": int(entry_pos),
        "entry_time": actual_entry_time,
        "entry_price": round(float(entry_fill), 8),
        "signal_reference_price": round(float(df_15m["close"].iloc[start_pos]), 8),
        "stop_loss": float(sl),
        "take_profit_1": float(tp1),
        "take_profit_2": float(tp2),
        "trailing_stop_loss": float(trailing_sl),
        "trailing_activation_price": float(activation_price) if use_trailing else np.nan,
        "trailing_activated": bool(trailing_activated),
        "trailing_exit": bool(trailing_exit),
        "partial_exit": bool(partial_exit),
        "partial_exit_fraction": round(float(tp1_exit_fraction), 6),
        "exit_time": exit_time,
        "exit_price": round(float(avg_exit_price), 8),
        "exit_reason": exit_reason,
        "holding_bars": int(max(1, len(legs) and max(leg["holding_bars"] for leg in legs) or 1)),
        "holding_hours": round(float(max((leg["holding_hours"] for leg in legs), default=0.25)), 4),
        "risk_per_unit": round(float(risk_per_unit), 8),
        "effective_risk_per_unit": round(float(risk_per_unit * filled_fraction), 8),
        "gross_pnl_per_unit": round(float(gross_pnl_per_unit), 8),
        "net_pnl_per_unit": round(float(net_pnl_per_unit), 8),
        "gross_r": round(float(gross_r), 4),
        "net_r": round(float(net_r), 4),
        "entry_fee_per_unit": round(float(entry_fee_per_unit), 8),
        "exit_fee_per_unit": round(float(exit_fee_per_unit), 8),
        "funding_per_unit": round(float(funding_per_unit), 8),
        "entry_execution_bps": round(float(entry_total_bps), 4),
        "first_touch": first_touch,
        "tp1_hit_any": bool(tp1_hit_any),
        "tp2_hit_any": bool(tp2_hit_any),
        "sl_hit_any": bool(sl_hit_any),
        "ambiguous_touch": False,
        "bars_to_first_touch": bars_to_first_touch,
        "mfe_r": round(float(mfe_r), 4),
        "mae_r": round(float(mae_r), 4),
        "legs_json": json.dumps(legs, default=str),
        "realized_fraction": round(float(realized_fraction), 6),
        "remaining_fraction": round(float(remaining_fraction), 6),
        "mark_price_at_data_end": round(float(avg_exit_price), 8) if pd.notna(avg_exit_price) else np.nan,
        "unrealized_mtm_r": 0.0,
    }


def classify_shadow_replay_outcome(trade_result):
    if not isinstance(trade_result, dict) or not trade_result:
        return "not_replayed"
    trade_status = str(trade_result.get("trade_status") or "")
    if trade_status == REASON_NOT_ENTERED:
        return "not_entered"
    if trade_status == REASON_OPEN_AT_DATA_END:
        unrealized = pd.to_numeric(pd.Series([trade_result.get("unrealized_mtm_r")]), errors="coerce").fillna(0.0).iloc[0]
        if unrealized > 0:
            return "open_positive"
        if unrealized < 0:
            return "open_negative"
        return "open_flat"
    net_r = pd.to_numeric(pd.Series([trade_result.get("net_r")]), errors="coerce").iloc[0]
    if pd.notna(net_r):
        if net_r > 0:
            return "winner"
        if net_r < 0:
            return "loser"
        return "flat"
    return "unknown"


def build_event(
    variant,
    structure_15m_limit,
    symbol,
    ts,
    state,
    decision,
    reason_code,
    reason_text,
    close_price,
    atr_pct,
    adx_value,
    vol_regime,
    adx_regime,
    symbol_bucket,
    liquidity_bucket,
    session_bucket,
    trend_regime,
    quote_volume,
    entry_fields=None,
    outcome_fields=None,
):
    event = {
        "variant": variant.name,
        "structure_15m_limit": int(structure_15m_limit),
        "experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}",
        "symbol": symbol,
        "timestamp": ts.isoformat(),
        "decision": decision,
        "reason_code": reason_code,
        "reason_text": reason_text,
        "trigger_type": state.get("trigger_type"),
        "structure_4h": state.get("structure_4h"),
        "structure_15m": state.get("structure_15m"),
        "expected_direction": state.get("expected_direction"),
        "expected_sweep_type": state.get("expected_sweep_type"),
        "sweep_type": state.get("sweep_type"),
        "swept_level": state.get("swept_level"),
        "sweep_candle_idx": state.get("sweep_candle_idx"),
        "breakout_idx": state.get("breakout_idx"),
        "breakout_level": state.get("breakout_level"),
        "breakout_direction": state.get("breakout_direction"),
        "breakout_retest_valid": state.get("breakout_retest_valid"),
        "breakout_retest_reason": state.get("breakout_retest_reason"),
        "breakout_er_score": state.get("breakout_er_score"),
        "eligible_high_count": state.get("eligible_high_count", 0),
        "eligible_low_count": state.get("eligible_low_count", 0),
        "operational_15m_window": state.get("operational_15m_window"),
        "direction_15m_window": state.get("direction_15m_window"),
        "level_15m_window": state.get("level_15m_window"),
        "invariant_ok": bool(state.get("invariant_ok", True)),
        "dol_reject": bool(state.get("dol_reject", False)),
        "dol_score": state.get("dol_score"),
        "close_price": round(float(close_price), 8),
        "atr_value": round(float(state.get("atr_value", 0.0)), 8),
        "atr_pct": round(float(atr_pct), 6),
        "adx": round(float(adx_value), 4),
        "vol_regime": vol_regime,
        "adx_regime": adx_regime,
        "trend_regime": trend_regime,
        "symbol_bucket": symbol_bucket,
        "liquidity_bucket": liquidity_bucket,
        "session_bucket": session_bucket,
        "quote_volume": round(float(quote_volume), 2) if pd.notna(quote_volume) else np.nan,
        "structure_alignment_pair": f"4h={state.get('structure_4h')}|15m={state.get('structure_15m')}",
        "structure_same_direction": (
            state.get("structure_4h") == state.get("structure_15m")
            and state.get("structure_4h") in ("bullish", "bearish")
        ),
        "structure_opposed_direction": (
            state.get("structure_4h") in ("bullish", "bearish")
            and state.get("structure_15m") in ("bullish", "bearish")
            and state.get("structure_4h") != state.get("structure_15m")
        ),
        "variant_enforce_15m_gate": bool(getattr(variant, "enforce_15m_gate", True)),
        "variant_require_major_break": bool(getattr(variant, "require_major_break", True)),
        "htf_deep_candle_limit_used": int(SIGNAL_CONFIG.get("htf_deep_candle_limit", 400)),
        "htf_1h_deep_candle_limit_used": int(SIGNAL_CONFIG.get("htf_1h_deep_candle_limit", 600)),
        "candlestick_limit_used": int(SIGNAL_CONFIG.get("candlestick_limit", 400)),
        "sweep_htf_trend_alignment_enabled": bool(SIGNAL_CONFIG.get("sweep_htf_trend_alignment", True)),
        "regime_key": (
            f"4h={state.get('structure_4h')}|15m={state.get('structure_15m')}"
            f"|trend={trend_regime}|vol={vol_regime}|liq={liquidity_bucket}|session={session_bucket}"
        ),
    }
    if entry_fields:
        event.update(entry_fields)
    if outcome_fields:
        event.update(outcome_fields)
    return event


def _base_shadow_event(
    variant,
    structure_15m_limit,
    symbol,
    ts,
    trigger_type,
    state,
    close_price,
    atr_pct,
    adx_value,
    vol_regime,
    adx_regime,
    symbol_bucket,
    liquidity_bucket,
    session_bucket,
    trend_regime,
    quote_volume,
):
    return {
        "variant": variant.name,
        "structure_15m_limit": int(structure_15m_limit),
        "experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}",
        "path_experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}|path={trigger_type}",
        "symbol": symbol,
        "timestamp": ts.isoformat(),
        "trigger_type": trigger_type,
        "structure_4h": state.get("structure_4h"),
        "structure_15m": state.get("structure_15m"),
        "close_price": round(float(close_price), 8),
        "atr_value": round(float(state.get("atr_value", 0.0)), 8),
        "atr_pct": round(float(atr_pct), 6),
        "adx": round(float(adx_value), 4),
        "vol_regime": vol_regime,
        "adx_regime": adx_regime,
        "trend_regime": trend_regime,
        "symbol_bucket": symbol_bucket,
        "liquidity_bucket": liquidity_bucket,
        "session_bucket": session_bucket,
        "quote_volume": round(float(quote_volume), 2) if pd.notna(quote_volume) else np.nan,
        "structure_alignment_pair": f"4h={state.get('structure_4h')}|15m={state.get('structure_15m')}",
        "structure_same_direction": (
            state.get("structure_4h") == state.get("structure_15m")
            and state.get("structure_4h") in ("bullish", "bearish")
        ),
        "structure_opposed_direction": (
            state.get("structure_4h") in ("bullish", "bearish")
            and state.get("structure_15m") in ("bullish", "bearish")
            and state.get("structure_4h") != state.get("structure_15m")
        ),
        "htf_deep_candle_limit_used": int(SIGNAL_CONFIG.get("htf_deep_candle_limit", 400)),
        "htf_1h_deep_candle_limit_used": int(SIGNAL_CONFIG.get("htf_1h_deep_candle_limit", 600)),
        "candlestick_limit_used": int(SIGNAL_CONFIG.get("candlestick_limit", 400)),
        "sweep_htf_trend_alignment_enabled": bool(SIGNAL_CONFIG.get("sweep_htf_trend_alignment", True)),
    }


def build_shadow_path_rows(
    variant,
    structure_15m_limit,
    symbol,
    ts,
    context,
    full_df_15m,
    pos,
    symbol_config,
    execution_model,
    lookahead,
    close_price,
    atr_pct,
    adx_value,
    vol_regime,
    adx_regime,
    symbol_bucket,
    liquidity_bucket,
    session_bucket,
    trend_regime,
    live_regime,
    quote_volume,
    prefix_1h,
    prefix_4h,
):
    shadow_events = []
    shadow_trades = []

    sweep_state = ensure_sweep_state(context, symbol)
    dol_state = ensure_dol_state(context, sweep_state)
    breakout_state = ensure_breakout_state(context)

    current_sweep_filter_code = current_sweep_filter_text = None
    if sweep_state.get("candidate_detected", False) or sweep_state.get("sweep_type"):
        current_sweep_filter_code, current_sweep_filter_text = evaluate_live_filter_rejection(
            "sweep",
            symbol_bucket,
            session_bucket,
            trend_regime,
            live_regime,
            context.get("structure_4h"),
            context.get("structure_15m"),
            vol_regime,
            liquidity_bucket,
        )
    current_breakout_filter_code = current_breakout_filter_text = None
    if breakout_state.get("breakout_direction") in ("BUY", "SELL"):
        current_breakout_filter_code, current_breakout_filter_text = evaluate_live_filter_rejection(
            "breakout",
            symbol_bucket,
            session_bucket,
            trend_regime,
            live_regime,
            context.get("structure_4h"),
            context.get("structure_15m"),
            vol_regime,
            liquidity_bucket,
        )

    path_rows = []

    sweep_detected = bool(sweep_state.get("candidate_detected", False))
    sweep_ready = bool(sweep_detected and sweep_state.get("invariant_ok", True))
    sweep_reason_code = sweep_state.get("no_signal_reason")
    sweep_reason_text = (
        "Sweep candidate detected."
        if sweep_ready
        else (
            "Sweep direction violated invariant."
            if sweep_reason_code == REASON_INVARIANT
            else "No valid sweep trigger detected."
        )
    )
    sweep_condition_flags = {
        "candidate_detected": sweep_detected,
        "invariant_ok": bool(sweep_state.get("invariant_ok", True)),
        "expected_direction_defined": sweep_state.get("expected_direction") in ("BUY", "SELL"),
        "has_swept_level": sweep_state.get("swept_level") is not None,
        "has_sweep_candle": sweep_state.get("sweep_candle_idx") is not None,
        "dol_reject": bool(dol_state.get("dol_reject", False)),
        "dol_score_positive": pd.notna(dol_state.get("dol_score")) and float(dol_state.get("dol_score") or 0.0) > 0.0,
        "current_15m_ranging_blocked": context.get("structure_15m") == "ranging",
        "current_15m_aligned_blocked": (
            context.get("structure_4h") == context.get("structure_15m")
            and context.get("structure_4h") in ("bullish", "bearish")
        ),
        "current_live_filter_blocked": bool(current_sweep_filter_code),
    }
    sweep_diagnostics = {
        "expected_direction": sweep_state.get("expected_direction"),
        "expected_sweep_type": sweep_state.get("expected_sweep_type"),
        "sweep_type": sweep_state.get("sweep_type"),
        "swept_level": sweep_state.get("swept_level"),
        "sweep_candle_idx": sweep_state.get("sweep_candle_idx"),
        "dol_score": dol_state.get("dol_score"),
        "dol_reason": dol_state.get("dol_reason"),
        "current_live_filter_code": current_sweep_filter_code,
        "current_live_filter_text": current_sweep_filter_text,
    }
    sweep_replay_state = (
        {
            **materialize_state(context),
            **sweep_state,
            **dol_state,
            "trigger_type": "sweep",
            "direction": sweep_state.get("expected_direction"),
        }
        if sweep_detected
        and sweep_state.get("expected_direction") in ("BUY", "SELL")
        and sweep_state.get("swept_level") is not None
        else None
    )
    path_rows.append(
        (
            "sweep",
            sweep_ready,
            sweep_reason_code,
            sweep_reason_text,
            sweep_condition_flags,
            sweep_diagnostics,
            sweep_replay_state if sweep_ready else None,
            sweep_replay_state,
        )
    )

    breakout_detected = bool(breakout_state.get("breakout_direction"))
    breakout_reason_code = breakout_state.get("no_signal_reason")
    if breakout_detected and breakout_state.get("ready"):
        breakout_reason_text = "Breakout candidate detected and path-ready."
    elif breakout_reason_code == REASON_BREAKOUT_STRUCTURE:
        breakout_reason_text = "Breakout direction conflicts with current 15m structure."
    elif breakout_reason_code == REASON_BREAKOUT_RETEST:
        breakout_reason_text = breakout_state.get("retest_reason") or "Breakout candidate failed retest validation."
    else:
        breakout_reason_text = "No breakout candidate detected."
    breakout_condition_flags = {
        "candidate_detected": breakout_detected,
        "has_breakout_idx": breakout_state.get("breakout_idx") is not None,
        "has_breakout_level": breakout_state.get("breakout_level") is not None,
        "structure_alignment_ok": bool(breakout_state.get("structure_alignment_ok", True)),
        "retest_valid": bool(breakout_state.get("retest_valid", False)),
        "effort_result_positive": pd.notna(breakout_state.get("er_score")) and float(breakout_state.get("er_score") or 0.0) > 0.0,
        "current_live_filter_blocked": bool(current_breakout_filter_code),
    }
    breakout_diagnostics = {
        "breakout_idx": breakout_state.get("breakout_idx"),
        "breakout_level": breakout_state.get("breakout_level"),
        "breakout_direction": breakout_state.get("breakout_direction"),
        "breakout_retest_reason": breakout_state.get("retest_reason"),
        "breakout_er_score": breakout_state.get("er_score"),
        "current_live_filter_code": current_breakout_filter_code,
        "current_live_filter_text": current_breakout_filter_text,
    }
    breakout_replay_state = (
        {
            **materialize_state(context),
            "trigger_type": "breakout",
            "direction": breakout_state.get("breakout_direction"),
            "expected_direction": breakout_state.get("breakout_direction"),
            "breakout_idx": breakout_state.get("breakout_idx"),
            "breakout_level": breakout_state.get("breakout_level"),
            "breakout_direction": breakout_state.get("breakout_direction"),
            "breakout_retest_valid": breakout_state.get("retest_valid"),
            "breakout_retest_reason": breakout_state.get("retest_reason"),
            "breakout_er_score": breakout_state.get("er_score"),
            "structure_alignment_ok": breakout_state.get("structure_alignment_ok"),
        }
        if breakout_detected
        and breakout_state.get("breakout_direction") in ("BUY", "SELL")
        and breakout_state.get("breakout_level") is not None
        else None
    )
    path_rows.append(
        (
            "breakout",
            bool(breakout_state.get("ready")),
            breakout_reason_code,
            breakout_reason_text,
            breakout_condition_flags,
            breakout_diagnostics,
            breakout_replay_state if breakout_state.get("ready") else None,
            breakout_replay_state,
        )
    )

    for trigger_type, ready, reason_code, reason_text, condition_flags, diagnostics, trade_state, replay_state in path_rows:
        flat_condition_fields = {
            f"cond_{str(key).strip()}": value
            for key, value in sorted(condition_flags.items(), key=lambda item: str(item[0]))
            if str(key).strip()
        }
        event = _base_shadow_event(
            variant,
            structure_15m_limit,
            symbol,
            ts,
            trigger_type,
            context,
            close_price,
            atr_pct,
            adx_value,
            vol_regime,
            adx_regime,
            symbol_bucket,
            liquidity_bucket,
            session_bucket,
            trend_regime,
            quote_volume,
        )
        event.update(
            {
                "candidate_detected": bool(condition_flags.get("candidate_detected", False)),
                "path_ready": bool(ready),
                "path_reason_code": reason_code,
                "path_reason_text": reason_text,
                "condition_flag_count": int(len(condition_flags)),
                "condition_true_count": int(
                    sum(
                        1
                        for value in condition_flags.values()
                        if isinstance(value, (bool, np.bool_)) and bool(value)
                    )
                ),
                "condition_flags_json": json.dumps(condition_flags, sort_keys=True, default=str),
                "diagnostics_json": json.dumps(diagnostics, sort_keys=True, default=str),
                **flat_condition_fields,
            }
        )
        parity_result = {
            "accepted": False,
            "reason_code": reason_code,
            "reason_text": reason_text,
            "voters": {},
            "total_votes": 0,
            "ensemble_min": 0,
            "ensemble_passed": False,
            "htf_score": 0.0,
            "htf_trigger_type": None,
            "htf_check_direction": None,
            "wyckoff_pattern": None,
            "wyckoff_conf": 0.0,
            "phase_info": {},
            "candle_pattern": None,
            "recent_confirmation_passed": False,
            "recent_confirmation_score": 0.0,
            "consolidation_break": None,
            "ibo_type": None,
            "divergence_type": None,
            "divergence_strength": 0,
            "entry_result": {
                "confirmed": False,
                "entry_price": np.nan,
                "entry_time": pd.NaT,
                "reason": reason_text,
                "reason_code": reason_code,
            },
        }
        if trade_state is not None:
            parity_result = evaluate_historical_path_parity(
                context=context,
                trade_state=trade_state,
                signal_ts=ts,
                close_price=close_price,
                prefix_1h=prefix_1h,
                prefix_4h=prefix_4h,
                symbol_config=symbol_config,
                live_regime=live_regime,
            )
        parity_voter_fields = {
            f"parity_vote_{str(name).strip()}": value
            for name, value in sorted((parity_result.get("voters") or {}).items(), key=lambda item: str(item[0]))
            if str(name).strip()
        }
        entry_result = parity_result.get("entry_result") or {}
        event.update(
            {
                "parity_accepted": bool(parity_result.get("accepted", False)),
                "parity_reason_code": parity_result.get("reason_code"),
                "parity_reason_text": parity_result.get("reason_text"),
                "parity_total_votes": int(parity_result.get("total_votes", 0) or 0),
                "parity_ensemble_min": int(parity_result.get("ensemble_min", 0) or 0),
                "parity_ensemble_passed": bool(parity_result.get("ensemble_passed", False)),
                "parity_htf_score": float(parity_result.get("htf_score", 0.0) or 0.0),
                "parity_htf_trigger_type": parity_result.get("htf_trigger_type"),
                "parity_htf_check_direction": parity_result.get("htf_check_direction"),
                "parity_wyckoff_pattern": parity_result.get("wyckoff_pattern"),
                "parity_wyckoff_conf": float(parity_result.get("wyckoff_conf", 0.0) or 0.0),
                "parity_phase_info_json": json.dumps(parity_result.get("phase_info", {}) or {}, sort_keys=True, default=str),
                "parity_candle_pattern": parity_result.get("candle_pattern"),
                "parity_recent_confirmation_passed": bool(parity_result.get("recent_confirmation_passed", False)),
                "parity_recent_confirmation_score": float(parity_result.get("recent_confirmation_score", 0.0) or 0.0),
                "parity_consolidation_break": parity_result.get("consolidation_break"),
                "parity_ibo_type": parity_result.get("ibo_type"),
                "parity_divergence_type": parity_result.get("divergence_type"),
                "parity_divergence_strength": int(parity_result.get("divergence_strength", 0) or 0),
                "parity_entry_ready": bool(entry_result.get("confirmed", False)),
                "parity_entry_time": entry_result.get("entry_time"),
                "parity_entry_reason": entry_result.get("reason"),
                "parity_entry_reason_code": entry_result.get("reason_code"),
                **parity_voter_fields,
            }
        )
        trade_result = None
        shadow_trade_mode = None
        replay_entry_model = None
        counterfactual_rejection_stage = None
        counterfactual_gate_reason_code = None
        counterfactual_gate_reason_text = None
        if trade_state is not None and parity_result.get("accepted"):
            trade_result = simulate_trade_path(
                symbol,
                full_df_15m,
                pos,
                trade_state,
                lookahead,
                symbol_config,
                execution_model,
                entry_time_override=entry_result.get("entry_time"),
                entry_price_override=entry_result.get("entry_price"),
            )
            shadow_trade_mode = "accepted_parity_trade"
            replay_entry_model = "static_parity_decision_close"
            counterfactual_rejection_stage = "accepted"
        elif replay_state is not None:
            trade_result = simulate_trade_path(
                symbol,
                full_df_15m,
                pos,
                replay_state,
                lookahead,
                symbol_config,
                execution_model,
            )
            shadow_trade_mode = "counterfactual_rejected_candidate"
            replay_entry_model = "next_bar_open_counterfactual"
            counterfactual_rejection_stage = "path_rejected" if not ready else "parity_rejected"
            counterfactual_gate_reason_code = reason_code if not ready else parity_result.get("reason_code")
            counterfactual_gate_reason_text = reason_text if not ready else parity_result.get("reason_text")
        event.update(
            {
                "shadow_trade_replayed": bool(trade_result),
                "replay_eligible": bool(replay_state is not None),
                "shadow_trade_mode": shadow_trade_mode,
                "replay_entry_model": replay_entry_model,
                "counterfactual_rejection_stage": counterfactual_rejection_stage,
                "counterfactual_gate_reason_code": counterfactual_gate_reason_code,
                "counterfactual_gate_reason_text": counterfactual_gate_reason_text,
            }
        )
        if trade_result is not None:
            row_state = trade_state if shadow_trade_mode == "accepted_parity_trade" else replay_state
            outcome_bucket = classify_shadow_replay_outcome(trade_result)
            trade_row = {
                "variant": variant.name,
                "structure_15m_limit": int(structure_15m_limit),
                "experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}",
                "path_experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}|path={trigger_type}",
                "trigger_type": trigger_type,
                "symbol": symbol,
                "timestamp": ts,
                "direction": row_state.get("expected_direction") or row_state.get("direction"),
                "structure_4h": context.get("structure_4h"),
                "structure_15m": context.get("structure_15m"),
                "symbol_bucket": symbol_bucket,
                "liquidity_bucket": liquidity_bucket,
                "session_bucket": session_bucket,
                "trend_regime": trend_regime,
                "vol_regime": vol_regime,
                "adx_regime": adx_regime,
                "quote_volume": quote_volume,
                "cluster_id": f"cluster_{base_symbol(symbol)}",
                "signal_entry_price": round(float(close_price), 8),
                "size_multiplier": float(symbol_config.get("size_multiplier", 0.001)),
                "volume_place": int(symbol_config.get("volume_place", 3)),
                "min_trade_num": float(symbol_config.get("min_trade_num", 0.001)),
                "swept_level": row_state.get("swept_level"),
                "breakout_level": row_state.get("breakout_level"),
                "candidate_detected": bool(condition_flags.get("candidate_detected", False)),
                "path_ready": bool(ready),
                "path_reason_code": reason_code,
                "path_reason_text": reason_text,
                "condition_flag_count": int(len(condition_flags)),
                "condition_true_count": int(
                    sum(
                        1
                        for value in condition_flags.values()
                        if isinstance(value, (bool, np.bool_)) and bool(value)
                    )
                ),
                "condition_flags_json": json.dumps(condition_flags, sort_keys=True, default=str),
                "diagnostics_json": json.dumps(diagnostics, sort_keys=True, default=str),
                "parity_accepted": bool(parity_result.get("accepted", False)),
                "parity_reason_code": parity_result.get("reason_code"),
                "parity_reason_text": parity_result.get("reason_text"),
                "parity_total_votes": int(parity_result.get("total_votes", 0) or 0),
                "parity_ensemble_min": int(parity_result.get("ensemble_min", 0) or 0),
                "parity_ensemble_passed": bool(parity_result.get("ensemble_passed", False)),
                "parity_htf_score": float(parity_result.get("htf_score", 0.0) or 0.0),
                "parity_htf_trigger_type": parity_result.get("htf_trigger_type"),
                "parity_htf_check_direction": parity_result.get("htf_check_direction"),
                "parity_wyckoff_pattern": parity_result.get("wyckoff_pattern"),
                "parity_wyckoff_conf": float(parity_result.get("wyckoff_conf", 0.0) or 0.0),
                "parity_phase_info_json": json.dumps(parity_result.get("phase_info", {}) or {}, sort_keys=True, default=str),
                "parity_candle_pattern": parity_result.get("candle_pattern"),
                "parity_recent_confirmation_passed": bool(parity_result.get("recent_confirmation_passed", False)),
                "parity_recent_confirmation_score": float(parity_result.get("recent_confirmation_score", 0.0) or 0.0),
                "parity_consolidation_break": parity_result.get("consolidation_break"),
                "parity_ibo_type": parity_result.get("ibo_type"),
                "parity_divergence_type": parity_result.get("divergence_type"),
                "parity_divergence_strength": int(parity_result.get("divergence_strength", 0) or 0),
                "parity_entry_ready": bool(entry_result.get("confirmed", False)),
                "parity_entry_time": entry_result.get("entry_time"),
                "parity_entry_reason": entry_result.get("reason"),
                "parity_entry_reason_code": entry_result.get("reason_code"),
                "shadow_trade_mode": shadow_trade_mode,
                "replay_entry_model": replay_entry_model,
                "counterfactual_rejection_stage": counterfactual_rejection_stage,
                "counterfactual_gate_reason_code": counterfactual_gate_reason_code,
                "counterfactual_gate_reason_text": counterfactual_gate_reason_text,
                "counterfactual_outcome_bucket": outcome_bucket,
                **parity_voter_fields,
                **flat_condition_fields,
            }
            trade_row.update(trade_result)
            shadow_trades.append(trade_row)
            event.update(trade_result)
            event["counterfactual_outcome_bucket"] = outcome_bucket
        shadow_events.append(event)

    return shadow_events, shadow_trades


def summarize_trade_metrics(trades_df, group_cols):
    if trades_df.empty:
        return pd.DataFrame()

    def _metrics(frame):
        def _numeric_series(column, default=0.0):
            if column in frame.columns:
                return pd.to_numeric(frame[column], errors="coerce").fillna(default)
            return pd.Series([default] * len(frame), index=frame.index, dtype="float64")

        net_r = _numeric_series("net_r", 0.0)
        gross_r = _numeric_series("gross_r", np.nan)
        if gross_r.isna().all():
            gross_r = net_r.copy()
        mfe_r = _numeric_series("mfe_r", 0.0)
        mae_r = _numeric_series("mae_r", 0.0)
        holding_bars = _numeric_series("holding_bars", 0.0)
        tp1_hit_any = _numeric_series("tp1_hit_any", 0.0)
        tp2_hit_any = _numeric_series("tp2_hit_any", 0.0)
        sl_hit_any = _numeric_series("sl_hit_any", 0.0)
        trailing_activated = _numeric_series("trailing_activated", 0.0)
        if "net_pnl_cash" in frame.columns:
            total_net_pnl = float(pd.to_numeric(frame["net_pnl_cash"], errors="coerce").fillna(0.0).sum())
        elif "net_pnl_per_unit" in frame.columns:
            total_net_pnl = float(pd.to_numeric(frame["net_pnl_per_unit"], errors="coerce").fillna(0.0).sum())
        else:
            total_net_pnl = 0.0
        wins = net_r[net_r > 0]
        losses = net_r[net_r < 0]
        std = float(net_r.std(ddof=0)) if len(net_r) > 1 else 0.0
        downside = net_r[net_r < 0]
        downside_std = float(downside.std(ddof=0)) if len(downside) > 1 else 0.0
        return pd.Series(
            {
                "trades": int(len(frame)),
                "wins": int((net_r > 0).sum()),
                "losses": int((net_r < 0).sum()),
                "win_rate_pct": round(float((net_r > 0).mean() * 100.0), 2),
                "expectancy_r": round(float(net_r.mean()), 4),
                "avg_net_r": round(float(net_r.mean()), 4),
                "avg_gross_r": round(float(gross_r.mean()), 4),
                "avg_win_r": round(float(wins.mean()), 4) if not wins.empty else 0.0,
                "avg_loss_r": round(float(losses.mean()), 4) if not losses.empty else 0.0,
                "profit_factor": round(float(wins.sum() / abs(losses.sum())), 4) if not losses.empty and abs(losses.sum()) > 0 else np.nan,
                "total_net_r": round(float(net_r.sum()), 4),
                "total_net_pnl": round(float(total_net_pnl), 4),
                "avg_mfe_r": round(float(mfe_r.mean()), 4),
                "avg_mae_r": round(float(mae_r.mean()), 4),
                "avg_holding_bars": round(float(holding_bars.mean()), 2),
                "tp1_hit_rate_pct": round(float(tp1_hit_any.mean() * 100.0), 2),
                "tp2_hit_rate_pct": round(float(tp2_hit_any.mean() * 100.0), 2),
                "sl_hit_rate_pct": round(float(sl_hit_any.mean() * 100.0), 2),
                "trailing_activation_rate_pct": round(float(trailing_activated.mean() * 100.0), 2),
                "sharpe_like": round(float((net_r.mean() / std) * math.sqrt(len(net_r))), 4) if std > 0 else np.nan,
                "downside_risk": round(float(downside_std), 4),
                "sortino_like": round(float((net_r.mean() / downside_std) * math.sqrt(len(net_r))), 4) if downside_std > 0 else np.nan,
            }
        )

    return trades_df.groupby(group_cols, dropna=False).apply(_metrics).reset_index()


def flatten_condition_flags(frame, json_col="condition_flags_json", prefix="cond_"):
    if frame.empty or json_col not in frame.columns:
        return frame, []

    def _load_flags(raw):
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _coerce_value(value):
        try:
            if pd.isna(value):
                return np.nan
        except TypeError:
            pass
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        return value

    parsed_flags = frame[json_col].apply(_load_flags)
    flag_keys = sorted(
        {
            str(key).strip()
            for flags in parsed_flags
            if isinstance(flags, dict)
            for key in flags.keys()
            if str(key).strip()
        }
    )
    condition_cols = []
    for key in flag_keys:
        col = f"{prefix}{key}"
        frame[col] = parsed_flags.apply(
            lambda flags: _coerce_value(flags.get(key, np.nan)) if isinstance(flags, dict) else np.nan
        )
        condition_cols.append(col)
    return frame, condition_cols


def build_shadow_condition_summary(events_df, condition_cols):
    if events_df.empty or not condition_cols:
        return pd.DataFrame()

    def _truthy_count(series):
        return int(series.fillna(False).astype(bool).sum())

    frames = []
    for trigger_type, trigger_frame in events_df.groupby("trigger_type", dropna=False):
        for col in condition_cols:
            if col not in trigger_frame.columns:
                continue
            if trigger_frame[col].notna().sum() == 0:
                continue
            grouped = (
                trigger_frame.groupby(["variant", "structure_15m_limit", "trigger_type", col], dropna=False)
                .agg(
                    observations=("symbol", "size"),
                    candidate_detected_count=("candidate_detected", _truthy_count),
                    path_ready_count=("path_ready", _truthy_count),
                )
                .reset_index()
                .rename(columns={col: "condition_value"})
            )
            grouped.insert(3, "condition_name", col.replace("cond_", "", 1))
            grouped["candidate_detected_rate_pct"] = grouped["candidate_detected_count"].div(grouped["observations"]).mul(100.0).round(2)
            grouped["path_ready_rate_pct"] = grouped["path_ready_count"].div(grouped["observations"]).mul(100.0).round(2)
            frames.append(grouped)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True).sort_values(
        ["variant", "structure_15m_limit", "trigger_type", "condition_name", "observations"],
        ascending=[True, True, True, True, False],
    )


def build_shadow_trade_condition_summary(shadow_entered_trades_df, condition_cols):
    if shadow_entered_trades_df.empty or not condition_cols:
        return pd.DataFrame()

    frames = []
    for trigger_type, trigger_frame in shadow_entered_trades_df.groupby("trigger_type", dropna=False):
        for col in condition_cols:
            if col not in trigger_frame.columns:
                continue
            if trigger_frame[col].notna().sum() == 0:
                continue
            summary = summarize_trade_metrics(
                trigger_frame,
                ["variant", "structure_15m_limit", "path_experiment_key", "trigger_type", col],
            )
            if summary.empty:
                continue
            summary = summary.rename(columns={col: "condition_value"})
            summary.insert(4, "condition_name", col.replace("cond_", "", 1))
            frames.append(summary)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True).sort_values(
        ["variant", "structure_15m_limit", "trigger_type", "condition_name", "trades"],
        ascending=[True, True, True, True, False],
    )


def assign_split_labels(df, portfolio_model, ts_col):
    if df.empty or ts_col not in df.columns:
        return pd.Series(dtype="object")
    timestamps = pd.Series(pd.to_datetime(df[ts_col])).sort_values().reset_index(drop=True)
    if timestamps.empty:
        return pd.Series(dtype="object")
    train_frac = float(portfolio_model.train_frac)
    validate_frac = float(portfolio_model.validate_frac)
    total = max(1, len(timestamps))
    train_idx = min(total - 1, max(0, int(total * train_frac) - 1))
    validate_idx = min(total - 1, max(train_idx, int(total * (train_frac + validate_frac)) - 1))
    train_cut = timestamps.iloc[train_idx]
    validate_cut = timestamps.iloc[validate_idx]
    labels = []
    for ts in pd.to_datetime(df[ts_col]):
        if ts <= train_cut:
            labels.append("train")
        elif ts <= validate_cut:
            labels.append("validate")
        else:
            labels.append("test")
    return pd.Series(labels, index=df.index)


def build_correlation_clusters(return_series_map, portfolio_model):
    if not return_series_map:
        return {}
    series_map = {
        symbol: series.tail(int(portfolio_model.correlation_lookback_bars))
        for symbol, series in return_series_map.items()
        if series is not None and not series.dropna().empty
    }
    if not series_map:
        return {}
    returns_df = pd.DataFrame(series_map).dropna(how="all")
    if returns_df.empty:
        return {symbol: symbol for symbol in return_series_map}
    corr = returns_df.corr(min_periods=max(20, int(portfolio_model.correlation_lookback_bars // 4)))
    parents = {symbol: symbol for symbol in corr.columns}

    def find(node):
        while parents[node] != node:
            parents[node] = parents[parents[node]]
            node = parents[node]
        return node

    def union(a, b):
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parents[root_b] = root_a

    for left in corr.columns:
        for right in corr.columns:
            if left >= right:
                continue
            if pd.notna(corr.loc[left, right]) and float(corr.loc[left, right]) >= float(portfolio_model.correlation_threshold):
                union(left, right)

    cluster_ids = {}
    seen = {}
    next_id = 1
    for symbol in corr.columns:
        root = find(symbol)
        if root not in seen:
            seen[root] = f"cluster_{next_id}"
            next_id += 1
        cluster_ids[symbol] = seen[root]
    return cluster_ids


def build_equity_stats(equity_curve):
    if equity_curve.empty:
        return {"ending_equity": np.nan, "total_return_pct": np.nan, "max_drawdown_pct": np.nan}
    equity = equity_curve["equity"].astype(float)
    running_max = equity.cummax()
    drawdown = (equity / running_max - 1.0).fillna(0.0)
    return {
        "ending_equity": round(float(equity.iloc[-1]), 4),
        "total_return_pct": round(float((equity.iloc[-1] / equity.iloc[0] - 1.0) * 100.0), 2) if equity.iloc[0] else np.nan,
        "max_drawdown_pct": round(float(drawdown.min() * 100.0), 2),
    }


def simulate_portfolio(trades_df, portfolio_model):
    if trades_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    portfolio_rows = []
    equity_rows = []
    for experiment_key, group in trades_df[trades_df["trade_status"] == "entered"].sort_values(["entry_time", "symbol"]).groupby("experiment_key"):
        equity = float(portfolio_model.starting_equity)
        open_positions = []
        cluster_counts = Counter()
        open_risk_cash = 0.0
        first_ts = pd.to_datetime(group["entry_time"]).min()
        equity_rows.append({"experiment_key": experiment_key, "timestamp": first_ts, "equity": equity})

        def flush_until(cutoff_ts):
            nonlocal equity, open_positions, open_risk_cash
            remaining = []
            for position in sorted(open_positions, key=lambda item: item["exit_time"]):
                if position["exit_time"] <= cutoff_ts:
                    equity += position["net_pnl_cash"]
                    open_risk_cash -= position["risk_cash"]
                    cluster_counts[position["cluster_id"]] -= 1
                    position["equity_after"] = round(float(equity), 4)
                    portfolio_rows.append(position)
                    equity_rows.append({"experiment_key": experiment_key, "timestamp": position["exit_time"], "equity": equity})
                else:
                    remaining.append(position)
            open_positions = remaining

        for row in group.itertuples(index=False):
            entry_ts = pd.Timestamp(row.entry_time)
            flush_until(entry_ts)
            entry_price = float(row.entry_price)
            risk_per_unit = float(row.effective_risk_per_unit)
            if risk_per_unit <= 0:
                continue
            if len(open_positions) >= int(portfolio_model.max_open_positions):
                continue
            risk_budget = equity * float(portfolio_model.risk_per_trade_percent)
            symbol_config = {
                "size_multiplier": float(getattr(row, "size_multiplier", 0.001)),
                "volume_place": int(getattr(row, "volume_place", 3)),
                "min_trade_num": float(getattr(row, "min_trade_num", 0.001)),
            }
            raw_units = risk_budget / risk_per_unit
            units = round_size(raw_units, symbol_config)
            if units < float(symbol_config["min_trade_num"]):
                continue
            actual_risk_cash = risk_per_unit * units
            if open_risk_cash + actual_risk_cash > equity * float(portfolio_model.max_total_risk_percent):
                continue
            cluster_id = getattr(row, "cluster_id", f"cluster_{base_symbol(row.symbol)}")
            if cluster_counts[cluster_id] >= int(portfolio_model.max_positions_per_cluster):
                continue

            position = {
                "experiment_key": experiment_key,
                "variant": row.variant,
                "structure_15m_limit": int(row.structure_15m_limit),
                "symbol": row.symbol,
                "cluster_id": cluster_id,
                "split": getattr(row, "split", "all"),
                "entry_time": entry_ts,
                "exit_time": pd.Timestamp(row.exit_time),
                "entry_price": entry_price,
                "exit_price": float(row.exit_price),
                "direction": row.direction,
                "units": float(units),
                "risk_cash": round(float(actual_risk_cash), 4),
                "equity_before": round(float(equity), 4),
                "net_pnl_cash": round(float(row.net_pnl_per_unit) * float(units), 4),
                "gross_pnl_cash": round(float(row.gross_pnl_per_unit) * float(units), 4),
                "net_r": float(row.net_r),
                "exit_reason": row.exit_reason,
                "symbol_bucket": row.symbol_bucket,
                "liquidity_bucket": row.liquidity_bucket,
                "session_bucket": row.session_bucket,
                "trend_regime": row.trend_regime,
            }
            open_positions.append(position)
            cluster_counts[cluster_id] += 1
            open_risk_cash += actual_risk_cash

        flush_until(pd.Timestamp.max)

    portfolio_df = pd.DataFrame(portfolio_rows)
    equity_df = pd.DataFrame(equity_rows).sort_values(["experiment_key", "timestamp"]).reset_index(drop=True) if equity_rows else pd.DataFrame()
    return portfolio_df, equity_df


def build_walkforward_summary(trades_df, portfolio_model):
    if trades_df.empty:
        return pd.DataFrame()
    timestamps = pd.Series(pd.to_datetime(trades_df["entry_time"]).dropna().sort_values().unique())
    if len(timestamps) < max(20, portfolio_model.walkforward_folds + 2):
        return pd.DataFrame()
    rows = []
    for fold in range(1, max(1, int(portfolio_model.walkforward_folds)) + 1):
        train_end_idx = int(len(timestamps) * fold / (portfolio_model.walkforward_folds + 1))
        test_end_idx = int(len(timestamps) * (fold + 1) / (portfolio_model.walkforward_folds + 1))
        if train_end_idx <= 0 or test_end_idx <= train_end_idx:
            continue
        train_cut = timestamps.iloc[train_end_idx - 1]
        test_cut = timestamps.iloc[min(len(timestamps) - 1, test_end_idx - 1)]
        train_df = trades_df[pd.to_datetime(trades_df["entry_time"]) <= train_cut]
        test_df = trades_df[(pd.to_datetime(trades_df["entry_time"]) > train_cut) & (pd.to_datetime(trades_df["entry_time"]) <= test_cut)]
        if train_df.empty or test_df.empty:
            continue
        ranked = summarize_trade_metrics(train_df, ["experiment_key"]).sort_values(["expectancy_r", "profit_factor", "trades"], ascending=[False, False, False])
        if ranked.empty:
            continue
        selected = ranked.iloc[0]["experiment_key"]
        selected_train = train_df[train_df["experiment_key"] == selected]
        selected_test = test_df[test_df["experiment_key"] == selected]
        if selected_test.empty:
            continue
        train_metrics = summarize_trade_metrics(selected_train, ["experiment_key"]).iloc[0]
        test_metrics = summarize_trade_metrics(selected_test, ["experiment_key"]).iloc[0]
        rows.append(
            {
                "fold": fold,
                "selected_experiment": selected,
                "train_trades": int(train_metrics["trades"]),
                "train_expectancy_r": float(train_metrics["expectancy_r"]),
                "test_trades": int(test_metrics["trades"]),
                "test_expectancy_r": float(test_metrics["expectancy_r"]),
                "test_profit_factor": float(test_metrics["profit_factor"]) if pd.notna(test_metrics["profit_factor"]) else np.nan,
                "test_win_rate_pct": float(test_metrics["win_rate_pct"]),
            }
        )
    return pd.DataFrame(rows)


def build_monte_carlo_summary(portfolio_df, portfolio_model):
    if portfolio_df.empty or portfolio_model.monte_carlo_runs <= 0:
        return pd.DataFrame()
    rows = []
    for experiment_key, group in portfolio_df.groupby("experiment_key"):
        pnls = group["net_pnl_cash"].astype(float).to_numpy()
        if len(pnls) < 2:
            continue
        risk_cash = group["risk_cash"].astype(float).replace(0, np.nan).fillna(group["risk_cash"].mean()).to_numpy()
        rng = np.random.default_rng(stable_seed("mc", experiment_key, portfolio_model.monte_carlo_runs))
        ending_equities = []
        max_drawdowns = []
        for _ in range(int(portfolio_model.monte_carlo_runs)):
            sampled_idx = rng.integers(0, len(pnls), size=len(pnls))
            sampled = pnls[sampled_idx].copy()
            noise_cash = (portfolio_model.monte_carlo_slippage_noise_bps / 10000.0) * risk_cash[sampled_idx]
            sampled -= rng.normal(0.0, noise_cash)
            equity = float(portfolio_model.starting_equity)
            curve = [equity]
            for pnl in sampled:
                equity += float(pnl)
                curve.append(equity)
            curve = pd.Series(curve)
            stats = build_equity_stats(pd.DataFrame({"equity": curve}))
            ending_equities.append(stats["ending_equity"])
            max_drawdowns.append(stats["max_drawdown_pct"])
        rows.append(
            {
                "experiment_key": experiment_key,
                "runs": int(portfolio_model.monte_carlo_runs),
                "ending_equity_mean": round(float(np.mean(ending_equities)), 4),
                "ending_equity_p05": round(float(np.percentile(ending_equities, 5)), 4),
                "ending_equity_p50": round(float(np.percentile(ending_equities, 50)), 4),
                "ending_equity_p95": round(float(np.percentile(ending_equities, 95)), 4),
                "max_drawdown_mean_pct": round(float(np.mean(max_drawdowns)), 2),
                "max_drawdown_p95_pct": round(float(np.percentile(max_drawdowns, 95)), 2),
            }
        )
    return pd.DataFrame(rows)


async def run_symbol(symbol, args, variants, handle, shadow_handle, execution_model, portfolio_model, metadata, liquidity_thresholds):
    max_structure_15m_limit = max(args.structure_15m_limits)
    operational_15m_limit = int(args.operational_15m_limit)
    directional_15m_limit = int(args.directional_15m_limit)
    deep_15m_limit = max(int(args.limit) + max_structure_15m_limit, max_structure_15m_limit, operational_15m_limit, directional_15m_limit)
    df = await fetch_data_async(symbol, "15m", deep_15m_limit)
    if df is None or len(df) < max(args.warmup + args.lookahead + 2, 220):
        return {
            "symbol": symbol,
            "status": "skipped",
            "reason": REASON_DATA,
            "rows": len(df) if df is not None else 0,
            "evaluations": 0,
            "signals": 0,
        }, [], [], None

    deep_4h_limit = max(int(SIGNAL_CONFIG.get("htf_deep_candle_limit", 400)), 200)
    df_4h_native = await fetch_data_async(symbol, "4H", deep_4h_limit)
    if df_4h_native is None or df_4h_native.empty or len(df_4h_native) < 100:
        return {
            "symbol": symbol,
            "status": "skipped",
            "reason": "insufficient_4h_data",
            "rows": len(df) if df is not None else 0,
            "evaluations": 0,
            "signals": 0,
        }, [], [], None

    one_h_limit = max(160, int(math.ceil(max(int(args.limit), 240) / 4.0)) + 80)
    df_1h_native = await fetch_data_async(symbol, "1H", one_h_limit)

    symbol_config = get_symbol_config(symbol)
    context = build_context(df)
    close_15m, ind_15m = context["close_15m"], context["ind_15m"]
    ind_4h = calculate_atr_only(df_4h_native.copy())
    ind_4h_parity = calculate_all_indicators(df_4h_native.copy())
    ind_1h_parity = calculate_all_indicators(df_1h_native.copy()) if df_1h_native is not None and not df_1h_native.empty else None
    atr_pct_series, adx_series = context["atr_pct"], context["adx"]
    vol_low, vol_high = context["vol_thresholds"]
    adx_low, adx_high = context["adx_thresholds"]
    needs_relaxed = any(not variant.require_major_break for variant in variants)
    trades = []
    shadow_trades = []
    event_count = 0
    signal_count = 0
    quote_volume = float(metadata.get("quote_volume", np.nan)) if metadata else np.nan
    symbol_bucket = classify_symbol_bucket(symbol)
    liquidity_bucket = classify_liquidity_bucket(quote_volume, liquidity_thresholds)
    returns_tail = close_15m["close"].pct_change().dropna().tail(int(portfolio_model.correlation_lookback_bars))
    eval_start = max(args.warmup, len(ind_15m) - int(args.limit))
    eval_end = len(ind_15m) - max(2, args.lookahead)

    for pos in range(eval_start, eval_end, max(1, args.step)):
        current_ts = ind_15m.index[pos]
        prefix_15m = ind_15m.iloc[:pos + 1].copy()
        op_prefix_15m = prefix_15m.tail(operational_15m_limit).copy()
        direction_prefix_15m = prefix_15m.tail(directional_15m_limit).copy()
        prefix_4h = ind_4h[ind_4h.index <= current_ts].copy()
        prefix_4h_parity = ind_4h_parity[ind_4h_parity.index <= current_ts].copy() if ind_4h_parity is not None else None
        prefix_1h_parity = ind_1h_parity[ind_1h_parity.index <= current_ts].copy() if ind_1h_parity is not None else None
        if prefix_4h.empty:
            continue

        state_by_limit = {}
        for structure_15m_limit in args.structure_15m_limits:
            level_prefix_15m = prefix_15m.tail(int(structure_15m_limit)).copy()
            base_context = prepare_state_context(
                op_prefix_15m,
                direction_prefix_15m,
                level_prefix_15m,
                prefix_4h,
                require_major_break=True,
                structure_15m_limit=structure_15m_limit,
            )
            relaxed_context = (
                prepare_state_context(
                    op_prefix_15m,
                    direction_prefix_15m,
                    level_prefix_15m,
                    prefix_4h,
                    require_major_break=False,
                    structure_15m_limit=structure_15m_limit,
                )
                if needs_relaxed
                else None
            )
            state_by_limit[int(structure_15m_limit)] = {"base": base_context, "relaxed": relaxed_context}

        close_price = float(op_prefix_15m["close"].iloc[-1])
        atr_pct = float(atr_pct_series.iloc[pos]) if pos < len(atr_pct_series) else 0.0
        adx_value = float(adx_series.iloc[pos]) if pos < len(adx_series) else 0.0
        vol_regime = classify_bucket(atr_pct, vol_low, vol_high)
        adx_regime = classify_bucket(adx_value, adx_low, adx_high)
        session_bucket = classify_session(current_ts)
        live_regime = detect_market_regime(op_prefix_15m, float(close_price * atr_pct) if atr_pct > 0 else float(close_price) * 0.005)

        for structure_15m_limit in args.structure_15m_limits:
            for variant in variants:
                state_bundle = state_by_limit[int(structure_15m_limit)]
                context = state_bundle["relaxed"] if not variant.require_major_break else state_bundle["base"]
                state = materialize_state(context)
                trend_regime = classify_trend_regime(state.get("structure_4h"), adx_regime)
                decision, reason_code, reason_text = "reject", None, None
                if state["structure_4h"] not in ("bullish", "bearish"):
                    reason_code, reason_text = REASON_4H_RANGING, "4H structure is ranging."
                elif variant.enforce_15m_gate and state["structure_15m"] == "ranging":
                    reason_code, reason_text = REASON_15M_RANGING, "15m ranging blocks the sweep path."
                elif variant.enforce_15m_gate and state["structure_4h"] == state["structure_15m"] and state["structure_4h"] in ("bullish", "bearish"):
                    reason_code, reason_text = REASON_15M_MISSED, "15m is already aligned with 4H, so the sweep is treated as missed."
                else:
                    sweep_state = ensure_sweep_state(context, symbol)
                    state.update(sweep_state)
                    if not state["invariant_ok"]:
                        reason_code, reason_text = REASON_INVARIANT, "Sweep direction violated the 4H gating invariant."
                    elif not state["sweep_type"]:
                        reason_code = state["no_signal_reason"] or REASON_NO_SWEEP
                        reason_text = "No eligible aged swing levels remained." if reason_code == REASON_NO_LEVELS else "No valid sweep trigger was detected."
                    else:
                        filter_reason_code, filter_reason_text = evaluate_live_filter_rejection(
                            "sweep",
                            symbol_bucket,
                            session_bucket,
                            trend_regime,
                            live_regime,
                            state.get("structure_4h"),
                            state.get("structure_15m"),
                            vol_regime,
                            liquidity_bucket,
                        )
                        if filter_reason_code:
                            reason_code, reason_text = filter_reason_code, filter_reason_text
                        elif variant.use_dol:
                            state.update(ensure_dol_state(context, sweep_state))
                        if variant.use_dol and state["dol_reject"]:
                            reason_code, reason_text = REASON_DOL, state["dol_reason"] or "DOL rejected the sweep."
                        elif reason_code is None:
                            state["trigger_type"] = "sweep"
                            decision, reason_code, reason_text = "signal", "signal", "Sweep signal accepted."

                entry_fields = outcome_fields = None
                if decision == "signal":
                    signal_count += 1
                    trade_result = simulate_trade_path(symbol, ind_15m, pos, state, args.lookahead, symbol_config, execution_model)
                    entry_fields = {
                        "direction": state["expected_direction"],
                        "signal_entry_price": round(float(close_price), 8),
                        "size_multiplier": float(symbol_config.get("size_multiplier", 0.001)),
                        "volume_place": int(symbol_config.get("volume_place", 3)),
                        "min_trade_num": float(symbol_config.get("min_trade_num", 0.001)),
                    }
                    outcome_fields = trade_result
                    trade_row = {
                        "variant": variant.name,
                        "structure_15m_limit": int(structure_15m_limit),
                        "experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}",
                        "path_experiment_key": f"ltf={int(structure_15m_limit)}|variant={variant.name}|path=sweep",
                        "trigger_type": "sweep",
                        "symbol": symbol,
                        "timestamp": current_ts,
                        "direction": state["expected_direction"],
                        "structure_4h": state.get("structure_4h"),
                        "structure_15m": state.get("structure_15m"),
                        "symbol_bucket": symbol_bucket,
                        "liquidity_bucket": liquidity_bucket,
                        "session_bucket": session_bucket,
                        "trend_regime": trend_regime,
                        "vol_regime": vol_regime,
                        "adx_regime": adx_regime,
                        "quote_volume": quote_volume,
                        "cluster_id": f"cluster_{base_symbol(symbol)}",
                    }
                    trade_row.update(entry_fields)
                    trade_row.update(trade_result)
                    trades.append(trade_row)

                event = build_event(
                    variant,
                    structure_15m_limit,
                    symbol,
                    current_ts,
                    state,
                    decision,
                    reason_code,
                    reason_text,
                    close_price,
                    atr_pct,
                    adx_value,
                    vol_regime,
                    adx_regime,
                    symbol_bucket,
                    liquidity_bucket,
                    session_bucket,
                    trend_regime,
                    quote_volume,
                    entry_fields,
                    outcome_fields,
                )
                handle.write(json.dumps(event, default=str) + "\n")
                event_count += 1
                shadow_events, symbol_shadow_trades = build_shadow_path_rows(
                    variant,
                    structure_15m_limit,
                    symbol,
                    current_ts,
                    context,
                    ind_15m,
                    pos,
                    symbol_config,
                    execution_model,
                    args.lookahead,
                    close_price,
                    atr_pct,
                    adx_value,
                    vol_regime,
                    adx_regime,
                    symbol_bucket,
                    liquidity_bucket,
                    session_bucket,
                    trend_regime,
                    live_regime,
                    quote_volume,
                    prefix_1h_parity,
                    prefix_4h_parity,
                )
                for shadow_event in shadow_events:
                    shadow_handle.write(json.dumps(shadow_event, default=str) + "\n")
                shadow_trades.extend(symbol_shadow_trades)
                if event_count % 100 == 0:
                    handle.flush()
                    shadow_handle.flush()

    handle.flush()
    shadow_handle.flush()
    return {
        "symbol": symbol,
        "status": "processed",
        "reason": "",
        "rows": len(close_15m),
        "evaluations": event_count,
        "signals": signal_count,
    }, trades, shadow_trades, returns_tail


async def run_symbol_with_semaphore(symbol, args, variants, handle, shadow_handle, semaphore, execution_model, portfolio_model, metadata, liquidity_thresholds):
    async with semaphore:
        return await run_symbol(symbol, args, variants, handle, shadow_handle, execution_model, portfolio_model, metadata, liquidity_thresholds)


async def main():
    args = parse_args()
    runtime_overrides = resolve_runtime_overrides(args)
    with temporary_runtime_config_overrides(
        runtime_overrides.get("signal_overrides"),
        runtime_overrides.get("trade_overrides"),
        runtime_overrides.get("research_overrides"),
    ):
        args = populate_arg_defaults(args)
        variants = get_variants(args.variants)
        args.structure_15m_limits = get_structure_15m_limits(args.structure_15m_limits)
        symbols, metadata_map = get_universe(args)
        if args.max_symbols > 0:
            symbols = symbols[:args.max_symbols]
        if args.shard_count < 1:
            raise ValueError("shard-count must be at least 1")
        if args.shard_index < 0 or args.shard_index >= args.shard_count:
            raise ValueError("shard-index must be between 0 and shard-count - 1")
        if args.shard_count > 1:
            symbols = [symbol for idx, symbol in enumerate(symbols) if idx % args.shard_count == args.shard_index]
        metadata_map = {symbol: metadata_map.get(symbol, {"symbol": symbol, "quote_volume": np.nan}) for symbol in symbols}
        if not symbols:
            raise RuntimeError("No symbols available for quant research.")

        execution_model = build_execution_model()
        portfolio_model = build_portfolio_model()
        liquidity_thresholds = build_liquidity_thresholds(metadata_map)
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        if args.shard_count > 1:
            run_id = f"{run_id}_sh{args.shard_index + 1}of{args.shard_count}"
        run_dir = os.path.join(args.output_dir or QUANT_RESEARCH_CONFIG.get("output_dir"), run_id)
        os.makedirs(run_dir, exist_ok=True)

        effective_signal_config = config_snapshot(SIGNAL_CONFIG)
        effective_trade_config = config_snapshot(TRADE_CONFIG)
        effective_research_config = config_snapshot(QUANT_RESEARCH_CONFIG)
        applied_overrides = {
            "profile_file": runtime_overrides.get("profile_file", ""),
            "profile_name": runtime_overrides.get("profile_name", ""),
            "profile_meta": runtime_overrides.get("profile_meta", {}),
            "signal_config_overrides_file": runtime_overrides.get("signal_config_overrides_file", ""),
            "trade_config_overrides_file": runtime_overrides.get("trade_config_overrides_file", ""),
            "research_config_overrides_file": runtime_overrides.get("research_config_overrides_file", ""),
            "signal_overrides": runtime_overrides.get("signal_overrides", {}),
            "trade_overrides": runtime_overrides.get("trade_overrides", {}),
            "research_overrides": runtime_overrides.get("research_overrides", {}),
        }
        config_hashes = {
            "signal_config_hash": stable_object_hash(effective_signal_config),
            "trade_config_hash": stable_object_hash(effective_trade_config),
            "research_config_hash": stable_object_hash(effective_research_config),
            "applied_overrides_hash": stable_object_hash(applied_overrides),
        }
        write_json_file(os.path.join(run_dir, "effective_signal_config.json"), effective_signal_config)
        write_json_file(os.path.join(run_dir, "effective_trade_config.json"), effective_trade_config)
        write_json_file(os.path.join(run_dir, "effective_research_config.json"), effective_research_config)
        write_json_file(os.path.join(run_dir, "applied_overrides.json"), applied_overrides)

        logger.info(
            "Starting quant research run %s | source=%s | symbols=%d | variants=%s | ltf_windows=%s",
            run_id,
            args.universe_source,
            len(symbols),
            ",".join(variant.name for variant in variants),
            ",".join(str(v) for v in args.structure_15m_limits),
        )

        started = time.time()
        universe_rows, all_trades, all_shadow_trades = [], [], []
        return_series_map = {}
        events_path = os.path.join(run_dir, "events.jsonl")
        shadow_events_path = os.path.join(run_dir, "shadow_events.jsonl")
        semaphore = asyncio.Semaphore(max(1, int(args.max_concurrency)))
        total_evaluations = 0
        total_signals = 0
        with open(events_path, "w", encoding="utf-8") as handle, open(shadow_events_path, "w", encoding="utf-8") as shadow_handle:
            tasks = []
            for symbol in symbols:
                logger.info("Queued %s", safe_log_text(symbol))
                tasks.append(
                    asyncio.create_task(
                        run_symbol_with_semaphore(
                            symbol,
                            args,
                            variants,
                            handle,
                            shadow_handle,
                            semaphore,
                            execution_model,
                            portfolio_model,
                            metadata_map.get(symbol, {"symbol": symbol, "quote_volume": np.nan}),
                            liquidity_thresholds,
                        )
                    )
                )

            completed = 0
            for task in asyncio.as_completed(tasks):
                universe_row, symbol_trades, symbol_shadow_trades, return_series = await task
                universe_rows.append(universe_row)
                all_trades.extend(symbol_trades)
                all_shadow_trades.extend(symbol_shadow_trades)
                if return_series is not None and not return_series.empty:
                    return_series_map[universe_row["symbol"]] = return_series
                completed += 1
                total_evaluations += int(universe_row.get("evaluations", 0))
                total_signals += int(universe_row.get("signals", 0))
                if completed % 10 == 0 or completed == len(symbols):
                    logger.info(
                        "Progress %d/%d symbols | elapsed %.1fs | events=%d | signals=%d | trades=%d",
                        completed,
                        len(symbols),
                        time.time() - started,
                        total_evaluations,
                        total_signals,
                        len(all_trades),
                    )

        universe_df = pd.DataFrame(universe_rows)
        events_df = pd.read_json(events_path, lines=True) if os.path.exists(events_path) and os.path.getsize(events_path) > 0 else pd.DataFrame()
        shadow_events_df = pd.read_json(shadow_events_path, lines=True) if os.path.exists(shadow_events_path) and os.path.getsize(shadow_events_path) > 0 else pd.DataFrame()
        trades_df = pd.DataFrame(all_trades)
        shadow_trades_df = pd.DataFrame(all_shadow_trades)
        shadow_events_df, shadow_event_condition_cols = flatten_condition_flags(shadow_events_df)
        shadow_trades_df, shadow_trade_condition_cols = flatten_condition_flags(shadow_trades_df)
        shadow_condition_cols = sorted(set(shadow_event_condition_cols) | set(shadow_trade_condition_cols))
        signals_df = events_df[events_df["decision"] == "signal"].copy() if not events_df.empty else pd.DataFrame()
        entered_trades_df = pd.DataFrame()
        open_trades_df = pd.DataFrame()
        shadow_entered_trades_df = pd.DataFrame()
        shadow_open_trades_df = pd.DataFrame()
        shadow_counterfactual_trades_df = pd.DataFrame()
        shadow_counterfactual_entered_trades_df = pd.DataFrame()
        shadow_counterfactual_open_trades_df = pd.DataFrame()
        variant_summary_df = pd.DataFrame()
        experiment_summary_df = pd.DataFrame()
        split_summary_df = pd.DataFrame()
        regime_summary_df = pd.DataFrame()
        rejection_summary_df = pd.DataFrame()
        symbol_summary_df = pd.DataFrame()
        bucket_summary_df = pd.DataFrame()
        trade_summary_df = pd.DataFrame()
        portfolio_df = pd.DataFrame()
        equity_df = pd.DataFrame()
        portfolio_summary_df = pd.DataFrame()
        walkforward_summary_df = pd.DataFrame()
        monte_carlo_summary_df = pd.DataFrame()
        selected_experiment_summary_df = pd.DataFrame()
        cluster_summary_df = pd.DataFrame()
        shadow_path_summary_df = pd.DataFrame()
        shadow_split_summary_df = pd.DataFrame()
        shadow_regime_summary_df = pd.DataFrame()
        shadow_bucket_summary_df = pd.DataFrame()
        shadow_symbol_summary_df = pd.DataFrame()
        shadow_rejection_summary_df = pd.DataFrame()
        shadow_parity_rejection_summary_df = pd.DataFrame()
        shadow_condition_summary_df = pd.DataFrame()
        shadow_trade_condition_summary_df = pd.DataFrame()
        shadow_counterfactual_path_summary_df = pd.DataFrame()
        shadow_counterfactual_gate_summary_df = pd.DataFrame()
        shadow_counterfactual_outcome_summary_df = pd.DataFrame()

    if not events_df.empty:
        events_df["timestamp"] = pd.to_datetime(events_df["timestamp"])
        events_df["split"] = assign_split_labels(events_df, portfolio_model, "timestamp")
        rejection_summary_df = (
            events_df[events_df["decision"] != "signal"]
            .groupby(["variant", "structure_15m_limit", "reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "count"], ascending=[True, True, False])
        )

    if not trades_df.empty:
        trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"])
        if "entry_time" in trades_df.columns:
            trades_df["entry_time"] = pd.to_datetime(trades_df["entry_time"], errors="coerce")
        if "exit_time" in trades_df.columns:
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"], errors="coerce")
        if "entry_time" in trades_df.columns:
            trades_df["split"] = assign_split_labels(trades_df, portfolio_model, "entry_time")
        else:
            trades_df["split"] = "all"
        cluster_map = build_correlation_clusters(return_series_map, portfolio_model)
        trades_df["cluster_id"] = trades_df["symbol"].map(cluster_map).fillna(trades_df["cluster_id"])
        cluster_summary_df = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "cluster_id": cluster_map.get(symbol, f"cluster_{base_symbol(symbol)}"),
                    "symbol_bucket": classify_symbol_bucket(symbol),
                    "quote_volume": metadata_map.get(symbol, {}).get("quote_volume", np.nan),
                }
                for symbol in symbols
            ]
        )
        entered_trades_df = trades_df[trades_df["trade_status"] == "entered"].copy()
        open_trades_df = trades_df[trades_df["trade_status"] == REASON_OPEN_AT_DATA_END].copy()

        if not entered_trades_df.empty:
            trade_summary_df = summarize_trade_metrics(entered_trades_df, ["variant", "structure_15m_limit", "experiment_key"])
            split_summary_df = summarize_trade_metrics(entered_trades_df, ["variant", "structure_15m_limit", "experiment_key", "split"])
            regime_summary_df = summarize_trade_metrics(
                entered_trades_df,
                ["experiment_key", "split", "structure_4h", "structure_15m", "trend_regime", "vol_regime", "liquidity_bucket"],
            )
            bucket_summary_df = summarize_trade_metrics(
                entered_trades_df,
                ["experiment_key", "split", "symbol_bucket", "liquidity_bucket", "session_bucket"],
            )
            symbol_summary_df = summarize_trade_metrics(entered_trades_df, ["experiment_key", "symbol"])

            portfolio_df, equity_df = simulate_portfolio(entered_trades_df, portfolio_model)
            if not portfolio_df.empty:
                portfolio_trade_summary = summarize_trade_metrics(portfolio_df, ["experiment_key", "split"])
                portfolio_summary_rows = []
                for experiment_key, group in portfolio_df.groupby("experiment_key"):
                    eq = equity_df[equity_df["experiment_key"] == experiment_key]
                    stats = build_equity_stats(eq)
                    metric_row = summarize_trade_metrics(group, ["experiment_key"]).iloc[0]
                    portfolio_summary_rows.append(
                        {
                            "experiment_key": experiment_key,
                            "trades": int(metric_row["trades"]),
                            "win_rate_pct": float(metric_row["win_rate_pct"]),
                            "expectancy_r": float(metric_row["expectancy_r"]),
                            "profit_factor": float(metric_row["profit_factor"]) if pd.notna(metric_row["profit_factor"]) else np.nan,
                            "total_net_pnl": float(metric_row["total_net_pnl"]),
                            **stats,
                        }
                    )
                portfolio_summary_df = pd.DataFrame(portfolio_summary_rows).sort_values("experiment_key")
                walkforward_summary_df = build_walkforward_summary(portfolio_df, portfolio_model)
                monte_carlo_summary_df = build_monte_carlo_summary(portfolio_df, portfolio_model)
            else:
                portfolio_trade_summary = pd.DataFrame()

            if not trade_summary_df.empty and not split_summary_df.empty:
                train_rank = split_summary_df[split_summary_df["split"] == "train"].sort_values(
                    ["expectancy_r", "profit_factor", "trades"], ascending=[False, False, False]
                )
                if not train_rank.empty:
                    chosen = train_rank.iloc[0]["experiment_key"]
                    selected_experiment_summary_df = split_summary_df[split_summary_df["experiment_key"] == chosen].copy()
                    selected_experiment_summary_df.insert(0, "selected_experiment", chosen)

        eval_summary_variant = (
            events_df.groupby("variant", as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s).sum())),
            )
            if not events_df.empty
            else pd.DataFrame()
        )
        eval_summary_experiment = (
            events_df.groupby(["variant", "structure_15m_limit", "experiment_key"], as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s).sum())),
            )
            if not events_df.empty
            else pd.DataFrame()
        )
        variant_metrics = summarize_trade_metrics(entered_trades_df, ["variant"]) if not entered_trades_df.empty else pd.DataFrame(columns=["variant"])
        trade_summary_ready = trade_summary_df if not trade_summary_df.empty else pd.DataFrame(columns=["variant", "structure_15m_limit", "experiment_key"])
        variant_summary_df = (
            eval_summary_variant.merge(variant_metrics, on="variant", how="left").fillna(0.0)
            if not eval_summary_variant.empty
            else variant_metrics
        )
        experiment_summary_df = (
            eval_summary_experiment.merge(trade_summary_ready, on=["variant", "structure_15m_limit", "experiment_key"], how="left").fillna(0.0)
            if not eval_summary_experiment.empty
            else trade_summary_ready
        )
    elif not events_df.empty:
        variant_summary_df = (
            events_df.groupby("variant", as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s).sum())),
            )
        )
        experiment_summary_df = (
            events_df.groupby(["variant", "structure_15m_limit", "experiment_key"], as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s).sum())),
            )
        )

    if not shadow_trades_df.empty:
        shadow_trades_df["timestamp"] = pd.to_datetime(shadow_trades_df["timestamp"])
        if "entry_time" in shadow_trades_df.columns:
            shadow_trades_df["entry_time"] = pd.to_datetime(shadow_trades_df["entry_time"], errors="coerce")
        if "exit_time" in shadow_trades_df.columns:
            shadow_trades_df["exit_time"] = pd.to_datetime(shadow_trades_df["exit_time"], errors="coerce")
        shadow_accepted_trades_df = shadow_trades_df[
            shadow_trades_df["shadow_trade_mode"] == "accepted_parity_trade"
        ].copy()
        shadow_counterfactual_trades_df = shadow_trades_df[
            shadow_trades_df["shadow_trade_mode"] == "counterfactual_rejected_candidate"
        ].copy()
        shadow_entered_trades_df = shadow_accepted_trades_df[
            shadow_accepted_trades_df["trade_status"] == "entered"
        ].copy()
        shadow_open_trades_df = shadow_accepted_trades_df[
            shadow_accepted_trades_df["trade_status"] == REASON_OPEN_AT_DATA_END
        ].copy()
        shadow_counterfactual_entered_trades_df = shadow_counterfactual_trades_df[
            shadow_counterfactual_trades_df["trade_status"] == "entered"
        ].copy()
        shadow_counterfactual_open_trades_df = shadow_counterfactual_trades_df[
            shadow_counterfactual_trades_df["trade_status"] == REASON_OPEN_AT_DATA_END
        ].copy()
        if not shadow_entered_trades_df.empty:
            if "entry_time" in shadow_entered_trades_df.columns:
                shadow_entered_trades_df["split"] = assign_split_labels(shadow_entered_trades_df, portfolio_model, "entry_time")
            else:
                shadow_entered_trades_df["split"] = "all"
            shadow_path_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["variant", "structure_15m_limit", "experiment_key", "path_experiment_key", "trigger_type"],
            )
            shadow_split_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["variant", "structure_15m_limit", "experiment_key", "path_experiment_key", "trigger_type", "split"],
            )
            shadow_regime_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                [
                    "path_experiment_key",
                    "trigger_type",
                    "split",
                    "structure_4h",
                    "structure_15m",
                    "trend_regime",
                    "vol_regime",
                    "liquidity_bucket",
                    "session_bucket",
                ],
            )
            shadow_bucket_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["path_experiment_key", "trigger_type", "split", "symbol_bucket", "liquidity_bucket", "session_bucket"],
            )
            shadow_symbol_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["path_experiment_key", "trigger_type", "symbol"],
            )
            shadow_trade_condition_summary_df = build_shadow_trade_condition_summary(
                shadow_entered_trades_df,
                shadow_condition_cols,
            )
        if not shadow_counterfactual_entered_trades_df.empty:
            if "entry_time" in shadow_counterfactual_entered_trades_df.columns:
                shadow_counterfactual_entered_trades_df["split"] = assign_split_labels(
                    shadow_counterfactual_entered_trades_df,
                    portfolio_model,
                    "entry_time",
                )
            else:
                shadow_counterfactual_entered_trades_df["split"] = "all"
            shadow_counterfactual_path_summary_df = summarize_trade_metrics(
                shadow_counterfactual_entered_trades_df,
                [
                    "variant",
                    "structure_15m_limit",
                    "path_experiment_key",
                    "trigger_type",
                    "counterfactual_rejection_stage",
                ],
            )
            shadow_counterfactual_gate_summary_df = summarize_trade_metrics(
                shadow_counterfactual_entered_trades_df,
                [
                    "variant",
                    "structure_15m_limit",
                    "trigger_type",
                    "counterfactual_rejection_stage",
                    "counterfactual_gate_reason_code",
                ],
            )
        if not shadow_counterfactual_trades_df.empty:
            shadow_counterfactual_outcome_summary_df = (
                shadow_counterfactual_trades_df.groupby(
                    [
                        "variant",
                        "structure_15m_limit",
                        "trigger_type",
                        "counterfactual_rejection_stage",
                        "counterfactual_gate_reason_code",
                        "counterfactual_outcome_bucket",
                    ],
                    dropna=False,
                )
                .size()
                .reset_index(name="count")
                .sort_values(
                    [
                        "variant",
                        "structure_15m_limit",
                        "trigger_type",
                        "counterfactual_rejection_stage",
                        "count",
                    ],
                    ascending=[True, True, True, True, False],
                )
            )

    if not shadow_events_df.empty:
        shadow_events_df["timestamp"] = pd.to_datetime(shadow_events_df["timestamp"])
        shadow_events_df["split"] = assign_split_labels(shadow_events_df, portfolio_model, "timestamp")
        shadow_rejection_summary_df = (
            shadow_events_df[~shadow_events_df["path_ready"].fillna(False)]
            .groupby(["variant", "structure_15m_limit", "trigger_type", "path_reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "trigger_type", "count"], ascending=[True, True, True, False])
        )
        shadow_parity_rejection_summary_df = (
            shadow_events_df[~shadow_events_df["parity_accepted"].fillna(False)]
            .groupby(["variant", "structure_15m_limit", "trigger_type", "parity_reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "trigger_type", "count"], ascending=[True, True, True, False])
        )
        shadow_condition_summary_df = build_shadow_condition_summary(shadow_events_df, shadow_condition_cols)

    universe_df.to_csv(os.path.join(run_dir, "universe.csv"), index=False)
    if not events_df.empty:
        events_df.to_csv(os.path.join(run_dir, "events.csv"), index=False)
    if not shadow_events_df.empty:
        shadow_events_df.to_csv(os.path.join(run_dir, "shadow_events.csv"), index=False)
    if not signals_df.empty:
        signals_df.to_csv(os.path.join(run_dir, "signals.csv"), index=False)
    if not trades_df.empty:
        trades_df.to_csv(os.path.join(run_dir, "trades.csv"), index=False)
    if not shadow_trades_df.empty:
        shadow_trades_df.to_csv(os.path.join(run_dir, "shadow_trades.csv"), index=False)
    if not shadow_counterfactual_trades_df.empty:
        shadow_counterfactual_trades_df.to_csv(os.path.join(run_dir, "shadow_counterfactual_trades.csv"), index=False)
    if not entered_trades_df.empty:
        entered_trades_df.to_csv(os.path.join(run_dir, "entered_trades.csv"), index=False)
    if not open_trades_df.empty:
        open_trades_df.to_csv(os.path.join(run_dir, "open_trades.csv"), index=False)
    if not shadow_entered_trades_df.empty:
        shadow_entered_trades_df.to_csv(os.path.join(run_dir, "shadow_entered_trades.csv"), index=False)
    if not shadow_open_trades_df.empty:
        shadow_open_trades_df.to_csv(os.path.join(run_dir, "shadow_open_trades.csv"), index=False)
    if not shadow_counterfactual_entered_trades_df.empty:
        shadow_counterfactual_entered_trades_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_entered_trades.csv"),
            index=False,
        )
    if not shadow_counterfactual_open_trades_df.empty:
        shadow_counterfactual_open_trades_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_open_trades.csv"),
            index=False,
        )
    if not trade_summary_df.empty:
        trade_summary_df.to_csv(os.path.join(run_dir, "trade_summary.csv"), index=False)
    if not shadow_path_summary_df.empty:
        shadow_path_summary_df.to_csv(os.path.join(run_dir, "shadow_path_summary.csv"), index=False)
    if not shadow_counterfactual_path_summary_df.empty:
        shadow_counterfactual_path_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_path_summary.csv"),
            index=False,
        )
    if not shadow_counterfactual_gate_summary_df.empty:
        shadow_counterfactual_gate_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_gate_summary.csv"),
            index=False,
        )
    if not shadow_counterfactual_outcome_summary_df.empty:
        shadow_counterfactual_outcome_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_outcome_summary.csv"),
            index=False,
        )
    if not variant_summary_df.empty:
        variant_summary_df.to_csv(os.path.join(run_dir, "variant_summary.csv"), index=False)
    if not experiment_summary_df.empty:
        experiment_summary_df.to_csv(os.path.join(run_dir, "experiment_summary.csv"), index=False)
    if not split_summary_df.empty:
        split_summary_df.to_csv(os.path.join(run_dir, "split_summary.csv"), index=False)
    if not shadow_split_summary_df.empty:
        shadow_split_summary_df.to_csv(os.path.join(run_dir, "shadow_split_summary.csv"), index=False)
    if not regime_summary_df.empty:
        regime_summary_df.to_csv(os.path.join(run_dir, "regime_summary.csv"), index=False)
    if not shadow_regime_summary_df.empty:
        shadow_regime_summary_df.to_csv(os.path.join(run_dir, "shadow_regime_summary.csv"), index=False)
    if not rejection_summary_df.empty:
        rejection_summary_df.to_csv(os.path.join(run_dir, "rejection_summary.csv"), index=False)
    if not shadow_rejection_summary_df.empty:
        shadow_rejection_summary_df.to_csv(os.path.join(run_dir, "shadow_rejection_summary.csv"), index=False)
    if not shadow_parity_rejection_summary_df.empty:
        shadow_parity_rejection_summary_df.to_csv(os.path.join(run_dir, "shadow_parity_rejection_summary.csv"), index=False)
    if not symbol_summary_df.empty:
        symbol_summary_df.to_csv(os.path.join(run_dir, "symbol_summary.csv"), index=False)
    if not shadow_symbol_summary_df.empty:
        shadow_symbol_summary_df.to_csv(os.path.join(run_dir, "shadow_symbol_summary.csv"), index=False)
    if not bucket_summary_df.empty:
        bucket_summary_df.to_csv(os.path.join(run_dir, "bucket_summary.csv"), index=False)
    if not shadow_bucket_summary_df.empty:
        shadow_bucket_summary_df.to_csv(os.path.join(run_dir, "shadow_bucket_summary.csv"), index=False)
    if not shadow_condition_summary_df.empty:
        shadow_condition_summary_df.to_csv(os.path.join(run_dir, "shadow_condition_summary.csv"), index=False)
    if not shadow_trade_condition_summary_df.empty:
        shadow_trade_condition_summary_df.to_csv(os.path.join(run_dir, "shadow_trade_condition_summary.csv"), index=False)
    if not cluster_summary_df.empty:
        cluster_summary_df.to_csv(os.path.join(run_dir, "cluster_summary.csv"), index=False)
    if not portfolio_df.empty:
        portfolio_df.to_csv(os.path.join(run_dir, "portfolio_trades.csv"), index=False)
    if not equity_df.empty:
        equity_df.to_csv(os.path.join(run_dir, "portfolio_equity.csv"), index=False)
    if not portfolio_summary_df.empty:
        portfolio_summary_df.to_csv(os.path.join(run_dir, "portfolio_summary.csv"), index=False)
    if not walkforward_summary_df.empty:
        walkforward_summary_df.to_csv(os.path.join(run_dir, "walkforward_summary.csv"), index=False)
    if not monte_carlo_summary_df.empty:
        monte_carlo_summary_df.to_csv(os.path.join(run_dir, "monte_carlo_summary.csv"), index=False)
    if not selected_experiment_summary_df.empty:
        selected_experiment_summary_df.to_csv(os.path.join(run_dir, "selected_experiment_summary.csv"), index=False)

    summary = {
        "run_id": run_id,
        "universe_source": args.universe_source,
        "symbols_requested": len(symbols),
        "symbols_processed": int((universe_df["status"] == "processed").sum()) if not universe_df.empty else 0,
        "symbols_skipped": int((universe_df["status"] != "processed").sum()) if not universe_df.empty else 0,
        "open_trades_count": int(len(open_trades_df)),
        "shadow_open_trades_count": int(len(shadow_open_trades_df)),
        "shadow_counterfactual_trades_count": int(len(shadow_counterfactual_trades_df)),
        "shadow_counterfactual_open_trades_count": int(len(shadow_counterfactual_open_trades_df)),
        "variants": [variant.name for variant in variants],
        "structure_15m_limits": args.structure_15m_limits,
        "limit": args.limit,
        "warmup": args.warmup,
        "step": args.step,
        "lookahead": args.lookahead,
        "duration_sec": round(time.time() - started, 2),
        "profile_name": runtime_overrides.get("profile_name", ""),
        "profile_file": runtime_overrides.get("profile_file", ""),
        "profile_meta": runtime_overrides.get("profile_meta", {}),
        "signal_config_overrides_file": runtime_overrides.get("signal_config_overrides_file", ""),
        "trade_config_overrides_file": runtime_overrides.get("trade_config_overrides_file", ""),
        "research_config_overrides_file": runtime_overrides.get("research_config_overrides_file", ""),
        "config_hashes": config_hashes,
        "execution_model": execution_model.__dict__,
        "portfolio_model": portfolio_model.__dict__,
        "shadow_research_mode": "path_aware_static_ensemble_decision_close",
        "variant_summary": variant_summary_df.to_dict(orient="records") if not variant_summary_df.empty else [],
        "experiment_summary": experiment_summary_df.to_dict(orient="records") if not experiment_summary_df.empty else [],
        "portfolio_summary": portfolio_summary_df.to_dict(orient="records") if not portfolio_summary_df.empty else [],
        "shadow_path_summary": shadow_path_summary_df.to_dict(orient="records") if not shadow_path_summary_df.empty else [],
        "shadow_counterfactual_path_summary": shadow_counterfactual_path_summary_df.to_dict(orient="records") if not shadow_counterfactual_path_summary_df.empty else [],
        "shadow_counterfactual_top_gates": shadow_counterfactual_gate_summary_df.head(25).to_dict(orient="records") if not shadow_counterfactual_gate_summary_df.empty else [],
        "shadow_counterfactual_outcomes": shadow_counterfactual_outcome_summary_df.head(50).to_dict(orient="records") if not shadow_counterfactual_outcome_summary_df.empty else [],
        "shadow_top_rejections": shadow_rejection_summary_df.head(25).to_dict(orient="records") if not shadow_rejection_summary_df.empty else [],
        "shadow_top_parity_rejections": shadow_parity_rejection_summary_df.head(25).to_dict(orient="records") if not shadow_parity_rejection_summary_df.empty else [],
        "shadow_condition_columns": [col.replace("cond_", "", 1) for col in shadow_condition_cols],
        "effective_signal_config_file": os.path.join(run_dir, "effective_signal_config.json"),
        "effective_trade_config_file": os.path.join(run_dir, "effective_trade_config.json"),
        "effective_research_config_file": os.path.join(run_dir, "effective_research_config.json"),
        "applied_overrides_file": os.path.join(run_dir, "applied_overrides.json"),
        "top_rejections": rejection_summary_df.head(25).to_dict(orient="records") if not rejection_summary_df.empty else [],
        "output_dir": run_dir,
    }
    with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    logger.info("Quant research complete in %.1fs", summary["duration_sec"])
    logger.info("Outputs written to %s", run_dir)
    if not experiment_summary_df.empty:
        logger.info("\n%s", experiment_summary_df.to_string(index=False))
    elif not variant_summary_df.empty:
        logger.info("\n%s", variant_summary_df.to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
