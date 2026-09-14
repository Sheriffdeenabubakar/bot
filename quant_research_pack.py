from __future__ import annotations

import csv
import json
import math
import os
import shutil
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo


TZ = ZoneInfo("Africa/Lagos")
QUANT_TELEMETRY_CUTOFF = datetime(2026, 6, 22, 0, 0, 0, tzinfo=TZ)
STABILITY_WINDOWS = 3
RESEARCH_ENGINE_GENERATION = "gen_deep_gates_arrays_dol_v1"
QUANT_PERCENTAGE_SUPPORT_FLOORS = str(os.getenv("QUANT_PERCENTAGE_SUPPORT_FLOORS", "1")).strip().lower() not in {"0", "false", "no"}
try:
    QUANT_REALIZED_R_CAP = float(os.getenv("QUANT_REALIZED_R_CAP", "5.0") or 5.0)
except Exception:
    QUANT_REALIZED_R_CAP = 5.0
PRECISION_TARGET_WR_PCT = 50.0
PRECISION_SELECTION_MIN_RETAINED_PCT = 0.0
PRECISION_WR_LCB_Z = 1.281551565545
PRECISION_MIN_RETAINED_PCT = 40.0
PRECISION_MIN_KEPT_ABS = 40
PRECISION_MIN_KEPT_RATIO = 0.40
PRECISION_MIN_LCB_ABS = 42.0
PRECISION_MIN_LCB_EDGE_VS_BASELINE = 8.0
PRECISION_MAX_DYNAMIC_RULES = 8
PRECISION_MAX_LCB_DROP_PCT = 0.5
PRECISION_ENGINE_MIN_WR_PCT = {
    "breakout_BUY": 55.0,
    "breakout_SELL": 52.0,
    "sweep_BUY": 50.0,
    "sweep_SELL": 50.0,
}
MIN_FILTER_ZERO_WIN_SUPPORT_ABS = 12
MIN_FILTER_ZERO_WIN_SUPPORT_RATIO = 0.015
MIN_FILTER_HARDBLOCK_SUPPORT_ABS = 18
MIN_FILTER_HARDBLOCK_SUPPORT_RATIO = 0.025
MIN_FILTER_BOOST_SUPPORT_ABS = 24
MIN_FILTER_BOOST_SUPPORT_RATIO = 0.03
MIN_FILTER_UPLIFT_SUPPORT_ABS = 28
MIN_FILTER_UPLIFT_SUPPORT_RATIO = 0.035
MIN_DYNAMIC_FAMILY_STEP_TRADES = 8
PRECISION_RULE_ADD_MIN_DELTA_WR = 2.0
PRECISION_RULE_ADD_MIN_DELTA_MEAN_R = 0.02
PRECISION_RULE_ADD_MIN_DELTA_RETAINED_PCT = 1.25
PRACTICAL_FALLBACK_TARGET_WR_PCT = 45.0
PRACTICAL_FALLBACK_MIN_RETAINED_PCT = 40.0
PRACTICAL_FALLBACK_MAX_ALLOWLIST_RULES = 32
PRACTICAL_FALLBACK_MAX_BLOCKLIST_RULES = 48
PRACTICAL_FALLBACK_MIN_CANDIDATE_WR_PCT = 40.0
BALANCED_RULE_ADD_MIN_DELTA_WR = 0.15
BALANCED_RULE_ADD_MIN_DELTA_MEAN_R = 0.01
BALANCED_RULE_ADD_MAX_RETENTION_LOSS_PCT = 1.0
BALANCED_PHASE1_MAX_RULE_RETENTION_LOSS_PCT = 5.0
BALANCED_PHASE2_MAX_RULE_RETENTION_LOSS_PCT = 1.5
GLOBAL_CONTEXT_SUPPORT_MULTIPLIER = 1.5
STABILITY_WINDOW_SUPPORT_RATIO = 0.01
INTERACTION_STACK_2WAY_SMALL_ENGINE_RATIO = 0.025
INTERACTION_STACK_DEFAULT_RATIO = 0.04
FEATURE_HEALTH_MIN_PRESENT_RATIO = 0.20
FEATURE_HEALTH_MAX_DOMINANT_STATE_PCT = 98.0
FEATURE_HEALTH_EXCLUDED_REASONS: dict[str, str] = {}
STATIC_RESEARCH_UNSAFE_FEATURES = {
    "structure_alignment",
    "alignment_state",
    "config_snapshot_full",
    "signal_payload_snapshot",
    "order_flow_confirmation_state",
    "order_flow_live_snapshot",
    "order_flow_candle_snapshot",
    "decision_time_provenance",
    "market_context_snapshot",
    "market_meta_snapshot",
    "order_flow_footprint",
    "order_flow_absorption",
    "order_flow_liquidity",
    "order_flow_queue",
    "order_flow_ws_diagnostics",
    "candle_absorption_proxy",
    "candle_divergence_proxy",
    "breakout_candle_proxy",
    "adx_audit_states",
}
POSITIVE_SIGNAL_QUARANTINE_FEATURES = {
    "has_sweep_anchor_divergence_flag",
    "strong_sweep_anchor_divergence_flag",
    "sweep_anchor_divergence_mode",
    "sweep_anchor_divergence_score_bucket",
}
WEBSOCKET_ORDERFLOW_FEATURE_PREFIXES = ("of_",)

ROOT = Path(__file__).resolve().parent
RESOLVED_PATH = ROOT / "live_trade_audit_resolved.jsonl"
OUT_DIR = ROOT / "quant_research_runtime"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
            dir=path.parent,
            prefix=f"{path.name}.",
            suffix=".tmp",
        ) as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
            tmp_name = fh.name
        os.replace(tmp_name, path)
    finally:
        if tmp_name:
            tmp_path = Path(tmp_name)
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass


def materialize_local_snapshot(path: Path) -> Path:
    if str(os.getenv("QUANT_USE_LOCAL_SNAPSHOT", "0")).strip().lower() not in {"1", "true", "yes"}:
        return path

    last_error = None
    for attempt in range(3):
        tmp_name = None
        try:
            with path.open("rb") as src:
                with tempfile.NamedTemporaryFile(
                    "wb",
                    delete=False,
                    suffix=path.suffix,
                ) as dst:
                    while True:
                        chunk = src.read(256 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                    dst.flush()
                    os.fsync(dst.fileno())
                    tmp_name = dst.name
            return Path(tmp_name)
        except OSError as exc:
            last_error = exc
            if tmp_name:
                tmp_path = Path(tmp_name)
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
            time.sleep(0.5 * (attempt + 1))
        except Exception:
            if tmp_name:
                tmp_path = Path(tmp_name)
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
            raise

    print(f"WARNING: local snapshot copy failed for {path}: {last_error}; reading source file directly")
    return path


def iter_jsonl_lines_resilient(path: Path):
    offset = 0
    buffer = b""
    chunk_size = 64 * 1024
    retry_sleep_s = 0.25
    retries_at_offset = 0
    max_retries_at_offset = 8

    while True:
        try:
            with path.open("rb") as fh:
                fh.seek(offset)
                chunk = fh.read(chunk_size)
        except OSError:
            retries_at_offset += 1
            if retries_at_offset > max_retries_at_offset:
                raise
            time.sleep(retry_sleep_s * retries_at_offset)
            chunk_size = max(4096, chunk_size // 2)
            continue

        retries_at_offset = 0
        if not chunk:
            if buffer:
                yield buffer.decode("utf-8", errors="replace")
            break

        offset += len(chunk)
        buffer += chunk
        lines = buffer.split(b"\n")
        buffer = lines.pop()
        for raw_line in lines:
            yield raw_line.decode("utf-8", errors="replace")


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return None


def pick_dt(row: dict[str, Any], *keys: str) -> datetime | None:
    if not isinstance(row, dict):
        return None
    for key in keys:
        dt = parse_dt(row.get(key))
        if dt is not None:
            return dt
    return None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def cap_realized_r(value: float | None) -> float | None:
    if value is None:
        return None
    cap = QUANT_REALIZED_R_CAP
    if cap <= 0:
        return value
    return max(-cap, min(cap, value))


def is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def bucket(value: float | None, specs: list[tuple[float | None, float | None, str]]) -> str | None:
    if value is None:
        return None
    for low, high, label in specs:
        if (low is None or value >= low) and (high is None or value < high):
            return label
    return None


def norm_cat(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    return text


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def pct(part: int, total: int) -> float | None:
    if total == 0:
        return None
    return 100.0 * part / total


def round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def scaled_support_floor(total: int, *, minimum: int, ratio: float) -> int:
    if total <= 0:
        return max(1, minimum)
    percentage_floor = max(1, int(math.ceil(total * ratio)))
    if QUANT_PERCENTAGE_SUPPORT_FLOORS:
        return percentage_floor
    return max(minimum, percentage_floor)


def wilson_lower_bound_pct(wins: int, n: int, z: float = PRECISION_WR_LCB_Z) -> float | None:
    if n <= 0:
        return None
    phat = wins / n
    denominator = 1.0 + (z * z / n)
    centre = phat + (z * z / (2.0 * n))
    margin = z * math.sqrt((phat * (1.0 - phat) / n) + (z * z / (4.0 * n * n)))
    return max(0.0, min(100.0, 100.0 * ((centre - margin) / denominator)))


def as_bool_flag(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip().lower()
    if text in {"true", "false"}:
        return text
    return None


def list_len_bucket(value: Any) -> str | None:
    if not isinstance(value, list):
        return None
    n = len(value)
    if n == 0:
        return "0"
    if n == 1:
        return "1"
    if n == 2:
        return "2"
    if n == 3:
        return "3"
    return "4plus"


def signed_strength_bucket(
    value: float | None,
    *,
    strong_negative: float,
    mild_negative: float,
    mild_positive: float,
    strong_positive: float,
) -> str | None:
    if value is None:
        return None
    if value < strong_negative:
        return "sharp_falling"
    if value < mild_negative:
        return "falling"
    if value <= mild_positive:
        return "flat"
    if value < strong_positive:
        return "rising"
    return "sharp_rising"


def side_bias_bucket(bid_count: int, ask_count: int) -> str | None:
    total = bid_count + ask_count
    if total <= 0:
        return None
    bid_share = bid_count / total
    if bid_share >= 0.60:
        return "bid_heavy"
    if bid_share <= 0.40:
        return "ask_heavy"
    return "balanced"


FEATURE_LABELS = {
    "session": "session",
    "market_regime": "market regime",
    "structure_alignment": "structure alignment",
    "selected_level_type": "selected level type",
    "audit_liquidity_bucket": "audit liquidity",
    "liquidity_bucket": "live liquidity",
    "htf_confluence_bucket": "HTF confluence",
    "htf_confluence_score_bucket": "HTF confluence score",
    "has_sweep_anchor_divergence_flag": "sweep-anchor divergence",
    "strong_sweep_anchor_divergence_flag": "strong sweep-anchor divergence",
    "sweep_anchor_divergence_mode": "sweep-anchor mode",
    "sweep_anchor_divergence_score_bucket": "sweep-anchor score",
    "gate_market_regime_ranging_flag": "gate ranging regime",
    "gate_of_final_score_gap_bucket": "gate OF score gap",
    "gate_static_score_gap_bucket": "gate static score gap",
    "gate_static_vote_gap_bucket": "gate ensemble vote gap",
    "gate_adx_failure_flag": "gate ADX fail",
    "gate_adx_timeframe": "gate ADX timeframe",
    "gate_adx_source": "gate ADX source",
    "gate_adx_fail_slope_bucket": "gate ADX fail slope",
    "gate_divergence_flag": "gate opposing divergence",
    "gate_divergence_type": "gate divergence type",
    "gate_divergence_strength_bucket": "gate divergence strength",
    "gate_no_htf_alignment_flag": "gate no HTF alignment",
    "gate_no_htf_trigger_type": "gate no HTF trigger",
    "gate_htf_score_bucket": "gate HTF score",
    "gate_volume_structure_fail_flag": "gate volume structure fail",
    "gate_volume_structure_score_bucket": "gate volume structure score",
    "gate_orderflow_unavailable_flag": "gate orderflow unavailable",
    "gate_recent_candle_fail_flag": "gate recent candle fail",
    "gate_recent_candle_score_bucket": "gate recent candle score",
    "gate_late_breakout_flag": "gate late breakout",
    "gate_late_breakout_atr_bucket": "gate late breakout ATR",
    "gate_path_specific_of_fail_flag": "gate path-specific OF fail",
    "gate_path_specific_directional_pressure_bucket": "gate path OF pressure",
    "gate_path_specific_imbalance_bucket": "gate path OF imbalance",
    "gate_path_specific_microstructure_bucket": "gate path microstructure pts",
    "gate_sweep_breakout_conflict_flag": "gate sweep-breakout conflict",
    "gate_dol_dominant_4h_flag": "gate dominant 4H liquidity",
    "gate_dol_inducement_ratio_bucket": "gate DOL inducement",
    "gate_dol_s1_score_bucket": "gate DOL S1 score",
    "gate_dol_source_4h_flag": "gate DOL 4H source",
    "gate_post_of_proximity_flag": "gate post-OF proximity",
    "gate_post_of_proximity_atr_bucket": "gate post-OF ATR",
    "order_flow_signal": "order-flow signal",
    "order_flow_live_source": "order-flow source",
    "of_live_available_flag": "live WS OF available",
    "candle_of_available_flag": "candle OF available",
    "of_trigger_type": "OF trigger",
    "of_iceberg_direction": "iceberg direction",
    "of_confirmation_score_bucket": "OF score",
    "of_confirmation_votes_bucket": "OF votes",
    "of_microstructure_score_bucket": "microstructure confirmation pts",
    "of_hard_block_flag": "OF hard block",
    "order_flow_confirmation_state": "order-flow confirmation",
    "market_pressure_state": "market pressure",
    "market_sentiment_classification": "sentiment",
    "market_sentiment_quarantine_flag": "sentiment quarantine",
    "basis_stress_state": "basis stress",
    "liquidation_environment_state": "liquidation environment",
    "btc_regime_15m": "BTC 15m regime",
    "btc_regime_1h": "BTC 1h regime",
    "btc_regime_4h": "BTC 4h regime",
    "eth_regime_15m": "ETH 15m regime",
    "eth_regime_1h": "ETH 1h regime",
    "eth_regime_4h": "ETH 4h regime",
    "selected_level_age_bucket": "selected level age",
    "selected_level_distance_bucket": "selected level distance",
    "swing_total_count_15m_bucket": "15m swing count",
    "swing_total_count_4h_bucket": "4h swing count",
    "market_pressure_score_bucket": "market pressure score",
    "major_coin_breadth_bullish_pct_bucket": "major breadth bullish",
    "scanned_symbol_bullish_pct_bucket": "scanner breadth bullish",
    "funding_breadth_positive_pct_bucket": "positive funding breadth",
    "funding_breadth_mean_bps_bucket": "funding breadth mean",
    "breakout_success_breadth_6h_bucket": "6h breakout breadth",
    "breakout_success_breadth_24h_bucket": "24h breakout breadth",
    "audit_adx_1h_slope_bucket": "1h ADX slope",
    "audit_adx_1h_current_bucket": "1h ADX",
    "audit_adx_5m_current_bucket": "5m ADX",
    "audit_adx_5m_slope_bucket": "5m ADX slope",
    "audit_adx_3m_current_bucket": "3m ADX",
    "audit_adx_3m_slope_bucket": "3m ADX slope",
    "audit_adx_15m_current_bucket": "15m ADX",
    "audit_adx_15m_slope_bucket": "15m ADX slope",
    "path_adx_current_bucket": "path ADX",
    "path_adx_slope_bucket": "path ADX slope",
    "ensemble_votes_bucket": "ensemble votes",
    "context_risk_multiplier_bucket": "context risk multiplier",
    "of_buy_pressure_bucket": "OF buy pressure",
    "of_sell_pressure_bucket": "OF sell pressure",
    "of_opposing_pressure_bucket": "OF opposing pressure",
    "of_aggression_bucket": "OF aggression",
    "of_imbalance_bucket": "OF imbalance",
    "of_cvd_sign": "OF CVD sign",
    "of_cvd_slope_bucket": "OF CVD slope",
    "of_cvd_intensity_bucket": "OF CVD intensity",
    "of_aggression_delta_bucket": "OF aggression delta",
    "of_directional_pressure_bucket": "OF directional pressure",
    "of_buy_pressure_slope_bucket": "OF buy-pressure slope",
    "of_sell_pressure_slope_bucket": "OF sell-pressure slope",
    "of_opposing_pressure_slope_bucket": "OF opposing-pressure slope",
    "of_imbalance_slope_bucket": "OF imbalance slope",
    "of_pressure_slope_bucket": "OF pressure slope",
    "of_pressure_alignment": "OF pressure alignment",
    "of_net_delta_sign": "OF net delta",
    "of_delta_divergence": "OF delta divergence",
    "of_divergence_strength_bucket": "OF divergence strength",
    "of_delta_efficiency_bucket": "OF delta efficiency",
    "of_price_move_bps_bucket": "OF price move",
    "of_absorption_detected": "OF absorption",
    "of_absorption_ratio_bucket": "OF absorption ratio",
    "of_absorbed_levels_bucket": "absorbed levels",
    "of_absorption_strong_levels_bucket": "strong absorbed levels",
    "of_absorption_key_levels_bucket": "absorbed key levels",
    "of_absorption_max_notional_bucket": "max absorbed notional",
    "of_ob_level_absorbed_flag": "OB absorbed",
    "of_total_vol_vs_vpt_bucket": "OF vol/VPT",
    "of_footprint_sufficient_data_flag": "footprint sufficient data",
    "of_absorption_sufficient_data_flag": "absorption sufficient data",
    "of_opposed_levels_bucket": "opposed levels",
    "of_top_opposed_notional_bucket": "top opposed notional",
    "of_opposed_concentration_bucket": "opposed concentration",
    "of_wall_count_bucket": "liquidity walls",
    "of_wall_side_bias": "wall side bias",
    "of_wall_max_mult_bucket": "wall max mult",
    "of_wall_avg_mult_bucket": "wall avg mult",
    "of_void_count_bucket": "liquidity voids",
    "of_void_side_bias": "void side bias",
    "of_void_max_span_bps_bucket": "void max span",
    "of_liquidity_sufficient_data_flag": "liquidity sufficient data",
    "of_migration_count_bucket": "liquidity migrations",
    "of_migration_direction_bucket": "migration direction",
    "of_migration_direction_alignment": "migration alignment",
    "of_migration_notional_bucket": "migration notional",
    "of_migration_delta_bps_bucket": "migration delta",
    "of_icebergs_detected_flag": "icebergs",
    "of_spoofs_detected_flag": "spoofs",
    "of_bid_icebergs_bucket": "bid icebergs",
    "of_ask_icebergs_bucket": "ask icebergs",
    "of_total_notional_bucket": "OF total notional",
    "of_notional_side_bias": "notional side bias",
    "of_queue_bid_z_bucket": "queue bid z-score",
    "of_queue_ask_z_bucket": "queue ask z-score",
    "of_queue_skew_bucket": "queue skew",
    "of_queue_bid_touch_rel_bucket": "queue bid touch/mean",
    "of_queue_ask_touch_rel_bucket": "queue ask touch/mean",
    "of_queue_min_samples_bucket": "queue min samples",
    "of_queue_sufficient_data_flag": "queue sufficient data",
    "of_queue_alignment": "queue alignment",
    "of_queue_drain_alignment": "queue drain alignment",
    "of_queue_support_alignment": "queue support alignment",
    "of_queue_touch_alignment": "queue touch alignment",
    "of_spread_bps_bucket": "spread bps",
    "of_ws_health_bucket": "WS health",
    "of_ws_issue_bucket": "WS issues",
    "of_ws_snapshots_bucket": "WS snapshots",
    "of_ws_updates_bucket": "WS updates",
    "of_ws_reconnect_bucket": "WS reconnects",
    "of_ws_checksum_failure_flag": "WS checksum failure",
    "of_ws_sequence_gap_flag": "WS sequence gap",
    "of_ws_stale_update_flag": "WS stale update",
    "of_ws_resync_request_flag": "WS resync request",
    "of_window_seconds_bucket": "OF window",
    "of_sweep_age_bucket": "OF sweep age",
    "breakout_body_ratio_bucket": "breakout body ratio",
    "breakout_range_atr_bucket": "breakout range",
    "breakout_strength_bucket": "breakout strength",
    "breakout_close_through_bucket": "breakout close-through",
    "breakout_progress_bucket": "breakout progress",
    "breakout_vol_decay_bucket": "breakout volume decay",
    "breakout_vol_ratio_bucket": "breakout volume ratio",
    "breakout_continuation_confirmed": "breakout continuation",
    "sweep_absorbed_flag": "sweep absorbed",
    "sweep_absorption_strength_bucket": "sweep absorption strength",
    "sweep_divergence_flag": "sweep divergence",
    "sweep_divergence_strength_bucket": "sweep divergence strength",
    "resolution_time_bucket": "resolution",
}


STATE_LABELS = {
    "same_direction": "same direction",
    "opposed_direction": "opposed direction",
    "ranging": "ranging",
    "trending_weak": "trending weak",
    "trending_strong": "trending strong",
    "bullish": "bullish",
    "bearish": "bearish",
    "mixed": "mixed",
    "balanced": "balanced",
    "risk_off_bearish": "risk-off bearish",
    "risk_on_bullish": "risk-on bullish",
    "normal": "normal",
    "elevated": "elevated",
    "none": "none",
    "true": "true",
    "false": "false",
    "lt30m": "<30m",
    "30_60m": "30-60m",
    "1_2h": "1-2h",
    "2_4h": "2-4h",
    "4_8h": "4-8h",
    "8_24h": "8-24h",
    "24hplus": "24h+",
    "lt3": "<3",
    "3_7": "3-7",
    "7_15": "7-15",
    "15plus": "15+",
    "le15": "<=15",
    "16_18": "16-18",
    "19_20": "19-20",
    "21plus": "21+",
    "lt30": "<30%",
    "30_40": "30-40%",
    "30_45": "30-45%",
    "40_50": "40-50%",
    "45_60": "45-60%",
    "50_60": "50-60%",
    "ge60": ">=60%",
    "lt40": "<40%",
    "lt50": "<50",
    "ge75": ">=75%",
    "le60": "<=60%",
    "60_65": "60-65%",
    "65_75": "65-75%",
    "le0": "<=0 bps",
    "0_1": "0-1 bps",
    "1_3": "1-3 bps",
    "ge3": ">=3 bps",
    "lt_-5p5": "<-5.5",
    "-5p5_to_-3p5": "-5.5 to -3.5",
    "-3p5_to_0": "-3.5 to 0",
    "ge0": ">=0",
    "lt2": "<2",
    "2_5": "2-5",
    "lt_-0p5": "<-0.5",
    "-0p5_to_0": "-0.5 to 0",
    "0_to_0p5": "0 to +0.5",
    "0p5_to_1": "+0.5 to +1.0",
    "ge1": ">=+1.0",
    "sharp_falling": "sharp falling",
    "falling": "falling",
    "flat": "flat",
    "rising": "rising",
    "sharp_rising": "sharp rising",
    "lt20": "<20",
    "20_25": "20-25",
    "25_30": "25-30",
    "30_40": "30-40",
    "40plus": "40+",
    "0": "0",
    "1": "1",
    "1_2": "1-2",
    "2": "2",
    "3": "3",
    "3plus": "3+",
    "3_4": "3-4",
    "4_5": "4-5",
    "4plus": "4+",
    "5plus": "5+",
    "6plus": "6+",
    "6_8": "6-8",
    "9plus": "9+",
    "lt45": "<45%",
    "45_55": "45-55%",
    "55_65": "55-65%",
    "65plus": "65%+",
    "lt0": "<0",
    "0_0p2": "0-0.2",
    "0p2_0p5": "0.2-0.5",
    "0p5plus": "0.5+",
    "lt0p1": "<0.1",
    "0p1_0p2": "0.1-0.2",
    "0p2_0p3": "0.2-0.3",
    "0p3plus": "0.3+",
    "0_0p05": "0-0.05",
    "0p05_0p15": "0.05-0.15",
    "0p15plus": "0.15+",
    "lt_-0p1": "<-0.1",
    "-0p1_to_0": "-0.1 to 0",
    "0_to_0p1": "0 to 0.1",
    "0p1plus": "0.1+",
    "lt_-1": "<-1",
    "-1_to_0": "-1 to 0",
    "0_to_1": "0 to 1",
    "lt_-0p05": "<-0.05",
    "-0p05_to_0": "-0.05 to 0",
    "0_to_0p05": "0 to 0.05",
    "0p05plus": "0.05+",
    "0_0p5": "0-0.5",
    "0p5_1p5": "0.5-1.5",
    "0_50": "0-50",
    "50_200": "50-200",
    "200plus": "200+",
    "aligned": "aligned",
    "opposed": "opposed",
    "bid_heavy": "bid heavy",
    "ask_heavy": "ask heavy",
    "toward_bid": "toward bid",
    "toward_ask": "toward ask",
    "lt0p25": "<0.25",
    "lt0p5": "<0.5",
    "0p25_0p5": "0.25-0.5",
    "0p5_1": "0.5-1.0",
    "0p5_0p75": "0.5-0.75",
    "0p75plus": "0.75+",
    "1_1p5": "1.0-1.5",
    "1p5plus": "1.5+",
    "1p5_2": "1.5-2.0",
    "2plus": "2+",
    "0p5_0p8": "0.5-0.8",
    "0p8plus": "0.8+",
    "lt1": "<1.0",
    "1_1p5": "1.0-1.5",
    "1p5_2p5": "1.5-2.5",
    "2p5plus": "2.5+",
    "lt10": "<10",
    "10_30": "10-30",
    "30_60": "30-60",
    "60plus": "60+",
    "lt25": "<25",
    "25_50": "25-50",
    "50_68": "50-68",
    "68plus": "68+",
    "lt500": "<500",
    "500_1500": "500-1.5k",
    "1500_5000": "1.5k-5k",
    "5000plus": "5k+",
    "lt5k": "<5k",
    "5k_20k": "5k-20k",
    "20k_75k": "20k-75k",
    "75kplus": "75k+",
    "clean": "clean",
    "minor_issues": "minor issues",
    "degraded": "degraded",
    "le0p75": "<=0.75",
    "0p75_0p9": "0.75-0.9",
    "0p9_1": "0.9-1.0",
    "ge1": ">=1.0",
}


def humanize_engine(engine: str) -> str:
    parts = engine.split("_")
    if len(parts) >= 2:
        setup = " ".join(parts[:-1]).replace("_", " ").title()
        direction = parts[-1].upper()
        return f"{setup} {direction}"
    return engine.replace("_", " ").title()


def humanize_state_token(state: str) -> str:
    text = str(state)
    return STATE_LABELS.get(text, text.replace("_", " "))


def humanize_feature_state(feature: str, state: str) -> str:
    label = FEATURE_LABELS.get(feature, feature.replace("_", " "))
    state_label = humanize_state_token(state)
    percent_features = {
        "major_coin_breadth_bullish_pct_bucket",
        "scanned_symbol_bullish_pct_bucket",
        "funding_breadth_positive_pct_bucket",
        "breakout_success_breadth_6h_bucket",
        "breakout_success_breadth_24h_bucket",
        "of_directional_pressure_bucket",
    }
    if feature in percent_features and "%" not in state_label:
        state_label = f"{state_label}%"
    if feature == "resolution_time_bucket":
        return f"resolution {state_label}"
    if feature == "selected_level_age_bucket":
        return f"selected level age {state_label} candles"
    if feature == "selected_level_distance_bucket":
        return f"selected level distance {state_label} ATR"
    if feature in {"of_wall_count_bucket", "of_void_count_bucket", "of_migration_count_bucket"}:
        noun = {
            "of_wall_count_bucket": "walls",
            "of_void_count_bucket": "voids",
            "of_migration_count_bucket": "migrations",
        }[feature]
        if state == "1":
            noun = noun[:-1]
        return f"{state_label} {noun}"
    if feature == "funding_breadth_mean_bps_bucket":
        return f"funding breadth mean {state_label}"
    if feature == "of_directional_pressure_bucket":
        return f"OF directional pressure {state_label}"
    if feature == "major_coin_breadth_bullish_pct_bucket":
        return f"major breadth bullish {state_label}"
    if feature == "scanned_symbol_bullish_pct_bucket":
        return f"scanner breadth bullish {state_label}"
    return f"{label} {state_label}".strip()


def describe_candidate_id(candidate_id: str) -> dict[str, Any]:
    if candidate_id.startswith("global_state::"):
        _, feature, state = candidate_id.split("::", 2)
        component = humanize_feature_state(feature, state)
        return {
            "engine": "ALL",
            "engine_label": "All engines",
            "components": [component],
            "component_count": 1,
            "candidate_label": f"All engines | {component}",
        }
    if candidate_id.startswith("state::"):
        _, engine, feature, state = candidate_id.split("::", 3)
        component = humanize_feature_state(feature, state)
        engine_label = humanize_engine(engine)
        return {
            "engine": engine,
            "engine_label": engine_label,
            "components": [component],
            "component_count": 1,
            "candidate_label": f"{engine_label} | {component}",
        }
    if candidate_id.startswith("stack::"):
        _, engine, stack_text = candidate_id.split("::", 2)
        components = [humanize_feature_state(*part.split("=", 1)) for part in stack_text.split(" | ")]
        engine_label = humanize_engine(engine)
        return {
            "engine": engine,
            "engine_label": engine_label,
            "components": components,
            "component_count": len(components),
            "candidate_label": f"{engine_label} | {' | '.join(components)}",
        }
    return {
        "engine": None,
        "engine_label": None,
        "components": [candidate_id],
        "component_count": 1,
        "candidate_label": candidate_id,
    }


POST_TRADE_FEATURES = {
    "resolution_time_bucket",
}


NON_NORMALIZED_STRUCTURED_FEATURES = {
    "order_flow_confirmation_state",
    "order_flow_live_snapshot",
    "order_flow_candle_snapshot",
    "decision_time_provenance",
    "market_context_snapshot",
    "market_meta_snapshot",
    "config_snapshot_full",
    "signal_payload_snapshot",
    "order_flow_footprint",
    "order_flow_absorption",
    "order_flow_liquidity",
    "order_flow_queue",
    "order_flow_ws_diagnostics",
    "candle_absorption_proxy",
    "candle_divergence_proxy",
    "breakout_candle_proxy",
    "adx_audit_states",
}


def extract_candidate_parts(candidate_id: str) -> list[tuple[str, str]]:
    if candidate_id.startswith("global_state::"):
        _, feature, state = candidate_id.split("::", 2)
        return [(feature, state)]
    if candidate_id.startswith("state::"):
        _, _engine, feature, state = candidate_id.split("::", 3)
        return [(feature, state)]
    if candidate_id.startswith("stack::"):
        _, _engine, stack_text = candidate_id.split("::", 2)
        out = []
        for part in stack_text.split(" | "):
            if "=" not in part:
                continue
            feature, state = part.split("=", 1)
            out.append((feature, state))
        return out
    return []


def candidate_engine(candidate_id: str) -> str | None:
    if candidate_id.startswith("global_state::"):
        return None
    if candidate_id.startswith("state::"):
        parts = candidate_id.split("::", 3)
        return parts[1] if len(parts) >= 2 else None
    if candidate_id.startswith("stack::"):
        parts = candidate_id.split("::", 2)
        return parts[1] if len(parts) >= 2 else None
    return None


def candidate_matches_trade(trade: "Trade", candidate_id: str) -> bool:
    engine = candidate_engine(candidate_id)
    if engine is not None and trade.engine != engine:
        return False
    parts = extract_candidate_parts(candidate_id)
    if not parts:
        return False
    for feature, state in parts:
        if str(trade.features.get(feature)) != str(state):
            return False
    return True


def trade_identity(trade: "Trade") -> str:
    return f"{trade.symbol}|{trade.order_id}"


def materialize_filter_selection(
    trades: list["Trade"],
    rule_ids: list[str],
    strategy: str,
) -> tuple[list["Trade"], list["Trade"]]:
    matched_keys = set()
    for cid in rule_ids:
        for trade in trades:
            if candidate_matches_trade(trade, cid):
                matched_keys.add(trade_identity(trade))
    if strategy == "allowlist":
        kept = [trade for trade in trades if trade_identity(trade) in matched_keys]
        blocked = [trade for trade in trades if trade_identity(trade) not in matched_keys]
    else:
        kept = [trade for trade in trades if trade_identity(trade) not in matched_keys]
        blocked = [trade for trade in trades if trade_identity(trade) in matched_keys]
    return kept, blocked


def is_live_safe_candidate_id(candidate_id: str) -> bool:
    for feature, state in extract_candidate_parts(candidate_id):
        if feature in POST_TRADE_FEATURES:
            return False
        if feature in NON_NORMALIZED_STRUCTURED_FEATURES:
            return False
        if feature in FEATURE_HEALTH_EXCLUDED_REASONS:
            return False
        if "{" in str(state) or "}" in str(state):
            return False
    return True


def candidate_live_safety_reason(candidate_id: str) -> str | None:
    for feature, state in extract_candidate_parts(candidate_id):
        if feature in POST_TRADE_FEATURES:
            return f"post_trade_feature:{feature}"
        if feature in NON_NORMALIZED_STRUCTURED_FEATURES:
            return f"structured_state:{feature}"
        feature_health_reason = FEATURE_HEALTH_EXCLUDED_REASONS.get(feature)
        if feature_health_reason:
            return f"feature_health:{feature}:{feature_health_reason}"
        if "{" in str(state) or "}" in str(state):
            return f"raw_structured_state:{feature}"
    return None


def candidate_positive_safety_reason(candidate_id: str) -> str | None:
    for feature, _state in extract_candidate_parts(candidate_id):
        if feature in POSITIVE_SIGNAL_QUARANTINE_FEATURES:
            return f"positive_signal_quarantined:{feature}"
    return None


def _feature_state_token(value: Any) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def build_feature_health_summary(trades: list["Trade"]) -> dict[str, Any]:
    global FEATURE_HEALTH_EXCLUDED_REASONS
    feature_values: dict[str, list[Any]] = defaultdict(list)
    total = len(trades)
    for trade in trades:
        for feature, value in (trade.features or {}).items():
            if is_filled(value):
                feature_values[feature].append(value)

    reasons: dict[str, str] = {
        feature: "static_research_unsafe"
        for feature in STATIC_RESEARCH_UNSAFE_FEATURES
    }
    source_tokens = {
        str(
            (trade.features or {}).get("order_flow_live_source")
            or (trade.row or {}).get("order_flow_live_source")
            or ""
        ).strip().lower()
        for trade in trades
    }
    source_tokens.discard("")
    websocket_orderflow_unavailable = bool(source_tokens) and source_tokens <= {
        "candle_proxy_only_ws_disabled",
        "ws_disabled",
        "disabled",
        "none",
        "unknown",
    }
    if websocket_orderflow_unavailable:
        for feature in feature_values:
            if feature not in {"of_live_available_flag"} and any(
                feature.startswith(prefix) for prefix in WEBSOCKET_ORDERFLOW_FEATURE_PREFIXES
            ):
                reasons.setdefault(feature, "websocket_orderflow_unavailable")

    rows = []
    for feature in sorted(feature_values):
        values = feature_values.get(feature, [])
        present_ratio = (len(values) / total) if total else 0.0
        counter = Counter(_feature_state_token(value) for value in values)
        unique_count = len(counter)
        dominant_state, dominant_count = counter.most_common(1)[0] if counter else ("", 0)
        dominant_pct = (dominant_count / len(values) * 100.0) if values else 0.0
        if unique_count <= 1:
            reasons.setdefault(feature, "constant_field")
        elif present_ratio < FEATURE_HEALTH_MIN_PRESENT_RATIO:
            reasons.setdefault(feature, "low_presence")
        elif dominant_pct >= FEATURE_HEALTH_MAX_DOMINANT_STATE_PCT:
            reasons.setdefault(feature, "dominant_placeholder_or_low_variance")
        rows.append({
            "feature_name": feature,
            "present_n": len(values),
            "present_ratio": round_or_none(present_ratio),
            "unique_count": unique_count,
            "dominant_state": dominant_state,
            "dominant_pct": round_or_none(dominant_pct),
            "excluded_from_live_rules": feature in reasons,
            "exclusion_reason": reasons.get(feature),
        })

    FEATURE_HEALTH_EXCLUDED_REASONS = {
        feature: reason
        for feature, reason in reasons.items()
        if feature in feature_values or feature in STATIC_RESEARCH_UNSAFE_FEATURES
    }
    excluded_counter = Counter(FEATURE_HEALTH_EXCLUDED_REASONS.values())
    features = {
        "trade_count": int(total),
        "total_features": len(feature_values),
        "excluded_features": len(FEATURE_HEALTH_EXCLUDED_REASONS),
        "excluded_by_reason": dict(sorted(excluded_counter.items())),
        "websocket_orderflow_unavailable": websocket_orderflow_unavailable,
        "order_flow_live_sources": sorted(source_tokens),
        "feature_rows": rows,
    }
    return features


@dataclass
class Trade:
    symbol: str
    order_id: str
    engine: str
    direction: str
    setup: str
    session: str | None
    regime: str | None
    structure_alignment: str | None
    placement_dt: datetime
    resolved_dt: datetime | None
    resolved_r: float
    win: int
    row: dict[str, Any]
    features: dict[str, Any]


def placement_window_label(index: int, total: int, window_count: int) -> str:
    if total <= 0:
        return "w1_recent"
    bucket = min(window_count - 1, int(index * window_count / total))
    if window_count == 1:
        suffix = "recent"
    elif bucket == 0:
        suffix = "oldest"
    elif bucket == window_count - 1:
        suffix = "recent"
    else:
        suffix = f"mid{bucket}"
    return f"w{bucket + 1}_{suffix}"


def assign_placement_windows(trades: list[Trade], window_count: int = STABILITY_WINDOWS) -> None:
    if not trades:
        return
    ordered = sorted(trades, key=lambda t: (t.placement_dt, t.symbol, t.order_id))
    total = len(ordered)
    window_count = max(1, min(window_count, total))
    for idx, trade in enumerate(ordered):
        trade.features["placement_window"] = placement_window_label(idx, total, window_count)


def resolve_research_window(all_trades: list[Trade]) -> tuple[list[Trade], datetime | None, datetime | None]:
    if not all_trades:
        return [], None, None

    ordered = sorted(all_trades, key=lambda t: (t.placement_dt, t.symbol, t.order_id))
    start_dt = ordered[0].placement_dt
    latest_dt = ordered[-1].placement_dt
    assign_placement_windows(ordered)
    return ordered, start_dt, latest_dt


def derive_features(row: dict[str, Any], placement_dt: datetime, resolved_dt: datetime | None) -> dict[str, Any]:
    direction = str(row.get("direction") or "").upper()
    buy_pressure = to_float(row.get("order_flow_buy_pressure"))
    sell_pressure = to_float(row.get("order_flow_sell_pressure"))
    directional_pressure = None
    pressure_alignment = None
    if buy_pressure is not None and sell_pressure is not None:
        total_pressure = buy_pressure + sell_pressure
        if total_pressure > 0:
            directional_pressure = max(buy_pressure, sell_pressure) / total_pressure
            if direction == "BUY":
                pressure_alignment = buy_pressure >= sell_pressure
            elif direction == "SELL":
                pressure_alignment = sell_pressure >= buy_pressure

    of_state = row.get("order_flow_confirmation_state") if isinstance(row.get("order_flow_confirmation_state"), dict) else {}
    live_of_snapshot = row.get("order_flow_live_snapshot") if isinstance(row.get("order_flow_live_snapshot"), dict) else {}
    candle_of_snapshot = row.get("order_flow_candle_snapshot") if isinstance(row.get("order_flow_candle_snapshot"), dict) else {}
    footprint = row.get("order_flow_footprint") if isinstance(row.get("order_flow_footprint"), dict) else {}
    if not footprint and isinstance(live_of_snapshot.get("footprint"), dict):
        footprint = live_of_snapshot.get("footprint") or {}
    absorption = row.get("order_flow_absorption") if isinstance(row.get("order_flow_absorption"), dict) else {}
    if not absorption and isinstance(live_of_snapshot.get("absorption"), dict):
        absorption = live_of_snapshot.get("absorption") or {}
    liquidity = row.get("order_flow_liquidity") if isinstance(row.get("order_flow_liquidity"), dict) else {}
    if not liquidity and isinstance(live_of_snapshot.get("liquidity"), dict):
        liquidity = live_of_snapshot.get("liquidity") or {}
    queue = row.get("order_flow_queue") if isinstance(row.get("order_flow_queue"), dict) else {}
    if not queue and isinstance(live_of_snapshot.get("queue"), dict):
        queue = live_of_snapshot.get("queue") or {}
    ws_diag = row.get("order_flow_ws_diagnostics") if isinstance(row.get("order_flow_ws_diagnostics"), dict) else {}
    if not ws_diag and isinstance(live_of_snapshot.get("ws_diagnostics"), dict):
        ws_diag = live_of_snapshot.get("ws_diagnostics") or {}
    gate_obs = row.get("gate_value_observations") if isinstance(row.get("gate_value_observations"), dict) else {}
    absorb_proxy = row.get("candle_absorption_proxy") if isinstance(row.get("candle_absorption_proxy"), dict) else {}
    if not absorb_proxy and isinstance(candle_of_snapshot.get("absorption_proxy"), dict):
        absorb_proxy = candle_of_snapshot.get("absorption_proxy") or {}
    div_proxy = row.get("candle_divergence_proxy") if isinstance(row.get("candle_divergence_proxy"), dict) else {}
    if not div_proxy and isinstance(candle_of_snapshot.get("divergence_proxy"), dict):
        div_proxy = candle_of_snapshot.get("divergence_proxy") or {}
    breakout_proxy = row.get("breakout_candle_proxy") if isinstance(row.get("breakout_candle_proxy"), dict) else {}
    if not breakout_proxy and isinstance(candle_of_snapshot.get("breakout_proxy"), dict):
        breakout_proxy = candle_of_snapshot.get("breakout_proxy") or {}
    opposed_levels = footprint.get("top_opposed_levels") if isinstance(footprint.get("top_opposed_levels"), list) else []
    absorbed_levels = absorption.get("absorbed_levels") if isinstance(absorption.get("absorbed_levels"), list) else []
    liquidity_walls = liquidity.get("liquidity_walls") if isinstance(liquidity.get("liquidity_walls"), list) else []
    liquidity_voids = liquidity.get("liquidity_voids") if isinstance(liquidity.get("liquidity_voids"), list) else []
    migration_events = liquidity.get("migration_events") if isinstance(liquidity.get("migration_events"), list) else []
    entry_ref_price = next(
        (
            value for value in [
                to_float(row.get("actual_fill_price")),
                to_float(row.get("entry_price")),
                to_float(row.get("planned_signal_entry_price")),
                to_float(row.get("requested_entry_price")),
            ]
            if value is not None and value > 0
        ),
        None,
    )

    def gate_dict(key: str) -> dict[str, Any]:
        value = gate_obs.get(key)
        return value if isinstance(value, dict) else {}

    def gate_flag(key: str) -> str | None:
        return "true" if key in gate_obs else None

    resolution_hours = None
    if resolved_dt is not None:
        resolution_hours = (resolved_dt - placement_dt).total_seconds() / 3600.0

    queue_bid_z = to_float(queue.get("bid_z_score"))
    queue_ask_z = to_float(queue.get("ask_z_score"))
    queue_skew = (queue_bid_z - queue_ask_z) if queue_bid_z is not None and queue_ask_z is not None else None
    queue_bid_n = to_float(queue.get("bid_n"))
    queue_ask_n = to_float(queue.get("ask_n"))
    queue_min_samples = min(queue_bid_n, queue_ask_n) if queue_bid_n is not None and queue_ask_n is not None else None
    queue_alignment = None
    if queue_bid_z is not None and queue_ask_z is not None:
        if direction == "BUY":
            queue_alignment = "aligned" if queue_bid_z >= queue_ask_z else "opposed"
        elif direction == "SELL":
            queue_alignment = "aligned" if queue_ask_z >= queue_bid_z else "opposed"

    queue_drain_alignment = None
    if direction == "BUY":
        if queue.get("queue_drain_ask") is True:
            queue_drain_alignment = "aligned"
        elif queue.get("queue_drain_bid") is True:
            queue_drain_alignment = "opposed"
    elif direction == "SELL":
        if queue.get("queue_drain_bid") is True:
            queue_drain_alignment = "aligned"
        elif queue.get("queue_drain_ask") is True:
            queue_drain_alignment = "opposed"

    queue_support_alignment = None
    if direction == "BUY":
        if queue.get("queue_stable_bid") is True:
            queue_support_alignment = "aligned"
        elif queue.get("queue_stable_ask") is True:
            queue_support_alignment = "opposed"
    elif direction == "SELL":
        if queue.get("queue_stable_ask") is True:
            queue_support_alignment = "aligned"
        elif queue.get("queue_stable_bid") is True:
            queue_support_alignment = "opposed"

    bid_touch_rel = None
    ask_touch_rel = None
    spread_bps = None
    bid_session_mean = to_float(queue.get("bid_session_mean"))
    ask_session_mean = to_float(queue.get("ask_session_mean"))
    bid_touch_size = to_float(queue.get("bid_touch_tw_size"))
    ask_touch_size = to_float(queue.get("ask_touch_tw_size"))
    best_bid = to_float(queue.get("best_bid_price"))
    best_ask = to_float(queue.get("best_ask_price"))
    if bid_session_mean and bid_session_mean > 0 and bid_touch_size is not None:
        bid_touch_rel = bid_touch_size / bid_session_mean
    if ask_session_mean and ask_session_mean > 0 and ask_touch_size is not None:
        ask_touch_rel = ask_touch_size / ask_session_mean
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask >= best_bid:
        mid_price = (best_bid + best_ask) / 2.0
        if mid_price > 0:
            spread_bps = ((best_ask - best_bid) / mid_price) * 10000.0

    queue_touch_alignment = None
    if bid_touch_rel is not None and ask_touch_rel is not None:
        if direction == "BUY":
            queue_touch_alignment = "aligned" if bid_touch_rel >= ask_touch_rel else "opposed"
        elif direction == "SELL":
            queue_touch_alignment = "aligned" if ask_touch_rel >= bid_touch_rel else "opposed"

    wall_bid_count = sum(1 for item in liquidity_walls if item.get("side") == "bid")
    wall_ask_count = sum(1 for item in liquidity_walls if item.get("side") == "ask")
    void_bid_count = sum(1 for item in liquidity_voids if item.get("side") == "bid")
    void_ask_count = sum(1 for item in liquidity_voids if item.get("side") == "ask")
    wall_side_bias = side_bias_bucket(wall_bid_count, wall_ask_count)
    void_side_bias = side_bias_bucket(void_bid_count, void_ask_count)

    strong_absorption_count = sum(
        1 for item in absorbed_levels if str(item.get("strength") or "").lower() == "strong"
    )
    key_absorption_count = sum(1 for item in absorbed_levels if item.get("is_key_level") is True)
    top_absorbed_notional = max((to_float(item.get("notional")) or 0.0) for item in absorbed_levels) if absorbed_levels else None
    top_opposed_notional = max((to_float(item.get("notional")) or 0.0) for item in opposed_levels) if opposed_levels else None
    opposed_notional_total = sum((to_float(item.get("notional")) or 0.0) for item in opposed_levels)
    opposed_concentration = (
        top_opposed_notional / opposed_notional_total
        if top_opposed_notional is not None and opposed_notional_total > 0
        else None
    )
    wall_mults = [to_float(item.get("mult")) for item in liquidity_walls if to_float(item.get("mult")) is not None]
    wall_max_mult = max(wall_mults) if wall_mults else None
    wall_avg_mult = (sum(wall_mults) / len(wall_mults)) if wall_mults else None
    void_spans_bps = []
    for item in liquidity_voids:
        low = to_float(item.get("price_low"))
        high = to_float(item.get("price_high"))
        denom = entry_ref_price if entry_ref_price and entry_ref_price > 0 else None
        if low is None or high is None or denom is None or denom <= 0:
            continue
        void_spans_bps.append(abs(high - low) / denom * 10000.0)
    void_max_span_bps = max(void_spans_bps) if void_spans_bps else None

    migration_bid_notional = 0.0
    migration_ask_notional = 0.0
    migration_delta_bps_values = []
    for item in migration_events:
        notional = to_float(item.get("notional")) or 0.0
        side = str(item.get("direction") or "")
        if side == "toward_bid":
            migration_bid_notional += notional
        elif side == "toward_ask":
            migration_ask_notional += notional
        delta_price = to_float(item.get("delta_price"))
        if delta_price is not None and entry_ref_price is not None and entry_ref_price > 0:
            migration_delta_bps_values.append(abs(delta_price) / entry_ref_price * 10000.0)
    migration_total_notional = migration_bid_notional + migration_ask_notional
    migration_delta_bps = max(migration_delta_bps_values) if migration_delta_bps_values else None
    migration_direction = None
    if migration_total_notional > 0:
        bid_share = migration_bid_notional / migration_total_notional
        if bid_share >= 0.60:
            migration_direction = "toward_bid"
        elif bid_share <= 0.40:
            migration_direction = "toward_ask"
        else:
            migration_direction = "balanced"

    migration_alignment = None
    if migration_direction == "balanced":
        migration_alignment = "balanced"
    elif direction == "BUY":
        migration_alignment = "aligned" if migration_direction == "toward_ask" else "opposed" if migration_direction else None
    elif direction == "SELL":
        migration_alignment = "aligned" if migration_direction == "toward_bid" else "opposed" if migration_direction else None

    buy_pressure_slope = to_float(of_state.get("buy_pressure_slope"))
    sell_pressure_slope = to_float(of_state.get("sell_pressure_slope"))
    buy_pressure_abs = to_float(of_state.get("buy_pressure")) or buy_pressure
    sell_pressure_abs = to_float(of_state.get("sell_pressure")) or sell_pressure
    opposing_pressure_abs = to_float(of_state.get("opposing_pressure"))
    opposing_pressure_slope = None
    if direction == "BUY":
        opposing_pressure_slope = sell_pressure_slope
    elif direction == "SELL":
        opposing_pressure_slope = buy_pressure_slope

    bid_notional = to_float(row.get("order_flow_bid_notional") or of_state.get("bid_notional"))
    ask_notional = to_float(row.get("order_flow_ask_notional") or of_state.get("ask_notional"))
    total_notional = None
    if bid_notional is not None or ask_notional is not None:
        total_notional = (bid_notional or 0.0) + (ask_notional or 0.0)
    notional_side_bias = (
        side_bias_bucket(bid_notional or 0.0, ask_notional or 0.0)
        if total_notional is not None and total_notional > 0
        else None
    )

    footprint_total_vol = to_float(footprint.get("total_vol"))
    footprint_net_delta = to_float(footprint.get("net_delta"))
    footprint_divergence_strength = to_float(footprint.get("divergence_strength"))
    footprint_price_move = to_float(footprint.get("price_move"))
    delta_efficiency = (
        abs(footprint_net_delta) / footprint_total_vol
        if footprint_net_delta is not None and footprint_total_vol is not None and footprint_total_vol > 0
        else None
    )
    price_move_bps = (
        abs(footprint_price_move) / entry_ref_price * 10000.0
        if footprint_price_move is not None and entry_ref_price is not None and entry_ref_price > 0
        else None
    )
    vpt_baseline = to_float(absorption.get("vpt_baseline"))
    total_vol_vs_vpt = (
        footprint_total_vol / vpt_baseline
        if footprint_total_vol is not None and vpt_baseline is not None and vpt_baseline > 0
        else None
    )

    ws_issue_count = 0
    for key in ("checksum_failures", "sequence_gaps", "stale_updates", "resync_requests", "reconnect_attempts"):
        ws_issue_count += int(to_float(ws_diag.get(key)) or 0)
    if ws_issue_count == 0:
        ws_health = "clean"
    elif ws_issue_count <= 2:
        ws_health = "minor_issues"
    else:
        ws_health = "degraded"

    gate_of_score = gate_dict("of_final_score_below_threshold")
    gate_static = gate_dict("signal_below_static_quality_threshold")
    gate_ensemble = gate_dict("static_ensemble_failed")
    gate_adx_fail = gate_dict("adx_confirmation_failed")
    gate_divergence = gate_dict("opposing_momentum_divergence")
    gate_no_htf = gate_dict("no_htf_alignment")
    gate_volume = gate_dict("volume_structure_failed")
    gate_orderflow_unavailable = gate_dict("orderflow_unavailable_cycle1")
    gate_recent = gate_dict("recent_candle_confirmation_failed")
    gate_late_breakout = gate_dict("late_breakout_entry")
    gate_path_specific_of = gate_dict("path_specific_orderflow_failed")
    gate_sweep_breakout = gate_dict("sweep_breakout_conflict")
    gate_dol_4h = gate_dict("dominant_4h_competing_liquidity")
    gate_post_of = gate_dict("cycle1_post_of_proximity_failed")

    of_score_gap = None
    of_final = to_float(gate_of_score.get("final_score"))
    of_required = to_float(gate_of_score.get("required_score"))
    if of_final is not None and of_required is not None:
        of_score_gap = max(0.0, of_required - of_final)

    static_score_gap = None
    static_score = to_float(gate_static.get("static_score"))
    static_threshold = to_float(gate_static.get("static_threshold"))
    if static_score is not None and static_threshold is not None:
        static_score_gap = max(0.0, static_threshold - static_score)

    ensemble_vote_gap = None
    ensemble_votes_fail = to_float(gate_ensemble.get("ensemble_votes"))
    ensemble_min_fail = to_float(gate_ensemble.get("ensemble_min"))
    if ensemble_votes_fail is not None and ensemble_min_fail is not None:
        ensemble_vote_gap = max(0.0, ensemble_min_fail - ensemble_votes_fail)

    htf_confluence_value = to_float(row.get("htf_confluence"))
    htf_confluence_score = to_float(row.get("htf_confluence_score"))
    sweep_anchor_score = to_float(row.get("sweep_anchor_divergence_score"))

    return {
        "session": norm_cat(row.get("audit_session_bucket") or row.get("session_bucket")),
        "market_regime": norm_cat(row.get("market_regime")),
        "structure_alignment": norm_cat(row.get("structure_alignment")),
        "selected_level_type": norm_cat(row.get("selected_level_type")),
        "audit_liquidity_bucket": norm_cat(row.get("audit_liquidity_bucket")),
        "liquidity_bucket": norm_cat(row.get("liquidity_bucket")),
        "htf_confluence_bucket": bucket(htf_confluence_value, [
            (None, 0.5, "0"),
            (0.5, 1.5, "1"),
            (1.5, 2.5, "2"),
            (2.5, None, "3plus"),
        ]),
        "htf_confluence_score_bucket": bucket(htf_confluence_score, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, 2.0, "1p5_2"),
            (2.0, None, "2plus"),
        ]),
        "has_sweep_anchor_divergence_flag": as_bool_flag(
            row.get("has_sweep_anchor_divergence") or row.get("sweep_anchor_divergence_confirmed")
        ),
        "strong_sweep_anchor_divergence_flag": as_bool_flag(row.get("strong_sweep_anchor_divergence")),
        "sweep_anchor_divergence_mode": norm_cat(row.get("sweep_anchor_divergence_mode")),
        "sweep_anchor_divergence_score_bucket": bucket(sweep_anchor_score, [
            (None, 25.0, "lt25"),
            (25.0, 50.0, "25_50"),
            (50.0, 68.0, "50_68"),
            (68.0, None, "68plus"),
        ]),
        "gate_market_regime_ranging_flag": gate_flag("market_regime_ranging"),
        "gate_of_final_score_gap_bucket": bucket(of_score_gap, [
            (None, 50.0, "0_50"),
            (50.0, 200.0, "50_200"),
            (200.0, None, "200plus"),
        ]),
        "gate_static_score_gap_bucket": bucket(static_score_gap, [
            (None, 50.0, "0_50"),
            (50.0, 200.0, "50_200"),
            (200.0, None, "200plus"),
        ]),
        "gate_static_vote_gap_bucket": bucket(ensemble_vote_gap, [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "gate_adx_failure_flag": gate_flag("adx_confirmation_failed"),
        "gate_adx_timeframe": norm_cat(gate_adx_fail.get("timeframe")),
        "gate_adx_source": norm_cat(gate_adx_fail.get("source")),
        "gate_adx_fail_slope_bucket": bucket(to_float(gate_adx_fail.get("adx_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "gate_divergence_flag": gate_flag("opposing_momentum_divergence"),
        "gate_divergence_type": norm_cat(gate_divergence.get("divergence_type")),
        "gate_divergence_strength_bucket": bucket(to_float(gate_divergence.get("divergence_strength")), [
            (None, 50.0, "lt50"),
            (50.0, 68.0, "50_68"),
            (68.0, None, "68plus"),
        ]),
        "gate_no_htf_alignment_flag": gate_flag("no_htf_alignment"),
        "gate_no_htf_trigger_type": norm_cat(gate_no_htf.get("trigger_type")),
        "gate_htf_score_bucket": bucket(to_float(gate_no_htf.get("htf_score")), [
            (None, 0.5, "0"),
            (0.5, 1.5, "1"),
            (1.5, 2.5, "2"),
            (2.5, None, "3plus"),
        ]),
        "gate_volume_structure_fail_flag": gate_flag("volume_structure_failed"),
        "gate_volume_structure_score_bucket": bucket(to_float(gate_volume.get("volume_structure_score")), [
            (None, 25.0, "lt25"),
            (25.0, 50.0, "25_50"),
            (50.0, 68.0, "50_68"),
            (68.0, None, "68plus"),
        ]),
        "gate_orderflow_unavailable_flag": gate_flag("orderflow_unavailable_cycle1"),
        "gate_recent_candle_fail_flag": gate_flag("recent_candle_confirmation_failed"),
        "gate_recent_candle_score_bucket": bucket(to_float(gate_recent.get("recent_candle_score")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "gate_late_breakout_flag": gate_flag("late_breakout_entry"),
        "gate_late_breakout_atr_bucket": bucket(to_float(gate_late_breakout.get("entry_distance_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "gate_path_specific_of_fail_flag": gate_flag("path_specific_orderflow_failed"),
        "gate_path_specific_directional_pressure_bucket": bucket(to_float(gate_path_specific_of.get("directional_pressure")), [
            (None, 45.0, "lt45"),
            (45.0, 55.0, "45_55"),
            (55.0, 65.0, "55_65"),
            (65.0, None, "65plus"),
        ]),
        "gate_path_specific_imbalance_bucket": bucket(to_float(gate_path_specific_of.get("imbalance")), [
            (None, 0.1, "lt0p1"),
            (0.1, 0.2, "0p1_0p2"),
            (0.2, 0.3, "0p2_0p3"),
            (0.3, None, "0p3plus"),
        ]),
        "gate_path_specific_microstructure_bucket": bucket(to_float(gate_path_specific_of.get("microstructure_score")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 4.0, "3"),
            (4.0, 6.0, "4_5"),
            (6.0, None, "6plus"),
        ]),
        "gate_sweep_breakout_conflict_flag": gate_flag("sweep_breakout_conflict"),
        "gate_dol_dominant_4h_flag": gate_flag("dominant_4h_competing_liquidity"),
        "gate_dol_inducement_ratio_bucket": bucket(to_float(gate_dol_4h.get("inducement_ratio")), [
            (None, 25.0, "lt25"),
            (25.0, 50.0, "25_50"),
            (50.0, 68.0, "50_68"),
            (68.0, None, "68plus"),
        ]),
        "gate_dol_s1_score_bucket": bucket(to_float(gate_dol_4h.get("s1_score")), [
            (None, 25.0, "lt25"),
            (25.0, 50.0, "25_50"),
            (50.0, 68.0, "50_68"),
            (68.0, None, "68plus"),
        ]),
        "gate_dol_source_4h_flag": "true" if "4h" in str(gate_dol_4h.get("s1_sources", "")).lower() else None,
        "gate_post_of_proximity_flag": gate_flag("cycle1_post_of_proximity_failed"),
        "gate_post_of_proximity_atr_bucket": bucket(to_float(gate_post_of.get("entry_distance_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "order_flow_signal": norm_cat(row.get("order_flow_signal")),
        "order_flow_live_source": norm_cat(row.get("order_flow_live_source") or of_state.get("live_orderflow_source")),
        "of_live_available_flag": as_bool_flag(row.get("order_flow_live_available") or live_of_snapshot.get("available")),
        "candle_of_available_flag": as_bool_flag(row.get("order_flow_candle_available") or candle_of_snapshot.get("available")),
        "of_trigger_type": norm_cat(of_state.get("trigger_type")),
        "of_iceberg_direction": norm_cat(row.get("order_flow_iceberg_direction") or of_state.get("iceberg_direction")),
        "of_confirmation_score_bucket": bucket(to_float(of_state.get("of_score")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_confirmation_votes_bucket": bucket(to_float(of_state.get("of_votes")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_microstructure_score_bucket": bucket(to_float(of_state.get("microstructure_score")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 4.0, "3"),
            (4.0, 6.0, "4_5"),
            (6.0, None, "6plus"),
        ]),
        "of_hard_block_flag": as_bool_flag(bool(of_state.get("hard_block_reason"))),
        "order_flow_confirmation_state": norm_cat(row.get("order_flow_confirmation_state")),
        "market_pressure_state": norm_cat(row.get("market_pressure_state")),
        "market_sentiment_classification": norm_cat(row.get("market_sentiment_classification")),
        "market_sentiment_quarantine_flag": as_bool_flag(row.get("market_sentiment_quarantine_flag")),
        "basis_stress_state": norm_cat(row.get("basis_stress_state")),
        "liquidation_environment_state": norm_cat(row.get("liquidation_environment_state")),
        "btc_regime_15m": norm_cat(row.get("btc_regime_15m")),
        "btc_regime_1h": norm_cat(row.get("btc_regime_1h")),
        "btc_regime_4h": norm_cat(row.get("btc_regime_4h")),
        "eth_regime_15m": norm_cat(row.get("eth_regime_15m")),
        "eth_regime_1h": norm_cat(row.get("eth_regime_1h")),
        "eth_regime_4h": norm_cat(row.get("eth_regime_4h")),
        "selected_level_age_bucket": bucket(to_float(row.get("selected_level_age_candles")), [
            (None, 3.0, "lt3"),
            (3.0, 7.0, "3_7"),
            (7.0, 15.0, "7_15"),
            (15.0, None, "15plus"),
        ]),
        "selected_level_distance_bucket": bucket(to_float(row.get("selected_level_distance_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "swing_total_count_15m_bucket": bucket(to_float(row.get("swing_total_count_15m")), [
            (None, 16.0, "le15"),
            (16.0, 19.0, "16_18"),
            (19.0, 21.0, "19_20"),
            (21.0, None, "21plus"),
        ]),
        "swing_total_count_4h_bucket": bucket(to_float(row.get("swing_total_count_4h")), [
            (None, 16.0, "le15"),
            (16.0, 19.0, "16_18"),
            (19.0, 21.0, "19_20"),
            (21.0, None, "21plus"),
        ]),
        "market_pressure_score_bucket": bucket(to_float(row.get("market_pressure_score")), [
            (None, -5.5, "lt_-5p5"),
            (-5.5, -3.5, "-5p5_to_-3p5"),
            (-3.5, 0.0, "-3p5_to_0"),
            (0.0, None, "ge0"),
        ]),
        "major_coin_breadth_bullish_pct_bucket": bucket(to_float(row.get("major_coin_breadth_bullish_pct")), [
            (None, 30.0, "lt30"),
            (30.0, 40.0, "30_40"),
            (40.0, 50.0, "40_50"),
            (50.0, 60.0, "50_60"),
            (60.0, None, "ge60"),
        ]),
        "scanned_symbol_bullish_pct_bucket": bucket(to_float(row.get("scanned_symbol_bullish_pct")), [
            (None, 40.0, "lt40"),
            (40.0, 50.0, "40_50"),
            (50.0, 60.0, "50_60"),
            (60.0, None, "ge60"),
        ]),
        "funding_breadth_positive_pct_bucket": bucket(to_float(row.get("funding_breadth_positive_pct")), [
            (None, 60.0, "le60"),
            (60.0, 65.0, "60_65"),
            (65.0, 75.0, "65_75"),
            (75.0, None, "ge75"),
        ]),
        "funding_breadth_mean_bps_bucket": bucket(to_float(row.get("funding_breadth_mean_bps")), [
            (None, 0.0, "le0"),
            (0.0, 1.0, "0_1"),
            (1.0, 3.0, "1_3"),
            (3.0, None, "ge3"),
        ]),
        "breakout_success_breadth_6h_bucket": bucket(to_float(row.get("breakout_success_breadth_6h_win_rate_pct")), [
            (None, 30.0, "lt30"),
            (30.0, 45.0, "30_45"),
            (45.0, 60.0, "45_60"),
            (60.0, None, "ge60"),
        ]),
        "breakout_success_breadth_24h_bucket": bucket(to_float(row.get("breakout_success_breadth_24h_win_rate_pct")), [
            (None, 30.0, "lt30"),
            (30.0, 45.0, "30_45"),
            (45.0, 60.0, "45_60"),
            (60.0, None, "ge60"),
        ]),
        "audit_adx_1h_slope_bucket": bucket(to_float(row.get("audit_adx_1h_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "audit_adx_1h_current_bucket": bucket(to_float(row.get("audit_adx_1h_current")), [
            (None, 20.0, "lt20"),
            (20.0, 25.0, "20_25"),
            (25.0, 30.0, "25_30"),
            (30.0, 40.0, "30_40"),
            (40.0, None, "40plus"),
        ]),
        "audit_adx_5m_current_bucket": bucket(to_float(row.get("audit_adx_5m_current")), [
            (None, 15.0, "lt15"),
            (15.0, 20.0, "15_20"),
            (20.0, 25.0, "20_25"),
            (25.0, 30.0, "25_30"),
            (30.0, 40.0, "30_40"),
            (40.0, None, "40plus"),
        ]),
        "audit_adx_5m_slope_bucket": bucket(to_float(row.get("audit_adx_5m_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "audit_adx_3m_current_bucket": bucket(to_float(row.get("audit_adx_3m_current")), [
            (None, 15.0, "lt15"),
            (15.0, 20.0, "15_20"),
            (20.0, 25.0, "20_25"),
            (25.0, 30.0, "25_30"),
            (30.0, 40.0, "30_40"),
            (40.0, None, "40plus"),
        ]),
        "audit_adx_3m_slope_bucket": bucket(to_float(row.get("audit_adx_3m_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "audit_adx_15m_current_bucket": bucket(to_float(row.get("audit_adx_15m_current")), [
            (None, 15.0, "lt15"),
            (15.0, 20.0, "15_20"),
            (20.0, 25.0, "20_25"),
            (25.0, 30.0, "25_30"),
            (30.0, 40.0, "30_40"),
            (40.0, None, "40plus"),
        ]),
        "audit_adx_15m_slope_bucket": bucket(to_float(row.get("audit_adx_15m_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "path_adx_current_bucket": bucket(to_float(row.get("path_adx_current")), [
            (None, 20.0, "lt20"),
            (20.0, 25.0, "20_25"),
            (25.0, 30.0, "25_30"),
            (30.0, 40.0, "30_40"),
            (40.0, None, "40plus"),
        ]),
        "path_adx_slope_bucket": bucket(to_float(row.get("path_adx_slope_per_bar")), [
            (None, -0.5, "lt_-0p5"),
            (-0.5, 0.0, "-0p5_to_0"),
            (0.0, 0.5, "0_to_0p5"),
            (0.5, 1.0, "0p5_to_1"),
            (1.0, None, "ge1"),
        ]),
        "ensemble_votes_bucket": bucket(to_float(row.get("ensemble_votes")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 4.0, "3"),
            (4.0, 6.0, "4_5"),
            (6.0, 9.0, "6_8"),
            (9.0, None, "9plus"),
        ]),
        "context_risk_multiplier_bucket": bucket(to_float(row.get("context_risk_multiplier")), [
            (None, 0.75, "le0p75"),
            (0.75, 0.9, "0p75_0p9"),
            (0.9, 1.0, "0p9_1"),
            (1.0, None, "ge1"),
        ]),
        "of_buy_pressure_bucket": bucket(buy_pressure_abs, [
            (None, 45.0, "lt45"),
            (45.0, 55.0, "45_55"),
            (55.0, 65.0, "55_65"),
            (65.0, None, "65plus"),
        ]),
        "of_sell_pressure_bucket": bucket(sell_pressure_abs, [
            (None, 45.0, "lt45"),
            (45.0, 55.0, "45_55"),
            (55.0, 65.0, "55_65"),
            (65.0, None, "65plus"),
        ]),
        "of_opposing_pressure_bucket": bucket(opposing_pressure_abs, [
            (None, 45.0, "lt45"),
            (45.0, 55.0, "45_55"),
            (55.0, 65.0, "55_65"),
            (65.0, None, "65plus"),
        ]),
        "of_aggression_bucket": bucket(to_float(row.get("order_flow_aggression_ratio")), [
            (None, 0.0, "lt0"),
            (0.0, 0.2, "0_0p2"),
            (0.2, 0.5, "0p2_0p5"),
            (0.5, None, "0p5plus"),
        ]),
        "of_cvd_sign": "ge0" if to_float(of_state.get("cvd") or row.get("order_flow_cvd")) is not None and to_float(of_state.get("cvd") or row.get("order_flow_cvd")) >= 0 else "lt0" if to_float(of_state.get("cvd") or row.get("order_flow_cvd")) is not None else None,
        "of_imbalance_bucket": bucket(to_float(row.get("order_flow_imbalance")), [
            (None, 0.1, "lt0p1"),
            (0.1, 0.2, "0p1_0p2"),
            (0.2, 0.3, "0p2_0p3"),
            (0.3, None, "0p3plus"),
        ]),
        "of_cvd_slope_bucket": signed_strength_bucket(
            to_float(row.get("order_flow_cvd_slope_per_trade") or of_state.get("cvd_slope_per_trade")),
            strong_negative=-150.0,
            mild_negative=-25.0,
            mild_positive=25.0,
            strong_positive=150.0,
        ),
        "of_cvd_intensity_bucket": signed_strength_bucket(
            to_float(row.get("order_flow_cvd_intensity_delta_halves") or of_state.get("cvd_intensity_delta_halves")),
            strong_negative=-200.0,
            mild_negative=-50.0,
            mild_positive=50.0,
            strong_positive=200.0,
        ),
        "of_aggression_delta_bucket": bucket(to_float(row.get("order_flow_aggression_ratio_delta_halves")), [
            (None, -0.1, "lt_-0p1"),
            (-0.1, 0.0, "-0p1_to_0"),
            (0.0, 0.1, "0_to_0p1"),
            (0.1, None, "0p1plus"),
        ]),
        "of_directional_pressure_bucket": bucket(directional_pressure, [
            (None, 0.45, "lt45"),
            (0.45, 0.55, "45_55"),
            (0.55, 0.65, "55_65"),
            (0.65, None, "65plus"),
        ]),
        "of_buy_pressure_slope_bucket": signed_strength_bucket(
            buy_pressure_slope,
            strong_negative=-0.02,
            mild_negative=-0.005,
            mild_positive=0.005,
            strong_positive=0.02,
        ),
        "of_sell_pressure_slope_bucket": signed_strength_bucket(
            sell_pressure_slope,
            strong_negative=-0.02,
            mild_negative=-0.005,
            mild_positive=0.005,
            strong_positive=0.02,
        ),
        "of_opposing_pressure_slope_bucket": signed_strength_bucket(
            opposing_pressure_slope,
            strong_negative=-0.02,
            mild_negative=-0.005,
            mild_positive=0.005,
            strong_positive=0.02,
        ),
        "of_imbalance_slope_bucket": signed_strength_bucket(
            to_float(row.get("order_flow_imbalance_slope") or of_state.get("imbalance_slope")),
            strong_negative=-0.0005,
            mild_negative=-0.0001,
            mild_positive=0.0001,
            strong_positive=0.0005,
        ),
        "of_pressure_slope_bucket": signed_strength_bucket(
            to_float(row.get("order_flow_pressure_slope") or of_state.get("directional_pressure_slope")),
            strong_negative=-0.02,
            mild_negative=-0.005,
            mild_positive=0.005,
            strong_positive=0.02,
        ),
        "of_pressure_alignment": "aligned" if pressure_alignment is True else "opposed" if pressure_alignment is False else None,
        "of_net_delta_sign": "ge0" if to_float(footprint.get("net_delta")) is not None and to_float(footprint.get("net_delta")) >= 0 else "lt0" if to_float(footprint.get("net_delta")) is not None else None,
        "of_delta_divergence": as_bool_flag(footprint.get("delta_divergence")),
        "of_divergence_strength_bucket": bucket(footprint_divergence_strength, [
            (None, 0.5, "lt0p5"),
            (0.5, 0.8, "0p5_0p8"),
            (0.8, None, "0p8plus"),
        ]),
        "of_delta_efficiency_bucket": bucket(delta_efficiency, [
            (None, 0.2, "0_0p2"),
            (0.2, 0.5, "0p2_0p5"),
            (0.5, None, "0p5plus"),
        ]),
        "of_price_move_bps_bucket": bucket(price_move_bps, [
            (None, 1.0, "lt1"),
            (1.0, 3.0, "1_3"),
            (3.0, None, "ge3"),
        ]),
        "of_opposed_levels_bucket": list_len_bucket(footprint.get("top_opposed_levels")),
        "of_top_opposed_notional_bucket": bucket(top_opposed_notional, [
            (None, 500.0, "lt500"),
            (500.0, 1500.0, "500_1500"),
            (1500.0, 5000.0, "1500_5000"),
            (5000.0, None, "5000plus"),
        ]),
        "of_opposed_concentration_bucket": bucket(opposed_concentration, [
            (None, 0.5, "lt0p5"),
            (0.5, 0.75, "0p5_0p75"),
            (0.75, None, "0p75plus"),
        ]),
        "of_absorption_detected": as_bool_flag(absorption.get("absorption_detected")),
        "of_absorption_ratio_bucket": bucket(to_float(absorption.get("max_absorption_ratio")), [
            (None, 2.0, "lt2"),
            (2.0, 5.0, "2_5"),
            (5.0, None, "5plus"),
        ]),
        "of_absorbed_levels_bucket": list_len_bucket(absorption.get("absorbed_levels")),
        "of_absorption_strong_levels_bucket": list_len_bucket([1] * strong_absorption_count),
        "of_absorption_key_levels_bucket": list_len_bucket([1] * key_absorption_count),
        "of_absorption_max_notional_bucket": bucket(top_absorbed_notional, [
            (None, 500.0, "lt500"),
            (500.0, 1500.0, "500_1500"),
            (1500.0, 5000.0, "1500_5000"),
            (5000.0, None, "5000plus"),
        ]),
        "of_ob_level_absorbed_flag": as_bool_flag(absorption.get("ob_level_absorbed")),
        "of_total_vol_vs_vpt_bucket": bucket(total_vol_vs_vpt, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "of_footprint_sufficient_data_flag": as_bool_flag(footprint.get("sufficient_data")),
        "of_absorption_sufficient_data_flag": as_bool_flag(absorption.get("sufficient_data")),
        "of_wall_count_bucket": list_len_bucket(liquidity_walls),
        "of_wall_side_bias": wall_side_bias,
        "of_wall_max_mult_bucket": bucket(wall_max_mult, [
            (None, 2.0, "lt2"),
            (2.0, 5.0, "2_5"),
            (5.0, None, "5plus"),
        ]),
        "of_wall_avg_mult_bucket": bucket(wall_avg_mult, [
            (None, 2.0, "lt2"),
            (2.0, 5.0, "2_5"),
            (5.0, None, "5plus"),
        ]),
        "of_void_count_bucket": list_len_bucket(liquidity_voids),
        "of_void_side_bias": void_side_bias,
        "of_void_max_span_bps_bucket": bucket(void_max_span_bps, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "of_liquidity_sufficient_data_flag": as_bool_flag(liquidity.get("sufficient_data")),
        "of_migration_count_bucket": list_len_bucket(migration_events),
        "of_migration_direction_bucket": migration_direction,
        "of_migration_direction_alignment": migration_alignment,
        "of_migration_notional_bucket": bucket(migration_total_notional if migration_total_notional > 0 else None, [
            (None, 5000.0, "lt5k"),
            (5000.0, 20000.0, "5k_20k"),
            (20000.0, 75000.0, "20k_75k"),
            (75000.0, None, "75kplus"),
        ]),
        "of_migration_delta_bps_bucket": bucket(migration_delta_bps, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "of_icebergs_detected_flag": as_bool_flag(row.get("order_flow_icebergs_detected")),
        "of_spoofs_detected_flag": as_bool_flag(row.get("order_flow_spoofs_detected")),
        "of_bid_icebergs_bucket": bucket(to_float(row.get("order_flow_bid_icebergs")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_ask_icebergs_bucket": bucket(to_float(row.get("order_flow_ask_icebergs")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_total_notional_bucket": bucket(total_notional if total_notional and total_notional > 0 else None, [
            (None, 5000.0, "lt5k"),
            (5000.0, 20000.0, "5k_20k"),
            (20000.0, 75000.0, "20k_75k"),
            (75000.0, None, "75kplus"),
        ]),
        "of_notional_side_bias": notional_side_bias,
        "of_queue_bid_z_bucket": bucket(queue_bid_z, [
            (None, -1.0, "lt_-1"),
            (-1.0, 0.0, "-1_to_0"),
            (0.0, 1.0, "0_to_1"),
            (1.0, None, "ge1"),
        ]),
        "of_queue_ask_z_bucket": bucket(queue_ask_z, [
            (None, -1.0, "lt_-1"),
            (-1.0, 0.0, "-1_to_0"),
            (0.0, 1.0, "0_to_1"),
            (1.0, None, "ge1"),
        ]),
        "of_queue_skew_bucket": bucket(queue_skew, [
            (None, -1.0, "lt_-1"),
            (-1.0, 0.0, "-1_to_0"),
            (0.0, 1.0, "0_to_1"),
            (1.0, None, "ge1"),
        ]),
        "of_queue_bid_touch_rel_bucket": bucket(bid_touch_rel, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "of_queue_ask_touch_rel_bucket": bucket(ask_touch_rel, [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "of_queue_min_samples_bucket": bucket(queue_min_samples, [
            (None, 10.0, "lt10"),
            (10.0, 30.0, "10_30"),
            (30.0, 60.0, "30_60"),
            (60.0, None, "60plus"),
        ]),
        "of_queue_sufficient_data_flag": as_bool_flag(queue.get("sufficient_data")),
        "of_queue_alignment": queue_alignment,
        "of_queue_drain_alignment": queue_drain_alignment,
        "of_queue_support_alignment": queue_support_alignment,
        "of_queue_touch_alignment": queue_touch_alignment,
        "of_spread_bps_bucket": bucket(spread_bps, [
            (None, 1.0, "lt1"),
            (1.0, 3.0, "1_3"),
            (3.0, None, "ge3"),
        ]),
        "of_ws_health_bucket": ws_health,
        "of_ws_issue_bucket": bucket(float(ws_issue_count), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_ws_snapshots_bucket": bucket(to_float(ws_diag.get("snapshots")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_ws_updates_bucket": bucket(to_float(ws_diag.get("updates")), [
            (None, 10.0, "lt10"),
            (10.0, 30.0, "10_30"),
            (30.0, 60.0, "30_60"),
            (60.0, None, "60plus"),
        ]),
        "of_ws_reconnect_bucket": bucket(to_float(ws_diag.get("reconnect_attempts")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "of_ws_checksum_failure_flag": as_bool_flag((to_float(ws_diag.get("checksum_failures")) or 0.0) > 0),
        "of_ws_sequence_gap_flag": as_bool_flag((to_float(ws_diag.get("sequence_gaps")) or 0.0) > 0),
        "of_ws_stale_update_flag": as_bool_flag((to_float(ws_diag.get("stale_updates")) or 0.0) > 0),
        "of_ws_resync_request_flag": as_bool_flag((to_float(ws_diag.get("resync_requests")) or 0.0) > 0),
        "of_window_seconds_bucket": bucket(to_float(row.get("order_flow_window_seconds") or of_state.get("live_orderflow_window_seconds")), [
            (None, 10.0, "lt10"),
            (10.0, 30.0, "10_30"),
            (30.0, 60.0, "30_60"),
            (60.0, None, "60plus"),
        ]),
        "of_sweep_age_bucket": bucket(to_float(row.get("order_flow_sweep_age_candles") or of_state.get("sweep_age_candles")), [
            (None, 1.0, "0"),
            (1.0, 3.0, "1_2"),
            (3.0, 5.0, "3_4"),
            (5.0, None, "5plus"),
        ]),
        "breakout_body_ratio_bucket": bucket(to_float(row.get("breakout_candle_proxy_body_ratio") or breakout_proxy.get("body_ratio")), [
            (None, 0.5, "lt0p5"),
            (0.5, 0.75, "0p5_0p75"),
            (0.75, None, "0p75plus"),
        ]),
        "breakout_range_atr_bucket": bucket(to_float(row.get("breakout_candle_proxy_breakout_range_atr") or breakout_proxy.get("breakout_range_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "breakout_strength_bucket": bucket(to_float(row.get("breakout_candle_proxy_strength") or breakout_proxy.get("continuation_strength")), [
            (None, 0.5, "lt0p5"),
            (0.5, 0.85, "0p5_0p85"),
            (0.85, None, "0p85plus"),
        ]),
        "breakout_close_through_bucket": bucket(to_float(row.get("breakout_candle_proxy_close_through_atr") or breakout_proxy.get("close_through_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, None, "1p5plus"),
        ]),
        "breakout_progress_bucket": bucket(to_float(row.get("breakout_candle_proxy_post_breakout_progress_atr") or breakout_proxy.get("post_breakout_progress_atr")), [
            (None, 0.5, "lt0p5"),
            (0.5, 1.0, "0p5_1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, 2.0, "1p5_2"),
            (2.0, None, "2plus"),
        ]),
        "breakout_vol_decay_bucket": bucket(to_float(row.get("breakout_candle_proxy_post_breakout_volume_decay") or breakout_proxy.get("post_breakout_volume_decay")), [
            (None, 0.5, "lt0p5"),
            (0.5, 0.8, "0p5_0p8"),
            (0.8, None, "0p8plus"),
        ]),
        "breakout_vol_ratio_bucket": bucket(to_float(row.get("breakout_candle_proxy_vol_ratio") or breakout_proxy.get("vol_ratio")), [
            (None, 1.0, "lt1"),
            (1.0, 1.5, "1_1p5"),
            (1.5, 2.5, "1p5_2p5"),
            (2.5, None, "2p5plus"),
        ]),
        "breakout_continuation_confirmed": as_bool_flag(
            row.get("breakout_candle_proxy_continuation_confirmed")
            or breakout_proxy.get("continuation_confirmed")
        ),
        "sweep_absorbed_flag": as_bool_flag(absorb_proxy.get("absorbed")),
        "sweep_absorption_strength_bucket": bucket(to_float(absorb_proxy.get("absorption_strength")), [
            (None, 0.5, "lt0p5"),
            (0.5, 0.8, "0p5_0p8"),
            (0.8, None, "0p8plus"),
        ]),
        "sweep_divergence_flag": as_bool_flag(div_proxy.get("delta_divergence")),
        "sweep_divergence_strength_bucket": bucket(to_float(div_proxy.get("divergence_strength")), [
            (None, 0.5, "lt0p5"),
            (0.5, 0.8, "0p5_0p8"),
            (0.8, None, "0p8plus"),
        ]),
        "resolution_time_bucket": bucket(resolution_hours, [
            (None, 0.5, "lt30m"),
            (0.5, 1.0, "30_60m"),
            (1.0, 2.0, "1_2h"),
            (2.0, 4.0, "2_4h"),
            (4.0, 8.0, "4_8h"),
            (8.0, 24.0, "8_24h"),
            (24.0, None, "24hplus"),
        ]),
        "placement_window": None,
    }
    orderflow_source = str(features.get("order_flow_live_source") or "").strip().lower()
    if not orderflow_source.startswith("live_ws"):
        ws_only_prefixes = (
            "of_buy_pressure",
            "of_sell_pressure",
            "of_opposing_pressure",
            "of_directional_pressure",
            "of_pressure_slope",
            "of_imbalance",
            "of_aggression",
            "of_cvd",
            "of_delta_efficiency",
            "of_price_move",
            "of_opposed",
            "of_top_opposed",
            "of_wall",
            "of_void",
            "of_liquidity",
            "of_migration",
            "of_icebergs",
            "of_spoofs",
            "of_bid_icebergs",
            "of_ask_icebergs",
            "of_total_notional",
            "of_notional",
            "of_queue",
            "of_spread",
            "of_ws",
            "of_window",
        )
        for key in list(features):
            if any(str(key).startswith(prefix) for prefix in ws_only_prefixes):
                features.pop(key, None)
    return features


def load_trades() -> tuple[list[Trade], datetime | None, datetime | None]:
    if not RESOLVED_PATH.exists():
        raise FileNotFoundError(
            f"Required quant input is missing: {RESOLVED_PATH}. "
            "Restore live_trade_audit_resolved.jsonl from Google Drive Trash/version history "
            "or provide a recovered copy before running quant refresh."
        )

    seen: set[tuple[str, str]] = set()
    all_trades: list[Trade] = []
    snapshot_path = materialize_local_snapshot(RESOLVED_PATH)
    malformed_rows = 0
    try:
        for line_no, line in enumerate(iter_jsonl_lines_resilient(snapshot_path), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                malformed_rows += 1
                if malformed_rows <= 5:
                    print(
                        f"WARNING: skipping malformed resolved JSONL row {line_no} "
                        f"in {snapshot_path.name}: {exc}"
                    )
                continue
            if not isinstance(row, dict):
                malformed_rows += 1
                if malformed_rows <= 5:
                    print(
                        f"WARNING: skipping non-object resolved JSONL row {line_no} "
                        f"in {snapshot_path.name}: type={type(row).__name__}"
                    )
                continue
            placement_dt = pick_dt(row, "placement_time", "logged_at", "recorded_at", "signal_timestamp")
            if placement_dt is None:
                continue
            if placement_dt < QUANT_TELEMETRY_CUTOFF:
                continue
            resolved_r = to_float(row.get("resolved_r_multiple"))
            if resolved_r is None:
                resolved_r = to_float(row.get("r_multiple"))
            if resolved_r is None:
                continue
            resolved_r = cap_realized_r(resolved_r)
            if resolved_r is None:
                continue
            outcome = str(row.get("outcome") or row.get("resolution_outcome") or "").lower()
            if outcome not in {"winner", "loser"}:
                continue
            order_id = str(row.get("order_id") or row.get("client_oid") or row.get("audit_id") or row.get("placement_time"))
            symbol = str(row.get("symbol") or "")
            dedupe_key = (symbol, order_id)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            setup = str(row.get("primary_setup") or row.get("trigger_type") or "unknown").lower()
            direction = str(row.get("direction") or "UNKNOWN").upper()
            engine = f"{setup}_{direction}"
            resolved_dt = pick_dt(row, "exit_time", "resolved_at")
            all_trades.append(
                Trade(
                    symbol=symbol,
                    order_id=order_id,
                    engine=engine,
                    direction=direction,
                    setup=setup,
                    session=norm_cat(row.get("audit_session_bucket") or row.get("session_bucket")),
                    regime=norm_cat(row.get("market_regime")),
                    structure_alignment=norm_cat(row.get("structure_alignment")),
                    placement_dt=placement_dt,
                    resolved_dt=resolved_dt,
                    resolved_r=resolved_r,
                    win=1 if outcome == "winner" else 0,
                    row=row,
                    features=derive_features(row, placement_dt, resolved_dt),
                )
            )
    finally:
        if snapshot_path != RESOLVED_PATH and snapshot_path.exists():
            try:
                snapshot_path.unlink()
            except Exception:
                pass
    if malformed_rows > 5:
        print(
            f"WARNING: skipped {malformed_rows} malformed resolved JSONL rows in "
            f"{RESOLVED_PATH.name}; first 5 were reported above."
        )
    return resolve_research_window(all_trades)


def trade_stats(trades: list[Trade]) -> dict[str, Any]:
    n = len(trades)
    wins = sum(t.win for t in trades)
    losses = n - wins
    rs = [t.resolved_r for t in trades]
    win_rs = [t.resolved_r for t in trades if t.win]
    loss_rs = [t.resolved_r for t in trades if not t.win]
    res_hours = [((t.resolved_dt - t.placement_dt).total_seconds() / 3600.0) for t in trades if t.resolved_dt is not None]
    return {
        "n": n,
        "winner_count": wins,
        "loser_count": losses,
        "win_rate_pct": round_or_none(pct(wins, n)),
        "mean_r": round_or_none(mean(rs)),
        "median_r": round_or_none(median(rs) if rs else None),
        "winner_mean_r": round_or_none(mean(win_rs)),
        "loser_mean_r": round_or_none(mean(loss_rs)),
        "avg_resolution_hours": round_or_none(mean(res_hours)),
        "realized_r_cap": QUANT_REALIZED_R_CAP if QUANT_REALIZED_R_CAP > 0 else None,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
    ) as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        fh.flush()
        os.fsync(fh.fileno())
        tmp_name = fh.name
    try:
        os.replace(tmp_name, path)
    finally:
        tmp_path = Path(tmp_name)
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def filter_retained_pct(row: dict[str, Any]) -> float:
    return to_float(row.get("trade_flow_retained_pct")) or 0.0


def filter_kept_wr(row: dict[str, Any]) -> float:
    return to_float(row.get("kept_wr")) or 0.0


def filter_kept_mean_r(row: dict[str, Any]) -> float:
    return to_float(row.get("kept_mean_r")) or 0.0


def filter_recent_validation_passes(row: dict[str, Any], *, min_retained_pct: float, min_wr_pct: float) -> bool:
    validation_n = to_float(row.get("validation_n")) or 0.0
    if validation_n < 20:
        return True
    validation_retained = to_float(row.get("validation_retained_pct")) or 0.0
    validation_wr = to_float(row.get("validation_wr")) or 0.0
    validation_mean_r = to_float(row.get("validation_mean_r"))
    return (
        validation_retained >= max(0.0, min_retained_pct * 0.75)
        and validation_wr >= max(0.0, min_wr_pct - 5.0)
        and (validation_mean_r is None or validation_mean_r > -0.10)
    )


def row_is_family(row: dict[str, Any], family: str) -> bool:
    return str(row.get("simulation_family") or "").strip().lower() == family


def select_best_filter_row(
    rows: list[dict[str, Any]],
    *,
    families: set[str],
    strategies: set[str] | None = None,
    min_retained_pct: float,
    min_wr_pct: float,
) -> dict[str, Any]:
    candidates = []
    normalized_families = {str(item).strip().lower() for item in families}
    normalized_strategies = {str(item).strip().lower() for item in strategies or set()}
    for row in rows:
        family = str(row.get("simulation_family") or "").strip().lower()
        strategy = str(row.get("simulation_strategy") or "").strip().lower()
        if family not in normalized_families:
            continue
        if normalized_strategies and strategy not in normalized_strategies:
            continue
        if filter_retained_pct(row) < min_retained_pct:
            continue
        if filter_kept_wr(row) < min_wr_pct:
            continue
        if filter_kept_mean_r(row) <= 0.0:
            continue
        if not filter_recent_validation_passes(row, min_retained_pct=min_retained_pct, min_wr_pct=min_wr_pct):
            continue
        candidates.append(row)
    if not candidates:
        return {}
    return dict(
        max(
            candidates,
            key=lambda row: (
                1.0 if row.get("validation_target_met") else 0.0,
                to_float(row.get("validation_wr")) or 0.0,
                to_float(row.get("validation_mean_r")) or 0.0,
                filter_kept_wr(row),
                filter_kept_mean_r(row),
                filter_retained_pct(row),
                to_float(row.get("kept_n")) or 0.0,
            ),
        )
    )


def decorate_live_filter_row(row: dict[str, Any], *, lane: str, reason: str) -> dict[str, Any]:
    if not row:
        return {}
    decorated = dict(row)
    decorated["live_selection_lane"] = lane
    decorated["live_selection_reason"] = reason
    return decorated


def build_quant_cycle_summary(
    *,
    trades: list[Trade],
    baseline_rows: list[dict[str, Any]],
    promotion_rows: list[dict[str, Any]],
    filter_rows: list[dict[str, Any]],
    window_start: datetime | None,
    window_end: datetime | None,
    feature_health_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    best_practical = select_best_filter_row(
        filter_rows,
        families={"dynamic_practical_allowlist", "dynamic_practical_blocklist"},
        min_retained_pct=PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
        min_wr_pct=PRACTICAL_FALLBACK_TARGET_WR_PCT,
    )
    best_precision = select_best_filter_row(
        filter_rows,
        families={"dynamic_precision"},
        strategies={"allowlist"},
        min_retained_pct=PRECISION_SELECTION_MIN_RETAINED_PCT,
        min_wr_pct=PRECISION_TARGET_WR_PCT,
    )
    best_balanced = select_best_filter_row(
        filter_rows,
        families={"dynamic_balanced"},
        strategies={"blocklist"},
        min_retained_pct=60.0,
        min_wr_pct=PRACTICAL_FALLBACK_TARGET_WR_PCT,
    )

    active = best_precision or best_practical or best_balanced
    if active:
        if row_is_family(active, "dynamic_precision"):
            lane = "precision"
            reason = f"precision_filter_ge_{PRECISION_TARGET_WR_PCT:g}pct_wr_retention_optional"
        elif row_is_family(active, "dynamic_balanced"):
            lane = "balanced"
            reason = f"balanced_filter_ge_{PRACTICAL_FALLBACK_TARGET_WR_PCT:g}pct_wr_ge_60pct_retained"
        else:
            lane = "practical"
            reason = f"practical_balanced_filter_ge_{PRACTICAL_FALLBACK_TARGET_WR_PCT:g}pct_wr_ge_{PRACTICAL_FALLBACK_MIN_RETAINED_PCT:g}pct_retained"
        active = decorate_live_filter_row(active, lane=lane, reason=reason)

    hard_blocks = [
        row for row in promotion_rows
        if row.get("promotion_decision") == "hard_block"
        and row.get("live_safe_candidate") is True
    ]
    hard_blocks.sort(
        key=lambda row: (
            to_float(row.get("support_n")) or 0.0,
            -(to_float(row.get("win_rate_pct")) or 100.0),
            -(to_float(row.get("mean_r")) or 0.0),
        ),
        reverse=True,
    )

    return {
        "generated_at": datetime.now(TZ).isoformat(),
        "engine_generation": RESEARCH_ENGINE_GENERATION,
        "research_scope": "post_cutoff_resolved_history",
        "telemetry_cutoff": QUANT_TELEMETRY_CUTOFF.isoformat(),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "resolved_rows_used": len(trades),
        "support_floor_mode": "percentage" if QUANT_PERCENTAGE_SUPPORT_FLOORS else "absolute_plus_percentage",
        "live_filter_target_wr_pct": PRACTICAL_FALLBACK_TARGET_WR_PCT,
        "live_filter_min_retained_pct": PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
        "feature_health": {
            key: value
            for key, value in (feature_health_summary or {}).items()
            if key != "feature_rows"
        },
        "active_dynamic_filter": active,
        "recommended_live_filter": active,
        "best_practical_filter": decorate_live_filter_row(
            best_practical,
            lane="practical",
            reason=f"best_practical_ge_{PRACTICAL_FALLBACK_TARGET_WR_PCT:g}pct_wr",
        ),
        "best_precision_filter": decorate_live_filter_row(
            best_precision,
            lane="precision",
            reason=f"best_precision_ge_{PRECISION_TARGET_WR_PCT:g}pct_wr",
        ),
        "best_balanced_filter": decorate_live_filter_row(
            best_balanced,
            lane="balanced",
            reason=f"best_balanced_ge_{PRACTICAL_FALLBACK_TARGET_WR_PCT:g}pct_wr",
        ),
        "engines": baseline_rows,
        "promotion_counts": {
            "hard_block": sum(1 for row in promotion_rows if row["promotion_decision"] == "hard_block"),
            "boost": sum(1 for row in promotion_rows if row["promotion_decision"] == "boost"),
            "quality_uplift": sum(1 for row in promotion_rows if row["promotion_decision"] == "quality_uplift"),
        },
        "promotion_watchlist": {
            "structural_hard_blocks": hard_blocks[:25],
            "fast_track_hard_blocks": hard_blocks[:25],
        },
    }


def build_engine_baseline_table(trades: list[Trade]) -> list[dict[str, Any]]:
    groups: dict[str, list[Trade]] = defaultdict(list)
    for trade in trades:
        groups[trade.engine].append(trade)
    rows = []
    for engine, items in sorted(groups.items()):
        row = {"engine": engine}
        row.update(trade_stats(items))
        rows.append(row)
    return rows


def feature_state_table(trades: list[Trade], features: list[str], table_name: str) -> list[dict[str, Any]]:
    engine_baselines = {row["engine"]: row for row in build_engine_baseline_table(trades)}
    rows = []
    group_keys = sorted({t.engine for t in trades})
    for feature in features:
        for engine in group_keys:
            subset = [t for t in trades if t.engine == engine]
            states = defaultdict(list)
            for trade in subset:
                state = trade.features.get(feature)
                if state is not None:
                    states[str(state)].append(trade)
            baseline = engine_baselines[engine]
            for state, items in states.items():
                stats = trade_stats(items)
                rows.append({
                    "table_name": table_name,
                    "engine": engine,
                    "feature_name": feature,
                    "state": state,
                    **stats,
                    "delta_wr_vs_engine": round_or_none((stats["win_rate_pct"] or 0) - (baseline["win_rate_pct"] or 0)),
                    "delta_r_vs_engine": round_or_none((stats["mean_r"] or 0) - (baseline["mean_r"] or 0)),
                })
    return rows


def global_context_state_table(trades: list[Trade], features: list[str]) -> list[dict[str, Any]]:
    baseline = trade_stats(trades)
    rows = []
    for feature in features:
        states = defaultdict(list)
        for trade in trades:
            state = trade.features.get(feature)
            if state is not None:
                states[str(state)].append(trade)
        for state, items in states.items():
            stats = trade_stats(items)
            rows.append({
                "table_name": "global_context_state",
                "engine": "ALL",
                "feature_name": feature,
                "state": state,
                "candidate_type": "global_context",
                **stats,
                "baseline_win_rate_pct": baseline["win_rate_pct"],
                "baseline_mean_r": baseline["mean_r"],
                "delta_wr_vs_engine": round_or_none((stats["win_rate_pct"] or 0) - (baseline["win_rate_pct"] or 0)),
                "delta_r_vs_engine": round_or_none((stats["mean_r"] or 0) - (baseline["mean_r"] or 0)),
            })
    return rows


def build_directional_context_table(trades: list[Trade], features: list[str]) -> list[dict[str, Any]]:
    rows = []
    for feature in features:
        states = sorted({str(t.features.get(feature)) for t in trades if t.features.get(feature) is not None})
        for state in states:
            buy = [t for t in trades if t.direction == "BUY" and str(t.features.get(feature)) == state]
            sell = [t for t in trades if t.direction == "SELL" and str(t.features.get(feature)) == state]
            if not buy and not sell:
                continue
            buy_stats = trade_stats(buy) if buy else {}
            sell_stats = trade_stats(sell) if sell else {}
            rows.append({
                "feature_name": feature,
                "state": state,
                "buy_n": buy_stats.get("n", 0),
                "buy_wr": buy_stats.get("win_rate_pct"),
                "buy_mean_r": buy_stats.get("mean_r"),
                "sell_n": sell_stats.get("n", 0),
                "sell_wr": sell_stats.get("win_rate_pct"),
                "sell_mean_r": sell_stats.get("mean_r"),
                "wr_gap_buy_minus_sell": round_or_none((buy_stats.get("win_rate_pct") or 0) - (sell_stats.get("win_rate_pct") or 0)),
            })
    return rows


def build_interaction_stack_table(trades: list[Trade], selected_features: dict[str, list[str]]) -> list[dict[str, Any]]:
    baselines = {row["engine"]: row for row in build_engine_baseline_table(trades)}
    rows = []
    for engine, features in selected_features.items():
        subset = [t for t in trades if t.engine == engine]
        if not subset:
            continue
        engine_n = len(subset)
        combos_seen = set()
        for size in (2, 3):
            if size == 2 and engine_n < 300:
                min_stack_trades = scaled_support_floor(
                    engine_n,
                    minimum=6,
                    ratio=INTERACTION_STACK_2WAY_SMALL_ENGINE_RATIO,
                )
            else:
                min_stack_trades = scaled_support_floor(
                    engine_n,
                    minimum=8,
                    ratio=INTERACTION_STACK_DEFAULT_RATIO,
                )
            for combo in combinations(features, size):
                buckets: dict[tuple[str, ...], list[Trade]] = defaultdict(list)
                for trade in subset:
                    vals = []
                    valid = True
                    for feature in combo:
                        value = trade.features.get(feature)
                        if value is None:
                            valid = False
                            break
                        vals.append(f"{feature}={value}")
                    if not valid:
                        continue
                    buckets[tuple(vals)].append(trade)
                for parts, items in buckets.items():
                    if len(items) < min_stack_trades:
                        continue
                    stack_id = " | ".join(parts)
                    if stack_id in combos_seen:
                        continue
                    combos_seen.add(stack_id)
                    stats = trade_stats(items)
                    rows.append({
                        "engine": engine,
                        "stack_size": size,
                        "stack_id": stack_id,
                        "feature_1": parts[0],
                        "feature_2": parts[1] if size >= 2 else None,
                        "feature_3": parts[2] if size >= 3 else None,
                        **stats,
                        "delta_wr_vs_engine": round_or_none((stats["win_rate_pct"] or 0) - (baselines[engine]["win_rate_pct"] or 0)),
                        "delta_r_vs_engine": round_or_none((stats["mean_r"] or 0) - (baselines[engine]["mean_r"] or 0)),
                    })
    rows.sort(key=lambda r: (r["engine"], -(r["delta_wr_vs_engine"] or -999), -(r["n"])))
    return rows


def candidate_windows(items: list[Trade]) -> dict[str, dict[str, Any]]:
    by_window: dict[str, list[Trade]] = defaultdict(list)
    for item in items:
        by_window[item.features["placement_window"]].append(item)
    return {window: trade_stats(window_items) for window, window_items in sorted(by_window.items())}


def build_time_stability_table(candidate_map: dict[str, list[Trade]]) -> list[dict[str, Any]]:
    rows = []
    for candidate_id, items in candidate_map.items():
        for window, stats in candidate_windows(items).items():
            rows.append({"candidate_id": candidate_id, "window_label": window, **stats})
    return rows


def build_concentration_table(candidate_map: dict[str, list[Trade]]) -> list[dict[str, Any]]:
    rows = []
    for candidate_id, items in candidate_map.items():
        n = len(items)
        if n == 0:
            continue
        symbol_counter = Counter(t.symbol for t in items)
        day_counter = Counter(t.placement_dt.date().isoformat() for t in items)
        session_counter = Counter(t.session or "unknown" for t in items)
        regime_counter = Counter(t.regime or "unknown" for t in items)
        rows.append({
            "candidate_id": candidate_id,
            "n": n,
            "top_symbol_pct": round_or_none(100.0 * symbol_counter.most_common(1)[0][1] / n),
            "top_day_pct": round_or_none(100.0 * day_counter.most_common(1)[0][1] / n),
            "top_session_pct": round_or_none(100.0 * session_counter.most_common(1)[0][1] / n),
            "top_regime_pct": round_or_none(100.0 * regime_counter.most_common(1)[0][1] / n),
        })
    return rows


def build_candidate_map(
    trades: list[Trade],
    single_state_rows: list[dict[str, Any]],
    interaction_rows: list[dict[str, Any]],
) -> dict[str, list[Trade]]:
    mapping: dict[str, list[Trade]] = {}
    for row in single_state_rows:
        if row.get("candidate_type") == "global_context" or row.get("engine") == "ALL":
            candidate_id = f"global_state::{row['feature_name']}::{row['state']}"
            mapping[candidate_id] = [
                t for t in trades
                if str(t.features.get(row["feature_name"])) == str(row["state"])
            ]
        else:
            candidate_id = f"state::{row['engine']}::{row['feature_name']}::{row['state']}"
            mapping[candidate_id] = [
                t for t in trades
                if t.engine == row["engine"] and str(t.features.get(row["feature_name"])) == str(row["state"])
            ]
    for row in interaction_rows:
        candidate_id = f"stack::{row['engine']}::{row['stack_id']}"
        parts = [p.strip() for p in row["stack_id"].split(" | ")]
        members = []
        for t in trades:
            if t.engine != row["engine"]:
                continue
            ok = True
            for part in parts:
                name, value = part.split("=", 1)
                if str(t.features.get(name)) != value:
                    ok = False
                    break
            if ok:
                members.append(t)
        mapping[candidate_id] = members
    return mapping


def promote_candidates(
    baseline_rows: list[dict[str, Any]],
    single_state_rows: list[dict[str, Any]],
    interaction_rows: list[dict[str, Any]],
    concentration_rows: list[dict[str, Any]],
    stability_rows: list[dict[str, Any]],
    total_trades: int,
) -> list[dict[str, Any]]:
    baseline_map = {row["engine"]: row for row in baseline_rows}
    concentration_map = {row["candidate_id"]: row for row in concentration_rows}
    stability_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stability_rows:
        stability_group[row["candidate_id"]].append(row)
    zero_win_support_floor = scaled_support_floor(
        total_trades,
        minimum=MIN_FILTER_ZERO_WIN_SUPPORT_ABS,
        ratio=MIN_FILTER_ZERO_WIN_SUPPORT_RATIO,
    )
    hard_block_support_floor = scaled_support_floor(
        total_trades,
        minimum=MIN_FILTER_HARDBLOCK_SUPPORT_ABS,
        ratio=MIN_FILTER_HARDBLOCK_SUPPORT_RATIO,
    )
    boost_support_floor = scaled_support_floor(
        total_trades,
        minimum=MIN_FILTER_BOOST_SUPPORT_ABS,
        ratio=MIN_FILTER_BOOST_SUPPORT_RATIO,
    )
    uplift_support_floor = scaled_support_floor(
        total_trades,
        minimum=MIN_FILTER_UPLIFT_SUPPORT_ABS,
        ratio=MIN_FILTER_UPLIFT_SUPPORT_RATIO,
    )
    stability_window_support_floor = scaled_support_floor(
        total_trades,
        minimum=5,
        ratio=STABILITY_WINDOW_SUPPORT_RATIO,
    )

    candidate_items: list[dict[str, Any]] = []
    for row in single_state_rows:
        if row.get("candidate_type") == "global_context" or row.get("engine") == "ALL":
            candidate_items.append({
                "candidate_id": f"global_state::{row['feature_name']}::{row['state']}",
                "candidate_type": "global_context",
                **row,
            })
        else:
            candidate_items.append({
                "candidate_id": f"state::{row['engine']}::{row['feature_name']}::{row['state']}",
                "candidate_type": "single_state",
                **row,
            })
    for row in interaction_rows:
        candidate_items.append({
            "candidate_id": f"stack::{row['engine']}::{row['stack_id']}",
            "candidate_type": "interaction_stack",
            "feature_name": row["stack_id"],
            "state": row["stack_id"],
            **row,
        })

    promotion_rows = []
    for item in candidate_items:
        engine = item["engine"]
        is_global_context = item.get("candidate_type") == "global_context" or engine == "ALL"
        if is_global_context:
            baseline = {
                "engine": "ALL",
                "win_rate_pct": item.get("baseline_win_rate_pct"),
                "mean_r": item.get("baseline_mean_r"),
            }
        else:
            baseline = baseline_map[engine]
        n = item["n"]
        wr = item["win_rate_pct"] or 0.0
        mean_r_value = item["mean_r"] or 0.0
        delta_wr = item.get("delta_wr_vs_engine") or 0.0
        delta_r = item.get("delta_r_vs_engine") or 0.0
        concentration = concentration_map.get(item["candidate_id"], {})
        windows = stability_group.get(item["candidate_id"], [])
        window_count = sum(1 for w in windows if (w.get("n") or 0) >= stability_window_support_floor)
        negative_windows = sum(1 for w in windows if (w.get("mean_r") or 0) < 0)
        positive_windows = sum(1 for w in windows if (w.get("mean_r") or 0) > 0)
        active_windows = max(1, len(windows))
        top_symbol_pct = concentration.get("top_symbol_pct") or 0.0
        top_day_pct = concentration.get("top_day_pct") or 0.0
        live_safety_reason = candidate_live_safety_reason(item["candidate_id"])
        live_safe_candidate = live_safety_reason is None
        positive_safety_reason = candidate_positive_safety_reason(item["candidate_id"])
        candidate_hard_block_support_floor = (
            int(round(hard_block_support_floor * GLOBAL_CONTEXT_SUPPORT_MULTIPLIER))
            if is_global_context else hard_block_support_floor
        )
        candidate_boost_support_floor = (
            int(round(boost_support_floor * GLOBAL_CONTEXT_SUPPORT_MULTIPLIER))
            if is_global_context else boost_support_floor
        )
        candidate_uplift_support_floor = (
            int(round(uplift_support_floor * GLOBAL_CONTEXT_SUPPORT_MULTIPLIER))
            if is_global_context else uplift_support_floor
        )

        decision = "watchlist"
        if not live_safe_candidate:
            decision = "watchlist"
        elif n >= zero_win_support_floor and item.get("winner_count") == 0:
            decision = "hard_block"
        elif (
            n >= candidate_hard_block_support_floor
            and window_count >= 2
            and wr <= (baseline["win_rate_pct"] or 0) - 12.0
            and (mean_r_value <= (baseline["mean_r"] or 0) - 0.20 or mean_r_value <= -0.25)
            and negative_windows / active_windows >= 0.70
        ):
            decision = "hard_block"
        elif (
            not is_global_context
            and n >= candidate_boost_support_floor
            and window_count >= 2
            and wr >= (baseline["win_rate_pct"] or 0) + 12.0
            and (mean_r_value >= (baseline["mean_r"] or 0) + 0.20 or mean_r_value >= 0.20)
            and positive_windows / active_windows >= 0.70
            and top_symbol_pct < 35.0
            and top_day_pct < 40.0
        ):
            decision = "boost"
        elif (
            not is_global_context
            and n >= candidate_uplift_support_floor
            and window_count >= 2
            and (
                ((baseline["win_rate_pct"] or 0) - 12.0 <= wr <= (baseline["win_rate_pct"] or 0) - 6.0)
                or ((baseline["mean_r"] or 0) - 0.25 <= mean_r_value <= (baseline["mean_r"] or 0) - 0.10)
            )
        ):
            decision = "quality_uplift"

        if positive_safety_reason and decision in {"boost", "quality_uplift"}:
            decision = "watchlist"

        descriptor = describe_candidate_id(item["candidate_id"])
        promotion_rows.append({
            "candidate_id": item["candidate_id"],
            "candidate_type": item["candidate_type"],
            "engine": engine,
            "engine_label": descriptor.get("engine_label"),
            "feature_name": item["feature_name"],
            "state": item["state"],
            "candidate_label": descriptor.get("candidate_label"),
            "component_count": descriptor.get("component_count"),
            "component_labels": " || ".join(descriptor.get("components") or []),
            "support_n": n,
            "win_rate_pct": round_or_none(wr),
            "mean_r": round_or_none(mean_r_value),
            "delta_wr_vs_engine": round_or_none(delta_wr),
            "delta_r_vs_engine": round_or_none(delta_r),
            "window_support_floor": stability_window_support_floor,
            "window_count_ge_support_floor": window_count,
            "negative_window_ratio": round_or_none(negative_windows / active_windows if active_windows else 0),
            "positive_window_ratio": round_or_none(positive_windows / active_windows if active_windows else 0),
            "top_symbol_pct": round_or_none(top_symbol_pct),
            "top_day_pct": round_or_none(top_day_pct),
            "live_safe_candidate": live_safe_candidate,
            "live_safety_reason": live_safety_reason,
            "positive_safety_reason": positive_safety_reason,
            "promotion_decision": decision,
        })
    return promotion_rows


def build_filter_simulation_table(
    trades: list[Trade],
    promotion_rows: list[dict[str, Any]],
    candidate_map: dict[str, list[Trade]],
) -> list[dict[str, Any]]:
    target_wr_pct = PRECISION_TARGET_WR_PCT
    total_trade_count = len(trades)
    baseline_stats = trade_stats(trades)
    baseline_mean_r = baseline_stats["mean_r"] or 0.0
    baseline_wr = baseline_stats["win_rate_pct"] or 0.0
    precision_min_kept = scaled_support_floor(
        total_trade_count,
        minimum=PRECISION_MIN_KEPT_ABS,
        ratio=PRECISION_MIN_KEPT_RATIO,
    )
    precision_lcb_floor = max(
        PRECISION_MIN_LCB_ABS,
        baseline_wr + PRECISION_MIN_LCB_EDGE_VS_BASELINE,
    )
    family_step_trades = scaled_support_floor(
        total_trade_count,
        minimum=MIN_DYNAMIC_FAMILY_STEP_TRADES,
        ratio=0.01,
    )
    hard_blocks = [
        r for r in promotion_rows
        if r["promotion_decision"] == "hard_block" and r.get("live_safe_candidate") is True
    ]
    def precision_engine_min_wr(row: dict[str, Any]) -> float:
        return float(
            PRECISION_ENGINE_MIN_WR_PCT.get(
                str(row.get("engine") or ""),
                PRECISION_TARGET_WR_PCT,
            )
        )

    def precision_candidate_ok(row: dict[str, Any]) -> bool:
        return (
            row["promotion_decision"] == "boost"
            and row.get("live_safe_candidate") is True
            and (row.get("win_rate_pct") or 0.0) >= precision_engine_min_wr(row)
            and (row.get("mean_r") or 0.0) > 0.0
        )

    boosts = [r for r in promotion_rows if precision_candidate_ok(r)]

    practical_allowlist_candidates = [
        r for r in promotion_rows
        if r.get("live_safe_candidate") is True
        and not r.get("positive_safety_reason")
        and (r.get("win_rate_pct") or 0.0) >= PRACTICAL_FALLBACK_MIN_CANDIDATE_WR_PCT
        and (r.get("mean_r") or 0.0) > 0.0
        and (r.get("support_n") or 0) >= family_step_trades
    ]

    def hard_block_efficiency(row: dict[str, Any]) -> tuple[float, float, float]:
        support_pct = max(((row.get("support_n") or 0.0) / max(total_trade_count, 1)) * 100.0, 0.01)
        wr_gap = max(0.0, baseline_wr - (row.get("win_rate_pct") or 0.0))
        mean_gap = max(0.0, baseline_mean_r - (row.get("mean_r") or 0.0))
        return (
            wr_gap / support_pct,
            mean_gap / support_pct,
            -support_pct,
        )

    hard_blocks.sort(
        key=lambda r: (
            hard_block_efficiency(r),
            r.get("negative_window_ratio") or 0.0,
            -(r.get("win_rate_pct") or 0.0),
        ),
        reverse=True,
    )
    boosts.sort(
        key=lambda r: (
            r["support_n"],
            r.get("positive_window_ratio") or 0.0,
            r["mean_r"] or 0.0,
            r["win_rate_pct"] or 0.0,
        ),
        reverse=True,
    )
    practical_allowlist_candidates.sort(
        key=lambda r: (
            r["support_n"],
            r["win_rate_pct"] or 0.0,
            r["mean_r"] or 0.0,
        ),
        reverse=True,
    )
    candidate_meta = {row["candidate_id"]: row for row in promotion_rows}
    rows = []

    def simulate(
        rule_ids: list[str],
        simulation_id: str,
        simulation_label: str,
        simulation_family: str,
        simulation_strategy: str,
        *,
        target_wr_override: float | None = None,
        min_retained_override: float | None = None,
        min_kept_override: int | None = None,
        lcb_floor_override: float | None = None,
    ) -> dict[str, Any]:
        matched_keys = set()
        for cid in rule_ids:
            for trade in candidate_map.get(cid, []):
                matched_keys.add((trade.symbol, trade.order_id))
        if simulation_strategy == "allowlist":
            kept = [t for t in trades if (t.symbol, t.order_id) in matched_keys]
            blocked = [t for t in trades if (t.symbol, t.order_id) not in matched_keys]
        else:
            kept = [t for t in trades if (t.symbol, t.order_id) not in matched_keys]
            blocked = [t for t in trades if (t.symbol, t.order_id) in matched_keys]
        kept_stats = trade_stats(kept)
        blocked_stats = trade_stats(blocked)
        local_target_wr_pct = target_wr_override if target_wr_override is not None else target_wr_pct
        local_min_retained_pct = min_retained_override if min_retained_override is not None else PRECISION_MIN_RETAINED_PCT
        local_min_kept = min_kept_override if min_kept_override is not None else precision_min_kept
        local_lcb_floor = lcb_floor_override if lcb_floor_override is not None else precision_lcb_floor
        validation_n = min(len(trades), max(1, int(math.ceil(len(trades) * 0.25)))) if trades else 0
        validation_universe = sorted(
            trades,
            key=lambda trade: (trade.placement_dt, trade.symbol, trade.order_id),
        )[-validation_n:] if validation_n else []
        if simulation_strategy == "allowlist":
            validation_kept = [
                t for t in validation_universe
                if (t.symbol, t.order_id) in matched_keys
            ]
            validation_blocked = [
                t for t in validation_universe
                if (t.symbol, t.order_id) not in matched_keys
            ]
        else:
            validation_kept = [
                t for t in validation_universe
                if (t.symbol, t.order_id) not in matched_keys
            ]
            validation_blocked = [
                t for t in validation_universe
                if (t.symbol, t.order_id) in matched_keys
            ]
        validation_kept_stats = trade_stats(validation_kept)
        validation_blocked_stats = trade_stats(validation_blocked)
        validation_retained_pct = round_or_none(pct(validation_kept_stats["n"], len(validation_universe)))
        validation_target_met = (
            (validation_kept_stats["win_rate_pct"] or 0.0) >= max(0.0, local_target_wr_pct - 5.0)
            and (validation_retained_pct or 0.0) >= max(0.0, local_min_retained_pct * 0.75)
            and (validation_kept_stats["mean_r"] or 0.0) > -0.10
        ) if validation_universe else None
        rule_labels = [
            candidate_meta.get(cid, {}).get("candidate_label")
            or describe_candidate_id(cid).get("candidate_label")
            or cid
            for cid in rule_ids
        ]
        target_met = None
        kept_wins = sum(t.win for t in kept)
        kept_wr_lcb = wilson_lower_bound_pct(kept_wins, kept_stats["n"])
        retained_pct = round_or_none(pct(kept_stats["n"], len(trades)))
        precision_quality_score = None
        if simulation_strategy == "allowlist":
            target_met = (
                (kept_stats["win_rate_pct"] or 0.0) >= local_target_wr_pct
                and (kept_stats["mean_r"] or 0.0) > 0.0
                and kept_stats["n"] >= local_min_kept
                and (retained_pct or 0.0) >= local_min_retained_pct
                and (kept_wr_lcb or 0.0) >= local_lcb_floor
            )
            precision_quality_score = round_or_none(
                ((kept_stats["mean_r"] or 0.0) * 100.0)
                + ((kept_wr_lcb or 0.0) * 0.75)
                + ((kept_stats["win_rate_pct"] or 0.0) * 0.35)
                + min(25.0, (retained_pct or 0.0) * 0.5)
            )
        elif target_wr_override is not None or min_retained_override is not None:
            target_met = (
                (kept_stats["win_rate_pct"] or 0.0) >= local_target_wr_pct
                and (kept_stats["mean_r"] or 0.0) > 0.0
                and (retained_pct or 0.0) >= local_min_retained_pct
            )
        return {
            "simulation_id": simulation_id,
            "simulation_label": simulation_label,
            "simulation_family": simulation_family,
            "simulation_strategy": simulation_strategy,
            "constituent_count": len(rule_ids),
            "rules_applied": " | ".join(rule_ids),
            "rule_ids_json": json.dumps(rule_ids),
            "rules_applied_labels": " || ".join(rule_labels),
            "rule_labels_json": json.dumps(rule_labels),
            "kept_n": kept_stats["n"],
            "blocked_n": blocked_stats["n"],
            "kept_wr": kept_stats["win_rate_pct"],
            "kept_mean_r": kept_stats["mean_r"],
            "blocked_wr": blocked_stats["win_rate_pct"],
            "blocked_mean_r": blocked_stats["mean_r"],
            "net_r_change": round_or_none((kept_stats["mean_r"] or 0) - baseline_mean_r),
            "trade_flow_retained_pct": retained_pct,
            "target_wr_pct": local_target_wr_pct if simulation_strategy == "allowlist" or target_wr_override is not None else None,
            "target_wr_met": target_met,
            "kept_wr_lcb_80": round_or_none(kept_wr_lcb),
            "precision_quality_score": precision_quality_score,
            "precision_min_kept": (min_kept_override if min_kept_override is not None else precision_min_kept) if simulation_strategy == "allowlist" else None,
            "precision_min_retained_pct": local_min_retained_pct if simulation_strategy == "allowlist" or min_retained_override is not None else None,
            "precision_lcb_floor": round_or_none((lcb_floor_override if lcb_floor_override is not None else precision_lcb_floor)) if simulation_strategy == "allowlist" else None,
            "validation_scope": "latest_25pct_resolved_trades",
            "validation_n": len(validation_universe),
            "validation_kept_n": validation_kept_stats["n"],
            "validation_blocked_n": validation_blocked_stats["n"],
            "validation_wr": validation_kept_stats["win_rate_pct"],
            "validation_mean_r": validation_kept_stats["mean_r"],
            "validation_blocked_wr": validation_blocked_stats["win_rate_pct"],
            "validation_blocked_mean_r": validation_blocked_stats["mean_r"],
            "validation_retained_pct": validation_retained_pct,
            "validation_target_met": validation_target_met,
            "resolved_rows_used": total_trade_count,
            "selection_basis": "all_resolved_trades_with_latest_25pct_validation",
        }

    def balanced_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
        return (
            to_float(row.get("kept_mean_r")) or float("-inf"),
            to_float(row.get("kept_wr")) or float("-inf"),
            to_float(row.get("kept_n")) or float("-inf"),
            to_float(row.get("trade_flow_retained_pct")) or float("-inf"),
        )

    def precision_retained_pct(row: dict[str, Any]) -> float:
        return to_float(row.get("trade_flow_retained_pct")) or 0.0

    def precision_retention_met(row: dict[str, Any]) -> bool:
        return precision_retained_pct(row) >= PRECISION_MIN_RETAINED_PCT

    def precision_key(row: dict[str, Any]) -> tuple[float, float, float, float, float, float, float, float]:
        target_met = 1.0 if row.get("target_wr_met") else 0.0
        retention_met = 1.0 if precision_retention_met(row) else 0.0
        return (
            target_met,
            retention_met,
            to_float(row.get("precision_quality_score")) or float("-inf"),
            to_float(row.get("kept_wr_lcb_80")) or float("-inf"),
            to_float(row.get("kept_wr")) or float("-inf"),
            to_float(row.get("kept_n")) or float("-inf"),
            precision_retained_pct(row),
            to_float(row.get("kept_mean_r")) or float("-inf"),
        )

    def precision_growth_key(row: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
        retained = precision_retained_pct(row)
        return (
            1.0 if precision_retention_met(row) else 0.0,
            retained,
            to_float(row.get("kept_wr")) or float("-inf"),
            to_float(row.get("kept_mean_r")) or float("-inf"),
            to_float(row.get("kept_wr_lcb_80")) or float("-inf"),
            to_float(row.get("kept_n")) or float("-inf"),
        )

    def practical_target_met(row: dict[str, Any]) -> bool:
        return (
            (to_float(row.get("trade_flow_retained_pct")) or 0.0) >= PRACTICAL_FALLBACK_MIN_RETAINED_PCT
            and (to_float(row.get("kept_wr")) or 0.0) >= PRACTICAL_FALLBACK_TARGET_WR_PCT
        )

    def practical_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
        return (
            1.0 if practical_target_met(row) else 0.0,
            min(to_float(row.get("trade_flow_retained_pct")) or 0.0, PRACTICAL_FALLBACK_MIN_RETAINED_PCT),
            to_float(row.get("kept_wr")) or float("-inf"),
            to_float(row.get("kept_mean_r")) or float("-inf"),
            to_float(row.get("kept_n")) or float("-inf"),
        )

    def precision_material_improvement(candidate: dict[str, Any], current: dict[str, Any] | None) -> bool:
        if current is None:
            return True
        delta_lcb = (to_float(candidate.get("kept_wr_lcb_80")) or 0.0) - (to_float(current.get("kept_wr_lcb_80")) or 0.0)
        if delta_lcb < -PRECISION_MAX_LCB_DROP_PCT:
            return False
        current_target_met = bool(current.get("target_wr_met"))
        candidate_target_met = bool(candidate.get("target_wr_met"))
        if current_target_met and not candidate_target_met:
            return False
        if not current_target_met and candidate_target_met:
            return True
        if not current_target_met and not candidate_target_met:
            return precision_key(candidate) > precision_key(current)

        delta_mean_r = (to_float(candidate.get("kept_mean_r")) or 0.0) - (to_float(current.get("kept_mean_r")) or 0.0)
        delta_wr = (to_float(candidate.get("kept_wr")) or 0.0) - (to_float(current.get("kept_wr")) or 0.0)
        delta_retained = (to_float(candidate.get("trade_flow_retained_pct")) or 0.0) - (to_float(current.get("trade_flow_retained_pct")) or 0.0)
        delta_kept_n = (to_float(candidate.get("kept_n")) or 0.0) - (to_float(current.get("kept_n")) or 0.0)
        return (
            delta_lcb >= PRECISION_RULE_ADD_MIN_DELTA_WR
            or
            delta_mean_r >= PRECISION_RULE_ADD_MIN_DELTA_MEAN_R
            or delta_wr >= PRECISION_RULE_ADD_MIN_DELTA_WR
            or (
                delta_kept_n >= family_step_trades
                and delta_retained >= PRECISION_RULE_ADD_MIN_DELTA_RETAINED_PCT
                and delta_mean_r >= -0.01
                and delta_wr >= -0.15
            )
        )

    def balanced_material_improvement(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
        delta_mean_r = (to_float(candidate.get("kept_mean_r")) or 0.0) - (to_float(current.get("kept_mean_r")) or 0.0)
        delta_wr = (to_float(candidate.get("kept_wr")) or 0.0) - (to_float(current.get("kept_wr")) or 0.0)
        delta_retained = (to_float(candidate.get("trade_flow_retained_pct")) or 0.0) - (to_float(current.get("trade_flow_retained_pct")) or 0.0)
        return (
            delta_mean_r >= BALANCED_RULE_ADD_MIN_DELTA_MEAN_R
            or delta_wr >= BALANCED_RULE_ADD_MIN_DELTA_WR
            or (delta_mean_r > 0.0 and delta_retained >= -BALANCED_RULE_ADD_MAX_RETENTION_LOSS_PCT)
        )

    for row in hard_blocks:
        rows.append(
            simulate(
                [row["candidate_id"]],
                f"single::{row['candidate_id']}",
                row.get("candidate_label") or row["candidate_id"],
                "single_rule",
                "blocklist",
            )
        )
    for row in boosts:
        rows.append(
            simulate(
                [row["candidate_id"]],
                f"single_allow::{row['candidate_id']}",
                row.get("candidate_label") or row["candidate_id"],
                "single_allow_rule",
                "allowlist",
            )
        )

    def build_balanced_family(*, min_retained_pct: float | None = None, max_rules: int | None = None) -> list[dict[str, Any]]:
        selected: list[str] = []
        current = {
            "kept_mean_r": baseline_mean_r,
            "kept_wr": baseline_wr,
            "trade_flow_retained_pct": 100.0,
        }
        remaining = [row["candidate_id"] for row in hard_blocks]
        family_rows: list[dict[str, Any]] = []
        while remaining and (max_rules is None or len(selected) < max_rules):
            best_row = None
            best_added = None
            for cid in remaining:
                candidate_row = simulate(
                    selected + [cid],
                    f"dynamic::balanced::{len(selected) + 1}",
                    f"Dynamic balanced filter ({len(selected) + 1} rules)",
                    "dynamic_balanced",
                    "blocklist",
                )
                retained = to_float(candidate_row.get("trade_flow_retained_pct"))
                if min_retained_pct is not None and (retained is None or retained < min_retained_pct):
                    continue
                current_retained = to_float(current.get("trade_flow_retained_pct")) or 100.0
                retention_loss = current_retained - (retained or current_retained)
                max_rule_loss = (
                    BALANCED_PHASE1_MAX_RULE_RETENTION_LOSS_PCT
                    if len(selected) < 2
                    else BALANCED_PHASE2_MAX_RULE_RETENTION_LOSS_PCT
                )
                if retention_loss > max_rule_loss:
                    continue
                candidate_sort_key = (
                    hard_block_efficiency(candidate_meta.get(cid, {})),
                    to_float(candidate_row.get("kept_wr")) or float("-inf"),
                    to_float(candidate_row.get("kept_mean_r")) or float("-inf"),
                    -(retention_loss or 0.0),
                )
                best_sort_key = (
                    hard_block_efficiency(candidate_meta.get(best_added, {})),
                    to_float((best_row or {}).get("kept_wr")) or float("-inf"),
                    to_float((best_row or {}).get("kept_mean_r")) or float("-inf"),
                    -(
                        (to_float(current.get("trade_flow_retained_pct")) or 100.0)
                        - (to_float((best_row or {}).get("trade_flow_retained_pct")) or 100.0)
                    ),
                )
                if best_row is None or candidate_sort_key > best_sort_key:
                    best_row = candidate_row
                    best_added = cid
            if (
                best_row is None
                or balanced_key(best_row) <= balanced_key(current)
                or not balanced_material_improvement(best_row, current)
            ):
                break
            selected.append(best_added)
            remaining.remove(best_added)
            family_rows.append(best_row)
            current = best_row
        return family_rows

    def build_precision_family(*, max_rules: int | None = None) -> list[dict[str, Any]]:
        selected: list[str] = []
        remaining = [row["candidate_id"] for row in boosts]
        family_rows: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None
        while remaining and (max_rules is None or len(selected) < max_rules):
            candidate_rows = []
            for cid in remaining:
                candidate_rows.append(
                    (
                        cid,
                        simulate(
                            selected + [cid],
                            f"dynamic::precision::{len(selected) + 1}",
                            f"Dynamic precision filter ({len(selected) + 1} rules)",
                            "dynamic_precision",
                            "allowlist",
                        ),
                    )
                )

            current_retained = precision_retained_pct(current or {})
            if current is None or current_retained < PRECISION_MIN_RETAINED_PCT:
                growth_rows = [
                    (cid, row)
                    for cid, row in candidate_rows
                    if precision_retained_pct(row) > current_retained + 1e-9
                ]
                if not growth_rows:
                    break
                target_rows = [(cid, row) for cid, row in growth_rows if row.get("target_wr_met")]
                retention_rows = [(cid, row) for cid, row in growth_rows if precision_retention_met(row)]
                pool = target_rows or retention_rows or growth_rows
                best_added, best_row = max(pool, key=lambda item: precision_growth_key(item[1]))
            elif current is not None and current.get("target_wr_met"):
                target_rows = [(cid, row) for cid, row in candidate_rows if row.get("target_wr_met")]
                if not target_rows:
                    break
                best_added, best_row = max(target_rows, key=lambda item: precision_key(item[1]))
            else:
                best_added, best_row = max(candidate_rows, key=lambda item: precision_key(item[1]))

            if (
                current is not None
                and current_retained >= PRECISION_MIN_RETAINED_PCT
                and (
                    precision_key(best_row) <= precision_key(current)
                    or not precision_material_improvement(best_row, current)
                )
            ):
                break

            selected.append(best_added)
            remaining.remove(best_added)
            family_rows.append(best_row)
            current = best_row
        return family_rows

    def build_practical_allowlist_family(*, max_rules: int | None = None) -> list[dict[str, Any]]:
        selected: list[str] = []
        remaining = [row["candidate_id"] for row in practical_allowlist_candidates]
        family_rows: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None
        while remaining and (max_rules is None or len(selected) < max_rules):
            current_retained = to_float((current or {}).get("trade_flow_retained_pct")) or 0.0
            candidate_rows = []
            for cid in remaining:
                candidate_row = simulate(
                    selected + [cid],
                    f"dynamic::practical_allowlist::{len(selected) + 1}",
                    f"Dynamic practical allowlist ({len(selected) + 1} rules)",
                    "dynamic_practical_allowlist",
                    "allowlist",
                    target_wr_override=PRACTICAL_FALLBACK_TARGET_WR_PCT,
                    min_retained_override=PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
                    min_kept_override=scaled_support_floor(
                        total_trade_count,
                        minimum=MIN_FILTER_BOOST_SUPPORT_ABS,
                        ratio=PRACTICAL_FALLBACK_MIN_RETAINED_PCT / 100.0,
                    ),
                    lcb_floor_override=0.0,
                )
                if (to_float(candidate_row.get("kept_wr")) or 0.0) < PRACTICAL_FALLBACK_TARGET_WR_PCT:
                    continue
                if (to_float(candidate_row.get("kept_mean_r")) or 0.0) <= 0.0:
                    continue
                if (to_float(candidate_row.get("trade_flow_retained_pct")) or 0.0) <= current_retained + 1e-9:
                    continue
                candidate_rows.append((cid, candidate_row))
            if not candidate_rows:
                break
            best_added, best_row = max(candidate_rows, key=lambda item: practical_key(item[1]))
            if current is not None and practical_key(best_row) <= practical_key(current):
                break
            selected.append(best_added)
            remaining.remove(best_added)
            family_rows.append(best_row)
            current = best_row
            if practical_target_met(best_row):
                remaining_target_rows = []
                for cid in remaining:
                    candidate_row = simulate(
                        selected + [cid],
                        f"dynamic::practical_allowlist::{len(selected) + 1}",
                        f"Dynamic practical allowlist ({len(selected) + 1} rules)",
                        "dynamic_practical_allowlist",
                        "allowlist",
                        target_wr_override=PRACTICAL_FALLBACK_TARGET_WR_PCT,
                        min_retained_override=PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
                        min_kept_override=scaled_support_floor(
                            total_trade_count,
                            minimum=MIN_FILTER_BOOST_SUPPORT_ABS,
                            ratio=PRACTICAL_FALLBACK_MIN_RETAINED_PCT / 100.0,
                        ),
                        lcb_floor_override=0.0,
                    )
                    if practical_target_met(candidate_row) and practical_key(candidate_row) > practical_key(best_row):
                        remaining_target_rows.append((cid, candidate_row))
                if not remaining_target_rows:
                    break
        return family_rows

    def build_practical_blocklist_family(*, min_retained_pct: float | None = None, max_rules: int | None = None) -> list[dict[str, Any]]:
        selected: list[str] = []
        current = {
            "kept_mean_r": baseline_mean_r,
            "kept_wr": baseline_wr,
            "trade_flow_retained_pct": 100.0,
        }
        remaining = [row["candidate_id"] for row in hard_blocks]
        family_rows: list[dict[str, Any]] = []
        while remaining and (max_rules is None or len(selected) < max_rules):
            best_row = None
            best_added = None
            for cid in remaining:
                candidate_row = simulate(
                    selected + [cid],
                    f"dynamic::practical_blocklist::{len(selected) + 1}",
                    f"Dynamic practical blocklist ({len(selected) + 1} rules)",
                    "dynamic_practical_blocklist",
                    "blocklist",
                    target_wr_override=PRACTICAL_FALLBACK_TARGET_WR_PCT,
                    min_retained_override=PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
                )
                retained = to_float(candidate_row.get("trade_flow_retained_pct"))
                if min_retained_pct is not None and (retained is None or retained < min_retained_pct):
                    continue
                candidate_sort_key = (
                    practical_key(candidate_row),
                    hard_block_efficiency(candidate_meta.get(cid, {})),
                )
                best_sort_key = (
                    practical_key(best_row or {}),
                    hard_block_efficiency(candidate_meta.get(best_added, {})),
                )
                if best_row is None or candidate_sort_key > best_sort_key:
                    best_row = candidate_row
                    best_added = cid
            if best_row is None:
                break
            if (
                (to_float(best_row.get("kept_wr")) or 0.0) <= (to_float(current.get("kept_wr")) or 0.0)
                and (to_float(best_row.get("kept_mean_r")) or 0.0) <= (to_float(current.get("kept_mean_r")) or 0.0)
            ):
                break
            selected.append(best_added)
            remaining.remove(best_added)
            family_rows.append(best_row)
            current = best_row
            if practical_target_met(best_row):
                break
        return family_rows

    rows.extend(build_precision_family(max_rules=PRECISION_MAX_DYNAMIC_RULES))
    rows.extend(build_practical_allowlist_family(max_rules=PRACTICAL_FALLBACK_MAX_ALLOWLIST_RULES))
    rows.extend(build_balanced_family(min_retained_pct=60.0))
    rows.extend(build_practical_blocklist_family(
        min_retained_pct=PRACTICAL_FALLBACK_MIN_RETAINED_PCT,
        max_rules=PRACTICAL_FALLBACK_MAX_BLOCKLIST_RULES,
    ))
    rows.sort(
        key=lambda row: (
            precision_key(row) if row.get("simulation_strategy") == "allowlist" else balanced_key(row),
            -(to_float(row.get("constituent_count")) or 0.0),
        ),
        reverse=True,
    )
    return rows


def main() -> None:
    trades, window_start, window_end = load_trades()
    feature_health_summary = build_feature_health_summary(trades)
    write_csv(OUT_DIR / "feature_health_table.csv", feature_health_summary.get("feature_rows", []))
    baseline_rows = build_engine_baseline_table(trades)
    write_csv(OUT_DIR / "engine_baseline_table.csv", baseline_rows)

    context_features = [
        "market_pressure_state",
        "market_pressure_score_bucket",
        "market_sentiment_classification",
        "market_sentiment_quarantine_flag",
        "major_coin_breadth_bullish_pct_bucket",
        "scanned_symbol_bullish_pct_bucket",
        "funding_breadth_positive_pct_bucket",
        "funding_breadth_mean_bps_bucket",
        "basis_stress_state",
        "liquidation_environment_state",
        "btc_regime_15m",
        "btc_regime_1h",
        "btc_regime_4h",
        "eth_regime_15m",
        "eth_regime_1h",
        "eth_regime_4h",
        "breakout_success_breadth_6h_bucket",
        "breakout_success_breadth_24h_bucket",
    ]
    directional_rows = build_directional_context_table(trades, context_features)
    write_csv(OUT_DIR / "directional_context_table.csv", directional_rows)

    outer_context_rows = feature_state_table(trades, context_features, "outer_context")
    write_csv(OUT_DIR / "outer_context_state_table.csv", outer_context_rows)

    global_context_features = [
        "market_pressure_state",
        "market_sentiment_classification",
        "market_sentiment_quarantine_flag",
        "major_coin_breadth_bullish_pct_bucket",
        "scanned_symbol_bullish_pct_bucket",
        "funding_breadth_positive_pct_bucket",
        "basis_stress_state",
        "liquidation_environment_state",
        "btc_regime_1h",
        "btc_regime_4h",
    ]
    global_context_rows = global_context_state_table(trades, global_context_features)
    write_csv(OUT_DIR / "global_context_state_table.csv", global_context_rows)

    categorical_features = [
        "session",
        "market_regime",
        "structure_alignment",
        "selected_level_type",
        "audit_liquidity_bucket",
        "liquidity_bucket",
        "market_sentiment_quarantine_flag",
        "ensemble_votes_bucket",
        "context_risk_multiplier_bucket",
        "order_flow_live_source",
        "of_trigger_type",
        "of_iceberg_direction",
        "of_hard_block_flag",
    ]
    categorical_rows = feature_state_table(trades, categorical_features, "categorical_state")
    write_csv(OUT_DIR / "categorical_state_table.csv", categorical_rows)

    gate_features = [
        "gate_market_regime_ranging_flag",
        "gate_of_final_score_gap_bucket",
        "gate_static_score_gap_bucket",
        "gate_static_vote_gap_bucket",
        "gate_adx_failure_flag",
        "gate_adx_timeframe",
        "gate_adx_source",
        "gate_adx_fail_slope_bucket",
        "gate_divergence_flag",
        "gate_divergence_type",
        "gate_divergence_strength_bucket",
        "gate_no_htf_alignment_flag",
        "gate_no_htf_trigger_type",
        "gate_htf_score_bucket",
        "gate_volume_structure_fail_flag",
        "gate_volume_structure_score_bucket",
        "gate_orderflow_unavailable_flag",
        "gate_recent_candle_fail_flag",
        "gate_recent_candle_score_bucket",
        "gate_late_breakout_flag",
        "gate_late_breakout_atr_bucket",
        "gate_path_specific_of_fail_flag",
        "gate_path_specific_directional_pressure_bucket",
        "gate_path_specific_imbalance_bucket",
        "gate_path_specific_microstructure_bucket",
        "gate_sweep_breakout_conflict_flag",
        "gate_dol_dominant_4h_flag",
        "gate_dol_inducement_ratio_bucket",
        "gate_dol_s1_score_bucket",
        "gate_dol_source_4h_flag",
        "gate_post_of_proximity_flag",
        "gate_post_of_proximity_atr_bucket",
    ]
    gate_rows = feature_state_table(trades, gate_features, "gate_state")
    write_csv(OUT_DIR / "gate_state_table.csv", gate_rows)

    structure_features = [
        "selected_level_age_bucket",
        "selected_level_distance_bucket",
        "selected_level_type",
        "swing_total_count_15m_bucket",
        "swing_total_count_4h_bucket",
    ]
    structure_rows = feature_state_table(trades, structure_features, "structure")
    write_csv(OUT_DIR / "structure_table.csv", structure_rows)

    adx_features = [
        "audit_adx_1h_slope_bucket",
        "audit_adx_1h_current_bucket",
        "audit_adx_5m_current_bucket",
        "audit_adx_5m_slope_bucket",
        "audit_adx_3m_current_bucket",
        "audit_adx_3m_slope_bucket",
        "audit_adx_15m_current_bucket",
        "audit_adx_15m_slope_bucket",
        "path_adx_current_bucket",
        "path_adx_slope_bucket",
    ]
    adx_rows = feature_state_table(trades, adx_features, "adx")
    write_csv(OUT_DIR / "adx_bucket_table.csv", adx_rows)

    orderflow_features = [
        "order_flow_signal",
        "of_trigger_type",
        "of_confirmation_score_bucket",
        "of_confirmation_votes_bucket",
        "of_microstructure_score_bucket",
        "of_buy_pressure_bucket",
        "of_sell_pressure_bucket",
        "of_opposing_pressure_bucket",
        "of_aggression_bucket",
        "of_aggression_delta_bucket",
        "of_imbalance_bucket",
        "of_cvd_sign",
        "of_imbalance_slope_bucket",
        "of_cvd_slope_bucket",
        "of_cvd_intensity_bucket",
        "of_directional_pressure_bucket",
        "of_buy_pressure_slope_bucket",
        "of_sell_pressure_slope_bucket",
        "of_opposing_pressure_slope_bucket",
        "of_pressure_slope_bucket",
        "of_pressure_alignment",
        "of_net_delta_sign",
        "of_delta_divergence",
        "of_divergence_strength_bucket",
        "of_delta_efficiency_bucket",
        "of_price_move_bps_bucket",
        "of_absorption_detected",
        "of_absorption_ratio_bucket",
        "of_absorbed_levels_bucket",
        "of_absorption_strong_levels_bucket",
        "of_absorption_key_levels_bucket",
        "of_absorption_max_notional_bucket",
        "of_ob_level_absorbed_flag",
        "of_total_vol_vs_vpt_bucket",
        "of_footprint_sufficient_data_flag",
        "of_absorption_sufficient_data_flag",
        "of_opposed_levels_bucket",
        "of_top_opposed_notional_bucket",
        "of_opposed_concentration_bucket",
        "of_wall_count_bucket",
        "of_wall_side_bias",
        "of_wall_max_mult_bucket",
        "of_wall_avg_mult_bucket",
        "of_void_count_bucket",
        "of_void_side_bias",
        "of_void_max_span_bps_bucket",
        "of_liquidity_sufficient_data_flag",
        "of_migration_count_bucket",
        "of_migration_direction_bucket",
        "of_migration_direction_alignment",
        "of_migration_notional_bucket",
        "of_migration_delta_bps_bucket",
        "of_icebergs_detected_flag",
        "of_spoofs_detected_flag",
        "of_bid_icebergs_bucket",
        "of_ask_icebergs_bucket",
        "of_total_notional_bucket",
        "of_notional_side_bias",
        "of_queue_bid_z_bucket",
        "of_queue_ask_z_bucket",
        "of_queue_skew_bucket",
        "of_queue_bid_touch_rel_bucket",
        "of_queue_ask_touch_rel_bucket",
        "of_queue_min_samples_bucket",
        "of_queue_sufficient_data_flag",
        "of_queue_alignment",
        "of_queue_drain_alignment",
        "of_queue_support_alignment",
        "of_queue_touch_alignment",
        "of_spread_bps_bucket",
        "of_window_seconds_bucket",
        "of_sweep_age_bucket",
    ]
    orderflow_rows = feature_state_table(trades, orderflow_features, "orderflow")
    write_csv(OUT_DIR / "orderflow_state_table.csv", orderflow_rows)

    geometry_features = [
        "breakout_body_ratio_bucket",
        "breakout_range_atr_bucket",
        "breakout_strength_bucket",
        "breakout_close_through_bucket",
        "breakout_progress_bucket",
        "breakout_vol_decay_bucket",
        "breakout_vol_ratio_bucket",
        "breakout_continuation_confirmed",
        "has_sweep_anchor_divergence_flag",
        "strong_sweep_anchor_divergence_flag",
        "sweep_anchor_divergence_mode",
        "sweep_anchor_divergence_score_bucket",
        "sweep_absorbed_flag",
        "sweep_absorption_strength_bucket",
        "sweep_divergence_flag",
        "sweep_divergence_strength_bucket",
        "resolution_time_bucket",
    ]
    geometry_rows = feature_state_table(trades, geometry_features, "geometry")
    write_csv(OUT_DIR / "geometry_table.csv", geometry_rows)

    selected_for_interactions = {
        "breakout_BUY": [
            "session",
            "market_regime",
            "structure_alignment",
            "market_pressure_state",
            "market_pressure_score_bucket",
            "major_coin_breadth_bullish_pct_bucket",
            "scanned_symbol_bullish_pct_bucket",
            "funding_breadth_positive_pct_bucket",
            "breakout_success_breadth_6h_bucket",
            "audit_adx_1h_slope_bucket",
            "audit_adx_5m_slope_bucket",
            "audit_adx_15m_slope_bucket",
            "selected_level_age_bucket",
            "swing_total_count_15m_bucket",
            "ensemble_votes_bucket",
            "context_risk_multiplier_bucket",
            "gate_of_final_score_gap_bucket",
            "gate_static_vote_gap_bucket",
            "gate_late_breakout_atr_bucket",
            "gate_dol_dominant_4h_flag",
            "of_microstructure_score_bucket",
            "of_buy_pressure_slope_bucket",
            "of_pressure_slope_bucket",
            "of_imbalance_slope_bucket",
            "of_directional_pressure_bucket",
            "of_top_opposed_notional_bucket",
            "of_opposed_levels_bucket",
            "of_opposed_concentration_bucket",
            "of_absorbed_levels_bucket",
            "of_notional_side_bias",
            "of_delta_efficiency_bucket",
            "of_wall_side_bias",
            "of_wall_count_bucket",
            "of_wall_max_mult_bucket",
            "of_void_side_bias",
            "of_void_max_span_bps_bucket",
            "of_migration_count_bucket",
            "of_migration_direction_alignment",
            "of_migration_notional_bucket",
            "of_queue_bid_touch_rel_bucket",
            "of_queue_ask_touch_rel_bucket",
            "of_queue_min_samples_bucket",
            "of_queue_alignment",
            "of_queue_support_alignment",
            "of_queue_touch_alignment",
            "of_spread_bps_bucket",
        ],
        "breakout_SELL": [
            "session",
            "market_regime",
            "structure_alignment",
            "market_pressure_state",
            "market_pressure_score_bucket",
            "major_coin_breadth_bullish_pct_bucket",
            "scanned_symbol_bullish_pct_bucket",
            "funding_breadth_mean_bps_bucket",
            "funding_breadth_positive_pct_bucket",
            "market_sentiment_classification",
            "audit_adx_1h_slope_bucket",
            "audit_adx_5m_slope_bucket",
            "audit_adx_15m_slope_bucket",
            "selected_level_age_bucket",
            "swing_total_count_15m_bucket",
            "ensemble_votes_bucket",
            "context_risk_multiplier_bucket",
            "gate_adx_failure_flag",
            "gate_adx_fail_slope_bucket",
            "gate_no_htf_alignment_flag",
            "gate_path_specific_directional_pressure_bucket",
            "gate_dol_inducement_ratio_bucket",
            "of_microstructure_score_bucket",
            "of_directional_pressure_bucket",
            "of_sell_pressure_slope_bucket",
            "of_opposing_pressure_slope_bucket",
            "of_pressure_slope_bucket",
            "of_imbalance_slope_bucket",
            "of_pressure_alignment",
            "of_top_opposed_notional_bucket",
            "of_opposed_levels_bucket",
            "of_opposed_concentration_bucket",
            "of_absorbed_levels_bucket",
            "of_absorption_strong_levels_bucket",
            "of_absorption_key_levels_bucket",
            "of_notional_side_bias",
            "of_delta_efficiency_bucket",
            "of_wall_side_bias",
            "of_wall_count_bucket",
            "of_wall_max_mult_bucket",
            "of_void_side_bias",
            "of_void_max_span_bps_bucket",
            "of_migration_count_bucket",
            "of_migration_direction_alignment",
            "of_migration_notional_bucket",
            "of_queue_bid_touch_rel_bucket",
            "of_queue_ask_touch_rel_bucket",
            "of_queue_min_samples_bucket",
            "of_queue_alignment",
            "of_queue_drain_alignment",
            "of_queue_touch_alignment",
            "of_spread_bps_bucket",
        ],
        "sweep_BUY": [
            "session",
            "market_regime",
            "structure_alignment",
            "market_pressure_state",
            "audit_adx_1h_slope_bucket",
            "audit_adx_5m_slope_bucket",
            "audit_liquidity_bucket",
            "htf_confluence_bucket",
            "htf_confluence_score_bucket",
            "selected_level_age_bucket",
            "has_sweep_anchor_divergence_flag",
            "strong_sweep_anchor_divergence_flag",
            "sweep_anchor_divergence_mode",
            "sweep_anchor_divergence_score_bucket",
            "gate_divergence_flag",
            "gate_divergence_strength_bucket",
            "gate_dol_dominant_4h_flag",
            "gate_dol_inducement_ratio_bucket",
            "of_absorption_detected",
            "of_absorbed_levels_bucket",
            "of_absorption_strong_levels_bucket",
            "of_absorption_key_levels_bucket",
            "of_microstructure_score_bucket",
            "of_notional_side_bias",
            "of_delta_efficiency_bucket",
            "of_opposed_levels_bucket",
            "of_wall_side_bias",
            "of_wall_count_bucket",
            "of_wall_max_mult_bucket",
            "of_void_side_bias",
            "of_void_max_span_bps_bucket",
            "of_migration_direction_alignment",
            "of_queue_bid_touch_rel_bucket",
            "of_queue_ask_touch_rel_bucket",
            "of_queue_min_samples_bucket",
            "of_queue_support_alignment",
            "of_queue_touch_alignment",
            "sweep_absorbed_flag",
            "sweep_divergence_flag",
            "sweep_divergence_strength_bucket",
        ],
        "sweep_SELL": [
            "session",
            "market_regime",
            "structure_alignment",
            "market_pressure_state",
            "audit_adx_1h_slope_bucket",
            "audit_adx_5m_slope_bucket",
            "audit_liquidity_bucket",
            "htf_confluence_bucket",
            "htf_confluence_score_bucket",
            "selected_level_age_bucket",
            "has_sweep_anchor_divergence_flag",
            "strong_sweep_anchor_divergence_flag",
            "sweep_anchor_divergence_mode",
            "sweep_anchor_divergence_score_bucket",
            "gate_divergence_flag",
            "gate_divergence_strength_bucket",
            "gate_dol_dominant_4h_flag",
            "gate_dol_inducement_ratio_bucket",
            "of_absorption_detected",
            "of_absorbed_levels_bucket",
            "of_absorption_strong_levels_bucket",
            "of_absorption_key_levels_bucket",
            "of_microstructure_score_bucket",
            "of_notional_side_bias",
            "of_delta_efficiency_bucket",
            "of_opposed_levels_bucket",
            "of_wall_side_bias",
            "of_wall_count_bucket",
            "of_wall_max_mult_bucket",
            "of_void_side_bias",
            "of_void_max_span_bps_bucket",
            "of_migration_direction_alignment",
            "of_queue_bid_touch_rel_bucket",
            "of_queue_ask_touch_rel_bucket",
            "of_queue_min_samples_bucket",
            "of_queue_support_alignment",
            "of_queue_touch_alignment",
            "sweep_absorbed_flag",
            "sweep_divergence_flag",
            "sweep_divergence_strength_bucket",
        ],
    }
    interaction_rows = build_interaction_stack_table(trades, selected_for_interactions)
    write_csv(OUT_DIR / "interaction_stack_table.csv", interaction_rows)

    candidate_map = build_candidate_map(
        trades,
        outer_context_rows + global_context_rows + categorical_rows + gate_rows + structure_rows + adx_rows + orderflow_rows + geometry_rows,
        interaction_rows,
    )
    stability_rows = build_time_stability_table(candidate_map)
    write_csv(OUT_DIR / "time_stability_table.csv", stability_rows)

    concentration_rows = build_concentration_table(candidate_map)
    write_csv(OUT_DIR / "symbol_concentration_table.csv", concentration_rows)

    promotion_rows = promote_candidates(
        baseline_rows,
        outer_context_rows + global_context_rows + categorical_rows + gate_rows + structure_rows + adx_rows + orderflow_rows + geometry_rows,
        interaction_rows,
        concentration_rows,
        stability_rows,
        len(trades),
    )
    write_csv(OUT_DIR / "promotion_ledger.csv", promotion_rows)

    filter_rows = build_filter_simulation_table(trades, promotion_rows, candidate_map)
    write_csv(OUT_DIR / "filter_simulation_table.csv", filter_rows)

    summary = {
        "engine_generation": RESEARCH_ENGINE_GENERATION,
        "research_scope": "post_cutoff_resolved_history",
        "telemetry_cutoff": QUANT_TELEMETRY_CUTOFF.isoformat(),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "stability_window_count": STABILITY_WINDOWS,
        "resolved_rows_used": len(trades),
        "support_floor_mode": "percentage" if QUANT_PERCENTAGE_SUPPORT_FLOORS else "absolute_plus_percentage",
        "support_floor_config": {
            "precision_min_retained_pct": PRECISION_MIN_RETAINED_PCT,
            "precision_min_kept_ratio": PRECISION_MIN_KEPT_RATIO,
            "zero_win_support_ratio": MIN_FILTER_ZERO_WIN_SUPPORT_RATIO,
            "hardblock_support_ratio": MIN_FILTER_HARDBLOCK_SUPPORT_RATIO,
            "boost_support_ratio": MIN_FILTER_BOOST_SUPPORT_RATIO,
            "uplift_support_ratio": MIN_FILTER_UPLIFT_SUPPORT_RATIO,
            "stability_window_support_ratio": STABILITY_WINDOW_SUPPORT_RATIO,
            "interaction_stack_2way_small_engine_ratio": INTERACTION_STACK_2WAY_SMALL_ENGINE_RATIO,
            "interaction_stack_default_ratio": INTERACTION_STACK_DEFAULT_RATIO,
        },
        "feature_health": {
            key: value
            for key, value in feature_health_summary.items()
            if key != "feature_rows"
        },
        "engines": baseline_rows,
        "hard_block_count": sum(1 for r in promotion_rows if r["promotion_decision"] == "hard_block"),
        "boost_count": sum(1 for r in promotion_rows if r["promotion_decision"] == "boost"),
        "quality_uplift_count": sum(1 for r in promotion_rows if r["promotion_decision"] == "quality_uplift"),
        "outputs": {
            "engine_baseline_table": str(OUT_DIR / "engine_baseline_table.csv"),
            "directional_context_table": str(OUT_DIR / "directional_context_table.csv"),
            "outer_context_state_table": str(OUT_DIR / "outer_context_state_table.csv"),
            "global_context_state_table": str(OUT_DIR / "global_context_state_table.csv"),
            "categorical_state_table": str(OUT_DIR / "categorical_state_table.csv"),
            "gate_state_table": str(OUT_DIR / "gate_state_table.csv"),
            "structure_table": str(OUT_DIR / "structure_table.csv"),
            "adx_bucket_table": str(OUT_DIR / "adx_bucket_table.csv"),
            "orderflow_state_table": str(OUT_DIR / "orderflow_state_table.csv"),
            "geometry_table": str(OUT_DIR / "geometry_table.csv"),
            "interaction_stack_table": str(OUT_DIR / "interaction_stack_table.csv"),
            "time_stability_table": str(OUT_DIR / "time_stability_table.csv"),
            "symbol_concentration_table": str(OUT_DIR / "symbol_concentration_table.csv"),
            "promotion_ledger": str(OUT_DIR / "promotion_ledger.csv"),
            "filter_simulation_table": str(OUT_DIR / "filter_simulation_table.csv"),
            "feature_health_table": str(OUT_DIR / "feature_health_table.csv"),
        },
    }
    atomic_write_text(OUT_DIR / "research_summary.json", json.dumps(summary, indent=2))
    cycle_summary = build_quant_cycle_summary(
        trades=trades,
        baseline_rows=baseline_rows,
        promotion_rows=promotion_rows,
        filter_rows=filter_rows,
        window_start=window_start,
        window_end=window_end,
        feature_health_summary=feature_health_summary,
    )
    atomic_write_text(OUT_DIR / "quant_cycle_summary.json", json.dumps(cycle_summary, indent=2))


if __name__ == "__main__":
    main()
