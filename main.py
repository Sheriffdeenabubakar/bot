import asyncio
import importlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from logging import Formatter, StreamHandler
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_LOG_FILE = os.path.join(BASE_DIR, "main_live_monitor.log")
REFRESH_STATE_FILE = os.path.join(BASE_DIR, "live_refresh_state.json")
QUANT_RUNTIME_DIR = os.path.join(BASE_DIR, "quant_research_runtime")
QUANT_RESEARCH_DIR = BASE_DIR
QUANT_LEGACY_RESEARCH_DIR = r"C:\Users\sabubakar\.vscode\quant_research_20260525"
QUANT_CYCLE_SUMMARY_FILE = os.path.join(QUANT_RUNTIME_DIR, "quant_cycle_summary.json")
_QUANT_REFRESH_MODULE = None
_QUANT_REFRESH_IMPORT_FAILED = False
LAGOS_TZ = ZoneInfo("Africa/Lagos")


def _ensure_parent(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _touch_text_file(path: str):
    _ensure_parent(path)
    if os.path.exists(path):
        return
    with open(path, "a", encoding="utf-8"):
        pass


def _save_json_file(path: str, payload: dict):
    _ensure_parent(path)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def _load_json_file(path: str, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


class LagosFormatter(Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, LAGOS_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="milliseconds")


# Configure main logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = StreamHandler(sys.stdout)
    formatter = LagosFormatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def _configure_runtime_file_logging():
    _touch_text_file(MAIN_LOG_FILE)
    root_logger = logging.getLogger()
    target = os.path.abspath(MAIN_LOG_FILE)
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and os.path.abspath(getattr(handler, "baseFilename", "")) == target:
            return
    file_handler = logging.FileHandler(MAIN_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(LagosFormatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s'))
    file_handler.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    root_logger.addHandler(file_handler)


_configure_runtime_file_logging()

_RUNTIME_ASYNC_LOOP = None


def _get_runtime_async_loop():
    global _RUNTIME_ASYNC_LOOP
    if _RUNTIME_ASYNC_LOOP is None or _RUNTIME_ASYNC_LOOP.is_closed():
        _RUNTIME_ASYNC_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_RUNTIME_ASYNC_LOOP)
    return _RUNTIME_ASYNC_LOOP


def _run_async(coro):
    loop = _get_runtime_async_loop()
    return loop.run_until_complete(coro)


def _cfg_bool(mapping, key, default=False):
    value = (mapping or {}).get(key, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _cfg_float(mapping, key, default=0.0):
    try:
        value = (mapping or {}).get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


# Import configurations and modules
try:
    from config import SIGNAL_CONFIG, TRADE_CONFIG, SCANNER_INTERVAL_SECONDS, SIGNALS_FILE_PATH
    from signal_analyzer import (
        _get_quant_live_filter_payload,
        generate_trade_signal,
        get_orderflow_health_report,
        get_signal_summary,
        prepare_orderflow_for_symbols,
        shutdown_orderflow_manager,
    )
    from live_intelligence import repair_live_audit_state, refresh_live_trade_only_telemetry as refresh_core_live_telemetry
    from scanner import scan_coins
except ImportError as e:
    logger.critical(f"Error importing necessary modules: {e}. Ensure all .py files are in the same directory and dependencies are installed.", exc_info=True)
    sys.exit(1)

# ─── Cross-exchange shadow layer (additive, OFF by default) ────────────────
# Extends the proven Bitget-only WS order-flow engine into a shadow
# consolidation across Bitget + Binance + OKX + Bybit. Disabled unless
# CX_ENABLE_SHADOW=true / crossexchange.cx_config.CROSSEXCHANGE_CONFIG is
# edited. Import failures here must NEVER take down the main bot, since this
# layer is purely observational (see crossexchange/README.md).
try:
    from crossexchange.shadow_runner import (
        start_cross_exchange_shadow_background,
        stop_cross_exchange_shadow_background,
        get_cross_exchange_health_report,
    )
    from crossexchange.cx_config import CROSSEXCHANGE_CONFIG as _CX_CONFIG
    _CROSS_EXCHANGE_AVAILABLE = True
except Exception as _cx_import_exc:
    logger.warning(f"Cross-exchange shadow layer unavailable (non-fatal): {_cx_import_exc}")
    _CROSS_EXCHANGE_AVAILABLE = False
    _CX_CONFIG = {"enable_cross_exchange_shadow": False}


def _upsert_signal_summary(summaries: list, summary: dict):
    symbol = (summary or {}).get("symbol")
    if not symbol:
        summaries.append(summary)
        return
    for idx, existing in enumerate(summaries):
        if (existing or {}).get("symbol") == symbol:
            summaries[idx] = summary
            return
    summaries.append(summary)


def save_signals_to_file(signals: list):
    """
    Saves active signals to a JSON file with enhanced error handling.
    """
    try:
        os.makedirs(os.path.dirname(SIGNALS_FILE_PATH), exist_ok=True)
        with open(SIGNALS_FILE_PATH, 'w', encoding="utf-8") as f:
            json.dump(signals, f, indent=4, default=str)
        logger.info(f"Active signals saved to {SIGNALS_FILE_PATH}")
    except IOError as e:
        logger.error(f"Failed to write signals to file {SIGNALS_FILE_PATH}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred while saving signals: {e}", exc_info=True)


def _get_quant_refresh_module():
    global _QUANT_REFRESH_MODULE, _QUANT_REFRESH_IMPORT_FAILED
    if _QUANT_REFRESH_MODULE is not None:
        return _QUANT_REFRESH_MODULE
    if _QUANT_REFRESH_IMPORT_FAILED:
        return None
    try:
        if os.path.isdir(QUANT_LEGACY_RESEARCH_DIR) and QUANT_LEGACY_RESEARCH_DIR not in sys.path:
            sys.path.append(QUANT_LEGACY_RESEARCH_DIR)
        if os.path.isdir(QUANT_RESEARCH_DIR) and QUANT_RESEARCH_DIR in sys.path:
            sys.path.remove(QUANT_RESEARCH_DIR)
        if os.path.isdir(QUANT_RESEARCH_DIR):
            sys.path.insert(0, QUANT_RESEARCH_DIR)
        _QUANT_REFRESH_MODULE = importlib.import_module("auto_refresh_quant_research")
        return _QUANT_REFRESH_MODULE
    except Exception as exc:
        _QUANT_REFRESH_IMPORT_FAILED = True
        logger.warning(f"Quant research module unavailable; cycle summary will be skipped. Error: {exc}")
        return None


def _load_quant_cycle_summary() -> dict:
    summary = _load_json_file(QUANT_CYCLE_SUMMARY_FILE, {}) or {}
    if summary:
        return summary
    legacy_summary_path = os.path.join(QUANT_LEGACY_RESEARCH_DIR, "quant_cycle_summary.json")
    return _load_json_file(legacy_summary_path, {}) or {}


def _refresh_quant_cycle_summary() -> dict:
    if not bool(SIGNAL_CONFIG.get("enable_quant_refresh", False)):
        return _load_quant_cycle_summary()
    module = _get_quant_refresh_module()
    if module is None:
        return _load_quant_cycle_summary()
    try:
        module.run_once(force=False)
    except Exception as exc:
        logger.warning(
            f"Quant research refresh failed during cycle footer; using last available snapshot. Error: {exc}",
            exc_info=True,
        )
    return _load_quant_cycle_summary()


def _log_quant_cycle_summary(summary: dict):
    if not summary:
        return

    def _rule_preview(row: dict | None, fallback_key: str) -> str:
        if not row:
            return ""
        return (
            row.get("rules_preview")
            or row.get("component_preview")
            or row.get(fallback_key)
            or ""
        )

    def _drift_trend(drift: dict | None, key: str) -> str:
        return (((drift or {}).get(key) or {}).get("trend")) or "n/a"

    def _core_rule_preview(rows: list[dict] | None, limit: int = 3) -> str:
        parts = []
        for row in (rows or [])[:limit]:
            label = row.get("candidate_label") or row.get("candidate_id") or "unknown"
            streak = row.get("consecutive_streak")
            appearance = row.get("appearance_rate_pct")
            support = row.get("current_support_n", row.get("support_n"))
            wr = row.get("current_wr", row.get("win_rate_pct"))
            durability = row.get("durability_score")
            status = row.get("watch_status")
            extra = []
            if streak is not None:
                extra.append(f"streak={streak}")
            if appearance is not None:
                extra.append(f"appear={appearance}%")
            if support is not None:
                extra.append(f"support={support}")
            if wr is not None:
                extra.append(f"wr={wr}%")
            if durability is not None:
                extra.append(f"durability={durability}")
            if status:
                extra.append(f"status={status}")
            details = ", ".join(extra)
            parts.append(f"{label} [{details}]")
        return " ; ".join(parts)

    promotion_counts = summary.get("promotion_counts") or {}
    best_precision = summary.get("best_precision_filter") or {}
    best_balanced = summary.get("best_balanced_filter") or {}
    best_practical = summary.get("best_practical_filter") or {}
    runtime_payload = {}
    try:
        runtime_payload = _get_quant_live_filter_payload() or {}
    except Exception as exc:
        logger.warning(f"Could not resolve runtime quant live filter for logging; using summary row. Error: {exc}")
    runtime_active_filter = runtime_payload.get("selected") if isinstance(runtime_payload, dict) else {}
    runtime_active_filter = runtime_active_filter if isinstance(runtime_active_filter, dict) else {}
    active_filter = runtime_active_filter or summary.get("active_dynamic_filter") or summary.get("recommended_live_filter") or {}
    top_boost = ((summary.get("top_boosts") or [{}])[0]) if (summary.get("top_boosts") or []) else {}
    top_hard_block = ((summary.get("top_hard_blocks") or [{}])[0]) if (summary.get("top_hard_blocks") or []) else {}
    drift_summary = summary.get("drift_summary") or {}
    precision_drift = drift_summary.get("precision") or {}
    balanced_drift = drift_summary.get("balanced") or {}
    promotion_watchlist = summary.get("promotion_watchlist") or {}
    persistence_health = summary.get("persistence_health") or {}
    watch_counts = promotion_watchlist.get("status_counts") or {}
    watch_by_decision = promotion_watchlist.get("by_promotion_decision") or {}
    precision_rule_watch = promotion_watchlist.get("precision_rule_watch") or {}
    balanced_rule_watch = promotion_watchlist.get("balanced_rule_watch") or {}

    logger.info(
        "QUANT FINDINGS: "
        f"resolved_window={summary.get('resolved_rows_used', 0)} | "
        f"generation={summary.get('engine_generation') or 'n/a'} | "
        f"hard_blocks={promotion_counts.get('hard_block', 0)} | "
        f"boosts={promotion_counts.get('boost', 0)} | "
        f"uplifts={promotion_counts.get('quality_uplift', 0)} | "
        f"quant_status={summary.get('quant_status', 'unknown')} | "
        f"last_refresh={summary.get('last_completed_at') or 'n/a'}"
    )

    if best_precision:
        precision_target = best_precision.get("target_wr_pct")
        precision_target_met = best_precision.get("target_wr_met")
        logger.info(
            "QUANT PRECISION FILTER: "
            f"{best_precision.get('simulation_label', best_precision.get('simulation_id', 'unknown'))} | "
            f"rules={best_precision.get('constituent_count', 0)} | "
            f"kept_wr={best_precision.get('kept_wr', 'n/a')}% | "
            f"wr_lcb80={best_precision.get('kept_wr_lcb_80', 'n/a')}% | "
            f"kept_mean_r={best_precision.get('kept_mean_r', 'n/a')} | "
            f"retained={best_precision.get('trade_flow_retained_pct', 'n/a')}% | "
            f"resolved={best_precision.get('resolved_rows_used', summary.get('resolved_rows_used', 'n/a'))} | "
            f"min_kept={best_precision.get('precision_min_kept', 'n/a')} | "
            f"min_retained={best_precision.get('precision_min_retained_pct', 'n/a')}% | "
            f"basis={best_precision.get('selection_basis', 'n/a')} | "
            f"target={precision_target or 'n/a'}% | "
            f"target_met={precision_target_met if precision_target_met is not None else 'n/a'}"
        )
        precision_rules = _rule_preview(best_precision, "rules_applied_labels")
        if precision_rules:
            logger.info(f"QUANT PRECISION RULES: {precision_rules}")

    if best_balanced:
        logger.info(
            "QUANT BALANCED FILTER: "
            f"{best_balanced.get('simulation_label', best_balanced.get('simulation_id', 'unknown'))} | "
            f"rules={best_balanced.get('constituent_count', 0)} | "
            f"kept_wr={best_balanced.get('kept_wr', 'n/a')}% | "
            f"kept_mean_r={best_balanced.get('kept_mean_r', 'n/a')} | "
            f"retained={best_balanced.get('trade_flow_retained_pct', 'n/a')}% | "
            f"resolved={best_balanced.get('resolved_rows_used', summary.get('resolved_rows_used', 'n/a'))} | "
            f"basis={best_balanced.get('selection_basis', 'n/a')}"
        )
        balanced_rules = _rule_preview(best_balanced, "rules_applied_labels")
        if balanced_rules:
            logger.info(f"QUANT BALANCED RULES: {balanced_rules}")

    if best_practical:
        logger.info(
            "QUANT PRACTICAL FALLBACK FILTER: "
            f"{best_practical.get('simulation_label', best_practical.get('simulation_id', 'unknown'))} | "
            f"strategy={best_practical.get('simulation_strategy', 'n/a')} | "
            f"rules={best_practical.get('constituent_count', 0)} | "
            f"kept_wr={best_practical.get('kept_wr', 'n/a')}% | "
            f"kept_mean_r={best_practical.get('kept_mean_r', 'n/a')} | "
            f"retained={best_practical.get('trade_flow_retained_pct', 'n/a')}% | "
            f"resolved={best_practical.get('resolved_rows_used', summary.get('resolved_rows_used', 'n/a'))} | "
            f"basis={best_practical.get('selection_basis', 'n/a')} | "
            f"target={best_practical.get('target_wr_pct', 'n/a')}% | "
            f"target_met={best_practical.get('target_wr_met', 'n/a')}"
        )
        practical_rules = _rule_preview(best_practical, "rules_applied_labels")
        if practical_rules:
            logger.info(f"QUANT PRACTICAL FALLBACK RULES: {practical_rules}")

    if active_filter:
        active_lane = active_filter.get("live_selection_lane") or active_filter.get("lane") or "n/a"
        active_strategy = active_filter.get("simulation_strategy") or active_filter.get("strategy") or "n/a"
        active_rules = active_filter.get("constituent_count")
        if active_rules is None:
            active_rules = len(active_filter.get("rules") or [])
        active_reason = active_filter.get("live_selection_reason") or active_filter.get("selection_reason") or "n/a"
        logger.info(
            "QUANT ACTIVE LIVE FILTER: "
            f"{active_filter.get('simulation_label', active_filter.get('simulation_id', 'unknown'))} | "
            f"lane={active_lane} | "
            f"strategy={active_strategy} | "
            f"rules={active_rules} | "
            f"kept_wr={active_filter.get('kept_wr', 'n/a')}% | "
            f"retained={active_filter.get('trade_flow_retained_pct', 'n/a')}% | "
            f"reason={active_reason}"
        )

    if top_boost:
        logger.info(
            "QUANT TOP BOOST: "
            f"{top_boost.get('candidate_label', top_boost.get('candidate_id', 'unknown'))} | "
            f"support={top_boost.get('support_n', 0)} | "
            f"wr={top_boost.get('win_rate_pct', 'n/a')}% | "
            f"mean_r={top_boost.get('mean_r', 'n/a')}"
        )

    if top_hard_block:
        logger.info(
            "QUANT TOP HARD BLOCK: "
            f"{top_hard_block.get('candidate_label', top_hard_block.get('candidate_id', 'unknown'))} | "
            f"support={top_hard_block.get('support_n', 0)} | "
            f"wr={top_hard_block.get('win_rate_pct', 'n/a')}% | "
            f"mean_r={top_hard_block.get('mean_r', 'n/a')}"
        )

    if precision_drift:
        logger.info(
            "QUANT PRECISION DRIFT: "
            f"grade={precision_drift.get('stability_grade', 'n/a')} | "
            f"scope={precision_drift.get('comparison_scope', 'n/a')} | "
            f"history={precision_drift.get('history_depth', 'n/a')} | "
            f"score={precision_drift.get('stability_score', 'n/a')} | "
            f"rule_overlap={precision_drift.get('rule_overlap_pct', 'n/a')}% | "
            f"rule_overlap_raw={precision_drift.get('rule_overlap_raw_pct', 'n/a')}% | "
            f"trade_overlap={precision_drift.get('trade_overlap_pct', 'n/a')}% | "
            f"composition_shift={precision_drift.get('composition_shift_avg_pct', 'n/a')}% | "
            f"resolved_delta={precision_drift.get('resolved_rows_delta', 'n/a')} | "
            f"wr_trend={_drift_trend(precision_drift, 'wr_drift')} | "
            f"mean_r_trend={_drift_trend(precision_drift, 'mean_r_drift')}"
        )
        precision_core = _core_rule_preview(precision_drift.get("persistent_core_rules"))
        if precision_core:
            logger.info(f"QUANT PRECISION CORE: {precision_core}")

    if balanced_drift:
        logger.info(
            "QUANT BALANCED DRIFT: "
            f"grade={balanced_drift.get('stability_grade', 'n/a')} | "
            f"scope={balanced_drift.get('comparison_scope', 'n/a')} | "
            f"history={balanced_drift.get('history_depth', 'n/a')} | "
            f"score={balanced_drift.get('stability_score', 'n/a')} | "
            f"rule_overlap={balanced_drift.get('rule_overlap_pct', 'n/a')}% | "
            f"rule_overlap_raw={balanced_drift.get('rule_overlap_raw_pct', 'n/a')}% | "
            f"trade_overlap={balanced_drift.get('trade_overlap_pct', 'n/a')}% | "
            f"composition_shift={balanced_drift.get('composition_shift_avg_pct', 'n/a')}% | "
            f"resolved_delta={balanced_drift.get('resolved_rows_delta', 'n/a')} | "
            f"wr_trend={_drift_trend(balanced_drift, 'wr_drift')} | "
            f"mean_r_trend={_drift_trend(balanced_drift, 'mean_r_drift')}"
        )

    if promotion_watchlist:
        logger.info(
            "QUANT PROMOTION WATCH: "
            f"stable_core={watch_counts.get('stable_core', 0)} | "
            f"softening={watch_counts.get('softening', 0)} | "
            f"fragile={watch_counts.get('fragile', 0)} | "
            f"demotion_watch={watch_counts.get('demotion_watch', 0)} | "
            f"demote_now={watch_counts.get('demote_now', 0)} | "
            f"lookback={promotion_watchlist.get('lookback_snapshots', 'n/a')}"
        )
        for decision_key, decision_label in [
            ("boost", "BOOST"),
            ("hard_block", "HARD BLOCK"),
            ("quality_uplift", "QUALITY UPLIFT"),
        ]:
            decision_summary = watch_by_decision.get(decision_key) or {}
            decision_counts = decision_summary.get("status_counts") or {}
            if decision_summary:
                logger.info(
                    f"QUANT {decision_label} WATCH: "
                    f"stable_core={decision_counts.get('stable_core', 0)} | "
                    f"softening={decision_counts.get('softening', 0)} | "
                    f"fragile={decision_counts.get('fragile', 0)} | "
                    f"demotion_watch={decision_counts.get('demotion_watch', 0)} | "
                    f"demote_now={decision_counts.get('demote_now', 0)}"
                )
        top_watch = _core_rule_preview(promotion_watchlist.get("top_demotion_watch"), limit=2)
        if top_watch:
            logger.info(f"QUANT DEMOTION WATCH: {top_watch}")
        top_demote_now = _core_rule_preview(promotion_watchlist.get("top_demote_now"), limit=2)
        if top_demote_now:
            logger.info(f"QUANT DEMOTE NOW: {top_demote_now}")
        top_stable = _core_rule_preview(promotion_watchlist.get("top_stable_core"), limit=2)
        if top_stable:
            logger.info(f"QUANT STABLE CORE: {top_stable}")
        if precision_rule_watch:
            precision_counts = precision_rule_watch.get("status_counts") or {}
            logger.info(
                "QUANT PRECISION RULE HEALTH: "
                f"worst_status={precision_rule_watch.get('worst_status', 'n/a')} | "
                f"stable_core={precision_counts.get('stable_core', 0)} | "
                f"softening={precision_counts.get('softening', 0)} | "
                f"fragile={precision_counts.get('fragile', 0)} | "
                f"demotion_watch={precision_counts.get('demotion_watch', 0)} | "
                f"demote_now={precision_counts.get('demote_now', 0)}"
            )
        if balanced_rule_watch:
            balanced_counts = balanced_rule_watch.get("status_counts") or {}
            logger.info(
                "QUANT BALANCED RULE HEALTH: "
                f"worst_status={balanced_rule_watch.get('worst_status', 'n/a')} | "
                f"stable_core={balanced_counts.get('stable_core', 0)} | "
                f"softening={balanced_counts.get('softening', 0)} | "
                f"fragile={balanced_counts.get('fragile', 0)} | "
                f"demotion_watch={balanced_counts.get('demotion_watch', 0)} | "
                f"demote_now={balanced_counts.get('demote_now', 0)}"
            )

    if persistence_health:
        logger.info(
            "QUANT PERSISTENCE: "
            f"grade={persistence_health.get('grade', 'n/a')} | "
            f"backup_recoveries={persistence_health.get('backup_recovery_count', 0)} | "
            f"parse_failures={persistence_health.get('primary_parse_failure_count', 0)} | "
            f"bad_line_events={persistence_health.get('jsonl_bad_line_event_count', 0)} | "
            f"unrecovered={persistence_health.get('unrecovered_load_failure_count', 0)}"
        )


def _save_refresh_state(
    *,
    status: str,
    mode: str,
    started_at: float | None = None,
    completed_at: float | None = None,
    active_signal_count: int = 0,
    eligible_symbol_count: int = 0,
    core_summary: dict | None = None,
    secondary_summary: dict | None = None,
    quant_summary: dict | None = None,
    last_error: str | None = None,
):
    core_summary = core_summary or {}
    secondary_summary = secondary_summary or {}
    quant_summary = quant_summary or {}
    orderflow_health = secondary_summary.get("orderflow_health") or {}
    quant_counts = quant_summary.get("promotion_counts") or {}
    quant_precision = quant_summary.get("best_precision_filter") or {}
    quant_balanced = quant_summary.get("best_balanced_filter") or {}
    quant_drift = quant_summary.get("drift_summary") or {}
    quant_precision_drift = quant_drift.get("precision") or {}
    quant_balanced_drift = quant_drift.get("balanced") or {}
    quant_watchlist = quant_summary.get("promotion_watchlist") or {}
    quant_watch_counts = quant_watchlist.get("status_counts") or {}
    quant_watch_by_decision = quant_watchlist.get("by_promotion_decision") or {}
    quant_watch_precision_rule_health = quant_watchlist.get("precision_rule_watch") or {}
    quant_watch_balanced_rule_health = quant_watchlist.get("balanced_rule_watch") or {}
    quant_watch_demotion = ((quant_watchlist.get("top_demotion_watch") or [{}])[0]) if (quant_watchlist.get("top_demotion_watch") or []) else {}
    quant_watch_demote_now = ((quant_watchlist.get("top_demote_now") or [{}])[0]) if (quant_watchlist.get("top_demote_now") or []) else {}
    quant_top_boost = ((quant_summary.get("top_boosts") or [{}])[0]) if (quant_summary.get("top_boosts") or []) else {}
    quant_top_hard_block = ((quant_summary.get("top_hard_blocks") or [{}])[0]) if (quant_summary.get("top_hard_blocks") or []) else {}
    quant_persistence = quant_summary.get("persistence_health") or {}
    payload = {
        "last_refresh_started_at": started_at,
        "last_refresh_completed_at": completed_at,
        "last_refresh_status": status,
        "last_refresh_mode": mode,
        "active_signal_count": int(active_signal_count or 0),
        "eligible_symbol_count": int(eligible_symbol_count or 0),
        "orderflow_warm_count": orderflow_health.get("warm_count"),
        "orderflow_not_ready_count": orderflow_health.get("not_ready_count"),
        "orderflow_active_groups": orderflow_health.get("active_groups"),
        "orderflow_book_channel": orderflow_health.get("book_channel"),
        "orderflow_failure_counts": orderflow_health.get("failure_counts"),
        "orderflow_avg_latest_trade_age_seconds": orderflow_health.get("avg_latest_trade_age_seconds"),
        "orderflow_avg_latest_book_age_seconds": orderflow_health.get("avg_latest_book_age_seconds"),
        "core_telemetry_cycle": core_summary.get("telemetry_cycle"),
        "core_refreshed_symbol_count": core_summary.get("refreshed_symbol_count"),
        "trade_pending": core_summary.get("trade_pending"),
        "resolved_total": core_summary.get("resolved_total"),
        "resolved_trade_count": core_summary.get("resolved_trade_count"),
        "win_rate_pct": core_summary.get("win_rate_pct"),
        "net_r": core_summary.get("net_r"),
        "refresh_scope": core_summary.get("refresh_scope"),
        "quant_status": quant_summary.get("quant_status"),
        "quant_engine_generation": quant_summary.get("engine_generation"),
        "quant_last_completed_at": quant_summary.get("last_completed_at"),
        "quant_resolved_rows_used": quant_summary.get("resolved_rows_used"),
        "quant_hard_block_count": quant_counts.get("hard_block"),
        "quant_boost_count": quant_counts.get("boost"),
        "quant_quality_uplift_count": quant_counts.get("quality_uplift"),
        "quant_best_precision_filter_id": quant_precision.get("simulation_id"),
        "quant_best_precision_filter_label": quant_precision.get("simulation_label"),
        "quant_best_precision_filter_wr": quant_precision.get("kept_wr"),
        "quant_best_precision_filter_mean_r": quant_precision.get("kept_mean_r"),
        "quant_best_precision_filter_retained_pct": quant_precision.get("trade_flow_retained_pct"),
        "quant_best_precision_target_wr_pct": quant_precision.get("target_wr_pct"),
        "quant_best_precision_target_met": quant_precision.get("target_wr_met"),
        "quant_best_precision_filter_rules": quant_precision.get("rules_preview"),
        "quant_best_precision_stability_grade": quant_precision_drift.get("stability_grade"),
        "quant_best_precision_comparison_scope": quant_precision_drift.get("comparison_scope"),
        "quant_best_precision_history_depth": quant_precision_drift.get("history_depth"),
        "quant_best_precision_stability_score": quant_precision_drift.get("stability_score"),
        "quant_best_precision_rule_overlap_pct": quant_precision_drift.get("rule_overlap_pct"),
        "quant_best_precision_trade_overlap_pct": quant_precision_drift.get("trade_overlap_pct"),
        "quant_best_precision_composition_shift_pct": quant_precision_drift.get("composition_shift_avg_pct"),
        "quant_best_precision_wr_trend": ((quant_precision_drift.get("wr_drift") or {}).get("trend")),
        "quant_best_precision_mean_r_trend": ((quant_precision_drift.get("mean_r_drift") or {}).get("trend")),
        "quant_best_precision_core_rules": [
            row.get("candidate_label") for row in (quant_precision_drift.get("persistent_core_rules") or [])[:3]
        ],
        "quant_best_balanced_filter_id": quant_balanced.get("simulation_id"),
        "quant_best_balanced_filter_label": quant_balanced.get("simulation_label"),
        "quant_best_balanced_filter_wr": quant_balanced.get("kept_wr"),
        "quant_best_balanced_filter_mean_r": quant_balanced.get("kept_mean_r"),
        "quant_best_balanced_filter_retained_pct": quant_balanced.get("trade_flow_retained_pct"),
        "quant_best_balanced_filter_rules": quant_balanced.get("rules_preview"),
        "quant_best_balanced_stability_grade": quant_balanced_drift.get("stability_grade"),
        "quant_best_balanced_comparison_scope": quant_balanced_drift.get("comparison_scope"),
        "quant_best_balanced_history_depth": quant_balanced_drift.get("history_depth"),
        "quant_best_balanced_stability_score": quant_balanced_drift.get("stability_score"),
        "quant_best_balanced_rule_overlap_pct": quant_balanced_drift.get("rule_overlap_pct"),
        "quant_best_balanced_trade_overlap_pct": quant_balanced_drift.get("trade_overlap_pct"),
        "quant_best_balanced_composition_shift_pct": quant_balanced_drift.get("composition_shift_avg_pct"),
        "quant_best_balanced_wr_trend": ((quant_balanced_drift.get("wr_drift") or {}).get("trend")),
        "quant_best_balanced_mean_r_trend": ((quant_balanced_drift.get("mean_r_drift") or {}).get("trend")),
        "quant_best_balanced_core_rules": [
            row.get("candidate_label") for row in (quant_balanced_drift.get("persistent_core_rules") or [])[:3]
        ],
        "quant_watch_stable_core_count": quant_watch_counts.get("stable_core"),
        "quant_watch_softening_count": quant_watch_counts.get("softening"),
        "quant_watch_fragile_count": quant_watch_counts.get("fragile"),
        "quant_watch_demotion_watch_count": quant_watch_counts.get("demotion_watch"),
        "quant_watch_demote_now_count": quant_watch_counts.get("demote_now"),
        "quant_watch_lookback_snapshots": quant_watchlist.get("lookback_snapshots"),
        "quant_watch_boost_status_counts": (quant_watch_by_decision.get("boost") or {}).get("status_counts"),
        "quant_watch_hard_block_status_counts": (quant_watch_by_decision.get("hard_block") or {}).get("status_counts"),
        "quant_watch_quality_uplift_status_counts": (quant_watch_by_decision.get("quality_uplift") or {}).get("status_counts"),
        "quant_watch_precision_rule_health": {
            "worst_status": quant_watch_precision_rule_health.get("worst_status"),
            "status_counts": quant_watch_precision_rule_health.get("status_counts"),
        },
        "quant_watch_balanced_rule_health": {
            "worst_status": quant_watch_balanced_rule_health.get("worst_status"),
            "status_counts": quant_watch_balanced_rule_health.get("status_counts"),
        },
        "quant_watch_top_demotion_candidate": quant_watch_demotion.get("candidate_id"),
        "quant_watch_top_demotion_label": quant_watch_demotion.get("candidate_label"),
        "quant_watch_top_demotion_status": quant_watch_demotion.get("watch_status"),
        "quant_watch_top_demote_now_candidate": quant_watch_demote_now.get("candidate_id"),
        "quant_watch_top_demote_now_label": quant_watch_demote_now.get("candidate_label"),
        "quant_watch_top_demote_now_status": quant_watch_demote_now.get("watch_status"),
        "quant_top_boost_candidate": quant_top_boost.get("candidate_id"),
        "quant_top_boost_label": quant_top_boost.get("candidate_label"),
        "quant_top_boost_wr": quant_top_boost.get("win_rate_pct"),
        "quant_top_hard_block_candidate": quant_top_hard_block.get("candidate_id"),
        "quant_top_hard_block_label": quant_top_hard_block.get("candidate_label"),
        "quant_top_hard_block_wr": quant_top_hard_block.get("win_rate_pct"),
        "quant_persistence_grade": quant_persistence.get("grade"),
        "quant_persistence_backup_recovery_count": quant_persistence.get("backup_recovery_count"),
        "quant_persistence_primary_parse_failure_count": quant_persistence.get("primary_parse_failure_count"),
        "quant_persistence_jsonl_bad_line_event_count": quant_persistence.get("jsonl_bad_line_event_count"),
        "quant_persistence_unrecovered_load_failure_count": quant_persistence.get("unrecovered_load_failure_count"),
        "quant_persistence_affected_files": quant_persistence.get("affected_files"),
        "last_error": last_error,
    }
    try:
        _save_json_file(REFRESH_STATE_FILE, payload)
    except Exception:
        logger.exception("Could not save live refresh state")


def _bootstrap_runtime_surface():
    _touch_text_file(MAIN_LOG_FILE)
    if not os.path.exists(SIGNALS_FILE_PATH):
        save_signals_to_file([])
    core_bootstrap = repair_live_audit_state(run_backfill=False)
    secondary_bootstrap = {"enabled": False, "refresh_scope": "disabled"}
    quant_bootstrap = _refresh_quant_cycle_summary()
    _save_refresh_state(
        status="bootstrapped",
        mode="startup",
        started_at=time.time(),
        completed_at=time.time(),
        core_summary=core_bootstrap,
        secondary_summary=secondary_bootstrap,
        quant_summary=quant_bootstrap,
    )
    return core_bootstrap, secondary_bootstrap


def main_loop():
    """
    Main loop for scanning coins and generating trade signals with enhanced logging.
    """
    signal_check_interval = TRADE_CONFIG.get("signal_check_interval_seconds", 60)
    scanner_once = _cfg_bool(SIGNAL_CONFIG, "scanner_run_once_on_startup", True)
    periodic_rescan_enabled = _cfg_bool(SIGNAL_CONFIG, "enable_periodic_symbol_rescan", False)
    scanner_interval = _cfg_float(
        SIGNAL_CONFIG,
        "periodic_symbol_rescan_seconds",
        SCANNER_INTERVAL_SECONDS if periodic_rescan_enabled else 21600,
    )
    main_loop_yield_seconds = _cfg_float(SIGNAL_CONFIG, "main_loop_yield_seconds", 0.0)
    error_retry_seconds = _cfg_float(SIGNAL_CONFIG, "main_loop_error_retry_seconds", 10.0)

    last_scan_time = 0
    eligible_symbols = []
    scanner_completed = False
    orderflow_startup_prepared = False


    logger.info("Starting SMC Signal Analyzer with dynamic coin scanning.")
    logger.info(f"Signal checks every {signal_check_interval} seconds per symbol (if not scanned).")
    logger.info(
        "Coin scanner mode: "
        + ("startup-only" if scanner_once and not periodic_rescan_enabled else f"periodic every {scanner_interval:.0f}s")
    )
    logger.info(
        "Main loop cadence: "
        + ("instant next cycle" if main_loop_yield_seconds <= 0 else f"{main_loop_yield_seconds:.2f}s yield between cycles")
    )
    logger.info(f"Trade execution is {'ENABLED' if os.getenv('ENABLE_LIVE_TRADING', 'False').lower() == 'true' else 'DISABLED'}. Only printing and saving signals.")
    logger.info(f"Signals will be saved to: {SIGNALS_FILE_PATH}")
    _bootstrap_runtime_surface()

    while True:
        cycle_started_at = time.time()
        core_telemetry_summary = {}
        secondary_telemetry_summary = {}
        try:
            current_time = time.time()
            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            should_scan = (
                not scanner_completed
                or (
                    periodic_rescan_enabled
                    and current_time - last_scan_time >= scanner_interval
                )
            )

            if should_scan:
                logger.info(f"\n--- Initiating coin scan at {current_time_str} ---")
                scanned_coins = scan_coins()
                eligible_symbols = list(dict.fromkeys(coin['symbol'] for coin in scanned_coins))
                if not eligible_symbols:
                    logger.warning("No eligible coins found by the scanner. Will retry before analysis starts.")
                else:
                    logger.info(f"Scanner found {len(eligible_symbols)} eligible symbols: {', '.join(eligible_symbols)}")
                    wait_for_warmup = not orderflow_startup_prepared
                    of_warmup_summary = _run_async(
                        prepare_orderflow_for_symbols(
                            eligible_symbols,
                            wait_for_warmup=wait_for_warmup,
                        )
                    )
                    if (of_warmup_summary or {}).get("enabled"):
                        logger.info(
                            ("Orderflow startup warm-up: " if wait_for_warmup else "Orderflow readiness: ")
                            +
                            f"warm={of_warmup_summary.get('warm_count', 0)}/"
                            f"{of_warmup_summary.get('prepared', len(eligible_symbols))} | "
                            f"active_groups={of_warmup_summary.get('active_groups', 0)} | "
                            f"elapsed={of_warmup_summary.get('warmup_elapsed_seconds', 0)}s | "
                            f"target_age={of_warmup_summary.get('target_age_seconds', 0)}s"
                        )
                        if of_warmup_summary.get("sample_not_ready"):
                            logger.warning(
                                "Orderflow not-ready sample: "
                                + ", ".join(str(s) for s in of_warmup_summary.get("sample_not_ready", [])[:10])
                            )
                    else:
                        logger.info(
                            f"Orderflow warm-up skipped: {(of_warmup_summary or {}).get('reason', 'disabled')}"
                        )
                    orderflow_startup_prepared = True
                    scanner_completed = bool(scanner_once or not periodic_rescan_enabled)

                    # Start (or extend) the cross-exchange shadow layer with the
                    # same symbol universe the proven Bitget path just warmed up.
                    # No-op unless enable_cross_exchange_shadow is turned on.
                    if _CROSS_EXCHANGE_AVAILABLE and _CX_CONFIG.get("enable_cross_exchange_shadow", False):
                        try:
                            cx_status = start_cross_exchange_shadow_background(eligible_symbols)
                            if cx_status.get("enabled"):
                                logger.info(f"Cross-exchange layer active: {cx_status}")
                                if _CX_CONFIG.get("cx_live_signal_mode", True):
                                    logger.info(
                                        "Cross-exchange LIVE basis ENABLED: orderflow warm/coverage/confirmation "
                                        "and entry-gate metrics (pressure/imbalance/CVD/aggression) now use the "
                                        "consolidated Bitget+Binance+OKX+Bybit book. USD thresholds scale with "
                                        "contributing-venue count; set CX_LIVE_SIGNAL_MODE=false to revert to "
                                        "shadow-only."
                                    )
                        except Exception:
                            logger.exception("Cross-exchange shadow layer failed to start (non-fatal)")
                last_scan_time = current_time

            active_signals = []
            if eligible_symbols:
                logger.info(f"\n--- Analyzing signals for {len(eligible_symbols)} symbols at {current_time_str} ---")
                for symbol in eligible_symbols:
                    symbol_yield_seconds = float(SIGNAL_CONFIG.get("of_between_symbol_yield_seconds", 0.05) or 0.0)
                    if symbol_yield_seconds > 0:
                        _run_async(asyncio.sleep(symbol_yield_seconds))
                    logger.debug(f"   --> Checking {symbol} for signals...")
                    signal, reason = _run_async(generate_trade_signal(symbol))
                    if signal:
                        summary = _run_async(get_signal_summary(symbol, signal=signal, reason=reason))
                        placement_result = signal.get("_placement_result") if isinstance(signal, dict) else None
                        placement_placed = bool(placement_result and placement_result.get("placed"))
                        placement_failed = bool(
                            placement_result is not None
                            and not placement_placed
                        )
                        if placement_placed:
                            active_signals.append(summary)
                        log_prefix = "TRADE PLACED" if placement_placed else "SIGNAL GENERATED BUT NOT PLACED"
                        logger.info(f"{log_prefix}: {symbol} - {summary['signal']} @ {summary.get('entry', 0):.4f} "
                                    f"(SL: {summary.get('stop_loss', 0):.4f}, TP1: {summary.get('take_profit_1', 0):.4f}, "
                                    f"TP2: {summary.get('take_profit_2', 0):.4f}, Trailing SL: {summary.get('trailing_stop_loss', 0):.4f}) | "
                                    f"Strength: {summary.get('strength', 0)} | Rationale: {summary.get('rationale', [])} | "
                                    f"OrderflowBasis: {summary.get('orderflow_basis') or 'n/a'}")
                        if placement_failed:
                            logger.warning(
                                f"[{symbol}] Placement failed after signal generation: "
                                f"{placement_result.get('reason_text') or placement_result.get('reason_code')}"
                            )
                    else:
                        logger.info(f"NO SIGNAL: No trade signal generated for {symbol}. Reason: {reason}")
            else:
                logger.info("No eligible symbols to analyze. Waiting for next scan cycle.")

            if active_signals:
                logger.info(f"\n--- {len(active_signals)} PLACED SIGNALS ---")
                for summary in active_signals:
                    logger.info(f"SIGNAL SUMMARY: {summary['symbol']} - {summary['signal']} @ {summary.get('entry', 0):.4f} "
                                f"(SL: {summary.get('stop_loss', 0):.4f}, TP1: {summary.get('take_profit_1', 0):.4f}, "
                                f"TP2: {summary.get('take_profit_2', 0):.4f}, Trailing SL: {summary.get('trailing_stop_loss', 0):.4f}) | "
                                f"Strength: {summary.get('strength', 0)} | Market Structure: {summary.get('market_structure', 'UNKNOWN')}")
            else:
                logger.info("No placed BUY/SELL trades in this cycle.")

            save_signals_to_file(active_signals)
            core_telemetry_summary = refresh_core_live_telemetry()
            orderflow_health_summary = {}
            try:
                orderflow_health_summary = _run_async(get_orderflow_health_report(eligible_symbols))
                if (orderflow_health_summary or {}).get("enabled"):
                    logger.info(
                        "ORDERFLOW HEALTH: "
                        f"warm={orderflow_health_summary.get('warm_count', 0)}/"
                        f"{orderflow_health_summary.get('prepared', len(eligible_symbols))} | "
                        f"not_ready={orderflow_health_summary.get('not_ready_count', 0)} | "
                        f"active_groups={orderflow_health_summary.get('active_groups', 0)} | "
                        f"book={orderflow_health_summary.get('book_channel')} | "
                        f"avg_trade_age={orderflow_health_summary.get('avg_latest_trade_age_seconds')}s | "
                        f"avg_book_age={orderflow_health_summary.get('avg_latest_book_age_seconds')}s "
                        f"(median={orderflow_health_summary.get('median_latest_book_age_seconds')}s) | "
                        f"failures={orderflow_health_summary.get('failure_counts', {})}"
                    )
                    if orderflow_health_summary.get("sample_not_ready"):
                        logger.warning(
                            "ORDERFLOW NOT-READY SAMPLE: "
                            + ", ".join(str(s) for s in orderflow_health_summary.get("sample_not_ready", [])[:10])
                        )
            except Exception as exc:
                logger.warning(f"Orderflow health report failed: {exc}")
            try:
                _cxh = get_cross_exchange_health_report() or {}
                _vh = _cxh.get("venue_health") or {}
                _parts = []
                for _v in ("bitget", "binance", "okx", "bybit"):
                    _hr = _vh.get(_v) or {}
                    _seg = f"{_v} {_hr.get('eligible_count', 0)}/{_hr.get('symbols_tracked', 0)}"
                    if _hr.get("avg_fresh_age_s") is not None:
                        _seg += f" (fresh ~{_hr.get('avg_fresh_age_s')}s)"
                    _parts.append(_seg)
                _cc = _vh.get("consolidated") or {}
                _down = (_vh.get("outage") or {}).get("down_venues") or []
                logger.info(
                    "CROSS-EXCHANGE: " + " | ".join(_parts)
                    + f" | multi-venue symbols: {_cc.get('multi_venue', 0)}"
                    + f" | bitget-only: {_cc.get('bitget_only', 0)}"
                    + f" | no-venue: {_cc.get('no_venue', 0)}"
                    + (f" | DOWN: {_down}" if _down else "")
                    + (f" | layer: {_cxh.get('error')}" if _cxh.get("error") else "")
                )
            except Exception:
                pass
            secondary_telemetry_summary = {
                "enabled": False,
                "refresh_scope": "disabled",
                "orderflow_health": orderflow_health_summary,
            }
            quant_cycle_summary = _refresh_quant_cycle_summary()

            if core_telemetry_summary.get("enabled"):
                health = core_telemetry_summary.get("health") or {}
                placed_metrics = health.get("placed_trade_metrics") or {}
                top_placed_path = health.get("top_placed_path") or {}
                logger.info(
                    "LIVE PLACED TRADE AUDIT: "
                    f"resolved_this_cycle={core_telemetry_summary.get('resolved_total', 0)} | "
                    f"pending={core_telemetry_summary.get('trade_pending', 0)} | "
                    f"resolved_total={core_telemetry_summary.get('resolved_trade_count', 0)} | "
                    f"win_rate={placed_metrics.get('win_rate_pct', 0.0)}% | "
                    f"net_r={placed_metrics.get('net_r', 0.0)}"
                )
                if top_placed_path:
                    logger.info(
                        "TOP PLACED PATH: "
                        f"{top_placed_path.get('primary_setup', 'unknown')} | "
                        f"{top_placed_path.get('direction', 'unknown')} | "
                        f"{top_placed_path.get('market_regime', 'unknown')} | "
                        f"net_r={top_placed_path.get('net_r', 0)} | "
                        f"wins={top_placed_path.get('winner_count', 0)}/"
                        f"{top_placed_path.get('resolved_trades', 0)}"
                    )
            _log_quant_cycle_summary(quant_cycle_summary)

            _save_refresh_state(
                status="ok",
                mode="automatic",
                started_at=cycle_started_at,
                completed_at=time.time(),
                active_signal_count=len(active_signals),
                eligible_symbol_count=len(eligible_symbols),
                core_summary=core_telemetry_summary,
                secondary_summary=secondary_telemetry_summary,
                quant_summary=quant_cycle_summary,
            )

            logger.info("=============================================\n")
            if main_loop_yield_seconds > 0:
                logger.info(f"Main loop yielding for {main_loop_yield_seconds:.2f} seconds...")
                _run_async(asyncio.sleep(main_loop_yield_seconds))
            else:
                logger.info("Main loop continuing immediately to next cycle.")
        except Exception as e:
            logger.critical(f"An unhandled error occurred in the main loop: {e}", exc_info=True)
            _save_refresh_state(
                status="error",
                mode="automatic",
                started_at=cycle_started_at,
                completed_at=time.time(),
                active_signal_count=0,
                eligible_symbol_count=len(eligible_symbols),
                core_summary=core_telemetry_summary,
                secondary_summary=secondary_telemetry_summary,
                quant_summary=_load_quant_cycle_summary(),
                last_error=str(e),
            )
            logger.info(f"Retrying after {error_retry_seconds:.2f} seconds...")
            if error_retry_seconds > 0:
                _run_async(asyncio.sleep(error_retry_seconds))


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Main loop interrupted by user. Exiting gracefully...")
    finally:
        try:
            _run_async(shutdown_orderflow_manager())
        except Exception:
            logger.exception("Error while shutting down orderflow manager")
        if _CROSS_EXCHANGE_AVAILABLE:
            try:
                stop_cross_exchange_shadow_background()
            except Exception:
                logger.exception("Error while shutting down cross-exchange shadow layer")
        if _RUNTIME_ASYNC_LOOP is not None and not _RUNTIME_ASYNC_LOOP.is_closed():
            _RUNTIME_ASYNC_LOOP.close()
