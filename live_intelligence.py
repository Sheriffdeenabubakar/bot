import hashlib
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from logging import Formatter, StreamHandler
from typing import Any

import pandas as pd

from bitget_client import api_client, get_candlestick_data
from config import SIGNAL_CONFIG


logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = StreamHandler(sys.stdout)
    formatter = Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_ACCEPTED_EVENTS_FILE = os.path.join(BASE_DIR, "live_accepted_signals.jsonl")
_ACCEPTED_PENDING_FILE = os.path.join(BASE_DIR, "live_accepted_pending.json")
_ACCEPTED_RESOLVED_FILE = os.path.join(BASE_DIR, "live_accepted_resolved.jsonl")

_REJECTED_EVENTS_FILE = os.path.join(BASE_DIR, "live_rejected_signals.jsonl")
_REJECTED_PENDING_FILE = os.path.join(BASE_DIR, "live_rejected_pending.json")
_REJECTED_RESOLVED_FILE = os.path.join(BASE_DIR, "live_rejected_resolved.jsonl")

_TRADE_EVENTS_FILE = os.path.join(BASE_DIR, "live_trade_audit_events.jsonl")
_TRADE_PENDING_FILE = os.path.join(BASE_DIR, "live_trade_audit_pending.json")
_TRADE_RESOLVED_FILE = os.path.join(BASE_DIR, "live_trade_audit_resolved.jsonl")
_TRADE_AUDIT_SUMMARY_FILE = os.path.join(BASE_DIR, "live_trade_audit_summary.json")

_PATH_SCORECARDS_FILE = os.path.join(BASE_DIR, "live_path_scorecards.json")
_CONTEXT_SCORECARDS_FILE = os.path.join(BASE_DIR, "live_context_scorecards.json")
_SYMBOL_QUALITY_FILE = os.path.join(BASE_DIR, "live_symbol_quality.json")
_HEALTH_REPORT_FILE = os.path.join(BASE_DIR, "live_health_report.json")
_KILLER_REPORT_FILE = os.path.join(BASE_DIR, "live_killer_report.json")
_GATE_EVIDENCE_FILE = os.path.join(BASE_DIR, "live_gate_evidence_report.json")
_CONFIG_SNAPSHOT_FILE = os.path.join(BASE_DIR, "live_config_snapshot.json")

_accepted_pending: dict[str, dict[str, Any]] = {}
_rejected_pending: dict[str, dict[str, Any]] = {}
_trade_pending: dict[str, dict[str, Any]] = {}

_accepted_known_ids: set[str] = set()
_rejected_known_ids: set[str] = set()
_trade_known_ids: set[str] = set()
_telemetry_cycle_counter = 0
_rejected_rotation_offset = 0

_LIVE_INTELLIGENCE_JSONL_FILES = (
    _TRADE_EVENTS_FILE,
    _TRADE_RESOLVED_FILE,
)

_LIVE_INTELLIGENCE_JSON_FILES = (
    _TRADE_PENDING_FILE,
    _TRADE_AUDIT_SUMMARY_FILE,
    _HEALTH_REPORT_FILE,
    _CONFIG_SNAPSHOT_FILE,
)


def _track_only_live_placed_trades() -> bool:
    return True


def _json_safe_default(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _ensure_parent(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _touch_text_file(path: str):
    _ensure_parent(path)
    if os.path.exists(path):
        return
    try:
        with open(path, "a", encoding="utf-8"):
            pass
    except Exception as exc:
        logger.warning(f"Could not materialize {path}: {exc}")


def _load_json_file(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8-sig") as handle:
            return json.load(handle)
    except Exception as exc:
        logger.warning(f"Could not load {path}: {exc}")
        return default


def _save_json_file(path: str, payload):
    try:
        _ensure_parent(path)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, default=_json_safe_default)
    except Exception as exc:
        logger.warning(f"Could not save {path}: {exc}")


def _append_jsonl(path: str, payload: dict[str, Any]):
    try:
        _ensure_parent(path)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=_json_safe_default) + "\n")
    except Exception as exc:
        logger.warning(f"Could not append JSONL {path}: {exc}")


def _save_jsonl_rows(path: str, rows: list[dict[str, Any]]):
    try:
        _ensure_parent(path)
        with open(path, "w", encoding="utf-8") as handle:
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                handle.write(json.dumps(row, default=_json_safe_default) + "\n")
    except Exception as exc:
        logger.warning(f"Could not save JSONL rows to {path}: {exc}")


def _load_jsonl_rows(path: str) -> list[dict[str, Any]]:
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, "r", encoding="utf-8-sig") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        rows.append(payload)
                except Exception:
                    continue
    except Exception as exc:
        logger.warning(f"Could not load JSONL rows from {path}: {exc}")
    return rows


def _load_known_ids(path: str) -> set[str]:
    known_ids = set()
    for row in _load_jsonl_rows(path):
        record_id = str(row.get("record_id") or "").strip()
        if record_id:
            known_ids.add(record_id)
    return known_ids


def _safe_float(value, default=None):
    try:
        numeric = float(value)
        if pd.isna(numeric):
            return default
        return numeric
    except Exception:
        return default


def _safe_int(value, default=None):
    try:
        return int(float(value))
    except Exception:
        return default


def _safe_text(value, default="unknown"):
    text = str(value or "").strip()
    return text if text else default


def _decision_ts_ms(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return int(numeric if numeric > 10_000_000_000 else numeric * 1000)
    try:
        return int(pd.Timestamp(value).timestamp() * 1000)
    except Exception:
        return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def _fingerprint(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _reason_code_from_text(reason_text: Any) -> str:
    text = str(reason_text or "").strip().lower()
    if not text:
        return "unknown"
    mapping = (
        ("Could not get current price", "current_price_unavailable"),
        ("Insufficient data", "insufficient_data"),
        ("bucket filter blocked", "bucket_filter_blocked"),
        ("market is ranging", "ranging_market_blocked"),
        ("No funding rate alignment", "funding_alignment_blocked"),
        ("Ensemble vote too low", "ensemble_vote_blocked"),
        ("No world-class signal found", "score_below_threshold"),
        ("disabled by research", "research_session_setup_blocked"),
        ("Higher timeframe alignment too weak", "htf_alignment_blocked"),
        ("SELL signals disabled", "sell_side_disabled"),
        ("requires strong confirmation", "breakout_confirmation_blocked"),
        ("requires at least medium confirmation", "sweep_confirmation_blocked"),
        ("weak confirmation", "weak_confirmation_blocked"),
        ("Precision profile blocked", "precision_profile_blocked"),
        ("Research quality gate blocked", "quality_gate_blocked"),
        ("Could not compute SL/TP", "risk_model_failed"),
        ("Strong-trend breakout trades must align", "breakout_buy_bias_blocked"),
    )
    for needle, code in mapping:
        if needle.lower() in text:
            return code
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text[:80]).strip("_")
    return cleaned or "unknown"


def _trade_audit_identity_key(payload: dict[str, Any]) -> tuple[str, str, str] | None:
    payload = payload or {}
    symbol = str(payload.get("symbol") or "").strip().upper()
    order_id = str(payload.get("order_id") or "").strip()
    client_oid = str(payload.get("client_oid") or "").strip()
    if symbol and order_id:
        return ("order_id", symbol, order_id)
    if symbol and client_oid:
        return ("client_oid", symbol, client_oid)
    return None


def _trade_audit_identity_token(payload: dict[str, Any]) -> str | None:
    identity = _trade_audit_identity_key(payload)
    if not identity:
        return None
    return "::".join(identity)


def _make_record_id(prefix: str, payload: dict[str, Any]) -> str:
    if prefix == "trade_audit":
        identity = _trade_audit_identity_key(payload)
        if identity is not None:
            return _fingerprint(prefix, *identity)
    return _fingerprint(
        prefix,
        payload.get("symbol"),
        payload.get("direction"),
        payload.get("primary_setup"),
        payload.get("decision_candle_ts_ms") or payload.get("decision_ts_ms"),
        payload.get("reason_code") or payload.get("audit_reason_code"),
        payload.get("rejection_stage") or payload.get("audit_status"),
    )


def _risk_metrics(entry_price, stop_loss, tp1, tp2):
    entry = _safe_float(entry_price)
    sl = _safe_float(stop_loss)
    tp1_val = _safe_float(tp1)
    tp2_val = _safe_float(tp2)
    if entry is None or sl is None:
        return None, None, None
    risk = abs(entry - sl)
    if risk <= 0:
        return None, None, None
    tp1_r = abs(tp1_val - entry) / risk if tp1_val is not None else None
    tp2_r = abs(tp2_val - entry) / risk if tp2_val is not None else None
    return risk, tp1_r, tp2_r


def _prepare_payload(stream: str, payload: dict[str, Any]) -> dict[str, Any]:
    row = dict(payload or {})
    row["stream"] = stream
    row["symbol"] = _safe_text(row.get("symbol"), "")
    row["direction"] = _safe_text(row.get("direction"), "UNKNOWN")
    row["primary_setup"] = _safe_text(row.get("primary_setup"), "unknown")
    row["session_bucket"] = _safe_text(row.get("session_bucket"), "unknown")
    row["symbol_bucket"] = _safe_text(row.get("symbol_bucket"), "unknown")
    row["market_regime"] = _safe_text(row.get("market_regime"), "unknown")
    row["quality_gate_rule_id"] = row.get("quality_gate_rule_id")
    row["reason_text"] = row.get("reason_text") or row.get("audit_reason_text")
    row["reason_code"] = row.get("reason_code") or _reason_code_from_text(row["reason_text"])
    row["audit_reason_code"] = row.get("audit_reason_code") or row["reason_code"]
    row["recorded_at"] = _safe_float(row.get("recorded_at"), time.time())
    row["decision_ts_ms"] = _decision_ts_ms(row.get("decision_ts_ms")) or _decision_ts_ms(row.get("timestamp")) or _now_ms()
    row["decision_candle_ts_ms"] = _decision_ts_ms(row.get("decision_candle_ts_ms")) or row["decision_ts_ms"]
    row["entry_price"] = _safe_float(row.get("entry_price") or row.get("entry"))
    row["stop_loss"] = _safe_float(row.get("stop_loss"))
    row["take_profit_1"] = _safe_float(row.get("take_profit_1"))
    row["take_profit_2"] = _safe_float(row.get("take_profit_2"))
    row["trailing_stop_loss"] = _safe_float(row.get("trailing_stop_loss"))
    row["confidence_score"] = _safe_float(row.get("confidence_score") or row.get("strength"), 0.0)
    row["confirmation_score"] = _safe_float(row.get("confirmation_score"), 0.0)
    row["htf_confluence"] = _safe_float(row.get("htf_confluence") or row.get("htf_confluence_score"), 0.0)
    row["htf_confluence_score"] = _safe_float(row.get("htf_confluence_score") or row.get("htf_confluence"), 0.0)
    row["funding_confluence"] = _safe_int(row.get("funding_confluence"), 0)
    row["analysis_duration_sec"] = _safe_float(row.get("analysis_duration_sec"))
    row["order_flow_signal"] = _safe_text(row.get("order_flow_signal"), "unknown")
    row["order_flow_buy_pressure"] = _safe_float(row.get("order_flow_buy_pressure"))
    row["order_flow_sell_pressure"] = _safe_float(row.get("order_flow_sell_pressure"))
    row["order_flow_imbalance"] = _safe_float(row.get("order_flow_imbalance"))
    row["adx_regime"] = _safe_text(row.get("adx_regime"), "unknown")
    row["audit_status"] = _safe_text(row.get("audit_status"), "not_applicable")
    row["audit_reason_text"] = row.get("audit_reason_text") or row.get("reason_text")
    row["order_id"] = str(row.get("order_id") or "").strip() or None
    row["client_oid"] = str(row.get("client_oid") or "").strip() or None
    placed_trade_status = stream == "trade_audit" and row["audit_status"] in {"placed", "placed_with_plan_fallback"}
    exchange_identity_ready = bool(row["order_id"] or row["client_oid"])
    row["exchange_resolution_required"] = placed_trade_status
    row["exchange_resolution_strict"] = placed_trade_status
    row["exchange_resolution_identity_ready"] = exchange_identity_ready
    row["exchange_resolution_blocked_reason"] = None
    row["exchange_identity_key"] = _trade_audit_identity_token(row) if stream == "trade_audit" else None
    row["actual_fill_price"] = _safe_float(row.get("actual_fill_price"))
    row["size"] = _safe_float(row.get("size"))
    row["rejection_stage"] = _safe_text(row.get("rejection_stage"), "accepted" if stream == "accepted" else "unknown")
    row["rationale"] = list(row.get("rationale") or [])
    risk, tp1_r, tp2_r = _risk_metrics(
        row.get("entry_price"),
        row.get("stop_loss"),
        row.get("take_profit_1"),
        row.get("take_profit_2"),
    )
    row["risk_per_unit"] = risk
    row["tp1_r"] = tp1_r
    row["tp2_r"] = tp2_r
    resolution_mode = "none"
    if stream == "rejected":
        resolution_mode = "candle"
    elif stream == "trade_audit":
        if placed_trade_status:
            if exchange_identity_ready:
                resolution_mode = "exchange"
            else:
                resolution_mode = "exchange_identity_missing"
                row["exchange_resolution_blocked_reason"] = "missing_order_identity"
        else:
            resolution_mode = "candle"
    row["resolution_mode"] = resolution_mode
    row["trackable"] = bool(
        resolution_mode in {"candle", "exchange"}
        and row["symbol"]
        and row["direction"] in {"BUY", "SELL"}
        and row["entry_price"] is not None
        and row["stop_loss"] is not None
        and (row["take_profit_1"] is not None or row["take_profit_2"] is not None)
        and risk not in (None, 0)
    )
    row["record_id"] = row.get("record_id") or _make_record_id(stream, row)
    return row


def _register_record(row: dict[str, Any], *, events_file: str, pending_file: str, pending_store: dict[str, dict[str, Any]], known_ids: set[str]):
    record_id = row["record_id"]
    exchange_identity_key = str(row.get("exchange_identity_key") or "").strip()
    if record_id in known_ids or record_id in pending_store:
        return record_id
    if exchange_identity_key:
        for existing in pending_store.values():
            if str((existing or {}).get("exchange_identity_key") or "").strip() == exchange_identity_key:
                return str((existing or {}).get("record_id") or record_id)
    _append_jsonl(events_file, row)
    known_ids.add(record_id)
    if row.get("trackable"):
        pending_store[record_id] = dict(row)
        _save_json_file(pending_file, pending_store)
    return record_id


def _rebuild_pending_store_from_events(stream: str, events_file: str, resolved_file: str) -> dict[str, dict[str, Any]]:
    resolved_ids = _load_known_ids(resolved_file)
    rebuilt: dict[str, dict[str, Any]] = {}
    for raw_row in _load_jsonl_rows(events_file):
        if not isinstance(raw_row, dict):
            continue
        row = _prepare_payload(stream, raw_row)
        record_id = str(row.get("record_id") or "").strip()
        if not row.get("trackable") or not record_id or record_id in resolved_ids:
            continue
        rebuilt[record_id] = row
    return rebuilt


def _ensure_pending_state_files():
    for path in _LIVE_INTELLIGENCE_JSONL_FILES:
        _touch_text_file(path)
    if _trade_pending or not os.path.exists(_TRADE_PENDING_FILE):
        _save_json_file(_TRADE_PENDING_FILE, _trade_pending)
    for path in _LIVE_INTELLIGENCE_JSON_FILES:
        if not os.path.exists(path):
            _save_json_file(path, {})


def materialize_live_intelligence_files():
    for path in _LIVE_INTELLIGENCE_JSONL_FILES:
        _touch_text_file(path)
    for path in _LIVE_INTELLIGENCE_JSON_FILES:
        if not os.path.exists(path):
            _save_json_file(path, {})
    _ensure_pending_state_files()


def repair_live_audit_state(run_backfill: bool = True):
    global _trade_pending, _trade_known_ids
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        materialize_live_intelligence_files()
        return {"enabled": False}
    _load_pending_state()
    materialize_live_intelligence_files()
    repaired_events, events_changed = _repair_trade_audit_event_file()
    repaired_rows, repaired_changed = _repair_trade_audit_resolved_file()
    _trade_pending = _rebuild_pending_store_from_events("trade_audit", _TRADE_EVENTS_FILE, _TRADE_RESOLVED_FILE)
    _save_json_file(_TRADE_PENDING_FILE, _trade_pending)
    _trade_known_ids = set(_trade_pending) | _load_known_ids(_TRADE_RESOLVED_FILE)
    health_payload = _save_trade_only_reports()
    return {
        "enabled": True,
        "refresh_scope": "placed_trade_only",
        "trade_pending": sum(1 for row in _trade_pending.values() if _is_live_placed_trade_row(row)),
        "event_rows_repaired": len(repaired_events),
        "event_file_rewritten": events_changed,
        "resolved_rows_repaired": len(repaired_rows),
        "resolved_file_rewritten": repaired_changed,
        "health": health_payload,
    }


def record_accepted_signal(payload: dict[str, Any]):
    if _track_only_live_placed_trades():
        return None
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        return None
    row = _prepare_payload("accepted", payload)
    return _register_record(
        row,
        events_file=_ACCEPTED_EVENTS_FILE,
        pending_file=_ACCEPTED_PENDING_FILE,
        pending_store=_accepted_pending,
        known_ids=_accepted_known_ids,
    )


def record_rejected_signal(payload: dict[str, Any]):
    if _track_only_live_placed_trades():
        return None
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        return None
    row = _prepare_payload("rejected", payload)
    return _register_record(
        row,
        events_file=_REJECTED_EVENTS_FILE,
        pending_file=_REJECTED_PENDING_FILE,
        pending_store=_rejected_pending,
        known_ids=_rejected_known_ids,
    )


def record_trade_audit(payload: dict[str, Any]):
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        return None
    row = _prepare_payload("trade_audit", payload)
    if _track_only_live_placed_trades() and not _is_live_placed_trade_row(row):
        return None
    if row.get("exchange_resolution_required") and not row.get("exchange_resolution_identity_ready"):
        logger.warning(
            f"[{row.get('symbol')}] Placed trade audit missing exchange identity; it will be logged but excluded from resolution tracking"
        )
    return _register_record(
        row,
        events_file=_TRADE_EVENTS_FILE,
        pending_file=_TRADE_PENDING_FILE,
        pending_store=_trade_pending,
        known_ids=_trade_known_ids,
    )


def _normalize_bitget_symbol(symbol):
    return str(symbol or "").replace("_UMCBL", "").replace("/", "").strip().upper()


def _parse_any_timestamp(raw_ts):
    if raw_ts in (None, ""):
        return None
    if isinstance(raw_ts, pd.Timestamp):
        ts = raw_ts
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")
    if isinstance(raw_ts, (int, float)) or str(raw_ts).strip().isdigit():
        try:
            value = int(float(raw_ts))
            unit = "ms" if abs(value) >= 10**11 else "s"
            return pd.to_datetime(value, unit=unit, utc=True)
        except Exception:
            return None
    try:
        ts = pd.Timestamp(raw_ts)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")
    except Exception:
        return None


def _infer_trade_resolution_basis(row: dict[str, Any]) -> str:
    row = row or {}
    existing = str(row.get("resolution_basis") or "").strip()
    if existing:
        return existing
    resolution_note = str(row.get("resolution_note") or "").strip().lower()
    resolution_status = str(row.get("resolution_status") or "").strip().lower()
    if resolution_status == "order_cancelled_unfilled" or resolution_note == "exchange_unfilled_cancellation":
        return "exchange_unfilled_cancellation"
    if (
        resolution_note == "exchange_fills_reconciled"
        or resolution_status.startswith("exchange_")
        or resolution_status in {"stop_loss_closed", "take_profit_closed", "profitable_exchange_close", "losing_exchange_close"}
    ):
        return "exchange_fills_reconciled"
    if resolution_status == "retired_symbol_removed":
        return "bitget_symbol_removed_public_api"
    if resolution_status:
        return "standardized_tp1_vs_sl_on_15m"
    return "unknown"


def _normalize_trade_resolution_row(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return row
    normalized = dict(row)
    resolved_ts = _parse_any_timestamp(
        normalized.get("resolved_at")
        or normalized.get("resolved_at_ms")
        or normalized.get("exit_time")
        or normalized.get("first_hit_ts_ms")
        or normalized.get("placement_time")
        or normalized.get("decision_ts_ms")
    )
    if resolved_ts is not None:
        normalized["resolved_at"] = resolved_ts.isoformat()
        normalized["resolved_at_epoch_s"] = round(float(resolved_ts.timestamp()), 6)
        normalized["resolved_at_ms"] = int(resolved_ts.timestamp() * 1000)
    else:
        resolved_epoch = _safe_float(normalized.get("resolved_at"))
        if resolved_epoch is not None:
            normalized["resolved_at_epoch_s"] = round(float(resolved_epoch), 6)
            inferred = _parse_any_timestamp(resolved_epoch)
            if inferred is not None:
                normalized["resolved_at"] = inferred.isoformat()
                normalized["resolved_at_ms"] = int(inferred.timestamp() * 1000)
    exit_ts = _parse_any_timestamp(normalized.get("exit_time") or normalized.get("first_hit_ts_ms"))
    normalized["exit_time"] = exit_ts.isoformat() if exit_ts is not None else None
    outcome_bucket = str(normalized.get("outcome_bucket") or normalized.get("outcome") or "").strip()
    if outcome_bucket:
        normalized["outcome_bucket"] = outcome_bucket
        normalized["outcome"] = outcome_bucket
        normalized["resolution_outcome"] = outcome_bucket
    realized_r = _safe_float(normalized.get("realized_r"))
    normalized["realized_r"] = round(float(realized_r), 4) if realized_r is not None else None
    normalized["r_multiple"] = normalized.get("r_multiple")
    if normalized["r_multiple"] is None:
        normalized["r_multiple"] = normalized["realized_r"]
    normalized["resolved_r_multiple"] = normalized.get("resolved_r_multiple")
    if normalized["resolved_r_multiple"] is None:
        normalized["resolved_r_multiple"] = normalized["realized_r"]
    normalized["resolution_basis"] = _infer_trade_resolution_basis(normalized)
    normalized["exit_reason"] = str(normalized.get("exit_reason") or normalized.get("resolution_status") or "").strip() or None
    if normalized.get("resolution_note") in (None, ""):
        normalized["resolution_note"] = normalized["resolution_basis"]
    time_to_resolution_minutes = _safe_float(normalized.get("time_to_resolution_minutes"))
    if time_to_resolution_minutes is not None:
        normalized["time_to_resolution_minutes"] = round(float(time_to_resolution_minutes), 2)
    risk_quote_value = _safe_float(normalized.get("risk_quote_value"))
    if risk_quote_value is not None:
        normalized["risk_quote_value"] = round(float(risk_quote_value), 10)
    return normalized


def _build_trade_audit_resolution_key(row: dict[str, Any]):
    row = row or {}
    identity = _trade_audit_identity_key(row)
    if identity is not None:
        return ("trade_identity",) + identity
    return (
        str(row.get("record_id") or ""),
        str(row.get("audit_id") or ""),
        str(row.get("order_id") or ""),
        str(row.get("client_oid") or ""),
        str(row.get("symbol") or ""),
        str(row.get("primary_setup") or ""),
        str(row.get("direction") or ""),
        str(row.get("audit_status") or ""),
        str(row.get("resolution_status") or ""),
        str(row.get("decision_ts_ms") or ""),
    )


def _dedupe_trade_audit_resolved_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for raw_row in rows or []:
        row = _normalize_trade_resolution_row(raw_row)
        key = _build_trade_audit_resolution_key(row)
        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = row
            continue
        current_ts = _parse_any_timestamp(
            current.get("resolved_at")
            or current.get("exit_time")
            or current.get("placement_time")
            or current.get("decision_ts_ms")
        ) or pd.Timestamp.min.tz_localize("UTC")
        row_ts = _parse_any_timestamp(
            row.get("resolved_at")
            or row.get("exit_time")
            or row.get("placement_time")
            or row.get("decision_ts_ms")
        ) or pd.Timestamp.min.tz_localize("UTC")
        if row_ts >= current_ts:
            latest_by_key[key] = row
    return list(latest_by_key.values())


def _dedupe_trade_audit_event_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for raw_row in rows or []:
        if not isinstance(raw_row, dict):
            continue
        row = _prepare_payload("trade_audit", raw_row)
        identity = _trade_audit_identity_key(row)
        key = (("trade_identity",) + identity) if identity is not None else ("record_id", str(row.get("record_id") or ""))
        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = row
            continue
        current_ts = _parse_any_timestamp(
            current.get("recorded_at")
            or current.get("placement_time")
            or current.get("decision_ts_ms")
        ) or pd.Timestamp.min.tz_localize("UTC")
        row_ts = _parse_any_timestamp(
            row.get("recorded_at")
            or row.get("placement_time")
            or row.get("decision_ts_ms")
        ) or pd.Timestamp.min.tz_localize("UTC")
        if row_ts >= current_ts:
            latest_by_key[key] = row
    return list(latest_by_key.values())


def _repair_trade_audit_event_file() -> tuple[list[dict[str, Any]], bool]:
    raw_rows = _load_jsonl_rows(_TRADE_EVENTS_FILE)
    repaired_rows = _dedupe_trade_audit_event_rows(raw_rows)
    changed = repaired_rows != raw_rows
    if changed:
        _save_jsonl_rows(_TRADE_EVENTS_FILE, repaired_rows)
    return repaired_rows, changed


def _repair_trade_audit_resolved_file() -> tuple[list[dict[str, Any]], bool]:
    raw_rows = _load_jsonl_rows(_TRADE_RESOLVED_FILE)
    repaired_rows = _dedupe_trade_audit_resolved_rows(raw_rows)
    changed = repaired_rows != raw_rows
    if changed:
        _save_jsonl_rows(_TRADE_RESOLVED_FILE, repaired_rows)
    return repaired_rows, changed


def _extract_bitget_rows(response):
    if not isinstance(response, dict) or str(response.get("code")) != "00000":
        return []
    data = response.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("entrustedList", "fillList", "orderList", "planList", "dataList", "items"):
            rows = data.get(key)
            if isinstance(rows, list):
                return rows
        if data:
            return [data]
    return []


def _parse_fee_detail_total(fee_detail):
    payload = fee_detail
    if payload in (None, "", []):
        return 0.0
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return _safe_float(payload, 0.0) or 0.0
    if isinstance(payload, dict):
        for key in ("totalFee", "fee", "deductionFee", "newFees", "actualFee"):
            value = _safe_float(payload.get(key))
            if value is not None:
                return value
        return 0.0
    if isinstance(payload, list):
        return sum(_parse_fee_detail_total(item) for item in payload)
    return _safe_float(payload, 0.0) or 0.0


def _extract_exchange_row_size(row):
    for key in ("baseVolume", "size", "filledQty", "fillQty", "volume", "qty"):
        value = _safe_float((row or {}).get(key))
        if value is not None and value > 0:
            return value
    return 0.0


def _extract_exchange_row_price(row, fallback=None):
    for key in ("fillPrice", "price", "averagePrice", "avgPrice", "tradePrice", "priceAvg"):
        value = _safe_float((row or {}).get(key))
        if value is not None and value > 0:
            return value
    return _safe_float(fallback)


def _extract_exchange_row_timestamp(row):
    row = row or {}
    for key in ("cTime", "uTime", "fillTime", "time", "ts"):
        ts = _parse_any_timestamp(row.get(key))
        if ts is not None:
            return ts
    return None


def _aggregate_exchange_fill_rows(rows):
    rows = list(rows or [])
    total_size = 0.0
    weighted_price = 0.0
    gross_profit_quote = 0.0
    total_fees_quote = 0.0
    fill_ids = []
    trade_sides = set()
    first_ts = None
    last_ts = None

    for row in rows:
        size = _extract_exchange_row_size(row)
        price = _extract_exchange_row_price(row)
        if size > 0 and price is not None:
            total_size += size
            weighted_price += size * price
        gross_profit_quote += _safe_float((row or {}).get("profit"), 0.0) or 0.0
        total_fees_quote += _parse_fee_detail_total((row or {}).get("feeDetail"))
        fill_id = str((row or {}).get("tradeId") or (row or {}).get("fillId") or "").strip()
        if fill_id:
            fill_ids.append(fill_id)
        trade_side = str((row or {}).get("tradeSide") or "").strip().lower()
        if trade_side:
            trade_sides.add(trade_side)
        ts = _extract_exchange_row_timestamp(row)
        if ts is not None:
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)

    avg_price = (weighted_price / total_size) if total_size > 0 else None
    return {
        "fill_count": len(rows),
        "total_size": round(float(total_size), 10) if total_size > 0 else 0.0,
        "avg_price": round(float(avg_price), 10) if avg_price is not None else None,
        "gross_profit_quote": round(float(gross_profit_quote), 10),
        "total_fees_quote": round(float(total_fees_quote), 10),
        "net_realized_quote": round(float(gross_profit_quote + total_fees_quote), 10),
        "fill_ids": fill_ids,
        "trade_sides": sorted(trade_sides),
        "first_fill_time": first_ts.isoformat() if first_ts is not None else None,
        "last_fill_time": last_ts.isoformat() if last_ts is not None else None,
    }


def _normalize_exchange_order_state(state):
    token = str(state or "").strip().lower()
    if token in {"cancelled", "canceled", "cancel"}:
        return "cancelled"
    if token in {"filled", "full_fill", "fully_filled"}:
        return "filled"
    if token in {"partial_fill", "partially_filled", "partial-filled"}:
        return "partial_fill"
    if token in {"init", "new", "live", "open"}:
        return "open"
    return token or "unknown"


def _is_close_trade_side(trade_side):
    token = str(trade_side or "").strip().lower()
    if not token:
        return False
    if token in {"close"}:
        return True
    return (
        "close" in token
        or token.startswith("reduce_")
        or token.startswith("burst_")
        or token.startswith("delivery_")
        or token.startswith("off_close")
        or token.startswith("dte_sys_adl")
    )


def _exchange_get(request_path: str, params: dict[str, Any]):
    try:
        return api_client.make_request("GET", request_path, params=params)
    except Exception as exc:
        logger.warning(f"Exchange GET failed for {request_path}: {exc}")
        return None


def _fetch_exchange_order_detail(record):
    symbol = _normalize_bitget_symbol(record.get("symbol"))
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}
    order_id = str(record.get("order_id") or "").strip()
    client_oid = str(record.get("client_oid") or "").strip()
    if order_id:
        params["orderId"] = order_id
    elif client_oid:
        params["clientOid"] = client_oid
    else:
        return None
    return _exchange_get("/api/v2/mix/order/detail", params)


def _fetch_exchange_order_history(record, *, order_id=None, start_ms=None, end_ms=None, limit=None):
    symbol = _normalize_bitget_symbol(record.get("symbol"))
    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "limit": str(limit or int(SIGNAL_CONFIG.get("live_trade_reconciliation_fill_limit", 100) or 100)),
    }
    resolved_order_id = str(order_id or "").strip()
    if resolved_order_id:
        params["orderId"] = resolved_order_id
    else:
        if start_ms is not None:
            params["startTime"] = str(int(start_ms))
        if end_ms is not None:
            params["endTime"] = str(int(end_ms))
    return _exchange_get("/api/v2/mix/order/orders-history", params)


def _fetch_exchange_order_fills(record, *, order_id=None, start_ms=None, end_ms=None, limit=None):
    symbol = _normalize_bitget_symbol(record.get("symbol"))
    params = {
        "symbol": symbol,
        "productType": "USDT-FUTURES",
        "limit": str(limit or int(SIGNAL_CONFIG.get("live_trade_reconciliation_fill_limit", 100) or 100)),
    }
    resolved_order_id = str(order_id or "").strip()
    if resolved_order_id:
        params["orderId"] = resolved_order_id
    else:
        if start_ms is not None:
            params["startTime"] = str(int(start_ms))
        if end_ms is not None:
            params["endTime"] = str(int(end_ms))
    return _exchange_get("/api/v2/mix/order/fills", params)


def _infer_exchange_exit_reason(record, close_fill_agg, close_order_row):
    exit_price = _safe_float(close_fill_agg.get("avg_price")) or _extract_exchange_row_price(close_order_row)
    stop_loss = _safe_float(record.get("stop_loss"))
    take_profit_2 = _safe_float(record.get("take_profit_2"))
    risk = abs((_safe_float(record.get("entry_price"), 0.0) or 0.0) - (stop_loss or 0.0))
    tolerance = risk * float(SIGNAL_CONFIG.get("live_trade_reconciliation_exit_match_tolerance_risk_fraction", 0.2) or 0.2)

    if exit_price is not None and stop_loss is not None and abs(exit_price - stop_loss) <= max(tolerance, 1e-9):
        return "stop_loss_closed"
    if exit_price is not None and take_profit_2 is not None and abs(exit_price - take_profit_2) <= max(tolerance, 1e-9):
        return "take_profit_closed"
    if _safe_float(close_fill_agg.get("net_realized_quote"), 0.0) > 0:
        return "profitable_exchange_close"
    if _safe_float(close_fill_agg.get("net_realized_quote"), 0.0) < 0:
        return "losing_exchange_close"
    close_trade_side = str((close_order_row or {}).get("tradeSide") or "").strip().lower()
    if close_trade_side:
        return f"exchange_close_{close_trade_side}"
    return "exchange_close_fill"


def _resolve_record_from_exchange(record: dict[str, Any]):
    if not bool(SIGNAL_CONFIG.get("enable_exchange_exact_trade_reconciliation", True)):
        return None
    placement_ts = _parse_any_timestamp(record.get("decision_candle_ts_ms") or record.get("decision_ts_ms") or record.get("timestamp"))
    if placement_ts is None:
        return None

    lookback_hours = float(SIGNAL_CONFIG.get("live_trade_reconciliation_lookback_hours", 96) or 96)
    start_ms = int((placement_ts - pd.Timedelta(hours=lookback_hours)).timestamp() * 1000)
    end_ms = _now_ms()

    order_detail_resp = _fetch_exchange_order_detail(record)
    order_detail_rows = _extract_bitget_rows(order_detail_resp)
    order_detail = dict(order_detail_rows[0]) if order_detail_rows else {}
    entry_state = _normalize_exchange_order_state(order_detail.get("state"))

    entry_fill_resp = _fetch_exchange_order_fills(record, order_id=record.get("order_id"), start_ms=start_ms, end_ms=end_ms)
    entry_fill_rows = _extract_bitget_rows(entry_fill_resp)
    entry_fill_agg = _aggregate_exchange_fill_rows(entry_fill_rows)

    if entry_fill_agg["total_size"] <= 0 and entry_state == "cancelled":
        return _normalize_trade_resolution_row({
            **record,
            "resolved_at": _utc_now_iso(),
            "resolved_at_ms": end_ms,
            "resolution_status": "order_cancelled_unfilled",
            "resolution_note": "exchange_unfilled_cancellation",
            "resolution_basis": "exchange_unfilled_cancellation",
            "outcome_bucket": "not_filled",
            "realized_r": None,
            "max_favorable_r": None,
            "max_adverse_r": None,
            "first_hit_ts_ms": None,
            "exit_time": None,
            "observed_candles": None,
            "time_to_resolution_minutes": None,
            "would_have_won": False,
            "would_have_lost": False,
            "exchange_entry_state": entry_state,
            "exchange_entry_fill_count": entry_fill_agg.get("fill_count"),
            "exchange_entry_order_detail": order_detail,
        })

    symbol_fill_resp = _fetch_exchange_order_fills(record, start_ms=start_ms, end_ms=end_ms)
    symbol_fill_rows = _extract_bitget_rows(symbol_fill_resp)
    close_fill_rows = []
    record_order_id = str(record.get("order_id") or "").strip()
    for row in symbol_fill_rows:
        row_ts = _extract_exchange_row_timestamp(row)
        if row_ts is None or row_ts < placement_ts:
            continue
        row_order_id = str((row or {}).get("orderId") or "").strip()
        if record_order_id and row_order_id == record_order_id:
            continue
        if _is_close_trade_side((row or {}).get("tradeSide")):
            close_fill_rows.append(row)

    if not close_fill_rows:
        return None

    close_groups = defaultdict(list)
    for row in close_fill_rows:
        close_groups[str((row or {}).get("orderId") or "unknown")].append(row)

    ranked_close_groups = []
    for order_id, rows in close_groups.items():
        first_ts = min((_extract_exchange_row_timestamp(row) for row in rows), default=None)
        ranked_close_groups.append((first_ts or pd.Timestamp.max.tz_localize("UTC"), order_id, rows))
    ranked_close_groups.sort(key=lambda item: item[0])
    _, close_order_id, selected_close_rows = ranked_close_groups[0]
    close_fill_agg = _aggregate_exchange_fill_rows(selected_close_rows)

    close_order_history_resp = _fetch_exchange_order_history(record, order_id=close_order_id, start_ms=start_ms, end_ms=end_ms)
    close_order_rows = _extract_bitget_rows(close_order_history_resp)
    close_order_row = dict(close_order_rows[0]) if close_order_rows else {}
    exit_price_exact = _safe_float(close_fill_agg.get("avg_price")) or _extract_exchange_row_price(close_order_row)

    entry_price_exact = (
        _safe_float(entry_fill_agg.get("avg_price"))
        or _extract_exchange_row_price(order_detail)
        or _safe_float(record.get("actual_fill_price"))
        or _safe_float(record.get("entry_price"))
    )
    entry_size_exact = (
        _safe_float(entry_fill_agg.get("total_size"))
        or _safe_float(order_detail.get("baseVolume"))
        or _safe_float(order_detail.get("size"))
        or _safe_float(record.get("size"))
        or 0.0
    )
    stop_loss = _safe_float(record.get("stop_loss"), 0.0) or 0.0
    risk_quote_value = abs((entry_price_exact or 0.0) - stop_loss) * entry_size_exact if entry_price_exact and stop_loss and entry_size_exact else None

    gross_profit_quote = _safe_float(close_fill_agg.get("gross_profit_quote"), 0.0) or 0.0
    total_fees_quote = (_safe_float(entry_fill_agg.get("total_fees_quote"), 0.0) or 0.0) + (_safe_float(close_fill_agg.get("total_fees_quote"), 0.0) or 0.0)
    net_realized_quote = gross_profit_quote + total_fees_quote

    realized_r = None
    if risk_quote_value and abs(risk_quote_value) > 1e-12:
        realized_r = round(float(net_realized_quote / risk_quote_value), 4)

    flat_tol = float(SIGNAL_CONFIG.get("live_trade_reconciliation_flat_pnl_tolerance_quote", 0.01) or 0.01)
    if net_realized_quote > flat_tol:
        outcome_bucket = "winner"
    elif net_realized_quote < -flat_tol:
        outcome_bucket = "loser"
    else:
        outcome_bucket = "flat"

    exit_reason = _infer_exchange_exit_reason(record, close_fill_agg, close_order_row)
    exit_time = _parse_any_timestamp(close_fill_agg.get("last_fill_time")) or _extract_exchange_row_timestamp(close_order_row)
    time_to_resolution_minutes = None
    if exit_time is not None:
        time_to_resolution_minutes = round((exit_time.timestamp() * 1000 - placement_ts.timestamp() * 1000) / 60000.0, 2)

    return _normalize_trade_resolution_row({
        **record,
        "resolved_at": _utc_now_iso(),
        "resolved_at_ms": end_ms,
        "resolution_status": exit_reason,
        "resolution_note": "exchange_fills_reconciled",
        "resolution_basis": "exchange_fills_reconciled",
        "outcome_bucket": outcome_bucket,
        "realized_r": realized_r,
        "max_favorable_r": None,
        "max_adverse_r": None,
        "first_hit_ts_ms": int(exit_time.timestamp() * 1000) if exit_time is not None else None,
        "exit_time": exit_time.isoformat() if exit_time is not None else None,
        "observed_candles": None,
        "time_to_resolution_minutes": time_to_resolution_minutes,
        "would_have_won": outcome_bucket == "winner",
        "would_have_lost": outcome_bucket == "loser",
        "entry_price": entry_price_exact,
        "size": entry_size_exact or record.get("size"),
        "risk_quote_value": round(float(risk_quote_value), 10) if risk_quote_value is not None else None,
        "exchange_entry_state": entry_state,
        "exchange_entry_fill_count": entry_fill_agg.get("fill_count"),
        "exchange_entry_avg_price": entry_fill_agg.get("avg_price"),
        "exchange_entry_fees_quote": entry_fill_agg.get("total_fees_quote"),
        "exchange_entry_fill_ids": entry_fill_agg.get("fill_ids"),
        "exchange_exit_order_id": close_order_id,
        "exchange_exit_fill_count": close_fill_agg.get("fill_count"),
        "exchange_exit_avg_price": exit_price_exact,
        "exchange_exit_trade_sides": close_fill_agg.get("trade_sides"),
        "exchange_exit_fill_ids": close_fill_agg.get("fill_ids"),
        "exchange_gross_profit_quote": round(float(gross_profit_quote), 10),
        "exchange_total_fees_quote": round(float(total_fees_quote), 10),
        "exchange_net_realized_quote": round(float(net_realized_quote), 10),
    })


def _choose_level_hit(candle_open: float, candidates: list[tuple[str, float]]) -> tuple[str, float]:
    if not candidates:
        return None, None
    chosen = min(candidates, key=lambda item: abs(candle_open - item[1]))
    return chosen


def _resolve_hit_status(direction: str, candle: dict[str, Any], stop_loss: float, tp1: float | None, tp2: float | None):
    candle_open = _safe_float(candle.get("open"), 0.0)
    candle_high = _safe_float(candle.get("high"), 0.0)
    candle_low = _safe_float(candle.get("low"), 0.0)

    if direction == "BUY":
        hit_stop = candle_low <= stop_loss if stop_loss is not None else False
        hit_tp1 = candle_high >= tp1 if tp1 is not None else False
        hit_tp2 = candle_high >= tp2 if tp2 is not None else False
    else:
        hit_stop = candle_high >= stop_loss if stop_loss is not None else False
        hit_tp1 = candle_low <= tp1 if tp1 is not None else False
        hit_tp2 = candle_low <= tp2 if tp2 is not None else False

    if hit_tp2:
        hit_tp1 = True

    hits = []
    if hit_stop:
        hits.append(("stop_loss_hit", stop_loss))
    if hit_tp2 and tp2 is not None:
        hits.append(("take_profit_2_hit", tp2))
    elif hit_tp1 and tp1 is not None:
        hits.append(("take_profit_1_hit", tp1))

    if len(hits) == 1:
        return hits[0][0], None
    if len(hits) > 1:
        chosen_status, _ = _choose_level_hit(candle_open, hits)
        return chosen_status, "intra_candle_collision_open_distance"
    return None, None


def _candle_limit_for_horizon(hours: float) -> int:
    interval = str(SIGNAL_CONFIG.get("candlestick_interval", "15m")).strip().lower()
    minutes = 15
    if interval.endswith("m"):
        minutes = max(int(interval[:-1] or 15), 1)
    elif interval.endswith("h"):
        minutes = max(int(interval[:-1] or 1), 1) * 60
    candles = int((hours * 60) / minutes) + 20
    return min(max(candles, 50), 1000)


def _interval_ms() -> int:
    interval = str(SIGNAL_CONFIG.get("candlestick_interval", "15m")).strip().lower()
    minutes = 15
    if interval.endswith("m"):
        minutes = max(int(interval[:-1] or 15), 1)
    elif interval.endswith("h"):
        minutes = max(int(interval[:-1] or 1), 1) * 60
    return minutes * 60 * 1000


def _fetch_resolution_candles(symbol: str, start_ms: int, horizon_hours: float):
    try:
        interval = SIGNAL_CONFIG.get("candlestick_interval", "15m")
        interval_ms = _interval_ms()
        batch_limit = min(int(SIGNAL_CONFIG.get("live_intelligence_candle_batch_limit", 200) or 200), 200)
        fetch_start = max(int(start_ms - 60 * 60 * 1000), 0)
        fetch_end = min(_now_ms(), int(start_ms + (horizon_hours * 3600 * 1000)))

        rows_by_ts = {}
        cursor = fetch_start
        max_batches = 12

        for _ in range(max_batches):
            if cursor >= fetch_end:
                break
            batch = get_candlestick_data(
                symbol,
                interval,
                limit=batch_limit,
                start_time=cursor,
                end_time=fetch_end,
            ) or []
            if not batch:
                break
            last_ts = None
            for candle in batch:
                ts = _safe_int(candle.get("timestamp"))
                if ts is None:
                    continue
                rows_by_ts[ts] = candle
                last_ts = ts
            if last_ts is None:
                break
            next_cursor = last_ts + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(batch) < batch_limit:
                break

        return [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
    except Exception as exc:
        logger.warning(f"Could not fetch telemetry candles for {symbol}: {exc}")
        return []


def _probe_symbol_removal_status(symbol: str, interval: str | None = None):
    symbol = _safe_text(symbol, "").strip()
    if not symbol:
        return {"removed": False, "code": "", "message": "missing_symbol"}

    try:
        response = requests.get(
            f"{BITGET_API_URL.rstrip('/')}/api/v2/mix/market/history-candles",
            params={
                "symbol": symbol,
                "granularity": interval or SIGNAL_CONFIG.get("candlestick_interval", "15m"),
                "limit": "1",
                "productType": "USDT-FUTURES",
            },
            timeout=float(SIGNAL_CONFIG.get("public_api_timeout_seconds", 5.0) or 5.0),
        )
        payload = response.json() if response.content else {}
    except Exception as exc:
        return {"removed": False, "code": "", "message": str(exc)}

    code = str((payload or {}).get("code") or "")
    message = str((payload or {}).get("msg") or "")
    removed = code == "40309" or "symbol has been removed" in message.lower()
    return {
        "removed": bool(removed),
        "code": code,
        "message": message,
        "payload": payload if isinstance(payload, dict) else {},
    }


def _build_removed_symbol_resolution(record: dict[str, Any], note: str):
    now_ms = _now_ms()
    return _normalize_trade_resolution_row({
        **record,
        "resolved_at": _utc_now_iso(),
        "resolved_at_ms": now_ms,
        "resolution_status": "retired_symbol_removed",
        "resolution_note": note,
        "resolution_basis": "bitget_symbol_removed_public_api",
        "outcome_bucket": "expired",
        "realized_r": 0.0,
        "max_favorable_r": 0.0,
        "max_adverse_r": 0.0,
        "first_hit_ts_ms": None,
        "exit_time": None,
        "observed_candles": 0,
        "time_to_resolution_minutes": None,
        "would_have_won": False,
        "would_have_lost": False,
    })


def _resolve_record(record: dict[str, Any], candles: list[dict[str, Any]]):
    direction = _safe_text(record.get("direction"), "")
    entry = _safe_float(record.get("entry_price"))
    stop_loss = _safe_float(record.get("stop_loss"))
    tp1 = _safe_float(record.get("take_profit_1"))
    tp2 = _safe_float(record.get("take_profit_2"))
    risk = _safe_float(record.get("risk_per_unit"))
    start_ms = _safe_int(record.get("decision_candle_ts_ms")) or _safe_int(record.get("decision_ts_ms"))
    horizon_hours = float(SIGNAL_CONFIG.get("live_intelligence_horizon_hours", 72) or 72)
    horizon_ms = int(horizon_hours * 3600 * 1000)
    deadline_ms = (start_ms or _now_ms()) + horizon_ms
    now_ms = _now_ms()

    if direction not in {"BUY", "SELL"} or entry is None or stop_loss is None or risk in (None, 0):
        return None

    eligible_candles = [
        candle for candle in candles
        if _safe_int(candle.get("timestamp"), 0) > (start_ms or 0)
        and _safe_int(candle.get("timestamp"), 0) <= min(now_ms, deadline_ms)
    ]

    if not eligible_candles:
        if now_ms < deadline_ms:
            return None
        return _normalize_trade_resolution_row({
            **record,
            "resolved_at": _utc_now_iso(),
            "resolved_at_ms": now_ms,
            "resolution_status": "expired_unresolved",
            "resolution_note": "horizon_expired_without_level_hit",
            "resolution_basis": "standardized_tp1_vs_sl_on_15m",
            "outcome_bucket": "expired",
            "realized_r": 0.0,
            "max_favorable_r": 0.0,
            "max_adverse_r": 0.0,
            "first_hit_ts_ms": None,
            "exit_time": None,
            "observed_candles": 0,
            "time_to_resolution_minutes": None,
            "would_have_won": False,
            "would_have_lost": False,
        })

    max_favorable_r = 0.0
    max_adverse_r = 0.0
    resolution_status = None
    resolution_note = None
    first_hit_ts_ms = None

    for candle in eligible_candles:
        candle_high = _safe_float(candle.get("high"), entry)
        candle_low = _safe_float(candle.get("low"), entry)
        if direction == "BUY":
            max_favorable_r = max(max_favorable_r, max(0.0, (candle_high - entry) / risk))
            max_adverse_r = max(max_adverse_r, max(0.0, (entry - candle_low) / risk))
        else:
            max_favorable_r = max(max_favorable_r, max(0.0, (entry - candle_low) / risk))
            max_adverse_r = max(max_adverse_r, max(0.0, (candle_high - entry) / risk))

        resolution_status, resolution_note = _resolve_hit_status(direction, candle, stop_loss, tp1, tp2)
        if resolution_status:
            first_hit_ts_ms = _safe_int(candle.get("timestamp"))
            break

    if not resolution_status:
        if now_ms < deadline_ms:
            return None
        resolution_status = "expired_unresolved"
        resolution_note = "horizon_expired_without_level_hit"

    realized_r = 0.0
    outcome_bucket = "expired"
    if resolution_status == "stop_loss_hit":
        realized_r = -1.0
        outcome_bucket = "loser"
    elif resolution_status == "take_profit_1_hit":
        realized_r = _safe_float(record.get("tp1_r"), 0.0) or 0.0
        outcome_bucket = "winner"
    elif resolution_status == "take_profit_2_hit":
        realized_r = _safe_float(record.get("tp2_r"), _safe_float(record.get("tp1_r"), 0.0)) or 0.0
        outcome_bucket = "winner"

    time_to_resolution_minutes = None
    if first_hit_ts_ms and start_ms:
        time_to_resolution_minutes = round((first_hit_ts_ms - start_ms) / 60000.0, 2)

    return _normalize_trade_resolution_row({
        **record,
        "resolved_at": _utc_now_iso(),
        "resolved_at_ms": now_ms,
        "resolution_status": resolution_status,
        "resolution_note": resolution_note,
        "resolution_basis": "standardized_tp1_vs_sl_on_15m",
        "outcome_bucket": outcome_bucket,
        "realized_r": round(float(realized_r), 4),
        "max_favorable_r": round(float(max_favorable_r), 4),
        "max_adverse_r": round(float(max_adverse_r), 4),
        "first_hit_ts_ms": first_hit_ts_ms,
        "exit_time": _parse_any_timestamp(first_hit_ts_ms).isoformat() if first_hit_ts_ms is not None else None,
        "observed_candles": len(eligible_candles),
        "time_to_resolution_minutes": time_to_resolution_minutes,
        "would_have_won": outcome_bucket == "winner",
        "would_have_lost": outcome_bucket == "loser",
    })


def _resolve_pending_stream(pending_store: dict[str, dict[str, Any]], pending_file: str, resolved_file: str, symbols: list[str]):
    resolved_total = 0
    for symbol in symbols:
        symbol_records = [
            row for row in pending_store.values()
            if str(row.get("symbol")) == str(symbol) and row.get("trackable")
        ]
        if not symbol_records:
            continue
        candle_records = [row for row in symbol_records if row.get("resolution_mode") == "candle"]
        start_ms = min(_safe_int(row.get("decision_candle_ts_ms"), _now_ms()) for row in candle_records) if candle_records else None
        candles = []
        removed_status = None
        if start_ms is not None:
            candles = _fetch_resolution_candles(symbol, start_ms, float(SIGNAL_CONFIG.get("live_intelligence_horizon_hours", 72) or 72))
            if not candles:
                removed_status = _probe_symbol_removal_status(symbol)
        for record_id, record in list(pending_store.items()):
            if str(record.get("symbol")) != str(symbol):
                continue
            if record.get("resolution_mode") == "exchange":
                resolved = _resolve_record_from_exchange(record)
            elif removed_status and removed_status.get("removed"):
                detail = _safe_text(removed_status.get("message"), "The symbol has been removed")
                code = _safe_text(removed_status.get("code"), "")
                note = detail if not code else f"{code}: {detail}"
                resolved = _build_removed_symbol_resolution(record, note)
            else:
                resolved = _resolve_record(record, candles)
            if not resolved:
                continue
            resolved = _normalize_trade_resolution_row(resolved)
            pending_store.pop(record_id, None)
            _append_jsonl(resolved_file, resolved)
            resolved_total += 1
    _save_json_file(pending_file, pending_store)
    return resolved_total


def _select_rejected_shadow_symbols(all_symbols: list[str]):
    global _telemetry_cycle_counter, _rejected_rotation_offset

    cadence = max(1, int(SIGNAL_CONFIG.get("live_rejected_shadow_refresh_every_cycles", 6) or 6))
    batch_cap = max(0, int(SIGNAL_CONFIG.get("live_rejected_shadow_refresh_max_symbols", 40) or 40))
    refresh_due = bool(all_symbols) and batch_cap > 0 and ((_telemetry_cycle_counter - 1) % cadence == 0)
    if not refresh_due:
        return [], False
    if batch_cap >= len(all_symbols):
        _rejected_rotation_offset = 0
        return list(all_symbols), True

    start = _rejected_rotation_offset % len(all_symbols)
    ordered = all_symbols[start:] + all_symbols[:start]
    selected = ordered[:batch_cap]
    _rejected_rotation_offset = (start + len(selected)) % len(all_symbols)
    return selected, True


def _compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    winners = sum(1 for row in rows if str(row.get("outcome_bucket")) == "winner")
    losers = sum(1 for row in rows if str(row.get("outcome_bucket")) == "loser")
    expired = sum(1 for row in rows if str(row.get("outcome_bucket")) == "expired")
    realized = [_safe_float(row.get("realized_r"), 0.0) or 0.0 for row in rows]
    favorable = [_safe_float(row.get("max_favorable_r"), 0.0) or 0.0 for row in rows]
    adverse = [_safe_float(row.get("max_adverse_r"), 0.0) or 0.0 for row in rows]
    time_to_resolve = [
        _safe_float(row.get("time_to_resolution_minutes"))
        for row in rows
        if _safe_float(row.get("time_to_resolution_minutes")) is not None
    ]
    return {
        "resolved_trades": total,
        "winner_count": winners,
        "loser_count": losers,
        "expired_count": expired,
        "win_rate_pct": round((winners / total * 100.0), 2) if total else 0.0,
        "net_r": round(float(sum(realized)), 4),
        "avg_realized_r": round(float(sum(realized) / total), 4) if total else 0.0,
        "avg_mfe_r": round(float(sum(favorable) / total), 4) if total else 0.0,
        "avg_mae_r": round(float(sum(adverse) / total), 4) if total else 0.0,
        "avg_time_to_resolution_min": round(float(sum(time_to_resolve) / len(time_to_resolve)), 2) if time_to_resolve else None,
    }


def _is_live_placed_trade_row(row: dict[str, Any]) -> bool:
    return str((row or {}).get("audit_status") or "") in {"placed", "placed_with_plan_fallback"}


def _group_metrics(rows: list[dict[str, Any]], group_fields: list[str]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return []
    for field in group_fields:
        if field not in frame.columns:
            frame[field] = "unknown"
        frame[field] = frame[field].fillna("unknown")
    payload = []
    grouped = frame.groupby(group_fields, dropna=False)
    for keys, group in grouped:
        key_values = keys if isinstance(keys, tuple) else (keys,)
        row = {field: value for field, value in zip(group_fields, key_values)}
        row.update(_compute_metrics(group.to_dict("records")))
        payload.append(row)
    payload.sort(key=lambda item: (item.get("net_r", 0.0), item.get("resolved_trades", 0)), reverse=True)
    return payload


def _top_and_bottom(rows: list[dict[str, Any]], *, limit: int = 20):
    ordered = sorted(rows, key=lambda item: (item.get("net_r", 0.0), item.get("resolved_trades", 0)), reverse=True)
    return ordered[:limit], list(reversed(ordered[-limit:])) if ordered else []


def _build_path_scorecards(accepted_rows, rejected_rows, trade_rows):
    accepted_paths = _group_metrics(
        accepted_rows,
        ["primary_setup", "direction", "market_regime"],
    )
    rejected_paths = _group_metrics(
        rejected_rows,
        ["rejection_stage", "reason_code", "primary_setup", "direction", "market_regime"],
    )
    trade_paths = _group_metrics(
        trade_rows,
        ["audit_status", "audit_reason_code", "primary_setup", "direction", "market_regime"],
    )
    best_accepted, worst_accepted = _top_and_bottom(accepted_paths)
    best_rejected_killers, _ = _top_and_bottom(rejected_paths)
    best_trade_killers, _ = _top_and_bottom(trade_paths)
    return {
        "generated_at": time.time(),
        "accepted_by_path": accepted_paths[:100],
        "rejected_counterfactual_by_reason": best_rejected_killers[:100],
        "trade_counterfactual_by_status": best_trade_killers[:100],
        "best_accepted_paths": best_accepted,
        "worst_accepted_paths": worst_accepted,
    }


def _build_context_scorecards(accepted_rows, rejected_rows, trade_rows):
    return {
        "generated_at": time.time(),
        "accepted_by_context": _group_metrics(
            accepted_rows,
            ["market_regime", "primary_setup", "direction"],
        )[:100],
        "rejected_would_have_won_by_context": _group_metrics(
            [row for row in rejected_rows if row.get("would_have_won")],
            ["market_regime", "primary_setup", "reason_code"],
        )[:100],
        "execution_missed_by_context": _group_metrics(
            [row for row in trade_rows if row.get("audit_status") != "placed"],
            ["market_regime", "primary_setup", "audit_reason_code"],
        )[:100],
    }


def _build_symbol_quality(accepted_rows, rejected_rows, trade_rows):
    symbols = sorted({
        str(row.get("symbol"))
        for row in accepted_rows + rejected_rows + trade_rows
        if str(row.get("symbol") or "").strip()
    })
    by_symbol = []
    for symbol in symbols:
        accepted = [row for row in accepted_rows if str(row.get("symbol")) == symbol]
        rejected = [row for row in rejected_rows if str(row.get("symbol")) == symbol]
        trade = [row for row in trade_rows if str(row.get("symbol")) == symbol]
        payload = {"symbol": symbol}
        payload.update({f"accepted_{k}": v for k, v in _compute_metrics(accepted).items()})
        payload.update({f"rejected_{k}": v for k, v in _compute_metrics(rejected).items()})
        payload.update({f"trade_{k}": v for k, v in _compute_metrics(trade).items()})
        payload["rejected_winner_count"] = sum(1 for row in rejected if row.get("would_have_won"))
        payload["trade_missed_winner_count"] = sum(
            1 for row in trade
            if row.get("audit_status") != "placed" and row.get("would_have_won")
        )
        payload["total_missed_counterfactual_r"] = round(
            float(
                sum(_safe_float(row.get("realized_r"), 0.0) or 0.0 for row in rejected)
                + sum(
                    _safe_float(row.get("realized_r"), 0.0) or 0.0
                    for row in trade
                    if row.get("audit_status") != "placed"
                )
            ),
            4,
        )
        by_symbol.append(payload)
    by_symbol.sort(
        key=lambda item: (
            item.get("total_missed_counterfactual_r", 0.0),
            item.get("accepted_net_r", 0.0),
        ),
        reverse=True,
    )
    return {"generated_at": time.time(), "by_symbol": by_symbol[:100]}


def _build_killer_report(accepted_rows, rejected_rows, trade_rows):
    rejection_groups = _group_metrics(rejected_rows, ["rejection_stage", "reason_code", "primary_setup", "direction"])
    execution_groups = _group_metrics(
        [row for row in trade_rows if row.get("audit_status") != "placed"],
        ["audit_status", "audit_reason_code", "primary_setup", "direction"],
    )
    accepted_groups = _group_metrics(accepted_rows, ["primary_setup", "direction", "market_regime"])

    rejection_groups.sort(key=lambda item: (item.get("net_r", 0.0), item.get("winner_count", 0)), reverse=True)
    execution_groups.sort(key=lambda item: (item.get("net_r", 0.0), item.get("winner_count", 0)), reverse=True)
    accepted_underperformers = sorted(
        accepted_groups,
        key=lambda item: (item.get("net_r", 0.0), -item.get("resolved_trades", 0)),
    )
    helpful_guardrails = sorted(rejection_groups, key=lambda item: (item.get("net_r", 0.0), item.get("resolved_trades", 0)))

    return {
        "generated_at": time.time(),
        "top_rejection_killers": rejection_groups[:50],
        "top_execution_killers": execution_groups[:50],
        "accepted_underperformers": accepted_underperformers[:50],
        "guardrails_saving_losses": helpful_guardrails[:50],
    }


def _build_gate_evidence(accepted_rows, rejected_rows):
    accepted_groups = _group_metrics(accepted_rows, ["primary_setup", "direction", "market_regime"])
    rejected_groups = _group_metrics(
        rejected_rows,
        ["reason_code", "primary_setup", "direction", "market_regime"],
    )
    return {
        "generated_at": time.time(),
        "accepted_live_contexts": accepted_groups[:100],
        "rejection_reason_evidence": rejected_groups[:100],
    }


def _build_health_report(accepted_rows, rejected_rows, trade_rows):
    accepted_metrics = _compute_metrics(accepted_rows)
    rejected_metrics = _compute_metrics(rejected_rows)
    trade_metrics = _compute_metrics(trade_rows)
    missed_exec_rows = [row for row in trade_rows if row.get("audit_status") != "placed"]
    missed_exec_metrics = _compute_metrics(missed_exec_rows)
    killer_report = _build_killer_report(accepted_rows, rejected_rows, trade_rows)
    top_rejection_killer = (killer_report.get("top_rejection_killers") or [{}])[0]
    top_execution_killer = (killer_report.get("top_execution_killers") or [{}])[0]
    top_underperformer = (killer_report.get("accepted_underperformers") or [{}])[0]
    return {
        "generated_at": time.time(),
        "accepted_pending": len(_accepted_pending),
        "rejected_pending": len(_rejected_pending),
        "trade_pending": len(_trade_pending),
        "accepted_metrics": accepted_metrics,
        "rejected_counterfactual_metrics": rejected_metrics,
        "trade_metrics": trade_metrics,
        "missed_execution_metrics": missed_exec_metrics,
        "rejected_would_have_won_count": sum(1 for row in rejected_rows if row.get("would_have_won")),
        "missed_execution_winner_count": sum(1 for row in missed_exec_rows if row.get("would_have_won")),
        "top_rejection_killer": top_rejection_killer,
        "top_execution_killer": top_execution_killer,
        "top_accepted_underperformer": top_underperformer,
    }


def _build_trade_only_health_report(placed_rows, placed_pending_rows):
    metrics = _compute_metrics(placed_rows)
    by_path = _group_metrics(
        placed_rows,
        ["primary_setup", "direction", "market_regime"],
    )
    top_path = by_path[0] if by_path else {}
    return {
        "generated_at": time.time(),
        "refresh_scope": "placed_trade_only",
        "accepted_pending": 0,
        "rejected_pending": 0,
        "trade_pending": len(placed_pending_rows),
        "placed_trade_metrics": metrics,
        "placed_trade_pending_count": len(placed_pending_rows),
        "placed_trade_resolved_count": len(placed_rows),
        "top_placed_path": top_path,
    }


def _save_reports():
    rejected_rows = _load_jsonl_rows(_REJECTED_RESOLVED_FILE)
    trade_rows = _load_jsonl_rows(_TRADE_RESOLVED_FILE)
    accepted_rows = [
        row for row in trade_rows
        if str(row.get("audit_status")) in {"placed", "placed_with_plan_fallback"}
    ]

    path_payload = _build_path_scorecards(accepted_rows, rejected_rows, trade_rows)
    context_payload = _build_context_scorecards(accepted_rows, rejected_rows, trade_rows)
    symbol_payload = _build_symbol_quality(accepted_rows, rejected_rows, trade_rows)
    killer_payload = _build_killer_report(accepted_rows, rejected_rows, trade_rows)
    health_payload = _build_health_report(accepted_rows, rejected_rows, trade_rows)
    gate_payload = _build_gate_evidence(accepted_rows, rejected_rows)

    _save_json_file(_PATH_SCORECARDS_FILE, path_payload)
    _save_json_file(_CONTEXT_SCORECARDS_FILE, context_payload)
    _save_json_file(_SYMBOL_QUALITY_FILE, symbol_payload)
    _save_json_file(_KILLER_REPORT_FILE, killer_payload)
    _save_json_file(_HEALTH_REPORT_FILE, health_payload)
    _save_json_file(_GATE_EVIDENCE_FILE, gate_payload)
    _save_json_file(
        _CONFIG_SNAPSHOT_FILE,
        {
            "generated_at": time.time(),
            "live_intelligence_enabled": bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)),
            "live_intelligence_horizon_hours": float(SIGNAL_CONFIG.get("live_intelligence_horizon_hours", 72) or 72),
            "live_intelligence_refresh_scope": "all_pending_symbols",
            "quality_gate_enabled": bool(SIGNAL_CONFIG.get("enable_quality_gate", False)),
            "bucket_filter_enabled": bool(SIGNAL_CONFIG.get("enable_bucket_filter", False)),
            "precision_profile_enabled": bool(SIGNAL_CONFIG.get("enable_precision_profile", False)),
            "min_signal_score": SIGNAL_CONFIG.get("min_signal_score"),
            "min_htf_confluence": SIGNAL_CONFIG.get("min_htf_confluence"),
            "require_funding_alignment": SIGNAL_CONFIG.get("require_funding_alignment"),
        },
    )
    return health_payload


def _save_trade_only_reports():
    trade_rows = _dedupe_trade_audit_resolved_rows(_load_jsonl_rows(_TRADE_RESOLVED_FILE))
    placed_rows = [row for row in trade_rows if _is_live_placed_trade_row(row)]
    placed_pending_rows = [row for row in _trade_pending.values() if _is_live_placed_trade_row(row)]
    health_payload = _build_trade_only_health_report(placed_rows, placed_pending_rows)
    summary_payload = {
        "updated_at": time.time(),
        "refresh_scope": "placed_trade_only",
        "pending_count": len(placed_pending_rows),
        "resolved_count": len(placed_rows),
        "by_outcome": dict(
            sorted(Counter(str(row.get("outcome_bucket") or "unknown") for row in placed_rows).items())
        ),
        "by_resolution_basis": dict(
            sorted(Counter(str(row.get("resolution_basis") or "unknown") for row in placed_rows).items())
        ),
        "exact_reconciled_count": sum(
            1 for row in placed_rows if str(row.get("resolution_basis") or "").startswith("exchange_")
        ),
        "metrics": _compute_metrics(placed_rows),
    }
    _save_json_file(_HEALTH_REPORT_FILE, health_payload)
    _save_json_file(_TRADE_AUDIT_SUMMARY_FILE, summary_payload)
    _save_json_file(
        _CONFIG_SNAPSHOT_FILE,
        {
            "generated_at": time.time(),
            "refresh_scope": "placed_trade_only",
            "live_intelligence_enabled": bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)),
            "live_intelligence_horizon_hours": float(SIGNAL_CONFIG.get("live_intelligence_horizon_hours", 72) or 72),
        },
    )
    return health_payload


def refresh_live_telemetry(symbols=None):
    if _track_only_live_placed_trades():
        return refresh_live_trade_only_telemetry(symbols=symbols)
    global _telemetry_cycle_counter
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        return {"enabled": False}
    _telemetry_cycle_counter += 1
    materialize_live_intelligence_files()

    accepted_symbols = sorted(
        {
            str(row.get("symbol")).strip()
            for row in _accepted_pending.values()
            if str(row.get("symbol") or "").strip()
        }
    )
    trade_symbols = sorted(
        {
            str(row.get("symbol")).strip()
            for row in _trade_pending.values()
            if str(row.get("symbol") or "").strip()
        }
    )
    rejected_symbols = sorted(
        {
            str(row.get("symbol")).strip()
            for row in _rejected_pending.values()
            if str(row.get("symbol") or "").strip()
        }
    )
    selected_rejected_symbols, rejected_due = _select_rejected_shadow_symbols(rejected_symbols)

    refresh_symbols = []
    seen = set()
    for symbol in accepted_symbols + trade_symbols + selected_rejected_symbols:
        if not symbol or symbol in seen:
            continue
        refresh_symbols.append(symbol)
        seen.add(symbol)

    if symbols is not None:
        for symbol in symbols:
            token = str(symbol or "").strip()
            if not token or token in seen:
                continue
            refresh_symbols.append(token)
            seen.add(token)

    symbol_list = refresh_symbols

    resolved_total = 0
    accepted_resolved = 0
    rejected_resolved = 0
    trade_resolved = 0
    if symbol_list:
        accepted_resolved = _resolve_pending_stream(_accepted_pending, _ACCEPTED_PENDING_FILE, _ACCEPTED_RESOLVED_FILE, symbol_list)
        trade_resolved = _resolve_pending_stream(_trade_pending, _TRADE_PENDING_FILE, _TRADE_RESOLVED_FILE, symbol_list)
        rejected_symbol_set = set(rejected_symbols)
        rejected_refresh_symbols = list(
            {
                symbol
                for symbol in selected_rejected_symbols
                if symbol
            }
            | (
                {
                    symbol
                    for symbol in accepted_symbols + trade_symbols
                    if symbol in rejected_symbol_set
                }
            )
        )
        if rejected_refresh_symbols:
            rejected_resolved = _resolve_pending_stream(
                _rejected_pending,
                _REJECTED_PENDING_FILE,
                _REJECTED_RESOLVED_FILE,
                rejected_refresh_symbols,
            )
        resolved_total = accepted_resolved + rejected_resolved + trade_resolved

    health_payload = _save_reports()
    return {
        "enabled": True,
        "refreshed_symbols": symbol_list,
        "refreshed_symbol_count": len(symbol_list),
        "resolved_total": resolved_total,
        "accepted_resolved": accepted_resolved,
        "rejected_resolved": rejected_resolved,
        "trade_resolved": trade_resolved,
        "accepted_pending": len(_accepted_pending),
        "rejected_pending": len(_rejected_pending),
        "trade_pending": len(_trade_pending),
        "telemetry_cycle": _telemetry_cycle_counter,
        "rejected_due": rejected_due,
        "rejected_total_symbols": len(rejected_symbols),
        "rejected_refreshed_symbols": len(selected_rejected_symbols),
        "health": health_payload,
    }


def _resolve_filtered_pending_stream(
    pending_store: dict[str, dict[str, Any]],
    pending_file: str,
    resolved_file: str,
    symbols: list[str],
    row_filter,
):
    resolved_total = 0
    for symbol in symbols:
        symbol_records = [
            row for row in pending_store.values()
            if str(row.get("symbol")) == str(symbol) and row.get("trackable") and row_filter(row)
        ]
        if not symbol_records:
            continue
        candle_records = [row for row in symbol_records if row.get("resolution_mode") == "candle"]
        start_ms = min(_safe_int(row.get("decision_candle_ts_ms"), _now_ms()) for row in candle_records) if candle_records else None
        candles = []
        removed_status = None
        if start_ms is not None:
            candles = _fetch_resolution_candles(symbol, start_ms, float(SIGNAL_CONFIG.get("live_intelligence_horizon_hours", 72) or 72))
            if not candles:
                removed_status = _probe_symbol_removal_status(symbol)
        for record_id, record in list(pending_store.items()):
            if str(record.get("symbol")) != str(symbol) or not row_filter(record):
                continue
            if record.get("resolution_mode") == "exchange":
                resolved = _resolve_record_from_exchange(record)
            elif removed_status and removed_status.get("removed"):
                detail = _safe_text(removed_status.get("message"), "The symbol has been removed")
                code = _safe_text(removed_status.get("code"), "")
                note = detail if not code else f"{code}: {detail}"
                resolved = _build_removed_symbol_resolution(record, note)
            else:
                resolved = _resolve_record(record, candles)
            if not resolved:
                continue
            resolved = _normalize_trade_resolution_row(resolved)
            pending_store.pop(record_id, None)
            _append_jsonl(resolved_file, resolved)
            resolved_total += 1
    _save_json_file(pending_file, pending_store)
    return resolved_total


def refresh_live_trade_only_telemetry(symbols=None):
    global _telemetry_cycle_counter
    if not bool(SIGNAL_CONFIG.get("enable_live_intelligence", True)):
        return {"enabled": False}
    _telemetry_cycle_counter += 1
    materialize_live_intelligence_files()

    trade_symbols = sorted(
        {
            str(row.get("symbol")).strip()
            for row in _trade_pending.values()
            if str(row.get("symbol") or "").strip() and _is_live_placed_trade_row(row)
        }
    )

    refresh_symbols = []
    seen = set()
    for symbol in trade_symbols:
        if symbol and symbol not in seen:
            refresh_symbols.append(symbol)
            seen.add(symbol)

    if symbols is not None:
        for symbol in symbols:
            token = str(symbol or "").strip()
            if token and token not in seen:
                refresh_symbols.append(token)
                seen.add(token)

    trade_resolved = 0
    if refresh_symbols:
        trade_resolved = _resolve_filtered_pending_stream(
            _trade_pending,
            _TRADE_PENDING_FILE,
            _TRADE_RESOLVED_FILE,
            refresh_symbols,
            _is_live_placed_trade_row,
        )

    health_payload = _save_trade_only_reports()
    placed_resolved_rows = [
        row for row in _dedupe_trade_audit_resolved_rows(_load_jsonl_rows(_TRADE_RESOLVED_FILE))
        if _is_live_placed_trade_row(row)
    ]
    placed_metrics = _compute_metrics(placed_resolved_rows)
    return {
        "enabled": True,
        "refresh_scope": "placed_trade_only",
        "refreshed_symbols": refresh_symbols,
        "refreshed_symbol_count": len(refresh_symbols),
        "resolved_total": trade_resolved,
        "trade_resolved": trade_resolved,
        "trade_pending": sum(1 for row in _trade_pending.values() if _is_live_placed_trade_row(row)),
        "resolved_trade_count": len(placed_resolved_rows),
        "win_rate_pct": placed_metrics.get("win_rate_pct", 0.0),
        "net_r": placed_metrics.get("net_r", 0.0),
        "health": health_payload,
        "telemetry_cycle": _telemetry_cycle_counter,
    }


def _load_pending_state():
    global _accepted_pending, _rejected_pending, _trade_pending
    global _accepted_known_ids, _rejected_known_ids, _trade_known_ids

    _accepted_pending = {}
    _rejected_pending = {}
    _trade_pending = _load_json_file(_TRADE_PENDING_FILE, {})

    if not _trade_pending and os.path.exists(_TRADE_EVENTS_FILE):
        rebuilt = _rebuild_pending_store_from_events("trade_audit", _TRADE_EVENTS_FILE, _TRADE_RESOLVED_FILE)
        if rebuilt:
            _trade_pending = rebuilt

    _ensure_pending_state_files()

    _accepted_known_ids = set()
    _rejected_known_ids = set()
    _trade_known_ids = set(_trade_pending) | _load_known_ids(_TRADE_RESOLVED_FILE)


_load_pending_state()
materialize_live_intelligence_files()
