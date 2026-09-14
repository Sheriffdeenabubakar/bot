from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_GATE_CACHE: dict[str, Any] = {
    "path": None,
    "mtime": None,
    "payload": None,
}


def normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text.lower() if text else None


def normalize_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text if text else None


def normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


def classify_adx_regime(adx_value: Any) -> str:
    try:
        adx = float(adx_value)
    except Exception:
        return "unknown"
    if adx >= 40:
        return "high"
    if adx >= 25:
        return "medium"
    return "low"


def classify_confirmation_bucket(score: Any, medium_threshold: float, strong_threshold: float) -> str:
    try:
        numeric_score = float(score)
    except Exception:
        return "weak"
    if numeric_score >= float(strong_threshold):
        return "strong"
    if numeric_score >= float(medium_threshold):
        return "medium"
    return "weak"


def classify_htf_alignment_bucket(htf_confluence: Any) -> str:
    try:
        confluence = int(float(htf_confluence))
    except Exception:
        return "none"
    if confluence >= 2:
        return "double"
    if confluence == 1:
        return "single"
    return "none"


def load_quality_gate(path: str | Path) -> dict[str, Any] | None:
    gate_path = Path(path).resolve()
    if not gate_path.exists():
        return None

    stat = gate_path.stat()
    if (
        _GATE_CACHE.get("path") == str(gate_path)
        and _GATE_CACHE.get("mtime") == stat.st_mtime
        and _GATE_CACHE.get("payload") is not None
    ):
        return _GATE_CACHE["payload"]

    payload = json.loads(gate_path.read_text(encoding="utf-8"))
    rules = payload.get("rules", [])
    rules = sorted(
        rules,
        key=lambda item: (
            int(item.get("priority", 0) or 0),
            int(item.get("opportunities", 0) or 0),
            float(item.get("win_rate_pct", 0.0) or 0.0),
            float(item.get("expectancy_r", 0.0) or 0.0),
        ),
        reverse=True,
    )
    payload["rules"] = rules
    payload["path"] = str(gate_path)
    _GATE_CACHE["path"] = str(gate_path)
    _GATE_CACHE["mtime"] = stat.st_mtime
    _GATE_CACHE["payload"] = payload
    return payload


def _match_text(context_value: Any, rule_value: Any, *, symbol: bool = False) -> bool:
    if rule_value in (None, "", "*"):
        return True
    left = normalize_symbol(context_value) if symbol else normalize_text(context_value)
    right = normalize_symbol(rule_value) if symbol else normalize_text(rule_value)
    return left == right


def rule_matches_context(rule: dict[str, Any], context: dict[str, Any]) -> bool:
    text_fields = {
        "symbol": True,
        "symbol_bucket": False,
        "direction": False,
        "primary_setup": False,
        "session_bucket": False,
        "live_market_regime": False,
        "adx_regime": False,
        "htf_alignment_bucket": False,
        "confirmation_bucket": False,
    }
    for field, use_symbol_normalization in text_fields.items():
        if not _match_text(context.get(field), rule.get(field), symbol=use_symbol_normalization):
            return False

    for field in (
        "has_wyckoff",
        "has_retest",
        "has_volume_spike",
        "has_momentum_divergence",
        "has_order_flow",
        "has_fibonacci_proximity",
        "has_vwap_proximity",
    ):
        rule_value = normalize_bool(rule.get(field))
        if rule_value is None:
            continue
        if normalize_bool(context.get(field)) is not rule_value:
            return False

    return True


def evaluate_quality_gate(
    *,
    enabled: bool,
    gate_path: str | Path,
    fail_closed: bool,
    context: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "enabled": bool(enabled),
        "blocked": False,
        "reason": None,
        "matched_rule": None,
        "gate_path": str(gate_path),
    }
    if not enabled:
        return result

    payload = load_quality_gate(gate_path)
    if not payload or not payload.get("rules"):
        result["blocked"] = bool(fail_closed)
        result["reason"] = (
            "Research quality gate file missing or empty"
            if result["blocked"]
            else "Research quality gate unavailable"
        )
        return result

    for rule in payload.get("rules", []):
        if rule_matches_context(rule, context):
            result["matched_rule"] = rule
            return result

    result["blocked"] = True
    result["reason"] = (
        "Research quality gate blocked unapproved setup: "
        f"{context.get('symbol')} {context.get('direction')} {context.get('primary_setup')} "
        f"{context.get('session_bucket')} {context.get('live_market_regime')} "
        f"{context.get('confirmation_bucket')} {context.get('htf_alignment_bucket')}"
    )
    return result
