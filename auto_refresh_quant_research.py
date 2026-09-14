from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import quant_research_pack as pack


TZ = ZoneInfo("Africa/Lagos")
ROOT = Path(__file__).resolve().parent
RESOLVED_PATH = ROOT / "live_trade_audit_resolved.jsonl"
OUT_DIR = ROOT / "quant_research_runtime"
STATE_PATH = OUT_DIR / "auto_refresh_state.json"
STATUS_PATH = OUT_DIR / "auto_refresh_status.json"
DRIFT_PATH = OUT_DIR / "candidate_drift_summary.json"
WATCHLIST_PATH = OUT_DIR / "candidate_watchlist_summary.json"
WATCHLIST_CSV_PATH = OUT_DIR / "candidate_decay_watchlist.csv"
QUANT_CYCLE_SUMMARY_PATH = OUT_DIR / "quant_cycle_summary.json"
HISTORY_DIR = OUT_DIR / "history"
DRIFT_HISTORY_PATH = OUT_DIR / "quant_filter_drift_history.jsonl"
PROMOTION_HISTORY_PATH = OUT_DIR / "candidate_promotion_history.jsonl"
LOG_PATH = OUT_DIR / "auto_refresh.log"
PROMOTION_LEDGER_PATH = OUT_DIR / "promotion_ledger.csv"
FILTER_SIM_PATH = OUT_DIR / "filter_simulation_table.csv"
RESEARCH_SUMMARY_PATH = OUT_DIR / "research_summary.json"
MAX_DRIFT_HISTORY = 400
MAX_PROMOTION_HISTORY = 180
IO_HEALTH: dict[str, list[dict[str, Any]]] = {}
LEGACY_ENGINE_GENERATION = "legacy"
CURRENT_ENGINE_GENERATION = getattr(pack, "RESEARCH_ENGINE_GENERATION", LEGACY_ENGINE_GENERATION)
CURRENT_ENGINE_MARKERS = (
    "gate_",
    "of_absorbed_levels_bucket",
    "of_opposed_levels_bucket",
    "of_void_max_span_bps_bucket",
    "of_wall_side_bias",
    "of_queue_bid_touch_rel_bucket",
    "of_queue_ask_touch_rel_bucket",
)


@dataclass
class SourceSignature:
    resolved_size: int
    resolved_mtime: float
    runner_mtime: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "resolved_size": self.resolved_size,
            "resolved_mtime": self.resolved_mtime,
            "runner_mtime": self.runner_mtime,
        }


def now_iso() -> str:
    return datetime.now(TZ).isoformat()


def normalize_generation(value: Any) -> str:
    text = str(value or "").strip()
    return text or LEGACY_ENGINE_GENERATION


def infer_engine_generation(payload: dict[str, Any] | None) -> str:
    payload = payload if isinstance(payload, dict) else {}
    explicit = payload.get("engine_generation") or payload.get("research_generation")
    if explicit:
        return normalize_generation(explicit)

    candidate_ids: list[str] = []
    lanes = payload.get("lanes") if isinstance(payload.get("lanes"), dict) else {}
    for lane in lanes.values():
        if not isinstance(lane, dict):
            continue
        candidate_ids.extend(str(rule_id) for rule_id in (lane.get("rule_ids") or []))
    for candidate in payload.get("promoted_candidates") or []:
        if not isinstance(candidate, dict):
            continue
        candidate_ids.append(str(candidate.get("candidate_id") or ""))

    haystack = " || ".join(candidate_ids)
    if haystack and any(marker in haystack for marker in CURRENT_ENGINE_MARKERS):
        return CURRENT_ENGINE_GENERATION
    return LEGACY_ENGINE_GENERATION


def filter_history_rows(
    history: list[dict[str, Any]],
    *,
    generation: str | None = None,
    skip_current: bool = False,
    lookback: int | None = None,
) -> list[dict[str, Any]]:
    safe_history = [row for row in (history or []) if isinstance(row, dict)]
    rows = safe_history[:-1] if skip_current else safe_history
    if generation is not None:
        wanted = normalize_generation(generation)
        rows = [row for row in rows if infer_engine_generation(row) == wanted]
    if lookback is not None:
        rows = rows[-lookback:]
    return rows


def log(message: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{now_iso()}] {message}"
    print(line, flush=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def reset_io_health() -> None:
    global IO_HEALTH
    IO_HEALTH = {
        "json_backup_recoveries": [],
        "jsonl_backup_recoveries": [],
        "primary_parse_failures": [],
        "jsonl_bad_lines": [],
        "unrecovered_load_failures": [],
    }


def note_io_event(kind: str, path: Path, detail: Any = None) -> None:
    IO_HEALTH.setdefault(kind, []).append(
        {
            "path": str(path),
            "detail": detail,
            "at": now_iso(),
        }
    )


def io_health_summary() -> dict[str, Any]:
    backup_recoveries = IO_HEALTH.get("json_backup_recoveries", []) + IO_HEALTH.get("jsonl_backup_recoveries", [])
    unrecovered = IO_HEALTH.get("unrecovered_load_failures", [])
    parse_failures = IO_HEALTH.get("primary_parse_failures", [])
    jsonl_bad_lines = IO_HEALTH.get("jsonl_bad_lines", [])
    affected_files = sorted(
        {
            event.get("path")
            for bucket in [backup_recoveries, unrecovered, parse_failures, jsonl_bad_lines]
            for event in bucket
            if event.get("path")
        }
    )
    if unrecovered:
        grade = "degraded"
    elif backup_recoveries:
        grade = "recovered"
    elif parse_failures or jsonl_bad_lines:
        grade = "warning"
    else:
        grade = "healthy"
    return {
        "grade": grade,
        "backup_recovery_count": len(backup_recoveries),
        "json_backup_recovery_count": len(IO_HEALTH.get("json_backup_recoveries", [])),
        "jsonl_backup_recovery_count": len(IO_HEALTH.get("jsonl_backup_recoveries", [])),
        "primary_parse_failure_count": len(parse_failures),
        "jsonl_bad_line_event_count": len(jsonl_bad_lines),
        "unrecovered_load_failure_count": len(unrecovered),
        "affected_files": affected_files,
        "events": IO_HEALTH,
    }


reset_io_health()


def backup_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".bak")


def atomic_write_text(path: Path, text: str, *, keep_backup: bool = False) -> None:
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
        if keep_backup and path.exists():
            shutil.copy2(path, backup_path(path))
        os.replace(tmp_name, path)
    finally:
        if tmp_name:
            tmp_path = Path(tmp_name)
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass


def get_signature() -> SourceSignature:
    resolved_stat = RESOLVED_PATH.stat()
    runner_stat = (ROOT / "quant_research_pack.py").stat()
    return SourceSignature(
        resolved_size=resolved_stat.st_size,
        resolved_mtime=resolved_stat.st_mtime,
        runner_mtime=runner_stat.st_mtime,
    )


def load_json(path: Path, default: Any) -> Any:
    candidates = [path, backup_path(path)]
    primary_failed = False
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if candidate != path:
                note_io_event("json_backup_recoveries", path, f"loaded backup {candidate.name}")
            return payload
        except Exception:
            if candidate == path:
                primary_failed = True
                note_io_event("primary_parse_failures", path, "json parse failure")
            continue
    if path.exists() and primary_failed:
        note_io_event("unrecovered_load_failures", path, "json default fallback")
    return default


def write_json(path: Path, payload: Any, *, keep_backup: bool = False) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2), keep_backup=keep_backup)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    rows = [row for row in (rows or []) if isinstance(row, dict)]
    if not rows:
        atomic_write_text(path, "")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        delete=False,
        dir=path.parent,
        prefix=f"{path.name}.",
        suffix=".tmp",
    ) as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
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


def to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def load_state() -> dict[str, Any]:
    state = load_json(
        STATE_PATH,
        {
            "last_completed_at": None,
            "last_success_signature": None,
            "last_summary_snapshot": None,
            "last_filter_snapshot": None,
            "last_drift_report": None,
            "last_watchlist_summary": None,
            "last_io_health": None,
            "run_count": 0,
        },
    )
    return state if isinstance(state, dict) else {}


def read_research_summary() -> dict[str, Any]:
    payload = load_json(RESEARCH_SUMMARY_PATH, {})
    return payload if isinstance(payload, dict) else {}


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rows.append(row)
    return rows


def safe_json_loads(value: Any, default: Any) -> Any:
    try:
        if value in (None, ""):
            return default
        return json.loads(value)
    except Exception:
        return default


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    def _read(candidate: Path) -> tuple[list[dict[str, Any]], int]:
        parsed_rows: list[dict[str, Any]] = []
        bad_lines = 0
        non_dict_rows = 0
        with candidate.open("r", encoding="utf-8") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                    if isinstance(payload, dict):
                        parsed_rows.append(payload)
                    else:
                        non_dict_rows += 1
                except Exception:
                    bad_lines += 1
        if non_dict_rows:
            note_io_event("jsonl_bad_lines", candidate, f"non_dict_rows={non_dict_rows}")
        return parsed_rows, bad_lines

    for candidate in [path, backup_path(path)]:
        if not candidate.exists():
            continue
        try:
            rows, bad_lines = _read(candidate)
            if candidate == path and bad_lines > 0:
                note_io_event("jsonl_bad_lines", path, f"bad_lines={bad_lines}")
            if rows or bad_lines == 0 or candidate == backup_path(path):
                if candidate != path:
                    note_io_event("jsonl_backup_recoveries", path, f"loaded backup {candidate.name}")
                return rows
        except Exception:
            if candidate == path:
                note_io_event("primary_parse_failures", path, "jsonl read failure")
            continue
    if path.exists():
        note_io_event("unrecovered_load_failures", path, "jsonl empty/default fallback")
    return []


def write_jsonl(path: Path, rows: list[dict[str, Any]], *, keep_backup: bool = False) -> None:
    payload = "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows)
    atomic_write_text(path, payload, keep_backup=keep_backup)


def hash_strings(values: list[str]) -> str:
    payload = "\n".join(sorted(str(v) for v in values if v is not None))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def jaccard_pct(left: set[str], right: set[str]) -> float | None:
    union = left | right
    if not union:
        return None
    return round((100.0 * len(left & right) / len(union)), 4)


def weighted_jaccard_pct(left: dict[str, float], right: dict[str, float]) -> float | None:
    keys = set(left or {}) | set(right or {})
    if not keys:
        return None
    overlap = 0.0
    union = 0.0
    for key in keys:
        left_weight = max(0.0, to_float((left or {}).get(key)) or 0.0)
        right_weight = max(0.0, to_float((right or {}).get(key)) or 0.0)
        overlap += min(left_weight, right_weight)
        union += max(left_weight, right_weight)
    if union <= 0:
        return None
    return round((100.0 * overlap / union), 4)


def top_mix(counter: Counter[str], total: int, limit: int = 6) -> list[dict[str, Any]]:
    if total <= 0:
        return []
    rows = []
    for label, count in counter.most_common(limit):
        rows.append(
            {
                "label": label,
                "n": count,
                "pct": round((100.0 * count / total), 4),
            }
        )
    return rows


def mix_map(counter: Counter[str], total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        label: round((100.0 * count / total), 4)
        for label, count in counter.items()
    }


def summarize_trade_mix(trades: list[pack.Trade]) -> dict[str, Any]:
    total = len(trades)
    engine_counter = Counter(t.engine for t in trades)
    session_counter = Counter((t.session or "unknown") for t in trades)
    regime_counter = Counter((t.regime or "unknown") for t in trades)
    structure_counter = Counter((t.structure_alignment or "unknown") for t in trades)
    symbol_counter = Counter(t.symbol for t in trades)
    top_symbol = symbol_counter.most_common(1)
    return {
        "engine_mix": top_mix(engine_counter, total),
        "engine_mix_map": mix_map(engine_counter, total),
        "session_mix": top_mix(session_counter, total),
        "session_mix_map": mix_map(session_counter, total),
        "regime_mix": top_mix(regime_counter, total),
        "regime_mix_map": mix_map(regime_counter, total),
        "structure_alignment_mix": top_mix(structure_counter, total),
        "structure_alignment_mix_map": mix_map(structure_counter, total),
        "symbol_mix": top_mix(symbol_counter, total),
        "symbol_concentration_pct": round((100.0 * top_symbol[0][1] / total), 4) if total and top_symbol else None,
    }


def mix_shift_pct(current_map: dict[str, Any], previous_map: dict[str, Any]) -> float | None:
    if not current_map or not previous_map:
        return None
    keys = set(current_map or {}) | set(previous_map or {})
    if not keys:
        return None
    shift = 0.0
    for key in keys:
        shift += abs((to_float((current_map or {}).get(key)) or 0.0) - (to_float((previous_map or {}).get(key)) or 0.0))
    return round(shift / 2.0, 4)


def clamp(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))


def avg_non_null(values: list[float | None]) -> float | None:
    usable = [float(v) for v in values if v is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable), 4)


def precision_min_retained_pct() -> float:
    return float(getattr(pack, "PRECISION_MIN_RETAINED_PCT", 40.0) or 40.0)


def practical_fallback_min_retained_pct() -> float:
    return float(getattr(pack, "PRACTICAL_FALLBACK_MIN_RETAINED_PCT", 40.0) or 40.0)


def practical_fallback_target_wr_pct() -> float:
    return float(getattr(pack, "PRACTICAL_FALLBACK_TARGET_WR_PCT", 40.0) or 40.0)


def filter_retained_pct(row: dict[str, Any]) -> float:
    return to_float(row.get("trade_flow_retained_pct")) or 0.0


def filter_family(row: dict[str, Any]) -> str:
    return str(row.get("simulation_family") or row.get("family") or "").strip().lower()


def filter_identity(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "").strip().lower()
        for key in ("simulation_family", "simulation_id", "simulation_label")
    )


def is_precision_filter_row(row: dict[str, Any]) -> bool:
    identity = filter_identity(row)
    family = filter_family(row)
    return (
        row.get("simulation_strategy") == "allowlist"
        and "practical" not in identity
        and (family == "dynamic_precision" or "precision" in identity)
    )


def is_practical_filter_row(row: dict[str, Any]) -> bool:
    identity = filter_identity(row)
    family = filter_family(row)
    return (
        row.get("simulation_strategy") in {"allowlist", "blocklist"}
        and (family == "dynamic_practical" or "practical" in identity)
    )


def force_active_filter_highest_wr(payload: dict[str, Any], reason: str) -> dict[str, Any] | None:
    precision = payload.get("best_precision_filter")
    if isinstance(precision, dict) and precision_filter_full_pass(precision):
        selected = dict(precision)
        selected["live_selection_lane"] = "precision"
        selected["live_selection_reason"] = f"{reason}_precision_retention_optional"
        payload["active_dynamic_filter"] = selected
        payload["recommended_live_filter"] = selected
        return selected
    practical = payload.get("best_practical_filter")
    balanced = payload.get("best_balanced_filter")
    candidates = []
    if isinstance(precision, dict) and precision:
        candidates.append(("precision", precision))
    if isinstance(balanced, dict) and balanced:
        candidates.append(("balanced", balanced))
    if isinstance(practical, dict) and practical:
        candidates.append(("practical", practical))
    if not candidates:
        return None
    lane, selected_row = max(
        candidates,
        key=lambda item: (
            to_float(item[1].get("kept_wr")) or float("-inf"),
            to_float(item[1].get("kept_mean_r")) or float("-inf"),
            filter_retained_pct(item[1]),
        ),
    )
    selected = dict(selected_row)
    selected["live_selection_lane"] = lane
    selected["live_selection_reason"] = reason
    payload["active_dynamic_filter"] = selected
    payload["recommended_live_filter"] = selected
    return selected


def filter_target_met(row: dict[str, Any]) -> bool:
    return row.get("target_wr_met") in {True, "True", "true"}


def precision_target_wr_met(row: dict[str, Any]) -> bool:
    target_wr = float(getattr(pack, "PRECISION_TARGET_WR_PCT", 50.0) or 50.0)
    row_target = to_float(row.get("target_wr_pct"))
    return (
        (filter_target_met(row) and (row_target is None or row_target >= target_wr))
        or (to_float(row.get("kept_wr")) or 0.0) >= target_wr
    )


def precision_filter_eligible(row: dict[str, Any]) -> bool:
    return is_precision_filter_row(row)


def precision_filter_full_pass(row: dict[str, Any]) -> bool:
    return (
        precision_filter_eligible(row)
        and precision_target_wr_met(row)
        and (to_float(row.get("kept_mean_r")) or 0.0) > 0.0
    )


def practical_filter_eligible(row: dict[str, Any]) -> bool:
    return (
        is_practical_filter_row(row)
        and filter_retained_pct(row) >= practical_fallback_min_retained_pct()
        and (to_float(row.get("kept_wr")) or 0.0) >= practical_fallback_target_wr_pct()
    )


def practical_filter_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    return (
        to_float(row.get("kept_wr")) or float("-inf"),
        to_float(row.get("kept_mean_r")) or float("-inf"),
        filter_retained_pct(row),
        to_float(row.get("kept_n")) or float("-inf"),
        -(to_float(row.get("constituent_count")) or 0.0),
    )


def precision_filter_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float, float, float, float, float]:
    retention_met = 1.0 if filter_retained_pct(row) >= precision_min_retained_pct() else 0.0
    return (
        1.0 if precision_filter_full_pass(row) else 0.0,
        to_float(row.get("kept_wr")) or float("-inf"),
        retention_met,
        to_float(row.get("precision_quality_score")) or float("-inf"),
        to_float(row.get("kept_wr_lcb_80")) or float("-inf"),
        to_float(row.get("kept_n")) or float("-inf"),
        filter_retained_pct(row),
        to_float(row.get("kept_mean_r")) or float("-inf"),
        -(to_float(row.get("constituent_count")) or 0.0),
    )


def balanced_filter_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    return (
        to_float(row.get("kept_mean_r")) or float("-inf"),
        to_float(row.get("kept_wr")) or float("-inf"),
        to_float(row.get("kept_n")) or float("-inf"),
        to_float(row.get("trade_flow_retained_pct")) or float("-inf"),
        -(to_float(row.get("constituent_count")) or 0.0),
    )


def filter_row_index(filters: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("simulation_id")): row
        for row in filters
        if row.get("simulation_id")
    }


def promotion_row_index(promotions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("candidate_id")): row
        for row in promotions
        if row.get("candidate_id")
    }


def promoted_candidate_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "candidate_label": row.get("candidate_label") or row.get("candidate_id"),
        "candidate_type": row.get("candidate_type"),
        "engine": row.get("engine"),
        "promotion_decision": row.get("promotion_decision"),
        "support_n": to_float(row.get("support_n")),
        "win_rate_pct": to_float(row.get("win_rate_pct")),
        "mean_r": to_float(row.get("mean_r")),
        "delta_wr_vs_engine": to_float(row.get("delta_wr_vs_engine")),
        "delta_r_vs_engine": to_float(row.get("delta_r_vs_engine")),
        "window_count_ge5": to_float(row.get("window_count_ge5")),
        "negative_window_ratio": to_float(row.get("negative_window_ratio")),
        "positive_window_ratio": to_float(row.get("positive_window_ratio")),
        "top_symbol_pct": to_float(row.get("top_symbol_pct")),
        "top_day_pct": to_float(row.get("top_day_pct")),
        "live_safe_candidate": row.get("live_safe_candidate"),
    }


def append_promotion_history(
    summary: dict[str, Any],
    promotions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    promoted_rows = [
        promoted_candidate_snapshot(row)
        for row in promotions
        if row.get("promotion_decision") in {"boost", "hard_block", "quality_uplift"}
    ]
    payload = {
        "generated_at": now_iso(),
        "engine_generation": normalize_generation(summary.get("engine_generation")),
        "research_scope": summary.get("research_scope"),
        "resolved_rows_used": summary.get("resolved_rows_used"),
        "promotion_counts": summary.get("promotion_counts"),
        "promoted_candidates": promoted_rows,
    }
    history = read_jsonl(PROMOTION_HISTORY_PATH)
    if len(history) < 6 and HISTORY_DIR.exists():
        snapshot_dirs = sorted([path for path in HISTORY_DIR.iterdir() if path.is_dir()], key=lambda path: path.name)
        rebuilt: list[dict[str, Any]] = []
        for snap_dir in snapshot_dirs[-40:]:
            promo_path = snap_dir / "promotion_ledger.csv"
            if not promo_path.exists():
                continue
            snap_promotions = read_csv_rows(promo_path)
            promoted = [
                promoted_candidate_snapshot(row)
                for row in snap_promotions
                if row.get("promotion_decision") in {"boost", "hard_block", "quality_uplift"}
            ]
            snap_summary = load_json(snap_dir / "research_summary.json", {})
            rebuilt.append(
                {
                    "generated_at": datetime.fromtimestamp(snap_dir.stat().st_mtime, TZ).isoformat(),
                    "engine_generation": normalize_generation(snap_summary.get("engine_generation")),
                    "research_scope": snap_summary.get("research_scope"),
                    "resolved_rows_used": snap_summary.get("resolved_rows_used"),
                    "promotion_counts": snap_summary.get("promotion_counts"),
                    "promoted_candidates": promoted,
                }
            )
        if rebuilt:
            history = rebuilt[-MAX_PROMOTION_HISTORY:]
    history.append(payload)
    if len(history) > MAX_PROMOTION_HISTORY:
        history = history[-MAX_PROMOTION_HISTORY:]
    write_jsonl(PROMOTION_HISTORY_PATH, history, keep_backup=True)
    return history


def candidate_margin_score(row: dict[str, Any]) -> float:
    decision = str(row.get("promotion_decision") or "")
    delta_wr = to_float(row.get("delta_wr_vs_engine")) or 0.0
    mean_r = to_float(row.get("mean_r")) or 0.0
    delta_r = to_float(row.get("delta_r_vs_engine"))
    if decision == "boost":
        wr_margin = delta_wr - 12.0
        mean_margin = max(mean_r - 0.20, (delta_r - 0.20) if delta_r is not None else float("-inf"))
        return round(min(wr_margin, mean_margin), 4)
    if decision == "hard_block":
        wr_margin = (-delta_wr) - 12.0
        mean_margin = ((-mean_r) - 0.25) if mean_r < 0 else float("-inf")
        return round(min(wr_margin, mean_margin), 4)
    if decision == "quality_uplift":
        wr_margin = (-delta_wr) - 6.0
        mean_margin = ((-mean_r) - 0.05) if mean_r < 0 else float("-inf")
        return round(max(wr_margin, mean_margin), 4)
    return 0.0


def candidate_support_floor(row: dict[str, Any]) -> float:
    decision = str(row.get("promotion_decision") or "")
    if decision == "quality_uplift":
        return 20.0
    return 15.0


def candidate_history_index(
    history: list[dict[str, Any]],
    candidate_ids: set[str],
    *,
    lookback: int = 24,
    generation: str | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    rows = filter_history_rows(history, generation=generation, lookback=lookback)
    out = {candidate_id: [] for candidate_id in candidate_ids}
    for item in rows:
        for candidate in item.get("promoted_candidates") or []:
            candidate_id = candidate.get("candidate_id")
            if candidate_id in out:
                entry = dict(candidate)
                entry["generated_at"] = item.get("generated_at")
                entry["engine_generation"] = infer_engine_generation(item)
                out[candidate_id].append(entry)
    return out, len(rows)


def build_rule_snapshot(rule_id: str, promotions_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    row = promotions_by_id.get(rule_id, {})
    return {
        "candidate_id": rule_id,
        "candidate_label": row.get("candidate_label") or rule_id,
        "engine": row.get("engine"),
        "support_n": to_float(row.get("support_n")),
        "win_rate_pct": to_float(row.get("win_rate_pct")),
        "mean_r": to_float(row.get("mean_r")),
        "promotion_decision": row.get("promotion_decision"),
        "live_safe_candidate": row.get("live_safe_candidate"),
    }


def select_filter_rows(filters: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    precision = None
    balanced = None

    allowlist_rows = [row for row in filters if is_precision_filter_row(row)]
    if allowlist_rows:
        target_met_rows = [row for row in allowlist_rows if precision_filter_full_pass(row)]
        pool = target_met_rows or allowlist_rows
        precision = max(pool, key=precision_filter_sort_key)

    blocklist_rows = [row for row in filters if row.get("simulation_strategy") == "blocklist"]
    retained_rows = [
        row
        for row in blocklist_rows
        if (to_float(row.get("trade_flow_retained_pct")) or float("-inf")) >= 60.0
    ]
    if retained_rows:
        balanced = max(retained_rows, key=balanced_filter_sort_key)
    return precision, balanced


def compact_summary(
    summary: dict[str, Any],
    promotions: list[dict[str, Any]],
    filters: list[dict[str, Any]],
) -> dict[str, Any]:
    engine_baselines = summary.get("engines", [])
    hard_blocks = [r for r in promotions if r.get("promotion_decision") == "hard_block"]
    boosts = [r for r in promotions if r.get("promotion_decision") == "boost"]
    uplifts = [r for r in promotions if r.get("promotion_decision") == "quality_uplift"]

    def top_rows(rows: list[dict[str, Any]], sort_keys: tuple[str, ...], limit: int = 10) -> list[dict[str, Any]]:
        def key_fn(row: dict[str, Any]) -> tuple[Any, ...]:
            out = []
            for key in sort_keys:
                value = row.get(key)
                try:
                    out.append(float(value))
                except Exception:
                    out.append(value)
            return tuple(out)

        return sorted(rows, key=key_fn, reverse=True)[:limit]

    def preview_rules(text: str | None, limit: int = 3) -> str:
        parts = [part.strip() for part in str(text or "").split(" || ") if part.strip()]
        if not parts:
            return ""
        preview = parts[:limit]
        extra = len(parts) - len(preview)
        suffix = f" ; +{extra} more" if extra > 0 else ""
        return " ; ".join(preview) + suffix

    def promotion_view(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "candidate_id": row.get("candidate_id"),
            "candidate_label": row.get("candidate_label") or row.get("candidate_id"),
            "engine": row.get("engine"),
            "support_n": row.get("support_n"),
            "win_rate_pct": row.get("win_rate_pct"),
            "mean_r": row.get("mean_r"),
            "component_count": row.get("component_count"),
            "component_labels": row.get("component_labels"),
            "component_preview": preview_rules(row.get("component_labels"), limit=3),
            "promotion_decision": row.get("promotion_decision"),
        }

    filter_rows = []
    for row in filters:
        filter_rows.append(
            {
                "simulation_id": row.get("simulation_id"),
                "simulation_label": row.get("simulation_label") or row.get("simulation_id"),
                "simulation_family": row.get("simulation_family"),
                "simulation_strategy": row.get("simulation_strategy"),
                "constituent_count": row.get("constituent_count"),
                "rule_ids_json": row.get("rule_ids_json"),
                "rules_applied_labels": row.get("rules_applied_labels"),
                "rule_labels_json": row.get("rule_labels_json"),
                "rules_preview": preview_rules(row.get("rules_applied_labels"), limit=3),
                "kept_n": row.get("kept_n"),
                "blocked_n": row.get("blocked_n"),
                "kept_wr": row.get("kept_wr"),
                "kept_mean_r": row.get("kept_mean_r"),
                "trade_flow_retained_pct": row.get("trade_flow_retained_pct"),
                "target_wr_pct": row.get("target_wr_pct"),
                "target_wr_met": row.get("target_wr_met"),
                "kept_wr_lcb_80": row.get("kept_wr_lcb_80"),
                "precision_quality_score": row.get("precision_quality_score"),
                "precision_min_kept": row.get("precision_min_kept"),
                "precision_min_retained_pct": row.get("precision_min_retained_pct"),
                "precision_lcb_floor": row.get("precision_lcb_floor"),
                "resolved_rows_used": row.get("resolved_rows_used"),
                "selection_basis": row.get("selection_basis"),
            }
        )

    def select_filter(rows: list[dict[str, Any]], *, min_retained_pct: float | None = None) -> dict[str, Any] | None:
        candidates = []
        for row in rows:
            retained = to_float(row.get("trade_flow_retained_pct"))
            if min_retained_pct is not None and (retained is None or retained < min_retained_pct):
                continue
            candidates.append(row)
        if not candidates:
            return None
        return max(candidates, key=balanced_filter_sort_key)

    def select_precision_filter(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        allowlist_rows = [row for row in rows if is_precision_filter_row(row)]
        if not allowlist_rows:
            return None
        target_met_rows = [row for row in allowlist_rows if precision_filter_full_pass(row)]
        pool = target_met_rows or allowlist_rows
        return max(pool, key=precision_filter_sort_key)

    def select_balanced_filter(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        blocklist_rows = [row for row in rows if row.get("simulation_strategy") == "blocklist"]
        return select_filter(blocklist_rows, min_retained_pct=60.0)

    def select_practical_filter(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        practical_rows = [row for row in rows if practical_filter_eligible(row)]
        if not practical_rows:
            return None
        return max(practical_rows, key=practical_filter_sort_key)

    def select_recommended_live_filter(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        precision = select_precision_filter(rows)
        balanced = select_balanced_filter(rows)
        practical = select_practical_filter(rows)
        if precision and precision_filter_full_pass(precision):
            selected = dict(precision)
            selected["live_selection_lane"] = "precision"
            selected["live_selection_reason"] = (
                f"precision_target_wr_met_retention_optional"
            )
            return selected
        if practical:
            selected = dict(practical)
            selected["live_selection_lane"] = "practical"
            selected["live_selection_reason"] = (
                f"precision_full_pass_not_met_practical_fallback_ge_"
                f"{practical_fallback_min_retained_pct():g}pct_retained_ge_"
                f"{practical_fallback_target_wr_pct():g}pct_wr"
            )
            return selected
        candidates = []
        if precision:
            candidates.append(("precision", precision))
        if balanced:
            candidates.append(("balanced", balanced))
        if not candidates:
            return None
        lane, selected_row = max(
            candidates,
            key=lambda item: (
                to_float(item[1].get("kept_wr")) or float("-inf"),
                to_float(item[1].get("kept_mean_r")) or float("-inf"),
                filter_retained_pct(item[1]),
            ),
        )
        selected = dict(selected_row)
        selected["live_selection_lane"] = lane
        selected["live_selection_reason"] = "precision_full_pass_not_met_highest_kept_wr_selected"
        return selected

    active_dynamic_filter = select_recommended_live_filter(filter_rows)

    return {
        "engine_generation": normalize_generation(summary.get("engine_generation")),
        "research_scope": summary.get("research_scope"),
        "window_start": summary.get("window_start"),
        "window_end": summary.get("window_end"),
        "resolved_rows_used": summary.get("resolved_rows_used"),
        "engines": engine_baselines,
        "promotion_counts": {
            "hard_block": len(hard_blocks),
            "boost": len(boosts),
            "quality_uplift": len(uplifts),
        },
        "top_hard_blocks": [promotion_view(row) for row in top_rows(hard_blocks, ("support_n",), 10)],
        "top_boosts": [promotion_view(row) for row in top_rows(boosts, ("win_rate_pct", "support_n"), 10)],
        "top_quality_uplifts": [promotion_view(row) for row in top_rows(uplifts, ("support_n",), 10)],
        "best_precision_filter": select_precision_filter(filter_rows),
        "best_balanced_filter": select_balanced_filter(filter_rows),
        "best_practical_filter": select_practical_filter(filter_rows),
        "active_dynamic_filter": active_dynamic_filter,
        "recommended_live_filter": active_dynamic_filter,
        "top_filter_sims": top_rows(filter_rows, ("kept_mean_r", "kept_wr"), 10),
    }


def build_filter_snapshot(
    lane_name: str,
    filter_row: dict[str, Any] | None,
    promotions_by_id: dict[str, dict[str, Any]],
    trades: list[pack.Trade],
) -> dict[str, Any] | None:
    if not filter_row:
        return None
    rule_ids = safe_json_loads(filter_row.get("rule_ids_json"), [])
    rule_labels = safe_json_loads(filter_row.get("rule_labels_json"), [])
    strategy = str(filter_row.get("simulation_strategy") or "")
    kept, blocked = pack.materialize_filter_selection(trades, rule_ids, strategy)
    kept_keys = sorted(pack.trade_identity(trade) for trade in kept)
    blocked_keys = sorted(pack.trade_identity(trade) for trade in blocked)
    rule_snapshots = []
    for idx, rule_id in enumerate(rule_ids):
        snap = build_rule_snapshot(rule_id, promotions_by_id)
        if idx < len(rule_labels):
            snap["candidate_label"] = rule_labels[idx]
        rule_snapshots.append(snap)
    return {
        "lane": lane_name,
        "engine_generation": CURRENT_ENGINE_GENERATION,
        "simulation_id": filter_row.get("simulation_id"),
        "simulation_label": filter_row.get("simulation_label"),
        "simulation_family": filter_row.get("simulation_family"),
        "simulation_strategy": strategy,
        "constituent_count": int(to_float(filter_row.get("constituent_count")) or len(rule_ids)),
        "rule_ids": rule_ids,
        "rule_labels": rule_labels,
        "rule_fingerprint": hash_strings(rule_ids),
        "rules_preview": filter_row.get("rules_preview"),
        "kept_n": int(to_float(filter_row.get("kept_n")) or len(kept)),
        "blocked_n": int(to_float(filter_row.get("blocked_n")) or len(blocked)),
        "kept_wr": to_float(filter_row.get("kept_wr")),
        "kept_mean_r": to_float(filter_row.get("kept_mean_r")),
        "trade_flow_retained_pct": to_float(filter_row.get("trade_flow_retained_pct")),
        "target_wr_pct": to_float(filter_row.get("target_wr_pct")),
        "target_wr_met": filter_row.get("target_wr_met") in {True, "True", "true"},
        "kept_wr_lcb_80": to_float(filter_row.get("kept_wr_lcb_80")),
        "precision_quality_score": to_float(filter_row.get("precision_quality_score")),
        "precision_min_kept": to_float(filter_row.get("precision_min_kept")),
        "precision_min_retained_pct": to_float(filter_row.get("precision_min_retained_pct")),
        "precision_lcb_floor": to_float(filter_row.get("precision_lcb_floor")),
        "resolved_rows_used": len(trades),
        "selection_basis": str(filter_row.get("selection_basis") or "all_resolved_trades"),
        "kept_trade_keys": kept_keys,
        "blocked_trade_keys_hash": hash_strings(blocked_keys),
        "kept_trade_fingerprint": hash_strings(kept_keys),
        "trade_mix": summarize_trade_mix(kept),
        "rule_snapshots": rule_snapshots,
    }


def append_drift_history(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    history = read_jsonl(DRIFT_HISTORY_PATH)
    history.append(snapshot)
    if len(history) > MAX_DRIFT_HISTORY:
        history = history[-MAX_DRIFT_HISTORY:]
    write_jsonl(DRIFT_HISTORY_PATH, history, keep_backup=True)
    return history


def latest_non_null(
    history: list[dict[str, Any]],
    lane_name: str,
    *,
    skip_current: bool = True,
    generation: str | None = None,
) -> dict[str, Any] | None:
    rows = filter_history_rows(history, generation=generation, skip_current=skip_current)
    for item in reversed(rows):
        lane = item.get("lanes", {}).get(lane_name)
        if lane:
            return lane
    return None


def lane_rule_persistence(
    history: list[dict[str, Any]],
    lane_name: str,
    current_lane: dict[str, Any] | None,
    *,
    lookback: int = 20,
    generation: str | None = None,
) -> list[dict[str, Any]]:
    if not current_lane:
        return []
    rows = filter_history_rows(history, generation=generation, lookback=lookback)
    current_rules = current_lane.get("rule_ids") or []
    out = []
    for rule_id in current_rules:
        appearances = 0
        streak = 0
        streak_active = True
        support_series = []
        wr_series = []
        mean_r_series = []
        for item in reversed(rows):
            lane = item.get("lanes", {}).get(lane_name) or {}
            rules = lane.get("rule_ids") or []
            if rule_id in rules:
                appearances += 1
                snap_map = {r.get("candidate_id"): r for r in lane.get("rule_snapshots") or []}
                snap = snap_map.get(rule_id) or {}
                if snap.get("support_n") is not None:
                    support_series.append(float(snap["support_n"]))
                if snap.get("win_rate_pct") is not None:
                    wr_series.append(float(snap["win_rate_pct"]))
                if snap.get("mean_r") is not None:
                    mean_r_series.append(float(snap["mean_r"]))
                if streak_active:
                    streak += 1
            else:
                streak_active = False
        current_snap = next((r for r in current_lane.get("rule_snapshots") or [] if r.get("candidate_id") == rule_id), {})
        out.append(
            {
                "candidate_id": rule_id,
                "candidate_label": current_snap.get("candidate_label") or rule_id,
                "appearances_last_n": appearances,
                "appearance_rate_pct": round((100.0 * appearances / len(rows)), 4) if rows else None,
                "consecutive_streak": streak,
                "current_support_n": current_snap.get("support_n"),
                "current_wr": current_snap.get("win_rate_pct"),
                "current_mean_r": current_snap.get("mean_r"),
                "avg_support_n": round(sum(support_series) / len(support_series), 4) if support_series else None,
                "avg_wr": round(sum(wr_series) / len(wr_series), 4) if wr_series else None,
                "avg_mean_r": round(sum(mean_r_series) / len(mean_r_series), 4) if mean_r_series else None,
            }
        )
    out.sort(key=lambda row: (row["appearances_last_n"], row["consecutive_streak"], row["current_support_n"] or 0), reverse=True)
    return out


def metric_series(history: list[dict[str, Any]], lane_name: str, metric: str, *, limit: int = 12) -> list[dict[str, Any]]:
    rows = []
    for item in filter_history_rows(history, lookback=limit):
        lane = item.get("lanes", {}).get(lane_name)
        if not lane:
            continue
        rows.append(
            {
                "generated_at": item.get("generated_at"),
                "engine_generation": infer_engine_generation(item),
                "simulation_id": lane.get("simulation_id"),
                "metric": metric,
                "value": lane.get(metric),
            }
        )
    return rows


def metric_drift_summary(
    history: list[dict[str, Any]],
    lane_name: str,
    metric: str,
    *,
    limit: int = 12,
    flat_threshold: float = 0.0,
    generation: str | None = None,
) -> dict[str, Any]:
    series = []
    for item in filter_history_rows(history, generation=generation, lookback=limit):
        lane = item.get("lanes", {}).get(lane_name)
        if not lane:
            continue
        series.append(
            {
                "generated_at": item.get("generated_at"),
                "engine_generation": infer_engine_generation(item),
                "simulation_id": lane.get("simulation_id"),
                "metric": metric,
                "value": lane.get(metric),
            }
        )
    values = [to_float(item.get("value")) for item in series]
    usable = [value for value in values if value is not None]
    if not usable:
        return {
            "metric": metric,
            "history_depth": 0,
            "trend": "insufficient_history",
            "series": series,
        }
    current_value = usable[-1]
    previous_value = usable[-2] if len(usable) >= 2 else None
    first_value = usable[0]
    delta_total = round(current_value - first_value, 4)
    delta_recent = round(current_value - previous_value, 4) if previous_value is not None else None
    if len(usable) < 3:
        trend = "warming_up"
    elif abs(delta_total) <= flat_threshold and (delta_recent is None or abs(delta_recent) <= flat_threshold):
        trend = "flat"
    elif delta_total > flat_threshold and (delta_recent is None or delta_recent > (-flat_threshold)):
        trend = "rising"
    elif delta_total < (-flat_threshold) and (delta_recent is None or delta_recent < flat_threshold):
        trend = "falling"
    else:
        trend = "volatile"
    return {
        "metric": metric,
        "history_depth": len(usable),
        "current": current_value,
        "previous": previous_value,
        "first": first_value,
        "delta_total": delta_total,
        "delta_recent": delta_recent,
        "min": round(min(usable), 4),
        "max": round(max(usable), 4),
        "trend": trend,
        "series": series,
    }


def persistent_core_rules(rule_persistence: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    if not rule_persistence:
        return []
    preferred = [
        row for row in rule_persistence
        if (to_float(row.get("appearance_rate_pct")) or 0.0) >= 60.0
        or (to_float(row.get("consecutive_streak")) or 0.0) >= 3.0
    ]
    pool = preferred or rule_persistence
    rows = []
    for row in pool[:limit]:
        rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "candidate_label": row.get("candidate_label"),
                "appearance_rate_pct": row.get("appearance_rate_pct"),
                "consecutive_streak": row.get("consecutive_streak"),
                "current_support_n": row.get("current_support_n"),
                "current_wr": row.get("current_wr"),
                "current_mean_r": row.get("current_mean_r"),
            }
        )
    return rows


def lane_assessment(
    lane_name: str,
    current_lane: dict[str, Any],
    *,
    history_depth: int,
    rule_overlap_pct: float | None,
    trade_overlap_pct: float | None,
    composition_shift_avg_pct: float | None,
    wr_drift: dict[str, Any],
    mean_r_drift: dict[str, Any],
    retained_drift: dict[str, Any],
    persistent_rules: list[dict[str, Any]],
    resolved_rows_delta: float | None = None,
    resolved_rows_current: float | None = None,
) -> dict[str, Any]:
    current_wr = to_float(current_lane.get("kept_wr")) or 0.0
    current_mean_r = to_float(current_lane.get("kept_mean_r")) or 0.0
    current_retained = to_float(current_lane.get("trade_flow_retained_pct")) or 0.0
    current_support = to_float(current_lane.get("kept_n")) or 0.0
    target_wr = to_float(current_lane.get("target_wr_pct"))
    target_met = bool(current_lane.get("target_wr_met"))

    overlaps = [value for value in [rule_overlap_pct, trade_overlap_pct] if value is not None]
    continuity_score = avg_non_null(overlaps)
    persistence_score = avg_non_null([
        to_float(rule.get("appearance_rate_pct"))
        for rule in persistent_rules
    ])
    history_score = clamp(history_depth * 8.0)
    support_score = clamp(current_support / 2.0)
    composition_score = None if composition_shift_avg_pct is None else clamp(100.0 - composition_shift_avg_pct)
    resolved_delta_floor = None
    if resolved_rows_current is not None:
        resolved_delta_floor = max(12.0, resolved_rows_current * 0.02)
    elif resolved_rows_delta is not None:
        resolved_delta_floor = 12.0
    small_resolved_delta = (
        resolved_rows_delta is not None
        and resolved_delta_floor is not None
        and abs(resolved_rows_delta) < resolved_delta_floor
    )

    if lane_name == "precision":
        effective_target = target_wr if target_wr is not None else 50.0
        wr_margin = current_wr - effective_target
        performance_score = clamp(60.0 + (wr_margin * 6.0) + (current_mean_r * 20.0))
        if target_met:
            performance_score = max(performance_score, 72.0)
    else:
        performance_score = clamp(50.0 + (current_mean_r * 110.0) + max(0.0, current_wr - 40.0) * 1.5 + max(0.0, current_retained - 60.0) * 0.35)

    component_scores = [
        history_score,
        support_score,
        continuity_score if continuity_score is not None else 55.0,
        persistence_score if persistence_score is not None else 55.0,
        performance_score,
        composition_score if composition_score is not None else 55.0,
    ]
    stability_score = round(
        (0.12 * component_scores[0])
        + (0.12 * component_scores[1])
        + (0.20 * component_scores[2])
        + (0.16 * component_scores[3])
        + (0.28 * component_scores[4])
        + (0.12 * component_scores[5]),
        4,
    )

    flags: list[str] = []
    if history_depth < 3:
        flags.append("insufficient_history")
    if lane_name == "precision" and not target_met:
        flags.append("target_not_met")
    if (rule_overlap_pct is not None and rule_overlap_pct < 60.0) or (trade_overlap_pct is not None and trade_overlap_pct < 60.0):
        if small_resolved_delta:
            flags.append("selection_rotation_watch")
        else:
            flags.append("selection_rotation")
    if composition_shift_avg_pct is not None and composition_shift_avg_pct >= 25.0:
        flags.append("composition_shift")
    if wr_drift.get("trend") == "falling":
        flags.append("wr_softening")
    if mean_r_drift.get("trend") == "falling":
        flags.append("expectancy_softening")
    if retained_drift.get("trend") == "falling":
        flags.append("retention_softening")
    if current_mean_r <= 0:
        flags.append("non_positive_expectancy")

    if history_depth < 3:
        grade = "warming_up"
    elif lane_name == "precision" and not target_met:
        grade = "off_target"
    elif stability_score >= 78.0 and (continuity_score or 0.0) >= 75.0 and (composition_shift_avg_pct or 0.0) <= 18.0 and current_mean_r > 0:
        if wr_drift.get("trend") == "rising" or mean_r_drift.get("trend") == "rising":
            grade = "strengthening"
        else:
            grade = "stable"
    elif current_mean_r > 0 and stability_score >= 68.0 and ((continuity_score or 0.0) >= 55.0):
        if "selection_rotation" in flags or "selection_rotation_watch" in flags or "composition_shift" in flags:
            grade = "rotating"
        elif "wr_softening" in flags or "expectancy_softening" in flags:
            grade = "softening"
        else:
            grade = "stable"
    elif current_mean_r > 0:
        if small_resolved_delta and (target_met or lane_name != "precision") and (continuity_score or 0.0) >= 40.0:
            grade = "rotating"
        else:
            grade = "fragile"
    else:
        grade = "degrading"

    return {
        "history_depth": history_depth,
        "history_score": round(history_score, 4),
        "support_score": round(support_score, 4),
        "continuity_score": round(continuity_score, 4) if continuity_score is not None else None,
        "persistence_score": round(persistence_score, 4) if persistence_score is not None else None,
        "performance_score": round(performance_score, 4),
        "composition_score": round(composition_score, 4) if composition_score is not None else None,
        "resolved_rows_delta": round(resolved_rows_delta, 4) if resolved_rows_delta is not None else None,
        "resolved_rows_current": round(resolved_rows_current, 4) if resolved_rows_current is not None else None,
        "small_resolved_delta_threshold": round(resolved_delta_floor, 4) if resolved_delta_floor is not None else None,
        "small_resolved_delta_flag": small_resolved_delta,
        "stability_score": stability_score,
        "stability_grade": grade,
        "assessment_flags": flags,
    }


def trend_delta(current: float | None, average: float | None) -> float | None:
    if current is None or average is None:
        return None
    return round(current - average, 4)


def candidate_watch_status(
    current: dict[str, Any],
    history_rows: list[dict[str, Any]],
    total_snapshots: int,
) -> dict[str, Any]:
    current_support = to_float(current.get("support_n")) or 0.0
    current_wr = to_float(current.get("win_rate_pct")) or 0.0
    current_mean_r = to_float(current.get("mean_r")) or 0.0
    current_top_symbol = to_float(current.get("top_symbol_pct")) or 0.0
    current_top_day = to_float(current.get("top_day_pct")) or 0.0
    margin_score = candidate_margin_score(current)
    support_floor = candidate_support_floor(current)
    seen_count = len(history_rows)
    appearance_rate = round((100.0 * seen_count / total_snapshots), 4) if total_snapshots > 0 else None

    consecutive_streak = 0
    for row in reversed(history_rows):
        if row.get("promotion_decision") == current.get("promotion_decision"):
            consecutive_streak += 1
        else:
            break

    decision_matches = sum(1 for row in history_rows if row.get("promotion_decision") == current.get("promotion_decision"))
    decision_consistency = round((100.0 * decision_matches / seen_count), 4) if seen_count > 0 else None

    avg_support = avg_non_null([to_float(row.get("support_n")) for row in history_rows])
    avg_wr = avg_non_null([to_float(row.get("win_rate_pct")) for row in history_rows])
    avg_mean_r = avg_non_null([to_float(row.get("mean_r")) for row in history_rows])
    avg_margin = avg_non_null([candidate_margin_score(row) for row in history_rows])

    previous_row = history_rows[-2] if len(history_rows) >= 2 else None
    previous_wr = to_float((previous_row or {}).get("win_rate_pct"))
    previous_mean_r = to_float((previous_row or {}).get("mean_r"))
    previous_margin = candidate_margin_score(previous_row) if previous_row else None

    wr_delta_from_avg = trend_delta(current_wr, avg_wr)
    mean_r_delta_from_avg = trend_delta(current_mean_r, avg_mean_r)
    margin_delta_from_avg = trend_delta(margin_score, avg_margin)
    wr_delta_recent = trend_delta(current_wr, previous_wr)
    mean_r_delta_recent = trend_delta(current_mean_r, previous_mean_r)
    margin_delta_recent = trend_delta(margin_score, previous_margin)

    decision = str(current.get("promotion_decision") or "")
    reasons: list[str] = []
    risk_score = 0.0

    if seen_count < 3:
        risk_score += 20.0
        reasons.append("short history")
    elif seen_count < 5:
        risk_score += 10.0
        reasons.append("limited history")

    if appearance_rate is not None and appearance_rate < 50.0:
        risk_score += 18.0
        reasons.append("low appearance rate")
    elif appearance_rate is not None and appearance_rate < 75.0:
        risk_score += 8.0
        reasons.append("moderate appearance rate")

    if decision_consistency is not None and decision_consistency < 60.0:
        risk_score += 22.0
        reasons.append("decision instability")
    elif decision_consistency is not None and decision_consistency < 80.0:
        risk_score += 10.0
        reasons.append("decision drift")

    if consecutive_streak < 2:
        risk_score += 12.0
        reasons.append("no persistent streak")
    elif consecutive_streak < 4:
        risk_score += 5.0

    if current_support < support_floor:
        risk_score += 15.0
        reasons.append("support below floor")
    elif current_support < (support_floor + 5.0):
        risk_score += 6.0
        reasons.append("thin support")

    if current_top_day >= 75.0:
        risk_score += 20.0
        reasons.append("extreme day concentration")
    elif current_top_day >= 60.0:
        risk_score += 10.0
        reasons.append("high day concentration")

    if current_top_symbol >= 25.0:
        risk_score += 14.0
        reasons.append("high symbol concentration")
    elif current_top_symbol >= 15.0:
        risk_score += 6.0
        reasons.append("moderate symbol concentration")

    if margin_score < 0.25:
        risk_score += 28.0
        reasons.append("edge margin near boundary")
    elif margin_score < 1.0:
        risk_score += 16.0
        reasons.append("edge margin compressing")
    elif margin_score < 2.0:
        risk_score += 8.0

    if decision == "boost":
        if (wr_delta_from_avg or 0.0) <= -4.0 or (wr_delta_recent or 0.0) <= -2.0:
            risk_score += 12.0
            reasons.append("win rate softening")
        if (mean_r_delta_from_avg or 0.0) <= -0.12 or (mean_r_delta_recent or 0.0) <= -0.08:
            risk_score += 14.0
            reasons.append("expectancy softening")
        if current_mean_r <= 0.0:
            risk_score += 18.0
            reasons.append("expectancy non-positive")
    elif decision == "hard_block":
        if (wr_delta_from_avg or 0.0) >= 4.0 or (wr_delta_recent or 0.0) >= 2.0:
            risk_score += 12.0
            reasons.append("win rate recovering")
        if (mean_r_delta_from_avg or 0.0) >= 0.12 or (mean_r_delta_recent or 0.0) >= 0.08:
            risk_score += 14.0
            reasons.append("loss severity easing")
        if current_mean_r >= 0.0:
            risk_score += 18.0
            reasons.append("negative expectancy broken")
    else:
        if (wr_delta_from_avg or 0.0) >= 4.0 or (mean_r_delta_from_avg or 0.0) >= 0.10:
            risk_score += 12.0
            reasons.append("uplift underperformance easing")
        if margin_score < 0.25:
            risk_score += 10.0

    if (margin_delta_from_avg or 0.0) <= -1.0 or (margin_delta_recent or 0.0) <= -0.75:
        risk_score += 10.0
        reasons.append("margin erosion")

    if decision == "boost" and current_mean_r > 0 and margin_score >= 2.0 and seen_count >= 5 and (appearance_rate or 0.0) >= 75.0 and (decision_consistency or 0.0) >= 85.0 and current_top_day < 60.0:
        stable_hint = True
    elif decision == "hard_block" and current_mean_r < -0.25 and margin_score >= 2.0 and seen_count >= 5 and (appearance_rate or 0.0) >= 75.0 and (decision_consistency or 0.0) >= 85.0 and current_top_day < 60.0:
        stable_hint = True
    else:
        stable_hint = False

    risk_score = clamp(risk_score)
    durability_score = round(100.0 - risk_score, 4)

    if seen_count < 3:
        status = "fragile"
    elif risk_score >= 78.0 or (
        margin_score < 0.15
        and (
            (decision_consistency is not None and decision_consistency < 70.0)
            or current_top_day >= 70.0
            or ((decision == "boost") and ((mean_r_delta_from_avg or 0.0) <= -0.15))
            or ((decision == "hard_block") and ((mean_r_delta_from_avg or 0.0) >= 0.15))
        )
    ):
        status = "demote_now"
    elif risk_score >= 60.0:
        status = "demotion_watch"
    elif stable_hint and risk_score <= 25.0:
        status = "stable_core"
    elif risk_score >= 40.0:
        status = "fragile"
    elif risk_score >= 25.0 or ("win rate softening" in reasons or "expectancy softening" in reasons or "loss severity easing" in reasons):
        status = "softening"
    else:
        status = "stable_core"

    return {
        "status": status,
        "durability_score": durability_score,
        "watch_risk_score": round(risk_score, 4),
        "seen_count": seen_count,
        "appearance_rate_pct": appearance_rate,
        "consecutive_streak": consecutive_streak,
        "decision_consistency_pct": decision_consistency,
        "current_margin_score": margin_score,
        "avg_margin_score": avg_margin,
        "previous_margin_score": previous_margin,
        "wr_delta_from_avg": wr_delta_from_avg,
        "mean_r_delta_from_avg": mean_r_delta_from_avg,
        "margin_delta_from_avg": margin_delta_from_avg,
        "wr_delta_recent": wr_delta_recent,
        "mean_r_delta_recent": mean_r_delta_recent,
        "margin_delta_recent": margin_delta_recent,
        "current_top_symbol_pct": current_top_symbol,
        "current_top_day_pct": current_top_day,
        "avg_support_n": avg_support,
        "avg_wr": avg_wr,
        "avg_mean_r": avg_mean_r,
        "reasons": reasons[:8],
    }


def build_candidate_watchlist(
    summary: dict[str, Any],
    promotions: list[dict[str, Any]],
    promotion_history: list[dict[str, Any]],
    *,
    lookback: int = 24,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    promoted_rows = [
        row for row in promotions
        if row.get("promotion_decision") in {"boost", "hard_block", "quality_uplift"}
    ]
    current_generation = normalize_generation(summary.get("engine_generation"))
    promoted_ids = {str(row.get("candidate_id")) for row in promoted_rows if row.get("candidate_id")}
    history_index, total_snapshots = candidate_history_index(
        promotion_history,
        promoted_ids,
        lookback=lookback,
        generation=current_generation,
    )
    full_history_index, total_snapshots_all = candidate_history_index(
        promotion_history,
        promoted_ids,
        lookback=lookback,
        generation=None,
    )
    watch_rows: list[dict[str, Any]] = []

    def stable_core_quality(status: str | None, row: dict[str, Any]) -> str | None:
        if status != "stable_core":
            return None
        decision = str(row.get("promotion_decision") or "")
        mean_r = to_float(row.get("mean_r"))
        delta_wr = to_float(row.get("delta_wr_vs_engine"))
        delta_r = to_float(row.get("delta_r_vs_engine"))
        if decision == "boost" and (mean_r or 0.0) > 0 and ((delta_wr is None or delta_wr > 0) or (delta_r is None or delta_r > 0)):
            return "stable_core_positive"
        if decision == "hard_block" and ((mean_r is not None and mean_r < 0) or (delta_wr is not None and delta_wr <= -5.0)):
            return "stable_core_negative"
        if mean_r is not None and mean_r < 0:
            return "stable_core_negative"
        return "stable_core_neutral"

    for row in promoted_rows:
        candidate_id = str(row.get("candidate_id"))
        health = candidate_watch_status(row, history_index.get(candidate_id, []), total_snapshots)
        cross_generation_seen = max(0, len(full_history_index.get(candidate_id, [])) - len(history_index.get(candidate_id, [])))
        watch_status = health.get("status")
        watch_rows.append(
            {
                "candidate_id": candidate_id,
                "candidate_label": row.get("candidate_label") or candidate_id,
                "engine_generation": current_generation,
                "engine": row.get("engine"),
                "candidate_type": row.get("candidate_type"),
                "promotion_decision": row.get("promotion_decision"),
                "watch_status": watch_status,
                "stable_core_quality": stable_core_quality(watch_status, row),
                "durability_score": health.get("durability_score"),
                "watch_risk_score": health.get("watch_risk_score"),
                "support_n": to_float(row.get("support_n")),
                "win_rate_pct": to_float(row.get("win_rate_pct")),
                "mean_r": to_float(row.get("mean_r")),
                "delta_wr_vs_engine": to_float(row.get("delta_wr_vs_engine")),
                "delta_r_vs_engine": to_float(row.get("delta_r_vs_engine")),
                "current_margin_score": health.get("current_margin_score"),
                "avg_margin_score": health.get("avg_margin_score"),
                "seen_count": health.get("seen_count"),
                "same_generation_seen_count": health.get("seen_count"),
                "cross_generation_seen_count": cross_generation_seen,
                "appearance_rate_pct": health.get("appearance_rate_pct"),
                "consecutive_streak": health.get("consecutive_streak"),
                "decision_consistency_pct": health.get("decision_consistency_pct"),
                "top_symbol_pct": health.get("current_top_symbol_pct"),
                "top_day_pct": health.get("current_top_day_pct"),
                "wr_delta_from_avg": health.get("wr_delta_from_avg"),
                "mean_r_delta_from_avg": health.get("mean_r_delta_from_avg"),
                "margin_delta_from_avg": health.get("margin_delta_from_avg"),
                "wr_delta_recent": health.get("wr_delta_recent"),
                "mean_r_delta_recent": health.get("mean_r_delta_recent"),
                "margin_delta_recent": health.get("margin_delta_recent"),
                "reasons": " ; ".join(health.get("reasons") or []),
            }
        )

    status_order = {
        "demote_now": 0,
        "demotion_watch": 1,
        "fragile": 2,
        "softening": 3,
        "stable_core": 4,
    }
    watch_rows.sort(
        key=lambda row: (
            status_order.get(str(row.get("watch_status")), 99),
            -(to_float(row.get("watch_risk_score")) or 0.0),
            (to_float(row.get("durability_score")) or 0.0),
            -(to_float(row.get("support_n")) or 0.0),
        )
    )

    def top_status_rows(rows_in: list[dict[str, Any]], status: str, limit: int = 5) -> list[dict[str, Any]]:
        rows = [row for row in rows_in if row.get("watch_status") == status]
        rows.sort(
            key=lambda row: (
                -(to_float(row.get("watch_risk_score")) or 0.0),
                -(to_float(row.get("support_n")) or 0.0),
            )
        )
        if status == "stable_core":
            rows.sort(
                key=lambda row: (
                    -(to_float(row.get("durability_score")) or 0.0),
                    -(to_float(row.get("support_n")) or 0.0),
                )
            )
        return rows[:limit]

    def top_stable_quality_rows(rows_in: list[dict[str, Any]], quality: str, limit: int = 5) -> list[dict[str, Any]]:
        rows = [row for row in rows_in if row.get("stable_core_quality") == quality]
        rows.sort(
            key=lambda row: (
                -(to_float(row.get("durability_score")) or 0.0),
                -(to_float(row.get("support_n")) or 0.0),
            )
        )
        return rows[:limit]

    def structural_hard_block_rows(rows_in: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
        rows = [
            row for row in rows_in
            if row.get("promotion_decision") == "hard_block"
            and row.get("stable_core_quality") == "stable_core_negative"
            and (to_float(row.get("support_n")) or 0.0) >= 100.0
            and (to_float(row.get("win_rate_pct")) or 100.0) <= 15.0
        ]
        rows.sort(
            key=lambda row: (
                to_float(row.get("win_rate_pct")) or 100.0,
                to_float(row.get("mean_r")) or 0.0,
                -(to_float(row.get("support_n")) or 0.0),
            )
        )
        return rows[:limit]

    def fast_track_hard_block_rows(rows_in: list[dict[str, Any]], limit: int = 30) -> list[dict[str, Any]]:
        rows = [
            row for row in rows_in
            if row.get("promotion_decision") == "hard_block"
            and row.get("watch_status") == "demote_now"
            and (to_float(row.get("support_n")) or 0.0) >= candidate_support_floor(row)
        ]
        rows.sort(
            key=lambda row: (
                -(to_float(row.get("watch_risk_score")) or 0.0),
                to_float(row.get("win_rate_pct")) or 100.0,
                -(to_float(row.get("support_n")) or 0.0),
            )
        )
        return rows[:limit]

    def status_summary(rows_in: list[dict[str, Any]], *, top_limit: int = 5) -> dict[str, Any]:
        counts = Counter(str(row.get("watch_status")) for row in rows_in)
        quality_counts = Counter(str(row.get("stable_core_quality")) for row in rows_in if row.get("stable_core_quality"))
        return {
            "candidate_count": len(rows_in),
            "status_counts": {
                "stable_core": counts.get("stable_core", 0),
                "stable_core_positive": quality_counts.get("stable_core_positive", 0),
                "stable_core_negative": quality_counts.get("stable_core_negative", 0),
                "stable_core_neutral": quality_counts.get("stable_core_neutral", 0),
                "softening": counts.get("softening", 0),
                "fragile": counts.get("fragile", 0),
                "demotion_watch": counts.get("demotion_watch", 0),
                "demote_now": counts.get("demote_now", 0),
            },
            "top_stable_core": top_status_rows(rows_in, "stable_core", top_limit),
            "top_stable_core_positive": top_stable_quality_rows(rows_in, "stable_core_positive", top_limit),
            "top_stable_core_negative": top_stable_quality_rows(rows_in, "stable_core_negative", top_limit),
            "top_softening": top_status_rows(rows_in, "softening", top_limit),
            "top_fragile": top_status_rows(rows_in, "fragile", top_limit),
            "top_demotion_watch": top_status_rows(rows_in, "demotion_watch", top_limit),
            "top_demote_now": top_status_rows(rows_in, "demote_now", top_limit),
        }

    full_status_summary = status_summary(watch_rows)
    by_decision: dict[str, Any] = {}
    for decision in ["boost", "hard_block", "quality_uplift"]:
        decision_rows = [row for row in watch_rows if row.get("promotion_decision") == decision]
        by_decision[decision] = status_summary(decision_rows, top_limit=5)

    watch_index = {str(row.get("candidate_id")): row for row in watch_rows if row.get("candidate_id")}

    def lane_rule_watch(filter_row: dict[str, Any] | None) -> dict[str, Any]:
        if not filter_row:
            return {}
        rule_ids = safe_json_loads(filter_row.get("rule_ids_json"), [])
        rows = [watch_index[rule_id] for rule_id in rule_ids if rule_id in watch_index]
        lane_summary = status_summary(rows, top_limit=8)
        lane_summary["rule_count"] = len(rows)
        lane_summary["rules"] = rows
        lane_summary["worst_status"] = next(
            (
                status for status in ["demote_now", "demotion_watch", "fragile", "softening", "stable_core"]
                if lane_summary["status_counts"].get(status, 0) > 0
            ),
            None,
        )
        return lane_summary

    summary_payload = {
        "generated_at": now_iso(),
        "engine_generation": current_generation,
        "research_scope": summary.get("research_scope"),
        "resolved_rows_used": summary.get("resolved_rows_used"),
        "comparison_scope": "within_generation",
        "lookback_snapshots": total_snapshots,
        "all_generation_lookback_snapshots": total_snapshots_all,
        "cross_generation_history_present": total_snapshots_all > total_snapshots,
        "promoted_candidate_count": len(watch_rows),
        **full_status_summary,
        "by_promotion_decision": by_decision,
        "precision_rule_watch": lane_rule_watch(summary.get("best_precision_filter")),
        "balanced_rule_watch": lane_rule_watch(summary.get("best_balanced_filter")),
        "structural_hard_blocks": structural_hard_block_rows(watch_rows),
        "fast_track_hard_blocks": fast_track_hard_block_rows(watch_rows),
    }
    return summary_payload, watch_rows


def lane_drift_report(
    lane_name: str,
    current_lane: dict[str, Any] | None,
    previous_lane: dict[str, Any] | None,
    history: list[dict[str, Any]],
    *,
    generation: str | None = None,
    previous_any_lane: dict[str, Any] | None = None,
    previous_any_generation: str | None = None,
    resolved_rows_delta: float | None = None,
    resolved_rows_current: float | None = None,
) -> dict[str, Any] | None:
    if not current_lane:
        return None
    comparison_generation = normalize_generation(generation or current_lane.get("engine_generation"))
    current_rules = set(current_lane.get("rule_ids") or [])
    current_trades = set(current_lane.get("kept_trade_keys") or [])
    current_rule_map = {r.get("candidate_id"): r for r in current_lane.get("rule_snapshots") or []}
    previous_rule_map = {r.get("candidate_id"): r for r in (previous_lane or {}).get("rule_snapshots") or []}
    current_rule_weights = {
        key: max(1.0, to_float(value.get("support_n")) or 1.0)
        for key, value in current_rule_map.items()
    }
    previous_rule_weights = {
        key: max(1.0, to_float(value.get("support_n")) or 1.0)
        for key, value in previous_rule_map.items()
    }
    previous_rules = set(previous_rule_map) if previous_lane else set()
    previous_trades = set((previous_lane or {}).get("kept_trade_keys") or []) if previous_lane else set()
    if previous_lane:
        added_rules = [current_rule_map[r].get("candidate_label") or r for r in sorted(current_rules - previous_rules) if r in current_rule_map]
        removed_rules = [previous_rule_map[r].get("candidate_label") or r for r in sorted(previous_rules - current_rules) if r in previous_rule_map]
        rule_overlap_raw = jaccard_pct(current_rules, previous_rules)
        rule_overlap_weighted = weighted_jaccard_pct(current_rule_weights, previous_rule_weights)
        rule_overlap = rule_overlap_weighted if rule_overlap_weighted is not None else rule_overlap_raw
        trade_overlap = jaccard_pct(current_trades, previous_trades)
    else:
        added_rules = []
        removed_rules = []
        rule_overlap_raw = None
        rule_overlap_weighted = None
        rule_overlap = None
        trade_overlap = None
    current_mix = current_lane.get("trade_mix") or {}
    previous_mix = (previous_lane or {}).get("trade_mix") or {}
    rule_persistence = lane_rule_persistence(history, lane_name, current_lane, generation=comparison_generation)
    core_rules = persistent_core_rules(rule_persistence)
    wr_drift = metric_drift_summary(history, lane_name, "kept_wr", flat_threshold=0.5, generation=comparison_generation)
    mean_r_drift = metric_drift_summary(history, lane_name, "kept_mean_r", flat_threshold=0.05, generation=comparison_generation)
    retained_drift = metric_drift_summary(history, lane_name, "trade_flow_retained_pct", flat_threshold=1.0, generation=comparison_generation)
    if previous_lane:
        composition_shift = {
            "engine_mix_shift_pct": mix_shift_pct(current_mix.get("engine_mix_map") or {}, previous_mix.get("engine_mix_map") or {}),
            "session_mix_shift_pct": mix_shift_pct(current_mix.get("session_mix_map") or {}, previous_mix.get("session_mix_map") or {}),
            "regime_mix_shift_pct": mix_shift_pct(current_mix.get("regime_mix_map") or {}, previous_mix.get("regime_mix_map") or {}),
            "structure_alignment_shift_pct": mix_shift_pct(current_mix.get("structure_alignment_mix_map") or {}, previous_mix.get("structure_alignment_mix_map") or {}),
        }
    else:
        composition_shift = {
            "engine_mix_shift_pct": None,
            "session_mix_shift_pct": None,
            "regime_mix_shift_pct": None,
            "structure_alignment_shift_pct": None,
        }
    composition_shift_avg = avg_non_null(list(composition_shift.values()))
    assessment = lane_assessment(
        lane_name,
        current_lane,
        history_depth=max(wr_drift.get("history_depth") or 0, mean_r_drift.get("history_depth") or 0, retained_drift.get("history_depth") or 0),
        rule_overlap_pct=rule_overlap,
        trade_overlap_pct=trade_overlap,
        composition_shift_avg_pct=composition_shift_avg,
        wr_drift=wr_drift,
        mean_r_drift=mean_r_drift,
        retained_drift=retained_drift,
        persistent_rules=core_rules,
        resolved_rows_delta=resolved_rows_delta,
        resolved_rows_current=resolved_rows_current,
    )

    def top_label(mix: dict[str, Any], key: str) -> str | None:
        rows = mix.get(key) or []
        return rows[0].get("label") if rows else None

    prev_kept_n = to_float((previous_lane or {}).get("kept_n"))
    prev_kept_wr = to_float((previous_lane or {}).get("kept_wr"))
    prev_kept_mean_r = to_float((previous_lane or {}).get("kept_mean_r"))
    prev_retained_pct = to_float((previous_lane or {}).get("trade_flow_retained_pct"))

    cross_generation = None
    previous_any_generation_norm = normalize_generation(previous_any_generation)
    if previous_any_lane and previous_any_generation_norm != comparison_generation:
        cross_rules = set(previous_any_lane.get("rule_ids") or [])
        cross_trades = set(previous_any_lane.get("kept_trade_keys") or [])
        cross_generation = {
            "previous_generation": previous_any_generation_norm,
            "current_generation": comparison_generation,
            "rule_overlap_pct": jaccard_pct(current_rules, cross_rules),
            "trade_overlap_pct": jaccard_pct(current_trades, cross_trades),
            "delta_kept_n": round((current_lane.get("kept_n") or 0) - (to_float(previous_any_lane.get("kept_n")) or 0), 4),
            "delta_kept_wr": round((to_float(current_lane.get("kept_wr")) or 0.0) - (to_float(previous_any_lane.get("kept_wr")) or 0.0), 4),
            "delta_kept_mean_r": round((to_float(current_lane.get("kept_mean_r")) or 0.0) - (to_float(previous_any_lane.get("kept_mean_r")) or 0.0), 4),
            "delta_retained_pct": round((to_float(current_lane.get("trade_flow_retained_pct")) or 0.0) - (to_float(previous_any_lane.get("trade_flow_retained_pct")) or 0.0), 4),
        }

    return {
        "engine_generation": comparison_generation,
        "comparison_scope": "within_generation",
        "same_generation_previous_exists": previous_lane is not None,
        "generation_transition_detected": cross_generation is not None,
        "generation_transition_from": previous_any_generation_norm if cross_generation is not None else None,
        "cross_generation_comparison": cross_generation,
        "simulation_id_changed": (previous_lane or {}).get("simulation_id") != current_lane.get("simulation_id"),
        "rule_fingerprint_changed": (previous_lane or {}).get("rule_fingerprint") != current_lane.get("rule_fingerprint"),
        "kept_trade_fingerprint_changed": (previous_lane or {}).get("kept_trade_fingerprint") != current_lane.get("kept_trade_fingerprint"),
        "delta_kept_n": round((current_lane.get("kept_n") or 0) - prev_kept_n, 4) if prev_kept_n is not None else None,
        "delta_kept_wr": round((to_float(current_lane.get("kept_wr")) or 0.0) - prev_kept_wr, 4) if prev_kept_wr is not None else None,
        "delta_kept_mean_r": round((to_float(current_lane.get("kept_mean_r")) or 0.0) - prev_kept_mean_r, 4) if prev_kept_mean_r is not None else None,
        "delta_retained_pct": round((to_float(current_lane.get("trade_flow_retained_pct")) or 0.0) - prev_retained_pct, 4) if prev_retained_pct is not None else None,
        "rule_overlap_pct": rule_overlap,
        "rule_overlap_raw_pct": rule_overlap_raw,
        "rule_overlap_support_weighted_pct": rule_overlap_weighted,
        "trade_overlap_pct": trade_overlap,
        "rule_turnover_pct": None if rule_overlap is None else round(100.0 - rule_overlap, 4),
        "trade_turnover_pct": None if trade_overlap is None else round(100.0 - trade_overlap, 4),
        "added_rules": added_rules,
        "removed_rules": removed_rules,
        "current_top_engine": top_label(current_mix, "engine_mix"),
        "previous_top_engine": top_label(previous_mix, "engine_mix"),
        "current_top_session": top_label(current_mix, "session_mix"),
        "previous_top_session": top_label(previous_mix, "session_mix"),
        "current_top_regime": top_label(current_mix, "regime_mix"),
        "previous_top_regime": top_label(previous_mix, "regime_mix"),
        "current_symbol_concentration_pct": current_mix.get("symbol_concentration_pct"),
        "previous_symbol_concentration_pct": previous_mix.get("symbol_concentration_pct"),
        "composition_shift_pct": composition_shift,
        "composition_shift_avg_pct": composition_shift_avg,
        "wr_drift": wr_drift,
        "mean_r_drift": mean_r_drift,
        "retained_drift": retained_drift,
        "wr_series": wr_drift.get("series"),
        "mean_r_series": mean_r_drift.get("series"),
        "retained_series": retained_drift.get("series"),
        "rule_persistence": rule_persistence,
        "persistent_core_rules": core_rules,
        **assessment,
    }


def build_quant_drift_snapshot(
    summary: dict[str, Any],
    promotions: list[dict[str, Any]],
    filters: list[dict[str, Any]],
) -> dict[str, Any]:
    promotions_by_id = promotion_row_index(promotions)
    precision_row, balanced_row = select_filter_rows(filters)
    trades, _window_start, _window_end = pack.load_trades()
    precision_lane = build_filter_snapshot("precision", precision_row, promotions_by_id, trades)
    balanced_lane = build_filter_snapshot("balanced", balanced_row, promotions_by_id, trades)
    return {
        "generated_at": now_iso(),
        "engine_generation": normalize_generation(summary.get("engine_generation")),
        "research_scope": summary.get("research_scope"),
        "resolved_rows_used": summary.get("resolved_rows_used"),
        "window_start": summary.get("window_start"),
        "window_end": summary.get("window_end"),
        "promotion_counts": summary.get("promotion_counts"),
        "lanes": {
            "precision": precision_lane,
            "balanced": balanced_lane,
        },
    }


def build_drift_report(
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    previous_drift_snapshot: dict[str, Any] | None,
    current_drift_snapshot: dict[str, Any],
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    prev_counts = (previous_summary or {}).get("promotion_counts", {})
    cur_counts = (current_summary or {}).get("promotion_counts", {})
    current_generation = normalize_generation(current_summary.get("engine_generation") or current_drift_snapshot.get("engine_generation"))
    previous_generation = infer_engine_generation(previous_drift_snapshot)
    resolved_rows_delta = (current_summary.get("resolved_rows_used") or 0) - ((previous_summary or {}).get("resolved_rows_used") or 0)
    previous_precision = latest_non_null(history, "precision", skip_current=True, generation=current_generation)
    previous_balanced = latest_non_null(history, "balanced", skip_current=True, generation=current_generation)
    previous_precision_any = (previous_drift_snapshot or {}).get("lanes", {}).get("precision")
    previous_balanced_any = (previous_drift_snapshot or {}).get("lanes", {}).get("balanced")
    current_precision = (current_drift_snapshot or {}).get("lanes", {}).get("precision")
    current_balanced = (current_drift_snapshot or {}).get("lanes", {}).get("balanced")
    precision_report = lane_drift_report(
        "precision",
        current_precision,
        previous_precision,
        history,
        generation=current_generation,
        previous_any_lane=previous_precision_any,
        previous_any_generation=previous_generation,
        resolved_rows_delta=resolved_rows_delta,
        resolved_rows_current=current_summary.get("resolved_rows_used"),
    )
    balanced_report = lane_drift_report(
        "balanced",
        current_balanced,
        previous_balanced,
        history,
        generation=current_generation,
        previous_any_lane=previous_balanced_any,
        previous_any_generation=previous_generation,
        resolved_rows_delta=resolved_rows_delta,
        resolved_rows_current=current_summary.get("resolved_rows_used"),
    )
    summary_flags: list[str] = []
    if precision_report and precision_report.get("stability_grade") in {"off_target", "degrading", "fragile"}:
        summary_flags.append(f"precision_{precision_report.get('stability_grade')}")
    if balanced_report and balanced_report.get("stability_grade") in {"degrading", "fragile"}:
        summary_flags.append(f"balanced_{balanced_report.get('stability_grade')}")
    if precision_report and (precision_report.get("rule_turnover_pct") or 0.0) >= 40.0:
        summary_flags.append("precision_rule_rotation")
    if precision_report and (precision_report.get("composition_shift_avg_pct") or 0.0) >= 25.0:
        summary_flags.append("precision_composition_shift")
    if previous_generation != current_generation:
        summary_flags.append("generation_transition")
    return {
        "generated_at": now_iso(),
        "engine_generation": current_generation,
        "comparison_scope": "within_generation_primary",
        "previous_generation": previous_generation,
        "generation_transition_detected": previous_generation != current_generation,
        "research_scope": current_summary.get("research_scope"),
        "resolved_rows_previous": (previous_summary or {}).get("resolved_rows_used"),
        "resolved_rows_current": (current_summary or {}).get("resolved_rows_used"),
        "resolved_rows_delta": resolved_rows_delta,
        "promotion_count_delta": {
            "hard_block": (cur_counts.get("hard_block") or 0) - (prev_counts.get("hard_block") or 0),
            "boost": (cur_counts.get("boost") or 0) - (prev_counts.get("boost") or 0),
            "quality_uplift": (cur_counts.get("quality_uplift") or 0) - (prev_counts.get("quality_uplift") or 0),
        },
        "global_assessment": {
            "precision_grade": precision_report.get("stability_grade") if precision_report else None,
            "balanced_grade": balanced_report.get("stability_grade") if balanced_report else None,
            "precision_score": precision_report.get("stability_score") if precision_report else None,
            "balanced_score": balanced_report.get("stability_score") if balanced_report else None,
            "summary_flags": summary_flags,
        },
        "precision": precision_report,
        "balanced": balanced_report,
    }

def write_cycle_summary(
    summary: dict[str, Any],
    *,
    status: str,
    completed_at: str | None,
    drift_summary: dict[str, Any] | None = None,
    watchlist_summary: dict[str, Any] | None = None,
    persistence_health: dict[str, Any] | None = None,
) -> None:
    payload = {
        "quant_status": status,
        "generated_at": now_iso(),
        "last_completed_at": completed_at,
        **(summary or {}),
    }
    if drift_summary is not None:
        payload["drift_summary"] = drift_summary
    if watchlist_summary is not None:
        payload["promotion_watchlist"] = watchlist_summary
    if persistence_health is not None:
        payload["persistence_health"] = persistence_health

    precision_drift = (drift_summary or {}).get("precision") or {}
    precision_wr_trend = ((precision_drift.get("wr_drift") or {}).get("trend"))
    if precision_drift.get("stability_grade") == "off_target" and precision_wr_trend == "volatile":
        force_active_filter_highest_wr(
            payload,
            "precision_drift_off_target_volatile_wr_fallback_highest_wr",
        )

    quant_warnings = []
    precision_watch = (watchlist_summary or {}).get("precision_rule_watch") or {}
    precision_status = precision_watch.get("status_counts") or {}
    precision_has_no_stable_core = (
        (precision_watch.get("rule_count") or 0) > 0
        and int(precision_status.get("stable_core") or 0) == 0
    )
    if precision_has_no_stable_core:
        quant_warnings.append(
            {
                "code": "PRECISION_HEALTH_WARNING",
                "message": "Precision filter has no stable_core rules; warning only, precision selection remains WR-led.",
                "worst_status": precision_watch.get("worst_status"),
                "status_counts": precision_status,
            }
        )
    if precision_drift.get("stability_grade") == "off_target" and precision_wr_trend == "volatile":
        quant_warnings.append(
            {
                "code": "PRECISION_DRIFT_WARNING",
                "message": "Precision filter is off-target with volatile WR trend; active selector should use WR fallback comparison.",
                "stability_grade": precision_drift.get("stability_grade"),
                "wr_trend": precision_wr_trend,
                "mean_r_trend": (precision_drift.get("mean_r_drift") or {}).get("trend"),
            }
        )
    balanced_drift = (drift_summary or {}).get("balanced") or {}
    if (
        balanced_drift.get("stability_grade") == "softening"
        and (balanced_drift.get("wr_drift") or {}).get("trend") == "falling"
        and (balanced_drift.get("mean_r_drift") or {}).get("trend") == "falling"
    ):
        quant_warnings.append(
            {
                "code": "BALANCED_DRIFT_ESCALATION",
                "message": "Balanced filter is softening with falling WR and expectancy.",
                "stability_grade": balanced_drift.get("stability_grade"),
            }
        )
    structural_count = len((watchlist_summary or {}).get("structural_hard_blocks") or [])
    fast_track_count = len((watchlist_summary or {}).get("fast_track_hard_blocks") or [])
    if structural_count:
        quant_warnings.append(
            {
                "code": "STRUCTURAL_HARD_BLOCK",
                "message": "Stable negative hard-block patterns promoted to always-on quant hard-block recommendations.",
                "count": structural_count,
            }
        )
    if fast_track_count:
        quant_warnings.append(
            {
                "code": "FAST_TRACK_HARD_BLOCK",
                "message": "Demote-now hard-block candidates added to live quant hard-block recommendations.",
                "count": fast_track_count,
            }
        )
    if quant_warnings:
        payload["quant_warnings"] = quant_warnings
    write_json(QUANT_CYCLE_SUMMARY_PATH, payload, keep_backup=True)


def snapshot_outputs(timestamp: str) -> None:
    snap_dir = HISTORY_DIR / timestamp
    snap_dir.mkdir(parents=True, exist_ok=True)
    for path in [
        RESEARCH_SUMMARY_PATH,
        PROMOTION_LEDGER_PATH,
        FILTER_SIM_PATH,
        STATUS_PATH,
        DRIFT_PATH,
        WATCHLIST_PATH,
        WATCHLIST_CSV_PATH,
        QUANT_CYCLE_SUMMARY_PATH,
    ]:
        if path.exists():
            snap_dir.joinpath(path.name).write_bytes(path.read_bytes())


def write_status(status: dict[str, Any]) -> None:
    write_json(STATUS_PATH, status, keep_backup=True)


def run_once(force: bool = False) -> bool:
    reset_io_health()
    state = load_state()
    signature = get_signature()
    current_signature = signature.to_dict()
    previous_signature = state.get("last_success_signature")

    if not force and previous_signature == current_signature:
        last_summary = state.get("last_summary_snapshot") or {}
        last_drift = state.get("last_drift_report") or {}
        last_watchlist = state.get("last_watchlist_summary") or {}
        last_io_health = state.get("last_io_health") or io_health_summary()
        write_cycle_summary(
            last_summary,
            status="idle",
            completed_at=state.get("last_completed_at"),
            drift_summary=last_drift,
            watchlist_summary=last_watchlist,
            persistence_health=last_io_health,
        )
        write_status(
            {
                "status": "idle",
                "checked_at": now_iso(),
                "message": "No source changes detected; research pack not rerun.",
                "current_signature": current_signature,
                "last_completed_at": state.get("last_completed_at"),
                "summary": last_summary,
                "drift_summary": last_drift,
                "watchlist_summary": last_watchlist,
                "persistence_health": last_io_health,
            }
        )
        return False

    previous_summary = state.get("last_summary_snapshot") or {}
    previous_drift_snapshot = state.get("last_filter_snapshot") or state.get("last_drift_snapshot") or {}

    write_status(
        {
            "status": "running",
            "started_at": now_iso(),
            "message": "Refreshing quant research pack.",
            "current_signature": current_signature,
        }
    )
    log("Starting quant research refresh.")

    pack.main()

    current_promotions = read_csv_rows(PROMOTION_LEDGER_PATH)
    current_filters = read_csv_rows(FILTER_SIM_PATH)
    current_summary = compact_summary(
        read_research_summary(),
        current_promotions,
        current_filters,
    )
    promotion_history = append_promotion_history(current_summary, current_promotions)
    watchlist_summary, watchlist_rows = build_candidate_watchlist(current_summary, current_promotions, promotion_history)
    write_json(WATCHLIST_PATH, watchlist_summary, keep_backup=True)
    write_csv(WATCHLIST_CSV_PATH, watchlist_rows)
    current_drift_snapshot = build_quant_drift_snapshot(current_summary, current_promotions, current_filters)
    history = append_drift_history(current_drift_snapshot)
    drift = build_drift_report(
        previous_summary,
        current_summary,
        previous_drift_snapshot,
        current_drift_snapshot,
        history,
    )
    write_json(DRIFT_PATH, drift, keep_backup=True)

    finished_at = now_iso()
    persistence_health = io_health_summary()
    state["last_completed_at"] = finished_at
    state["last_success_signature"] = current_signature
    state["last_summary_snapshot"] = current_summary
    state["last_filter_snapshot"] = current_drift_snapshot
    state["last_drift_report"] = drift
    state["last_watchlist_summary"] = watchlist_summary
    state["last_io_health"] = persistence_health
    state["run_count"] = int(state.get("run_count") or 0) + 1
    write_json(STATE_PATH, state, keep_backup=True)
    write_cycle_summary(
        current_summary,
        status="ok",
        completed_at=finished_at,
        drift_summary=drift,
        watchlist_summary=watchlist_summary,
        persistence_health=persistence_health,
    )
    snapshot_outputs(finished_at.replace(":", "").replace("+", "_plus_"))
    write_status(
        {
            "status": "ok",
            "completed_at": finished_at,
            "message": "Quant research pack refreshed successfully.",
            "current_signature": current_signature,
            "run_count": state["run_count"],
            "summary": current_summary,
            "drift_summary": drift,
            "watchlist_summary": watchlist_summary,
            "persistence_health": persistence_health,
        }
    )
    if persistence_health.get("grade") != "healthy":
        log(
            "Quant persistence health warning: "
            f"grade={persistence_health.get('grade')} | "
            f"backup_recoveries={persistence_health.get('backup_recovery_count')} | "
            f"unrecovered={persistence_health.get('unrecovered_load_failure_count')} | "
            f"affected_files={len(persistence_health.get('affected_files') or [])}"
        )
    log("Quant research refresh completed successfully.")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-refresh the quant research pack when live resolved trades change.")
    parser.add_argument("--once", action="store_true", help="Run one refresh check and exit.")
    parser.add_argument("--force", action="store_true", help="Force a refresh even if source signatures did not change.")
    parser.add_argument("--poll-seconds", type=int, default=300, help="Polling interval for watch mode. Default: 300 seconds.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    if args.once:
        changed = run_once(force=args.force)
        log("Single-run mode finished." if changed else "Single-run mode found no changes.")
        return

    log(f"Entering watch mode with poll interval {args.poll_seconds}s.")
    while True:
        try:
            run_once(force=args.force)
            args.force = False
        except Exception as exc:  # pragma: no cover
            write_status(
                {
                    "status": "error",
                    "errored_at": now_iso(),
                    "message": str(exc),
                }
            )
            log(f"Quant research refresh failed: {exc!r}")
        time.sleep(max(30, args.poll_seconds))


if __name__ == "__main__":
    main()
