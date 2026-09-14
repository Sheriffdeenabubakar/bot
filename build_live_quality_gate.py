from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from quality_gate import classify_adx_regime, classify_confirmation_bucket, classify_htf_alignment_bucket


SYMBOL_RULE_FIELDS = [
    "symbol",
    "direction",
    "primary_setup",
    "session_bucket",
    "live_market_regime",
    "adx_regime",
    "htf_alignment_bucket",
    "confirmation_bucket",
]

SETUP_RULE_FIELDS = [
    "symbol_bucket",
    "direction",
    "primary_setup",
    "session_bucket",
    "live_market_regime",
    "adx_regime",
    "htf_alignment_bucket",
    "confirmation_bucket",
]

OPTIONAL_BOOL_FIELDS = [
    "has_wyckoff",
    "has_retest",
    "has_volume_spike",
    "has_momentum_divergence",
    "has_order_flow",
    "has_fibonacci_proximity",
    "has_vwap_proximity",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an approved live quality gate from completed research trades.")
    parser.add_argument(
        "--run-dirs",
        nargs="+",
        required=True,
        help="One or more completed quant output directories that contain trades.csv.",
    )
    parser.add_argument(
        "--output-json",
        default=str(Path(__file__).resolve().parent / "research" / "live_quality_gate.json"),
        help="Path to write the live quality gate JSON.",
    )
    parser.add_argument(
        "--output-csv",
        default=str(Path(__file__).resolve().parent / "research" / "live_quality_gate_rules.csv"),
        help="Path to write the approved rules CSV.",
    )
    parser.add_argument("--min-opportunities", type=int, default=2)
    parser.add_argument("--min-win-rate-pct", type=float, default=70.0)
    parser.add_argument("--min-expectancy-r", type=float, default=0.0)
    parser.add_argument(
        "--generalize-symbols",
        action="store_true",
        help="Build setup-level rules that apply to any matching scanner symbol within the same symbol bucket.",
    )
    parser.add_argument(
        "--require-bool-consistency",
        action="store_true",
        help="Attach boolean requirements only when every approved opportunity in a rule agrees.",
    )
    return parser.parse_args()


def load_trade_rows(run_dir: Path) -> pd.DataFrame:
    trades_path = run_dir / "trades.csv"
    if not trades_path.exists():
        raise FileNotFoundError(f"Missing trades.csv in {run_dir}")
    frame = pd.read_csv(trades_path, low_memory=False)
    if frame.empty:
        return frame
    frame["source_run"] = run_dir.name
    return frame


def normalize_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    if "trade_status" in result.columns:
        result = result[result["trade_status"].astype(str).str.lower() == "entered"].copy()
    if result.empty:
        return result

    for col in ("signal_time", "entry_time", "exit_time"):
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors="coerce")

    for col in ("net_r", "confidence_score", "confirmation_score", "htf_confluence", "adx"):
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    if "adx_regime" not in result.columns:
        source = result["adx"] if "adx" in result.columns else None
        result["adx_regime"] = source.map(classify_adx_regime) if source is not None else "unknown"
    if "confirmation_bucket" not in result.columns:
        result["confirmation_bucket"] = result["confirmation_score"].map(
            lambda value: classify_confirmation_bucket(value, medium_threshold=2.5, strong_threshold=4.0)
        )
    if "htf_alignment_bucket" not in result.columns:
        result["htf_alignment_bucket"] = result["htf_confluence"].map(classify_htf_alignment_bucket)

    result["symbol"] = result["symbol"].astype(str).str.upper()
    result["market_opportunity_id"] = (
        result["source_run"].astype(str)
        + "|"
        + result["symbol"].astype(str)
        + "|"
        + result["signal_time"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("missing_ts")
    )
    return result


def build_opportunity_frame(trades_df: pd.DataFrame, rule_fields: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped = trades_df.groupby("market_opportunity_id", dropna=False)
    for opportunity_id, frame in grouped:
        if frame.empty:
            continue
        wins = int((pd.to_numeric(frame["net_r"], errors="coerce") > 0).sum())
        variant_count = int(len(frame))
        mean_net_r = float(pd.to_numeric(frame["net_r"], errors="coerce").mean())
        win_share = wins / variant_count if variant_count else 0.0
        row: dict[str, Any] = {
            "market_opportunity_id": opportunity_id,
            "variant_count": variant_count,
            "mean_net_r": round(mean_net_r, 6),
            "win_share": round(win_share, 6),
            "consensus_win": bool(win_share >= 0.60 and mean_net_r > 0.0),
            "confidence_score": float(pd.to_numeric(frame.get("confidence_score"), errors="coerce").median())
            if "confidence_score" in frame.columns
            else None,
        }
        sample = frame.iloc[0]
        for field in rule_fields:
            row[field] = sample.get(field)
        for field in OPTIONAL_BOOL_FIELDS:
            if field in frame.columns:
                values = frame[field].dropna()
                row[field] = bool(values.mean() >= 0.5) if not values.empty else None
        rows.append(row)
    result = pd.DataFrame(rows)
    if not result.empty:
        sort_fields = [field for field in ("symbol", "symbol_bucket", "market_opportunity_id") if field in result.columns]
        result = result.sort_values(sort_fields).reset_index(drop=True)
    return result


def build_rule_rows(
    opportunities_df: pd.DataFrame,
    *,
    rule_fields: list[str],
    min_opportunities: int,
    min_win_rate_pct: float,
    min_expectancy_r: float,
    require_bool_consistency: bool,
) -> pd.DataFrame:
    if opportunities_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    grouped = opportunities_df.groupby(rule_fields, dropna=False)
    for keys, frame in grouped:
        opportunity_count = int(len(frame))
        if opportunity_count < int(min_opportunities):
            continue
        wins = int(frame["consensus_win"].sum())
        win_rate_pct = (wins / opportunity_count) * 100.0 if opportunity_count else 0.0
        expectancy_r = float(pd.to_numeric(frame["mean_net_r"], errors="coerce").mean())
        if win_rate_pct < float(min_win_rate_pct) or expectancy_r <= float(min_expectancy_r):
            continue

        row = {
            field: value
            for field, value in zip(rule_fields, keys)
        }
        row.update(
            {
                "rule_id": f"rule_{len(rows) + 1:03d}",
                "opportunities": opportunity_count,
                "wins": wins,
                "losses": opportunity_count - wins,
                "win_rate_pct": round(win_rate_pct, 4),
                "expectancy_r": round(expectancy_r, 6),
                "avg_variant_count": round(float(pd.to_numeric(frame["variant_count"], errors="coerce").mean()), 4),
                "avg_confidence_score": round(float(pd.to_numeric(frame["confidence_score"], errors="coerce").mean()), 4)
                if "confidence_score" in frame.columns
                else None,
            }
        )
        if require_bool_consistency:
            for field in OPTIONAL_BOOL_FIELDS:
                if field not in frame.columns:
                    continue
                values = frame[field].dropna()
                if values.empty:
                    continue
                if values.nunique(dropna=True) == 1:
                    row[field] = bool(values.iloc[0])
        row["priority"] = int(1000 + (opportunity_count * 10) + round(win_rate_pct))
        rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(
            ["priority", "opportunities", "win_rate_pct", "expectancy_r"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)
    return result


def main() -> None:
    args = parse_args()
    rule_fields = SETUP_RULE_FIELDS if bool(args.generalize_symbols) else SYMBOL_RULE_FIELDS
    run_dirs = [Path(item).resolve() for item in args.run_dirs]
    frames = [normalize_trade_frame(load_trade_rows(path)) for path in run_dirs]
    trades_df = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True, sort=False)
    if trades_df.empty:
        raise RuntimeError("No entered trade rows were available in the supplied run directories.")

    opportunities_df = build_opportunity_frame(trades_df, rule_fields)
    rules_df = build_rule_rows(
        opportunities_df,
        rule_fields=rule_fields,
        min_opportunities=int(args.min_opportunities),
        min_win_rate_pct=float(args.min_win_rate_pct),
        min_expectancy_r=float(args.min_expectancy_r),
        require_bool_consistency=bool(args.require_bool_consistency),
    )

    output_json = Path(args.output_json).resolve()
    output_csv = Path(args.output_csv).resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    rules = rules_df.where(pd.notna(rules_df), None).to_dict(orient="records") if not rules_df.empty else []
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_run_dirs": [str(path) for path in run_dirs],
        "filters": {
            "min_opportunities": int(args.min_opportunities),
            "min_win_rate_pct": float(args.min_win_rate_pct),
            "min_expectancy_r": float(args.min_expectancy_r),
            "generalize_symbols": bool(args.generalize_symbols),
            "require_bool_consistency": bool(args.require_bool_consistency),
            "consensus_definition": "win_share >= 0.60 and mean_net_r > 0",
        },
        "rule_fields": rule_fields,
        "rules": rules,
    }

    output_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    rules_df.to_csv(output_csv, index=False)

    print(f"Quality gate rules written to: {output_json}")
    print(f"Approved rules: {len(rules_df)}")


if __name__ == "__main__":
    main()
