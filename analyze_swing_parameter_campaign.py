import argparse
import json
import math
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze a swing-parameter research campaign and rank evidence-backed figures."
    )
    parser.add_argument("--project-dir", default="")
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--min-accepted-trades", type=float, default=3.0)
    parser.add_argument("--min-coverage", type=float, default=0.85)
    return parser.parse_args()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str, payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def read_csv_if_exists(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_case_value(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return ""
        try:
            if "." in stripped:
                return float(stripped)
            return int(stripped)
        except ValueError:
            return stripped
    return value


def score_shadow_paths(shadow_path_df: pd.DataFrame) -> Dict[str, float]:
    if shadow_path_df.empty:
        return {
            "shadow_support_score": 0.0,
            "shadow_penalty_score": 0.0,
            "shadow_support_mass": 0.0,
        }
    frame = shadow_path_df.copy()
    frame["trades"] = frame["trades"].map(as_float)
    frame["expectancy_r"] = frame["expectancy_r"].map(as_float)
    support_score = 0.0
    penalty_score = 0.0
    support_mass = 0.0
    for _, row in frame.iterrows():
        trades = max(as_float(row.get("trades")), 0.0)
        expectancy = as_float(row.get("expectancy_r"))
        if trades <= 0:
            continue
        weight = math.sqrt(trades)
        if expectancy >= 0:
            support_score += expectancy * weight
            support_mass += expectancy * trades
        else:
            penalty_score += abs(expectancy) * weight
    return {
        "shadow_support_score": support_score,
        "shadow_penalty_score": penalty_score,
        "shadow_support_mass": support_mass,
    }


def score_counterfactual_pressure(gate_df: pd.DataFrame) -> Dict[str, float]:
    if gate_df.empty:
        return {
            "positive_rejected_trade_count": 0.0,
            "positive_rejected_expectancy_mass": 0.0,
            "positive_rejected_gate_count": 0.0,
        }
    frame = gate_df.copy()
    frame["trades"] = frame["trades"].map(as_float)
    frame["expectancy_r"] = frame["expectancy_r"].map(as_float)
    frame["win_rate_pct"] = frame["win_rate_pct"].map(as_float)
    positive = frame[(frame["trades"] > 0) & (frame["expectancy_r"] > 0) & (frame["win_rate_pct"] >= 50.0)].copy()
    if positive.empty:
        return {
            "positive_rejected_trade_count": 0.0,
            "positive_rejected_expectancy_mass": 0.0,
            "positive_rejected_gate_count": 0.0,
        }
    positive["expectancy_mass"] = positive["trades"] * positive["expectancy_r"]
    return {
        "positive_rejected_trade_count": float(positive["trades"].sum()),
        "positive_rejected_expectancy_mass": float(positive["expectancy_mass"].sum()),
        "positive_rejected_gate_count": float(len(positive)),
    }


def build_case_metrics(case_record: Dict[str, Any], case_meta: Dict[str, Any], min_accepted_trades: float, min_coverage: float) -> Dict[str, Any]:
    summary_path = str(case_record.get("summary_path") or "").strip()
    summary_payload = load_json(summary_path) if summary_path and os.path.exists(summary_path) else {}
    run_dir = os.path.dirname(summary_path) if summary_path else ""

    variant_row = {}
    variant_summary = list(summary_payload.get("variant_summary") or [])
    if variant_summary:
        variant_row = dict(variant_summary[0] or {})

    shadow_path_df = read_csv_if_exists(os.path.join(run_dir, "shadow_path_summary.csv"))
    gate_df = read_csv_if_exists(os.path.join(run_dir, "shadow_counterfactual_gate_summary.csv"))

    trades = as_float(variant_row.get("trades"))
    win_rate = as_float(variant_row.get("win_rate_pct"))
    expectancy_r = as_float(variant_row.get("expectancy_r"))
    profit_factor = as_float(variant_row.get("profit_factor"))
    signal_rate_pct = as_float(variant_row.get("signal_rate_pct"))
    evaluations = as_float(variant_row.get("evaluations"))
    signals = as_float(variant_row.get("signals"))

    requested = as_float(case_record.get("symbols_requested") or summary_payload.get("symbols_requested"))
    processed = as_float(case_record.get("symbols_processed") or summary_payload.get("symbols_processed"))
    coverage = (processed / requested) if requested > 0 else 0.0

    shadow_scores = score_shadow_paths(shadow_path_df)
    rejection_scores = score_counterfactual_pressure(gate_df)

    accepted_score = (
        expectancy_r * 120.0
        + (win_rate - 50.0) * 1.5
        + max(min(profit_factor, 3.0) - 1.0, -1.0) * 20.0
        + min(trades, 25.0) * 1.5
        + min(signal_rate_pct, 1.0) * 10.0
    )
    research_support_score = (
        shadow_scores["shadow_support_score"] * 8.0
        - shadow_scores["shadow_penalty_score"] * 4.0
    )
    rejection_pressure_penalty = rejection_scores["positive_rejected_expectancy_mass"] * 0.12
    coverage_adjustment = (coverage - min_coverage) * 25.0
    low_trade_penalty = -35.0 if trades < min_accepted_trades else 0.0
    low_coverage_penalty = -25.0 if coverage < min_coverage else 0.0
    composite_score = (
        accepted_score
        + research_support_score
        - rejection_pressure_penalty
        + coverage_adjustment
        + low_trade_penalty
        + low_coverage_penalty
    )

    return {
        "case_name": case_record.get("name"),
        "profile_name": case_meta.get("profile_name", ""),
        "parameter_name": case_meta.get("parameter_name", ""),
        "parameter_group": case_meta.get("parameter_group", ""),
        "parameter_value": normalize_case_value(case_meta.get("parameter_value")),
        "parameter_description": case_meta.get("parameter_description", ""),
        "return_code": int(case_record.get("return_code") or 0),
        "summary_path": summary_path,
        "trades": trades,
        "win_rate_pct": win_rate,
        "expectancy_r": expectancy_r,
        "profit_factor": profit_factor,
        "signal_rate_pct": signal_rate_pct,
        "evaluations": evaluations,
        "signals": signals,
        "symbols_requested": requested,
        "symbols_processed": processed,
        "coverage_ratio": coverage,
        "shadow_support_score": shadow_scores["shadow_support_score"],
        "shadow_penalty_score": shadow_scores["shadow_penalty_score"],
        "shadow_support_mass": shadow_scores["shadow_support_mass"],
        "positive_rejected_trade_count": rejection_scores["positive_rejected_trade_count"],
        "positive_rejected_expectancy_mass": rejection_scores["positive_rejected_expectancy_mass"],
        "positive_rejected_gate_count": rejection_scores["positive_rejected_gate_count"],
        "accepted_score": accepted_score,
        "research_support_score": research_support_score,
        "rejection_pressure_penalty": rejection_pressure_penalty,
        "coverage_adjustment": coverage_adjustment,
        "low_trade_penalty": low_trade_penalty,
        "low_coverage_penalty": low_coverage_penalty,
        "composite_score": composite_score,
    }


def build_parameter_recommendations(case_metrics_df: pd.DataFrame) -> List[Dict[str, Any]]:
    recommendations: List[Dict[str, Any]] = []
    if case_metrics_df.empty:
        return recommendations
    baseline_row = case_metrics_df[case_metrics_df["parameter_name"] == "baseline"]
    baseline_score = as_float(baseline_row["composite_score"].iloc[0]) if not baseline_row.empty else 0.0
    baseline_expectancy = as_float(baseline_row["expectancy_r"].iloc[0]) if not baseline_row.empty else 0.0
    baseline_win_rate = as_float(baseline_row["win_rate_pct"].iloc[0]) if not baseline_row.empty else 0.0

    analysis_df = case_metrics_df[case_metrics_df["parameter_name"] != "baseline"].copy()
    if analysis_df.empty:
        return recommendations

    for parameter_name, group_df in analysis_df.groupby("parameter_name"):
        ranked = group_df.sort_values("composite_score", ascending=False).reset_index(drop=True)
        best_row = ranked.iloc[0].to_dict()
        best_score = as_float(best_row.get("composite_score"))
        tolerance = max(3.0, abs(best_score) * 0.1)
        robust_df = ranked[ranked["composite_score"] >= (best_score - tolerance)].copy()
        robust_values = robust_df["parameter_value"].tolist()
        if robust_values and all(isinstance(item, (int, float)) for item in robust_values):
            robust_values = sorted(robust_values)
        recommendation = {
            "parameter_name": parameter_name,
            "parameter_group": best_row.get("parameter_group", ""),
            "parameter_description": best_row.get("parameter_description", ""),
            "tested_values": ranked["parameter_value"].tolist(),
            "recommended_value": best_row.get("parameter_value"),
            "recommended_case": best_row.get("case_name"),
            "recommended_score": best_score,
            "delta_vs_baseline_score": best_score - baseline_score,
            "delta_vs_baseline_expectancy_r": as_float(best_row.get("expectancy_r")) - baseline_expectancy,
            "delta_vs_baseline_win_rate_pct": as_float(best_row.get("win_rate_pct")) - baseline_win_rate,
            "robust_values": robust_values,
            "robust_value_range": robust_values if len(robust_values) <= 1 else [robust_values[0], robust_values[-1]],
            "top_candidates": ranked.head(3)[
                [
                    "case_name",
                    "parameter_value",
                    "composite_score",
                    "expectancy_r",
                    "win_rate_pct",
                    "trades",
                    "positive_rejected_expectancy_mass",
                ]
            ].to_dict(orient="records"),
        }
        recommendations.append(recommendation)
    return recommendations


def main():
    args = parse_args()
    campaign_root = os.path.normpath(args.campaign_root)
    output_dir = os.path.normpath(args.output_dir or os.path.join(campaign_root, "swing_analysis"))
    project_dir = os.path.normpath(args.project_dir) if str(args.project_dir or "").strip() else ""
    os.makedirs(output_dir, exist_ok=True)

    manifest_path = os.path.join(campaign_root, "campaign_manifest.json")
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"campaign manifest not found: {manifest_path}")
    manifest = load_json(manifest_path)

    campaign_file = str(manifest.get("campaign_file") or "").strip()
    campaign_source = load_json(campaign_file) if campaign_file and os.path.exists(campaign_file) else {}
    source_cases = {
        str(item.get("name") or "").strip(): dict(item or {})
        for item in list(campaign_source.get("cases") or [])
        if str(item.get("name") or "").strip()
    }

    rows: List[Dict[str, Any]] = []
    for case_record in list(manifest.get("cases") or []):
        case_name = str(case_record.get("name") or "").strip()
        if not case_name:
            continue
        case_meta = source_cases.get(case_name, {})
        if not case_meta:
            case_meta = dict(case_record.get("case_metadata") or {})
        summary_path = str(case_record.get("summary_path") or "").strip()
        if not summary_path or not os.path.exists(summary_path):
            continue
        rows.append(
            build_case_metrics(
                case_record,
                case_meta,
                min_accepted_trades=args.min_accepted_trades,
                min_coverage=args.min_coverage,
            )
        )

    case_metrics_df = pd.DataFrame(rows)
    if case_metrics_df.empty:
        raise ValueError("No completed cases with summary.json were available for swing campaign analysis.")

    case_metrics_df = case_metrics_df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    recommendations = build_parameter_recommendations(case_metrics_df)
    recommendation_df = pd.DataFrame(recommendations)

    case_metrics_path = os.path.join(output_dir, "swing_case_metrics.csv")
    ranking_path = os.path.join(output_dir, "swing_parameter_ranking.csv")
    report_path = os.path.join(output_dir, "swing_parameter_report.json")
    live_swing_report_path = os.path.join(project_dir, "live_swing_evidence_report.json") if project_dir else ""
    live_swing_report = (
        load_json(live_swing_report_path)
        if live_swing_report_path and os.path.exists(live_swing_report_path)
        else {}
    )

    case_metrics_df.to_csv(case_metrics_path, index=False)
    if not recommendation_df.empty:
        recommendation_df.to_csv(ranking_path, index=False)

    best_case = case_metrics_df.iloc[0].to_dict()
    report_payload = {
        "generated_at": datetime.now().isoformat(),
        "campaign_root": campaign_root,
        "campaign_file": campaign_file,
        "output_dir": output_dir,
        "baseline_case": case_metrics_df[
            case_metrics_df["parameter_name"] == "baseline"
        ].head(1).to_dict(orient="records"),
        "best_case": best_case,
        "score_formula": {
            "accepted_score": "expectancy_r*120 + (win_rate_pct-50)*1.5 + clipped_profit_factor_bonus + min(trades,25)*1.5 + min(signal_rate_pct,1.0)*10",
            "research_support_score": "positive_shadow_expectancy_support*8 - negative_shadow_expectancy_penalty*4",
            "rejection_pressure_penalty": "positive_rejected_expectancy_mass*0.12",
            "coverage_adjustment": "(coverage_ratio-min_coverage)*25",
            "low_trade_penalty": "-35 if accepted trades below threshold",
            "low_coverage_penalty": "-25 if processed-symbol coverage below threshold",
        },
        "thresholds": {
            "min_accepted_trades": args.min_accepted_trades,
            "min_coverage": args.min_coverage,
        },
        "live_swing_evidence_path": live_swing_report_path if live_swing_report else "",
        "live_swing_evidence_snapshot": live_swing_report if live_swing_report else {},
        "top_cases": case_metrics_df.head(10).to_dict(orient="records"),
        "parameter_recommendations": recommendations,
    }
    write_json(report_path, report_payload)

    print(f"Wrote swing case metrics: {case_metrics_path}")
    print(f"Wrote swing ranking: {ranking_path}")
    print(f"Wrote swing report: {report_path}")


if __name__ == "__main__":
    main()
