import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a sequence of batched quant research cases and collect comparable summaries."
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--campaign-file", required=True)
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_path(raw, base_dir, project_dir):
    value = str(raw or "").strip()
    if not value:
        return ""
    if os.path.isabs(value):
        return value
    candidate = os.path.normpath(os.path.join(base_dir, value))
    if os.path.exists(candidate):
        return candidate
    return os.path.normpath(os.path.join(project_dir, value))


def newest_timestamped_run(root_dir):
    if not os.path.isdir(root_dir):
        return ""
    candidates = [
        os.path.join(root_dir, name)
        for name in os.listdir(root_dir)
        if os.path.isdir(os.path.join(root_dir, name)) and str(name)[:8].isdigit()
    ]
    if not candidates:
        return ""
    return max(candidates, key=os.path.getmtime)


def build_case_command(project_dir, case, defaults, case_root):
    batch_root = os.path.join(case_root, "batches")
    merge_root = os.path.join(case_root, "merged")
    os.makedirs(batch_root, exist_ok=True)
    os.makedirs(merge_root, exist_ok=True)

    merged = dict(defaults or {})
    merged.update(case or {})
    merge_label = str(merged.get("merge_label") or merged.get("name") or "campaign_case").strip()

    cmd = [
        sys.executable,
        os.path.join(project_dir, "run_scanner_research_batches.py"),
        "--project-dir",
        project_dir,
        "--batch-root",
        batch_root,
        "--merge-output-root",
        merge_root,
    ]

    arg_map = {
        "batch_size": "--batch-size",
        "attempts": "--attempts",
        "max_concurrency": "--max-concurrency",
        "operational_15m_limit": "--operational-15m-limit",
        "directional_15m_limit": "--directional-15m-limit",
        "structure_15m_limits": "--structure-15m-limits",
        "variants": "--variants",
        "limit": "--limit",
        "warmup": "--warmup",
        "step": "--step",
        "lookahead": "--lookahead",
        "profile_file": "--profile-file",
        "profile_name": "--profile-name",
        "signal_config_overrides_file": "--signal-config-overrides-file",
        "trade_config_overrides_file": "--trade-config-overrides-file",
        "research_config_overrides_file": "--research-config-overrides-file",
    }
    for key, flag in arg_map.items():
        value = merged.get(key)
        if value is None or str(value).strip() == "":
            continue
        cmd.extend([flag, str(value)])

    cmd.extend(["--merge-label", merge_label])
    return cmd, batch_root, merge_root, merge_label


def extract_summary_metrics(summary_path):
    if not summary_path or not os.path.exists(summary_path):
        return {}
    payload = load_json(summary_path)
    variant_summary = payload.get("variant_summary") or []
    primary_variant = variant_summary[0] if variant_summary else {}
    return {
        "summary_path": summary_path,
        "symbols_requested": payload.get("symbols_requested"),
        "symbols_processed": payload.get("symbols_processed"),
        "symbols_skipped": payload.get("symbols_skipped"),
        "open_trades_count": payload.get("open_trades_count"),
        "shadow_open_trades_count": payload.get("shadow_open_trades_count"),
        "shadow_counterfactual_trades_count": payload.get("shadow_counterfactual_trades_count"),
        "shadow_counterfactual_open_trades_count": payload.get("shadow_counterfactual_open_trades_count"),
        "trades": primary_variant.get("trades"),
        "win_rate_pct": primary_variant.get("win_rate_pct"),
        "expectancy_r": primary_variant.get("expectancy_r"),
        "profit_factor": primary_variant.get("profit_factor"),
        "signal_rate_pct": primary_variant.get("signal_rate_pct"),
        "evaluations": primary_variant.get("evaluations"),
        "signals": primary_variant.get("signals"),
    }


def main():
    args = parse_args()
    project_dir = os.path.normpath(args.project_dir)
    campaign_file = os.path.normpath(args.campaign_file)
    campaign_root = os.path.normpath(args.campaign_root)
    os.makedirs(campaign_root, exist_ok=True)

    payload = load_json(campaign_file)
    campaign_dir = os.path.dirname(campaign_file)
    defaults = dict(payload.get("defaults") or {})
    cases = list(payload.get("cases") or [])
    if not cases:
        raise ValueError("campaign file must contain a non-empty 'cases' list.")

    for key in (
        "profile_file",
        "signal_config_overrides_file",
        "trade_config_overrides_file",
        "research_config_overrides_file",
    ):
        if key in defaults:
            defaults[key] = resolve_path(defaults.get(key), campaign_dir, project_dir)

    campaign_state = {
        "started_at": datetime.now().isoformat(),
        "campaign_file": campaign_file,
        "campaign_root": campaign_root,
        "defaults": defaults,
        "cases": [],
    }
    manifest_path = os.path.join(campaign_root, "campaign_manifest.json")

    def write_manifest():
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(campaign_state, handle, indent=2)

    write_manifest()
    summary_rows = []

    for index, case in enumerate(cases, start=1):
        case = dict(case or {})
        case_name = str(case.get("name") or f"case_{index:02d}").strip()
        for key in (
            "profile_file",
            "signal_config_overrides_file",
            "trade_config_overrides_file",
            "research_config_overrides_file",
        ):
            if key in case:
                case[key] = resolve_path(case.get(key), campaign_dir, project_dir)

        case_root = os.path.join(campaign_root, case_name)
        os.makedirs(case_root, exist_ok=True)
        cmd, batch_root, merge_root, merge_label = build_case_command(project_dir, case, defaults, case_root)
        completed = subprocess.run(cmd, cwd=project_dir, check=False)
        merged_run_dir = newest_timestamped_run(merge_root)
        summary_path = os.path.join(merged_run_dir, "summary.json") if merged_run_dir else ""
        metrics = extract_summary_metrics(summary_path)
        case_metadata = {
            "profile_name": merged.get("profile_name"),
            "profile_file": merged.get("profile_file"),
            "parameter_name": case.get("parameter_name", ""),
            "parameter_group": case.get("parameter_group", ""),
            "parameter_value": case.get("parameter_value", ""),
            "parameter_description": case.get("parameter_description", ""),
        }
        case_record = {
            "name": case_name,
            "description": str(case.get("description") or ""),
            "return_code": int(completed.returncode),
            "batch_root": batch_root,
            "merge_root": merge_root,
            "merge_label": merge_label,
            "summary_path": summary_path,
            "command": cmd,
            "case_metadata": case_metadata,
            **metrics,
        }
        campaign_state["cases"].append(case_record)
        write_manifest()
        summary_rows.append(case_record)
        if completed.returncode != 0 and not args.continue_on_error:
            break

    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(os.path.join(campaign_root, "campaign_summary.csv"), index=False)


if __name__ == "__main__":
    main()
