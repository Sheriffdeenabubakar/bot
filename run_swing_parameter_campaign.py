import argparse
import copy
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate and optionally run a dedicated swing-parameter research campaign."
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--campaign-root", required=True)
    parser.add_argument("--parameter-space-file", default="")
    parser.add_argument("--selected-parameters", default="")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    return parser.parse_args()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def dump_json(path: str, payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def resolve_path(raw: str, base_dir: str, project_dir: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if os.path.isabs(value):
        return os.path.normpath(value)
    candidate = os.path.normpath(os.path.join(base_dir, value))
    if os.path.exists(candidate):
        return candidate
    return os.path.normpath(os.path.join(project_dir, value))


def slugify(value: Any) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value))
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "value"


def deep_merge(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base or {})
    for key, value in (updates or {}).items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged.get(key) or {}, value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def replace_value_tokens(payload: Any, value: Any) -> Any:
    if isinstance(payload, dict):
        return {key: replace_value_tokens(item, value) for key, item in payload.items()}
    if isinstance(payload, list):
        return [replace_value_tokens(item, value) for item in payload]
    if isinstance(payload, str):
        if payload == "$value":
            return value
        return payload.replace("$value_slug", slugify(value)).replace("$value", str(value))
    return payload


def load_base_profile(space: Dict[str, Any], base_dir: str, project_dir: str) -> Dict[str, Any]:
    profile_file = resolve_path(space.get("base_profile_file", ""), base_dir, project_dir)
    profile_name = str(space.get("base_profile_name") or "").strip()
    if not profile_file or not profile_name:
        return {
            "description": "Current live baseline without explicit profile inheritance.",
            "signal_config_overrides": {},
            "trade_config_overrides": {},
            "research_config_overrides": {},
        }
    payload = load_json(profile_file)
    profiles = dict(payload.get("profiles") or {})
    if profile_name not in profiles:
        raise KeyError(f"base profile '{profile_name}' not found in {profile_file}")
    profile = copy.deepcopy(profiles[profile_name])
    profile.setdefault("description", f"Inherited from {profile_name}")
    profile.setdefault("signal_config_overrides", {})
    profile.setdefault("trade_config_overrides", {})
    profile.setdefault("research_config_overrides", {})
    return profile


def filter_parameters(parameters: List[Dict[str, Any]], selected_raw: str) -> List[Dict[str, Any]]:
    selected = [item.strip() for item in str(selected_raw or "").split(",") if item.strip()]
    if not selected:
        return [item for item in parameters if bool(item.get("enabled", True))]
    wanted = {item.lower() for item in selected}
    filtered = []
    for item in parameters:
        name = str(item.get("name") or "").strip()
        if not name or not bool(item.get("enabled", True)):
            continue
        if name.lower() in wanted:
            filtered.append(item)
    missing = sorted(wanted - {str(item.get("name") or "").strip().lower() for item in filtered})
    if missing:
        raise ValueError(f"selected parameters not found in space file: {', '.join(missing)}")
    return filtered


def build_profiles_and_cases(space: Dict[str, Any], project_dir: str, base_dir: str):
    baseline_profile_name = "baseline_current_live"
    baseline_profile = load_base_profile(space, base_dir, project_dir)
    selected_parameters = filter_parameters(list(space.get("parameters") or []), space.get("_selected_parameters", ""))

    profiles = {
        baseline_profile_name: baseline_profile,
    }
    cases: List[Dict[str, Any]] = [
        {
            "name": baseline_profile_name,
            "description": "Baseline reference using the current post-research live swing profile.",
            "profile_name": baseline_profile_name,
            "parameter_name": "baseline",
            "parameter_group": "baseline",
            "parameter_value": "",
        }
    ]

    for parameter in selected_parameters:
        param_name = str(parameter.get("name") or "").strip()
        if not param_name:
            continue
        description = str(parameter.get("description") or "").strip()
        group = str(parameter.get("group") or "swing").strip()
        values = list(parameter.get("values") or [])
        if not values:
            continue
        for value in values:
            case_slug = f"{slugify(param_name)}__{slugify(value)}"
            candidate_profile = copy.deepcopy(baseline_profile)
            candidate_profile["description"] = (
                f"{description or param_name} | {param_name}={value}"
            ).strip()
            for section in (
                "signal_config_overrides",
                "trade_config_overrides",
                "research_config_overrides",
            ):
                resolved_updates = replace_value_tokens(parameter.get(section, {}), value)
                candidate_profile[section] = deep_merge(
                    candidate_profile.get(section) or {},
                    resolved_updates or {},
                )
            profiles[case_slug] = candidate_profile
            cases.append(
                {
                    "name": case_slug,
                    "description": f"{description or param_name} [{param_name}={value}]",
                    "profile_name": case_slug,
                    "parameter_name": param_name,
                    "parameter_group": group,
                    "parameter_value": value,
                    "parameter_description": description,
                }
            )

    return profiles, cases, baseline_profile_name


def main():
    args = parse_args()
    project_dir = os.path.normpath(args.project_dir)
    campaign_root = os.path.normpath(args.campaign_root)
    os.makedirs(campaign_root, exist_ok=True)

    parameter_space_file = resolve_path(
        args.parameter_space_file or "swing_parameter_space.template.json",
        campaign_root,
        project_dir,
    )
    parameter_space = load_json(parameter_space_file)
    parameter_space["_selected_parameters"] = args.selected_parameters
    parameter_space_dir = os.path.dirname(parameter_space_file)

    profiles, cases, baseline_profile_name = build_profiles_and_cases(
        parameter_space,
        project_dir,
        parameter_space_dir,
    )
    generated_profiles_path = os.path.join(campaign_root, "generated_swing_profiles.json")
    generated_campaign_path = os.path.join(campaign_root, "generated_swing_campaign.json")

    dump_json(
        generated_profiles_path,
        {
            "profiles": profiles,
        },
    )

    defaults = dict(parameter_space.get("defaults") or {})
    defaults["profile_file"] = generated_profiles_path
    defaults["profile_name"] = baseline_profile_name

    campaign_payload = {
        "description": str(parameter_space.get("description") or "").strip(),
        "generated_at": datetime.now().isoformat(),
        "parameter_space_file": parameter_space_file,
        "defaults": defaults,
        "cases": cases,
    }
    dump_json(generated_campaign_path, campaign_payload)

    print(f"Generated swing profile file: {generated_profiles_path}")
    print(f"Generated swing campaign file: {generated_campaign_path}")
    print(f"Generated cases: {len(cases)}")

    if args.generate_only:
        return

    run_campaign_cmd = [
        sys.executable,
        os.path.join(project_dir, "run_research_campaign.py"),
        "--project-dir",
        project_dir,
        "--campaign-file",
        generated_campaign_path,
        "--campaign-root",
        campaign_root,
    ]
    if args.continue_on_error:
        run_campaign_cmd.append("--continue-on-error")
    subprocess.run(run_campaign_cmd, cwd=project_dir, check=False)

    analyze_cmd = [
        sys.executable,
        os.path.join(project_dir, "analyze_swing_parameter_campaign.py"),
        "--project-dir",
        project_dir,
        "--campaign-root",
        campaign_root,
    ]
    subprocess.run(analyze_cmd, cwd=project_dir, check=False)


if __name__ == "__main__":
    main()
