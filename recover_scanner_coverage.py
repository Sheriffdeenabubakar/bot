import argparse
import json
import math
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from typing import Dict, List, Set

import pandas as pd


@dataclass
class BatchResult:
    batch_index: int
    attempt: int
    symbols_file: str
    batch_symbols: List[str]
    run_root: str
    run_dir: str
    stdout_log: str
    stderr_log: str
    success: bool
    return_code: int
    summary_path: str


def parse_args():
    parser = argparse.ArgumentParser(description="Recover scanner-universe coverage by rerunning network-failed symbols in smaller batches.")
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--base-run-dir", required=True)
    parser.add_argument("--retry-symbols-file", required=True)
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--merge-output-root", required=True)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--operational-15m-limit", type=int, default=300)
    parser.add_argument("--directional-15m-limit", type=int, default=300)
    parser.add_argument("--structure-15m-limits", default="2000")
    parser.add_argument("--variants", default="baseline")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--warmup", type=int, default=None)
    parser.add_argument("--step", type=int, default=None)
    parser.add_argument("--lookahead", type=int, default=None)
    parser.add_argument("--profile-file", default="")
    parser.add_argument("--profile-name", default="")
    parser.add_argument("--signal-config-overrides-file", default="")
    parser.add_argument("--trade-config-overrides-file", default="")
    parser.add_argument("--research-config-overrides-file", default="")
    return parser.parse_args()


def load_requested_and_completed(base_run_dir: str) -> tuple[List[str], Set[str], pd.DataFrame]:
    universe_path = os.path.join(base_run_dir, "universe.csv")
    universe_df = pd.read_csv(universe_path)
    requested = sorted(universe_df["symbol"].dropna().astype(str).unique().tolist())
    completed = set(universe_df.loc[universe_df["status"] == "processed", "symbol"].dropna().astype(str).tolist())
    return requested, completed, universe_df


def load_retry_symbols(path: str, requested_set: Set[str]) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read()
    symbols = []
    seen = set()
    for chunk in raw.replace("\r", "\n").replace(",", "\n").split("\n"):
        symbol = chunk.replace("\ufeff", "").strip()
        if not symbol or symbol in seen:
            continue
        if symbol not in requested_set:
            continue
        symbols.append(symbol)
        seen.add(symbol)
    return symbols


def list_successful_batch_runs(batch_root: str) -> List[str]:
    if not os.path.isdir(batch_root):
        return []
    successful = []
    for entry in sorted(os.listdir(batch_root)):
        run_root = os.path.join(batch_root, entry)
        if not os.path.isdir(run_root) or not entry.startswith("batch_"):
            continue
        for child in sorted(os.listdir(run_root)):
            run_dir = os.path.join(run_root, child)
            if os.path.isdir(run_dir) and os.path.exists(os.path.join(run_dir, "summary.json")):
                successful.append(run_dir)
    return successful


def load_completed_from_successful_runs(run_dirs: List[str]) -> Set[str]:
    completed: Set[str] = set()
    for run_dir in run_dirs:
        universe_path = os.path.join(run_dir, "universe.csv")
        if not os.path.exists(universe_path):
            continue
        df = pd.read_csv(universe_path)
        completed.update(df.loc[df["status"] == "processed", "symbol"].dropna().astype(str).tolist())
    return completed


def chunk_symbols(symbols: List[str], batch_size: int) -> List[List[str]]:
    return [symbols[idx: idx + batch_size] for idx in range(0, len(symbols), batch_size)]


def write_symbols_file(path: str, symbols: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(symbols))
        handle.write("\n")


def newest_timestamped_run(run_root: str) -> str:
    if not os.path.isdir(run_root):
        return ""
    candidates = [
        os.path.join(run_root, name)
        for name in os.listdir(run_root)
        if os.path.isdir(os.path.join(run_root, name)) and name[:8].isdigit()
    ]
    if not candidates:
        return ""
    return max(candidates, key=os.path.getmtime)


def run_batch(args, batch_index: int, symbols: List[str], attempt: int) -> BatchResult:
    batch_name = f"batch_{batch_index:03d}"
    run_root = os.path.join(args.batch_root, batch_name, f"attempt_{attempt}")
    logs_dir = os.path.join(run_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    symbols_file = os.path.join(run_root, f"{batch_name}_symbols.txt")
    stdout_log = os.path.join(logs_dir, "stdout.log")
    stderr_log = os.path.join(logs_dir, "stderr.log")
    write_symbols_file(symbols_file, symbols)

    cmd = [
        sys.executable,
        os.path.join(args.project_dir, "quant_research.py"),
        "--universe-source",
        "scanner",
        "--symbols-file",
        symbols_file,
        "--variants",
        args.variants,
        "--structure-15m-limits",
        args.structure_15m_limits,
        "--operational-15m-limit",
        str(args.operational_15m_limit),
        "--directional-15m-limit",
        str(args.directional_15m_limit),
        "--max-concurrency",
        str(args.max_concurrency),
        "--output-dir",
        run_root,
    ]

    if args.limit is not None:
        cmd.extend(["--limit", str(args.limit)])
    if args.warmup is not None:
        cmd.extend(["--warmup", str(args.warmup)])
    if args.step is not None:
        cmd.extend(["--step", str(args.step)])
    if args.lookahead is not None:
        cmd.extend(["--lookahead", str(args.lookahead)])
    if str(args.profile_file).strip():
        cmd.extend(["--profile-file", str(args.profile_file).strip()])
    if str(args.profile_name).strip():
        cmd.extend(["--profile-name", str(args.profile_name).strip()])
    if str(args.signal_config_overrides_file).strip():
        cmd.extend(["--signal-config-overrides-file", str(args.signal_config_overrides_file).strip()])
    if str(args.trade_config_overrides_file).strip():
        cmd.extend(["--trade-config-overrides-file", str(args.trade_config_overrides_file).strip()])
    if str(args.research_config_overrides_file).strip():
        cmd.extend(["--research-config-overrides-file", str(args.research_config_overrides_file).strip()])

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    with open(stdout_log, "w", encoding="utf-8") as out_handle, open(stderr_log, "w", encoding="utf-8") as err_handle:
        completed = subprocess.run(cmd, cwd=args.project_dir, env=env, stdout=out_handle, stderr=err_handle)

    run_dir = newest_timestamped_run(run_root)
    summary_path = os.path.join(run_dir, "summary.json") if run_dir else ""
    success = bool(run_dir and os.path.exists(summary_path))
    return BatchResult(
        batch_index=batch_index,
        attempt=attempt,
        symbols_file=symbols_file,
        batch_symbols=symbols,
        run_root=run_root,
        run_dir=run_dir,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        success=success,
        return_code=int(completed.returncode),
        summary_path=summary_path,
    )


def write_manifest(path: str, payload: Dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def main():
    args = parse_args()
    requested, base_completed, universe_df = load_requested_and_completed(args.base_run_dir)
    requested_set = set(requested)
    retry_symbols = load_retry_symbols(args.retry_symbols_file, requested_set)
    successful_batch_runs = list_successful_batch_runs(args.batch_root)
    recovered_completed = load_completed_from_successful_runs(successful_batch_runs)

    target_symbols = [symbol for symbol in retry_symbols if symbol not in recovered_completed]
    pending_symbols = [symbol for symbol in target_symbols if symbol not in base_completed]
    batches = chunk_symbols(pending_symbols, args.batch_size)

    non_network_remaining = sorted(
        set(requested)
        - set(base_completed)
        - set(retry_symbols)
    )

    run_state = {
        "requested_symbols": len(requested),
        "base_completed": len(base_completed),
        "retry_target_symbols": len(retry_symbols),
        "already_recovered_from_batches": len(recovered_completed),
        "pending_retry_symbols": len(pending_symbols),
        "batch_size": args.batch_size,
        "planned_batches": len(batches),
        "limit": args.limit,
        "warmup": args.warmup,
        "step": args.step,
        "lookahead": args.lookahead,
        "profile_file": args.profile_file,
        "profile_name": args.profile_name,
        "signal_config_overrides_file": args.signal_config_overrides_file,
        "trade_config_overrides_file": args.trade_config_overrides_file,
        "research_config_overrides_file": args.research_config_overrides_file,
        "non_network_remaining": non_network_remaining,
        "successful_batch_runs": successful_batch_runs,
        "batch_results": [],
    }

    manifest_path = os.path.join(args.batch_root, "recovery_manifest.json")
    write_manifest(manifest_path, run_state)

    successful_runs = list(successful_batch_runs)
    for batch_index, batch_symbols in enumerate(batches, start=1):
        batch_success = False
        for attempt in range(1, int(args.attempts) + 1):
            result = run_batch(args, batch_index, batch_symbols, attempt)
            run_state["batch_results"].append(asdict(result))
            write_manifest(manifest_path, run_state)
            if result.success:
                successful_runs.append(result.run_dir)
                batch_success = True
                break
        if not batch_success:
            continue

    merged_sources = [args.base_run_dir] + successful_runs
    merge_cmd = [
        sys.executable,
        os.path.join(args.project_dir, "merge_quant_runs.py"),
        "--runs",
        ",".join(merged_sources),
        "--output-dir",
        args.merge_output_root,
        "--label",
        "scanner_recovered",
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    subprocess.run(merge_cmd, cwd=args.project_dir, env=env, check=False)

    run_state["merged_sources"] = merged_sources
    write_manifest(manifest_path, run_state)

    print(json.dumps({
        "base_completed": len(base_completed),
        "retry_target_symbols": len(retry_symbols),
        "batch_runs_successful": len(successful_runs),
        "pending_retry_symbols": len(pending_symbols),
        "non_network_remaining": non_network_remaining,
        "manifest": manifest_path,
    }, indent=2))


if __name__ == "__main__":
    main()
