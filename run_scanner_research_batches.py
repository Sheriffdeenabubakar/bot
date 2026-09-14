import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Set

import pandas as pd

from scanner import scan_coins


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
    parser = argparse.ArgumentParser(
        description="Run full scanner-universe quant research in small resumable batches."
    )
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--batch-root", required=True)
    parser.add_argument("--merge-output-root", required=True)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--operational-15m-limit", type=int, default=300)
    parser.add_argument("--directional-15m-limit", type=int, default=300)
    parser.add_argument("--structure-15m-limits", default="2000")
    parser.add_argument("--variants", default="baseline")
    parser.add_argument("--limit", type=int, default=240)
    parser.add_argument("--warmup", type=int, default=220)
    parser.add_argument("--step", type=int, default=60)
    parser.add_argument("--lookahead", type=int, default=12)
    parser.add_argument("--profile-file", default="")
    parser.add_argument("--profile-name", default="")
    parser.add_argument("--signal-config-overrides-file", default="")
    parser.add_argument("--trade-config-overrides-file", default="")
    parser.add_argument("--research-config-overrides-file", default="")
    parser.add_argument("--merge-label", default="")
    return parser.parse_args()


def load_scanner_symbols() -> List[Dict]:
    ranked = sorted(
        scan_coins(),
        key=lambda item: float(item.get("quoteVolume", 0) or 0),
        reverse=True,
    )
    deduped = []
    seen: Set[str] = set()
    for item in ranked:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        deduped.append(
            {
                "symbol": symbol,
                "quoteVolume": float(item.get("quoteVolume", 0) or 0),
            }
        )
        seen.add(symbol)
    return deduped


def write_symbols_file(path: str, symbols: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(symbols))
        handle.write("\n")


def chunk_symbols(symbols: List[str], batch_size: int) -> List[List[str]]:
    return [symbols[idx: idx + batch_size] for idx in range(0, len(symbols), batch_size)]


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


def list_successful_batch_runs(batch_root: str) -> List[str]:
    if not os.path.isdir(batch_root):
        return []
    successful = []
    for entry in sorted(os.listdir(batch_root)):
        run_root = os.path.join(batch_root, entry)
        if not os.path.isdir(run_root) or not entry.startswith("batch_"):
            continue
        for child in sorted(os.listdir(run_root)):
            attempt_root = os.path.join(run_root, child)
            if not os.path.isdir(attempt_root):
                continue
            run_dir = newest_timestamped_run(attempt_root)
            if run_dir and os.path.exists(os.path.join(run_dir, "summary.json")):
                successful.append(run_dir)
    return successful


def next_batch_index(successful_run_dirs: List[str]) -> int:
    max_index = 0
    for run_dir in successful_run_dirs:
        match = re.search(r"batch_(\d{3})", str(run_dir))
        if not match:
            continue
        max_index = max(max_index, int(match.group(1)))
    return max_index + 1 if max_index > 0 else 1


def load_processed_symbols(run_dirs: List[str]) -> Set[str]:
    processed: Set[str] = set()
    for run_dir in run_dirs:
        universe_path = os.path.join(run_dir, "universe.csv")
        if not os.path.exists(universe_path):
            continue
        df = pd.read_csv(universe_path)
        processed.update(
            df.loc[df["status"] == "processed", "symbol"].dropna().astype(str).tolist()
        )
    return processed


def sanitize_slug(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value))
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "profile"


def resolve_merge_label(args) -> str:
    if str(args.merge_label or "").strip():
        return str(args.merge_label).strip()
    if str(args.profile_name or "").strip():
        return f"scanner_batched_{sanitize_slug(args.profile_name)}"
    return "scanner_batched"


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
        "--limit",
        str(args.limit),
        "--warmup",
        str(args.warmup),
        "--step",
        str(args.step),
        "--lookahead",
        str(args.lookahead),
        "--max-concurrency",
        str(args.max_concurrency),
        "--output-dir",
        run_root,
    ]
    if str(args.profile_file or "").strip():
        cmd.extend(["--profile-file", str(args.profile_file).strip()])
    if str(args.profile_name or "").strip():
        cmd.extend(["--profile-name", str(args.profile_name).strip()])
    if str(args.signal_config_overrides_file or "").strip():
        cmd.extend(["--signal-config-overrides-file", str(args.signal_config_overrides_file).strip()])
    if str(args.trade_config_overrides_file or "").strip():
        cmd.extend(["--trade-config-overrides-file", str(args.trade_config_overrides_file).strip()])
    if str(args.research_config_overrides_file or "").strip():
        cmd.extend(["--research-config-overrides-file", str(args.research_config_overrides_file).strip()])

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    with open(stdout_log, "w", encoding="utf-8") as out_handle, open(
        stderr_log, "w", encoding="utf-8"
    ) as err_handle:
        completed = subprocess.run(
            cmd,
            cwd=args.project_dir,
            env=env,
            stdout=out_handle,
            stderr=err_handle,
        )

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


def merge_runs(project_dir: str, merge_output_root: str, run_dirs: List[str], label: str) -> subprocess.CompletedProcess:
    cmd = [
        sys.executable,
        os.path.join(project_dir, "merge_quant_runs.py"),
        "--runs",
        ",".join(run_dirs),
        "--output-dir",
        merge_output_root,
        "--label",
        label,
    ]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return subprocess.run(cmd, cwd=project_dir, env=env, check=False)


def main():
    args = parse_args()
    merge_label = resolve_merge_label(args)
    scanner_rows = load_scanner_symbols()
    scanner_symbols = [row["symbol"] for row in scanner_rows]
    if not scanner_symbols:
        raise RuntimeError("Scanner returned no symbols for research batching.")

    os.makedirs(args.batch_root, exist_ok=True)
    os.makedirs(args.merge_output_root, exist_ok=True)

    scanner_snapshot_path = os.path.join(args.batch_root, "scanner_symbols_snapshot.csv")
    pd.DataFrame(scanner_rows).to_csv(scanner_snapshot_path, index=False)

    successful_batch_runs = list_successful_batch_runs(args.batch_root)
    processed_symbols = load_processed_symbols(successful_batch_runs)
    pending_symbols = [symbol for symbol in scanner_symbols if symbol not in processed_symbols]
    batches = chunk_symbols(pending_symbols, int(args.batch_size))
    first_pending_batch_index = next_batch_index(successful_batch_runs)

    run_state = {
        "started_at": datetime.now().isoformat(),
        "scanner_symbols": len(scanner_symbols),
        "already_completed_symbols": len(processed_symbols),
        "pending_symbols": len(pending_symbols),
        "batch_size": int(args.batch_size),
        "planned_batches": len(batches),
        "first_pending_batch_index": first_pending_batch_index,
        "successful_batch_runs": successful_batch_runs,
        "scanner_snapshot_path": scanner_snapshot_path,
        "profile_file": str(args.profile_file or "").strip(),
        "profile_name": str(args.profile_name or "").strip(),
        "signal_config_overrides_file": str(args.signal_config_overrides_file or "").strip(),
        "trade_config_overrides_file": str(args.trade_config_overrides_file or "").strip(),
        "research_config_overrides_file": str(args.research_config_overrides_file or "").strip(),
        "merge_label": merge_label,
        "batch_results": [],
        "merge_output_root": args.merge_output_root,
    }
    manifest_path = os.path.join(args.batch_root, "research_manifest.json")
    write_manifest(manifest_path, run_state)

    merged_sources = list(successful_batch_runs)
    for batch_index, batch_symbols in enumerate(batches, start=first_pending_batch_index):
        batch_success = False
        for attempt in range(1, int(args.attempts) + 1):
            result = run_batch(args, batch_index, batch_symbols, attempt)
            run_state["batch_results"].append(asdict(result))
            write_manifest(manifest_path, run_state)
            if result.success:
                merged_sources.append(result.run_dir)
                run_state["successful_batch_runs"] = merged_sources
                batch_success = True
                write_manifest(manifest_path, run_state)
                break
        if not batch_success:
            continue

    merge_result = merge_runs(args.project_dir, args.merge_output_root, merged_sources, merge_label) if merged_sources else None
    run_state["merged_sources"] = merged_sources
    run_state["merge_return_code"] = int(merge_result.returncode) if merge_result is not None else None
    write_manifest(manifest_path, run_state)

    print(
        json.dumps(
            {
                "scanner_symbols": len(scanner_symbols),
                "already_completed_symbols": len(processed_symbols),
                "pending_symbols": len(pending_symbols),
                "planned_batches": len(batches),
                "successful_batch_runs": len(merged_sources),
                "manifest": manifest_path,
                "merge_output_root": args.merge_output_root,
                "merge_label": merge_label,
                "scanner_snapshot_path": scanner_snapshot_path,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
