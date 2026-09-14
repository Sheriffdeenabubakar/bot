from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from append_missing_quant_research import (
    build_summary,
    combine_frames,
    extract_completed_symbols,
    newest_run_dir,
    read_csv_or_empty,
    resolve_target_symbols,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run unresolved quant-research symbols in durable batches and merge after each finished batch."
    )
    parser.add_argument(
        "--project-dir",
        default=str(Path(__file__).resolve().parent),
        help="Project root that contains quant_research.py.",
    )
    parser.add_argument("--base-run-dir", required=True, help="Existing quant output directory to extend.")
    parser.add_argument(
        "--universe-source",
        choices=("summary", "scanner"),
        default="summary",
        help="Where to get the target symbol universe if --symbols-file is not provided.",
    )
    parser.add_argument(
        "--symbols-file",
        default="",
        help="Optional explicit symbol file. If provided, only unresolved symbols from this file are appended.",
    )
    parser.add_argument("--limit", type=int, default=1500)
    parser.add_argument("--warmup", type=int, default=320)
    parser.add_argument("--step", type=int, default=3)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--variants", default="")
    parser.add_argument("--retry-attempts", type=int, default=3)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Number of unresolved symbols to process per durable batch merge.",
    )
    parser.add_argument(
        "--state-file",
        default="",
        help="Optional state file path. If it exists, the batch campaign resumes from it.",
    )
    parser.add_argument(
        "--campaign-id",
        default="",
        help="Optional short id used for batch state and merged output names.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items[:]]
    return [items[idx: idx + size] for idx in range(0, len(items), size)]


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def load_or_init_state(
    args: argparse.Namespace,
    project_dir: Path,
    base_run_dir: Path,
    output_root: Path,
) -> tuple[Path, dict[str, Any]]:
    state_file = Path(str(args.state_file).strip()).resolve() if str(args.state_file).strip() else None
    if state_file and state_file.exists():
        state = json.loads(state_file.read_text(encoding="utf-8"))
        return state_file, state

    target_symbols = list(dict.fromkeys(resolve_target_symbols(project_dir, base_run_dir, args)))
    if args.max_symbols and int(args.max_symbols) > 0:
        target_symbols = target_symbols[: int(args.max_symbols)]
    if not target_symbols:
        raise RuntimeError("No target symbols resolved for batched append run.")

    campaign_id = str(args.campaign_id).strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    if state_file is None:
        state_file = output_root / f"append_batch_state_{campaign_id}.json"

    state = {
        "campaign_id": campaign_id,
        "project_dir": str(project_dir),
        "original_base_run_dir": str(base_run_dir),
        "current_base_run_dir": str(base_run_dir),
        "target_symbols": target_symbols,
        "retry_attempts": int(args.retry_attempts),
        "batch_size": int(args.batch_size),
        "history": [],
        "last_updated": datetime.now().isoformat(),
    }
    save_state(state_file, state)
    return state_file, state


def run_batch_attempt(
    project_dir: Path,
    output_root: Path,
    base_run_dir: Path,
    batch_symbols: list[str],
    args: argparse.Namespace,
    campaign_id: str,
    attempt_number: int,
    batch_number: int,
) -> tuple[Path, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    symbols_file = output_root / f"batch_{campaign_id}_a{attempt_number:02d}_b{batch_number:03d}_symbols.txt"
    symbols_file.write_text("\n".join(batch_symbols), encoding="utf-8")
    print(f"Attempt {attempt_number} batch {batch_number}: symbols file -> {symbols_file}")

    started_at = time.time()
    command = [
        sys.executable,
        str(project_dir / "quant_research.py"),
        "--symbols-file",
        str(symbols_file),
        "--limit",
        str(args.limit),
        "--warmup",
        str(args.warmup),
        "--step",
        str(args.step),
    ]
    if str(args.variants).strip():
        command.extend(["--variants", str(args.variants).strip()])

    env = os.environ.copy()
    env["LOG_LEVEL"] = "INFO"

    print(
        f"Attempt {attempt_number} batch {batch_number}: launching quant research for {len(batch_symbols)} symbols..."
    )
    subprocess.run(command, cwd=project_dir, check=True, env=env)
    append_run_dir = newest_run_dir(output_root, started_at, base_run_dir.name)
    print(f"Attempt {attempt_number} batch {batch_number}: append run completed -> {append_run_dir}")
    return append_run_dir, symbols_file


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args()
    project_dir = Path(args.project_dir).resolve()
    base_run_dir = Path(args.base_run_dir).resolve()
    output_root = project_dir / "quant_outputs_advanced"

    state_file, state = load_or_init_state(args, project_dir, base_run_dir, output_root)
    current_base_run_dir = Path(str(state["current_base_run_dir"])).resolve()
    target_symbols = [str(symbol).upper() for symbol in state.get("target_symbols", []) if str(symbol).strip()]
    campaign_id = str(state.get("campaign_id", "")).strip() or datetime.now().strftime("%Y%m%d_%H%M%S")

    if not target_symbols:
        raise RuntimeError("State did not contain any target symbols.")

    sys.path.insert(0, str(project_dir))
    import quant_research as qr  # type: ignore

    final_summary: dict[str, Any] | None = None

    for attempt_number in range(1, int(args.retry_attempts) + 1):
        base_events = read_csv_or_empty(current_base_run_dir / "events.csv")
        base_trades = read_csv_or_empty(current_base_run_dir / "trades.csv")
        completed_before = extract_completed_symbols(base_events, base_trades)
        completed_set = set(completed_before)
        missing_symbols = [symbol for symbol in target_symbols if symbol not in completed_set]

        print(f"Campaign state file: {state_file}")
        print(f"Campaign id: {campaign_id}")
        print(f"Attempt {attempt_number}/{int(args.retry_attempts)}")
        print(f"Completed before retry: {len(completed_before)}")
        print(f"Missing before retry: {len(missing_symbols)}")

        if not missing_symbols:
            print("No missing symbols remain. Nothing to append.")
            break
        if args.dry_run:
            return

        batches = chunked(missing_symbols, int(args.batch_size))
        for batch_number, batch_symbols in enumerate(batches, start=1):
            batch_missing_before = missing_symbols[:]
            append_run_dir, symbols_file = run_batch_attempt(
                project_dir=project_dir,
                output_root=output_root,
                base_run_dir=current_base_run_dir,
                batch_symbols=batch_symbols,
                args=args,
                campaign_id=campaign_id,
                attempt_number=attempt_number,
                batch_number=batch_number,
            )

            base_summary = json.loads((current_base_run_dir / "summary.json").read_text(encoding="utf-8"))
            append_summary = json.loads((append_run_dir / "summary.json").read_text(encoding="utf-8"))
            base_events = read_csv_or_empty(current_base_run_dir / "events.csv")
            base_trades = read_csv_or_empty(current_base_run_dir / "trades.csv")
            append_events = read_csv_or_empty(append_run_dir / "events.csv")
            append_trades = read_csv_or_empty(append_run_dir / "trades.csv")

            combined_events = combine_frames(base_events, append_events, ["timestamp", "entry_time", "exit_time", "signal_time"])
            combined_trades = combine_frames(base_trades, append_trades, ["signal_time", "entry_time", "exit_time"])

            suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            combined_dir = output_root / f"appendmerge_{campaign_id}_a{attempt_number:02d}_b{batch_number:03d}_{suffix}"
            combined_dir.mkdir(parents=True, exist_ok=True)

            final_summary = build_summary(
                combined_dir=combined_dir,
                combined_events=combined_events,
                combined_trades=combined_trades,
                target_symbols=target_symbols,
                missing_symbols_before=batch_missing_before,
                base_summary=base_summary,
                append_summary=append_summary,
                qr=qr,
            )
            (combined_dir / "summary.json").write_text(
                json.dumps(final_summary, indent=2, default=str),
                encoding="utf-8",
            )
            print(f"Attempt {attempt_number} batch {batch_number}: combined output -> {combined_dir}")

            unresolved_after = [str(symbol).upper() for symbol in final_summary.get("unresolved_symbols_after", [])]
            print(f"Attempt {attempt_number} batch {batch_number}: unresolved after merge -> {len(unresolved_after)}")

            current_base_run_dir = combined_dir
            state["current_base_run_dir"] = str(current_base_run_dir)
            state["last_updated"] = datetime.now().isoformat()
            state.setdefault("history", []).append(
                {
                    "attempt_number": attempt_number,
                    "batch_number": batch_number,
                    "symbols_file": str(symbols_file),
                    "batch_symbol_count": len(batch_symbols),
                    "append_run_dir": str(append_run_dir),
                    "combined_dir": str(combined_dir),
                    "unresolved_after": len(unresolved_after),
                }
            )
            save_state(state_file, state)

            if not unresolved_after:
                break

        if final_summary and not final_summary.get("unresolved_symbols_after"):
            break

    print(f"State file: {state_file}")
    if final_summary:
        print(f"Final authoritative merged run: {final_summary.get('run_dir')}")
    else:
        print(f"Latest authoritative merged run: {current_base_run_dir}")


if __name__ == "__main__":
    main()
