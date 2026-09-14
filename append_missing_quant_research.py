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

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run only the still-missing quant-research symbols and merge them into a fresh authoritative folder."
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
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=3,
        help="Maximum number of append attempts to make for still-missing symbols.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def read_symbol_file(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8")
    return [item.strip().upper() for item in raw.replace(",", "\n").splitlines() if item.strip()]


def newest_run_dir(output_root: Path, started_at: float, exclude_name: str) -> Path:
    candidates = [
        path
        for path in output_root.iterdir()
        if path.is_dir() and path.name != exclude_name and path.stat().st_mtime >= started_at - 1
    ]
    if not candidates:
        raise RuntimeError("Could not locate the append run directory after quant_research completed.")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def combine_frames(base_frame: pd.DataFrame, retry_frame: pd.DataFrame, ts_cols: list[str]) -> pd.DataFrame:
    if base_frame.empty and retry_frame.empty:
        return pd.DataFrame()
    result = pd.concat([base_frame, retry_frame], ignore_index=True, sort=False)
    for column in ts_cols:
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], errors="coerce")
    return result.drop_duplicates().reset_index(drop=True)


def extract_completed_symbols(events_df: pd.DataFrame, trades_df: pd.DataFrame) -> list[str]:
    symbols: list[str] = []
    for frame in (events_df, trades_df):
        if frame.empty or "symbol" not in frame.columns:
            continue
        for symbol in frame["symbol"].dropna().astype(str).str.upper():
            if symbol and symbol not in symbols:
                symbols.append(symbol)
    return symbols


def resolve_target_symbols(project_dir: Path, base_run_dir: Path, args: argparse.Namespace) -> list[str]:
    if str(args.symbols_file).strip():
        return read_symbol_file(Path(str(args.symbols_file).strip()).resolve())

    if args.universe_source == "summary":
        summary = json.loads((base_run_dir / "summary.json").read_text(encoding="utf-8"))
        return [str(symbol).upper() for symbol in summary.get("symbols", []) if str(symbol).strip()]

    sys.path.insert(0, str(project_dir))
    import scanner  # type: ignore

    ranked = sorted(scanner.scan_coins(), key=lambda item: float(item.get("quoteVolume", 0) or 0), reverse=True)
    return [str(item.get("symbol", "")).upper() for item in ranked if str(item.get("symbol", "")).strip()]


def run_append_attempt(
    project_dir: Path,
    base_run_dir: Path,
    missing_symbols: list[str],
    args: argparse.Namespace,
    attempt_number: int,
) -> tuple[Path, Path]:
    output_root = project_dir / "quant_outputs_advanced"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    symbols_file = output_root / f"missing_symbols_attempt{attempt_number}_{timestamp}.txt"
    symbols_file.write_text("\n".join(missing_symbols), encoding="utf-8")
    print(f"Attempt {attempt_number}: missing symbols file -> {symbols_file}")

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

    print(f"Attempt {attempt_number}: launching append run for {len(missing_symbols)} symbols...")
    subprocess.run(command, cwd=project_dir, check=True, env=env)
    append_run_dir = newest_run_dir(output_root, started_at, base_run_dir.name)
    print(f"Attempt {attempt_number}: append run completed -> {append_run_dir}")
    return append_run_dir, symbols_file


def build_summary(
    combined_dir: Path,
    combined_events: pd.DataFrame,
    combined_trades: pd.DataFrame,
    target_symbols: list[str],
    missing_symbols_before: list[str],
    base_summary: dict[str, Any],
    append_summary: dict[str, Any],
    qr: Any,
) -> dict[str, Any]:
    split_model = qr.build_split_model()
    entered_trades_df = (
        combined_trades[combined_trades["trade_status"] == "entered"].copy()
        if not combined_trades.empty and "trade_status" in combined_trades.columns
        else pd.DataFrame()
    )

    rejection_summary_df = (
        combined_events[combined_events["decision"] != "signal"]
        .groupby(["variant", "reason_code"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["variant", "count"], ascending=[True, False])
        if not combined_events.empty
        else pd.DataFrame()
    )

    experiment_summary_df = pd.DataFrame()
    split_summary_df = pd.DataFrame()
    regime_summary_df = pd.DataFrame()
    condition_summary_df = pd.DataFrame()
    loss_driver_summary_df = pd.DataFrame()
    win_driver_summary_df = pd.DataFrame()
    setup_combo_summary_df = pd.DataFrame()
    positive_combo_summary_df = pd.DataFrame()
    walkforward_summary_df = pd.DataFrame()
    monte_carlo_summary_df = pd.DataFrame()
    robustness_summary_df = pd.DataFrame()

    if not entered_trades_df.empty:
        entered_trades_df["entry_time"] = pd.to_datetime(entered_trades_df["entry_time"])
        entered_trades_df["split"] = qr.assign_split_labels(entered_trades_df, split_model, "entry_time")
        experiment_summary_df = qr.summarize_trade_metrics(entered_trades_df, ["variant", "experiment_key"])
        split_summary_df = qr.summarize_trade_metrics(entered_trades_df, ["variant", "experiment_key", "split"])
        regime_summary_df = qr.summarize_trade_metrics(
            entered_trades_df,
            ["experiment_key", "split", "live_market_regime", "vol_regime", "liquidity_bucket", "session_bucket"],
        )
        condition_summary_df = qr.build_condition_summary(entered_trades_df)
        loss_driver_summary_df = qr.build_loss_driver_summary(condition_summary_df)
        win_driver_summary_df = qr.build_win_driver_summary(condition_summary_df)
        setup_combo_summary_df = qr.build_setup_combo_summary(entered_trades_df)
        positive_combo_summary_df = qr.build_positive_combo_summary(setup_combo_summary_df)
        walkforward_summary_df = qr.build_walkforward_summary(entered_trades_df, split_model)
        monte_carlo_summary_df = qr.build_monte_carlo_summary(entered_trades_df, split_model)
        robustness_summary_df = qr.build_robustness_summary(
            experiment_summary_df,
            split_summary_df,
            walkforward_summary_df,
            monte_carlo_summary_df,
            regime_summary_df,
        )

    outputs = {
        "events.csv": combined_events,
        "trades.csv": combined_trades,
        "rejection_summary.csv": rejection_summary_df,
        "experiment_summary.csv": experiment_summary_df,
        "split_summary.csv": split_summary_df,
        "regime_summary.csv": regime_summary_df,
        "condition_summary.csv": condition_summary_df,
        "loss_driver_summary.csv": loss_driver_summary_df,
        "win_driver_summary.csv": win_driver_summary_df,
        "setup_combo_summary.csv": setup_combo_summary_df,
        "positive_combo_summary.csv": positive_combo_summary_df,
        "walkforward_summary.csv": walkforward_summary_df,
        "monte_carlo_summary.csv": monte_carlo_summary_df,
        "robustness_summary.csv": robustness_summary_df,
    }

    for name, frame in outputs.items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        if name == "events.csv":
            frame.to_csv(combined_dir / name, index=False)
            frame.to_json(combined_dir / "events.jsonl", orient="records", lines=True, date_format="iso")
        else:
            frame.to_csv(combined_dir / name, index=False)

    completed_after = extract_completed_symbols(combined_events, combined_trades)
    unresolved_after = [symbol for symbol in target_symbols if symbol not in set(completed_after)]
    return {
        "run_dir": str(combined_dir),
        "base_run_dir": base_summary.get("run_dir"),
        "append_run_dir": append_summary.get("run_dir"),
        "symbols": target_symbols,
        "missing_symbols_before": missing_symbols_before,
        "completed_symbols_after": completed_after,
        "unresolved_symbols_after": unresolved_after,
        "variants": base_summary.get("variants", append_summary.get("variants", [])),
        "duration_sec": round(float(base_summary.get("duration_sec", 0.0)) + float(append_summary.get("duration_sec", 0.0)), 2),
        "events": int(len(combined_events)),
        "trades": int(len(entered_trades_df)),
        "top_robustness": robustness_summary_df.head(10).to_dict(orient="records") if not robustness_summary_df.empty else [],
        "top_rejections": rejection_summary_df.head(20).to_dict(orient="records") if not rejection_summary_df.empty else [],
        "top_loss_drivers": loss_driver_summary_df.head(20).to_dict(orient="records") if not loss_driver_summary_df.empty else [],
        "top_win_drivers": win_driver_summary_df.head(20).to_dict(orient="records") if not win_driver_summary_df.empty else [],
        "top_negative_combos": setup_combo_summary_df[setup_combo_summary_df["expectancy_r"] < 0].head(20).to_dict(orient="records") if not setup_combo_summary_df.empty else [],
        "top_positive_combos": positive_combo_summary_df.head(20).to_dict(orient="records") if not positive_combo_summary_df.empty else [],
    }


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args()
    project_dir = Path(args.project_dir).resolve()
    base_run_dir = Path(args.base_run_dir).resolve()
    output_root = project_dir / "quant_outputs_advanced"

    target_symbols = list(dict.fromkeys(resolve_target_symbols(project_dir, base_run_dir, args)))
    if args.max_symbols and int(args.max_symbols) > 0:
        target_symbols = target_symbols[: int(args.max_symbols)]
    if not target_symbols:
        raise RuntimeError("No target symbols resolved for append run.")

    sys.path.insert(0, str(project_dir))
    import quant_research as qr  # type: ignore
    current_base_run_dir = base_run_dir
    final_summary: dict[str, Any] | None = None

    for attempt_number in range(1, int(args.retry_attempts) + 1):
        base_events = read_csv_or_empty(current_base_run_dir / "events.csv")
        base_trades = read_csv_or_empty(current_base_run_dir / "trades.csv")
        completed_before = extract_completed_symbols(base_events, base_trades)
        completed_set = set(completed_before)
        missing_symbols = [symbol for symbol in target_symbols if symbol not in completed_set]

        print(f"Target symbols: {len(target_symbols)}")
        print(f"Attempt {attempt_number}/{int(args.retry_attempts)}")
        print(f"Completed before append: {len(completed_before)}")
        print(f"Missing before append: {len(missing_symbols)}")
        if missing_symbols:
            print(", ".join(missing_symbols))

        if not missing_symbols:
            print("No missing symbols remain. Nothing to append.")
            break
        if args.dry_run:
            return

        append_run_dir, _symbols_file = run_append_attempt(
            project_dir=project_dir,
            base_run_dir=current_base_run_dir,
            missing_symbols=missing_symbols,
            args=args,
            attempt_number=attempt_number,
        )

        base_summary = json.loads((current_base_run_dir / "summary.json").read_text(encoding="utf-8"))
        append_summary = json.loads((append_run_dir / "summary.json").read_text(encoding="utf-8"))
        append_events = read_csv_or_empty(append_run_dir / "events.csv")
        append_trades = read_csv_or_empty(append_run_dir / "trades.csv")

        combined_events = combine_frames(base_events, append_events, ["timestamp", "entry_time", "exit_time", "signal_time"])
        combined_trades = combine_frames(base_trades, append_trades, ["signal_time", "entry_time", "exit_time"])

        suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        combined_dir = output_root / f"{current_base_run_dir.name}_missing_append_merged_{suffix}"
        combined_dir.mkdir(parents=True, exist_ok=True)

        final_summary = build_summary(
            combined_dir=combined_dir,
            combined_events=combined_events,
            combined_trades=combined_trades,
            target_symbols=target_symbols,
            missing_symbols_before=missing_symbols,
            base_summary=base_summary,
            append_summary=append_summary,
            qr=qr,
        )
        (combined_dir / "summary.json").write_text(json.dumps(final_summary, indent=2, default=str), encoding="utf-8")
        print(f"Combined output written to: {combined_dir}")

        unresolved_after = final_summary.get("unresolved_symbols_after", [])
        print(f"Attempt {attempt_number}: unresolved after merge -> {len(unresolved_after)}")
        current_base_run_dir = combined_dir
        if not unresolved_after:
            break

    if final_summary:
        print(f"Final authoritative merged run: {final_summary.get('run_dir')}")


if __name__ == "__main__":
    main()
