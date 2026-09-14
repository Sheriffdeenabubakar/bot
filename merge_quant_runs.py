import argparse
import json
import os
from typing import List

import numpy as np
import pandas as pd

from quant_research import (
    assign_split_labels,
    base_symbol,
    build_shadow_condition_summary,
    build_shadow_trade_condition_summary,
    build_correlation_clusters,
    build_equity_stats,
    build_execution_model,
    build_monte_carlo_summary,
    build_portfolio_model,
    build_walkforward_summary,
    classify_symbol_bucket,
    simulate_portfolio,
    summarize_trade_metrics,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Merge completed quant research run folders into one aggregate output.")
    parser.add_argument("--runs", required=True, help="Comma-separated list of completed run directories.")
    parser.add_argument("--output-dir", required=True, help="Directory where the merged aggregate output should be written.")
    parser.add_argument("--label", default="", help="Optional label suffix for the merged run id.")
    return parser.parse_args()


def load_csv_if_exists(run_dir: str, filename: str) -> pd.DataFrame:
    path = os.path.join(run_dir, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def concat_nonempty(frames: List[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid:
        return pd.DataFrame()
    return pd.concat(valid, ignore_index=True)


def dedupe_df(df: pd.DataFrame, subset: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    dedupe_keys = [column for column in subset if column in df.columns]
    if not dedupe_keys:
        return df.reset_index(drop=True)
    return df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)


def main():
    args = parse_args()
    run_dirs = [item.strip() for item in str(args.runs).split(",") if item.strip()]
    if not run_dirs:
        raise ValueError("At least one run directory is required.")

    execution_model = build_execution_model()
    portfolio_model = build_portfolio_model()

    universe_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "universe.csv") for run_dir in run_dirs]),
        ["symbol"],
    )
    events_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "events.csv") for run_dir in run_dirs]),
        ["experiment_key", "symbol", "timestamp", "decision", "reason_code"],
    )
    signals_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "signals.csv") for run_dir in run_dirs]),
        ["experiment_key", "symbol", "timestamp", "entry_time", "entry_price"],
    )
    trades_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "trades.csv") for run_dir in run_dirs]),
        ["experiment_key", "symbol", "timestamp", "entry_time", "entry_price", "exit_time"],
    )
    shadow_events_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "shadow_events.csv") for run_dir in run_dirs]),
        ["path_experiment_key", "symbol", "timestamp", "trigger_type", "path_reason_code", "parity_reason_code"],
    )
    shadow_trades_df = dedupe_df(
        concat_nonempty([load_csv_if_exists(run_dir, "shadow_trades.csv") for run_dir in run_dirs]),
        ["path_experiment_key", "symbol", "timestamp", "trigger_type", "entry_time", "entry_price", "entry_reason"],
    )

    entered_trades_df = pd.DataFrame()
    open_trades_df = pd.DataFrame()
    shadow_entered_trades_df = pd.DataFrame()
    shadow_open_trades_df = pd.DataFrame()
    shadow_counterfactual_trades_df = pd.DataFrame()
    shadow_counterfactual_entered_trades_df = pd.DataFrame()
    shadow_counterfactual_open_trades_df = pd.DataFrame()
    trade_summary_df = pd.DataFrame()
    shadow_path_summary_df = pd.DataFrame()
    shadow_counterfactual_path_summary_df = pd.DataFrame()
    shadow_counterfactual_gate_summary_df = pd.DataFrame()
    shadow_counterfactual_outcome_summary_df = pd.DataFrame()
    split_summary_df = pd.DataFrame()
    shadow_split_summary_df = pd.DataFrame()
    regime_summary_df = pd.DataFrame()
    shadow_regime_summary_df = pd.DataFrame()
    bucket_summary_df = pd.DataFrame()
    shadow_bucket_summary_df = pd.DataFrame()
    symbol_summary_df = pd.DataFrame()
    shadow_symbol_summary_df = pd.DataFrame()
    cluster_summary_df = pd.DataFrame()
    portfolio_df = pd.DataFrame()
    equity_df = pd.DataFrame()
    portfolio_summary_df = pd.DataFrame()
    walkforward_summary_df = pd.DataFrame()
    monte_carlo_summary_df = pd.DataFrame()
    selected_experiment_summary_df = pd.DataFrame()
    variant_summary_df = pd.DataFrame()
    experiment_summary_df = pd.DataFrame()
    rejection_summary_df = pd.DataFrame()
    shadow_rejection_summary_df = pd.DataFrame()
    shadow_parity_rejection_summary_df = pd.DataFrame()
    shadow_condition_summary_df = pd.DataFrame()
    shadow_trade_condition_summary_df = pd.DataFrame()
    shadow_trigger_summary_df = pd.DataFrame()

    if not events_df.empty:
        events_df["timestamp"] = pd.to_datetime(events_df["timestamp"], errors="coerce")
        events_df["split"] = assign_split_labels(events_df, portfolio_model, "timestamp")
        rejection_summary_df = (
            events_df[events_df["decision"] != "signal"]
            .groupby(["variant", "structure_15m_limit", "reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "count"], ascending=[True, True, False])
        )

    if not shadow_events_df.empty:
        shadow_events_df["timestamp"] = pd.to_datetime(shadow_events_df["timestamp"], errors="coerce")
        shadow_events_df["split"] = assign_split_labels(shadow_events_df, portfolio_model, "timestamp")
        shadow_rejection_summary_df = (
            shadow_events_df[~shadow_events_df["path_ready"].fillna(False)]
            .groupby(["variant", "structure_15m_limit", "trigger_type", "path_reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "trigger_type", "count"], ascending=[True, True, True, False])
        )
        shadow_parity_rejection_summary_df = (
            shadow_events_df[~shadow_events_df["parity_accepted"].fillna(False)]
            .groupby(["variant", "structure_15m_limit", "trigger_type", "parity_reason_code"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["variant", "structure_15m_limit", "trigger_type", "count"], ascending=[True, True, True, False])
        )
        shadow_condition_cols = [column for column in shadow_events_df.columns if str(column).startswith("cond_")]
        if shadow_condition_cols:
            shadow_condition_summary_df = build_shadow_condition_summary(shadow_events_df, shadow_condition_cols)
        shadow_trigger_summary_df = (
            shadow_events_df.groupby("trigger_type", dropna=False)
            .agg(
                rows=("trigger_type", "size"),
                candidate_detected=("candidate_detected", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())) if "candidate_detected" in shadow_events_df.columns else ("trigger_type", "size"),
                path_ready=("path_ready", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())) if "path_ready" in shadow_events_df.columns else ("trigger_type", "size"),
                parity_accepted=("parity_accepted", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())) if "parity_accepted" in shadow_events_df.columns else ("trigger_type", "size"),
                parity_entry_ready=("parity_entry_ready", lambda s: int(pd.Series(s).fillna(False).astype(bool).sum())) if "parity_entry_ready" in shadow_events_df.columns else ("trigger_type", "size"),
            )
            .reset_index()
            .sort_values("trigger_type")
        )

    if not trades_df.empty:
        trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"], errors="coerce")
        trades_df["entry_time"] = pd.to_datetime(trades_df["entry_time"], errors="coerce")
        trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"], errors="coerce")
        trades_df["split"] = assign_split_labels(trades_df, portfolio_model, "entry_time")

        universe_quote_map = {}
        if not universe_df.empty and "symbol" in universe_df.columns:
            quote_series = (
                universe_df.set_index("symbol")["quote_volume"]
                if "quote_volume" in universe_df.columns
                else pd.Series(dtype="float64")
            )
            universe_quote_map = quote_series.to_dict()

        return_series_map = {}
        for run_dir in run_dirs:
            events_path = os.path.join(run_dir, "events.csv")
            if os.path.exists(events_path):
                # Return series are only used for clustering. We reuse symbols from trades if available.
                continue
        cluster_map = build_correlation_clusters(return_series_map, portfolio_model)
        if "cluster_id" in trades_df.columns:
            trades_df["cluster_id"] = trades_df["cluster_id"].fillna("")
        else:
            trades_df["cluster_id"] = ""
        trades_df["cluster_id"] = trades_df.apply(
            lambda row: row["cluster_id"] if str(row["cluster_id"]).strip() else cluster_map.get(row["symbol"], f"cluster_{base_symbol(row['symbol'])}"),
            axis=1,
        )
        cluster_summary_df = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "cluster_id": cluster_map.get(symbol, f"cluster_{base_symbol(symbol)}"),
                    "symbol_bucket": classify_symbol_bucket(symbol),
                    "quote_volume": universe_quote_map.get(symbol, np.nan),
                }
                for symbol in sorted(trades_df["symbol"].dropna().astype(str).unique())
            ]
        )

        entered_trades_df = trades_df[trades_df["trade_status"] == "entered"].copy()
        open_trades_df = trades_df[trades_df["trade_status"] == "open_at_data_end"].copy()
        if not entered_trades_df.empty:
            trade_summary_df = summarize_trade_metrics(entered_trades_df, ["variant", "structure_15m_limit", "experiment_key"])
            split_summary_df = summarize_trade_metrics(entered_trades_df, ["variant", "structure_15m_limit", "experiment_key", "split"])
            regime_summary_df = summarize_trade_metrics(
                entered_trades_df,
                ["experiment_key", "split", "structure_4h", "structure_15m", "trend_regime", "vol_regime", "liquidity_bucket"],
            )
            bucket_summary_df = summarize_trade_metrics(
                entered_trades_df,
                ["experiment_key", "split", "symbol_bucket", "liquidity_bucket", "session_bucket"],
            )
            symbol_summary_df = summarize_trade_metrics(entered_trades_df, ["experiment_key", "symbol"])

            portfolio_df, equity_df = simulate_portfolio(entered_trades_df, portfolio_model)
            if not portfolio_df.empty:
                portfolio_summary_rows = []
                for experiment_key, group in portfolio_df.groupby("experiment_key"):
                    eq = equity_df[equity_df["experiment_key"] == experiment_key]
                    stats = build_equity_stats(eq)
                    metric_row = summarize_trade_metrics(group, ["experiment_key"]).iloc[0]
                    portfolio_summary_rows.append(
                        {
                            "experiment_key": experiment_key,
                            "trades": int(metric_row["trades"]),
                            "win_rate_pct": float(metric_row["win_rate_pct"]),
                            "expectancy_r": float(metric_row["expectancy_r"]),
                            "profit_factor": float(metric_row["profit_factor"]) if pd.notna(metric_row["profit_factor"]) else np.nan,
                            "total_net_pnl": float(metric_row["total_net_pnl"]),
                            **stats,
                        }
                    )
                portfolio_summary_df = pd.DataFrame(portfolio_summary_rows).sort_values("experiment_key")
                walkforward_summary_df = build_walkforward_summary(portfolio_df, portfolio_model)
                monte_carlo_summary_df = build_monte_carlo_summary(portfolio_df, portfolio_model)

            if not trade_summary_df.empty and not split_summary_df.empty:
                train_rank = split_summary_df[split_summary_df["split"] == "train"].sort_values(
                    ["expectancy_r", "profit_factor", "trades"], ascending=[False, False, False]
                )
                if not train_rank.empty:
                    chosen = train_rank.iloc[0]["experiment_key"]
                    selected_experiment_summary_df = split_summary_df[split_summary_df["experiment_key"] == chosen].copy()
                    selected_experiment_summary_df.insert(0, "selected_experiment", chosen)

    if not shadow_trades_df.empty:
        shadow_trades_df["timestamp"] = pd.to_datetime(shadow_trades_df["timestamp"], errors="coerce")
        if "entry_time" in shadow_trades_df.columns:
            shadow_trades_df["entry_time"] = pd.to_datetime(shadow_trades_df["entry_time"], errors="coerce")
        if "exit_time" in shadow_trades_df.columns:
            shadow_trades_df["exit_time"] = pd.to_datetime(shadow_trades_df["exit_time"], errors="coerce")
        shadow_accepted_trades_df = shadow_trades_df[
            shadow_trades_df["shadow_trade_mode"] == "accepted_parity_trade"
        ].copy()
        shadow_counterfactual_trades_df = shadow_trades_df[
            shadow_trades_df["shadow_trade_mode"] == "counterfactual_rejected_candidate"
        ].copy()
        shadow_entered_trades_df = shadow_accepted_trades_df[
            shadow_accepted_trades_df["trade_status"] == "entered"
        ].copy()
        shadow_open_trades_df = shadow_accepted_trades_df[
            shadow_accepted_trades_df["trade_status"] == "open_at_data_end"
        ].copy()
        shadow_counterfactual_entered_trades_df = shadow_counterfactual_trades_df[
            shadow_counterfactual_trades_df["trade_status"] == "entered"
        ].copy()
        shadow_counterfactual_open_trades_df = shadow_counterfactual_trades_df[
            shadow_counterfactual_trades_df["trade_status"] == "open_at_data_end"
        ].copy()
        if not shadow_entered_trades_df.empty:
            if "entry_time" in shadow_entered_trades_df.columns:
                shadow_entered_trades_df["split"] = assign_split_labels(shadow_entered_trades_df, portfolio_model, "entry_time")
            else:
                shadow_entered_trades_df["split"] = "all"
            shadow_path_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["variant", "structure_15m_limit", "experiment_key", "path_experiment_key", "trigger_type"],
            )
            shadow_split_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["variant", "structure_15m_limit", "experiment_key", "path_experiment_key", "trigger_type", "split"],
            )
            shadow_regime_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["path_experiment_key", "trigger_type", "split", "structure_4h", "structure_15m", "trend_regime", "vol_regime", "liquidity_bucket", "session_bucket"],
            )
            shadow_bucket_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["path_experiment_key", "trigger_type", "split", "symbol_bucket", "liquidity_bucket", "session_bucket"],
            )
            shadow_symbol_summary_df = summarize_trade_metrics(
                shadow_entered_trades_df,
                ["path_experiment_key", "trigger_type", "symbol"],
            )
            shadow_trade_condition_cols = [column for column in shadow_entered_trades_df.columns if str(column).startswith("cond_")]
            if shadow_trade_condition_cols:
                shadow_trade_condition_summary_df = build_shadow_trade_condition_summary(
                    shadow_entered_trades_df,
                    shadow_trade_condition_cols,
                )
        if not shadow_counterfactual_entered_trades_df.empty:
            if "entry_time" in shadow_counterfactual_entered_trades_df.columns:
                shadow_counterfactual_entered_trades_df["split"] = assign_split_labels(
                    shadow_counterfactual_entered_trades_df,
                    portfolio_model,
                    "entry_time",
                )
            else:
                shadow_counterfactual_entered_trades_df["split"] = "all"
            shadow_counterfactual_path_summary_df = summarize_trade_metrics(
                shadow_counterfactual_entered_trades_df,
                [
                    "variant",
                    "structure_15m_limit",
                    "path_experiment_key",
                    "trigger_type",
                    "counterfactual_rejection_stage",
                ],
            )
            shadow_counterfactual_gate_summary_df = summarize_trade_metrics(
                shadow_counterfactual_entered_trades_df,
                [
                    "variant",
                    "structure_15m_limit",
                    "trigger_type",
                    "counterfactual_rejection_stage",
                    "counterfactual_gate_reason_code",
                ],
            )
        if not shadow_counterfactual_trades_df.empty:
            shadow_counterfactual_outcome_summary_df = (
                shadow_counterfactual_trades_df.groupby(
                    [
                        "variant",
                        "structure_15m_limit",
                        "trigger_type",
                        "counterfactual_rejection_stage",
                        "counterfactual_gate_reason_code",
                        "counterfactual_outcome_bucket",
                    ],
                    dropna=False,
                )
                .size()
                .reset_index(name="count")
                .sort_values(
                    [
                        "variant",
                        "structure_15m_limit",
                        "trigger_type",
                        "counterfactual_rejection_stage",
                        "count",
                    ],
                    ascending=[True, True, True, True, False],
                )
            )

    if not events_df.empty:
        eval_summary_variant = (
            events_df.groupby("variant", as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s.astype(bool)).sum())),
            )
        )
        eval_summary_experiment = (
            events_df.groupby(["variant", "structure_15m_limit", "experiment_key"], as_index=False)
            .agg(
                evaluations=("decision", "size"),
                signals=("decision", lambda s: int((s == "signal").sum())),
                signal_rate_pct=("decision", lambda s: round(float((s == "signal").mean() * 100.0), 2)),
                invariant_violations=("invariant_ok", lambda s: int((~s.astype(bool)).sum())),
            )
        )
        variant_metrics = summarize_trade_metrics(entered_trades_df, ["variant"]) if not entered_trades_df.empty else pd.DataFrame(columns=["variant"])
        trade_summary_ready = trade_summary_df if not trade_summary_df.empty else pd.DataFrame(columns=["variant", "structure_15m_limit", "experiment_key"])
        variant_summary_df = eval_summary_variant.merge(variant_metrics, on="variant", how="left").fillna(0.0)
        experiment_summary_df = eval_summary_experiment.merge(
            trade_summary_ready,
            on=["variant", "structure_15m_limit", "experiment_key"],
            how="left",
        ).fillna(0.0)

    os.makedirs(args.output_dir, exist_ok=True)
    run_id = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    if args.label:
        run_id = f"{run_id}_{args.label}"
    run_dir = os.path.join(args.output_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)

    if not universe_df.empty:
        universe_df.to_csv(os.path.join(run_dir, "universe.csv"), index=False)
    if not events_df.empty:
        events_df.to_csv(os.path.join(run_dir, "events.csv"), index=False)
        with open(os.path.join(run_dir, "events.jsonl"), "w", encoding="utf-8") as handle:
            for record in events_df.to_dict(orient="records"):
                handle.write(json.dumps(record, default=str) + "\n")
    if not shadow_events_df.empty:
        shadow_events_df.to_csv(os.path.join(run_dir, "shadow_events.csv"), index=False)
        with open(os.path.join(run_dir, "shadow_events.jsonl"), "w", encoding="utf-8") as handle:
            for record in shadow_events_df.to_dict(orient="records"):
                handle.write(json.dumps(record, default=str) + "\n")
    if not signals_df.empty:
        signals_df.to_csv(os.path.join(run_dir, "signals.csv"), index=False)
    if not trades_df.empty:
        trades_df.to_csv(os.path.join(run_dir, "trades.csv"), index=False)
    if not shadow_trades_df.empty:
        shadow_trades_df.to_csv(os.path.join(run_dir, "shadow_trades.csv"), index=False)
    if not shadow_counterfactual_trades_df.empty:
        shadow_counterfactual_trades_df.to_csv(os.path.join(run_dir, "shadow_counterfactual_trades.csv"), index=False)
    if not entered_trades_df.empty:
        entered_trades_df.to_csv(os.path.join(run_dir, "entered_trades.csv"), index=False)
    if not open_trades_df.empty:
        open_trades_df.to_csv(os.path.join(run_dir, "open_trades.csv"), index=False)
    if not shadow_entered_trades_df.empty:
        shadow_entered_trades_df.to_csv(os.path.join(run_dir, "shadow_entered_trades.csv"), index=False)
    if not shadow_open_trades_df.empty:
        shadow_open_trades_df.to_csv(os.path.join(run_dir, "shadow_open_trades.csv"), index=False)
    if not shadow_counterfactual_entered_trades_df.empty:
        shadow_counterfactual_entered_trades_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_entered_trades.csv"),
            index=False,
        )
    if not shadow_counterfactual_open_trades_df.empty:
        shadow_counterfactual_open_trades_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_open_trades.csv"),
            index=False,
        )
    if not trade_summary_df.empty:
        trade_summary_df.to_csv(os.path.join(run_dir, "trade_summary.csv"), index=False)
    if not shadow_path_summary_df.empty:
        shadow_path_summary_df.to_csv(os.path.join(run_dir, "shadow_path_summary.csv"), index=False)
    if not shadow_counterfactual_path_summary_df.empty:
        shadow_counterfactual_path_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_path_summary.csv"),
            index=False,
        )
    if not shadow_counterfactual_gate_summary_df.empty:
        shadow_counterfactual_gate_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_gate_summary.csv"),
            index=False,
        )
    if not shadow_counterfactual_outcome_summary_df.empty:
        shadow_counterfactual_outcome_summary_df.to_csv(
            os.path.join(run_dir, "shadow_counterfactual_outcome_summary.csv"),
            index=False,
        )
    if not variant_summary_df.empty:
        variant_summary_df.to_csv(os.path.join(run_dir, "variant_summary.csv"), index=False)
    if not experiment_summary_df.empty:
        experiment_summary_df.to_csv(os.path.join(run_dir, "experiment_summary.csv"), index=False)
    if not split_summary_df.empty:
        split_summary_df.to_csv(os.path.join(run_dir, "split_summary.csv"), index=False)
    if not shadow_split_summary_df.empty:
        shadow_split_summary_df.to_csv(os.path.join(run_dir, "shadow_split_summary.csv"), index=False)
    if not regime_summary_df.empty:
        regime_summary_df.to_csv(os.path.join(run_dir, "regime_summary.csv"), index=False)
    if not shadow_regime_summary_df.empty:
        shadow_regime_summary_df.to_csv(os.path.join(run_dir, "shadow_regime_summary.csv"), index=False)
    if not rejection_summary_df.empty:
        rejection_summary_df.to_csv(os.path.join(run_dir, "rejection_summary.csv"), index=False)
    if not shadow_rejection_summary_df.empty:
        shadow_rejection_summary_df.to_csv(os.path.join(run_dir, "shadow_rejection_summary.csv"), index=False)
    if not shadow_parity_rejection_summary_df.empty:
        shadow_parity_rejection_summary_df.to_csv(os.path.join(run_dir, "shadow_parity_rejection_summary.csv"), index=False)
    if not symbol_summary_df.empty:
        symbol_summary_df.to_csv(os.path.join(run_dir, "symbol_summary.csv"), index=False)
    if not shadow_symbol_summary_df.empty:
        shadow_symbol_summary_df.to_csv(os.path.join(run_dir, "shadow_symbol_summary.csv"), index=False)
    if not bucket_summary_df.empty:
        bucket_summary_df.to_csv(os.path.join(run_dir, "bucket_summary.csv"), index=False)
    if not shadow_bucket_summary_df.empty:
        shadow_bucket_summary_df.to_csv(os.path.join(run_dir, "shadow_bucket_summary.csv"), index=False)
    if not shadow_condition_summary_df.empty:
        shadow_condition_summary_df.to_csv(os.path.join(run_dir, "shadow_condition_summary.csv"), index=False)
    if not shadow_trade_condition_summary_df.empty:
        shadow_trade_condition_summary_df.to_csv(os.path.join(run_dir, "shadow_trade_condition_summary.csv"), index=False)
    if not cluster_summary_df.empty:
        cluster_summary_df.to_csv(os.path.join(run_dir, "cluster_summary.csv"), index=False)
    if not portfolio_df.empty:
        portfolio_df.to_csv(os.path.join(run_dir, "portfolio_trades.csv"), index=False)
    if not equity_df.empty:
        equity_df.to_csv(os.path.join(run_dir, "portfolio_equity.csv"), index=False)
    if not portfolio_summary_df.empty:
        portfolio_summary_df.to_csv(os.path.join(run_dir, "portfolio_summary.csv"), index=False)
    if not walkforward_summary_df.empty:
        walkforward_summary_df.to_csv(os.path.join(run_dir, "walkforward_summary.csv"), index=False)
    if not monte_carlo_summary_df.empty:
        monte_carlo_summary_df.to_csv(os.path.join(run_dir, "monte_carlo_summary.csv"), index=False)
    if not selected_experiment_summary_df.empty:
        selected_experiment_summary_df.to_csv(os.path.join(run_dir, "selected_experiment_summary.csv"), index=False)

    requested_symbols = 0
    processed_symbols = 0
    skipped_symbols = 0
    if not universe_df.empty:
        requested_symbols = int(universe_df["symbol"].nunique())
        processed_symbols = int((universe_df["status"] == "processed").sum()) if "status" in universe_df.columns else 0
        skipped_symbols = int((universe_df["status"] != "processed").sum()) if "status" in universe_df.columns else 0

    summary = {
        "run_id": run_id,
        "merge_sources": run_dirs,
        "symbols_requested": requested_symbols,
        "symbols_processed": processed_symbols,
        "symbols_skipped": skipped_symbols,
        "open_trades_count": int(len(open_trades_df)),
        "shadow_open_trades_count": int(len(shadow_open_trades_df)),
        "shadow_counterfactual_trades_count": int(len(shadow_counterfactual_trades_df)),
        "shadow_counterfactual_open_trades_count": int(len(shadow_counterfactual_open_trades_df)),
        "execution_model": execution_model.__dict__,
        "portfolio_model": portfolio_model.__dict__,
        "variant_summary": variant_summary_df.to_dict(orient="records") if not variant_summary_df.empty else [],
        "experiment_summary": experiment_summary_df.to_dict(orient="records") if not experiment_summary_df.empty else [],
        "portfolio_summary": portfolio_summary_df.to_dict(orient="records") if not portfolio_summary_df.empty else [],
        "shadow_trigger_summary": shadow_trigger_summary_df.to_dict(orient="records") if not shadow_trigger_summary_df.empty else [],
        "shadow_path_summary": shadow_path_summary_df.to_dict(orient="records") if not shadow_path_summary_df.empty else [],
        "shadow_counterfactual_path_summary": shadow_counterfactual_path_summary_df.to_dict(orient="records") if not shadow_counterfactual_path_summary_df.empty else [],
        "shadow_counterfactual_top_gates": shadow_counterfactual_gate_summary_df.head(25).to_dict(orient="records") if not shadow_counterfactual_gate_summary_df.empty else [],
        "shadow_counterfactual_outcomes": shadow_counterfactual_outcome_summary_df.head(50).to_dict(orient="records") if not shadow_counterfactual_outcome_summary_df.empty else [],
        "shadow_top_rejections": shadow_rejection_summary_df.head(25).to_dict(orient="records") if not shadow_rejection_summary_df.empty else [],
        "shadow_top_parity_rejections": shadow_parity_rejection_summary_df.head(25).to_dict(orient="records") if not shadow_parity_rejection_summary_df.empty else [],
        "top_rejections": rejection_summary_df.head(25).to_dict(orient="records") if not rejection_summary_df.empty else [],
        "output_dir": run_dir,
    }
    with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)

    print(f"Merged outputs written to {run_dir}")


if __name__ == "__main__":
    main()
