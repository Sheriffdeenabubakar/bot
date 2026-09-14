from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


TARGET_WIN_RATE = 0.70
DEFAULT_THRESHOLDS = [round(x, 2) for x in np.arange(0.50, 0.96, 0.05)]
BOOL_TRUE_SET = {"1", "true", "yes", "y", "on"}
BOOL_FALSE_SET = {"0", "false", "no", "n", "off"}
RAW_TEXT_FEATURES = {"signal_rationale_text", "entry_reason"}


@dataclass(frozen=True)
class DatasetSplit:
    train_ids: set[str]
    validate_ids: set[str]
    test_ids: set[str]


@dataclass(frozen=True)
class SplitRunArtifacts:
    feature_cols: list[str]
    categorical_cols: list[str]
    numeric_cols: list[str]
    bool_cols: list[str]
    skipped_features: list[dict[str, Any]]
    model: Pipeline
    threshold_df: pd.DataFrame
    selected_threshold: float
    threshold_selection_reason: str
    train_eval: dict[str, Any]
    validate_eval: dict[str, Any]
    test_eval: dict[str, Any]
    split_metrics: dict[str, Any]
    train_df: pd.DataFrame
    validate_df: pd.DataFrame
    test_df: pd.DataFrame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a research-only probability filter from completed quant research outputs."
    )
    parser.add_argument(
        "--run-dirs",
        nargs="+",
        required=True,
        help="One or more completed quant research output directories that contain trades.csv.",
    )
    parser.add_argument(
        "--output-root",
        default=str(Path(__file__).resolve().parent / "probability_filter_outputs"),
        help="Folder where probability-filter research artifacts should be written.",
    )
    parser.add_argument(
        "--min-opportunity-variants",
        type=int,
        default=3,
        help="Minimum number of variant trade rows required to keep an opportunity.",
    )
    parser.add_argument(
        "--train-frac",
        type=float,
        default=0.60,
        help="Chronological train fraction at market-opportunity level.",
    )
    parser.add_argument(
        "--validate-frac",
        type=float,
        default=0.20,
        help="Chronological validation fraction at market-opportunity level.",
    )
    parser.add_argument(
        "--min-threshold-samples",
        type=int,
        default=5,
        help="Minimum selected opportunities required before a threshold is considered actionable.",
    )
    parser.add_argument(
        "--skip-model-save",
        action="store_true",
        help="Skip saving the fitted sklearn pipeline to disk.",
    )
    parser.add_argument(
        "--direction",
        choices=["BUY", "SELL"],
        help="Optional direction filter for direction-specific probability research.",
    )
    parser.add_argument(
        "--exclude-features",
        nargs="*",
        default=[],
        help="Optional extra feature names to exclude from modeling.",
    )
    parser.add_argument(
        "--walkforward-folds",
        type=int,
        default=4,
        help="Number of expanding walk-forward folds to run after the main split.",
    )
    parser.add_argument(
        "--max-categorical-cardinality",
        type=int,
        default=24,
        help="Maximum unique values allowed for a categorical feature before it is auto-excluded.",
    )
    parser.add_argument(
        "--max-categorical-ratio",
        type=float,
        default=0.20,
        help="Maximum unique/row ratio allowed for a categorical feature before it is auto-excluded.",
    )
    return parser.parse_args()


def coerce_bool(value: Any) -> Any:
    if pd.isna(value):
        return np.nan
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in BOOL_TRUE_SET:
        return True
    if lowered in BOOL_FALSE_SET:
        return False
    return value


def safe_float(value: Any, default: float = np.nan) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def make_one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


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

    for col in ["signal_time", "entry_time", "exit_time"]:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors="coerce")

    numeric_cols = [
        "confidence_score",
        "htf_confluence",
        "funding_confluence",
        "confirmation_score",
        "setup_tag_count",
        "signal_rationale_count",
        "net_r",
        "gross_r",
        "bars_to_first_touch",
        "mfe_r",
        "mae_r",
    ]
    for col in numeric_cols:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    bool_cols = [col for col in result.columns if col.startswith("has_") or col.startswith("is_")]
    for col in bool_cols:
        result[col] = result[col].map(coerce_bool)

    result["market_opportunity_id"] = (
        result["symbol"].astype(str)
        + "|"
        + result["signal_time"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("missing_ts")
    )
    return result


def first_non_null(series: pd.Series) -> Any:
    non_null = series.dropna()
    if non_null.empty:
        return np.nan
    return non_null.iloc[0]


def mode_or_first(series: pd.Series) -> Any:
    cleaned = series.dropna()
    if cleaned.empty:
        return np.nan
    modes = cleaned.mode(dropna=True)
    if not modes.empty:
        return modes.iloc[0]
    return cleaned.iloc[0]


def build_opportunity_dataset(trades_df: pd.DataFrame, min_variants: int) -> pd.DataFrame:
    if trades_df.empty:
        return pd.DataFrame()

    feature_cols = [
        "source_run",
        "symbol",
        "symbol_bucket",
        "liquidity_bucket",
        "session_bucket",
        "live_market_regime",
        "trend_regime",
        "vol_regime",
        "adx_regime",
        "direction",
        "primary_setup",
        "confirmation_bucket",
        "confidence_bucket",
        "htf_alignment_bucket",
        "setup_signature",
        "first_touch",
        "has_liquidity_sweep",
        "has_structure_breakout",
        "has_wyckoff",
        "has_consolidation_break",
        "has_ibo",
        "has_retest",
        "has_candlestick_confirmation",
        "has_volume_spike",
        "has_momentum_divergence",
        "has_order_flow",
        "has_depth_imbalance",
        "has_fibonacci_proximity",
        "has_vwap_proximity",
        "has_funding_alignment",
        "has_ensemble_alignment",
        "has_1h_confirmation",
        "has_4h_confirmation",
        "has_confirmation_bonus",
        "is_trending_strong",
        "is_trending_weak",
        "is_ranging_context",
        "confidence_score",
        "confirmation_score",
        "htf_confluence",
        "funding_confluence",
        "setup_tag_count",
        "signal_rationale_count",
    ]
    feature_cols = [col for col in feature_cols if col in trades_df.columns]

    group_cols = ["source_run", "market_opportunity_id", "symbol", "signal_time"]
    grouped = trades_df.groupby(group_cols, dropna=False)

    rows: list[dict[str, Any]] = []
    for keys, frame in grouped:
        source_run, market_opportunity_id, symbol, signal_time = keys
        if len(frame) < int(min_variants):
            continue

        wins = int((pd.to_numeric(frame["net_r"], errors="coerce") > 0).sum())
        losses = int((pd.to_numeric(frame["net_r"], errors="coerce") <= 0).sum())
        variant_count = int(len(frame))
        win_share = wins / variant_count if variant_count else 0.0
        mean_net_r = float(pd.to_numeric(frame["net_r"], errors="coerce").mean())
        median_net_r = float(pd.to_numeric(frame["net_r"], errors="coerce").median())

        row = {
            "source_run": source_run,
            "market_opportunity_id": market_opportunity_id,
            "symbol": symbol,
            "signal_time": pd.Timestamp(signal_time),
            "variant_count": variant_count,
            "wins": wins,
            "losses": losses,
            "win_share": round(win_share, 6),
            "mean_net_r": round(mean_net_r, 6),
            "median_net_r": round(median_net_r, 6),
            "consensus_win": bool((win_share >= 0.60) and (mean_net_r > 0.0)),
            "any_win": bool(wins > 0),
        }

        for col in feature_cols:
            series = frame[col]
            if series.dropna().empty:
                row[col] = np.nan
            elif pd.api.types.is_bool_dtype(series.dropna()) or all(
                isinstance(val, (bool, np.bool_)) for val in series.dropna()
            ):
                row[col] = bool(series.dropna().mean() >= 0.5)
            elif pd.api.types.is_numeric_dtype(series):
                row[col] = float(series.dropna().median())
            else:
                row[col] = mode_or_first(series)

        rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(["signal_time", "symbol", "source_run"]).reset_index(drop=True)
    return result


def split_market_opportunities(
    opportunities_df: pd.DataFrame,
    train_frac: float,
    validate_frac: float,
) -> DatasetSplit:
    unique_market = (
        opportunities_df[["market_opportunity_id", "signal_time"]]
        .drop_duplicates()
        .sort_values(["signal_time", "market_opportunity_id"])
        .reset_index(drop=True)
    )
    total = max(1, len(unique_market))
    train_end = max(1, int(total * train_frac))
    validate_end = max(train_end + 1, int(total * (train_frac + validate_frac)))
    validate_end = min(validate_end, total)

    train_ids = set(unique_market.iloc[:train_end]["market_opportunity_id"].tolist())
    validate_ids = set(unique_market.iloc[train_end:validate_end]["market_opportunity_id"].tolist())
    test_ids = set(unique_market.iloc[validate_end:]["market_opportunity_id"].tolist())

    if not validate_ids and test_ids:
        validate_ids = {sorted(test_ids)[0]}
        test_ids = test_ids - validate_ids
    if not test_ids and validate_ids:
        moved = {sorted(validate_ids)[-1]}
        validate_ids = validate_ids - moved
        test_ids = moved

    return DatasetSplit(train_ids=train_ids, validate_ids=validate_ids, test_ids=test_ids)


def build_feature_sets(
    opportunities_df: pd.DataFrame,
    excluded_features: set[str] | None = None,
    max_categorical_cardinality: int = 24,
    max_categorical_ratio: float = 0.20,
) -> tuple[list[str], list[str], list[str], list[dict[str, Any]]]:
    reserved = {
        "source_run",
        "market_opportunity_id",
        "symbol",
        "signal_time",
        "variant_count",
        "wins",
        "losses",
        "win_share",
        "mean_net_r",
        "median_net_r",
        "consensus_win",
        "any_win",
        "first_touch",
        "split",
        "predicted_win_prob",
    }

    excluded = set(excluded_features or set())
    skipped_features: list[dict[str, Any]] = []
    feature_cols = [
        col
        for col in opportunities_df.columns
        if col not in reserved and col not in RAW_TEXT_FEATURES and col not in excluded
    ]
    for col in opportunities_df.columns:
        if col in RAW_TEXT_FEATURES:
            skipped_features.append({"feature": col, "reason": "raw_text"})
        elif col in excluded:
            skipped_features.append({"feature": col, "reason": "manually_excluded"})
    bool_cols: list[str] = []
    numeric_cols: list[str] = []
    categorical_cols: list[str] = []
    total_rows = max(1, len(opportunities_df))

    for col in feature_cols:
        series = opportunities_df[col]
        non_null = series.dropna()
        if non_null.empty:
            skipped_features.append({"feature": col, "reason": "all_null"})
            continue
        unique_count = int(non_null.nunique(dropna=True))
        if unique_count <= 1:
            skipped_features.append({"feature": col, "reason": "constant", "unique_count": unique_count})
            continue
        if all(isinstance(val, (bool, np.bool_)) for val in non_null):
            bool_cols.append(col)
        elif pd.api.types.is_numeric_dtype(series):
            numeric_cols.append(col)
        else:
            unique_ratio = unique_count / total_rows
            if unique_count > int(max_categorical_cardinality) or unique_ratio > float(max_categorical_ratio):
                skipped_features.append(
                    {
                        "feature": col,
                        "reason": "high_cardinality",
                        "unique_count": unique_count,
                        "unique_ratio": round(unique_ratio, 6),
                    }
                )
                continue
            categorical_cols.append(col)

    return categorical_cols, numeric_cols, bool_cols, skipped_features


def prepare_model_frame(opportunities_df: pd.DataFrame, bool_cols: list[str]) -> pd.DataFrame:
    model_df = opportunities_df.copy()
    for col in bool_cols:
        if col not in model_df.columns:
            continue
        model_df[col] = model_df[col].map(lambda value: np.nan if pd.isna(value) else float(bool(value)))
    return model_df


def build_model(categorical_cols: list[str], numeric_cols: list[str], bool_cols: list[str]) -> Pipeline:
    transformers = []
    if categorical_cols:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", make_one_hot_encoder()),
                    ]
                ),
                categorical_cols,
            )
        )
    if numeric_cols:
        transformers.append(
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_cols,
            )
        )
    if bool_cols:
        transformers.append(
            (
                "boolean",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                    ]
                ),
                bool_cols,
            )
        )

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")
    estimator = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        random_state=42,
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", estimator),
        ]
    )


def evaluate_predictions(df: pd.DataFrame, threshold: float) -> dict[str, Any]:
    selected = df[df["predicted_win_prob"] >= threshold].copy()
    selected_count = int(len(selected))
    if selected_count == 0:
        return {
            "threshold": threshold,
            "selected_opportunities": 0,
            "selected_share_pct": 0.0,
            "win_rate_pct": np.nan,
            "expectancy_r": np.nan,
            "avg_win_prob": np.nan,
            "consensus_win_rate_pct": np.nan,
        }

    consensus_win_rate = float(selected["consensus_win"].mean() * 100.0)
    expectancy_r = float(selected["mean_net_r"].mean())
    avg_win_prob = float(selected["predicted_win_prob"].mean())
    return {
        "threshold": round(float(threshold), 4),
        "selected_opportunities": selected_count,
        "selected_share_pct": round((selected_count / max(1, len(df))) * 100.0, 4),
        "win_rate_pct": round(consensus_win_rate, 4),
        "expectancy_r": round(expectancy_r, 6),
        "avg_win_prob": round(avg_win_prob, 6),
        "consensus_win_rate_pct": round(consensus_win_rate, 4),
    }


def compute_model_metrics(y_true: pd.Series, probs: np.ndarray) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if len(set(y_true.tolist())) >= 2:
        metrics["roc_auc"] = round(float(roc_auc_score(y_true, probs)), 6)
        metrics["log_loss"] = round(float(log_loss(y_true, probs, labels=[0, 1])), 6)
        metrics["brier_score"] = round(float(brier_score_loss(y_true, probs)), 6)
    else:
        metrics["roc_auc"] = np.nan
        metrics["log_loss"] = np.nan
        metrics["brier_score"] = round(
            float(np.mean((np.asarray(y_true, dtype=float) - np.asarray(probs, dtype=float)) ** 2)),
            6,
        )
    return metrics


def choose_threshold(
    threshold_df: pd.DataFrame,
    min_samples: int,
    target_win_rate: float = TARGET_WIN_RATE,
) -> tuple[float, str]:
    eligible = threshold_df[
        (pd.to_numeric(threshold_df["selected_opportunities"], errors="coerce").fillna(0) >= int(min_samples))
        & (pd.to_numeric(threshold_df["win_rate_pct"], errors="coerce").fillna(-np.inf) >= (target_win_rate * 100.0))
        & (pd.to_numeric(threshold_df["expectancy_r"], errors="coerce").fillna(-np.inf) > 0.0)
    ].copy()
    if not eligible.empty:
        ranked = eligible.sort_values(
            ["selected_opportunities", "expectancy_r", "threshold"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        return float(ranked.iloc[0]["threshold"]), "target_achieved"

    fallback = threshold_df[
        pd.to_numeric(threshold_df["selected_opportunities"], errors="coerce").fillna(0) >= int(min_samples)
    ].copy()
    if fallback.empty:
        return DEFAULT_THRESHOLDS[-1], "no_threshold_with_min_samples"

    fallback = fallback.sort_values(
        ["win_rate_pct", "expectancy_r", "selected_opportunities", "threshold"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    return float(fallback.iloc[0]["threshold"]), "best_available"


def fit_split_model(
    train_df: pd.DataFrame,
    validate_df: pd.DataFrame,
    test_df: pd.DataFrame,
    excluded_features: set[str],
    min_threshold_samples: int,
    max_categorical_cardinality: int,
    max_categorical_ratio: float,
) -> SplitRunArtifacts:
    categorical_cols, numeric_cols, bool_cols, skipped_features = build_feature_sets(
        train_df,
        excluded_features=excluded_features,
        max_categorical_cardinality=max_categorical_cardinality,
        max_categorical_ratio=max_categorical_ratio,
    )
    feature_cols = categorical_cols + numeric_cols + bool_cols
    if not feature_cols:
        raise RuntimeError("No eligible model features remained after feature filtering.")

    model = build_model(categorical_cols, numeric_cols, bool_cols)
    train_model_df = prepare_model_frame(train_df, bool_cols)
    validate_model_df = prepare_model_frame(validate_df, bool_cols)
    test_model_df = prepare_model_frame(test_df, bool_cols)

    model.fit(train_model_df[feature_cols], train_df["consensus_win"].astype(int))
    for raw_frame, model_frame in (
        (train_df, train_model_df),
        (validate_df, validate_model_df),
        (test_df, test_model_df),
    ):
        probs = model.predict_proba(model_frame[feature_cols])[:, 1]
        raw_frame["predicted_win_prob"] = probs

    threshold_df = pd.DataFrame([evaluate_predictions(validate_df, threshold) for threshold in DEFAULT_THRESHOLDS])
    selected_threshold, threshold_selection_reason = choose_threshold(
        threshold_df,
        min_samples=int(min_threshold_samples),
    )

    split_metrics = {
        "train": {
            "rows": int(len(train_df)),
            **compute_model_metrics(train_df["consensus_win"].astype(int), train_df["predicted_win_prob"].to_numpy()),
        },
        "validate": {
            "rows": int(len(validate_df)),
            **compute_model_metrics(validate_df["consensus_win"].astype(int), validate_df["predicted_win_prob"].to_numpy()),
        },
        "test": {
            "rows": int(len(test_df)),
            **compute_model_metrics(test_df["consensus_win"].astype(int), test_df["predicted_win_prob"].to_numpy()),
        },
    }

    return SplitRunArtifacts(
        feature_cols=feature_cols,
        categorical_cols=categorical_cols,
        numeric_cols=numeric_cols,
        bool_cols=bool_cols,
        skipped_features=skipped_features,
        model=model,
        threshold_df=threshold_df,
        selected_threshold=selected_threshold,
        threshold_selection_reason=threshold_selection_reason,
        train_eval=evaluate_predictions(train_df, selected_threshold),
        validate_eval=evaluate_predictions(validate_df, selected_threshold),
        test_eval=evaluate_predictions(test_df, selected_threshold),
        split_metrics=split_metrics,
        train_df=train_df,
        validate_df=validate_df,
        test_df=test_df,
    )


def build_walkforward_splits(opportunities_df: pd.DataFrame, fold_count: int) -> list[tuple[int, DatasetSplit]]:
    if int(fold_count) <= 0:
        return []
    unique_market = (
        opportunities_df[["market_opportunity_id", "signal_time"]]
        .drop_duplicates()
        .sort_values(["signal_time", "market_opportunity_id"])
        .reset_index(drop=True)
    )
    bucket_count = int(fold_count) + 2
    if len(unique_market) < bucket_count:
        return []

    buckets = [bucket for bucket in np.array_split(unique_market.index.to_numpy(), bucket_count) if len(bucket) > 0]
    if len(buckets) < bucket_count:
        return []

    splits: list[tuple[int, DatasetSplit]] = []
    for fold_idx in range(int(fold_count)):
        train_idx = np.concatenate(buckets[: fold_idx + 1])
        validate_idx = buckets[fold_idx + 1]
        test_idx = buckets[fold_idx + 2]
        if len(train_idx) == 0 or len(validate_idx) == 0 or len(test_idx) == 0:
            continue
        splits.append(
            (
                fold_idx + 1,
                DatasetSplit(
                    train_ids=set(unique_market.iloc[train_idx]["market_opportunity_id"].tolist()),
                    validate_ids=set(unique_market.iloc[validate_idx]["market_opportunity_id"].tolist()),
                    test_ids=set(unique_market.iloc[test_idx]["market_opportunity_id"].tolist()),
                ),
            )
        )
    return splits


def run_walkforward_analysis(
    opportunities_df: pd.DataFrame,
    excluded_features: set[str],
    min_threshold_samples: int,
    walkforward_folds: int,
    max_categorical_cardinality: int,
    max_categorical_ratio: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict[str, Any]] = []
    threshold_rows: list[dict[str, Any]] = []

    for fold_number, split in build_walkforward_splits(opportunities_df, walkforward_folds):
        train_df = opportunities_df[opportunities_df["market_opportunity_id"].isin(split.train_ids)].copy()
        validate_df = opportunities_df[opportunities_df["market_opportunity_id"].isin(split.validate_ids)].copy()
        test_df = opportunities_df[opportunities_df["market_opportunity_id"].isin(split.test_ids)].copy()
        if train_df.empty or validate_df.empty or test_df.empty:
            continue

        artifacts = fit_split_model(
            train_df=train_df,
            validate_df=validate_df,
            test_df=test_df,
            excluded_features=excluded_features,
            min_threshold_samples=min_threshold_samples,
            max_categorical_cardinality=max_categorical_cardinality,
            max_categorical_ratio=max_categorical_ratio,
        )

        for _, threshold_row in artifacts.threshold_df.iterrows():
            threshold_payload = threshold_row.to_dict()
            threshold_payload["fold"] = fold_number
            threshold_rows.append(threshold_payload)

        fold_rows.append(
            {
                "fold": fold_number,
                "train_rows": int(len(train_df)),
                "validate_rows": int(len(validate_df)),
                "test_rows": int(len(test_df)),
                "feature_count": int(len(artifacts.feature_cols)),
                "categorical_feature_count": int(len(artifacts.categorical_cols)),
                "numeric_feature_count": int(len(artifacts.numeric_cols)),
                "boolean_feature_count": int(len(artifacts.bool_cols)),
                "skipped_feature_count": int(len(artifacts.skipped_features)),
                "selected_threshold": round(float(artifacts.selected_threshold), 4),
                "threshold_selection_reason": artifacts.threshold_selection_reason,
                "validate_selected_opportunities": int(artifacts.validate_eval["selected_opportunities"]),
                "validate_win_rate_pct": artifacts.validate_eval["win_rate_pct"],
                "validate_expectancy_r": artifacts.validate_eval["expectancy_r"],
                "test_selected_opportunities": int(artifacts.test_eval["selected_opportunities"]),
                "test_win_rate_pct": artifacts.test_eval["win_rate_pct"],
                "test_expectancy_r": artifacts.test_eval["expectancy_r"],
                "test_roc_auc": artifacts.split_metrics["test"]["roc_auc"],
                "test_log_loss": artifacts.split_metrics["test"]["log_loss"],
                "test_brier_score": artifacts.split_metrics["test"]["brier_score"],
            }
        )

    return pd.DataFrame(fold_rows), pd.DataFrame(threshold_rows)


def build_feature_importance(model: Pipeline) -> pd.DataFrame:
    preprocessor: ColumnTransformer = model.named_steps["preprocessor"]
    classifier: LogisticRegression = model.named_steps["classifier"]
    feature_names = preprocessor.get_feature_names_out()
    coefs = classifier.coef_[0]
    frame = pd.DataFrame(
        {
            "feature": feature_names,
            "coefficient": coefs,
            "abs_coefficient": np.abs(coefs),
            "direction": np.where(coefs >= 0, "supports_win", "supports_loss"),
        }
    )
    return frame.sort_values(["abs_coefficient", "coefficient"], ascending=[False, False]).reset_index(drop=True)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def main() -> None:
    args = parse_args()
    run_dirs = [Path(item).resolve() for item in args.run_dirs]
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_dir.mkdir(parents=True, exist_ok=True)

    trade_frames = [normalize_trade_frame(load_trade_rows(run_dir)) for run_dir in run_dirs]
    trades_df = pd.concat([frame for frame in trade_frames if not frame.empty], ignore_index=True, sort=False)
    if trades_df.empty:
        raise RuntimeError("No entered trade rows were available in the supplied run directories.")
    if args.direction:
        trades_df = trades_df[trades_df["direction"].astype(str).str.upper() == str(args.direction).upper()].copy()
    if trades_df.empty:
        raise RuntimeError("No entered trade rows were available after applying the direction filter.")

    opportunity_df = build_opportunity_dataset(trades_df, int(args.min_opportunity_variants))
    if opportunity_df.empty:
        raise RuntimeError("No research opportunities were available after applying the minimum-variant filter.")

    split = split_market_opportunities(opportunity_df, float(args.train_frac), float(args.validate_frac))
    opportunity_df["split"] = np.where(
        opportunity_df["market_opportunity_id"].isin(split.train_ids),
        "train",
        np.where(
            opportunity_df["market_opportunity_id"].isin(split.validate_ids),
            "validate",
            "test",
        ),
    )

    excluded_features = set(args.exclude_features or [])
    train_df = opportunity_df[opportunity_df["split"] == "train"].copy()
    validate_df = opportunity_df[opportunity_df["split"] == "validate"].copy()
    test_df = opportunity_df[opportunity_df["split"] == "test"].copy()
    if train_df.empty or validate_df.empty or test_df.empty:
        raise RuntimeError("Chronological split left one of train/validate/test empty. Add more data or adjust split fractions.")

    artifacts = fit_split_model(
        train_df=train_df,
        validate_df=validate_df,
        test_df=test_df,
        excluded_features=excluded_features,
        min_threshold_samples=int(args.min_threshold_samples),
        max_categorical_cardinality=int(args.max_categorical_cardinality),
        max_categorical_ratio=float(args.max_categorical_ratio),
    )
    opportunity_model_df = prepare_model_frame(opportunity_df.copy(), artifacts.bool_cols)
    opportunity_df["predicted_win_prob"] = artifacts.model.predict_proba(opportunity_model_df[artifacts.feature_cols])[:, 1]

    threshold_df = artifacts.threshold_df.copy()
    selected_threshold = artifacts.selected_threshold
    threshold_selection_reason = artifacts.threshold_selection_reason
    train_eval = artifacts.train_eval
    validate_eval = artifacts.validate_eval
    test_eval = artifacts.test_eval
    full_eval = evaluate_predictions(opportunity_df, selected_threshold)
    split_metrics = artifacts.split_metrics

    feature_importance_df = build_feature_importance(artifacts.model)
    skipped_features_df = pd.DataFrame(artifacts.skipped_features)
    selected_test_df = artifacts.test_df[artifacts.test_df["predicted_win_prob"] >= selected_threshold].copy()
    selected_full_df = opportunity_df[opportunity_df["predicted_win_prob"] >= selected_threshold].copy()
    walkforward_summary_df, walkforward_threshold_df = run_walkforward_analysis(
        opportunities_df=opportunity_df.copy(),
        excluded_features=excluded_features,
        min_threshold_samples=int(args.min_threshold_samples),
        walkforward_folds=int(args.walkforward_folds),
        max_categorical_cardinality=int(args.max_categorical_cardinality),
        max_categorical_ratio=float(args.max_categorical_ratio),
    )

    threshold_df.to_csv(output_dir / "threshold_summary_validate.csv", index=False)
    selected_test_df.to_csv(output_dir / "selected_test_opportunities.csv", index=False)
    selected_full_df.to_csv(output_dir / "selected_all_opportunities.csv", index=False)
    feature_importance_df.to_csv(output_dir / "feature_importance.csv", index=False)
    opportunity_df.to_csv(output_dir / "opportunity_dataset.csv", index=False)
    if not skipped_features_df.empty:
        skipped_features_df.to_csv(output_dir / "skipped_features.csv", index=False)
    if not walkforward_summary_df.empty:
        walkforward_summary_df.to_csv(output_dir / "walkforward_summary.csv", index=False)
    if not walkforward_threshold_df.empty:
        walkforward_threshold_df.to_csv(output_dir / "walkforward_thresholds.csv", index=False)

    if not args.skip_model_save:
        joblib.dump(artifacts.model, output_dir / "probability_filter_model.joblib")

    walkforward_overview = {
        "requested_folds": int(args.walkforward_folds),
        "completed_folds": int(len(walkforward_summary_df)),
    }
    if not walkforward_summary_df.empty:
        walkforward_overview.update(
            {
                "mean_test_win_rate_pct": round(float(pd.to_numeric(walkforward_summary_df["test_win_rate_pct"], errors="coerce").mean()), 4),
                "median_test_win_rate_pct": round(float(pd.to_numeric(walkforward_summary_df["test_win_rate_pct"], errors="coerce").median()), 4),
                "mean_test_expectancy_r": round(float(pd.to_numeric(walkforward_summary_df["test_expectancy_r"], errors="coerce").mean()), 6),
                "mean_test_roc_auc": round(float(pd.to_numeric(walkforward_summary_df["test_roc_auc"], errors="coerce").mean()), 6),
                "folds_meeting_target": int(
                    (
                        (pd.to_numeric(walkforward_summary_df["test_win_rate_pct"], errors="coerce") >= TARGET_WIN_RATE * 100.0)
                        & (pd.to_numeric(walkforward_summary_df["test_expectancy_r"], errors="coerce") > 0.0)
                    ).sum()
                ),
                "total_selected_test_opportunities": int(
                    pd.to_numeric(walkforward_summary_df["test_selected_opportunities"], errors="coerce").fillna(0).sum()
                ),
            }
        )

    summary = {
        "output_dir": str(output_dir),
        "run_dirs": [str(path) for path in run_dirs],
        "source_runs": [path.name for path in run_dirs],
        "input_trade_rows": int(len(trades_df)),
        "opportunity_rows": int(len(opportunity_df)),
        "direction_filter": args.direction,
        "excluded_features": sorted(excluded_features),
        "auto_skipped_features": skipped_features_df.to_dict(orient="records") if not skipped_features_df.empty else [],
        "feature_counts": {
            "categorical": len(artifacts.categorical_cols),
            "numeric": len(artifacts.numeric_cols),
            "boolean": len(artifacts.bool_cols),
            "total": len(artifacts.feature_cols),
        },
        "split_rows": {
            "train": int(len(train_df)),
            "validate": int(len(validate_df)),
            "test": int(len(test_df)),
        },
        "target_definition": {
            "consensus_win": "win_share >= 0.60 and mean_net_r > 0",
            "min_opportunity_variants": int(args.min_opportunity_variants),
        },
        "selected_threshold": round(float(selected_threshold), 4),
        "threshold_selection_reason": threshold_selection_reason,
        "target_win_rate_pct": TARGET_WIN_RATE * 100.0,
        "train_threshold_eval": train_eval,
        "validate_threshold_eval": validate_eval,
        "test_threshold_eval": test_eval,
        "full_threshold_eval": full_eval,
        "split_metrics": split_metrics,
        "walkforward_overview": walkforward_overview,
        "top_supports_win": feature_importance_df.head(20).to_dict(orient="records"),
        "top_supports_loss": feature_importance_df.sort_values(["coefficient"], ascending=[True]).head(20).to_dict(orient="records"),
    }
    save_json(output_dir / "summary.json", summary)

    print(f"Probability filter research complete: {output_dir}")
    print(f"Opportunities: {len(opportunity_df)} | threshold={selected_threshold:.2f} ({threshold_selection_reason})")
    print(
        "Validate @ threshold -> "
        f"selected={int(summary['validate_threshold_eval']['selected_opportunities'])}, "
        f"win_rate={summary['validate_threshold_eval']['win_rate_pct']}, "
        f"expectancy={summary['validate_threshold_eval']['expectancy_r']}"
    )
    print(
        "Test @ threshold -> "
        f"selected={int(summary['test_threshold_eval']['selected_opportunities'])}, "
        f"win_rate={summary['test_threshold_eval']['win_rate_pct']}, "
        f"expectancy={summary['test_threshold_eval']['expectancy_r']}"
    )


if __name__ == "__main__":
    main()
