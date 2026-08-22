#!/usr/bin/env python3
"""Build a conservative point-in-time S&P 500 panel from local DoltHub tables.

The DoltHub statement tables contain fiscal period dates but not filing or
accepted timestamps. This script therefore applies explicit availability lags
before joining fundamentals into the modeling panel.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


FUNDAMENTAL_TABLES = (
    "income_statement",
    "balance_sheet_assets",
    "balance_sheet_equity",
    "balance_sheet_liabilities",
    "cash_flow_statement",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--quarter-lag-days", type=int, default=45)
    parser.add_argument("--annual-lag-days", type=int, default=90)
    parser.add_argument(
        "--output-name",
        default="fundamentals_estimates_prices_panel_pit.parquet",
        help="Output file name under --data-dir.",
    )
    return parser.parse_args()


def normalize_date(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, errors="coerce")
        .dt.tz_localize(None)
        .dt.normalize()
        .astype("datetime64[ns]")
    )


def load_table(data_dir: Path, name: str) -> pd.DataFrame:
    path = data_dir / "tables" / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "act_symbol" in df.columns:
        df["act_symbol"] = df["act_symbol"].astype(str).str.upper()
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df[col] = normalize_date(df[col])
    return df


def add_statement_asof(df: pd.DataFrame, quarter_lag_days: int, annual_lag_days: int) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    period = out.get("period", pd.Series(index=out.index, dtype="object")).astype(str).str.lower()
    lag_days = period.map(lambda value: annual_lag_days if "year" in value else quarter_lag_days)
    out["source_period_end_date"] = out["date"]
    out["known_asof_date"] = out["source_period_end_date"] + pd.to_timedelta(lag_days, unit="D")
    out = out.dropna(subset=["act_symbol", "known_asof_date"])
    return out.drop(columns=["date"])


def filter_estimates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = normalize_date(out["date"])
    out["period_end_date"] = normalize_date(out["period_end_date"])
    before_period_end = out["period_end_date"].gt(out["date"])
    return out.loc[before_period_end].copy()


def pivot_estimates(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return df
    id_cols = ["act_symbol", "date"]
    value_cols = [
        col
        for col in df.columns
        if col not in {"act_symbol", "date", "period", "period_end_date"}
    ]
    wide = df.pivot_table(index=id_cols, columns="period", values=value_cols, aggfunc="last")
    wide.columns = [
        f"{prefix}_{str(period).lower().replace(' ', '_')}_{metric}"
        for metric, period in wide.columns
    ]
    wide = wide.reset_index()
    period_dates = df.pivot_table(
        index=id_cols,
        columns="period",
        values="period_end_date",
        aggfunc="last",
    )
    period_dates.columns = [
        f"{prefix}_{str(period).lower().replace(' ', '_')}_period_end_date"
        for period in period_dates.columns
    ]
    return wide.merge(period_dates.reset_index(), on=id_cols, how="left")


def prefix_non_keys(df: pd.DataFrame, prefix: str, keys: set[str]) -> pd.DataFrame:
    return df.rename(columns={col: f"{prefix}_{col}" for col in df.columns if col not in keys})


def asof_by_symbol(
    base: pd.DataFrame,
    features: pd.DataFrame,
    right_date_col: str,
    prefix: str,
    keep_cols: set[str] | None = None,
) -> pd.DataFrame:
    if base.empty or features.empty:
        return base
    keys = {"act_symbol", right_date_col}
    if keep_cols:
        keys |= keep_cols
    right = prefix_non_keys(features, prefix, keys)
    outputs: list[pd.DataFrame] = []
    for symbol, left_part in base.groupby("act_symbol", sort=False):
        right_part = right[right["act_symbol"].eq(symbol)].sort_values(right_date_col)
        if right_part.empty:
            outputs.append(left_part)
            continue
        joined = pd.merge_asof(
            left_part.sort_values("date"),
            right_part,
            left_on="date",
            right_on=right_date_col,
            by="act_symbol",
            direction="backward",
        )
        if right_date_col != "date" and right_date_col in joined.columns:
            joined = joined.drop(columns=[right_date_col])
        outputs.append(joined)
    return pd.concat(outputs, ignore_index=True)


def asof_join_prices(panel: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    if panel.empty or prices.empty:
        return panel
    out: list[pd.DataFrame] = []
    prices = prices.copy()
    prices["date"] = normalize_date(prices["date"])
    for symbol, left_part in panel.groupby("act_symbol", sort=False):
        right_part = prices[prices["act_symbol"].eq(symbol)].sort_values("date")
        if right_part.empty:
            out.append(left_part)
            continue
        out.append(
            pd.merge_asof(
                left_part.sort_values("date"),
                right_part.sort_values("date"),
                on="date",
                by="act_symbol",
                direction="backward",
            )
        )
    return pd.concat(out, ignore_index=True)


def build_panel(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, object]]:
    data_dir = args.data_dir
    eps_est_raw = filter_estimates(load_table(data_dir, "eps_estimate"))
    sales_est_raw = filter_estimates(load_table(data_dir, "sales_estimate"))
    rank_score = load_table(data_dir, "rank_score")

    eps_est = pivot_estimates(eps_est_raw, "eps_est")
    sales_est = pivot_estimates(sales_est_raw, "sales_est")

    base_parts = [
        frame[["act_symbol", "date"]]
        for frame in (eps_est, sales_est, rank_score)
        if not frame.empty and {"act_symbol", "date"}.issubset(frame.columns)
    ]
    if not base_parts:
        raise RuntimeError("No estimate or rank-score dates are available to anchor the panel.")

    panel = (
        pd.concat(base_parts, ignore_index=True)
        .drop_duplicates()
        .sort_values(["act_symbol", "date"])
        .reset_index(drop=True)
    )
    panel["date"] = normalize_date(panel["date"])

    if not eps_est.empty:
        panel = panel.merge(eps_est, on=["act_symbol", "date"], how="left")
    if not sales_est.empty:
        panel = panel.merge(sales_est, on=["act_symbol", "date"], how="left")
    if not rank_score.empty:
        panel = panel.merge(rank_score, on=["act_symbol", "date"], how="left")

    for name in FUNDAMENTAL_TABLES:
        statement = add_statement_asof(load_table(data_dir, name), args.quarter_lag_days, args.annual_lag_days)
        panel = asof_by_symbol(panel, statement, "known_asof_date", name)

    eps_history = load_table(data_dir, "eps_history")
    if not eps_history.empty:
        eps_history = eps_history.copy()
        eps_history["source_period_end_date"] = normalize_date(eps_history["period_end_date"])
        eps_history["known_asof_date"] = eps_history["source_period_end_date"] + pd.Timedelta(days=args.quarter_lag_days)
        eps_history = eps_history.drop(columns=["period_end_date"])
        panel = asof_by_symbol(panel, eps_history, "known_asof_date", "eps_hist")

    yfinance = load_table(data_dir, "yfinance_ohlcv")
    if not yfinance.empty:
        panel = asof_join_prices(panel, yfinance)

    drop_prefixes = ("defeatbeta_", "target_ret_")
    drop_substrings = ("_update_time",)
    drop_cols = [
        col
        for col in panel.columns
        if col.startswith(drop_prefixes) or any(token in col for token in drop_substrings)
    ]
    panel = panel.drop(columns=drop_cols, errors="ignore")

    panel = panel.sort_values(["act_symbol", "date"]).reset_index(drop=True)
    manifest = {
        "created_panel": args.output_name,
        "rows": int(len(panel)),
        "columns": int(len(panel.columns)),
        "symbols": int(panel["act_symbol"].nunique()),
        "from": panel["date"].min().date().isoformat(),
        "to": panel["date"].max().date().isoformat(),
        "quarter_lag_days": args.quarter_lag_days,
        "annual_lag_days": args.annual_lag_days,
        "estimate_rows": {
            "eps_raw_after_filter": int(len(eps_est_raw)),
            "sales_raw_after_filter": int(len(sales_est_raw)),
        },
        "removed_columns": drop_cols,
        "notes": [
            "Statement features are joined only after conservative period-end lags because source filing timestamps are unavailable.",
            "EPS history is joined only after the quarter lag.",
            "Estimate rows are retained only when their target period end is after the estimate date.",
            "DefeatBeta features are excluded from this panel because the audit found future-dated update timestamps.",
        ],
    }
    return panel, manifest


def main() -> int:
    args = parse_args()
    panel, manifest = build_panel(args)
    output_path = args.data_dir / args.output_name
    panel.to_parquet(output_path, index=False)
    manifest_path = args.data_dir / "pit_panel_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output_path), "manifest": str(manifest_path), **manifest}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
