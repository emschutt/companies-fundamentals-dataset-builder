#!/usr/bin/env python3
"""Audit timing leakage risks in the local DoltHub S&P 500 dataset."""

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
    parser.add_argument(
        "--panel-file",
        type=Path,
        help="Panel file to audit. Defaults to the enriched panel under --data-dir.",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("outputs/leakage_audit_sp500"))
    parser.add_argument("--quarter-lag-days", type=int, default=45)
    parser.add_argument("--annual-lag-days", type=int, default=90)
    parser.add_argument("--sample-rows", type=int, default=200)
    return parser.parse_args()


def as_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.tz_localize(None).dt.normalize()


def pct(numer: int | float, denom: int | float) -> float:
    return float(numer / denom) if denom else 0.0


def quantiles(series: pd.Series) -> dict[str, float | None]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {str(q): None for q in (0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1)}
    return {
        str(q): float(clean.quantile(q))
        for q in (0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1)
    }


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def audit_fundamental_raw_tables(data_dir: Path, quarter_lag_days: int, annual_lag_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    samples: list[pd.DataFrame] = []
    for table in FUNDAMENTAL_TABLES:
        path = data_dir / "tables" / f"{table}.parquet"
        if not path.exists():
            rows.append({"check": "fundamental_raw_table", "table": table, "status": "missing"})
            continue

        df = pd.read_parquet(path)
        if df.empty or "date" not in df.columns:
            rows.append({"check": "fundamental_raw_table", "table": table, "status": "empty_or_no_date"})
            continue

        df = df.copy()
        df["date"] = as_date(df["date"])
        df["period"] = df.get("period", pd.Series(index=df.index, dtype="object")).astype(str)
        df["conservative_lag_days"] = df["period"].str.lower().map(
            lambda period: annual_lag_days if "year" in period else quarter_lag_days
        )
        df["conservative_available_date"] = df["date"] + pd.to_timedelta(df["conservative_lag_days"], unit="D")

        quarter_rows = int(df["period"].str.lower().str.contains("quarter", na=False).sum())
        annual_rows = int(df["period"].str.lower().str.contains("year", na=False).sum())
        rows.append(
            {
                "check": "fundamental_raw_table",
                "table": table,
                "status": "high_risk",
                "rows": int(len(df)),
                "symbols": int(df["act_symbol"].nunique()) if "act_symbol" in df.columns else None,
                "quarter_rows": quarter_rows,
                "annual_rows": annual_rows,
                "min_period_date": df["date"].min().date().isoformat(),
                "max_period_date": df["date"].max().date().isoformat(),
                "problem": "table date is used as as-of date by the builder but appears to be fiscal period/end date",
                "recommended_fix": f"use filing/accepted date, or lag quarter rows by {quarter_lag_days} days and annual rows by {annual_lag_days} days",
            }
        )

        sample_cols = [col for col in ["act_symbol", "date", "period", "conservative_available_date"] if col in df.columns]
        sample = df[sample_cols].sort_values(["date", "act_symbol"]).head(40).copy()
        sample.insert(0, "table", table)
        samples.append(sample)

    sample_df = pd.concat(samples, ignore_index=True) if samples else pd.DataFrame()
    return pd.DataFrame(rows), sample_df


def audit_panel_timing(data_dir: Path, panel_file: Path | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel_path = panel_file or data_dir / "fundamentals_estimates_prices_panel.parquet"
    if not panel_path.exists():
        panel_path = data_dir / "fundamentals_estimates_panel.parquet"
    if not panel_path.exists():
        return pd.DataFrame([{"check": "panel", "status": "missing"}]), pd.DataFrame()

    df = pd.read_parquet(panel_path)
    df = df.copy()
    df["date"] = as_date(df["date"])
    rows: list[dict[str, object]] = [
        {
            "check": "panel_shape",
            "status": "info",
            "path": str(panel_path),
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "symbols": int(df["act_symbol"].nunique()) if "act_symbol" in df.columns else None,
            "min_date": df["date"].min().date().isoformat(),
            "max_date": df["date"].max().date().isoformat(),
        }
    ]

    samples: list[pd.DataFrame] = []
    if "period_end_date" in df.columns:
        df["period_end_date"] = as_date(df["period_end_date"])
        gap = (df["date"] - df["period_end_date"]).dt.days
        usable = gap.notna()
        suspicious = usable & (gap < 45)
        rows.append(
            {
                "check": "eps_history_period_end_gap",
                "status": "high_risk" if suspicious.any() else "ok",
                "rows_with_period_end": int(usable.sum()),
                "rows_less_than_45_days_after_period_end": int(suspicious.sum()),
                "share_less_than_45_days_after_period_end": pct(int(suspicious.sum()), int(usable.sum())),
                "gap_quantiles_json": json.dumps(quantiles(gap)),
                "problem": "EPS history source is merged on period_end_date; reported EPS can appear before its earnings release if no filing/release date is used.",
            }
        )
        sample = df.loc[suspicious, ["act_symbol", "date", "period_end_date"]].copy()
        if not sample.empty:
            sample["days_after_period_end"] = (sample["date"] - sample["period_end_date"]).dt.days
            sample.insert(0, "issue", "eps_history_available_too_soon")
            samples.append(sample.sort_values(["days_after_period_end", "date"]).head(100))

    for prefix in ("eps_est", "sales_est"):
        date_cols = [col for col in df.columns if col.startswith(prefix) and col.endswith("_period_end_date")]
        for col in date_cols:
            period_label = col.removeprefix(prefix + "_").removesuffix("_period_end_date")
            pdate = as_date(df[col])
            gap = (pdate - df["date"]).dt.days
            usable = gap.notna()
            already_ended = usable & (gap < 0)
            ends_soon = usable & (gap.between(0, 7))
            status = "high_risk" if already_ended.any() else ("review" if ends_soon.any() else "ok")
            rows.append(
                {
                    "check": "estimate_target_period_gap",
                    "status": status,
                    "feature_family": prefix,
                    "period_label": period_label,
                    "rows_with_period_end": int(usable.sum()),
                    "rows_after_estimated_period_end": int(already_ended.sum()),
                    "share_after_estimated_period_end": pct(int(already_ended.sum()), int(usable.sum())),
                    "rows_0_to_7_days_before_period_end": int(ends_soon.sum()),
                    "gap_quantiles_json": json.dumps(quantiles(gap)),
                    "problem": "negative gaps may be stale or post-period estimates; use earnings-release filters before treating them as live expectations.",
                }
            )

    if "next_earnings_date" in df.columns:
        next_date = as_date(df["next_earnings_date"])
        gap = (next_date - df["date"]).dt.days
        usable = gap.notna()
        same_day = usable & (gap == 0)
        negative = usable & (gap < 0)
        rows.append(
            {
                "check": "next_earnings_alignment",
                "status": "review" if same_day.any() or negative.any() else "ok",
                "rows_with_next_earnings": int(usable.sum()),
                "same_day_next_earnings_rows": int(same_day.sum()),
                "negative_gap_rows": int(negative.sum()),
                "gap_quantiles_json": json.dumps(quantiles(gap)),
                "problem": "same-day earnings features are only safe if the trading decision occurs after the announcement timestamp.",
            }
        )

    report_date_cols = [
        col
        for col in df.columns
        if ("report_date" in col.lower() or "update_time" in col.lower())
    ]
    for col in report_date_cols:
        rdate = as_date(df[col])
        gap = (df["date"] - rdate).dt.days
        usable = gap.notna()
        future = usable & (gap < 0)
        rows.append(
            {
                "check": "explicit_report_or_update_date",
                "status": "high_risk" if future.any() else "ok",
                "column": col,
                "rows_with_date": int(usable.sum()),
                "rows_where_report_date_after_panel_date": int(future.sum()),
                "share_future_report_date": pct(int(future.sum()), int(usable.sum())),
                "gap_quantiles_json": json.dumps(quantiles(gap)),
            }
        )
        if future.any():
            sample = df.loc[future, ["act_symbol", "date", col]].copy()
            sample["days_until_report_or_update"] = (rdate.loc[future] - df.loc[future, "date"]).dt.days
            sample.insert(0, "issue", f"future_{col}")
            samples.append(sample.sort_values(["days_until_report_or_update", "date"]).head(100))

    dangerous_tokens = ("target", "future", "forward", "actual_future", "next_period_end_price")
    dangerous_cols = [
        col
        for col in df.columns
        if any(token in col.lower() for token in dangerous_tokens)
    ]
    rows.append(
        {
            "check": "dangerous_feature_names",
            "status": "high_risk" if dangerous_cols else "ok",
            "count": len(dangerous_cols),
            "columns": ", ".join(dangerous_cols[:80]),
            "problem": "these columns are targets or future-looking metadata and must be excluded from model features.",
        }
    )

    sample_df = pd.concat(samples, ignore_index=True) if samples else pd.DataFrame()
    return pd.DataFrame(rows), sample_df


def audit_membership(data_dir: Path, panel_file: Path | None = None) -> pd.DataFrame:
    panel_path = panel_file or data_dir / "fundamentals_estimates_prices_panel.parquet"
    intervals_path = data_dir / "sp500_selected_intervals.parquet"
    if not panel_path.exists() or not intervals_path.exists():
        return pd.DataFrame([{"check": "membership_filter", "status": "missing_input"}])

    panel = pd.read_parquet(panel_path, columns=["act_symbol", "date"]).reset_index(drop=True)
    panel["_row_id"] = panel.index
    intervals = pd.read_parquet(intervals_path)
    panel["date"] = as_date(panel["date"])
    for col in ("effective_start", "effective_end"):
        intervals[col] = as_date(intervals[col])

    merged = panel.merge(
        intervals[["act_symbol", "effective_start", "effective_end"]],
        on="act_symbol",
        how="left",
    )
    inside = (merged["date"] >= merged["effective_start"]) & (merged["date"] <= merged["effective_end"])
    by_row = inside.groupby(merged["_row_id"]).any()
    violations = int((~by_row.reindex(panel["_row_id"], fill_value=False)).sum())
    return pd.DataFrame(
        [
            {
                "check": "membership_filter",
                "status": "high_risk" if violations else "ok",
                "panel_rows": int(len(panel)),
                "rows_outside_membership_interval": violations,
                "share_outside_membership_interval": pct(violations, len(panel)),
            }
        ]
    )


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    fundamental_summary, fundamental_samples = audit_fundamental_raw_tables(
        args.data_dir,
        args.quarter_lag_days,
        args.annual_lag_days,
    )
    panel_summary, panel_samples = audit_panel_timing(args.data_dir, args.panel_file)
    membership_summary = audit_membership(args.data_dir, args.panel_file)

    summary = pd.concat([fundamental_summary, panel_summary, membership_summary], ignore_index=True)
    write_csv(summary, args.out_dir / "leakage_audit_summary.csv")
    if not fundamental_samples.empty:
        write_csv(fundamental_samples.head(args.sample_rows), args.out_dir / "fundamental_period_date_samples.csv")
    if not panel_samples.empty:
        write_csv(panel_samples.head(args.sample_rows), args.out_dir / "panel_high_risk_samples.csv")

    panel_high_risk = summary[
        summary["check"].ne("fundamental_raw_table")
        & summary["status"].eq("high_risk")
    ]
    status = "high_risk" if not panel_high_risk.empty else "panel_ok_source_limitations_remain"
    main_findings = [
        "DoltHub statement source tables use period/end dates and do not expose filing/accepted timestamps.",
    ]
    if status == "high_risk":
        main_findings.extend(
            [
                "One or more panel-level checks still have high-risk leakage flags.",
                "Review the summary CSV before using this panel in a backtest.",
            ]
        )
    else:
        main_findings.extend(
            [
                "No panel-level high-risk flags remain in this audit.",
                "Any remaining review flags are timing assumptions to inspect, not direct future-dated rows.",
                "Membership filtering passes if rows_outside_membership_interval is zero in the summary.",
            ]
        )

    verdict = {
        "status": status,
        "data_dir": str(args.data_dir),
        "summary": str(args.out_dir / "leakage_audit_summary.csv"),
        "fundamental_samples": str(args.out_dir / "fundamental_period_date_samples.csv"),
        "panel_samples": str(args.out_dir / "panel_high_risk_samples.csv"),
        "main_findings": main_findings,
    }
    (args.out_dir / "verdict.json").write_text(json.dumps(verdict, indent=2), encoding="utf-8")

    print(json.dumps(verdict, indent=2))
    print("\nTop summary rows:")
    display_cols = [col for col in ["check", "table", "status", "rows", "problem"] if col in summary.columns]
    print(summary[display_cols].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
