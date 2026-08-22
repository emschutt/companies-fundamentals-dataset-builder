#!/usr/bin/env python3
"""Build a survivorship-aware S&P 500 price/fundamental dataset from DoltHub.

Sources:
- Historical S&P 500 membership: https://github.com/fja05680/sp500
- Prices: post-no-preference/stocks on DoltHub
- Fundamentals and estimates: post-no-preference/earnings on DoltHub

The DoltHub SQL API currently returns at most 1,000 rows per request, so this
script uses keyset pagination and writes resumable per-table Parquet files.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests


DOLTHUB_SQL = "https://www.dolthub.com/api/v1alpha1/{owner}/{database}"
SP500_INTERVALS_URL = (
    "https://raw.githubusercontent.com/fja05680/sp500/master/"
    "sp500_ticker_start_end.csv"
)

ROW_LIMIT = 1000
PRICE_TABLE = "ohlcv"
STOCKS_DB = ("post-no-preference", "stocks")
EARNINGS_DB = ("post-no-preference", "earnings")

FUNDAMENTAL_TABLES = (
    "income_statement",
    "balance_sheet_assets",
    "balance_sheet_equity",
    "balance_sheet_liabilities",
    "cash_flow_statement",
)
ANALYST_TABLES = (
    "eps_estimate",
    "sales_estimate",
    "eps_history",
    "earnings_calendar",
    "rank_score",
)
ALL_TABLES = (PRICE_TABLE,) + FUNDAMENTAL_TABLES + ANALYST_TABLES


@dataclass(frozen=True)
class DoltTable:
    name: str
    owner: str
    database: str
    date_col: str
    order_cols: tuple[str, ...]
    min_date_padding_days: int = 0


TABLES: dict[str, DoltTable] = {
    PRICE_TABLE: DoltTable(PRICE_TABLE, *STOCKS_DB, "date", ("date", "act_symbol")),
    "income_statement": DoltTable(
        "income_statement", *EARNINGS_DB, "date", ("date", "act_symbol", "period"), 730
    ),
    "balance_sheet_assets": DoltTable(
        "balance_sheet_assets", *EARNINGS_DB, "date", ("date", "act_symbol", "period"), 730
    ),
    "balance_sheet_equity": DoltTable(
        "balance_sheet_equity", *EARNINGS_DB, "date", ("date", "act_symbol", "period"), 730
    ),
    "balance_sheet_liabilities": DoltTable(
        "balance_sheet_liabilities", *EARNINGS_DB, "date", ("date", "act_symbol", "period"), 730
    ),
    "cash_flow_statement": DoltTable(
        "cash_flow_statement", *EARNINGS_DB, "date", ("date", "act_symbol", "period"), 730
    ),
    "eps_estimate": DoltTable(
        "eps_estimate", *EARNINGS_DB, "date", ("date", "act_symbol", "period")
    ),
    "sales_estimate": DoltTable(
        "sales_estimate", *EARNINGS_DB, "date", ("date", "act_symbol", "period")
    ),
    "eps_history": DoltTable(
        "eps_history", *EARNINGS_DB, "period_end_date", ("period_end_date", "act_symbol")
    ),
    "earnings_calendar": DoltTable(
        "earnings_calendar", *EARNINGS_DB, "date", ("date", "act_symbol")
    ),
    "rank_score": DoltTable("rank_score", *EARNINGS_DB, "date", ("date", "act_symbol")),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a survivorship-aware S&P 500 dataset from DoltHub."
    )
    parser.add_argument("--out-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Dataset end date, default today.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=list(ALL_TABLES),
        choices=list(ALL_TABLES),
        help="Tables to download.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=40,
        help="Ticker count per DoltHub query chunk.",
    )
    parser.add_argument(
        "--date-window-days",
        type=int,
        default=31,
        help="Calendar-day span for dense DoltHub tables like estimates/rank scores.",
    )
    parser.add_argument(
        "--sparse-date-window-days",
        type=int,
        default=4000,
        help="Calendar-day span for sparse DoltHub tables like fundamentals.",
    )
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument(
        "--min-fundamental-rows",
        type=int,
        default=8,
        help="Minimum combined statement rows required to keep a ticker.",
    )
    parser.add_argument(
        "--min-price-rows",
        type=int,
        default=252,
        help="Minimum in-index price rows required to keep a ticker.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=0,
        help="Optional cap after coverage ranking. 0 means all eligible symbols.",
    )
    parser.add_argument(
        "--random-sample",
        type=int,
        default=0,
        help="Optional random sample after coverage filters. 0 disables sampling.",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse table Parquet files when present.",
    )
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Only build membership and coverage files, not the large tables.",
    )
    parser.add_argument(
        "--skip-coverage-map",
        action="store_true",
        help="Select all historical S&P symbols without precomputing fundamental coverage.",
    )
    parser.add_argument(
        "--price-source",
        choices=("none", "dolthub", "fmp"),
        default="none",
        help="Source for daily OHLCV prices. Use none to build only DoltHub fundamentals/estimates.",
    )
    parser.add_argument(
        "--dolt-table-source",
        choices=("csv", "sql"),
        default="csv",
        help="Use DoltHub CSV exports or SQL pagination for non-price tables.",
    )
    parser.add_argument(
        "--csv-chunksize",
        type=int,
        default=500_000,
        help="Rows per pandas chunk when filtering DoltHub CSV exports locally.",
    )
    parser.add_argument(
        "--fmp-api-key-env",
        default="FMP_API_KEY",
        help="Environment variable containing the FMP API key when --price-source=fmp.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sql_literal(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def dolt_query(session: requests.Session, table: DoltTable, query: str) -> dict[str, Any]:
    url = DOLTHUB_SQL.format(owner=table.owner, database=table.database)
    response = session.get(url, params={"q": query}, timeout=90)
    response.raise_for_status()
    payload = response.json()
    status = payload.get("query_execution_status")
    if status not in {"Success", "RowLimit"}:
        raise RuntimeError(f"DoltHub query failed: {status}: {payload.get('query_execution_message')}")
    return payload


def download_sp500_intervals(out_dir: Path) -> pd.DataFrame:
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / "sp500_ticker_start_end.csv"
    if not path.exists():
        df = pd.read_csv(SP500_INTERVALS_URL)
        df.to_csv(path, index=False)
    else:
        df = pd.read_csv(path)

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    return df.dropna(subset=["ticker", "start_date"]).sort_values(["ticker", "start_date"])


def overlapping_intervals(intervals: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    max_end = pd.Timestamp.max.normalize()
    out = intervals[
        (intervals["start_date"] <= end)
        & (intervals["end_date"].fillna(max_end) >= start)
    ].copy()
    out["effective_start"] = out["start_date"].clip(lower=start)
    out["effective_end"] = out["end_date"].fillna(end).clip(upper=end)
    return out


def in_list(symbols: list[str]) -> str:
    return ", ".join(sql_literal(symbol) for symbol in symbols)


def keyset_condition(order_cols: tuple[str, ...], last: dict[str, Any]) -> str:
    clauses = []
    for i, col in enumerate(order_cols):
        equal_prefix = " AND ".join(f"{p} = {sql_literal(last[p])}" for p in order_cols[:i])
        greater = f"{col} > {sql_literal(last[col])}"
        clauses.append(f"({equal_prefix + ' AND ' if equal_prefix else ''}{greater})")
    return "(" + " OR ".join(clauses) + ")"


def normalize_dolt_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in df.columns:
        if col in {"date", "period_end_date", "last_seen"}:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            continue
        if col == "act_symbol":
            df[col] = df[col].astype(str).str.upper()
            continue
        if col == "period":
            df[col] = df[col].astype(str)
            continue
        if df[col].dtype == object:
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().sum() and numeric.notna().sum() >= df[col].notna().sum() * 0.8:
                df[col] = numeric
    return df


def fetch_group_counts(
    session: requests.Session,
    table: DoltTable,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    chunk_size: int,
    sleep: float,
) -> pd.DataFrame:
    frames = []
    min_date = start - pd.Timedelta(days=table.min_date_padding_days)
    for group in chunks(symbols, chunk_size):
        query = f"""
            SELECT act_symbol, COUNT(*) AS row_count
            FROM {table.name}
            WHERE act_symbol IN ({in_list(group)})
              AND {table.date_col} >= {sql_literal(min_date.date())}
              AND {table.date_col} <= {sql_literal(end.date())}
            GROUP BY act_symbol
            ORDER BY act_symbol
            LIMIT {ROW_LIMIT};
        """
        payload = dolt_query(session, table, query)
        rows = payload.get("rows", [])
        if rows:
            frame = pd.DataFrame(rows)
            frame = frame.rename(columns={"row_count": "rows"})
            frame["rows"] = pd.to_numeric(frame["rows"], errors="coerce").fillna(0).astype(int)
            frames.append(frame)
        time.sleep(sleep)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["act_symbol", "rows"])


def membership_filter(df: pd.DataFrame, intervals: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty:
        return df
    pieces = []
    interval_symbol_col = "ticker" if "ticker" in intervals.columns else "act_symbol"
    interval_groups = {
        ticker: group for ticker, group in intervals.groupby(interval_symbol_col)
    }
    for ticker, rows in df.groupby("act_symbol", sort=False):
        windows = interval_groups.get(ticker)
        if windows is None:
            continue
        keep = pd.Series(False, index=rows.index)
        for _, window in windows.iterrows():
            keep |= (
                (rows[date_col] >= window["effective_start"])
                & (rows[date_col] <= window["effective_end"])
            )
        pieces.append(rows.loc[keep])
    return pd.concat(pieces, ignore_index=True) if pieces else df.iloc[0:0].copy()


def fetch_table(
    session: requests.Session,
    table: DoltTable,
    symbols: list[str],
    intervals: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_dir: Path,
    chunk_size: int,
    date_window_days: int,
    sleep: float,
    skip_existing: bool,
) -> pd.DataFrame:
    path = out_dir / "tables" / f"{table.name}.parquet"
    if skip_existing and path.exists():
        print(f"[{table.name}] using existing {path}")
        return pd.read_parquet(path)

    frames = []
    min_date = start - pd.Timedelta(days=table.min_date_padding_days)
    total_rows = 0
    for group_num, group in enumerate(chunks(symbols, chunk_size), start=1):
        print(f"[{table.name}] symbol chunk {group_num}: {group[0]} ... {group[-1]}")
        window_start = min_date
        while window_start <= end:
            window_end = min(window_start + pd.Timedelta(days=date_window_days - 1), end)
            last: dict[str, Any] | None = None
            while True:
                where = [
                    f"act_symbol IN ({in_list(group)})",
                    f"{table.date_col} >= {sql_literal(window_start.date())}",
                    f"{table.date_col} <= {sql_literal(window_end.date())}",
                ]
                if last:
                    where.append(keyset_condition(table.order_cols, last))
                query = f"""
                    SELECT *
                    FROM {table.name}
                    WHERE {" AND ".join(where)}
                    ORDER BY {", ".join(table.order_cols)}
                    LIMIT {ROW_LIMIT};
                """
                payload = dolt_query(session, table, query)
                rows = payload.get("rows", [])
                if not rows:
                    break
                frame = normalize_dolt_frame(rows)
                frames.append(frame)
                total_rows += len(frame)
                last_row = rows[-1]
                last = {col: last_row[col] for col in table.order_cols}
                print(f"[{table.name}] fetched {total_rows:,} rows", end="\r")
                if len(rows) < ROW_LIMIT:
                    break
                time.sleep(sleep)
            window_start = window_end + pd.Timedelta(days=1)
            time.sleep(sleep)
    print(f"[{table.name}] fetched {total_rows:,} raw rows")

    table_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not table_df.empty:
        table_df = membership_filter(table_df, intervals, table.date_col)
        path.parent.mkdir(parents=True, exist_ok=True)
        table_df.to_parquet(path, index=False)
        print(f"[{table.name}] wrote {len(table_df):,} in-index rows to {path}")
    return table_df


def download_csv_export(
    session: requests.Session,
    table: DoltTable,
    out_dir: Path,
    skip_existing: bool,
) -> Path:
    path = out_dir / "raw" / "dolthub_csv" / table.database / f"{table.name}.csv"
    if skip_existing and path.exists() and path.stat().st_size > 0:
        print(f"[{table.name}] using existing CSV export {path}")
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".csv.part")
    url = f"https://www.dolthub.com/csv/{table.owner}/{table.database}/master/{table.name}"
    print(f"[{table.name}] downloading CSV export...")
    with session.get(url, stream=True, timeout=(20, 300)) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    handle.write(chunk)
    tmp_path.replace(path)
    print(f"[{table.name}] saved CSV export: {path} ({path.stat().st_size:,} bytes)")
    return path


def fetch_csv_table(
    session: requests.Session,
    table: DoltTable,
    symbols: list[str],
    intervals: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_dir: Path,
    chunksize: int,
    skip_existing: bool,
) -> pd.DataFrame:
    path = out_dir / "tables" / f"{table.name}.parquet"
    if skip_existing and path.exists():
        print(f"[{table.name}] using existing {path}")
        return pd.read_parquet(path)

    csv_path = download_csv_export(session, table, out_dir, skip_existing)
    symbol_set = set(symbols)
    min_date = start - pd.Timedelta(days=table.min_date_padding_days)
    frames = []
    total_seen = 0
    total_kept = 0
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, dtype={"act_symbol": "string"}):
        total_seen += len(chunk)
        if "act_symbol" not in chunk.columns or table.date_col not in chunk.columns:
            continue
        chunk["act_symbol"] = chunk["act_symbol"].astype(str).str.upper()
        chunk = chunk[chunk["act_symbol"].isin(symbol_set)].copy()
        if chunk.empty:
            continue
        chunk[table.date_col] = pd.to_datetime(chunk[table.date_col], errors="coerce")
        chunk = chunk[
            (chunk[table.date_col] >= min_date)
            & (chunk[table.date_col] <= end)
        ].copy()
        if chunk.empty:
            continue
        chunk = membership_filter(chunk, intervals, table.date_col)
        if chunk.empty:
            continue
        chunk = normalize_dolt_frame(chunk.to_dict("records"))
        frames.append(chunk)
        total_kept += len(chunk)
        print(f"[{table.name}] scanned {total_seen:,} rows, kept {total_kept:,}", end="\r")

    table_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not table_df.empty:
        path.parent.mkdir(parents=True, exist_ok=True)
        table_df.to_parquet(path, index=False)
    print(f"[{table.name}] scanned {total_seen:,} rows, wrote {len(table_df):,} rows to {path}")
    return table_df


def fetch_fmp_prices(
    session: requests.Session,
    api_key: str,
    symbols: list[str],
    intervals: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_dir: Path,
    sleep: float,
    skip_existing: bool,
) -> pd.DataFrame:
    path = out_dir / "tables" / "ohlcv.parquet"
    raw_dir = out_dir / "raw" / "fmp_prices"
    error_path = out_dir / "fmp_price_errors.json"
    permanent_blocked: set[str] = set()
    if skip_existing and error_path.exists():
        prior_errors = json.loads(error_path.read_text(encoding="utf-8"))
        permanent_blocked = {
            row["act_symbol"]
            for row in prior_errors
            if "HTTP 402" in row.get("error", "")
        }
    if skip_existing and path.exists():
        print(f"[fmp prices] rebuilding {path} from cached raw files and retryable misses")

    frames = []
    errors = []
    raw_dir.mkdir(parents=True, exist_ok=True)
    for i, symbol in enumerate(symbols, start=1):
        if skip_existing and symbol in permanent_blocked:
            print(f"[fmp prices] {i}/{len(symbols)} {symbol}: skipped prior 402")
            continue
        raw_path = raw_dir / f"{symbol}.json"
        try:
            if skip_existing and raw_path.exists():
                payload = json.loads(raw_path.read_text(encoding="utf-8"))
            else:
                response = session.get(
                    "https://financialmodelingprep.com/stable/historical-price-eod/full",
                    params={
                        "symbol": symbol,
                        "from": start.date().isoformat(),
                        "to": end.date().isoformat(),
                        "apikey": api_key,
                    },
                    timeout=90,
                )
                response.raise_for_status()
                payload = response.json()
                raw_path.write_text(json.dumps(payload), encoding="utf-8")

            frame = pd.DataFrame(payload if isinstance(payload, list) else [])
            if not frame.empty:
                frame = frame.rename(columns={"symbol": "act_symbol"})
                frame["act_symbol"] = frame["act_symbol"].astype(str).str.upper()
                frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
                for col in ["open", "high", "low", "close", "volume", "change", "changePercent", "vwap"]:
                    if col in frame.columns:
                        frame[col] = pd.to_numeric(frame[col], errors="coerce")
                frames.append(frame)
            print(f"[fmp prices] {i}/{len(symbols)} {symbol}: {len(frame):,} rows")
        except requests.HTTPError as exc:
            response = exc.response
            detail = (
                f"HTTP {response.status_code}: {response.text[:250]}"
                if response is not None
                else str(exc)
            )
            errors.append({"act_symbol": symbol, "error": detail})
            print(f"[fmp prices] {i}/{len(symbols)} {symbol}: {detail}")
            if response is not None and response.status_code == 429:
                print("[fmp prices] rate limit reached; stop and rerun later with --skip-existing")
                break
        time.sleep(sleep)

    prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not prices.empty:
        prices = membership_filter(prices, intervals, "date")
        path.parent.mkdir(parents=True, exist_ok=True)
        prices.to_parquet(path, index=False)
    if errors:
        existing = []
        if error_path.exists():
            existing = json.loads(error_path.read_text(encoding="utf-8"))
        by_symbol = {row["act_symbol"]: row for row in existing}
        by_symbol.update({row["act_symbol"]: row for row in errors})
        error_path.write_text(json.dumps(list(by_symbol.values()), indent=2), encoding="utf-8")
    print(f"[fmp prices] wrote {len(prices):,} in-index rows to {path}")
    return prices


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
        index=id_cols, columns="period", values="period_end_date", aggfunc="last"
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
    date_col: str,
    prefix: str,
    keep_cols: set[str] | None = None,
) -> pd.DataFrame:
    if base.empty or features.empty:
        return base
    keys = {"act_symbol", date_col}
    if keep_cols:
        keys |= keep_cols
    features = prefix_non_keys(features, prefix, keys)
    outputs = []
    for ticker, prices in base.groupby("act_symbol", sort=False):
        f = features[features["act_symbol"] == ticker].sort_values(date_col)
        if f.empty:
            outputs.append(prices)
            continue
        joined = pd.merge_asof(
            prices.sort_values("date"),
            f,
            left_on="date",
            right_on=date_col,
            by="act_symbol",
            direction="backward",
        )
        outputs.append(joined)
    return pd.concat(outputs, ignore_index=True)


def build_panel(tables: dict[str, pd.DataFrame], out_dir: Path) -> pd.DataFrame:
    prices = tables.get(PRICE_TABLE, pd.DataFrame()).copy()
    if prices.empty:
        return prices
    prices = prices.sort_values(["act_symbol", "date"]).copy()
    grouped = prices.groupby("act_symbol", group_keys=False)
    prices["ret_1d"] = grouped["close"].pct_change()
    prices["ret_5d"] = grouped["close"].pct_change(5)
    prices["ret_21d"] = grouped["close"].pct_change(21)
    prices["dollar_volume"] = prices["close"] * prices["volume"]
    prices["volume_21d_avg"] = grouped["volume"].transform(lambda s: s.rolling(21, min_periods=5).mean())
    prices["volatility_21d"] = grouped["ret_1d"].transform(lambda s: s.rolling(21, min_periods=10).std())
    for horizon in (1, 5, 21, 63):
        prices[f"target_ret_{horizon}d"] = grouped["close"].shift(-horizon) / prices["close"] - 1

    panel = prices
    for name in FUNDAMENTAL_TABLES:
        panel = asof_by_symbol(panel, tables.get(name, pd.DataFrame()), "date", name)

    eps_est = pivot_estimates(tables.get("eps_estimate", pd.DataFrame()), "eps_est")
    sales_est = pivot_estimates(tables.get("sales_estimate", pd.DataFrame()), "sales_est")
    panel = asof_by_symbol(panel, eps_est, "date", "eps_est")
    panel = asof_by_symbol(panel, sales_est, "date", "sales_est")
    panel = asof_by_symbol(panel, tables.get("rank_score", pd.DataFrame()), "date", "rank_score")
    panel = asof_by_symbol(panel, tables.get("eps_history", pd.DataFrame()), "period_end_date", "eps_hist")

    path = out_dir / "aligned_sp500_daily_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(path, index=False)
    return panel


def build_fundamentals_estimates_panel(tables: dict[str, pd.DataFrame], out_dir: Path) -> pd.DataFrame:
    eps_est = pivot_estimates(tables.get("eps_estimate", pd.DataFrame()), "eps_est")
    sales_est = pivot_estimates(tables.get("sales_estimate", pd.DataFrame()), "sales_est")
    rank_score = tables.get("rank_score", pd.DataFrame()).copy()

    base_parts = []
    for frame in [eps_est, sales_est, rank_score]:
        if not frame.empty and {"act_symbol", "date"}.issubset(frame.columns):
            base_parts.append(frame[["act_symbol", "date"]])
    if not base_parts:
        return pd.DataFrame()

    base = (
        pd.concat(base_parts, ignore_index=True)
        .drop_duplicates()
        .sort_values(["act_symbol", "date"])
        .reset_index(drop=True)
    )

    panel = base
    if not eps_est.empty:
        panel = panel.merge(eps_est, on=["act_symbol", "date"], how="left")
    if not sales_est.empty:
        panel = panel.merge(sales_est, on=["act_symbol", "date"], how="left")
    if not rank_score.empty:
        panel = panel.merge(rank_score, on=["act_symbol", "date"], how="left")

    for name in FUNDAMENTAL_TABLES:
        panel = asof_by_symbol(panel, tables.get(name, pd.DataFrame()), "date", name)

    panel = asof_by_symbol(panel, tables.get("eps_history", pd.DataFrame()), "period_end_date", "eps_hist")

    earnings_calendar = tables.get("earnings_calendar", pd.DataFrame()).copy()
    if not earnings_calendar.empty:
        earnings_calendar = earnings_calendar.rename(columns={"when": "earnings_when"})
        panel = panel.merge(earnings_calendar, on=["act_symbol", "date"], how="left")
        earnings_calendar = earnings_calendar.sort_values(["act_symbol", "date"])
        next_events = []
        for ticker, rows in panel[["act_symbol", "date"]].groupby("act_symbol", sort=False):
            events = earnings_calendar[earnings_calendar["act_symbol"] == ticker][
                ["date", "earnings_when"]
            ].sort_values("date")
            if events.empty:
                continue
            joined = pd.merge_asof(
                rows.sort_values("date"),
                events.rename(columns={"date": "next_earnings_date"}),
                left_on="date",
                right_on="next_earnings_date",
                direction="forward",
            )
            next_events.append(joined)
        if next_events:
            next_df = pd.concat(next_events, ignore_index=True)
            next_df["days_to_next_earnings"] = (
                next_df["next_earnings_date"] - next_df["date"]
            ).dt.days
            panel = panel.merge(
                next_df[
                    [
                        "act_symbol",
                        "date",
                        "next_earnings_date",
                        "earnings_when",
                        "days_to_next_earnings",
                    ]
                ].rename(columns={"earnings_when": "next_earnings_when"}),
                on=["act_symbol", "date"],
                how="left",
            )

    path = out_dir / "fundamentals_estimates_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(path, index=False)
    return panel


def build_dataset(args: argparse.Namespace) -> int:
    as_of = pd.Timestamp(args.as_of_date)
    start = as_of - pd.Timedelta(days=args.years * 365 + 3)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()

    intervals_all = download_sp500_intervals(out_dir)
    intervals = overlapping_intervals(intervals_all, start, as_of)
    symbols = sorted(intervals["ticker"].unique().tolist())
    intervals.to_parquet(out_dir / "sp500_membership_intervals_10y.parquet", index=False)

    print(f"Historical S&P symbols overlapping window: {len(symbols)}")
    if args.skip_coverage_map:
        coverage = pd.DataFrame({"act_symbol": symbols})
        for name in [PRICE_TABLE, *FUNDAMENTAL_TABLES]:
            coverage[f"{name}_rows"] = 0
        coverage["fundamental_rows"] = 0
        coverage["coverage_score"] = 0
        eligible = coverage.copy()
    else:
        coverage_frames = []
        for name in FUNDAMENTAL_TABLES:
            counts = fetch_group_counts(
                session, TABLES[name], symbols, start, as_of, args.chunk_size, args.sleep
            ).rename(columns={"rows": f"{name}_rows"})
            coverage_frames.append(counts)
            print(f"[coverage] {name}: {len(counts)} symbols")

        coverage = pd.DataFrame({"act_symbol": symbols})
        for frame in coverage_frames:
            coverage = coverage.merge(frame, on="act_symbol", how="left")
        coverage["ohlcv_rows"] = 0
        row_cols = [col for col in coverage.columns if col.endswith("_rows")]
        coverage[row_cols] = coverage[row_cols].fillna(0).astype(int)
        coverage["fundamental_rows"] = coverage[
            [f"{name}_rows" for name in FUNDAMENTAL_TABLES]
        ].sum(axis=1)
        coverage["coverage_score"] = coverage["ohlcv_rows"] + 20 * coverage["fundamental_rows"]
        coverage = coverage.sort_values(
            ["fundamental_rows", "ohlcv_rows", "act_symbol"], ascending=[False, False, True]
        )
        eligible = coverage[
            (coverage["fundamental_rows"] >= args.min_fundamental_rows)
        ].copy()
    if args.random_sample:
        eligible = eligible.sample(
            n=min(args.random_sample, len(eligible)), random_state=args.seed
        ).sort_values(["fundamental_rows", "ohlcv_rows"], ascending=False)
    if args.max_symbols:
        eligible = eligible.head(args.max_symbols)

    selected_symbols = sorted(eligible["act_symbol"].tolist())
    selected_intervals = intervals[intervals["ticker"].isin(selected_symbols)].copy()
    selected_intervals = selected_intervals.rename(columns={"ticker": "act_symbol"})

    coverage.to_parquet(out_dir / "sp500_candidate_coverage.parquet", index=False)
    eligible.to_parquet(out_dir / "sp500_selected_coverage.parquet", index=False)
    selected_intervals.to_parquet(out_dir / "sp500_selected_intervals.parquet", index=False)
    print(f"Selected symbols: {len(selected_symbols)}")

    manifest: dict[str, Any] = {
        "created_at": utc_now(),
        "sources": {
            "sp500_membership": SP500_INTERVALS_URL,
            "prices": "https://www.dolthub.com/repositories/post-no-preference/stocks",
            "earnings": "https://www.dolthub.com/repositories/post-no-preference/earnings",
        },
        "as_of_date": str(as_of.date()),
        "start_date": str(start.date()),
        "historical_sp500_symbols_overlapping_window": len(symbols),
        "selected_symbols": len(selected_symbols),
        "filters": {
            "min_price_rows": args.min_price_rows,
            "min_fundamental_rows": args.min_fundamental_rows,
            "max_symbols": args.max_symbols,
            "random_sample": args.random_sample,
        },
        "tables": {},
        "notes": [
            "Membership is point-in-time by ticker interval to reduce survivorship bias.",
            "Rows are kept only while the ticker was a historical S&P 500 constituent.",
            "Fundamental statement dates are period/end dates, not filing dates; treat these as period-date aligned, not perfect point-in-time filings.",
            "Corporate actions are not joined into the panel.",
        ],
    }

    if args.coverage_only:
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Wrote coverage files under {out_dir}")
        return 0

    tables: dict[str, pd.DataFrame] = {}
    if PRICE_TABLE in args.tables and args.price_source == "none":
        print("[ohlcv] skipped because --price-source=none")
    elif PRICE_TABLE in args.tables and args.price_source == "fmp":
        fmp_key = os.getenv(args.fmp_api_key_env)
        if not fmp_key:
            raise RuntimeError(f"Missing FMP key. Set {args.fmp_api_key_env}.")
        table_df = fetch_fmp_prices(
            session,
            fmp_key,
            selected_symbols,
            selected_intervals,
            start,
            as_of,
            out_dir,
            args.sleep,
            args.skip_existing,
        )
        tables[PRICE_TABLE] = table_df
        manifest["tables"][PRICE_TABLE] = {
            "rows": int(len(table_df)),
            "path": str(out_dir / "tables" / "ohlcv.parquet"),
            "source": "Financial Modeling Prep",
        }

    for name in args.tables:
        if name == PRICE_TABLE and args.price_source in {"none", "fmp"}:
            continue
        if args.dolt_table_source == "csv":
            table_df = fetch_csv_table(
                session,
                TABLES[name],
                selected_symbols,
                selected_intervals,
                start,
                as_of,
                out_dir,
                args.csv_chunksize,
                args.skip_existing,
            )
        else:
            table_window_days = (
                args.sparse_date_window_days
                if name in FUNDAMENTAL_TABLES or name in {"eps_history", "earnings_calendar"}
                else args.date_window_days
            )
            table_df = fetch_table(
                session,
                TABLES[name],
                selected_symbols,
                selected_intervals,
                start,
                as_of,
                out_dir,
                args.chunk_size,
                table_window_days,
                args.sleep,
                args.skip_existing,
            )
        tables[name] = table_df
        manifest["tables"][name] = {
            "rows": int(len(table_df)),
            "path": str(out_dir / "tables" / f"{name}.parquet"),
            "source": "DoltHub",
        }
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    fe_panel = build_fundamentals_estimates_panel(tables, out_dir)
    manifest["fundamentals_estimates_panel"] = {
        "rows": int(len(fe_panel)),
        "columns": int(len(fe_panel.columns)) if not fe_panel.empty else 0,
        "path": str(out_dir / "fundamentals_estimates_panel.parquet"),
    }
    if not fe_panel.empty:
        manifest["fundamentals_estimates_panel"]["symbols"] = int(fe_panel["act_symbol"].nunique())
        manifest["fundamentals_estimates_panel"]["from"] = str(fe_panel["date"].min().date())
        manifest["fundamentals_estimates_panel"]["to"] = str(fe_panel["date"].max().date())

    if PRICE_TABLE in tables:
        panel = build_panel(tables, out_dir)
        manifest["aligned_panel"] = {
            "rows": int(len(panel)),
            "columns": int(len(panel.columns)) if not panel.empty else 0,
            "path": str(out_dir / "aligned_sp500_daily_panel.parquet"),
        }
        if not panel.empty:
            manifest["aligned_panel"]["from"] = str(panel["date"].min().date())
            manifest["aligned_panel"]["to"] = str(panel["date"].max().date())

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote manifest: {out_dir / 'manifest.json'}")
    return 0


def main() -> int:
    return build_dataset(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
