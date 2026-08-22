#!/usr/bin/env python3
"""Build an aligned FMP price/fundamental dataset.

This downloader is designed for API plans that allow per-symbol historical
prices and a limited number of financial statement rows per symbol.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_URL = "https://financialmodelingprep.com/stable"
DEFAULT_ENDPOINTS = {
    "prices": ("historical-price-eod/full", {"from": "{from_date}", "to": "{to_date}"}),
    "profile": ("profile", {}),
    "income_quarter": ("income-statement", {"period": "quarter", "limit": "{limit}"}),
    "balance_quarter": (
        "balance-sheet-statement",
        {"period": "quarter", "limit": "{limit}"},
    ),
    "cashflow_quarter": (
        "cash-flow-statement",
        {"period": "quarter", "limit": "{limit}"},
    ),
    "enterprise_quarter": (
        "enterprise-values",
        {"period": "quarter", "limit": "{limit}"},
    ),
    "growth_quarter": ("financial-growth", {"period": "quarter", "limit": "{limit}"}),
    "income_annual": ("income-statement", {"period": "annual", "limit": "{limit}"}),
    "balance_annual": (
        "balance-sheet-statement",
        {"period": "annual", "limit": "{limit}"},
    ),
    "cashflow_annual": (
        "cash-flow-statement",
        {"period": "annual", "limit": "{limit}"},
    ),
    "key_metrics_annual": ("key-metrics", {"period": "annual", "limit": "{limit}"}),
    "ratios_annual": ("ratios", {"period": "annual", "limit": "{limit}"}),
    "enterprise_annual": ("enterprise-values", {"period": "annual", "limit": "{limit}"}),
    "growth_annual": ("financial-growth", {"period": "annual", "limit": "{limit}"}),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download FMP prices/fundamentals and build an aligned panel."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["AAPL"],
        help="Ticker symbols to download.",
    )
    parser.add_argument(
        "--ticker-file",
        type=Path,
        help="Optional file with one ticker per line. Combined with --tickers.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Years of daily prices to request.",
    )
    parser.add_argument(
        "--fundamental-limit",
        type=int,
        default=5,
        help="Rows per symbol for each statement endpoint. Current key appears capped at 5.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/fmp"),
        help="Output directory.",
    )
    parser.add_argument(
        "--api-key-env",
        default="FMP_API_KEY",
        help="Environment variable containing the FMP API key.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Seconds to sleep between API calls.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing raw JSON responses when present.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_tickers(args: argparse.Namespace) -> list[str]:
    tickers = list(args.tickers or [])
    if args.ticker_file:
        lines = args.ticker_file.read_text(encoding="utf-8").splitlines()
        tickers.extend(line.strip() for line in lines if line.strip())
    cleaned = []
    seen = set()
    for ticker in tickers:
        symbol = ticker.strip().upper()
        if symbol and symbol not in seen:
            cleaned.append(symbol)
            seen.add(symbol)
    return cleaned


def request_json(
    session: requests.Session,
    api_key: str,
    route: str,
    params: dict[str, str],
) -> Any:
    query = dict(params)
    query["apikey"] = api_key
    response = session.get(f"{BASE_URL}/{route}", params=query, timeout=90)
    response.raise_for_status()
    return response.json()


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_or_fetch(
    session: requests.Session,
    api_key: str,
    raw_path: Path,
    route: str,
    params: dict[str, str],
    skip_existing: bool,
) -> tuple[Any, bool]:
    if skip_existing and raw_path.exists():
        return json.loads(raw_path.read_text(encoding="utf-8")), True
    payload = request_json(session, api_key, route, params)
    save_json(raw_path, payload)
    return payload, False


def as_frame(payload: Any) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict):
        if "historical" in payload and isinstance(payload["historical"], list):
            return pd.DataFrame(payload["historical"])
        return pd.DataFrame([payload])
    return pd.DataFrame()


def endpoint_params(template: dict[str, str], values: dict[str, str]) -> dict[str, str]:
    return {key: value.format(**values) for key, value in template.items()}


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def normalize_table(df: pd.DataFrame, source: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["source_table"] = source
    symbol_col = "symbol" if "symbol" in out.columns else None
    if symbol_col:
        out = out.rename(columns={symbol_col: "ticker"})
    for col in ("date", "filingDate", "acceptedDate"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def prefix_feature_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    keep = {
        "ticker",
        "date",
        "filingDate",
        "acceptedDate",
        "fiscalYear",
        "period",
        "reportedCurrency",
        "source_table",
    }
    return df.rename(columns={col: f"{prefix}_{col}" for col in df.columns if col not in keep})


def merge_fundamental_parts(parts: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    key = ["ticker", "date", "fiscalYear", "period"]
    for name, frame in parts:
        if frame.empty:
            continue
        available_key = [col for col in key if col in frame.columns]
        data = prefix_feature_columns(frame, name)
        if merged is None:
            merged = data
            continue
        join_key = [col for col in available_key if col in merged.columns]
        if not join_key:
            continue
        merged = merged.merge(data, on=join_key, how="outer", suffixes=("", f"_{name}"))

    if merged is None:
        return pd.DataFrame()
    filing_cols = [col for col in merged.columns if col.endswith("filingDate")]
    accepted_cols = [col for col in merged.columns if col.endswith("acceptedDate")]
    if "filingDate" not in merged.columns and filing_cols:
        merged["filingDate"] = merged[filing_cols].bfill(axis=1).iloc[:, 0]
    if "acceptedDate" not in merged.columns and accepted_cols:
        merged["acceptedDate"] = merged[accepted_cols].bfill(axis=1).iloc[:, 0]
    if "filingDate" in merged.columns:
        merged["asof_date"] = pd.to_datetime(merged["filingDate"], errors="coerce")
    else:
        merged["asof_date"] = pd.to_datetime(merged["date"], errors="coerce")
    return merged.sort_values(["ticker", "asof_date", "date"])


def asof_join(prices: pd.DataFrame, fundamentals: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if prices.empty or fundamentals.empty:
        return prices
    outputs = []
    for ticker, px in prices.groupby("ticker", sort=False):
        fund = fundamentals[fundamentals["ticker"] == ticker].copy()
        if fund.empty:
            outputs.append(px)
            continue
        fund = fund.sort_values("asof_date")
        px = px.sort_values("date")
        fund = fund.rename(
            columns={
                col: f"{prefix}_{col}"
                for col in fund.columns
                if col not in {"ticker", "asof_date"}
            }
        )
        joined = pd.merge_asof(
            px,
            fund,
            left_on="date",
            right_on="asof_date",
            by="ticker",
            direction="backward",
        )
        outputs.append(joined)
    return pd.concat(outputs, ignore_index=True)


def add_price_features(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel
    panel = panel.sort_values(["ticker", "date"]).copy()
    grouped = panel.groupby("ticker", group_keys=False)
    panel["ret_1d"] = grouped["close"].pct_change()
    panel["ret_5d"] = grouped["close"].pct_change(5)
    panel["ret_21d"] = grouped["close"].pct_change(21)
    panel["volume_21d_avg"] = grouped["volume"].transform(
        lambda s: s.rolling(21, min_periods=5).mean()
    )
    panel["volatility_21d"] = grouped["ret_1d"].transform(
        lambda s: s.rolling(21, min_periods=10).std()
    )
    for horizon in (1, 5, 21, 63):
        panel[f"target_ret_{horizon}d"] = grouped["close"].shift(-horizon) / panel["close"] - 1
    return panel


def build_dataset(args: argparse.Namespace) -> int:
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        print(f"Missing API key. Set {args.api_key_env} before running.")
        return 2

    tickers = get_tickers(args)
    today = date.today()
    from_date = today - timedelta(days=365 * args.years + 3)
    values = {
        "from_date": from_date.isoformat(),
        "to_date": today.isoformat(),
        "limit": str(args.fundamental_limit),
    }

    session = requests.Session()
    raw_dir = args.out_dir / "raw"
    table_dir = args.out_dir / "tables"
    manifest: dict[str, Any] = {
        "created_at": utc_now(),
        "source": "Financial Modeling Prep stable API",
        "years": args.years,
        "fundamental_limit": args.fundamental_limit,
        "tickers": tickers,
        "tables": {},
        "notes": [
            "Daily panel uses filingDate for fundamental as-of alignment.",
            "The current tested key blocks bulk/universe endpoints and caps statement history at 5 rows per symbol.",
        ],
    }

    collected: dict[str, list[pd.DataFrame]] = {name: [] for name in DEFAULT_ENDPOINTS}
    for ticker in tickers:
        print(f"\n=== {ticker} ===")
        for name, (route, params_template) in DEFAULT_ENDPOINTS.items():
            params = {"symbol": ticker}
            params.update(endpoint_params(params_template, values))
            raw_path = raw_dir / ticker / f"{name}.json"
            try:
                payload, reused = load_or_fetch(
                    session,
                    api_key,
                    raw_path,
                    route,
                    params,
                    args.skip_existing,
                )
                frame = normalize_table(as_frame(payload), name)
                collected[name].append(frame)
                manifest["tables"].setdefault(name, {"rows": 0, "errors": []})
                manifest["tables"][name]["rows"] += len(frame)
                print(f"[{ticker}] {name}: {len(frame)} rows{' reused' if reused else ''}")
            except requests.HTTPError as exc:
                response = exc.response
                detail = (
                    f"HTTP {response.status_code}: {response.text[:300]}"
                    if response is not None
                    else str(exc)
                )
                manifest["tables"].setdefault(name, {"rows": 0, "errors": []})
                manifest["tables"][name]["errors"].append({"ticker": ticker, "error": detail})
                print(f"[{ticker}] {name}: {detail}")
            time.sleep(args.sleep)

    tables: dict[str, pd.DataFrame] = {}
    for name, frames in collected.items():
        table = pd.concat([df for df in frames if not df.empty], ignore_index=True) if frames else pd.DataFrame()
        tables[name] = table
        if not table.empty:
            write_parquet(table, table_dir / f"{name}.parquet")

    prices = tables.get("prices", pd.DataFrame()).copy()
    if not prices.empty:
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        prices = prices.sort_values(["ticker", "date"])

    quarterly = merge_fundamental_parts(
        [
            ("income_q", tables.get("income_quarter", pd.DataFrame())),
            ("balance_q", tables.get("balance_quarter", pd.DataFrame())),
            ("cashflow_q", tables.get("cashflow_quarter", pd.DataFrame())),
            ("enterprise_q", tables.get("enterprise_quarter", pd.DataFrame())),
            ("growth_q", tables.get("growth_quarter", pd.DataFrame())),
        ]
    )
    annual = merge_fundamental_parts(
        [
            ("income_y", tables.get("income_annual", pd.DataFrame())),
            ("balance_y", tables.get("balance_annual", pd.DataFrame())),
            ("cashflow_y", tables.get("cashflow_annual", pd.DataFrame())),
            ("metrics_y", tables.get("key_metrics_annual", pd.DataFrame())),
            ("ratios_y", tables.get("ratios_annual", pd.DataFrame())),
            ("enterprise_y", tables.get("enterprise_annual", pd.DataFrame())),
            ("growth_y", tables.get("growth_annual", pd.DataFrame())),
        ]
    )
    if not quarterly.empty:
        write_parquet(quarterly, table_dir / "fundamentals_quarterly_merged.parquet")
    if not annual.empty:
        write_parquet(annual, table_dir / "fundamentals_annual_merged.parquet")

    panel = asof_join(prices, quarterly, "q")
    panel = asof_join(panel, annual, "y")
    panel = add_price_features(panel)
    if not panel.empty:
        write_parquet(panel, args.out_dir / "aligned_daily_panel.parquet")
        manifest["aligned_daily_panel"] = {
            "rows": len(panel),
            "columns": len(panel.columns),
            "from": str(panel["date"].min().date()),
            "to": str(panel["date"].max().date()),
            "path": str(args.out_dir / "aligned_daily_panel.parquet"),
        }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(f"\nWrote manifest: {args.out_dir / 'manifest.json'}")
    return 0


def main() -> int:
    return build_dataset(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
