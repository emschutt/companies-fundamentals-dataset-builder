#!/usr/bin/env python3
"""Add yfinance OHLCV and optional DefeatBeta ratios to the DoltHub S&P panel."""

from __future__ import annotations

import argparse
import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf


DEFAULT_DEFEATBETA_METHODS = [
    "ttm_eps",
    "ttm_pe",
    "market_capitalization",
    "ps_ratio",
    "pb_ratio",
    "peg_ratio",
    "roe",
    "roa",
    "roic",
    "wacc",
    "equity_multiplier",
    "asset_turnover",
    "enterprise_value",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--include-defeatbeta", action="store_true")
    parser.add_argument(
        "--defeatbeta-methods",
        nargs="+",
        default=DEFAULT_DEFEATBETA_METHODS,
        help="Ticker method names exposed by defeatbeta-api.",
    )
    return parser.parse_args()


def yfinance_symbol(symbol: str) -> str:
    return symbol.replace(".", "-").strip()


def load_base(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel_path = data_dir / "fundamentals_estimates_panel.parquet"
    intervals_path = data_dir / "sp500_selected_intervals.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(f"Missing base panel: {panel_path}")
    if not intervals_path.exists():
        raise FileNotFoundError(f"Missing membership intervals: {intervals_path}")

    panel = pd.read_parquet(panel_path)
    intervals = pd.read_parquet(intervals_path)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()
    for col in ["effective_start", "effective_end", "start_date", "end_date"]:
        if col in intervals.columns:
            intervals[col] = pd.to_datetime(intervals[col]).dt.normalize()
    return panel, intervals


def symbol_universe(panel: pd.DataFrame, intervals: pd.DataFrame, max_symbols: int | None) -> list[str]:
    symbols = sorted(set(panel["act_symbol"].dropna()).intersection(intervals["act_symbol"].dropna()))
    if max_symbols:
        symbols = symbols[:max_symbols]
    return symbols


def download_yfinance_symbol(
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    raw_dir: Path,
    skip_existing: bool,
) -> tuple[pd.DataFrame, str | None]:
    out = raw_dir / f"{symbol.replace('/', '_')}.parquet"
    if skip_existing and out.exists():
        return pd.read_parquet(out), None

    yf_symbol = yfinance_symbol(symbol)
    try:
        data = yf.download(
            yf_symbol,
            start=start.date().isoformat(),
            end=end.date().isoformat(),
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:  # yfinance raises heterogeneous errors.
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"

    if data.empty:
        return pd.DataFrame(), "empty yfinance response"

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]

    data = data.reset_index()
    rename = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    data = data.rename(columns=rename)
    keep = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    data = data[[col for col in keep if col in data.columns]].copy()
    data["act_symbol"] = symbol
    data["yf_symbol"] = yf_symbol
    data["date"] = pd.to_datetime(data["date"]).dt.tz_localize(None).dt.normalize()
    raw_dir.mkdir(parents=True, exist_ok=True)
    data.to_parquet(out, index=False)
    return data, None


def download_yfinance(
    symbols: Iterable[str],
    years: int,
    raw_dir: Path,
    skip_existing: bool,
    sleep: float,
) -> tuple[pd.DataFrame, dict[str, str]]:
    end = pd.Timestamp.today().normalize() + timedelta(days=1)
    start = end - timedelta(days=years * 365 + 14)
    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    symbols = list(symbols)
    for idx, symbol in enumerate(symbols, 1):
        print(f"[yfinance] {idx}/{len(symbols)} {symbol}", flush=True)
        frame, error = download_yfinance_symbol(symbol, start, end, raw_dir, skip_existing)
        if error:
            errors[symbol] = error
        elif not frame.empty:
            frames.append(frame)
        if sleep:
            time.sleep(sleep)

    if not frames:
        return pd.DataFrame(), errors
    return pd.concat(frames, ignore_index=True), errors


def apply_membership_filter(prices: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices
    merged = prices.merge(intervals[["act_symbol", "effective_start", "effective_end"]], on="act_symbol", how="left")
    mask = (merged["date"] >= merged["effective_start"]) & (merged["date"] <= merged["effective_end"])
    return merged.loc[mask].drop(columns=["effective_start", "effective_end"]).copy()


def add_price_features(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices
    prices = prices.sort_values(["act_symbol", "date"]).copy()
    group = prices.groupby("act_symbol", group_keys=False)
    close = group["adj_close"].transform(lambda s: s.ffill())
    prices["yf_ret_1d"] = group["adj_close"].pct_change(1)
    prices["yf_ret_5d"] = group["adj_close"].pct_change(5)
    prices["yf_ret_21d"] = group["adj_close"].pct_change(21)
    prices["yf_dollar_volume"] = close * prices["volume"]
    prices["yf_volume_21d_avg"] = group["volume"].transform(lambda s: s.rolling(21, min_periods=10).mean())
    prices["yf_volatility_21d"] = group["adj_close"].transform(
        lambda s: s.pct_change().rolling(21, min_periods=10).std()
    )
    for horizon in [1, 5, 21, 63]:
        prices[f"target_ret_{horizon}d"] = group["adj_close"].shift(-horizon) / prices["adj_close"] - 1
    return prices


def asof_join_panel(panel: pd.DataFrame, right: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if right.empty:
        return panel

    panel_sorted = panel.sort_values(["act_symbol", "date"]).copy()
    right_sorted = right.sort_values(["act_symbol", "date"]).copy()
    panel_sorted["date"] = pd.to_datetime(panel_sorted["date"]).dt.normalize().astype("datetime64[ns]")
    right_sorted["date"] = pd.to_datetime(right_sorted["date"]).dt.normalize().astype("datetime64[ns]")
    right_cols = [col for col in right_sorted.columns if col not in {"act_symbol", "date"}]
    rename_map = {}
    for col in right_cols:
        if prefix == "yf" and (col.startswith("yf_") or col.startswith("target_ret_")):
            rename_map[col] = col
        elif prefix == "defeatbeta" and col.startswith("db_"):
            rename_map[col] = f"defeatbeta_{col}"
        else:
            rename_map[col] = f"{prefix}_{col}"
    renamed = right_sorted.rename(columns=rename_map)
    frames = []
    for symbol, left_part in panel_sorted.groupby("act_symbol", sort=False):
        right_part = renamed[renamed["act_symbol"] == symbol]
        if right_part.empty:
            frames.append(left_part)
            continue
        frames.append(
            pd.merge_asof(
                left_part.sort_values("date"),
                right_part.sort_values("date"),
                on="date",
                by="act_symbol",
                direction="backward",
            )
        )
    return pd.concat(frames, ignore_index=True)


def fetch_defeatbeta(symbols: Iterable[str], methods: list[str], raw_dir: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    try:
        from defeatbeta_api.data.ticker import Ticker
    except Exception as exc:
        return pd.DataFrame(), {"__import__": f"{type(exc).__name__}: {exc}"}

    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    raw_dir.mkdir(parents=True, exist_ok=True)
    symbols = list(symbols)
    for idx, symbol in enumerate(symbols, 1):
        print(f"[defeatbeta] {idx}/{len(symbols)} {symbol}", flush=True)
        db_symbol = yfinance_symbol(symbol)
        try:
            ticker = Ticker(db_symbol)
        except Exception as exc:
            errors[symbol] = f"Ticker init {type(exc).__name__}: {exc}"
            continue
        for method in methods:
            if not hasattr(ticker, method):
                errors[f"{symbol}.{method}"] = "method not available"
                continue
            try:
                result = getattr(ticker, method)()
                frame = pd.DataFrame(result)
            except Exception as exc:
                errors[f"{symbol}.{method}"] = f"{type(exc).__name__}: {exc}"
                continue
            if frame.empty:
                continue
            if "report_date" in frame.columns:
                frame = frame.rename(columns={"report_date": "date"})
            elif "date" not in frame.columns:
                errors[f"{symbol}.{method}"] = "no report_date/date column"
                continue
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
            frame = frame.dropna(subset=["date"])
            frame["act_symbol"] = symbol
            value_cols = [col for col in frame.columns if col not in {"symbol", "act_symbol", "date"}]
            frame = frame[["act_symbol", "date"] + value_cols].copy()
            frame = frame.rename(columns={col: f"db_{method}_{col}" for col in value_cols})
            frame.to_parquet(raw_dir / f"{symbol.replace('/', '_')}_{method}.parquet", index=False)
            frames.append(frame)

    if not frames:
        return pd.DataFrame(), errors

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["act_symbol", "date"])
    combined = combined.groupby(["act_symbol", "date"], as_index=False).last()
    return combined, errors


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir
    raw_dir = data_dir / "raw"
    tables_dir = data_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    panel, intervals = load_base(data_dir)
    symbols = symbol_universe(panel, intervals, args.max_symbols)

    yfinance_raw, yfinance_errors = download_yfinance(
        symbols=symbols,
        years=args.years,
        raw_dir=raw_dir / "yfinance_ohlcv",
        skip_existing=args.skip_existing,
        sleep=args.sleep,
    )
    prices = apply_membership_filter(yfinance_raw, intervals)
    prices = add_price_features(prices)
    if not prices.empty:
        prices.to_parquet(tables_dir / "yfinance_ohlcv.parquet", index=False)
        panel = asof_join_panel(panel, prices, "yf")

    defeatbeta = pd.DataFrame()
    defeatbeta_errors: dict[str, str] = {}
    if args.include_defeatbeta:
        defeatbeta, defeatbeta_errors = fetch_defeatbeta(
            symbols=symbols,
            methods=args.defeatbeta_methods,
            raw_dir=raw_dir / "defeatbeta",
        )
        defeatbeta = apply_membership_filter(defeatbeta, intervals)
        if not defeatbeta.empty:
            defeatbeta.to_parquet(tables_dir / "defeatbeta_ratios.parquet", index=False)
            panel = asof_join_panel(panel, defeatbeta, "defeatbeta")

    output_path = data_dir / "fundamentals_estimates_prices_panel.parquet"
    panel.to_parquet(output_path, index=False)

    manifest = {
        "base_panel_rows": int(len(panel)),
        "symbols_requested": len(symbols),
        "yfinance_rows_raw": int(len(yfinance_raw)),
        "yfinance_rows_membership_filtered": int(len(prices)),
        "yfinance_symbols": int(prices["act_symbol"].nunique()) if not prices.empty else 0,
        "yfinance_errors": yfinance_errors,
        "defeatbeta_enabled": bool(args.include_defeatbeta),
        "defeatbeta_rows": int(len(defeatbeta)),
        "defeatbeta_symbols": int(defeatbeta["act_symbol"].nunique()) if not defeatbeta.empty else 0,
        "defeatbeta_errors": defeatbeta_errors,
        "output": str(output_path),
    }
    (data_dir / "price_enrichment_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
