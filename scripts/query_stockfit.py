#!/usr/bin/env python3
"""Materialize an all-metric issuer snapshot at an explicit UTC decision time."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from build_stockfit_universe import FEATURE, atomic_parquet
from expand_stockfit import issuer_id, write_json


def asof_events(events, decision_at, filing_only=False, max_age_days=None):
    decision = pd.Timestamp(decision_at)
    decision = decision.tz_localize("UTC") if decision.tzinfo is None else decision.tz_convert("UTC")
    if events.empty:
        return events.assign(decision_at=pd.Series(dtype="datetime64[ns, UTC]"),
                             age_days=pd.Series(dtype="int64"))
    when = pd.to_datetime(events.available_at, utc=True, format="mixed")
    period = pd.to_datetime(events.period_end, utc=True)
    selected = events.loc[when.le(decision) & period.le(decision)].copy()
    selected["_time"] = when.loc[selected.index]
    selected = selected.sort_values("_time").drop_duplicates(FEATURE, keep="last").drop(columns="_time")
    # Filter after selection: never resurrect an older value after a null/restricted revision.
    if filing_only:
        selected = selected.loc[selected.pit_status.eq("filing_traced")]
    selected["decision_at"] = decision
    selected["age_days"] = (decision - pd.to_datetime(selected.period_end, utc=True)).dt.days
    if max_age_days is not None:
        selected = selected.loc[selected.age_days.le(max_age_days)]
    return selected.reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--as-of", required=True, help="Date means 00:00 UTC; timestamps may specify a timezone")
    parser.add_argument("--ciks", type=int, nargs="+")
    parser.add_argument("--filing-only", action="store_true")
    parser.add_argument("--max-age-days", type=int)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.max_age_days is not None and args.max_age_days < 0:
        parser.error("max-age-days cannot be negative")
    if args.out_dir.exists():
        parser.error("Use a new output directory")
    paths = [args.dataset / "events" / f"{issuer_id(c)}.parquet" for c in args.ciks] if args.ciks else sorted((args.dataset / "events").glob("*.parquet"))
    if not paths or any(not p.exists() for p in paths):
        parser.error("Requested issuer partitions are missing")
    args.out_dir.mkdir(parents=True)
    rows = 0
    for path in paths:
        result = asof_events(pd.read_parquet(path), args.as_of, args.filing_only, args.max_age_days)
        if not result.empty:
            atomic_parquet(result, args.out_dir / path.name)
            rows += len(result)
    if rows == 0:
        atomic_parquet(result, args.out_dir / "empty.parquet")
    write_json(args.out_dir / "_query.json", {
        "dataset": str(args.dataset.resolve()), "decision_at": args.as_of,
        "ciks": args.ciks, "filing_only": args.filing_only,
        "max_age_days": args.max_age_days, "rows": rows,
        "note": "Issuer fundamentals only; historical listing/index membership is not inferred.",
    })
    print(f"Selected {rows:,} feature values (including null tombstones) across {len(paths):,} issuer partitions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
