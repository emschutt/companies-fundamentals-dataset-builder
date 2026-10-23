#!/usr/bin/env python3
"""Independently reread stored issuer partitions and audit historical queries."""
import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd

from build_stockfit_universe import validate_frames
from expand_stockfit import write_json
from query_stockfit import asof_events


def audit_partition(job):
    root, identity = job
    latest, versions, events = [pd.read_parquet(root / name / f"{identity}.parquet")
                               for name in ("latest", "vintages", "events")]
    counts = Counter(validate_frames(latest, versions, events))
    completed = json.loads((root / "completed" / f"{identity}.json").read_text())
    counts["row_count_mismatches"] = int((len(latest), len(versions), len(events)) != (
        completed["latest_facts"], completed["vintage_rows"], completed["feature_events"]))
    counts["issuer_identity_mismatches"] = sum(int((~f.issuer_id.eq(identity)).sum()) for f in (latest, versions, events))
    query_counts = {}
    for date in ("2005-01-01", "2010-01-01", "2012-06-01", "2017-01-01", "2020-01-01", "2026-10-23"):
        result = asof_events(events, date, filing_only=True)
        when = pd.to_datetime(result.available_at, utc=True, format="mixed")
        period = pd.to_datetime(result.period_end, utc=True)
        decision = pd.Timestamp(date, tz="UTC")
        counts["query_future_availability"] += int(when.gt(decision).sum())
        counts["query_future_period"] += int(period.gt(decision).sum())
        query_counts[date] = {"issuers_with_nonnull_facts": int(result.value.notna().any()),
                              "nonnull_values": int(result.value.notna().sum())}
    return {"issuer_id": identity, "violations": dict(counts), "queries": query_counts,
            "latest_facts": len(latest), "vintage_rows": len(versions), "events": len(events)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("workers must be 1..16")
    summary = json.loads((args.dataset / "summary.json").read_text())
    paths = sorted((args.dataset / "completed").glob("*.json"))
    violations, totals, queries = Counter(), Counter(), {}
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for number, row in enumerate(pool.map(audit_partition, [(args.dataset, p.stem) for p in paths], chunksize=8), 1):
            violations.update(row["violations"])
            totals.update({k: row[k] for k in ("latest_facts", "vintage_rows", "events")})
            for date, counts in row["queries"].items():
                queries.setdefault(date, Counter()).update(counts)
            if number % 1000 == 0:
                print(f"Audited {number:,}/{len(paths):,} stored issuer partitions", flush=True)
    violations["manifest_count_mismatches"] = int(
        len(paths) != summary["processed_issuers"] or
        totals["latest_facts"] != summary["latest_facts"] or
        totals["vintage_rows"] != summary["vintage_rows"] or totals["events"] != summary["feature_events"])
    result = {"audited_at": datetime.now(timezone.utc).isoformat(), "audited_issuers": len(paths),
              "complete_universe": summary["complete"], "counts": dict(totals),
              "violations": dict(violations), "historical_queries": {d: dict(v) for d, v in queries.items()}}
    write_json(args.dataset / "audit.json", result)
    print(json.dumps(result, indent=2))
    return 2 if any(violations.values()) or not paths else 0


if __name__ == "__main__":
    raise SystemExit(main())
