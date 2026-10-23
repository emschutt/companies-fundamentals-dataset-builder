#!/usr/bin/env python3
"""Independently check stored StockFit panel lineage and version-table keys."""
import argparse
import json
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assessment", type=Path, required=True)
    args = parser.parse_args()
    root = args.assessment
    manifest = json.loads((root / "assessment_manifest.json").read_text())
    counts = {"rows": 0, "nonnull_values": 0, "vintage_rows": 0,
              "future_availability": 0, "future_period": 0, "missing_lineage": 0,
              "duplicate_keys": 0, "duplicate_vintage_keys": 0, "invalid_traced_timing": 0}
    for path in sorted((root / "panel").glob("*.parquet")):
        panel = pd.read_parquet(path)
        decision = pd.to_datetime(panel.date, utc=True)
        counts["rows"] += len(panel)
        counts["duplicate_keys"] += int(panel.duplicated(["act_symbol", "date"]).sum())
        for name in [c for c in panel if c.startswith("sf_") and "__" not in c]:
            valid = panel[name].notna()
            available = panel[name + "__available_at"]
            period = panel[name + "__period_end"]
            counts["nonnull_values"] += int(valid.sum())
            counts["future_availability"] += int(available.gt(decision).sum())
            counts["future_period"] += int(period.gt(panel.date).sum())
            counts["missing_lineage"] += int((valid & (available.isna() | period.isna())).sum())
    keys = ["act_symbol", "statement", "period_kind", "period_end", "metric", "available_at"]
    for path in sorted((root / "vintages").glob("*.parquet")):
        frame = pd.read_parquet(path, columns=keys + ["pit_status", "retrieved_at", "accession"])
        counts["vintage_rows"] += len(frame)
        counts["duplicate_vintage_keys"] += int(frame.duplicated(keys).sum())
        traced = frame.loc[frame.pit_status.eq("filing_traced")]
        available = pd.to_datetime(traced.available_at, utc=True, format="mixed")
        retrieved = pd.to_datetime(traced.retrieved_at, utc=True, format="mixed")
        period = pd.to_datetime(traced.period_end, utc=True)
        counts["invalid_traced_timing"] += int((available.isna() | available.gt(retrieved) |
                                                available.lt(period) | traced.accession.isna()).sum())
    counts["row_count_mismatch"] = int(counts["rows"] != manifest.get("panel_rows", 0))
    counts["vintage_count_mismatch"] = int(counts["vintage_rows"] != manifest["vintage_rows"])
    (root / "validation.json").write_text(json.dumps(counts, indent=2))
    print(json.dumps(counts, indent=2))
    checks = set(counts) - {"rows", "nonnull_values", "vintage_rows"}
    return 2 if any(counts[k] for k in checks) else 0


if __name__ == "__main__":
    raise SystemExit(main())
