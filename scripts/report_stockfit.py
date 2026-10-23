#!/usr/bin/env python3
"""Summarize a completed StockFit assessment and its coverage extensions."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path

import pandas as pd

from assess_stockfit import MAPPINGS, candidate_period


def markdown_table(headers, rows):
    return "\n".join(["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"] +
                     ["| " + " | ".join(str(v).replace("|", "/") for v in row) + " |" for row in rows])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assessment", type=Path, required=True)
    args = parser.parse_args()
    root = args.assessment
    manifest = json.loads((root / "assessment_manifest.json").read_text())
    data = Path(manifest["source_data"])
    coverage = pd.read_parquet(root / "coverage.parquet")
    comparison = pd.read_parquet(root / "comparison") if list((root / "comparison").glob("*.parquet")) else pd.DataFrame(
        columns=["status", "stockfit_first_traced_value", "stockfit_latest_value"])
    timing = pd.read_parquet(root / "timing") if list((root / "timing").glob("*.parquet")) else pd.DataFrame(
        {"lag_minus_filing_days": pd.Series(dtype=float)})
    invalid_timing = pd.Series(False, index=timing.index)
    timing_example = ""
    if not timing.empty:
        invalid_timing = pd.to_datetime(timing.filed_at, utc=True, format="mixed").lt(
            pd.to_datetime(timing.stockfit_period_end, utc=True))
        timing.loc[invalid_timing].to_parquet(root / "reference_timing_anomalies.parquet", index=False)
        if invalid_timing.any():
            example = timing.loc[invalid_timing].iloc[0]
            timing_example = f" Example: {example.act_symbol}, period {example.stockfit_period_end}, reported filing {example.filed_at}."
        timing = timing.loc[~invalid_timing].copy()
    original = pd.read_parquet(data / "fundamentals_estimates_prices_panel_pit.parquet", columns=["act_symbol", "date"])
    base_periods = {}
    local_symbols = set()
    for table, (statement, _) in MAPPINGS.items():
        frame = pd.read_parquet(data / "tables" / f"{table}.parquet", columns=["act_symbol", "date", "period"])
        local_symbols.update(frame.act_symbol)
        for row in frame.itertuples():
            kind = {"Year": "annual", "Quarter": "quarter"}.get(row.period)
            base_periods.setdefault((row.act_symbol, statement, kind), set()).add(pd.Timestamp(row.date).date().isoformat())
    extensions, metric_set, period_counts = [], set(), Counter()
    for path in sorted((root / "latest").glob("*.parquet")):
        frame = pd.read_parquet(path, columns=["act_symbol", "statement", "period_kind", "period_end", "metric"])
        metric_set.update(zip(frame.statement, frame.metric))
        periods = frame.drop(columns="metric").drop_duplicates()
        for row in periods.itertuples():
            period_counts[row.period_kind] += 1
            dates = base_periods.get((row.act_symbol, row.statement, row.period_kind), set())
            if candidate_period(row.period_end, dates) is not None:
                continue
            category = "new_symbol_or_statement" if not dates else (
                "earlier_history" if row.period_end < min(dates) else
                "newer_period" if row.period_end > max(dates) else "internal_gap")
            extensions.append({"act_symbol": row.act_symbol, "statement": row.statement,
                               "period_kind": row.period_kind, "period_end": row.period_end, "extension": category})
    extension_frame = pd.DataFrame(extensions, columns=["act_symbol", "statement", "period_kind", "period_end", "extension"])
    extension_frame.to_parquet(root / "coverage_extensions.parquet", index=False)
    status = comparison.status.value_counts()
    comparable_statuses = ["match", "difference", "share_basis_or_value_difference", "sign_difference", "ytd_basis_match"]
    comparable = int(status.reindex(comparable_statuses, fill_value=0).sum())
    comparable_matches = int(status.get("match", 0))
    match_percentage = f"{100*comparable_matches/comparable:.1f}%" if comparable else "not available"
    new_symbols = sorted(set(coverage.loc[coverage.latest_facts.gt(0), "act_symbol"]) - local_symbols)
    extension_counts = extension_frame.extension.value_counts().to_dict() if len(extension_frame) else {}
    first_diff = comparison.loc[comparison.stockfit_first_traced_value.notna() & comparison.stockfit_latest_value.notna()]
    first_diff = first_diff.loc[first_diff.stockfit_first_traced_value.ne(first_diff.stockfit_latest_value)]
    summary = {"comparable_cells": comparable, "matching_cells": comparable_matches,
               "match_rate": comparable_matches / comparable if comparable else None,
               "extensions": extension_counts, "new_symbols_vs_raw_statements": new_symbols,
               "distinct_statement_metrics": len(metric_set), "reference_statement_periods": dict(period_counts),
               "comparison_cells_first_vintage_differs_from_latest": len(first_diff),
               "invalid_original_filing_dates_before_period_end": int(invalid_timing.sum()),
               "valid_timing_comparisons": len(timing)}
    (root / "report_summary.json").write_text(json.dumps(summary, indent=2))
    lines = ["# Fundamentals dataset assessment against StockFit", "", f"Snapshot date: {manifest['built_at'][:10]}", "",
             "StockFit is the requested reference. This report compares the saved local dataset with the downloaded responses; it does not independently certify every StockFit value against SEC filings.", "",
             "## Findings", "",
             f"1. Fixed lags preceded StockFit's reported original filing timestamp in **{manifest['statement_periods_lag_before_filing']:,} of {len(timing):,} valid matched table-period rows**. Earlier earnings releases may contain some of these figures; this test establishes that 45/90-day lags alone do not guarantee filing-based availability. Impossible pre-period filing dates are excluded from this denominator.",
             f"2. **{int(status.get('ytd_basis_match', 0)):,} cash-flow cells** matched cumulative fiscal-year totals rather than standalone quarters, including possible sign differences. These are period-definition differences, not simply missing data.",
             f"3. **{len(first_diff):,} mapped cells** reference a StockFit fact whose earliest traced value differs from its latest value. A lag applied to the latest figure cannot remove revision leakage. This count includes split-related and other filing changes.",
             f"4. **{manifest['quality_counts'].get('observed_only',0):,} latest reference facts** lack sufficient dated provenance for this adapter's historical gate. They remain available in the snapshot but enter the version table only at observation time.",
             f"5. The download returned facts for **{manifest['symbols_with_facts']} of {manifest['symbols_requested']} requested symbols**. Verified CIK overrides recover selected predecessor/delisted issuers; other identity gaps remain explicit.", "",
             "## Coverage", "",
             markdown_table(["Measure", "Result"], [
                 ["Original research panel", f"{len(original):,} rows; {original.act_symbol.nunique()} symbols"],
                 ["Original panel dates", f"{original.date.min().date()} to {original.date.max().date()}"],
                 ["StockFit fiscal-period labels", f"{coverage.period_min.min()} to {coverage.period_max.max()}"],
                 ["Normalized latest facts", f"{manifest['latest_facts']:,}"],
                 ["Version records", f"{manifest['vintage_rows']:,}"],
                 ["Distinct statement/metric pairs", len(metric_set)],
                 ["Facts carrying revision entries", f"{manifest['facts_with_revisions']:,}"],
                 ["Panel rows with reference values", f"{manifest.get('panel_rows_with_reference_values',0):,} / {manifest.get('panel_rows',0):,}"],
                 ["Additional symbols vs raw local statements", ", ".join(new_symbols) or "None"],
             ]), "", "New reference statement-period rows (quarter/annual remain separate):", "",
             markdown_table(["Extension", "Rows"], [[k, f"{v:,}"] for k, v in extension_counts.items()]), "",
             "These are additions to the reference layer, not overwrites of the original DoltHub tables. A newly available fact may still be restricted to snapshot use.", "",
             "## Value Comparison", "",
             f"**{comparable_matches:,} / {comparable:,} comparable cells ({match_percentage})** match within the stated tolerance. Missing reference cells, local nulls and unverified currency conversions are excluded from that denominator.", "",
             markdown_table(["Classification", "Cells"], [[k, f"{v:,}"] for k, v in status.items()]), "",
             "Matching uses the same symbol, statement and frequency, with a unique fiscal-date candidate within 15 days. Both providers can label fiscal dates differently, so the original labels and offsets are retained. Value tolerance is 1% of the larger magnitude, with a 1,000-unit absolute floor (0.011 for EPS). 'Difference' means an unresolved discrepancy, not proof that a local value is wrong. Mappings such as cash plus short-term investments, equity and shares can differ in definition. Sign and share-basis differences are reported separately and never silently corrected.", "",
             "## Timing", "",
             f"The reference itself contains **{int(invalid_timing.sum()):,} table-period timing anomalies** with a reported original filing timestamp before the fiscal period ends. These are retained in `reference_timing_anomalies.parquet` and excluded from the timing summary.{timing_example} Very late timestamps also require source-level review before interpreting them as actual original reporting delays. Per-fact source checks independently restrict invalid pre-period timestamps to snapshot use.", "",
             markdown_table(["Statistic", "Days"], [
                 ["Median fixed-lag availability minus filing time", round(timing.lag_minus_filing_days.median(),2)],
                 ["Earliest relative to filing", round(timing.lag_minus_filing_days.min(),2)],
                 ["Latest relative to filing", round(timing.lag_minus_filing_days.max(),2)],
             ]), "",
             "The replacement fundamentals panel selects each fact version known at 00:00 UTC on the row date. Intraday releases therefore enter a later anchor date. Date-only filings wait until the following UTC day. This is a daily research convention, not an exchange-calendar execution model. No future filing is joined; older-period restatements cannot displace newer fiscal periods. Raw vintages retain accession, source concept, unit, timestamp and snapshot hash.", "",
             "Undated synthetic quarter reconstructions, provider-derived facts, FX-converted records, missing units and ambiguous source timestamps are restricted to observation time. Quarterly cash flow may consequently have gaps in historical panels; annual cash flow remains separate. Values can remain stale until a newer usable period arrives, so inspect the per-feature period-end columns.", "",
             "## Remaining Gaps", "",
             "Symbols without reference facts: " + (", ".join(manifest["symbols_without_facts"]) or "None") + ".", "",
             "The new panel does not certify analyst estimates, ranks, prices or Defeat Beta ratios. Those sources remain outside this fundamentals validation. It uses the original panel's dates and saved historical membership intervals, so the panel's universe and market-data horizon are not extended to today's S&P 500. The versioned filing dataset extends independently. Current-profile CIKs are recorded, but ticker reuse and corporate reorganizations are not exhaustively resolved; only the documented overrides are applied.", "",
             "## Artifacts", "",
             "- `latest/`: all downloaded numeric facts, including snapshot-only values.",
             "- `vintages/`: per-fact versions and availability evidence.",
             "- `panel/`: replacement fundamentals features on original eligible research dates, with quarterly/annual values and availability/period-date columns.",
             "- `comparison/`: every mapped local cell and its reference comparison.",
             "- `timing/`: fixed-lag versus original filing-date comparisons.",
             "- `coverage.parquet`, `coverage_extensions.parquet`: issuer and statement-period coverage.",
             "- `assessment_manifest.json`, `report_summary.json`: machine-readable run totals.", "",
             "Read any populated Parquet directory with `pandas.read_parquet(path)`. Raw downloads and data artifacts are local and ignored by Git. API credentials are read from the environment and are not written into caches or manifests.", "",
             "## References", "",
             "- [StockFit API reference](https://api.stockfit.io/docs)",
             "- [StockFit filing and revision data model](https://developer.stockfit.io/blog/holy-grail-api-stock-backtesting)",
             "- [Identity override evidence](../config/stockfit_cik_overrides.json)", ""]
    validation_path = root / "validation.json"
    if validation_path.exists():
        validation = json.loads(validation_path.read_text())
        index = lines.index("## Artifacts")
        lines[index:index] = ["## Validation", "",
                             markdown_table(["Whole-panel check", "Result"], [[k, f"{v:,}"] for k, v in validation.items()]),
                             "", "The separate full-panel scan checks stored outputs, including every non-null feature's timing lineage. Unit tests cover revision reconstruction and boundary cases. These checks validate the adapter's rules; they do not establish completeness of the provider's original filing history.", ""]
    (root / "REPORT.md").write_text("\n".join(lines))
    print(json.dumps(summary, indent=2))
    print(root / "REPORT.md")


if __name__ == "__main__":
    main()
