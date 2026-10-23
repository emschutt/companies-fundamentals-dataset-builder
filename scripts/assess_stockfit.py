#!/usr/bin/env python3
"""Compare local DoltHub fundamentals with cached StockFit filing vintages."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pandas as pd

from stockfit_pit import normalize_period, timestamp, select_fact_events


# These are comparison hypotheses, not unconditional overwrite instructions.
MAPPINGS = {
    "income_statement": ("income-statement", {
        "sales": "revenue", "cost_of_goods": "costOfRevenue", "gross_profit": "grossProfit",
        "income_after_depreciation_and_amortization": "operatingIncome",
        "interest_expense": "interestExpense", "pretax_income": "incomeBeforeTax",
        "income_taxes": "incomeTaxExpense", "net_income": "netIncome",
        "average_shares": "weightedAverageSharesOutstandingDiluted", "diluted_net_eps": "epsDiluted",
    }),
    "balance_sheet_assets": ("balance-sheet", {
        "cash_and_equivalents": "cashAndShortTermInvestments", "receivables": "netReceivables",
        "inventories": "inventory", "total_current_assets": "currentAssets",
        "net_property_and_equipment": "propertyPlantAndEquipmentNet",
        "other_current_assets": "otherCurrentAssets", "total_assets": "assets",
    }),
    "balance_sheet_liabilities": ("balance-sheet", {
        "accounts_payable": "accountsPayable", "income_taxes_payable": "taxesPayable",
        "current_portion_long_term_debt": "longTermDebtCurrent", "long_term_debt": "longTermDebt",
        "total_current_liabilities": "currentLiabilities", "total_liabilities": "liabilities",
    }),
    "balance_sheet_equity": ("balance-sheet", {
        "retained_earnings": "retainedEarnings", "common_stock": "commonStock",
        "capital_surplus": "additionalPaidInCapital", "preferred_stock": "preferredStock",
        "total_equity": "stockholdersEquity", "shares_outstanding": "sharesOutstanding",
    }),
    "cash_flow_statement": ("cash-flow-statement", {
        "net_income": "netIncome", "depreciation_amortization_and_depletion": "depreciationAndAmortization",
        "net_cash_from_operating_activities": "operatingCashFlow",
        "net_cash_from_investing_activities": "investingCashFlow",
        "net_cash_from_financing_activities": "financingCashFlow",
        "property_and_equipment": "capitalExpenditure",
        "payment_of_dividends_and_other_distributions": "cashDividendsPaid",
        "cash_at_end_of_period": "endCash", "cash_at_beginning_of_period": "beginningCash",
        "net_change_in_cash_and_equivalents": "netChangeInCashIncludingFx",
        "effect_of_exchange_rate_changes": "effectOfExchangeRateChanges",
    }),
}

PANEL_METRICS = {
    "income-statement": ["revenue", "costOfRevenue", "grossProfit", "researchAndDevelopment",
                         "operatingIncome", "incomeBeforeTax", "incomeTaxExpense", "netIncome",
                         "netIncomeCommonStockholders", "epsDiluted", "weightedAverageSharesOutstandingDiluted"],
    "balance-sheet": ["assets", "currentAssets", "cash", "cashAndShortTermInvestments", "inventory",
                      "propertyPlantAndEquipmentNet", "liabilities", "currentLiabilities", "longTermDebt",
                      "stockholdersEquity", "retainedEarnings", "sharesOutstanding"],
    "cash-flow-statement": ["operatingCashFlow", "investingCashFlow", "financingCashFlow", "capitalExpenditure",
                            "stockBasedCompensation", "cashDividendsPaid", "commonStockRepurchased", "endCash"],
}
FACT_COLUMNS = ["act_symbol", "cik", "source", "statement", "period_kind", "period_end", "fiscal_year",
                "fiscal_period", "metric", "unit", "value_unit", "period_filed_at", "retrieved_at", "has_revisions",
                "snapshot_sha256", "value", "available_at", "availability_basis", "pit_status",
                "accession", "concept", "diagnostic"]


def fact_frame(records):
    frame = pd.DataFrame(records, columns=FACT_COLUMNS)
    for column in FACT_COLUMNS:
        if column not in ("fiscal_year", "value", "has_revisions"):
            frame[column] = frame[column].astype("string")
    frame["fiscal_year"] = frame["fiscal_year"].astype("Int64")
    frame["value"] = frame["value"].astype("float64")
    frame["has_revisions"] = frame["has_revisions"].astype("bool")
    return frame


def close(a, b, metric, rtol=.01):
    if pd.isna(a) or pd.isna(b):
        return False
    atol = .011 if metric.lower().startswith("eps") else 1000
    return abs(a - b) <= max(atol, rtol * max(abs(a), abs(b)))


def candidate_period(date, dates, tolerance_days=15):
    """Accept only a unique nearby fiscal date; never guess between candidates."""
    nearby = [d for d in dates if abs((pd.Timestamp(d) - pd.Timestamp(date)).days) <= tolerance_days]
    return nearby[0] if len(nearby) == 1 else None


def compare_symbol(symbol, tables, latest, versions):
    rows, timing = [], []
    lookup = {(r.statement, r.period_kind, r.period_end, r.metric): r for r in latest.itertuples()}
    period_lookup = {}
    for row in latest.itertuples():
        period_lookup.setdefault((row.statement, row.period_kind), {})[row.period_end] = row
    first = versions.sort_values("available_at").drop_duplicates(
        ["statement", "period_kind", "period_end", "metric"])
    first_lookup = {(r.statement, r.period_kind, r.period_end, r.metric): r for r in first.itertuples()}
    for table, (statement, mapping) in MAPPINGS.items():
        local = tables[table]
        local = local.loc[local.act_symbol.eq(symbol)]
        for record in local.to_dict("records"):
            kind = {"Year": "annual", "Quarter": "quarter"}.get(record["period"])
            if kind is None:
                continue
            date = pd.Timestamp(record["date"])
            dates = period_lookup.get((statement, kind), {})
            matched = candidate_period(date, dates)
            known = dates.get(matched)
            lag_at = date.tz_localize("UTC") + pd.Timedelta(days=90 if kind == "annual" else 45)
            filed_at = timestamp(known.period_filed_at) if known is not None and pd.notna(known.period_filed_at) else None
            if filed_at is not None:
                timing.append({"act_symbol": symbol, "table": table, "period_kind": kind,
                               "local_period_end": date.date().isoformat(), "stockfit_period_end": matched,
                               "assumed_available_at": lag_at.isoformat(), "filed_at": filed_at.isoformat(),
                               "lag_minus_filing_days": (lag_at - filed_at).total_seconds()/86400,
                               "lag_before_filing": bool(lag_at < filed_at)})
            for local_metric, metric in mapping.items():
                local_value = record.get(local_metric)
                ref = lookup.get((statement, kind, matched, metric))
                ref_value = ref.value if ref is not None else None
                status = "difference"
                if matched is None:
                    status = "no_reference_period"
                elif ref is None or pd.isna(ref_value):
                    status = "no_reference_value"
                elif "currency_conversion_or_native_fx" in ref.diagnostic:
                    status = "currency_unverified"
                elif pd.isna(local_value):
                    status = "local_missing"
                elif close(local_value, ref_value, metric):
                    status = "match"
                elif close(local_value, -ref_value, metric):
                    status = "sign_difference"
                elif metric in ("epsDiluted", "weightedAverageSharesOutstandingDiluted", "sharesOutstanding"):
                    status = "share_basis_or_value_difference"
                # Cumulative diagnosis is deliberately limited to cash-flow flows.
                if status == "difference" and table == "cash_flow_statement" and kind == "quarter" and metric not in ("endCash", "beginningCash"):
                    q = str(ref.fiscal_period) if ref is not None else ""
                    if q in ("Q2", "Q3", "Q4"):
                        parts = latest.loc[(latest.statement == statement) & (latest.period_kind == "quarter") &
                                           (latest.fiscal_year == ref.fiscal_year) & (latest.metric == metric) &
                                           latest.fiscal_period.isin([f"Q{i}" for i in range(1, int(q[-1])+1)])]
                        if len(parts) == int(q[-1]) and parts.fiscal_period.nunique() == len(parts) and parts.value.notna().all():
                            ytd = parts.value.sum()
                            if close(local_value, ytd, metric) or close(local_value, -ytd, metric):
                                status = "ytd_basis_match"
                initial = first_lookup.get((statement, kind, matched, metric))
                earliest_value = initial.value if initial is not None else None
                rows.append({"act_symbol": symbol, "table": table, "period_kind": kind,
                             "local_period_end": date.date().isoformat(), "stockfit_period_end": matched,
                             "local_metric": local_metric, "stockfit_metric": metric,
                             "local_value": local_value, "stockfit_latest_value": ref_value,
                             "stockfit_first_traced_value": earliest_value if initial is not None and initial.pit_status == "filing_traced" else None,
                             "status": status,
                             "period_offset_days": (pd.Timestamp(matched)-date).days if matched else None,
                             "reference_has_revisions": bool(ref.has_revisions) if ref is not None else False,
                             "reference_pit_status": ref.pit_status if ref is not None else None})
    comparison = pd.DataFrame(rows)
    if not comparison.empty:
        numeric_columns = {"local_value", "stockfit_latest_value", "stockfit_first_traced_value", "period_offset_days"}
        for col in comparison:
            if col in numeric_columns:
                comparison[col] = comparison[col].astype("float64")
            elif col != "reference_has_revisions":
                comparison[col] = comparison[col].astype("string")
    return comparison, pd.DataFrame(timing)


def panel_symbol(anchors, versions):
    """Separate quarter/annual features and retain availability and fiscal-date lineage."""
    output = anchors.sort_values("date").reset_index(drop=True).copy()
    decision = pd.DataFrame({"decision_at": pd.to_datetime(output.date, utc=True).astype("datetime64[ns, UTC]")})
    group = {(s, k, m): v for (s, k, m), v in versions.groupby(["statement", "period_kind", "metric"])}
    columns = {}
    for statement, metrics in PANEL_METRICS.items():
        for kind in ("quarter", "annual"):
            for metric in metrics:
                name = f"sf_{statement.replace('-', '_')}_{kind}_{metric}"
                features = group.get((statement, kind, metric))
                if features is None:
                    columns[name] = pd.Series(np.nan, index=output.index)
                    columns[name + "__available_at"] = pd.Series(pd.NaT, index=output.index, dtype="datetime64[ns, UTC]")
                    columns[name + "__period_end"] = pd.Series(pd.NaT, index=output.index, dtype="datetime64[ns]")
                    continue
                events = select_fact_events(features)[["available_at", "period_end", "value"]].copy()
                events["available_at"] = pd.to_datetime(events.available_at, utc=True, format="mixed").astype("datetime64[ns, UTC]")
                events["period_end"] = pd.to_datetime(events.period_end).astype("datetime64[ns]")
                joined = pd.merge_asof(decision, events.sort_values("available_at"),
                                       left_on="decision_at", right_on="available_at", direction="backward")
                columns[name] = joined.value
                columns[name + "__available_at"] = joined.available_at
                columns[name + "__period_end"] = joined.period_end
                if (joined.available_at > joined.decision_at).any():
                    raise AssertionError("Future fact entered historical panel")
    return pd.concat([output, pd.DataFrame(columns)], axis=1)


def membership_mask(anchors, intervals):
    mask = pd.Series(False, index=anchors.index)
    for row in intervals.itertuples():
        mask |= (anchors.act_symbol.eq(row.act_symbol) & anchors.date.ge(row.effective_start)
                 & anchors.date.le(row.effective_end))
    return mask


def build_parallel(args, symbols):
    """Independent offline shards use bounded memory and merge by unique symbol."""
    def build_shard(index, subset):
        destination = args.out_dir / "parts" / f"part-{index:02d}"
        log_path = args.out_dir / f"part-{index:02d}.log"
        command = [sys.executable, str(Path(__file__).resolve()), "--data-dir", str(args.data_dir),
                   "--snapshot", str(args.snapshot), "--out-dir", str(destination), "--workers", "1",
                   "--symbols", *subset]
        if args.identity_snapshot:
            command.extend(["--identity-snapshot", str(args.identity_snapshot)])
        if args.skip_panel:
            command.append("--skip-panel")
        with log_path.open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            raise RuntimeError(f"Assessment shard failed; inspect {log_path}")
        return destination

    shards = [symbols[i::args.workers] for i in range(args.workers)]
    completed = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(build_shard, i, s) for i, s in enumerate(shards) if s]
        for future in as_completed(futures):
            completed.append(future.result())
            print(f"Completed {len(completed)}/{len(futures)} assessment shards", flush=True)
    manifests = [json.loads((p / "assessment_manifest.json").read_text()) for p in completed]
    manifest = manifests[0].copy()
    counts = ["symbols_requested", "symbols_with_facts", "statement_periods_lag_before_filing",
              "statement_periods_with_filing_comparison", "panel_rows", "panel_rows_with_reference_values",
              "panel_nonnull_values", "latest_facts", "vintage_rows", "facts_with_revisions", "additional_metric_facts"]
    for key in counts:
        manifest[key] = sum(m.get(key, 0) for m in manifests)
    for key in ("comparison_counts", "quality_counts"):
        counter = Counter()
        for m in manifests:
            counter.update(m[key])
        manifest[key] = dict(counter)
    manifest["symbols_without_facts"] = sorted(s for m in manifests for s in m["symbols_without_facts"])
    manifest["built_at"] = datetime.now(timezone.utc).isoformat()
    manifest["workers"] = args.workers
    for folder in ("latest", "vintages", "comparison", "timing", "panel"):
        for part in completed:
            for path in (part / folder).glob("*.parquet"):
                target = args.out_dir / folder / path.name
                if target.exists():
                    raise ValueError(f"Overlapping shard output: {target}")
                shutil.move(path, target)
    coverage = pd.concat([pd.read_parquet(p / "coverage.parquet") for p in completed], ignore_index=True)
    coverage.sort_values("act_symbol").to_parquet(args.out_dir / "coverage.parquet", index=False)
    (args.out_dir / "assessment_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--identity-snapshot", type=Path,
                        help="Optional separately fetched, verified historical CIK overrides")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--skip-panel", action="store_true")
    parser.add_argument("--workers", type=int, default=4, help="Parallel offline build processes")
    args = parser.parse_args()
    if args.workers < 1 or args.workers > 16:
        parser.error("--workers must be between 1 and 16")
    if args.out_dir.exists():
        parser.error("Use a new --out-dir so results from different runs cannot be mixed")
    args.out_dir.mkdir(parents=True)
    for directory in ("latest", "vintages", "comparison", "timing", "panel"):
        (args.out_dir / directory).mkdir()
    if args.workers > 1:
        symbols = sorted(set(args.symbols or pd.read_parquet(args.data_dir / "sp500_selected_intervals.parquet").act_symbol.unique()))
        return build_parallel(args, symbols)
    tables = {n: pd.read_parquet(args.data_dir / "tables" / f"{n}.parquet") for n in MAPPINGS}
    anchors = pd.read_parquet(args.data_dir / "fundamentals_estimates_prices_panel_pit.parquet",
                              columns=["act_symbol", "date"]).drop_duplicates()
    intervals = pd.read_parquet(args.data_dir / "sp500_selected_intervals.parquet")
    in_universe = membership_mask(anchors, intervals)
    excluded_anchors = int((~in_universe).sum())
    anchors = anchors.loc[in_universe]
    symbols = sorted(set(args.symbols or intervals.act_symbol.unique()))
    overrides = {}
    if args.identity_snapshot:
        identity_manifest = json.loads((args.identity_snapshot / "download_manifest.json").read_text())
        overrides = {s: v for s, v in identity_manifest.get("cik_map", {}).items()
                     if s in identity_manifest["requested_symbols"]}
        for s in set(overrides) & set(symbols):
            successful = [r for r in identity_manifest["results"] if r["symbol"] == s and
                          r["status"] == "ok" and r["endpoint"].startswith("financials/")]
            if len(successful) != 6:
                raise ValueError(f"Historical CIK snapshot is incomplete for {s}")
    coverage, comparison_counts, quality_counts = [], Counter(), Counter()
    totals = Counter()
    mapped_reference = {(s, m) for s, mapping in MAPPINGS.values() for m in mapping.values()}
    for number, symbol in enumerate(symbols, 1):
        latest_records, version_records, envelopes = [], [], []
        cik, profile = None, {}
        paths = sorted((args.snapshot / "raw" / symbol).glob("*.json"))
        if symbol in overrides:
            paths += sorted((args.identity_snapshot / "raw" / symbol).glob("*.json"))
        for path in paths:
            envelope = json.loads(path.read_text())
            if envelope["endpoint"] == "company/details":
                if "symbol" in envelope["params"]:
                    profile = envelope["data"]
                    cik = profile.get("cik")
                elif not profile:
                    profile = envelope["data"]
                    cik = profile.get("cik")
            else:
                envelopes.append(envelope)
        seen = set()
        for envelope in envelopes:
            key = (envelope["endpoint"], envelope["params"]["period"], envelope["params"].get("cik"))
            if key in seen:
                raise ValueError(f"Multiple requests for {symbol}/{key}; use a single snapshot configuration")
            seen.add(key)
            for period in envelope["data"]:
                override = overrides.get(symbol)
                if override:
                    historical = "cik" in envelope["params"]
                    if historical != (period["period"] <= override["period_end_through"]):
                        continue
                latest_rows, vintage_rows = normalize_period(period, envelope, envelope["params"].get("cik", cik))
                latest_records.extend(latest_rows)
                version_records.extend(vintage_rows)
        latest = fact_frame(latest_records)
        versions = fact_frame(version_records)
        key = ["act_symbol", "statement", "period_kind", "period_end", "metric"]
        if latest.duplicated(key).any():
            raise ValueError(f"Duplicate reference fact keys for {symbol}")
        if not latest.empty:
            latest.to_parquet(args.out_dir / "latest" / f"{symbol}.parquet", index=False)
            versions.to_parquet(args.out_dir / "vintages" / f"{symbol}.parquet", index=False)
        comparison, timing = compare_symbol(symbol, tables, latest, versions)
        if not comparison.empty:
            comparison.to_parquet(args.out_dir / "comparison" / f"{symbol}.parquet", index=False)
            comparison_counts.update(comparison.status)
        if not timing.empty:
            timing.to_parquet(args.out_dir / "timing" / f"{symbol}.parquet", index=False)
            totals["statement_periods_lag_before_filing"] += int(timing.lag_before_filing.sum())
            totals["statement_periods_with_filing_comparison"] += len(timing)
        local_anchors = anchors.loc[anchors.act_symbol.eq(symbol)]
        if not args.skip_panel and not local_anchors.empty:
            panel = panel_symbol(local_anchors, versions)
            panel.to_parquet(args.out_dir / "panel" / f"{symbol}.parquet", index=False)
            values = [c for c in panel if c.startswith("sf_") and "__" not in c]
            totals["panel_rows"] += len(panel)
            totals["panel_rows_with_reference_values"] += int(panel[values].notna().any(axis=1).sum())
            totals["panel_nonnull_values"] += int(panel[values].notna().sum().sum())
        quality_counts.update(latest.pit_status)
        totals["latest_facts"] += len(latest)
        totals["vintage_rows"] += len(versions)
        totals["facts_with_revisions"] += int(latest.has_revisions.sum())
        totals["additional_metric_facts"] += sum((r.statement, r.metric) not in mapped_reference for r in latest.itertuples())
        coverage.append({"act_symbol": symbol, "cik": cik, "company": profile.get("name"),
                         "sector": profile.get("sector"), "returned_symbols": ",".join(profile.get("symbols", [])),
                         "statement_responses": len(envelopes), "latest_facts": len(latest),
                         "vintage_rows": len(versions), "period_min": latest.period_end.min() if len(latest) else None,
                         "period_max": latest.period_end.max() if len(latest) else None,
                         "traced_facts": int(latest.pit_status.eq("filing_traced").sum()),
                         "observed_only_facts": int(latest.pit_status.eq("observed_only").sum())})
        if number == 1 or number % 20 == 0 or number == len(symbols):
            print(f"Assessed {number}/{len(symbols)} symbols; {totals['latest_facts']:,} facts", flush=True)
    pd.DataFrame(coverage).to_parquet(args.out_dir / "coverage.parquet", index=False)
    manifest = {"built_at": datetime.now(timezone.utc).isoformat(), "snapshot": str(args.snapshot),
                "identity_snapshot": str(args.identity_snapshot) if args.identity_snapshot else None,
                "verified_cik_overrides": overrides,
                "source_data": str(args.data_dir), "symbols_requested": len(symbols),
                "symbols_with_facts": sum(c["latest_facts"] > 0 for c in coverage),
                "symbols_without_facts": [c["act_symbol"] for c in coverage if not c["latest_facts"]],
                "comparison_counts": dict(comparison_counts), "quality_counts": dict(quality_counts),
                "excluded_anchors_outside_saved_membership": excluded_anchors, **totals,
                "decision_time": "00:00 UTC on anchor date; same-day later filings are excluded",
                "date_tolerance_days": 15, "relative_value_tolerance": .01,
                "notes": ["StockFit is the reference, not independently certified SEC truth.",
                          "Latest-value comparisons do not establish historical equality.",
                          "Snapshot-only values are never backdated to original period filing dates.",
                          "Annual and quarterly features are separate; quarterly cash flow is not assumed to be YTD.",
                          "All raw source tables remain unchanged.",
                          "Ticker-to-issuer identity is reported from current profiles; historical ticker reuse is not resolved.",
                          "Panel anchors and membership end with the existing dataset; the filing dataset extends independently."]}
    (args.out_dir / "assessment_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
