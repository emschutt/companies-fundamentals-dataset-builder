#!/usr/bin/env python3
"""Build all-metric filing histories and PIT feature events, without S&P/date caps."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import pandas as pd

from assess_stockfit import fact_frame
from expand_stockfit import write_json
from stockfit_pit import normalize_period, select_fact_events

CONTRACT_VERSION = 3
KEY = ["issuer_id", "statement", "period_kind", "period_end", "metric"]
FEATURE = ["issuer_id", "statement", "period_kind", "metric"]


def atomic_parquet(frame, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def feature_events(versions):
    """Compact step functions: all metrics, all dates, with full source lineage."""
    if versions.empty:
        return versions.copy()
    # An impossible future fiscal label must not evict an otherwise usable period.
    period = pd.to_datetime(versions.period_end, utc=True, errors="coerce")
    available = pd.to_datetime(versions.available_at, utc=True, format="mixed")
    usable = versions.loc[period.notna() & period.le(available)]
    groups = [select_fact_events(group) for _, group in usable.groupby(FEATURE)]
    return pd.concat(groups, ignore_index=True) if groups else versions.iloc[:0].copy()


def validate_frames(latest, versions, events):
    failures = Counter()
    failures["duplicate_latest_keys"] = int(latest.duplicated(KEY).sum())
    failures["duplicate_version_keys"] = int(versions.duplicated(KEY + ["available_at", "accession"]).sum())
    failures["duplicate_feature_event_keys"] = int(events.duplicated(FEATURE + ["available_at"]).sum())
    if len(versions):
        available = pd.to_datetime(versions.available_at, utc=True, format="mixed")
        retrieved = pd.to_datetime(versions.retrieved_at, utc=True, format="mixed")
        period = pd.to_datetime(versions.period_end, utc=True, errors="coerce")
        traced = versions.pit_status.eq("filing_traced")
        failures["invalid_traced_timing"] = int((traced & (available.lt(period) | available.gt(retrieved) |
                                                                      period.isna() | available.isna())).sum())
        failures["missing_traced_provenance"] = int((traced & (versions.accession.isna() | versions.value_unit.isna())).sum())
        failures["backdated_observations"] = int((~traced & available.ne(retrieved)).sum())
    if len(events):
        when = pd.to_datetime(events.available_at, utc=True, format="mixed")
        end = pd.to_datetime(events.period_end, utc=True)
        failures["future_event_period"] = int(end.gt(when).sum())
        ordered = events.assign(_time=when).sort_values(FEATURE + ["_time"])
        failures["regressing_selected_period"] = int(ordered.groupby(FEATURE).period_end
                                                       .transform(lambda s: s.lt(s.shift())).sum())
    if any(failures.values()):
        raise ValueError(f"Validation failed: {dict(failures)}")
    return dict(failures)


def build_issuer(snapshot, output, record):
    identity = record["issuer_id"]
    status = json.loads((snapshot / "status" / f"{identity}.json").read_text())
    if not status["complete"]:
        raise ValueError("Incomplete statement downloads")
    paths = sorted((snapshot / "raw" / identity).glob("*.json"))
    if len(paths) != 6:
        raise ValueError(f"Expected exactly six statement responses, got {len(paths)}")
    latest_rows, version_rows, fingerprint, request_keys, capped = [], [], hashlib.sha256(), set(), []
    source_min, source_max = None, None
    for path in paths:
        raw = path.read_bytes()
        fingerprint.update(raw)
        envelope = json.loads(raw)
        params = envelope["params"]
        if params.get("cik") != record["cik"] or params.get("currency") != "original" or params.get("limit") != 80:
            raise ValueError("Mixed issuer, currency or depth configuration")
        request_key = (envelope["endpoint"], params["period"])
        if request_key in request_keys:
            raise ValueError("Duplicate statement request")
        request_keys.add(request_key)
        if len(envelope["data"]) >= 80:
            capped.append("/".join(request_key))
        retrieved = envelope["retrieved_at"]
        source_min = min(source_min, retrieved) if source_min else retrieved
        source_max = max(source_max, retrieved) if source_max else retrieved
        for period in envelope["data"]:
            latest, versions = normalize_period(period, envelope, record["cik"])
            latest_rows.extend(latest)
            version_rows.extend(versions)
    expected = {(f"financials/{s}", k) for s in ("income-statement", "balance-sheet", "cash-flow-statement")
                for k in ("annual", "quarter")}
    if request_keys != expected:
        raise ValueError("Incomplete statement request set")
    latest = fact_frame(latest_rows).rename(columns={"act_symbol": "issuer_id"})
    versions = fact_frame(version_rows).rename(columns={"act_symbol": "issuer_id"})
    if len(latest) and not latest.issuer_id.eq(identity).all():
        raise ValueError("Issuer cache identity mismatch")
    events = feature_events(versions)
    validation = validate_frames(latest, versions, events)
    for name, frame in (("latest", latest), ("vintages", versions), ("events", events)):
        atomic_parquet(frame, output / name / f"{identity}.parquet")
    traced = versions.loc[versions.pit_status.eq("filing_traced") & versions.value.notna()]
    metrics = sorted(set(zip(latest.statement, latest.metric)))
    result = {"issuer_id": identity, "cik": record["cik"], "contract_version": CONTRACT_VERSION,
              "input_sha256": fingerprint.hexdigest(), "latest_facts": len(latest),
              "nonnull_latest_facts": int(latest.value.notna().sum()), "vintage_rows": len(versions),
              "feature_events": len(events), "quality_counts": dict(Counter(latest.pit_status)),
              "period_min": latest.period_end.min() if len(latest) else None,
              "period_max": latest.period_end.max() if len(latest) else None,
              "traced_available_min": traced.available_at.min() if len(traced) else None,
              "traced_available_max": traced.available_at.max() if len(traced) else None,
              "retrieved_min": source_min, "retrieved_max": source_max,
              "metrics": metrics, "capped_requests": capped, "validation": validation,
              "fiscal_year_counts": {str(k): int(v) for k, v in latest.groupby("fiscal_year").size().items()},
              "availability_year_counts": {str(k): int(v) for k, v in traced.groupby(traced.available_at.str[:4]).size().items()}}
    # This marker is written last, after all three validated partitions are durable.
    write_json(output / "completed" / f"{identity}.json", result)
    return result


def summarize(snapshot, output, universe, errors):
    records = [json.loads(p.read_text()) for p in sorted((output / "completed").glob("*.json"))]
    quality, validation, yearly, availability = Counter(), Counter(), Counter(), Counter()
    metrics = set()
    for row in records:
        quality.update(row["quality_counts"])
        validation.update(row["validation"])
        yearly.update(row["fiscal_year_counts"])
        availability.update(row["availability_year_counts"])
        metrics.update(tuple(m) for m in row["metrics"])
    def extreme(key, function):
        values = [r[key] for r in records if r.get(key)]
        return function(values) if values else None
    summary = {"built_at": datetime.now(timezone.utc).isoformat(), "universe_issuers": len(universe["issuers"]),
               "processed_issuers": len(records), "issuers_with_facts": sum(r["latest_facts"] > 0 for r in records),
               "issuers_without_facts": [r["issuer_id"] for r in records if not r["latest_facts"]],
               "latest_facts": sum(r["latest_facts"] for r in records),
               "nonnull_latest_facts": sum(r["nonnull_latest_facts"] for r in records),
               "vintage_rows": sum(r["vintage_rows"] for r in records),
               "feature_events": sum(r["feature_events"] for r in records),
               "metric_pairs": len(metrics), "metrics": sorted(metrics), "quality_counts": dict(quality),
               "period_min": extreme("period_min", min), "period_max": extreme("period_max", max),
               "traced_available_min": extreme("traced_available_min", min),
               "traced_available_max": extreme("traced_available_max", max),
               "retrieved_min": extreme("retrieved_min", min), "retrieved_max": extreme("retrieved_max", max),
               "requests_at_80_period_cap": sum(len(r["capped_requests"]) for r in records),
               "fiscal_year_fact_counts": dict(sorted(yearly.items())),
               "availability_year_version_counts": dict(sorted(availability.items())),
               "validation": dict(validation), "build_errors": errors,
               "complete": len(records) == len(universe["issuers"]) and not errors}
    write_json(output / "summary.json", summary)
    if records:
        coverage = pd.DataFrame([{k: v for k, v in r.items() if k not in (
            "metrics", "validation", "quality_counts", "capped_requests", "fiscal_year_counts", "availability_year_counts")}
                                for r in records])
        atomic_parquet(coverage, output / "coverage.parquet")
    text = ["# StockFit expanded issuer dataset", "", f"Built: {summary['built_at']}", "",
            "| Measure | Value |", "| --- | --- |"]
    for label, key in (("Discovered issuers", "universe_issuers"), ("Processed issuers", "processed_issuers"),
                       ("Issuers with facts", "issuers_with_facts"), ("Latest facts", "latest_facts"),
                       ("Filing-version rows", "vintage_rows"), ("PIT feature events", "feature_events"),
                       ("Statement/metric pairs", "metric_pairs"), ("Earliest fiscal period", "period_min"),
                       ("Latest fiscal period", "period_max"), ("First traced disclosure", "traced_available_min"),
                       ("Latest traced disclosure", "traced_available_max"),
                       ("Requests reaching 80-period cap", "requests_at_80_period_cap")):
        value = summary[key]
        text.append(f"| {label} | {value:,} |" if isinstance(value, int) else f"| {label} | {value} |")
    text += ["", "## Interpretation", "",
             "All three statements, annual and quarterly, in native currency. No fiscal-date or S&P membership filter.",
             "`events/` contains all-metric, full-window causal feature histories; use the as-of query for dated snapshots.",
             "CIK is the join key. Current and former tickers are metadata, not historical membership evidence.",
             "Delisted companies are included, but historical universe completeness and tradability are not certified.",
             "Fiscal period range is not the usable PIT range: values enter history only at supported availability times.",
             "Observation-only facts remain unusable before collection. Native currencies must not be summed across issuers.",
             "80 periods is the documented maximum; capped responses may omit older history.",
             "No listing-date filter is imposed, so post-delisting issuer filings remain in the filing archive.",
             "No price, estimate or index-membership refresh is implied.", "", "## Quality", ""]
    text += [f"- {key}: {value:,}" for key, value in sorted(quality.items())]
    text += ["", f"Complete universe build: {summary['complete']}.", f"Build errors: {len(errors)}.",
             f"Validation violations: {sum(validation.values())}.", "",
             "These checks validate implemented timing and key constraints, not independent SEC accuracy.", "",
             "Source: [StockFit API reference](https://api.stockfit.io/docs)."]
    (output / "REPORT.md").write_text("\n".join(text) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--follow", action="store_true", help="Process completed downloads until this active download finishes")
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("workers must be 1..16")
    universe_path = args.snapshot / "universe.json"
    universe = json.loads(universe_path.read_text())
    config = {"snapshot": str(args.snapshot.resolve()), "contract_version": CONTRACT_VERSION,
              "universe_sha256": hashlib.sha256(universe_path.read_bytes()).hexdigest()}
    config_path = args.out_dir / "build_config.json"
    if config_path.exists() and json.loads(config_path.read_text()) != config:
        parser.error("Output belongs to a different snapshot or build contract; use a new directory")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_json(config_path, config)
    write_json(args.out_dir / "universe.json", universe)
    done = set()
    for path in (args.out_dir / "completed").glob("*.json"):
        if all((args.out_dir / folder / f"{path.stem}.parquet").exists() for folder in ("latest", "vintages", "events")):
            done.add(path.stem)
    errors = {}
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        while True:
            ready = []
            for row in universe["issuers"]:
                key = row["issuer_id"]
                status = args.snapshot / "status" / f"{key}.json"
                if key not in done and key not in errors and status.exists() and json.loads(status.read_text())["complete"]:
                    ready.append(row)
            futures = {pool.submit(build_issuer, args.snapshot, args.out_dir, r): r["issuer_id"] for r in ready}
            for future in as_completed(futures):
                key = futures[future]
                try:
                    future.result()
                    done.add(key)
                except Exception as exc:
                    errors[key] = f"{type(exc).__name__}: {exc}"
                    write_json(args.out_dir / "build_errors.json", errors)
                if len(done) % 100 == 0 or len(futures) < 10:
                    print(f"Built {len(done):,}/{len(universe['issuers']):,} issuers; {len(errors)} errors", flush=True)
            progress = {"updated_at": datetime.now(timezone.utc).isoformat(), "processed_issuers": len(done),
                        "universe_issuers": len(universe["issuers"]), "errors": errors}
            write_json(args.out_dir / "progress.json", progress)
            download_path = args.snapshot / "download_summary.json"
            download = json.loads(download_path.read_text()) if download_path.exists() else {}
            download_finished = download.get("completed_issuers") == download.get("requested_issuers") and bool(download)
            if not args.follow or len(done) + len(errors) == len(universe["issuers"]) or download.get("stopped"):
                break
            if download_finished and not ready:
                break
            if not ready:
                time.sleep(30)
    summary = summarize(args.snapshot, args.out_dir, universe, errors)
    print(json.dumps({k: v for k, v in summary.items() if k not in ("metrics", "issuers_without_facts")}, indent=2))
    return 0 if summary["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
