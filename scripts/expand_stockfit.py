#!/usr/bin/env python3
"""Discover and download StockFit's active and delisted stock issuers by CIK."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil

from fetch_stockfit import Client, DownloadError, STATEMENTS


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def issuer_id(cik):
    number = int(cik)
    if number <= 0:
        raise ValueError("CIK must be positive")
    return f"CIK{number:010d}"


def discover(client, snapshot, legacy_snapshots=()):
    """Keep source profiles, but do not mistake today's roster for past membership."""
    path = snapshot / "universe.json"
    if path.exists():
        return json.loads(path.read_text())
    issuers, pages, excluded = {}, [], []
    for endpoint, filters in (("symbols", {"type": "stock", "status": "operating"}),
                              ("symbols", {"type": "stock", "status": "delisted"}),
                              ("company/delisted", {})):
        page, total_pages = 1, 1
        while page <= total_pages:
            envelope = client.get(endpoint, dict(filters, page=page, pageSize=1000), "_universe")
            payload = envelope["data"]
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise DownloadError("Unexpected universe response")
            total_pages = int(payload["totalPages"])
            if payload["page"] != page or total_pages > 1000:
                raise DownloadError("Invalid universe pagination")
            pages.append({"endpoint": endpoint, "filters": filters, "page": page,
                          "rows": len(payload["data"]), "totalResults": payload["totalResults"],
                          "retrieved_at": envelope["retrieved_at"], "sha256": envelope["sha256"]})
            for profile in payload["data"]:
                if profile.get("type") != "stock" or not profile.get("cik"):
                    excluded.append(profile)
                    continue
                key = issuer_id(profile["cik"])
                record = issuers.setdefault(key, {"issuer_id": key, "cik": int(profile["cik"]),
                                                 "profiles": [], "legacy_symbols": []})
                if profile not in record["profiles"]:
                    record["profiles"].append(profile)
            print(f"Universe: {endpoint} {filters} page {page}/{total_pages}; {len(issuers):,} issuers", flush=True)
            page += 1
    # Retain known legacy issuers even when absent from the current directory.
    for legacy in legacy_snapshots:
        for path in sorted((legacy / "raw").glob("*/details-*.json")):
            envelope = json.loads(path.read_text())
            profile = envelope["data"]
            if not profile.get("cik"):
                continue
            key = issuer_id(profile["cik"])
            record = issuers.setdefault(key, {"issuer_id": key, "cik": int(profile["cik"]),
                                             "profiles": [], "legacy_symbols": []})
            symbol = envelope.get("requested_symbol") or path.parent.name
            if symbol not in record["legacy_symbols"]:
                record["legacy_symbols"].append(symbol)
            if not record["profiles"]:
                record["profiles"].append(dict(profile, discovery="legacy_profile"))
    result = {"discovered_at": datetime.now(timezone.utc).isoformat(), "pages": pages,
              "issuers": sorted(issuers.values(), key=lambda r: r["issuer_id"]),
              "excluded_profiles": excluded,
              "notes": ["CIK-keyed company fundamentals, not a security master or historical investable universe.",
                        "Profiles, classifications and ticker aliases are observed metadata, never historical membership.",
                        "Legacy symbols identify prior download routes, not verified historic ticker intervals."]}
    write_json(snapshot / "universe.json", result)
    return result


def download_issuer(client, record, snapshot, min_free_gb):
    if shutil.disk_usage(snapshot).free < min_free_gb * 1024**3:
        client.stop.set()
        raise DownloadError("Disk reserve reached; successes remain resumable")
    results = []
    for statement in STATEMENTS:
        for kind in ("annual", "quarter"):
            params = {"cik": record["cik"], "period": kind, "limit": 80,
                      "currency": "original", "includeSourceDetails": "true"}
            if statement != "cash-flow-statement":
                params["splitAdjust"] = "false"
            result = {"endpoint": f"financials/{statement}", "period_kind": kind}
            try:
                envelope = client.get(result["endpoint"], params, record["issuer_id"])
                result.update(status="ok", rows=len(envelope["data"]),
                              retrieved_at=envelope["retrieved_at"], sha256=envelope["sha256"])
            except DownloadError as exc:
                result.update(status="error", error=str(exc))
            results.append(result)
    status = {"issuer_id": record["issuer_id"], "cik": record["cik"], "results": results,
              "complete": all(r["status"] == "ok" for r in results)}
    write_json(snapshot / "status" / f"{record['issuer_id']}.json", status)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--legacy-snapshot", type=Path, action="append", default=[])
    parser.add_argument("--discover-only", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--requests-per-second", type=float, default=6)
    parser.add_argument("--min-free-gb", type=float, default=15)
    parser.add_argument("--max-issuers", type=int, help="Explicit smoke-test subset, not a complete download")
    args = parser.parse_args()
    if args.workers < 1 or args.requests_per_second <= 0 or args.min_free_gb < 1:
        parser.error("workers/rate must be positive and min-free-gb at least 1")
    if args.max_issuers is not None and args.max_issuers < 1:
        parser.error("max-issuers must be positive")
    token = os.environ.get("STOCKFIT_API_KEY")
    if not token:
        parser.error("Set STOCKFIT_API_KEY in the environment")
    args.snapshot.mkdir(parents=True, exist_ok=True)
    client = Client(token, args.snapshot / "raw", args.requests_per_second)
    universe = discover(client, args.snapshot, args.legacy_snapshot)
    if args.discover_only:
        return 0
    issuers = universe["issuers"]
    # Prioritize the existing research universe, then deterministically fetch all others.
    issuers = sorted(issuers, key=lambda r: (not bool(r["legacy_symbols"]), r["issuer_id"]))
    if args.max_issuers:
        issuers = issuers[:args.max_issuers]
    completed, failures = 0, 0
    started = datetime.now(timezone.utc).isoformat()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = [pool.submit(download_issuer, client, r, args.snapshot, args.min_free_gb) for r in issuers]
        for future in as_completed(pending):
            try:
                status = future.result()
                failures += sum(r["status"] != "ok" for r in status["results"])
            except Exception as exc:
                client.stop.set()
                failures += 1
                print(f"Download stopped: {type(exc).__name__}", flush=True)
            completed += 1
            if completed == 1 or completed % 100 == 0 or completed == len(issuers):
                summary = {"started_at": started, "updated_at": datetime.now(timezone.utc).isoformat(),
                           "universe_issuers": len(universe["issuers"]), "requested_issuers": len(issuers),
                           "completed_issuers": completed, "endpoint_failures": failures,
                           "currency": "original", "limit": 80, "stopped": client.stop.is_set(),
                           "full_universe_requested": len(issuers) == len(universe["issuers"])}
                write_json(args.snapshot / "download_summary.json", summary)
                print(json.dumps(summary), flush=True)
            if client.stop.is_set():
                for task in pending:
                    task.cancel()
                break
    write_json(args.snapshot / "download_summary.json", {
        "started_at": started, "updated_at": datetime.now(timezone.utc).isoformat(),
        "universe_issuers": len(universe["issuers"]), "requested_issuers": len(issuers),
        "completed_issuers": completed, "endpoint_failures": failures,
        "currency": "original", "limit": 80, "stopped": client.stop.is_set(),
        "full_universe_requested": len(issuers) == len(universe["issuers"]),
    })
    return 2 if failures or client.stop.is_set() else 0


if __name__ == "__main__":
    raise SystemExit(main())
