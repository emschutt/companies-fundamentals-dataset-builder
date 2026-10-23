#!/usr/bin/env python3
"""Download reproducible StockFit snapshots without persisting credentials."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time

import pandas as pd
import requests

BASE_URL = "https://api.stockfit.io/v1/api"
STATEMENTS = ("income-statement", "balance-sheet", "cash-flow-statement")


class DownloadError(RuntimeError):
    pass


class Client:
    def __init__(self, token: str, root: Path, requests_per_second: float = 6):
        self.token, self.root = token, root
        self.interval = 1 / requests_per_second
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.stop = threading.Event()

    def get(self, endpoint: str, params: dict, cache_symbol: str | None = None) -> dict:
        identity = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True)
        digest = hashlib.sha256(identity.encode()).hexdigest()[:20]
        symbol = re.sub(r"[^A-Za-z0-9_.-]", "_", cache_symbol or params.get("symbol", str(params.get("cik", "lookup"))))
        path = self.root / symbol / f"{endpoint.rsplit('/', 1)[-1]}-{digest}.json"
        if path.exists():
            cached = json.loads(path.read_text())
            if cached["endpoint"] != endpoint or cached["params"] != params:
                raise DownloadError("Cache identity mismatch")
            return cached
        for attempt in range(5):
            if self.stop.is_set():
                raise DownloadError("Stopped after authentication or daily quota failure")
            with self.lock:
                delay = max(0, self.next_request - time.monotonic())
                self.next_request = max(time.monotonic(), self.next_request) + self.interval
            time.sleep(delay)
            try:
                response = requests.get(
                    f"{BASE_URL}/{endpoint}", params=params,
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=(10, 60), allow_redirects=False,
                )
            except requests.RequestException:
                if attempt == 4:
                    raise DownloadError("Network error after five attempts") from None
                time.sleep(2 ** attempt)
                continue
            if response.status_code in (401, 403):
                self.stop.set()
                raise DownloadError(f"HTTP {response.status_code}; check API access")
            if response.status_code == 429:
                if response.headers.get("X-RateLimit-Remaining-Day") == "0":
                    self.stop.set()
                    raise DownloadError("Daily request quota exhausted")
                try:
                    wait = float(response.headers.get("Retry-After", "60"))
                except ValueError:
                    wait = 60
                time.sleep(min(60, max(1, wait)))
                continue
            if response.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            if response.status_code != 200:
                raise DownloadError(f"HTTP {response.status_code}")
            try:
                payload = response.json()
            except ValueError:
                raise DownloadError("Response was not JSON") from None
            if endpoint.startswith("financials/") and not isinstance(payload, list):
                raise DownloadError("Unexpected financial statement response shape")
            if endpoint == "company/details" and not isinstance(payload, dict):
                raise DownloadError("Unexpected company response shape")
            envelope = {
                "endpoint": endpoint, "params": params,
                "requested_symbol": cache_symbol or params.get("symbol"),
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "sha256": hashlib.sha256(response.content).hexdigest(),
                "rate_limits": {k: v for k, v in response.headers.items()
                                if k.lower().startswith("x-ratelimit-")},
                "data": payload,
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(envelope, separators=(",", ":")))
            temporary.replace(path)
            return envelope
        raise DownloadError("Retry limit reached")


def fetch_symbol(client: Client, symbol: str, limit: int, cik: int | None = None) -> list[dict]:
    results = []
    identity = {"cik": cik} if cik is not None else {"symbol": symbol}
    jobs = [("company/details", identity)]
    for statement in STATEMENTS:
        for period in ("annual", "quarter"):
            params = {**identity, "period": period, "limit": limit,
                      "includeSourceDetails": "true", "currency": "USD"}
            if statement != "cash-flow-statement":
                params["splitAdjust"] = "false"
            jobs.append((f"financials/{statement}", params))
    for endpoint, params in jobs:
        record = {"symbol": symbol, "endpoint": endpoint, "period": params.get("period")}
        try:
            envelope = client.get(endpoint, params, cache_symbol=symbol)
            data = envelope["data"]
            record.update(status="ok", rows=len(data) if isinstance(data, list) else 1,
                          retrieved_at=envelope["retrieved_at"])
        except DownloadError as exc:
            record.update(status="error", error=str(exc))
        results.append(record)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--snapshot", type=Path, required=True,
                        help="Use a new directory for each refresh; existing successes resume offline.")
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--cik-map", type=Path,
                        help="Verified JSON mapping of requested tickers to SEC CIK integers")
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--requests-per-second", type=float, default=6)
    args = parser.parse_args()
    if not 1 <= args.limit <= 80 or args.workers < 1 or args.requests_per_second <= 0:
        parser.error("limit must be 1..80; workers and request rate must be positive")
    token = os.environ.get("STOCKFIT_API_KEY")
    if not token:
        parser.error("Set STOCKFIT_API_KEY in the environment")
    if args.symbols:
        symbols = sorted({s.upper() for s in args.symbols})
    else:
        symbols = sorted(pd.read_parquet(args.data_dir / "sp500_selected_intervals.parquet")
                         ["act_symbol"].dropna().unique())
    args.snapshot.mkdir(parents=True, exist_ok=True)
    client = Client(token, args.snapshot / "raw", args.requests_per_second)
    cik_map = json.loads(args.cik_map.read_text()) if args.cik_map else {}
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(fetch_symbol, client, s, args.limit,
                               cik_map[s]["cik"] if isinstance(cik_map.get(s), dict) else cik_map.get(s)): s
                   for s in symbols}
        for number, future in enumerate(as_completed(pending), 1):
            results.extend(future.result())
            manifest = {"requested_symbols": symbols, "cik_map": cik_map, "limit": args.limit, "results": results,
                        "completed_symbols": number, "updated_at": datetime.now(timezone.utc).isoformat()}
            (args.snapshot / "download_manifest.json").write_text(json.dumps(manifest, indent=2))
            if number == 1 or number % 20 == 0 or number == len(symbols):
                failures = sum(r["status"] == "error" for r in results)
                print(f"Downloaded {number}/{len(symbols)} symbols; {failures} endpoint failures", flush=True)
    return 2 if any(r["status"] == "error" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
