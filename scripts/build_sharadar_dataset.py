#!/usr/bin/env python3
"""Download and prepare Sharadar company price/fundamental data.

The script keeps raw Sharadar zip files and converts each CSV payload to
Parquet so the data is practical for factor research and ML workflows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests


BASE_URL = "https://api.sharadar.com/v1.0"
DEFAULT_TABLES = (
    "tickers",
    "descriptions",
    "fundamentals",
    "stocks",
    "daily",
    "metrics",
    "sp500",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a 10-year Sharadar dataset for factor and ML modeling."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/sharadar"),
        help="Destination directory for raw zips, schemas, parquet files, and manifest.",
    )
    parser.add_argument(
        "--years",
        default="10",
        choices=("5", "10", "full"),
        help="Sharadar bulk history length.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=list(DEFAULT_TABLES),
        help="Sharadar tables to fetch. Defaults omit corporate actions/events.",
    )
    parser.add_argument(
        "--api-key-env",
        default="SHARADAR_API_KEY",
        help="Environment variable containing the Sharadar API key.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing raw zips and parquet outputs when present.",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download raw zips and schemas but do not convert CSV payloads to Parquet.",
    )
    parser.add_argument(
        "--status-only",
        action="store_true",
        help="Only print table status metadata from Sharadar.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "x-api-key": api_key,
            "User-Agent": "etfs-sharadar-dataset-builder/1.0",
        }
    )
    return session


def request_json(session: requests.Session, url: str, params: dict[str, str]) -> dict:
    response = session.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def get_status(session: requests.Session, table: str) -> dict:
    return request_json(session, f"{BASE_URL}/data/{table}", {"status": "True"})


def save_schema(session: requests.Session, table: str, schema_dir: Path) -> Path:
    schema_dir.mkdir(parents=True, exist_ok=True)
    path = schema_dir / f"{table}.sqlite.sql"
    response = session.get(
        f"{BASE_URL}/schema/{table}",
        params={"format": "sqlite"},
        timeout=60,
    )
    response.raise_for_status()
    path.write_text(response.text, encoding="utf-8")
    return path


def download_zip(
    session: requests.Session,
    table: str,
    years: str,
    raw_dir: Path,
    skip_existing: bool,
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    history_label = "full" if years == "full" else f"{years}y"
    path = raw_dir / f"{table}_{history_label}.csv.zip"
    if skip_existing and path.exists() and path.stat().st_size > 0:
        print(f"[{table}] using existing raw zip: {path}")
        return path

    tmp_path = path.with_suffix(path.suffix + ".part")
    if tmp_path.exists():
        tmp_path.unlink()

    print(f"[{table}] downloading {history_label} bulk zip...")
    with session.get(
        f"{BASE_URL}/data/{table}",
        params={"years": years},
        stream=True,
        timeout=(30, 300),
        allow_redirects=True,
    ) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    tmp_path.replace(path)
    print(f"[{table}] saved raw zip: {path} ({path.stat().st_size:,} bytes)")
    return path


def choose_bulk_years(table_status: dict, requested_years: str) -> str:
    files = table_status.get("files")
    if not isinstance(files, list):
        return requested_years

    available = {
        str(item.get("history")) for item in files if item.get("available") is True
    }
    if requested_years in available:
        return requested_years
    if "full" in available:
        return "full"
    return requested_years


def csv_member(zip_path: Path) -> str:
    with ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"Expected exactly one CSV in {zip_path}, found {members}")
    return members[0]


def zip_csv_to_parquet(zip_path: Path, parquet_path: Path, skip_existing: bool) -> dict:
    if skip_existing and parquet_path.exists() and parquet_path.stat().st_size > 0:
        print(f"[{zip_path.stem}] using existing parquet: {parquet_path}")
        return {"rows": None, "parquet_path": str(parquet_path)}

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = parquet_path.with_suffix(parquet_path.suffix + ".part")
    if tmp_path.exists():
        tmp_path.unlink()

    member = csv_member(zip_path)
    rows = 0
    writer = None
    read_options = pacsv.ReadOptions(block_size=32 * 1024 * 1024)
    convert_options = pacsv.ConvertOptions(strings_can_be_null=True)

    print(f"[{zip_path.stem}] converting {member} to parquet...")
    try:
        with ZipFile(zip_path) as archive:
            with archive.open(member) as csv_file:
                reader = pacsv.open_csv(
                    csv_file,
                    read_options=read_options,
                    convert_options=convert_options,
                )
                for batch in reader:
                    table = pa.Table.from_batches([batch])
                    if writer is None:
                        writer = pq.ParquetWriter(
                            tmp_path,
                            table.schema,
                            compression="zstd",
                            use_dictionary=True,
                        )
                    writer.write_table(table)
                    rows += table.num_rows
    finally:
        if writer is not None:
            writer.close()

    tmp_path.replace(parquet_path)
    print(f"[{zip_path.stem}] wrote {rows:,} rows to {parquet_path}")
    return {"rows": rows, "parquet_path": str(parquet_path)}


def write_manifest(out_dir: Path, manifest: dict) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return path


def build_dataset(args: argparse.Namespace) -> int:
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        print(f"Missing API key. Set {args.api_key_env} before running.", file=sys.stderr)
        return 2

    session = make_session(api_key)
    out_dir = args.out_dir
    raw_dir = out_dir / "raw"
    schema_dir = out_dir / "schemas"
    parquet_dir = out_dir / "parquet"

    manifest = {
        "created_at": utc_now(),
        "source": "Sharadar Direct API",
        "base_url": BASE_URL,
        "years": args.years,
        "tables": {},
        "notes": [
            "Corporate actions and material events are intentionally excluded.",
            "Raw API archives are retained under raw/; modeling-ready Parquet lives under parquet/.",
        ],
    }

    for table in args.tables:
        started = time.time()
        print(f"\n=== {table} ===")
        table_meta: dict = {"started_at": utc_now()}

        try:
            table_meta["status"] = get_status(session, table)
            print(json.dumps(table_meta["status"], indent=2, sort_keys=True))
            if args.status_only:
                manifest["tables"][table] = table_meta
                continue

            schema_path = save_schema(session, table, schema_dir)
            table_meta["schema_path"] = str(schema_path)

            download_years = choose_bulk_years(table_meta["status"], args.years)
            table_meta["download_years"] = download_years
            zip_path = download_zip(
                session=session,
                table=table,
                years=download_years,
                raw_dir=raw_dir,
                skip_existing=args.skip_existing,
            )
            table_meta["raw_zip_path"] = str(zip_path)
            table_meta["raw_zip_bytes"] = zip_path.stat().st_size

            if not args.download_only:
                parquet_path = parquet_dir / f"{table}.parquet"
                table_meta.update(
                    zip_csv_to_parquet(
                        zip_path=zip_path,
                        parquet_path=parquet_path,
                        skip_existing=args.skip_existing,
                    )
                )

            table_meta["finished_at"] = utc_now()
            table_meta["elapsed_seconds"] = round(time.time() - started, 2)
            manifest["tables"][table] = table_meta
            write_manifest(out_dir, manifest)
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None:
                detail = f"HTTP {response.status_code}: {response.text[:500]}"
            else:
                detail = str(exc)
            table_meta["error"] = detail
            manifest["tables"][table] = table_meta
            write_manifest(out_dir, manifest)
            print(f"[{table}] failed: {detail}", file=sys.stderr)
            if args.status_only:
                continue
            return 1

    manifest_path = write_manifest(out_dir, manifest)
    print(f"\nWrote manifest: {manifest_path}")
    return 0


def main() -> int:
    return build_dataset(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
