#!/usr/bin/env python3
"""Run the open fundamentals dataset pipeline in the intended order."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the DoltHub -> enrichment -> PIT -> audit pipeline."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/dolthub_sp500"))
    parser.add_argument("--years", type=int, default=10)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--include-defeatbeta", action="store_true")
    parser.add_argument("--quarter-lag-days", type=int, default=45)
    parser.add_argument("--annual-lag-days", type=int, default=90)
    parser.add_argument(
        "--skip-audit",
        action="store_true",
        help="Build the PIT panel but skip the leakage audit step.",
    )
    return parser.parse_args()


def run_step(args: list[str]) -> None:
    print("$", " ".join(args), flush=True)
    subprocess.run(args, check=True)


def main() -> int:
    args = parse_args()

    build_cmd = [
        sys.executable,
        "scripts/build_dolthub_sp500_dataset.py",
        "--out-dir",
        str(args.data_dir),
        "--years",
        str(args.years),
    ]
    if args.skip_existing:
        build_cmd.append("--skip-existing")
    run_step(build_cmd)

    enrich_cmd = [
        sys.executable,
        "scripts/enrich_dolthub_sp500_prices.py",
        "--data-dir",
        str(args.data_dir),
        "--years",
        str(args.years),
    ]
    if args.skip_existing:
        enrich_cmd.append("--skip-existing")
    if args.include_defeatbeta:
        enrich_cmd.append("--include-defeatbeta")
    run_step(enrich_cmd)

    pit_cmd = [
        sys.executable,
        "scripts/build_pit_sp500_panel.py",
        "--data-dir",
        str(args.data_dir),
        "--quarter-lag-days",
        str(args.quarter_lag_days),
        "--annual-lag-days",
        str(args.annual_lag_days),
    ]
    run_step(pit_cmd)

    if not args.skip_audit:
        audit_cmd = [
            sys.executable,
            "scripts/audit_sp500_leakage.py",
            "--data-dir",
            str(args.data_dir),
            "--panel-file",
            str(args.data_dir / "fundamentals_estimates_prices_panel_pit.parquet"),
            "--out-dir",
            "outputs/leakage_audit_sp500_pit",
            "--quarter-lag-days",
            str(args.quarter_lag_days),
            "--annual-lag-days",
            str(args.annual_lag_days),
        ]
        run_step(audit_cmd)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
