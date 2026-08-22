# Open Company Fundamentals Pipeline

This repo builds a research dataset for public-company fundamentals using open or low-cost sources.

The main goal is simple:

1. Download company fundamentals and analyst-style data from public sources.
2. Align everything by ticker and date.
3. Build a point-in-time-safe panel for modeling.
4. Keep the leakage assumptions explicit.

This repo is built around two sources mentioned in the thread:

- DoltHub `post-no-preference/earnings` and `post-no-preference/stocks`
- Defeat Beta for extra ratio-style features

The core point-in-time logic is:

- Use the earnings calendar when a direct future event date is available.
- Use conservative availability lags when filing timestamps are not available.
- Quarterly statement data becomes available after `45` days.
- Annual statement data becomes available after `90` days.

## What You Get

The pipeline produces two main outputs:

- `data/dolthub_sp500/fundamentals_estimates_prices_panel.parquet`
  This is the broad aligned panel. It is useful for exploration, feature checks, and coverage work.
- `data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet`
  This is the safer modeling panel. It applies conservative point-in-time rules and drops known risky fields.

It also writes manifests so you can inspect what was downloaded and what was built:

- `data/dolthub_sp500/manifest.json`
- `data/dolthub_sp500/price_enrichment_manifest.json`
- `data/dolthub_sp500/pit_panel_manifest.json`

## Repo Layout

- [scripts/build_dolthub_sp500_dataset.py](/Users/ems/Documents/ETFs/scripts/build_dolthub_sp500_dataset.py)
  Downloads and aligns open S&P 500 fundamentals, estimates, prices, and membership intervals from DoltHub.
- [scripts/enrich_dolthub_sp500_prices.py](/Users/ems/Documents/ETFs/scripts/enrich_dolthub_sp500_prices.py)
  Adds Yahoo Finance OHLCV and optional Defeat Beta features.
- [scripts/build_pit_sp500_panel.py](/Users/ems/Documents/ETFs/scripts/build_pit_sp500_panel.py)
  Builds the conservative point-in-time panel.
- [scripts/audit_sp500_leakage.py](/Users/ems/Documents/ETFs/scripts/audit_sp500_leakage.py)
  Audits leakage risk in the raw or PIT panels.
- [scripts/run_open_fundamentals_pipeline.py](/Users/ems/Documents/ETFs/scripts/run_open_fundamentals_pipeline.py)
  Convenience entrypoint that runs the full open-data pipeline in order.

## Install

Create a virtual environment, then install the core dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you also want the optional Defeat Beta layer:

```bash
pip install -r requirements-optional.txt
```

## Quick Start

Run the full open-data pipeline:

```bash
python3 scripts/run_open_fundamentals_pipeline.py --years 10 --include-defeatbeta
```

If you only want the safer point-in-time panel without the optional Defeat Beta layer:

```bash
python3 scripts/run_open_fundamentals_pipeline.py --years 10
```

## Step By Step

### 1. Build the open base dataset from DoltHub

```bash
python3 scripts/build_dolthub_sp500_dataset.py --years 10 --skip-existing
```

This writes:

- historical S&P 500 membership intervals
- local Parquet copies of the source tables
- `fundamentals_estimates_panel.parquet`
- `aligned_sp500_daily_panel.parquet` when price tables are included

### 2. Add market data and optional Defeat Beta features

```bash
python3 scripts/enrich_dolthub_sp500_prices.py --data-dir data/dolthub_sp500 --years 10 --skip-existing
```

Optional Defeat Beta:

```bash
python3 scripts/enrich_dolthub_sp500_prices.py --data-dir data/dolthub_sp500 --years 10 --skip-existing --include-defeatbeta
```

This step adds:

- Yahoo Finance OHLCV
- trailing return features
- rolling volume and volatility features
- optional Defeat Beta ratios
- forward return columns in the broad panel for research only

### 3. Build the conservative point-in-time panel

```bash
python3 scripts/build_pit_sp500_panel.py --data-dir data/dolthub_sp500
```

This step:

- applies `45`-day lag rules to quarterly statement rows
- applies `90`-day lag rules to annual statement rows
- keeps estimate rows only when the target period end is still in the future
- joins EPS history only after the quarter lag
- removes known risky columns such as forward targets and Defeat Beta fields

### 4. Audit leakage risk

```bash
python3 scripts/audit_sp500_leakage.py \
  --panel-file data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet \
  --out-dir outputs/leakage_audit_sp500_pit
```

## Data Logic

The most important detail in this repo is timing discipline.

The raw DoltHub statements expose fiscal period dates, but they do not provide full filing or accepted timestamps. That means a direct join on statement date is too optimistic for a real backtest.

To avoid that, the PIT builder uses a conservative rule:

- quarterly data is usable `45` days after the fiscal period end
- annual data is usable `90` days after the fiscal period end

For estimates:

- estimate rows are kept only while their target period end is still ahead of the observation date
- earnings calendar fields can be used as event timing anchors when available

This is not the same as having true filing timestamps, but it is much safer than treating fiscal period dates as if they were release dates.

## What To Expect

Expected behavior:

- the build can take time because several tables are downloaded and filtered locally
- some historical tickers will have incomplete Yahoo Finance coverage
- some optional Defeat Beta methods may fail for individual tickers
- all of those gaps should appear in the manifest files instead of failing silently

Expected output quality:

- good enough for exploratory factor research
- much safer for backtests once you switch to the PIT panel
- still not a perfect replacement for a premium vendor with real filing timestamps

## Optional Paid Data Paths

There are also scripts for paid or semi-paid alternatives:

- [scripts/build_sharadar_dataset.py](/Users/ems/Documents/ETFs/scripts/build_sharadar_dataset.py)
- [scripts/build_fmp_dataset.py](/Users/ems/Documents/ETFs/scripts/build_fmp_dataset.py)

Sharadar is the cleaner route when you have access to a proper bulk fundamentals feed. FMP is currently included as a fallback for smaller symbol lists.
