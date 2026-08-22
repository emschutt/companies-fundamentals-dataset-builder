# Companies Fundamentals Dataset Builder

Build an aligned public-company fundamentals dataset from open or low-cost sources and generate a conservative point-in-time panel for research workflows.

## Why this repo exists

Building a usable fundamentals dataset is often harder than the modeling itself.

The main problems are usually:

- finding sources with enough statement and estimate coverage
- aligning inconsistent schemas across providers
- handling ticker changes and index membership
- avoiding accidental timing leakage

This repo packages a practical pipeline around:

- DoltHub `post-no-preference/earnings`
- DoltHub `post-no-preference/stocks`
- optional Defeat Beta ratios for extra market and fundamental features

## What the pipeline does

1. Downloads open fundamentals, analyst-style estimates, prices, and S&P 500 membership history.
2. Filters the universe with historical membership intervals to reduce survivorship bias.
3. Aligns fundamentals, estimates, and market data by ticker and date.
4. Builds a conservative point-in-time panel.
5. Audits the final panel for leakage risk.

## Point-in-time logic

The key idea in this repo is simple:

- use direct event timing when a trustworthy date is available
- otherwise use conservative lag assumptions

For the open DoltHub statements, true filing timestamps are not available in the source tables used here. So the safer panel assumes:

- quarterly statement data becomes usable after `45` days
- annual statement data becomes usable after `90` days
- EPS history is delayed by the quarter lag
- estimate rows are kept only while their target period end is still in the future

That gives you something much safer than treating fiscal period dates as if they were publication dates.

## Repository structure

```text
.
├── README.md
├── requirements.txt
├── requirements-optional.txt
├── scripts/
│   ├── run_open_fundamentals_pipeline.py
│   ├── build_dolthub_sp500_dataset.py
│   ├── enrich_dolthub_sp500_prices.py
│   ├── build_pit_sp500_panel.py
│   ├── audit_sp500_leakage.py
│   └── build_fmp_dataset.py
└── DOLTHUB_SP500_DATASET.md
```

## Main scripts

- [scripts/run_open_fundamentals_pipeline.py](scripts/run_open_fundamentals_pipeline.py)
  Runs the open-data pipeline in the intended order.
- [scripts/build_dolthub_sp500_dataset.py](scripts/build_dolthub_sp500_dataset.py)
  Downloads and aligns the base open dataset.
- [scripts/enrich_dolthub_sp500_prices.py](scripts/enrich_dolthub_sp500_prices.py)
  Adds Yahoo Finance OHLCV and optional Defeat Beta features.
- [scripts/build_pit_sp500_panel.py](scripts/build_pit_sp500_panel.py)
  Builds the conservative point-in-time modeling panel.
- [scripts/audit_sp500_leakage.py](scripts/audit_sp500_leakage.py)
  Audits raw and final outputs for timing leakage.
- [scripts/build_fmp_dataset.py](scripts/build_fmp_dataset.py)
  Fallback path for smaller per-symbol dataset builds when the main open-data route is not enough.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional layer:

```bash
pip install -r requirements-optional.txt
```

Core dependencies:

- `pandas`
- `pyarrow`
- `requests`
- `yfinance`

Optional dependency:

- `defeatbeta-api`

## Quick start

Run the full open-data build:

```bash
python3 scripts/run_open_fundamentals_pipeline.py --years 10
```

Run the same build with optional Defeat Beta features:

```bash
python3 scripts/run_open_fundamentals_pipeline.py --years 10 --include-defeatbeta
```

Skip the final audit if you only want the dataset artifacts:

```bash
python3 scripts/run_open_fundamentals_pipeline.py --years 10 --skip-audit
```

## Step-by-step usage

### 1. Build the open base dataset

```bash
python3 scripts/build_dolthub_sp500_dataset.py --years 10 --skip-existing
```

This writes:

- historical S&P 500 membership intervals
- local Parquet copies of the source tables
- `fundamentals_estimates_panel.parquet`
- `aligned_sp500_daily_panel.parquet`
- `manifest.json`

### 2. Add market data and optional Defeat Beta features

```bash
python3 scripts/enrich_dolthub_sp500_prices.py \
  --data-dir data/dolthub_sp500 \
  --years 10 \
  --skip-existing
```

Optional Defeat Beta:

```bash
python3 scripts/enrich_dolthub_sp500_prices.py \
  --data-dir data/dolthub_sp500 \
  --years 10 \
  --skip-existing \
  --include-defeatbeta
```

This step adds:

- Yahoo Finance OHLCV
- trailing returns
- rolling volume and volatility
- forward return targets for research use
- optional Defeat Beta features

### 3. Build the conservative PIT panel

```bash
python3 scripts/build_pit_sp500_panel.py --data-dir data/dolthub_sp500
```

This writes:

- `data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet`
- `data/dolthub_sp500/pit_panel_manifest.json`

### 4. Audit for leakage

```bash
python3 scripts/audit_sp500_leakage.py \
  --panel-file data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet \
  --out-dir outputs/leakage_audit_sp500_pit
```

## Output files

The most important outputs are:

- `data/dolthub_sp500/fundamentals_estimates_prices_panel.parquet`
  Broad aligned panel for exploration and feature work.
- `data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet`
  Safer panel for modeling and backtests.
- `data/dolthub_sp500/manifest.json`
  Build metadata for the base dataset.
- `data/dolthub_sp500/price_enrichment_manifest.json`
  Coverage and error metadata for Yahoo Finance and Defeat Beta enrichment.
- `data/dolthub_sp500/pit_panel_manifest.json`
  Metadata for the PIT output.

## Data sources

### Open-data path

- DoltHub `post-no-preference/earnings`
- DoltHub `post-no-preference/stocks`
- historical S&P 500 membership from `fja05680/sp500`
- Yahoo Finance for OHLCV enrichment
- Defeat Beta for optional ratio-style features

### Fallback extension

- [scripts/build_fmp_dataset.py](scripts/build_fmp_dataset.py)

FMP is included as a fallback path for smaller symbol lists or narrower experiments.

## What to expect

This repo is useful for:

- factor research
- ranking models
- leakage-aware backtests
- building a public baseline before moving to premium vendors

Practical limitations:

- some delisted or renamed tickers will have incomplete Yahoo Finance coverage
- some Defeat Beta methods may fail for individual symbols
- the open-data path still relies on conservative lags instead of true filing timestamps
- this is safer than a naive join, but still not the same as a premium point-in-time vendor

## Related notes

- [DOLTHUB_SP500_DATASET.md](DOLTHUB_SP500_DATASET.md)

## License

No license file has been added yet. If you plan to make the repo broadly reusable, adding a license should be one of the next steps.
