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
- optional StockFit statements with per-fact filing and revision provenance

## What the pipeline does

1. Downloads open fundamentals, analyst-style estimates, prices, and S&P 500 membership history.
2. Filters the universe with historical membership intervals to reduce survivorship bias.
3. Aligns fundamentals, estimates, and market data by ticker and date.
4. Builds a conservative point-in-time panel.
5. Audits the final panel for leakage risk.

## Point-in-time logic

There are two separate fundamentals paths: the original lag-based DoltHub panel
and a StockFit reference layer with filing versions. The original builder does
not use the earnings calendar to establish statement availability.

For the open DoltHub statements, true filing timestamps are not available in the source tables used here. So the safer panel assumes:

- quarterly statement data becomes usable after `45` days
- annual statement data becomes usable after `90` days
- EPS history is delayed by the quarter lag
- estimate rows are kept only while their target period end is still in the future

These are assumptions, not a point-in-time guarantee. Late filings can exceed
the lags, and a lag cannot remove revisions from a latest-value source. The
original panel also mixes quarterly and annual statement features. For validated
filing timing and separate frequencies, use the StockFit extension below.

## StockFit assessment and extension

The StockFit path compares existing fundamentals, downloads all available numeric
statement fields, and builds a separate versioned dataset. It preserves the raw
DoltHub tables. It requires an API key in `STOCKFIT_API_KEY`; do not put keys in
source files or commit downloaded data.

```bash
# Set STOCKFIT_API_KEY in your shell or secret manager first.
python scripts/fetch_stockfit.py \
  --snapshot data/stockfit/2026-10-23

# Recover the documented historical issuer mappings.
python scripts/fetch_stockfit.py \
  --snapshot data/stockfit/2026-10-23-identities \
  --symbols AET ANDV DWDP LLTC LVLT SCG STJ WFM XOM \
  --cik-map config/stockfit_cik_overrides.json

python scripts/assess_stockfit.py \
  --snapshot data/stockfit/2026-10-23 \
  --identity-snapshot data/stockfit/2026-10-23-identities \
  --out-dir data/stockfit/assessment-2026-10-23

python scripts/report_stockfit.py \
  --assessment data/stockfit/assessment-2026-10-23
```

Check every stored panel feature and filing version with
`python scripts/validate_stockfit.py --assessment data/stockfit/assessment-2026-10-23`.
Rerun the report command afterward to include these validation results.

Use a new snapshot directory for a refresh and a new assessment directory for
each build. Downloads resume from cached successes. Historical depth is limited
by the provider, plan and the endpoint's maximum of 80 periods per request.

Outputs include all normalized facts, filing versions, cell-level comparisons,
coverage gaps, a fundamentals-only panel and a generated `REPORT.md`. The panel
uses the existing research dates and membership intervals; the filing dataset
can extend beyond them. Calculated values without sufficient dated provenance
are retained for snapshot use and are not backdated into historical features.

See [StockFit methodology and usage](docs/STOCKFIT.md). Downloaded data,
assessment results, coverage reports and corrected/enriched datasets are private
local artifacts and are not published in this repository. The optional adapter
is code only; users supply their own authorized API access.

## Full-universe StockFit dataset

The standalone expansion removes the old S&P membership and research-date caps.
It discovers active and delisted stock issuers, deduplicates by SEC CIK, and
requests the maximum available annual and quarterly statement history. It keeps
all numeric statement fields, not just the original panel's 31 selected metrics.

```bash
python scripts/expand_stockfit.py --snapshot data/stockfit/expanded-2026-10-23

python scripts/build_stockfit_universe.py \
  --snapshot data/stockfit/expanded-2026-10-23 \
  --out-dir data/stockfit/universe-2026-10-23

python scripts/query_stockfit.py \
  --dataset data/stockfit/universe-2026-10-23 \
  --as-of 2012-06-01 --ciks 320193 789019 --filing-only \
  --out-dir outputs/stockfit-2012-06-01
```

The `events/` dataset is a compact, all-metric PIT feature history. The query
materializes values at an explicit decision time, with units, fiscal dates and
filing provenance. Native currencies are preserved; do not sum different
currencies. A date alone means 00:00 UTC. Historical prices and index membership
are not inferred from today's company list.

Downloads and issuer builds resume from completed partitions. The original
dataset remains unchanged. See [expanded dataset usage](docs/STOCKFIT_UNIVERSE.md)
for scope, full-history queries, limits, and validation.

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
- `aligned_sp500_daily_panel.parquet` when the builder also downloads prices
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
  Legacy lag-based panel. Availability and original filing vintages are unverified.
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
