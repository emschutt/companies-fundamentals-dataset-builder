# Sharadar Dataset

The dataset builder is `scripts/build_sharadar_dataset.py`. It downloads raw
Sharadar bulk CSV zip files, converts them to Parquet, saves SQLite schemas, and
writes a manifest with status, file sizes, paths, and row counts.

Run from the repository root:

```bash
export SHARADAR_API_KEY="..."
python3 scripts/build_sharadar_dataset.py --years 10 --skip-existing
```

Default tables:

- `tickers`: securities master and metadata
- `descriptions`: field-level indicator descriptions
- `fundamentals`: all company financial statement and fundamental indicators
- `stocks`: daily company OHLCV and adjusted prices
- `daily`: daily point-in-time fundamental ratios
- `metrics`: price-based metrics
- `sp500`: historical S&P 500 membership

Corporate actions and material corporate events are intentionally excluded.

Outputs are written under `data/sharadar/`, which is ignored by git:

- `raw/`: original Sharadar zip archives
- `parquet/`: modeling-ready Parquet files
- `schemas/`: table schemas
- `manifest.json`: build metadata

## FMP Fallback

`scripts/build_fmp_dataset.py` builds a per-symbol dataset from Financial
Modeling Prep when bulk Sharadar access is unavailable.

```bash
export FMP_API_KEY="..."
python3 scripts/build_fmp_dataset.py --tickers AAPL MSFT NVDA --years 10 --skip-existing
```

The current tested FMP key allows 10-year daily prices by ticker, but blocks
bulk/universe endpoints and caps statement history at 5 rows per symbol. The
aligned panel therefore works for selected symbols, but it is not a full
10-year, all-company factor dataset unless the API plan is upgraded.
