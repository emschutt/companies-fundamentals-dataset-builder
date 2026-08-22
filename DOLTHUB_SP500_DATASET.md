# DoltHub S&P 500 Dataset

`scripts/build_dolthub_sp500_dataset.py` builds a survivorship-aware S&P 500
research dataset from public DoltHub data.

Sources:

- Historical S&P 500 membership:
  `https://github.com/fja05680/sp500`
- Daily prices:
  `https://www.dolthub.com/repositories/post-no-preference/stocks`
- Fundamentals and analyst-style estimates:
  `https://www.dolthub.com/repositories/post-no-preference/earnings`

Run the full build:

```bash
python3 scripts/build_dolthub_sp500_dataset.py --years 10 --skip-existing
```

Add yfinance OHLCV and optional DefeatBeta ratios:

```bash
python3 scripts/enrich_dolthub_sp500_prices.py --data-dir data/dolthub_sp500 --years 10 --skip-existing
```

Build the conservative point-in-time panel used by the backtests:

```bash
python3 scripts/build_pit_sp500_panel.py
```

This writes:

- `fundamentals_estimates_prices_panel_pit.parquet`
- `pit_panel_manifest.json`

The PIT panel is the preferred modeling input. It applies conservative
availability lags to statement data because DoltHub does not expose filing or
accepted timestamps: quarterly rows are available after 45 days and annual rows
after 90 days. EPS history is also delayed by 45 days. Analyst estimate rows are
kept only while their target period end is still in the future. DefeatBeta
features and precomputed `target_ret_*` columns are excluded from the PIT panel.

Run the leakage audit:

```bash
python3 scripts/audit_sp500_leakage.py \
  --panel-file data/dolthub_sp500/fundamentals_estimates_prices_panel_pit.parquet \
  --out-dir outputs/leakage_audit_sp500_pit
```

Run the leakage-aware learning-to-rank backtest:

```bash
python3 learning_to_rank_pit_backtest.py \
  --universe-size 300 \
  --long-top-n 25 \
  --catboost-iterations 100 \
  --out-dir outputs/sp500_ltr_pit
```

This trains only on prior rebalance groups, treats each rebalance date as a
ranking query, and ranks stocks by next-period cross-sectional return labels.
The default run writes predictions, strategy curves, ranking metrics, strategy
metrics, an equity plot, and a run manifest under `outputs/sp500_ltr_pit/`.

For the optional DefeatBeta ratio layer, create a local environment and run:

```bash
python3 -m venv .venv_defeatbeta
.venv_defeatbeta/bin/python -m pip install defeatbeta-api yfinance pyarrow pandas
.venv_defeatbeta/bin/python scripts/enrich_dolthub_sp500_prices.py --data-dir data/dolthub_sp500 --years 10 --skip-existing --include-defeatbeta
```

Run only the coverage map first:

```bash
python3 scripts/build_dolthub_sp500_dataset.py --years 10 --coverage-only
```

Outputs under `data/dolthub_sp500/`:

- `sp500_membership_intervals_10y.parquet`
- `sp500_candidate_coverage.parquet`
- `sp500_selected_coverage.parquet`
- `tables/*.parquet`
- `tables/yfinance_ohlcv.parquet`
- `tables/defeatbeta_ratios.parquet`
- `aligned_sp500_daily_panel.parquet`
- `fundamentals_estimates_prices_panel.parquet`
- `manifest.json`
- `price_enrichment_manifest.json`

The aligned panel keeps rows only when the ticker was inside the historical
S&P 500 membership interval. This reduces survivorship bias versus using only
today's constituents.

Important limitation: the DoltHub earnings tables expose statement period dates,
not filing dates. The original `fundamentals_estimates_prices_panel.parquet` is
therefore useful for exploratory factor research but not a perfect
point-in-time filing-date backtest. Use
`fundamentals_estimates_prices_panel_pit.parquet` for conservative backtests
unless a source with true filing/accepted timestamps is available.

Price and ratio enrichment is as-of aligned to the existing DoltHub panel dates.
The price layer includes OHLCV, adjusted close, trailing returns, rolling volume
and volatility, and forward return targets. Yahoo Finance has gaps for some
historical S&P 500 tickers that were acquired, renamed, or delisted; those gaps
are recorded in `price_enrichment_manifest.json`.
