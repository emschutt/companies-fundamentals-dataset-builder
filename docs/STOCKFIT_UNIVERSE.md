# Full-universe StockFit fundamentals

This is a standalone issuer dataset, not an extension of S&P membership history.
It removes the old 2017-2026 panel-date restriction and fetches every discoverable
StockFit stock issuer, including delisted companies. It does not require the
original DoltHub data. The older comparison pipeline remains available unchanged.

## Coverage

- Three statements: income, balance sheet, and cash flow.
- Annual and quarterly records kept separately, up to 80 periods per request.
- Every returned numeric statement field, including null revision tombstones.
- Latest values, reconstructed filing versions, and all-metric PIT feature events.
- Active issuers, OTC issuers, delisted issuers, and optionally known legacy CIKs.
- Native reported currencies and source units, without snapshot FX or split adjustments.

No arbitrary starting date is imposed. Historical depth is whatever the provider
returns, and varies by company, statement, metric and subscription. The API has
no documented offset/date pagination for these statement endpoints. Requests
returning 80 periods are recorded as possibly truncated, not complete histories.

The fiscal-period span is not the usable PIT span. An old comparative period
first disclosed years later becomes usable only at its supported disclosure time.
Observation-only facts cannot be backdated. This often makes usable history much
shorter than the oldest fiscal label.

## Download

Install the repository requirements and set `STOCKFIT_API_KEY` through your shell
or secret manager. Never add the token to source files or downloaded metadata.

```bash
python scripts/expand_stockfit.py \
  --snapshot data/stockfit/expanded-2026-10-23 \
  --legacy-snapshot data/stockfit/2026-10-23 \
  --legacy-snapshot data/stockfit/2026-10-23-identities
```

Omit the two optional legacy paths for a clean standalone run. Their company
profiles supply known CIKs that may no longer appear in the provider directory.
They do not relabel one issuer's history as another issuer's history.

The discovery stage paginates both active/delisted stock lists and the separate
delisted-company directory. Original profiles and directory-response hashes are
retained in `universe.json`. Multiple share classes and duplicate directory
entries are deduplicated by CIK before downloading. Requests use CIK, never a
potentially reused ticker. An issuer with no current ticker is still included.

Default pacing is six requests per second with 16 concurrent workers. There are
six statement requests per issuer. The full universe is a multi-hour download;
budget disk space and API quota accordingly. `--requests-per-second`, `--workers`
and `--min-free-gb` are configurable. The default free-disk reserve is 15 GiB.
No paid plan upgrades or additional data purchases are performed.

Each response is cached atomically. Successful responses are reused on restart;
failed requests are retried. Rate limits use bounded retries. Authentication or
daily-quota failures stop the run. Inspect `download_summary.json` and individual
`status/` records for completeness. A completed request can legitimately contain
no financial records.

`--discover-only` creates the directory manifest without requesting statements.
`--max-issuers N` is an explicitly incomplete smoke test, not a full-universe run.
The discovered universe is frozen for that snapshot: use a new snapshot directory
for a refresh or changes to discovery inputs.

## Build

```bash
python scripts/build_stockfit_universe.py \
  --snapshot data/stockfit/expanded-2026-10-23 \
  --out-dir data/stockfit/universe-2026-10-23 --workers 4
```

Add `--follow` to process completed issuer downloads while the current downloader
is still running. This is an ordinary foreground process, not a scheduled job.
An incomplete offline build exits with code 2 and reports its partial coverage.
Rerun against the same snapshot/output to resume. Use a new output directory when
changing the source snapshot or normalization contract. Cached raw successes
are immutable inputs; do not manually edit a snapshot and reuse its old output.

Every issuer must have all six successful statement responses before it is built.
The builder validates each partition before writing its completion marker.
Errors are recorded rather than silently removing problematic issuers. The
original raw snapshots and original S&P panel are never overwritten.

After the build, independently reread and check all stored partitions:

```bash
python scripts/audit_stockfit_universe.py \
  --dataset data/stockfit/universe-2026-10-23 --workers 4
```

This writes `audit.json`, checks manifest/partition counts, and exercises six
historical query dates from 2005 through October 2026. It also reports how many
issuers actually have non-null, filing-traced values on each date.

## Outputs

| Path | Contents |
| --- | --- |
| `universe.json` | Observed company identities, profiles, aliases and discovery evidence |
| `latest/` | All latest normalized statement facts, including observation-only values |
| `vintages/` | All reconstructed versions, including older-period restatements |
| `events/` | Causal, newest-available-period feature changes for every metric |
| `completed/` | Per-issuer counts, input fingerprints and validation results |
| `coverage.parquet` | Per-issuer coverage and date ranges |
| `summary.json` | Complete/partial status, aggregate counts, yearly coverage and failures |
| `REPORT.md` | Human-readable coverage and quality report |

Parquet datasets are partitioned by `issuer_id`, formatted as `CIK0000320193`.
The `issuer_id` and `cik` columns identify the same issuer; neither is a ticker.
The old symbol-keyed comparison tables are deliberately separate.

Rows include statement, annual/quarter frequency, fiscal period, metric, value,
source unit, value unit, availability timestamp, quality class, accession,
source concept, retrieval time, and raw-response hash. Values are not comparable
across different currencies without an explicit, historically appropriate FX
conversion. No unproven ratios or reconstructed quarterly values are inserted
into history.

`events/` stores a step function instead of repeating values every trading day.
It selects the newest usable fiscal period for each issuer/statement/frequency/
metric at each information event. A late restatement of an old period cannot
displace a newer period. Use `vintages/` when you need revisions to every period,
not just the newest-period research feature. Null revisions remain tombstones.

## Historical snapshots

```bash
python scripts/query_stockfit.py \
  --dataset data/stockfit/universe-2026-10-23 \
  --as-of 2012-06-01 --ciks 320193 789019 \
  --filing-only --max-age-days 550 \
  --out-dir outputs/stockfit-2012-06-01
```

Omit `--ciks` for the full discovered universe. Use an explicit timestamp such
as `2026-10-23T14:30:00Z` for intraday decisions. A bare date means 00:00 UTC.
Exact acceptance timestamps are respected; date-only sources become available
at 00:00 UTC the next day. There is no exchange-calendar execution simulation.

The output is long-form Parquet: one selected row per issuer, statement,
frequency, and metric, with full lineage and `age_days`. Use a new output folder
for each query. `--max-age-days` optionally removes stale fiscal periods; no
arbitrary staleness cutoff is imposed by default. `--filing-only` excludes an
observation-only selected value, without resurrecting a superseded value.

```python
import pandas as pd

snapshot = pd.read_parquet("outputs/stockfit-2012-06-01")
revenue = snapshot.loc[
    snapshot.statement.eq("income-statement")
    & snapshot.period_kind.eq("annual")
    & snapshot.metric.eq("revenue")
]
print(revenue[["issuer_id", "value", "value_unit", "period_end", "available_at"]])
```

To sample another grid, call the same selector at each required date. There is
no need to rebuild the source history. Annual and quarterly fields must remain
separate when pivoting. Keep the units and availability columns.

## Safety and limitations

The rules from [the StockFit methodology](STOCKFIT.md#availability-rules) still
apply. Source-less calculations, ambiguous revisions and invalid timing are
observation-only. Future fiscal labels are excluded from feature events so they
cannot suppress an otherwise usable historical period; raw/latest facts remain
available for diagnosis.

Each build checks duplicate fact/version/event keys, traced availability between
period end and retrieval, required traced units/accessions, observation-only
availability at retrieval, and non-regressing selected fiscal periods. Unit
tests exercise historical selection, nulls, currency preservation, CIK deduplication,
pagination, separate frequencies, and complete issuer partitions.

These checks establish consistency with the adapter's rules, not an independent
audit of StockFit or SEC truth. This is broad filing coverage, not a certified
survivorship-free security master. Current sector labels, names and tickers are
observations only. Do not use today's roster as proof a security was investable
on a past date. Delisting dates can be missing or implausible, and post-delisting
filings are intentionally kept in the issuer archive. Historical index membership,
listing dates and corporate-action predecessor links require a separate layer.

The scope is standardized company statements. Prices, historical consensus,
ownership, filing text, fund holdings and uncurated custom XBRL concepts are not
part of this build. No synthetic early history is created to fill unavailable data.
Provider responses remain local and git-ignored; redistribution rights are not
assumed.

Sources: [StockFit API reference](https://api.stockfit.io/docs) and
[StockFit historical-data guidance](https://developer.stockfit.io/quant).
