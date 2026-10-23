# StockFit reference layer

This extension downloads StockFit fundamentals, compares them with the existing
DoltHub tables, and builds a separate fundamentals panel using filing versions.
The original data is preserved. StockFit is the comparison reference; the adapter
does not independently certify all provider values against SEC filings.

## Requirements

Use Python 3.10+ and the repository's core requirements. The implementation uses
pandas 2.0+ and PyArrow. The live assessment was run with pandas 3.0.5.

Set `STOCKFIT_API_KEY` through your shell or secret manager. The program reads it
from the environment. It is never saved in downloaded files, output manifests,
or source code. Do not commit your key or account data.

The base dataset must already contain the five statement tables,
`sp500_selected_intervals.parquet`, and
`fundamentals_estimates_prices_panel_pit.parquet`. The last file provides only
symbol/date anchors for this build; its old features are not copied into the
new fundamentals panel.

## Download

```bash
python scripts/fetch_stockfit.py --snapshot data/stockfit/2026-10-23
```

The default universe is every symbol in the saved membership intervals. A smaller
run can use `--symbols AAPL MSFT JPM`. Each symbol makes seven requests: one
company profile and annual/quarterly income, balance-sheet and cash-flow data.
The request asks for 80 periods, the documented endpoint maximum. The API may
return fewer periods or no data, including for delisted symbols.

`--workers` controls concurrent symbols; `--requests-per-second` controls the
global request rate. Defaults are eight workers and six requests per second.
Set a lower rate if your account has a lower allowance. Retries are bounded;
authentication failures and exhausted daily quotas stop additional requests.
The downloader exits with code 2 if any endpoint failed, while preserving all
successful responses and recording failures in `download_manifest.json`.

Each cached response includes its request parameters, retrieval timestamp and
payload hash. Successful requests resume without network calls. Use a new dated
directory to refresh data; a cache directory is a snapshot, not a rolling feed.
The API token and authorization header are not included in the cache.

Requests use `includeSourceDetails=true`, `currency=USD`, and
`splitAdjust=false` where supported. Disabling automatic current-basis share
adjustments avoids mixing those adjustments with the as-filed revision trail.
Per-share values must still be interpreted on their filing-date share basis.

## Historical identifiers

A ticker is not a permanent issuer identifier. XOM, for example, resolved to a
new holding-company CIK in the October 2026 snapshot. Its predecessor holds most
of the historical statements. Some delisted tickers fail by symbol but work by
CIK.

`config/stockfit_cik_overrides.json` records verified historical issuer mappings,
period boundaries, and evidence. Fetch these separately, then supply the
identity snapshot to the assessment. The historical CIK is used through the
configured fiscal-period boundary, and the current symbol route after it.
An index-removal boundary is used for selected delisted issuers to exclude later
subsidiary-only reports. It is not represented as an exact corporate-action date.

Do not add mappings from similar names alone. Multiple businesses can share a
name, and a parent and subsidiary can have different reporting scopes. The
current override format supports one historical CIK per symbol; more complex
histories require an explicit extension.

## Build and compare

```bash
python scripts/assess_stockfit.py \
  --snapshot data/stockfit/2026-10-23 \
  --identity-snapshot data/stockfit/2026-10-23-identities \
  --out-dir data/stockfit/assessment-2026-10-23

python scripts/report_stockfit.py \
  --assessment data/stockfit/assessment-2026-10-23
```

Run `python scripts/validate_stockfit.py --assessment data/stockfit/assessment-2026-10-23`
to scan all stored panel cells and version keys. It writes `validation.json` and
exits with code 2 on a violation. Run the report again to include the scan results.

The identity snapshot is optional. Use `--skip-panel` for an assessment and
version tables without materializing the research panel. Use a new output
directory for every build; partial runs are not silently merged with old results.
Offline builds use four processes by default; use `--workers 1` to reduce CPU
and memory use. Per-process logs are retained in the output directory.

The normalized tables preserve every numeric field returned by the three
statement endpoints, including fields outside the comparison mapping. The
fundamentals panel selects 31 core metrics at each of two frequencies. Raw
source JSON preserves every other response field.

### Comparison rules

Match on requested symbol, statement type and annual/quarterly frequency. Accept
only a unique fiscal-date candidate within 15 days, and retain both date labels
and their offset. Some provider dates are normalized fiscal labels rather than
the literal end dates printed on filings. A common fiscal date is not enough to
prove issuer identity; the CIK override checks remain separate.

The explicit metric mapping is in `scripts/assess_stockfit.py`. It is a set of
comparison hypotheses, not instructions to overwrite raw data. The value
tolerance is 1% of the larger absolute value, with an absolute floor of 1,000
units or 0.011 for EPS. Zero is preserved; null is not converted to zero.

Statuses separate matches, unresolved differences, sign differences, share-basis
differences, local nulls and unavailable reference values/periods. A quarterly
comparison with FX-converted reference data is marked `currency_unverified`
and excluded from the match-rate denominator. The normalized `unit` field is
the source unit, while `value_unit` describes the returned amount after any
conversion. Both are retained because they can differ. A quarterly
cash-flow difference is classified as a YTD match only if all preceding fiscal
quarters are present and their sum matches, allowing the opposite sign. This
diagnosis does not relabel the raw table or certify every cash-flow row's basis.

### Availability rules

Each fact's version chain is reconstructed independently from its dated sources
and `before` values. The period's original filing date is not used to backdate a
fact first disclosed later. Source events with no change preserve the previous
value. A late revision of an older period cannot replace a newer fiscal period.

The daily panel's decision time is 00:00 UTC on each saved research date. A filing
later that day enters a subsequent anchor row. Date-only filings become usable
at 00:00 UTC the following day. Exact SEC acceptance timestamps are retained.
The panel is not a market-open or exchange-holiday execution simulation.

Facts with undated synthetic reconstructions, provider-derived values, incomplete
units, FX conversions, future/invalid source times, or ambiguous same-time source
events are classified `observed_only`. They are usable from the retrieval time
onward, and never from an assumed historical filing date. In particular, some
StockFit Q2/Q3 cash-flow and Q4 reconstructions cannot be fully reconstructed
historically from the normalized endpoint's provenance alone. They are retained
in `latest/`, while historical panels may have gaps or older usable values.

Quarterly and annual features are separate. No quarterly/YTD conversion is
guessed for the original source. No current-basis EPS adjustment is silently
applied. No derived ratio lacking vintage inputs is inserted into history.

## Outputs

| Path | Contents |
| --- | --- |
| `latest/` | Latest normalized numeric facts, including observation-only facts |
| `vintages/` | Versions with CIK, accession, concept, unit, availability and snapshot hash |
| `panel/` | Core fundamentals on the saved research dates, with availability and period lineage |
| `comparison/` | Local values, latest reference values, first traced values, and status |
| `timing/` | Fixed-lag dates compared with original filing timestamps |
| `coverage.parquet` | Issuer-level coverage, identity and date range |
| `coverage_extensions.parquet` | Added statement-period coverage, generated by the report step |
| `assessment_manifest.json` | Run configuration, quality classes and counts |
| `report_summary.json` | Comparison and extension totals |
| `REPORT.md` | Readable assessment |

Each directory contains ordinary Parquet files partitioned by requested symbol.
Read one symbol to keep memory use small, or read a whole directory:

```python
from pathlib import Path
import pandas as pd

root = Path("data/stockfit/assessment-2026-10-23")
panel = pd.read_parquet(root / "panel")
aapl_versions = pd.read_parquet(root / "vintages" / "AAPL.parquet")
revenue = panel[["act_symbol", "date",
                 "sf_income_statement_quarter_revenue",
                 "sf_income_statement_quarter_revenue__available_at",
                 "sf_income_statement_quarter_revenue__period_end"]]
```

The panel contains fundamentals only. Join selected prices or estimates by
`act_symbol` and `date` explicitly, after checking their own availability rules.
The saved research dates and membership horizon are unchanged; new filing
periods are available in the long tables without claiming a refreshed index
universe or refreshed prices. Inspect each feature's period end for staleness.

## Tests

```bash
python -m unittest discover -s tests -v
```

Tests cover original/revised values, after-hours filings, missing timestamps,
late fact introductions, synthetic values, separate reporting frequencies,
older-period revisions, null revisions, date matching, membership and caching.

## Sources

- [StockFit API reference](https://api.stockfit.io/docs)
- [StockFit filing and revision model](https://developer.stockfit.io/blog/holy-grail-api-stock-backtesting)
- [Historical CIK evidence](../config/stockfit_cik_overrides.json)

Downloaded provider data is not published by this repository. Data access and
redistribution remain subject to the provider's terms and the user's account.
