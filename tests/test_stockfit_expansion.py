"""Issuer discovery, unrestricted history, event selection and complete partitions."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from assess_stockfit import fact_frame
from build_stockfit_universe import build_issuer, feature_events, validate_frames
from expand_stockfit import discover, download_issuer, issuer_id, write_json
from query_stockfit import asof_events
from stockfit_pit import normalize_period
from test_stockfit import fixture


def version_fixture():
    period, envelope = fixture()
    envelope["requested_symbol"] = issuer_id(123)
    latest, versions = normalize_period(period, envelope, 123)
    return (fact_frame(latest).rename(columns={"act_symbol": "issuer_id"}),
            fact_frame(versions).rename(columns={"act_symbol": "issuer_id"}))


class ExpansionTest(unittest.TestCase):
    def test_discovery_paginates_and_deduplicates_cik_not_ticker(self):
        client = Mock()
        calls = []
        def response(endpoint, params, cache):
            calls.append((endpoint, params))
            page = params["page"]
            active = endpoint == "symbols" and params["status"] == "operating"
            profile = {"cik": page if active else 3, "symbols": ["REUSED"], "type": "stock"}
            return {"data": {"page": page, "totalPages": 2 if active else 1,
                             "totalResults": 2 if active else 1, "data": [profile]},
                    "retrieved_at": "2026-10-23T12:00:00Z", "sha256": "test"}
        client.get.side_effect = response
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = discover(client, root)
            self.assertEqual(len(calls), 4)
            self.assertEqual([r["cik"] for r in universe["issuers"]], [1, 2, 3])
            self.assertEqual(len(universe["issuers"][2]["profiles"]), 1)
            self.assertEqual(discover(client, root), universe)
            self.assertEqual(len(calls), 4)

    def test_native_currency_max_depth_and_cik_only(self):
        client = Mock()
        client.get.return_value = {"data": [], "retrieved_at": "2026-10-23T12:00:00Z", "sha256": "test"}
        with tempfile.TemporaryDirectory() as directory:
            result = download_issuer(client, {"cik": 123, "issuer_id": issuer_id(123)}, Path(directory), 0)
        self.assertTrue(result["complete"])
        self.assertEqual(client.get.call_count, 6)
        for call in client.get.call_args_list:
            params = call.args[1]
            self.assertEqual(params["currency"], "original")
            self.assertEqual(params["limit"], 80)
            self.assertNotIn("symbol", params)
            self.assertEqual(params["cik"], 123)

    def test_all_metrics_and_pre2017_history(self):
        latest, versions = version_fixture()
        versions["metric"] = "uncommonMetricNotInCorePanel"
        versions["period_end"] = "2004-12-31"
        versions["available_at"] = ["2005-02-01T12:00:00+00:00", "2005-05-01T12:00:00+00:00"]
        events = feature_events(versions)
        result = asof_events(events, "2005-03-01")
        self.assertEqual(result.value.tolist(), [100])
        self.assertEqual(result.metric.tolist(), ["uncommonMetricNotInCorePanel"])

    def test_null_tombstone_and_filing_only_do_not_resurrect_old_value(self):
        _, versions = version_fixture()
        versions.loc[1, "value"] = None
        events = feature_events(versions)
        self.assertTrue(pd.isna(asof_events(events, "2024-06-01").value.iloc[0]))
        versions.loc[1, "pit_status"] = "observed_only"
        events = feature_events(versions)
        self.assertTrue(asof_events(events, "2024-06-01", filing_only=True).empty)

    def test_native_currency_is_preserved(self):
        period, envelope = fixture()
        envelope["params"]["currency"] = "original"
        for source in period["sources"].values():
            source["facts"]["revenue"]["unit"] = "EUR"
        latest, _ = normalize_period(period, envelope)
        self.assertEqual(latest[0]["value_unit"], "EUR")
        self.assertEqual(latest[0]["pit_status"], "filing_traced")

    def test_native_passthrough_fx_metadata_is_not_conversion(self):
        period, envelope = fixture()
        envelope["params"]["currency"] = "original"
        period["fx"] = {"targetCurrency": "original", "rates": [],
                        "unconverted": {"EUR": ["revenue"]}}
        for source in period["sources"].values():
            source["facts"]["revenue"]["unit"] = "EUR/shares"
        latest, versions = normalize_period(period, envelope)
        self.assertEqual(latest[0]["pit_status"], "filing_traced")
        self.assertEqual(latest[0]["value_unit"], "EUR/shares")
        self.assertEqual(latest[0]["unit"], "EUR/shares")
        self.assertEqual([v["value"] for v in versions], [100, 120])
        period["fx"]["rates"] = [{"from": "EUR", "rate": 1.1}]
        self.assertEqual(normalize_period(period, envelope)[0][0]["pit_status"], "observed_only")

    def test_conflicting_native_currency_is_quarantined(self):
        period, envelope = fixture()
        envelope["params"]["currency"] = "original"
        period["fx"] = {"targetCurrency": "original", "rates": [], "unconverted": {"EUR": ["revenue"]}}
        latest, _ = normalize_period(period, envelope)
        self.assertEqual(latest[0]["pit_status"], "observed_only")
        self.assertIn("native_currency_unit_conflict", latest[0]["diagnostic"])

    def test_future_period_cannot_evict_old_value(self):
        _, versions = version_fixture()
        future = versions.iloc[[1]].copy()
        future["period_end"] = "2030-12-31"
        future["pit_status"] = "observed_only"
        events = feature_events(pd.concat([versions, future], ignore_index=True))
        self.assertEqual(asof_events(events, "2024-06-01").period_end.tolist(), ["2023-12-31"])

    def test_utc_boundary_and_staleness(self):
        _, versions = version_fixture()
        events = feature_events(versions)
        self.assertTrue(asof_events(events, "2024-02-01").empty)
        self.assertEqual(asof_events(events, "2024-02-01T18:30:00-03:00").value.tolist(), [100])
        self.assertTrue(asof_events(events, "2024-06-01", max_age_days=30).empty)

    def test_duplicate_key_validation(self):
        latest, versions = version_fixture()
        with self.assertRaises(ValueError):
            validate_frames(pd.concat([latest, latest]), versions, feature_events(versions))

    def test_complete_issuer_build_keeps_all_periods_and_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot, output = root / "snapshot", root / "output"
            identity = issuer_id(123)
            write_json(snapshot / "status" / f"{identity}.json", {"complete": True})
            for statement in ("income-statement", "balance-sheet", "cash-flow-statement"):
                for kind in ("annual", "quarter"):
                    period, envelope = fixture()
                    envelope.update(endpoint=f"financials/{statement}", requested_symbol=identity)
                    envelope["params"] = {"cik": 123, "period": kind, "currency": "original", "limit": 80}
                    envelope["data"] = [period]
                    write_json(snapshot / "raw" / identity / f"{statement}-{kind}.json", envelope)
            result = build_issuer(snapshot, output, {"cik": 123, "issuer_id": identity})
            self.assertEqual(result["latest_facts"], 6)
            self.assertEqual(result["vintage_rows"], 12)
            self.assertEqual(result["feature_events"], 12)
            self.assertEqual(sum(result["validation"].values()), 0)
            self.assertEqual(len(pd.read_parquet(output / "events")), 12)
            self.assertTrue((output / "completed" / f"{identity}.json").exists())


if __name__ == "__main__":
    unittest.main()
