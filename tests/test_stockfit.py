"""Causality, accounting-period alignment, and download-cache regression tests."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from stockfit_pit import normalize_period, public_time, select_fact_events, join_fact
from assess_stockfit import candidate_period, fact_frame, panel_symbol, membership_mask
from fetch_stockfit import Client, DownloadError


def fixture():
    envelope = {"params": {"symbol": "TEST", "period": "quarter"},
                "endpoint": "financials/income-statement", "sha256": "fixture",
                "retrieved_at": "2025-01-01T12:00:00Z"}
    period = {"period": "2023-12-31", "fiscalYear": 2023, "fiscalPeriod": "Q4",
              "dateFiled": "2024-02-01", "acceptedAt": "2024-02-01T21:30:00Z",
              "facts": {"revenue": 120}, "sources": {
                  "original": {"acceptedAt": "2024-02-01T21:30:00Z", "facts": {"revenue": {"unit": "USD"}}},
                  "revision": {"acceptedAt": "2024-05-01T14:00:00Z", "facts": {"revenue": {"before": 100, "unit": "USD"}}}}}
    return period, envelope


class VintagesTest(unittest.TestCase):
    def test_prior_values_and_intraday_boundary(self):
        p, e = fixture()
        _, versions = normalize_period(p, e)
        self.assertEqual([v["value"] for v in versions], [100, 120])
        joined = join_fact(pd.DataFrame({"date": pd.to_datetime(["2024-02-01", "2024-02-02", "2024-05-01", "2024-05-02"])}), pd.DataFrame(versions))
        self.assertTrue(pd.isna(joined.value.iloc[0]))
        self.assertEqual(joined.value.iloc[1:].tolist(), [100, 100, 120])

    def test_multiple_revisions_and_unchanged_retagging(self):
        p, e = fixture()
        p["facts"]["revenue"] = 150
        p["sources"]["second"] = {"acceptedAt": "2024-06-01T14:00:00Z", "facts": {"revenue": {"before": 120, "unit": "USD"}}}
        p["sources"]["unchanged"] = {"acceptedAt": "2024-07-01T14:00:00Z", "facts": {"revenue": {"unit": "USD"}}}
        _, versions = normalize_period(p, e)
        self.assertEqual([v["value"] for v in versions], [100, 120, 150, 150])

    def test_fact_first_disclosed_later_is_not_backdated(self):
        p, e = fixture()
        del p["sources"]["original"]
        del p["sources"]["revision"]["facts"]["revenue"]["before"]
        _, versions = normalize_period(p, e)
        self.assertTrue(versions[0]["available_at"].startswith("2024-05-01"))

    def test_date_only_source_waits_until_next_day(self):
        when, basis = public_time({"dateFiled": "2024-02-01"})
        self.assertEqual(when, pd.Timestamp("2024-02-02", tz="UTC"))
        self.assertEqual(basis, "filing_date_next_day")

    def test_undated_reconstruction_is_observed_only(self):
        p, e = fixture()
        p["sources"] = {"Q4-Recon-FY-9M": {"facts": {"revenue": {"unit": "USD"}}}}
        latest, versions = normalize_period(p, e)
        self.assertEqual(latest[0]["pit_status"], "observed_only")
        self.assertEqual(versions[0]["available_at"], "2025-01-01T12:00:00+00:00")

    def test_derived_fx_missing_units_and_future_sources_are_quarantined(self):
        for mutation in (lambda p: p.update(derived=["revenue"]),
                         lambda p: p.update(fx={"targetCurrency": "USD"}),
                         lambda p: p["sources"]["revision"].update(acceptedAt="2026-01-01T00:00:00Z")):
            p, e = fixture()
            mutation(p)
            self.assertEqual(normalize_period(p, e)[0][0]["pit_status"], "observed_only")

    def test_missing_units_ambiguous_times_and_preperiod_sources(self):
        for case in ("units", "same_time", "preperiod"):
            p, e = fixture()
            if case == "units":
                for source in p["sources"].values():
                    source["facts"]["revenue"].pop("unit")
            elif case == "same_time":
                p["sources"]["revision"]["acceptedAt"] = p["sources"]["original"]["acceptedAt"]
            else:
                p["sources"]["original"]["acceptedAt"] = "2023-01-01T00:00:00Z"
            self.assertEqual(normalize_period(p, e)[0][0]["pit_status"], "observed_only")

    def test_cik_request_preserves_requested_ticker(self):
        p, e = fixture()
        e["params"] = {"cik": 123, "period": "quarter"}
        e["requested_symbol"] = "OLD"
        latest, _ = normalize_period(p, e, cik=123)
        self.assertEqual(latest[0]["act_symbol"], "OLD")
        self.assertEqual(latest[0]["cik"], "123")

    def test_fx_value_unit_does_not_mislabel_converted_values(self):
        p, e = fixture()
        for source in p["sources"].values():
            source["facts"]["revenue"]["unit"] = "EUR"
        p["fx"] = {"targetCurrency": "USD", "rates": [{"from": "EUR", "rate": 1.1}]}
        latest, _ = normalize_period(p, e)
        self.assertEqual(latest[0]["unit"], "EUR")
        self.assertEqual(latest[0]["value_unit"], "USD")
        self.assertEqual(latest[0]["pit_status"], "observed_only")

    def test_old_period_restatement_does_not_replace_new_period(self):
        p, e = fixture()
        _, old = normalize_period(p, e)
        new = copy.deepcopy(old[0])
        new.update(period_end="2024-03-31", available_at="2024-04-20T12:00:00+00:00", value=180)
        selected = select_fact_events(pd.DataFrame(old + [new]))
        self.assertEqual(selected.value.tolist(), [100, 180])

    def test_null_revision_does_not_forward_fill_old_value(self):
        p, e = fixture()
        p["facts"]["revenue"] = None
        _, versions = normalize_period(p, e)
        joined = join_fact(pd.DataFrame({"date": pd.to_datetime(["2024-05-02"])}), pd.DataFrame(versions))
        self.assertTrue(pd.isna(joined.value.iloc[0]))

    def test_mixed_timestamp_precision(self):
        p, e = fixture()
        p["sources"]["revision"]["acceptedAt"] = "2024-05-01T14:00:00.123456Z"
        _, versions = normalize_period(p, e)
        result = join_fact(pd.DataFrame({"date": pd.to_datetime(["2024-05-02"]).astype("datetime64[ns]")}), pd.DataFrame(versions))
        self.assertEqual(result.value.iloc[0], 120)

    def test_annual_and_quarter_features_remain_separate(self):
        p, e = fixture()
        _, quarters = normalize_period(p, e)
        e["params"]["period"] = "annual"
        p["facts"]["revenue"] = 500
        p["sources"] = {"original": p["sources"]["original"]}
        _, annual = normalize_period(p, e)
        anchors = pd.DataFrame({"act_symbol": ["TEST"], "date": pd.to_datetime(["2024-03-01"])})
        result = panel_symbol(anchors, fact_frame(quarters + annual))
        self.assertEqual(result.sf_income_statement_quarter_revenue.iloc[0], 100)
        self.assertEqual(result.sf_income_statement_annual_revenue.iloc[0], 500)

    def test_fiscal_date_matching_rejects_ambiguity_and_distant_periods(self):
        self.assertEqual(candidate_period("2024-09-30", ["2024-09-26"]), "2024-09-26")
        self.assertIsNone(candidate_period("2024-09-30", ["2024-09-26", "2024-09-28"]))
        self.assertIsNone(candidate_period("2024-09-30", ["2024-08-31"]))

    def test_membership_intervals(self):
        anchors = pd.DataFrame({"act_symbol": ["TEST"] * 3, "date": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"])})
        intervals = pd.DataFrame({"act_symbol": ["TEST"], "effective_start": pd.to_datetime(["2020-06-01"]), "effective_end": pd.to_datetime(["2021-06-01"])})
        self.assertEqual(membership_mask(anchors, intervals).tolist(), [False, True, False])


class DownloadTest(unittest.TestCase):
    def test_cache_avoids_requests_and_does_not_store_token(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("SECRET_TEST_TOKEN", Path(directory), 10000)
            response = Mock(status_code=200, headers={}, content=b"[]")
            response.json.return_value = []
            with patch("fetch_stockfit.requests.get", return_value=response) as get:
                params = {"symbol": "TEST", "period": "annual"}
                client.get("financials/income-statement", params)
                client.get("financials/income-statement", params)
                self.assertEqual(get.call_count, 1)
            self.assertNotIn("SECRET_TEST_TOKEN", next(Path(directory).rglob("*.json")).read_text())

    def test_authentication_failure_stops_further_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("SECRET", Path(directory), 10000)
            with patch("fetch_stockfit.requests.get", return_value=Mock(status_code=401)) as get:
                for _ in range(2):
                    with self.assertRaises(DownloadError):
                        client.get("company/details", {"symbol": "TEST"})
                self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
