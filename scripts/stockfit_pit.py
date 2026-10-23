"""Per-fact StockFit vintages and causal panel selection.

Provider-calculated facts without a dated source trail are usable only from the
snapshot observation time. A period's original filing date cannot date a later
restatement or an untraceable calculation.
"""
from __future__ import annotations

import math
from itertools import groupby

import pandas as pd


def timestamp(value):
    if not value:
        return None
    try:
        return pd.Timestamp(value).tz_localize("UTC") if pd.Timestamp(value).tzinfo is None else pd.Timestamp(value).tz_convert("UTC")
    except (ValueError, TypeError):
        return None


def public_time(source):
    accepted = timestamp(source.get("acceptedAt"))
    if accepted is not None:
        return accepted, "filing_acceptance"
    filed = timestamp(source.get("dateFiled"))
    if filed is not None:
        return filed.normalize() + pd.Timedelta(days=1), "filing_date_next_day"
    return None, "missing_timestamp"


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def iso(value):
    return value.isoformat() if value is not None else None


def normalize_period(period, envelope, cik=None):
    """Return (latest records, version records), preserving source units and dates."""
    params = envelope["params"]
    retrieved = timestamp(envelope["retrieved_at"])
    period_time, _ = public_time(period)
    latest, versions = [], []
    sources = period.get("sources") or {}
    native = {fact: currency for currency, facts in (period.get("fx") or {}).get("unconverted", {}).items()
              for fact in facts}
    for metric, latest_value in (period.get("facts") or {}).items():
        if latest_value is not None and not numeric(latest_value):
            continue
        events, undated = [], []
        for accession, source in sources.items():
            if metric not in source.get("facts", {}):
                continue
            when, basis = public_time(source)
            metadata = source["facts"][metric] or {}
            item = (when, accession, source, metadata, basis)
            if when is None:
                undated.append(item)
            else:
                events.append(item)
        events.sort(key=lambda e: (e[0], e[1]))
        units = {e[3].get("unit") for e in events + undated} - {None}
        unit = next(iter(units)) if len(units) == 1 else None
        fx = period.get("fx") or {}
        value_unit = native.get(metric, unit)
        if fx and metric not in native and unit and unit not in ("shares", "pure"):
            value_unit = "USD/shares" if unit.endswith("/shares") else "USD"
        reasons = []
        if undated:
            reasons.append("undated_reconstruction")
        if not events:
            reasons.append("no_dated_fact_source")
        if metric in period.get("derived", []):
            reasons.append("provider_derived")
        if len({e[0] for e in events}) != len(events):
            reasons.append("ambiguous_same_time_sources")
        if any(e[0] > retrieved for e in events):
            reasons.append("source_after_snapshot")
        period_end = timestamp(period["period"])
        if period_end is None or any(e[0] < period_end for e in events):
            reasons.append("invalid_period_or_source_before_period_end")
        if len(units) > 1:
            reasons.append("changing_units")
        if fx:
            # A snapshot FX conversion does not prove historical conversion inputs.
            reasons.append("currency_conversion_or_native_fx")
        if not unit:
            reasons.append("missing_unit")
        if period.get("splitFactor") not in (None, 1):
            reasons.append("provider_split_adjustment")
        base = {
            "act_symbol": envelope.get("requested_symbol") or params["symbol"], "cik": str(cik) if cik else None,
            "source": "stockfit", "statement": envelope["endpoint"].split("/")[-1],
            "period_kind": params["period"], "period_end": period["period"],
            "fiscal_year": period.get("fiscalYear"), "fiscal_period": period.get("fiscalPeriod"),
            "metric": metric, "unit": native.get(metric, unit),
            "value_unit": value_unit,
            "period_filed_at": iso(period_time), "retrieved_at": iso(retrieved),
            "has_revisions": any("before" in e[3] for e in events),
            "snapshot_sha256": envelope["sha256"],
        }
        if reasons:
            row = dict(base, value=latest_value, available_at=iso(retrieved),
                       availability_basis="observed_snapshot", pit_status="observed_only",
                       accession=None, concept=None, diagnostic=";".join(reasons))
            latest.append(row)
            versions.append(row)
            continue
        value = latest_value
        fact_versions = []
        for when, accession, _, metadata, basis in reversed(events):
            fact_versions.append(dict(base, value=value, available_at=iso(when),
                                      availability_basis=basis, pit_status="filing_traced",
                                      accession=accession, concept=metadata.get("concept"), diagnostic=""))
            if "before" in metadata:
                prior = metadata["before"]
                if prior is not None and not numeric(prior):
                    raise ValueError("Non-numeric prior fact value")
                value = prior
        fact_versions.reverse()
        latest.append(fact_versions[-1])
        versions.extend(fact_versions)
    return latest, versions


def select_fact_events(versions: pd.DataFrame) -> pd.DataFrame:
    """Select the newest fiscal period at each release, never an older restatement.

Input is one symbol / statement / frequency / metric. Keep null tombstones and
update provenance even if a new period happens to have the same numeric value.
"""
    if versions.empty:
        return versions
    ordered = versions.assign(_event_time=pd.to_datetime(versions.available_at, utc=True, format="mixed"))
    ordered = ordered.sort_values(["_event_time", "period_end", "accession"], na_position="first")
    state, selected, output = {}, None, []
    # Convert once: creating a DataFrame for every release dominates large builds.
    for _, simultaneous in groupby(ordered.to_dict("records"), key=lambda row: row["_event_time"]):
        for row in simultaneous:
            state[row["period_end"]] = row
        candidate = state[max(state)]
        identity = (candidate["period_end"], candidate["available_at"], candidate["accession"])
        if identity != selected:
            output.append(candidate)
            selected = identity
    return pd.DataFrame(output).drop(columns="_event_time")


def join_fact(anchors: pd.DataFrame, versions: pd.DataFrame) -> pd.DataFrame:
    """Join a single fact at 00:00 UTC on each anchor date (before the US open)."""
    events = select_fact_events(versions)
    if events.empty:
        return anchors.copy()
    left = anchors.copy()
    left["decision_at"] = pd.to_datetime(left["date"], utc=True).astype("datetime64[ns, UTC]")
    right = events.copy()
    right["available_at"] = pd.to_datetime(right["available_at"], utc=True, format="mixed").astype("datetime64[ns, UTC]")
    return pd.merge_asof(left.sort_values("decision_at"), right.sort_values("available_at"),
                         left_on="decision_at", right_on="available_at", direction="backward")
