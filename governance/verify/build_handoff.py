#!/usr/bin/env python3
"""Build the handoff manifest from verified evidence.

Inputs are read-only artifacts:
  governance/fixtures/extended-seed.json
  governance/evidence/series-verification.json
  examples/session.json  (recorded baseline discovery, for seed ID cross-check)

The script does not open or modify any database. It turns the observed
SeriesIDs and the business statements into a machine-readable mapping so the
access team can apply it without re-deriving identities from display names.
"""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "governance/fixtures/extended-seed.json"
EVIDENCE = ROOT / "governance/evidence/series-verification.json"
SESSION = ROOT / "examples/session.json"
OUT = ROOT / "governance/handoff/mapping-manifest.json"

LEGACY = "dev:ridge-a@legacy"
REPLACEMENT = "dev:ridge-a@replacement"
LAB = "dev:ridge-a.lab"
PROD = "dev:ridge-a.prod"
B = "dev:ridge-b"


def main():
    fixture = json.loads(FIXTURE.read_text())
    evidence = json.loads(EVIDENCE.read_text())
    session = json.loads(SESSION.read_text())

    series_rows = {row["key"]: row for row in evidence["series"]}
    fixture_rows = {row["key"]: row for row in fixture["series"]}
    day = fixture["governance_day"]
    window = fixture["change_window"]
    day_start, day_end = day["start_ms"], day["end_ms"]
    win_start, win_end = window["start_ms"], window["end_ms"]

    # Recorded baseline: four label sets already exist in examples/session.json.
    session_ids = {
        tuple(sorted(entry["labels"].items())): entry["id"]
        for entry in session[4]["response"]
    }

    def segments_for(node, labels, key):
        """Identity segments that preserve the physical-device meaning."""
        node_lower = node.lower()
        if node_lower == "ridge-a":
            return [
                {
                    "start_ms": day_start,
                    "end_ms": win_start,
                    "device_key": LEGACY,
                    "validity": "confirmed_interval",
                    "basis": ["B4", "legacy log pre-window"],
                },
                {
                    "start_ms": win_start,
                    "end_ms": win_end,
                    "device_key": None,
                    "validity": "undecided",
                    "basis": ["B4", "B5", "log resolves only a ten-minute bound"],
                },
                {
                    "start_ms": win_end,
                    "end_ms": day_end,
                    "device_key": REPLACEMENT,
                    "validity": "confirmed_interval",
                    "basis": ["B4", "legacy log post-window"],
                },
            ]
        if node_lower == "ridge-a.lab":
            return [
                {
                    "start_ms": day_start,
                    "end_ms": day_end,
                    "device_key": LAB,
                    "validity": "label_exact_distinct_from_prod",
                    "basis": ["B2"],
                }
            ]
        if node_lower == "ridge-a.prod":
            return [
                {
                    "start_ms": day_start,
                    "end_ms": day_end,
                    "device_key": PROD,
                    "validity": "label_exact_distinct_from_lab",
                    "basis": ["B2"],
                }
            ]
        if node_lower == "ridge-b":
            return [
                {
                    "start_ms": day_start,
                    "end_ms": day_end,
                    "device_key": B,
                    "validity": "label_exact",
                    "basis": ["no normalization proposed"],
                }
            ]
        return [
            {
                "start_ms": day_start,
                "end_ms": day_end,
                "device_key": "dev:" + node_lower,
                "validity": "label_exact",
                "basis": ["no normalization proposed"],
            }
        ]

    series_out = []
    for key in sorted(fixture_rows, key=lambda k: (k[:2], k)):
        fixture_row = fixture_rows[key]
        evidence_row = series_rows[key]
        labels = fixture_row["labels"]
        node = labels["node"]
        session_key = tuple(sorted(labels.items()))
        series_out.append(
            {
                "key": key,
                "series_id": evidence_row["engine_series_id"],
                "series_id_source": (
                    "examples/session.json discovery"
                    if session_key in session_ids
                    else "governance fixture discovery"
                ),
                "seed_cross_check": (
                    session_ids.get(session_key) == evidence_row["engine_series_id"]
                    if session_key in session_ids
                    else None
                ),
                "labels": labels,
                "metric": labels["__name__"],
                "node_case": fixture_row["node_case"],
                "node_suffix": fixture_row["suffix"] or None,
                "rack_state": fixture_row["rack_state"],
                "zone": labels.get("zone"),
                "identity_segments": segments_for(node, labels, key),
                "attributes": {
                    "rack_presence": "present" if "rack" in labels else "absent",
                    "rack_value": labels.get("rack"),
                    "rack_source": "unknown (B3)",
                    "zone_value": labels.get("zone"),
                    "zone_use_for_identity": "forbidden; seed has ridge-b/east and ridge-b/west",
                },
                "normalization": {
                    "case_fold_merge_with": sorted(
                        row["engine_series_id"]
                        for row in evidence["series"]
                        if row["engine_series_id"] != evidence_row["engine_series_id"]
                        and row["labels"]["__name__"] == labels["__name__"]
                        and row["labels"]["node"].lower() == node.lower()
                        and ("rack" in row["labels"]) == ("rack" in labels)
                        and row["labels"].get("rack") == labels.get("rack")
                        and row["labels"].get("zone") == labels.get("zone")
                    ),
                    "case_fold_basis": (
                        "confirmed_all_time (B1)" if node.lower() == "ridge-a" else
                        "inferred by rule; business table names only ridge-a/RIDGE-A (U5)"
                        if node.lower().startswith("ridge-a.") else
                        "not covered by the business table; no counterpart series observed"
                    ),
                    "suffix_strip_merge_with": sorted(
                        row["engine_series_id"]
                        for row in evidence["series"]
                        if row["engine_series_id"] != evidence_row["engine_series_id"]
                        and row["labels"]["__name__"] == labels["__name__"]
                        and row["labels"].get("zone") == labels.get("zone")
                        and ("rack" in row["labels"]) == ("rack" in labels)
                        and row["labels"].get("rack") == labels.get("rack")
                        and row["labels"]["node"].lower().split(".")[0]
                        == node.lower().split(".")[0]
                    ),
                    "suffix_strip_effect": (
                        "no sibling series; suffix strip is a no-op for this row"
                        if not any(
                            row["engine_series_id"] != evidence_row["engine_series_id"]
                            and row["labels"]["__name__"] == labels["__name__"]
                            and row["labels"].get("zone") == labels.get("zone")
                            and ("rack" in row["labels"]) == ("rack" in labels)
                            and row["labels"].get("rack") == labels.get("rack")
                            and row["labels"]["node"].lower().split(".")[0]
                            == node.lower().split(".")[0]
                            for row in evidence["series"]
                        ) else
                        "merges distinct devices; reject"
                    ),
                    "suffix_strip_decision": "reject (B2, B6)",
                },
            }
        )

    # Human-readable conflict summaries, anchored to exact SeriesIDs/values.
    conflicts_out = []
    suffix_conflicts = [
        conflict
        for conflict in evidence["conflicts"]
        if conflict["normalization"] == "case_fold_and_strip_suffix"
    ]
    suffix_only_conflicts = [
        conflict
        for conflict in evidence["conflicts"]
        if conflict["normalization"] == "strip_suffix_only"
    ]
    for conflict in evidence["conflicts"]:
        if conflict["normalization"] != "case_fold_node":
            continue
        conflicts_out.append(
            {
                "id": "C1",
                "kind": "same_device_same_time_different_value",
                "normalization": "case_fold_node",
                "metric": conflict["group_key"][0],
                "node_display": "ridge-a / RIDGE-A",
                "series_ids": [entry["series_id"] for entry in conflict["observations"]],
                "time": conflict["time"],
                "time_iso": "2026-11-18T12:04:00Z",
                "inside_change_window": win_start <= conflict["time"] < win_end,
                "distinct_values": conflict["distinct_values"],
                "observations": conflict["observations"],
                "meaning": (
                    "Case folding is authorized by B1, so after the merge these "
                    "become two observations of one device at one timestamp. The "
                    "engine keeps them apart today because their SeriesIDs differ; "
                    "the conflict is created by the proposed normalization. A "
                    "downstream join on lower(node) would see two values at this "
                    "timestamp with no defined precedence: 412 has commit "
                    "sequence 3, 512 has sequence 2, but they are different "
                    "series, so sequence does not order them as one timeline."
                ),
                "resolution": "undecided; hand off to access team (U4)",
            }
        )

    if suffix_only_conflicts or suffix_conflicts:
        sample = (suffix_only_conflicts or suffix_conflicts)[0]

        def timestamps_for(rows):
            seen_times = {}
            for conflict in rows:
                key = conflict["time"]
                if key in seen_times:
                    seen_times[key]["distinct_values"] = sorted(
                        set(seen_times[key]["distinct_values"])
                        | set(conflict["distinct_values"])
                    )
                    continue
                seen_times[key] = {
                    "time": key,
                    "inside_change_window": win_start <= key < win_end,
                    "distinct_values": list(conflict["distinct_values"]),
                }
            return [seen_times[key] for key in sorted(seen_times)]

        conflicts_out.append(
            {
                "id": "C2",
                "kind": "merge_of_distinct_devices",
                "normalization": "strip_suffix_only and case_fold_and_strip_suffix",
                "metric": sample["group_key"][0],
                "merged_series_count": len(sample["merged_series_ids"]),
                "merged_series_ids": sample["merged_series_ids"],
                "conflict_timestamps_by_normalization": {
                    "strip_suffix_only": timestamps_for(suffix_only_conflicts),
                    "case_fold_and_strip_suffix": timestamps_for(suffix_conflicts),
                },
                "sample_observations": sample["observations"],
                "sample_time": sample["time"],
                "meaning": (
                    "This is not a data error. B2 says .lab and .prod are "
                    "different devices, so stripping the suffix merges distinct "
                    "device timelines. The conflict appears even without case "
                    "folding, so the case-folding decision is not what makes "
                    "suffix stripping invalid. Outside the change window the "
                    "merged bucket yields 3 distinct values at every observed "
                    "timestamp, the signature of an invalid identity merge "
                    "rather than a recoverable conflict."
                ),
                "resolution": "reject normalization; no value reconciliation applies",
            }
        )

    overwrite = evidence["checks"]["overwritten_value_visible_only_in_submission_log"]
    same_series_conflict = {
        "id": "C3",
        "kind": "same_series_same_time_overwritten",
        "series_id": overwrite["series_id"],
        "time": overwrite["timestamp"],
        "submitted_values": overwrite["submitted_values"],
        "stored_value": overwrite["stored_value"],
        "inside_change_window": win_start <= overwrite["timestamp"] < win_end,
        "meaning": (
            "Two producers using the same node label wrote the same series at "
            "the same timestamp during the cutover. The engine kept the last "
            "committed value, so the first value exists only in the submission "
            "log. This is a same-time value conflict at the storage layer and "
            "cannot be repaired by identity mapping."
        ),
        "resolution": "undecided; hand off to access team (U4)",
    }

    # Aggregate impact measured by the engine.
    impact = evidence["impact"]
    t4_cell = next(
        cell for cell in impact["case_insensitive_avg"]
        if cell["start"] == fixture["timestamps"]["T4"]
    )
    suffix_t1 = next(
        cell for cell in impact["suffix_stripped_avg"]
        if cell["start"] == fixture["timestamps"]["T1"]
    )

    undecided = [
        {
            "id": "U1",
            "topic": "exact cutover instant",
            "fact": "The log bounds the replacement to [12:00, 12:10) UTC; it does not identify the second.",
            "consequence": "Every sample in the window has an unknown physical producer.",
            "owner": "access team + legacy collector operations",
            "needed_evidence": "per-device serial or agent ID near T3..T5, or a timestamped operations record",
        },
        {
            "id": "U2",
            "topic": "base ridge-a versus .lab/.prod",
            "fact": "B2 only states .lab != .prod; the business table does not relate the base name to either.",
            "consequence": "Cannot use .lab/.prod to infer the base device identity, and cannot strip suffixes.",
            "owner": "business owner",
            "needed_evidence": "asset registry entry linking display names to device serials",
        },
        {
            "id": "U3",
            "topic": "rack empty source",
            "fact": "B3 says the source of an empty rack value is unknown.",
            "consequence": "Missing rack and explicit empty rack have different SeriesIDs; whether they are the same state is unknown.",
            "owner": "collection owner",
            "needed_evidence": "producer configuration showing whether rack is omitted or emitted as empty, and why",
        },
        {
            "id": "U4",
            "topic": "same-time value conflicts at T4",
            "fact": "C1 (412 vs 512 across case variants) and C3 (410 overwritten by 412 in one series) both fall inside the window.",
            "consequence": "Value selection at T4 is not determined by identity alone.",
            "owner": "access team + collection owners of both devices",
            "needed_evidence": "per-device raw submission log with producer identity",
        },
        {
            "id": "U5",
            "topic": "case fold for .lab/.prod variants",
            "fact": "B1 names only ridge-a/RIDGE-A; it does not state that RIDGE-A.lab is ridge-a.lab.",
            "consequence": "The case-fold merge for suffix variants is recommended by rule but not confirmed.",
            "owner": "business owner",
            "needed_evidence": "explicit statement or asset registry match",
        },
        {
            "id": "U6",
            "topic": "zone as an identity attribute",
            "fact": "Seed contains node=ridge-b with zone=east (pulse_total) and zone=west (load_ratio).",
            "consequence": "zone cannot be used to join or split device identity; it is an inconsistent attribute.",
            "owner": "collection owner",
            "needed_evidence": "authoritative site/zone assignment per device",
        },
        {
            "id": "U7",
            "topic": "validity outside the governance day",
            "fact": "The replacement log covers one day. No statement covers earlier or later days.",
            "consequence": "The legacy/replacement time split is confirmed only for 2026-11-18 UTC.",
            "owner": "legacy collector operations",
            "needed_evidence": "change history for the full retention horizon",
        },
    ]

    handoff_queries = evidence["handoff_queries"]
    ci_count = len(handoff_queries["case_insensitive_node_discovery"])
    exact_count = len(handoff_queries["exact_node_ridge_a_discovery"])
    rack_count = len(handoff_queries["rack_equals_empty_discovery"])
    mask_counts = evidence["checks"]["mask_point_counts"]
    catalog_count = evidence["checks"]["engine_catalog_count"]
    replay = handoff_queries["conflict_replay"]
    replay_values = sorted(
        point["value"] for entry in replay for point in entry["points"]
    )
    replay_ids = sorted(
        (
            entry["id"],
            point["value"],
        )
        for entry in replay
        for point in entry["points"]
    )

    handoff = {
        "freeze": [
            "Do not rewrite labels, SeriesIDs, head points, blocks or tombstones.",
            "Do not run a suffix-stripping or case-collapsing migration; both would either merge distinct devices or orphan stored keys.",
            "Keep the source database read-only for the reconciliation window.",
        ],
        "query_pack": {
            "namespace": fixture["namespace"]["name"],
            "note": "All intervals are half-open. Run against a copy or a read-only namespace.",
            "pre_confirmed": {
                "query": {"start": day_start, "end": win_start},
                "expected": (
                    f"{catalog_count} series, {mask_counts['pre_confirmed']} points "
                    "(T1, T2 per series), directly mapable"
                ),
                "observed_point_count": mask_counts["pre_confirmed"],
            },
            "undecided_window": {
                "query": {"start": win_start, "end": win_end},
                "expected": (
                    f"{catalog_count} series, {mask_counts['undecided']} points "
                    "(T3, T4, T5 per series); these must not be assigned to a "
                    "physical device"
                ),
                "observed_point_count": mask_counts["undecided"],
            },
            "post_confirmed": {
                "query": {"start": win_end, "end": day_end},
                "expected": (
                    f"{catalog_count} series, {mask_counts['post_confirmed']} points "
                    "(T6, T7 per series), directly mapable"
                ),
                "observed_point_count": mask_counts["post_confirmed"],
            },
            "case_insensitive_discovery": {
                "query": {"matchers": [{"label": "node", "op": "=~", "value": "(?i)ridge-a"}]},
                "expected": (
                    f"{ci_count} series: both case variants for ridge-a, "
                    "ridge-a.lab and ridge-a.prod; proves both identities exist"
                ),
                "observed_series_count": ci_count,
            },
            "exact_case_discovery": {
                "query": {"matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]},
                "expected": f"{exact_count} series with node exactly ridge-a",
                "observed_series_count": exact_count,
            },
            "rack_empty_discovery": {
                "query": {"matchers": [{"label": "rack", "op": "=", "value": ""}]},
                "expected": (
                    f"{rack_count} series: explicit rack=\"\" and series with no "
                    "rack label are both returned; the engine matcher treats "
                    "absent as empty even though SeriesIDs differ"
                ),
                "observed_series_count": rack_count,
            },
            "conflict_replay": {
                "query": {"start": fixture["timestamps"]["T4"], "end": fixture["timestamps"]["T4"] + 1},
                "expected": (
                    f"case-fold conflict values {replay_values}; 410 is not "
                    "retrievable because it was overwritten in its own series"
                ),
                "observed": [
                    {"series_id": sid, "value": value} for sid, value in replay_ids
                ],
            },
        },
        "per_series_queries": [
            {
                "key": row["key"],
                "series_id": row["series_id"],
                "query": {
                    "start": day_start,
                    "end": day_end,
                    "matchers": [
                        {"label": "__name__", "op": "=", "value": row["labels"]["__name__"]},
                        {"label": "node", "op": "=", "value": row["labels"]["node"]},
                    ]
                    + (
                        [{"label": "rack", "op": "=", "value": ""}]
                        if "rack" in row["labels"]
                        else []
                    ),
                },
                "note": (
                    "Exact node matcher is required. A case-insensitive matcher "
                    "returns the sibling identity as well. Rows that differ only "
                    "by rack presence cannot be isolated by any matcher; filter "
                    "the result by the expected SeriesID."
                ),
            }
            for row in series_out
        ],
        "steps": [
            "1. Confirm U1-U7 with the named owners; record answers by series ID, not display name.",
            "2. Pull the pre_confirmed and post_confirmed masks; these may be mapped now.",
            "3. Pull the undecided window and hold it aside; do not label it legacy or replacement.",
            "4. For each point in the window, attach producer evidence (serial/agent ID) and then split it.",
            "5. Resolve C1/C3 values from the submission log; never average or silently pick a value.",
            "6. Only after U1 and U4 are answered, publish device-level series; keep SeriesID as the storage key.",
            "7. Re-run governance/verify/verify_series.py after any producer label change.",
        ],
    }

    # Node-level business mapping: display name -> device identity, with the
    # time validity of each conclusion. This is the table an access team reads
    # first; the per-series table is the machine-checkable expansion.
    node_summary = []
    for normalized in sorted({row["labels"]["node"].lower() for row in series_out}):
        rows = [
            row for row in series_out if row["labels"]["node"].lower() == normalized
        ]
        display_names = sorted({row["labels"]["node"] for row in rows})
        segments = rows[0]["identity_segments"]
        if len(rows) != len(
            {
                row["series_id"]
                for row in rows
            }
        ):
            raise SystemExit(f"duplicate series id within node {normalized}")
        node_summary.append(
            {
                "display_nodes": display_names,
                "normalized_node": normalized,
                "business_device": (
                    "ridge-a (legacy hardware before the window; replacement "
                    "hardware after the window)"
                    if normalized == "ridge-a" else
                    normalized
                ),
                "relation_to_ridge_a": (
                    "same device as ridge-a/RIDGE-A (B1)"
                    if normalized == "ridge-a" else
                    "distinct device; relation to base ridge-a unknown (B2, B6)"
                    if normalized in ("ridge-a.lab", "ridge-a.prod") else
                    "separate device, not covered by the business table"
                ),
                "validity": (
                    "label equivalence ridge-a/RIDGE-A holds at all times (B1); "
                    "the physical device is time-split, see segments"
                    if normalized == "ridge-a" else
                    "label identity holds at all times; relation to ridge-a is "
                    "not stated by the business table"
                    if normalized.startswith("ridge-a.") else
                    "label identity holds at all times; no relation to ridge-a "
                    "is proposed"
                ),
                "segments": segments,
                "series_keys": [row["key"] for row in rows],
                "series_ids": [row["series_id"] for row in rows],
            }
        )

    manifest = {
        "schema_version": 1,
        "database_change": "none",
        "scope": "governance analysis only",
        "generated_at": "2026-11-22",
        "provenance": {
            "fixture": "governance/fixtures/extended-seed.json",
            "fixture_sha256": hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
            "evidence": "governance/evidence/series-verification.json",
            "seed_session": "examples/session.json",
            "seed_series_ids": {
                "ae37407678ef71c5a33ebe50d20bef7a18161f25a7a244d2c842f41f0bf0dc87": {
                    "__name__": "pulse_total", "node": "ridge-a", "zone": "east"
                },
                "39221d1b5192ba577bd99dc708b70f6d0ad8d49fe739499eb321c5761d975d5f": {
                    "__name__": "load_ratio", "node": "ridge-a", "rack": "", "zone": "east"
                },
                "c9c72d2a25e46bc3425bd051c939c305b7e03e5fffc80d9b83c3815ca46cad69": {
                    "__name__": "pulse_total", "node": "ridge-b", "zone": "east"
                },
                "0f8fa51d16cf44764e3ef8d5bec20a41dee666c396716d60014c4f6859710888": {
                    "__name__": "load_ratio", "node": "ridge-b", "zone": "west"
                },
            },
            "identity_rule": "SeriesID = sha256(json.Marshal(labels)); see internal/model/validate.go:39",
            "namespace_independence": "SeriesID depends only on labels, not on namespace name",
        },
        "recommended_policy": {
            "case_fold": {
                "decision": "apply_to_node_value_only",
                "confirmed": ["ridge-a", "RIDGE-A"],
                "inferred": [
                    ["ridge-a.lab", "RIDGE-A.lab"],
                    ["ridge-a.prod", "RIDGE-A.prod"],
                ],
                "basis": "B1 for the base pair; rule extension for suffix pairs (U5)",
            },
            "suffix_strip": {
                "decision": "reject",
                "basis": ["B2", "B6"],
                "reason": (
                    "B2 makes .lab and .prod different devices. Removing the "
                    "suffix merges 6 pulse_total series into one at every one of "
                    "the 7 observed timestamps, with 3 distinct values at each."
                ),
            },
            "rack_presence_collapse": {
                "decision": "reject_until_source_confirmed",
                "basis": ["B3"],
                "reason": (
                    "Missing and explicit empty rack are distinct SeriesIDs. "
                    "There is no observed value conflict, but absence of a "
                    "conflict does not prove the same source."
                ),
            },
            "zone": {
                "decision": "do_not_use_for_identity",
                "basis": ["seed observation B7"],
            },
            "storage": {
                "decision": "keep_existing_series_ids",
                "reason": (
                    "Head point keys, blocks, tombstones and rollup outputs "
                    "reference SeriesID. There is no relabel API and rewriting "
                    "identity would orphan stored references."
                ),
            },
        },
        "node_business_mapping": node_summary,
        "series": series_out,
        "conflicts": conflicts_out + [same_series_conflict],
        "aggregate_impact": {
            "case_fold_conflict_cell": {
                "window": [t4_cell["start"], t4_cell["end"]],
                "avg_with_case_insensitive_selector": t4_cell["value"],
                "count": t4_cell["count"],
                "exact_ridge_a_avg": next(
                    cell["value"] for cell in impact["exact_ridge_a_avg"]
                    if cell["start"] == fixture["timestamps"]["T4"]
                ),
                "note": (
                    "A case-insensitive selector averages the conflicting values "
                    "into 462; the exact selector returns 412. Neither is a "
                    "validated device value."
                ),
            },
            "suffix_strip_impact_cell": {
                "window": [suffix_t1["start"], suffix_t1["end"]],
                "avg_after_suffix_strip": suffix_t1["value"],
                "count": suffix_t1["count"],
                "note": (
                    "Averaging distinct devices (.lab and .prod) with the base "
                    "produces a number that describes no physical device."
                ),
            },
            "series_counts": {
                "engine_series": evidence["checks"]["engine_catalog_count"],
                "after_case_fold": evidence["display_name_dedup"]["case_fold_node"]["groups"],
                "after_case_fold_and_suffix_strip": evidence["display_name_dedup"]["case_fold_and_strip_suffix"]["groups"],
                "after_rack_presence_collapse": evidence["display_name_dedup"]["collapse_rack_presence"]["groups"],
                "after_all_three": evidence["display_name_dedup"]["case_fold_strip_suffix_and_collapse_rack"]["groups"],
            },
        },
        "undecidable": undecided,
        "case_variant_time_coverage": evidence["case_variant_time_coverage"],
        "assumptions_to_check_against_production": [
            {
                "id": "A1",
                "assumption": (
                    "Both case variants of every node exist on both sides of the "
                    "hardware change in the governance fixture."
                ),
                "risk": (
                    "If production has RIDGE-A only before the change and "
                    "ridge-a only after, case folding would merge the legacy "
                    "device with its replacement across the cutover. Then B1 "
                    "('same device') would hold only within each period."
                ),
                "check": (
                    "Select node=~'(?i)ridge-a' and compare first/last times per "
                    "SeriesID before applying the mapping."
                ),
                "status": "verified true in fixture; unverified in production",
            },
            {
                "id": "A2",
                "assumption": (
                    "Case variants of one device agree on values at each "
                    "timestamp outside the change window."
                ),
                "risk": (
                    "If they disagree, case folding turns every disagreement "
                    "into a same-time conflict even though the engine stores the "
                    "two SeriesIDs separately. In the fixture the only "
                    "disagreement is C1 at T4, inside the window."
                ),
                "check": (
                    "Replay the pre/post masks with node =~ '(?i)ridge-a' and "
                    "compare values per timestamp before declaring the pair safe."
                ),
                "status": "true in fixture outside the window; unverified in production",
            }
        ],
        "handoff": handoff,
        "verification": evidence["checks"],
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n")

    # Flat CSV for spreadsheet review. One row per series, with the three time
    # segments spelled out so that filtering by time cannot silently promote an
    # undecided point into a confirmed device.
    csv_rows = [
        [
            "key",
            "series_id",
            "metric",
            "node_display",
            "rack_state",
            "zone",
            "device_scope",
            "device_all_time",
            "pre_device",
            "pre_validity",
            "window_device",
            "window_validity",
            "post_device",
            "post_validity",
            "case_fold_merge_with",
            "suffix_strip_decision",
        ]
    ]
    for row in series_out:
        segments = row["identity_segments"]
        pre = next((s for s in segments if s["start_ms"] == day_start and s["end_ms"] == win_start), None)
        win = next((s for s in segments if s["start_ms"] == win_start and s["end_ms"] == win_end), None)
        post = next((s for s in segments if s["start_ms"] == win_end and s["end_ms"] == day_end), None)
        whole = next((s for s in segments if s["start_ms"] == day_start and s["end_ms"] == day_end), None)
        csv_rows.append(
            [
                row["key"],
                row["series_id"],
                row["metric"],
                row["labels"]["node"],
                row["rack_state"],
                row["zone"] or "",
                "all_time" if whole else "time_split",
                (whole or {}).get("device_key") or "",
                (pre or {}).get("device_key") or "",
                (pre or {}).get("validity") or "",
                (win or {}).get("device_key") or ("UNDECIDED" if win else ""),
                (win or {}).get("validity") or "",
                (post or {}).get("device_key") or "",
                (post or {}).get("validity") or "",
                ";".join(row["normalization"]["case_fold_merge_with"]),
                row["normalization"]["suffix_strip_decision"],
            ]
        )

    def esc(value):
        text = str(value)
        return '"' + text.replace('"', '""') + '"' if any(c in text for c in ',"\n') else text

    csv_out = OUT.parent / "mapping.csv"
    csv_out.write_text("\n".join(",".join(esc(cell) for cell in row) for row in csv_rows) + "\n")
    print(f"wrote {csv_out}")
    print(f"wrote {OUT}")
    print(f"series={len(series_out)} conflicts={len(manifest['conflicts'])} undecidable={len(undecided)}")


if __name__ == "__main__":
    main()
