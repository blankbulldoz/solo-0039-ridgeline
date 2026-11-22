#!/usr/bin/env python3
"""Verify governance mappings against authoritative SeriesIDs.

This script never touches an existing database. It builds a disposable bbolt
file in a temporary directory, ingests governance/fixtures/extended-seed.json
through the public HTTP API, and reports the SeriesIDs the engine actually
assigned. Display-name grouping is compared against those IDs so that the
mapping tables cannot silently collapse distinct series.

Run with:
    python3 -I governance/verify/verify_series.py \
        --binary /path/to/ridgeline \
        --out governance/evidence/series-verification.json

The fixture is authored for this repository and contains synthetic data.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "governance/fixtures/extended-seed.json"


def request(base, route, payload=None, method=None, status=200):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route,
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        response = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        raw = response.read()
        if response.status != status:
            raise RuntimeError(f"{route}: {response.status}: {raw.decode(errors='replace')}")
        return json.loads(raw)


def launch(binary, database, log):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = f"127.0.0.1:{port}"
    process = subprocess.Popen(
        [str(binary)],
        env={**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address},
        stdout=log,
        stderr=log,
    )
    base = "http://" + address
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError("service exited before readiness")
        try:
            request(base, "health")
            return process, base
        except (OSError, RuntimeError):
            time.sleep(0.05)
    process.terminate()
    process.wait(timeout=15)
    raise RuntimeError("service did not become ready")


def stop(process):
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=20)
    if process.returncode != 0:
        raise RuntimeError(f"service exit {process.returncode}")


def independent_series_id(labels):
    """Reproduce model.SeriesID: sha256 over Go json.Marshal(label map).

    Go sorts map keys, emits compact separators and does not escape ASCII
    letters, digits or punctuation used by this fixture.
    """
    encoded = json.dumps(labels, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def expand_points(series, timestamps):
    points = []
    for item in series:
        for name, value in item["values"].items():
            points.append(
                {
                    "labels": item["labels"],
                    "time": timestamps[name],
                    "value": value,
                }
            )
    return points


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--fixture", type=Path, default=FIXTURE)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    fixture = json.loads(args.fixture.read_text())
    timestamps = fixture["timestamps"]
    observation_time = fixture["observation_time"]
    namespace = fixture["namespace"]["name"]
    space = f"spaces/{namespace}"

    evidence = {
        "fixture": str(args.fixture.relative_to(ROOT)),
        "database_change": "none",
        "authoritative_source": "SeriesID returned by the running engine, cross-checked by SHA-256 of the canonical label map",
        "fixture_sha256": hashlib.sha256(args.fixture.read_bytes()).hexdigest(),
        "observation_time": observation_time,
        "ingest": [],
        "series": [],
        "range": {},
        "checks": {},
        "conflicts": [],
        "display_name_dedup": {},
        "baseline_groups": {},
        "node_label_level": {},
        "impact": {},
        "handoff_queries": {},
    }

    with tempfile.TemporaryDirectory(prefix="ridgeline-governance-") as scratch:
        scratch = Path(scratch)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(args.binary, scratch / "governance.db", log)
            try:
                request(base, "spaces", fixture["namespace"])

                # Batch 1: every declared observation at its initial value.
                batch1 = expand_points(fixture["series"], timestamps)
                receipt1 = request(
                    base, space + "/points", {"now": observation_time, "points": batch1}
                )
                evidence["ingest"].append({"batch": 1, "points": len(batch1), "receipt": receipt1})

                # Batch 2: the logged same-series correction inside the window.
                for correction in fixture.get("corrections", []):
                    target = next(s for s in fixture["series"] if s["key"] == correction["series"])
                    point = {
                        "labels": target["labels"],
                        "time": timestamps[correction["timestamp"]],
                        "value": correction["value"],
                    }
                    receipt2 = request(
                        base, space + "/points", {"now": observation_time, "points": [point]}
                    )
                    evidence["ingest"].append(
                        {
                            "batch": 2,
                            "points": 1,
                            "labels": target["labels"],
                            "timestamp": correction["timestamp"],
                            "value": correction["value"],
                            "receipt": receipt2,
                        }
                    )

                discovered = request(base, space + "/series", {"matchers": []})
                ids_in_engine = {entry["id"] for entry in discovered}

                by_key = {}
                for item in fixture["series"]:
                    labels = item["labels"]
                    computed = independent_series_id(labels)
                    matches = [entry for entry in discovered if entry["labels"] == labels]
                    engine_id = matches[0]["id"] if len(matches) == 1 else None
                    by_key[item["key"]] = engine_id
                    evidence["series"].append(
                        {
                            "key": item["key"],
                            "labels": labels,
                            "node_case": item["node_case"],
                            "suffix": item["suffix"],
                            "rack_state": item["rack_state"],
                            "computed_sha256": computed,
                            "engine_series_id": engine_id,
                            "id_matches_computation": engine_id == computed,
                            "distinct_engine_entries_for_labels": len(matches),
                        }
                    )
                evidence["checks"]["all_ids_match_computation"] = all(
                    entry["id_matches_computation"] for entry in evidence["series"]
                )
                evidence["checks"]["every_declared_label_set_has_one_identity"] = all(
                    entry["distinct_engine_entries_for_labels"] == 1
                    for entry in evidence["series"]
                )
                evidence["checks"]["declared_series_count"] = len(fixture["series"])
                evidence["checks"]["engine_catalog_count"] = len(ids_in_engine)

                # Full raw range; this is the authoritative per-series timeline.
                start = fixture["governance_day"]["start_ms"]
                end = fixture["governance_day"]["end_ms"]
                raw = request(base, space + "/range", {"start": start, "end": end})
                by_id = {entry["series"]["id"]: entry for entry in raw["series"]}
                evidence["range"] = {
                    "sequence": raw["sequence"],
                    "floor": raw["floor"],
                    "series_returned": len(raw["series"]),
                    "truncated": raw["truncated"],
                    "series": [
                        {
                            "id": entry["series"]["id"],
                            "labels": entry["series"]["labels"],
                            "points": entry["points"],
                        }
                        for entry in raw["series"]
                    ],
                }
                evidence["checks"]["range_series_set_equals_catalog"] = (
                    set(by_id) == ids_in_engine
                )

                # Confirm the stored-model fact that a same-series duplicate
                # timestamp is resolved to the last committed value.
                s1a_id = by_key["S1a"]
                s1a_points = by_id[s1a_id]["points"]
                t4 = timestamps["T4"]
                t4_values = [p["value"] for p in s1a_points if p["time"] == t4]
                evidence["checks"]["same_series_same_time_has_one_stored_value"] = (
                    t4_values == [412]
                )
                evidence["checks"]["overwritten_value_visible_only_in_submission_log"] = {
                    "series_id": s1a_id,
                    "timestamp": t4,
                    "submitted_values": [410, 412],
                    "stored_value": t4_values[0] if t4_values else None,
                }

                # Simulated normalizations, evaluated per engine SeriesID.
                # Each mode changes exactly one axis so that merge counts and
                # conflicts can be attributed to that axis alone.
                groups = {
                    "case_fold_node": {},
                    "strip_suffix_only": {},
                    "case_fold_and_strip_suffix": {},
                    "collapse_rack_presence": {},
                    "case_fold_and_collapse_rack_presence": {},
                    "case_fold_strip_suffix_and_collapse_rack": {},
                }
                for entry in evidence["series"]:
                    labels = entry["labels"]
                    node = labels["node"]
                    rack_present = "rack" in labels
                    rack_value = labels.get("rack", "<absent>")
                    keys = {
                        # Baseline: exact engine identity. No merge should occur.
                        "unchanged": (labels["__name__"], node, labels.get("zone", "<absent>"),
                                      rack_present, rack_value),
                        # Case folding only.
                        "case_fold_node": (labels["__name__"], node.lower(),
                                           labels.get("zone", "<absent>"),
                                           rack_present, rack_value),
                        # Suffix stripping without case folding, to show that the
                        # two axes are independent. This is the case where
                        # RIDGE-A.prod and RIDGE-A.lab collide but ridge-a does
                        # not; it proves that case folding alone is not the
                        # source of the invalid merge.
                        "strip_suffix_only": (labels["__name__"],
                                              node.split(".")[0],
                                              labels.get("zone", "<absent>"),
                                              rack_present, rack_value),
                        # Case folding plus removal of a dotted domain suffix.
                        "case_fold_and_strip_suffix": (labels["__name__"],
                                                       node.lower().split(".")[0],
                                                       labels.get("zone", "<absent>"),
                                                       rack_present, rack_value),
                        # Treat absent and present-empty rack as one identity.
                        "collapse_rack_presence": (labels["__name__"], node,
                                                   labels.get("zone", "<absent>"),
                                                   labels.get("rack", "")),
                        # Case folding plus rack presence collapse.
                        "case_fold_and_collapse_rack_presence": (
                            labels["__name__"], node.lower(), labels.get("zone", "<absent>"),
                            labels.get("rack", ""),
                        ),
                        # The combined worst case: all three axes at once.
                        "case_fold_strip_suffix_and_collapse_rack": (
                            labels["__name__"], node.lower().split(".")[0],
                            labels.get("zone", "<absent>"),
                            labels.get("rack", ""),
                        ),
                    }
                    for mode, key in keys.items():
                        if mode == "unchanged":
                            evidence["baseline_groups"].setdefault(key, []).append(
                                entry["engine_series_id"]
                            )
                            continue
                        groups[mode].setdefault(key, []).append(entry["engine_series_id"])

                evidence["baseline_groups"] = [
                    {"key": key, "series_ids": sorted(value), "count": len(value)}
                    for key, value in sorted(
                        evidence["baseline_groups"].items(), key=lambda kv: str(kv[0])
                    )
                ]
                evidence["checks"]["baseline_has_no_merges"] = all(
                    group["count"] == 1 for group in evidence["baseline_groups"]
                )

                for mode, buckets in groups.items():
                    evidence["display_name_dedup"][mode] = {
                        "groups": len(buckets),
                        "inputs": len(evidence["series"]),
                        "series_after_normalization": len(buckets),
                        "bucket_detail": [
                            {"key": list(key), "series_ids": sorted(value), "count": len(value)}
                            for key, value in sorted(buckets.items(), key=lambda kv: str(kv[0]))
                        ],
                    }

                # Same-time conflicts after each candidate normalization.
                def conflicts_for(mode, bucket):
                    members = set(bucket)
                    values = {}
                    for sid, series in by_id.items():
                        if sid not in members:
                            continue
                        for point in series["points"]:
                            values.setdefault(point["time"], []).append(
                                (sid, point["value"], point["sequence"])
                            )
                    out = []
                    for timestamp, observations in sorted(values.items()):
                        distinct = {value for _, value, _ in observations}
                        if len(distinct) > 1:
                            out.append(
                                {
                                    "time": timestamp,
                                    "distinct_values": sorted(distinct),
                                    "observations": [
                                        {"series_id": sid, "value": value, "sequence": seq}
                                        for sid, value, seq in observations
                                    ],
                                }
                            )
                    return out

                for mode, buckets in groups.items():
                    for key, bucket in sorted(buckets.items(), key=lambda kv: str(kv[0])):
                        if len(bucket) < 2:
                            continue
                        conflicts = conflicts_for(mode, bucket)
                        for conflict in conflicts:
                            evidence["conflicts"].append(
                                {
                                    "normalization": mode,
                                    "group_key": list(key),
                                    "merged_series_ids": sorted(bucket),
                                    "time": conflict["time"],
                                    "distinct_values": conflict["distinct_values"],
                                    "observations": conflict["observations"],
                                }
                            )

                # Case-insensitive discovery proves the engine keeps both
                # identities as separate catalog rows.
                insensitive = request(
                    base,
                    space + "/series",
                    {"matchers": [{"label": "node", "op": "=~", "value": "(?i)ridge-a"}]},
                )
                evidence["handoff_queries"]["case_insensitive_node_discovery"] = [
                    {"id": entry["id"], "labels": entry["labels"]} for entry in insensitive
                ]
                evidence["handoff_queries"]["exact_node_ridge_a_discovery"] = [
                    {"id": entry["id"], "labels": entry["labels"]}
                    for entry in request(
                        base,
                        space + "/series",
                        {"matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]},
                    )
                ]

                # Absent and present-empty rack both satisfy rack="" in matchers,
                # although their SeriesIDs differ. This is the trap the mapping
                # must not resolve by display name.
                rack_empty = request(
                    base,
                    space + "/series",
                    {"matchers": [{"label": "rack", "op": "=", "value": ""}]},
                )
                evidence["handoff_queries"]["rack_equals_empty_discovery"] = [
                    {"id": entry["id"], "labels": entry["labels"]} for entry in rack_empty
                ]
                rack_ids = {entry["id"] for entry in rack_empty}
                evidence["checks"]["absent_and_empty_rack_both_match_empty"] = {
                    "s3a_present_empty_in_result": by_key["S3a"] in rack_ids,
                    "s3c_absent_in_result": by_key["S3c"] in rack_ids,
                    "ids_are_distinct": by_key["S3a"] != by_key["S3c"],
                }

                # Aggregate impact: what a downstream system would publish if it
                # lowercased `node` before grouping. The engine itself groups by
                # exact label value; this query selects both case variants with
                # a case-insensitive regex and reduces them into one cell.
                impact_query = {
                    "start": start,
                    "end": end,
                    "step": 60000,
                    "origin": 0,
                    "group_by": ["zone"],
                    "function": "avg",
                    "matchers": [
                        {"label": "__name__", "op": "=", "value": "pulse_total"},
                        {"label": "node", "op": "=~", "value": "(?i)ridge-a"},
                    ],
                }
                evidence["impact"]["case_insensitive_avg"] = request(
                    base, space + "/aggregate", impact_query
                )["cells"]
                exact_query = {
                    **impact_query,
                    "matchers": [
                        {"label": "__name__", "op": "=", "value": "pulse_total"},
                        {"label": "node", "op": "=", "value": "ridge-a"},
                    ],
                }
                evidence["impact"]["exact_ridge_a_avg"] = request(
                    base, space + "/aggregate", exact_query
                )["cells"]
                suffix_query = {
                    **impact_query,
                    "matchers": [
                        {"label": "__name__", "op": "=", "value": "pulse_total"},
                        {"label": "node", "op": "=~", "value": "(?i)ridge-a(\\..*)?"},
                    ],
                }
                evidence["impact"]["suffix_stripped_avg"] = request(
                    base, space + "/aggregate", suffix_query
                )["cells"]

                # Node-label level view: how many distinct display names exist
                # before and after each normalization.
                node_values = sorted({entry["labels"]["node"] for entry in evidence["series"]})
                casefolded = sorted({value.lower() for value in node_values})
                stripped = sorted({value.lower().split(".")[0] for value in node_values})
                evidence["node_label_level"] = {
                    "raw_values": node_values,
                    "raw_count": len(node_values),
                    "after_case_fold": casefolded,
                    "after_case_fold_count": len(casefolded),
                    "after_case_fold_and_suffix_strip": stripped,
                    "after_case_fold_and_suffix_strip_count": len(stripped),
                }

                window = fixture["change_window"]
                pre_end, post_start = window["start_ms"], window["end_ms"]
                masks = {
                    "pre_confirmed": {"start": start, "end": pre_end},
                    "undecided": {"start": pre_end, "end": post_start},
                    "post_confirmed": {"start": post_start, "end": end},
                }
                for name, query in masks.items():
                    result = request(base, space + "/range", query)
                    evidence["handoff_queries"][name] = {
                        "query": query,
                        "series": [
                            {
                                "id": entry["series"]["id"],
                                "labels": entry["series"]["labels"],
                                "points": entry["points"],
                            }
                            for entry in result["series"]
                        ],
                    }

                # Checks on the handoff query pack.
                def ids(result):
                    return {entry["series"]["id"] for entry in result["series"]}

                pre_ids = ids(
                    request(base, space + "/range", masks["pre_confirmed"])
                )
                undecided_ids = ids(
                    request(base, space + "/range", masks["undecided"])
                )
                post_ids = ids(
                    request(base, space + "/range", masks["post_confirmed"])
                )
                evidence["checks"]["masks_cover_all_series"] = (
                    pre_ids == ids_in_engine
                    and undecided_ids == ids_in_engine
                    and post_ids == ids_in_engine
                )
                evidence["checks"]["windows_are_disjoint_and_contiguous"] = (
                    masks["pre_confirmed"]["end"] == masks["undecided"]["start"]
                    and masks["undecided"]["end"] == masks["post_confirmed"]["start"]
                )
                evidence["checks"]["mask_point_counts"] = {
                    name: sum(len(entry["points"]) for entry in evidence["handoff_queries"][name]["series"])
                    for name in masks
                }

                evidence["checks"]["case_insensitive_matcher_returns_both_identities"] = {
                    "returned": len(insensitive),
                    "expected": len(
                        [
                            entry
                            for entry in evidence["series"]
                            if entry["labels"]["node"].lower() == "ridge-a"
                        ]
                    ),
                    "has_lower": len(
                        [
                            entry
                            for entry in insensitive
                            if entry["labels"]["node"] == "ridge-a"
                        ]
                    ),
                    "has_upper": len(
                        [
                            entry
                            for entry in insensitive
                            if entry["labels"]["node"] == "RIDGE-A"
                        ]
                    ),
                }

                # Each recommended per-series query must return exactly its own
                # identity when scoped by exact labels. Rows that differ only by
                # rack presence cannot be isolated by any matcher because an
                # absent label compares as empty; those rows must be filtered by
                # SeriesID on the client. Record that limitation explicitly.
                per_series_queries = []
                for entry in evidence["series"]:
                    labels = entry["labels"]
                    matchers = [
                        {"label": "__name__", "op": "=", "value": labels["__name__"]},
                        {"label": "node", "op": "=", "value": labels["node"]},
                        {"label": "zone", "op": "=", "value": labels["zone"]},
                    ]
                    if "rack" in labels:
                        matchers.append({"label": "rack", "op": "=", "value": labels["rack"]})
                    query = {"start": start, "end": end, "matchers": matchers}
                    result = request(base, space + "/range", query)
                    returned = ids(result)
                    presence_ambiguous = any(
                        sid != entry["engine_series_id"]
                        and next(
                            row for row in evidence["series"]
                            if row["engine_series_id"] == sid
                        )["labels"]["node"] == labels["node"]
                        and ("rack" in next(
                            row for row in evidence["series"]
                            if row["engine_series_id"] == sid
                        )["labels"]) != ("rack" in labels)
                        for sid in returned
                    )
                    per_series_queries.append(
                        {
                            "key": entry["key"],
                            "series_id": entry["engine_series_id"],
                            "query": query,
                            "returned_series_ids": sorted(returned),
                            "matches_exactly_one_identity": returned == {entry["engine_series_id"]},
                            "matcher_isolates_identity": returned == {entry["engine_series_id"]},
                            "presence_ambiguous_result": presence_ambiguous,
                            "client_action": (
                                "select only returned points whose series id equals this SeriesID"
                                if returned != {entry["engine_series_id"]}
                                else "single identity returned; no client filter needed"
                            ),
                        }
                    )
                evidence["handoff_queries"]["per_series_exact"] = per_series_queries
                evidence["checks"]["exact_queries_isolate_single_identity"] = all(
                    row["matches_exactly_one_identity"] for row in per_series_queries
                )
                ambiguous = [
                    row["key"] for row in per_series_queries if row["presence_ambiguous_result"]
                ]
                evidence["checks"]["rack_presence_needs_series_id_filter"] = {
                    "rows": ambiguous,
                    "explanation": (
                        "Absent and present-empty labels compare as the empty "
                        "string, so rack=\"\" matches both. No matcher can select "
                        "presence alone; the client must filter by SeriesID."
                    ),
                }
                allowed_ambiguity = {"S3a", "S3b", "S3c", "S3d"}
                evidence["checks"]["only_rack_rows_are_presence_ambiguous"] = (
                    set(ambiguous) == allowed_ambiguity
                )

                # Replay the case-fold conflict cell as a selector would see it.
                replay_start = timestamps["T4"]
                replay = request(
                    base,
                    space + "/range",
                    {
                        "start": replay_start,
                        "end": replay_start + 1,
                        "matchers": [
                            {"label": "__name__", "op": "=", "value": "pulse_total"},
                            {"label": "node", "op": "=~", "value": "(?i)ridge-a"},
                        ],
                    },
                )
                evidence["handoff_queries"]["conflict_replay"] = [
                    {
                        "id": entry["series"]["id"],
                        "labels": entry["series"]["labels"],
                        "points": entry["points"],
                    }
                    for entry in replay["series"]
                ]
                replay_values = sorted(
                    point["value"]
                    for entry in replay["series"]
                    for point in entry["points"]
                )
                evidence["checks"]["replay_shows_conflicting_values"] = replay_values == [412, 512]

                # Time extent per identity. If a case variant only exists on one
                # side of the cutover, case folding would merge a pre-change and
                # a post-change device, which is not covered by the business
                # table. Record the extents so the access team can check it
                # against real production data rather than trusting the fixture.
                evidence["time_extents"] = []
                change_start = fixture["change_window"]["start_ms"]
                change_end = fixture["change_window"]["end_ms"]
                for sid, entry in sorted(by_id.items()):
                    series = entry["series"]
                    times = [point["time"] for point in entry["points"]]
                    evidence["time_extents"].append(
                        {
                            "series_id": sid,
                            "labels": series["labels"],
                            "first_time": min(times),
                            "last_time": max(times),
                            "point_count": len(times),
                            "straddles_change_window": min(times) < change_end
                            and max(times) >= change_start,
                            "has_point_inside_change_window": any(
                                change_start <= value < change_end for value in times
                            ),
                        }
                    )
                # Case folding is only safe across the hardware change if both
                # case variants already coexist on both sides of the window.
                # If production shows one variant only before and the other
                # only after the change, case folding would silently merge the
                # legacy device with its replacement.
                by_lower = {}
                for entry in evidence["time_extents"]:
                    key = (
                        entry["labels"]["__name__"],
                        entry["labels"]["node"].lower(),
                        "rack" in entry["labels"],
                        entry["labels"].get("rack"),
                        entry["labels"].get("zone"),
                    )
                    by_lower.setdefault(key, []).append(entry)
                spans = []
                for key, rows in sorted(by_lower.items(), key=lambda kv: str(kv[0])):
                    if len(rows) < 2:
                        continue
                    spans.append(
                        {
                            "labels_key": list(key),
                            "variants": [
                                {
                                    "node": row["labels"]["node"],
                                    "first_time": row["first_time"],
                                    "last_time": row["last_time"],
                                    "has_pre_window_point": row["first_time"] < change_start,
                                    "has_post_window_point": row["last_time"] >= change_end,
                                }
                                for row in rows
                            ],
                            "both_variants_span_change": all(
                                row["first_time"] < change_start
                                and row["last_time"] >= change_end
                                for row in rows
                            ),
                        }
                    )
                evidence["case_variant_time_coverage"] = spans
                evidence["checks"]["case_variants_span_whole_day_in_fixture"] = all(
                    row["both_variants_span_change"] for row in spans
                ) and bool(spans)
            finally:
                stop(process)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(evidence, indent=2, sort_keys=False) + "\n")
    print(json.dumps({k: v for k, v in evidence["checks"].items() if not isinstance(v, dict)}, indent=2))
    print(f"wrote {args.out}")
    required = [
        "all_ids_match_computation",
        "every_declared_label_set_has_one_identity",
        "range_series_set_equals_catalog",
        "same_series_same_time_has_one_stored_value",
        "baseline_has_no_merges",
        "masks_cover_all_series",
        "windows_are_disjoint_and_contiguous",
        "only_rack_rows_are_presence_ambiguous",
        "replay_shows_conflicting_values",
    ]
    if not all(evidence["checks"].get(name) for name in required):
        raise SystemExit("verification failed; inspect the evidence file")


if __name__ == "__main__":
    main()
