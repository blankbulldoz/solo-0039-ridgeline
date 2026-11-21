"""Run a bounded production API demonstration in a temporary directory."""

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

ROOT = Path(__file__).resolve().parents[1]


def series_id(labels):
    """Match the engine's canonical identity: SHA-256 over key-sorted JSON."""
    return hashlib.sha256(json.dumps(labels, sort_keys=True).encode()).hexdigest()


FLOW_NAMES = [
    "namespace", "ingest", "discovery", "range", "windows", "counters", "asof", "seal",
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain"
]


# Sparse temperature readings correlated against dense power observations.
# Two temperature series share node "ridge-a" so equal-distance ties have to be
# resolved by series identity; power at 3500 sits exactly between two inlet
# probes so a same-series equal-distance tie resolves toward the earlier time.
ASOF_POINTS = [
    {"labels": {"__name__": "temperature", "node": "ridge-a", "sensor": "inlet"},
     "time": 1000, "value": 21.0},
    {"labels": {"__name__": "temperature", "node": "ridge-a", "sensor": "inlet"},
     "time": 3000, "value": 23.0},
    {"labels": {"__name__": "temperature", "node": "ridge-a", "sensor": "outlet"},
     "time": 2000, "value": 27.0},
    {"labels": {"__name__": "temperature", "node": "ridge-b", "sensor": "inlet"},
     "time": 1200, "value": 19.5},
    {"labels": {"__name__": "power", "node": "ridge-a"}, "time": 500, "value": 110.0},
    {"labels": {"__name__": "power", "node": "ridge-a"}, "time": 1500, "value": 130.0},
    {"labels": {"__name__": "power", "node": "ridge-a"}, "time": 2500, "value": 150.0},
    {"labels": {"__name__": "power", "node": "ridge-a"}, "time": 3500, "value": 155.0},
    {"labels": {"__name__": "power", "node": "ridge-a"}, "time": 4500, "value": 160.0},
    {"labels": {"__name__": "power", "node": "ridge-b"}, "time": 1500, "value": 90.0},
]
# Late report arriving after the first batch: an out-of-order inlet probe that
# becomes the closest backward neighbor of the 4500 power observation.
ASOF_LATE = [
    {"labels": {"__name__": "temperature", "node": "ridge-a", "sensor": "inlet"},
     "time": 4000, "value": 25.0},
]


def asof_pairs(result, key):
    return [p for p in result["pairs"] if p["pair_key"] == key]


def require_pair(pairs, left_time):
    for pair in pairs:
        if pair["left"]["time"] == left_time:
            return pair
    raise RuntimeError(f"missing pair for left observation at {left_time}")


def assert_matched(pair, sensor, right_time, distance):
    if not pair["matched"] or pair["right"] is None or pair["right_series"] is None:
        raise RuntimeError(f"expected match at {pair['left']['time']}")
    if pair["right"]["time"] != right_time or pair["distance_ms"] != distance:
        raise RuntimeError("unexpected neighbor time or distance")
    if pair["right"]["series_id"] != pair["right_series"]["id"]:
        raise RuntimeError("neighbor point lost its series identity")
    if pair["right_series"]["labels"].get("sensor") != sensor:
        raise RuntimeError("neighbor series labels do not identify the chosen series")
    if pair["right"]["sequence"] == 0:
        raise RuntimeError("neighbor point lost its commit sequence")


def assert_unmatched(pair):
    if pair["matched"] or pair["right"] is not None or pair["right_series"] is not None:
        raise RuntimeError(f"expected unmatched pair at {pair['left']['time']}")
    if pair["distance_ms"] != 0:
        raise RuntimeError("unmatched pair must not carry a distance")


def exercise_asof(base, space, op, sensor_ids, sealed=False):
    prefix = "sealed " if sealed else ""
    temperature = [{"label": "__name__", "op": "=", "value": "temperature"}]
    power = [{"label": "__name__", "op": "=", "value": "power"}]
    base_query = {"start": 0, "end": 5000, "pair_label": "node",
                  "left_matchers": power, "right_matchers": temperature}

    backward = op(prefix + "asof backward", space + "/asof",
                  {**base_query, "direction": "backward"})
    a = asof_pairs(backward, "ridge-a")
    assert_unmatched(require_pair(a, 500))
    assert_matched(require_pair(a, 1500), "inlet", 1000, 500)
    assert_matched(require_pair(a, 2500), "outlet", 2000, 500)
    assert_matched(require_pair(a, 3500), "inlet", 3000, 500)
    assert_matched(require_pair(a, 4500), "inlet", 4000, 500)
    assert_matched(require_pair(asof_pairs(backward, "ridge-b"), 1500),
                   "inlet", 1200, 300)

    bounded = op(prefix + "asof bounded backward", space + "/asof",
                 {**base_query, "direction": "backward", "max_distance_ms": 200})
    # Every ridge-a neighbor sits exactly 500ms away and the ridge-b probe
    # 300ms away, so a 200ms bound rejects every pair. The bound is inclusive;
    # the following 500ms request re-admits the ridge-a neighbors.
    ba = asof_pairs(bounded, "ridge-a")
    for pair in ba:
        assert_unmatched(pair)
    bb = asof_pairs(bounded, "ridge-b")
    assert_unmatched(require_pair(bb, 1500))
    bounded_wide = op(prefix + "asof inclusive bound", space + "/asof",
                      {**base_query, "direction": "backward", "max_distance_ms": 500})
    assert_matched(require_pair(asof_pairs(bounded_wide, "ridge-a"), 1500),
                   "inlet", 1000, 500)
    assert_matched(require_pair(asof_pairs(bounded_wide, "ridge-b"), 1500),
                   "inlet", 1200, 300)

    forward = op(prefix + "asof forward", space + "/asof",
                 {**base_query, "direction": "forward"})
    fa = asof_pairs(forward, "ridge-a")
    assert_matched(require_pair(fa, 500), "inlet", 1000, 500)
    assert_matched(require_pair(fa, 1500), "outlet", 2000, 500)
    assert_matched(require_pair(fa, 2500), "inlet", 3000, 500)
    assert_matched(require_pair(fa, 3500), "inlet", 4000, 500)
    assert_unmatched(require_pair(fa, 4500))
    assert_unmatched(require_pair(asof_pairs(forward, "ridge-b"), 1500))

    nearest = op(prefix + "asof nearest", space + "/asof",
                 {**base_query, "direction": "nearest"})
    na = asof_pairs(nearest, "ridge-a")
    # At 1500 and 2500 the inlet and outlet series are equally far on opposite
    # sides; series identity breaks the cross-series tie deterministically.
    tie_winner = min(sensor_ids["inlet"], sensor_ids["outlet"])
    for left_time in (1500, 2500):
        tied = require_pair(na, left_time)
        if not tied["matched"] or tied["distance_ms"] != 500 or \
                tied["right"]["series_id"] != tie_winner:
            raise RuntimeError("equal-distance multi-series tie was not resolved by series ID")
    # At 3500 two inlet probes tie at 500ms within one series; the earlier one
    # wins and the late 4000 probe still loses to the 3000 probe here.
    assert_matched(require_pair(na, 3500), "inlet", 3000, 500)
    assert_matched(require_pair(na, 4500), "inlet", 4000, 500)
    assert_matched(require_pair(asof_pairs(nearest, "ridge-b"), 1500),
                   "inlet", 1200, 300)

    limited = op(prefix + "asof limit", space + "/asof",
                 {**base_query, "direction": "nearest", "limit": 2})
    if not limited["truncated"] or len(limited["pairs"]) != 2:
        raise RuntimeError("as-of limit must truncate and report the indicator")

    op(prefix + "asof invalid direction", space + "/asof",
       {**base_query, "direction": "sideways"}, status=400)
    op(prefix + "asof invalid distance", space + "/asof",
       {**base_query, "direction": "nearest", "max_distance_ms": -1}, status=400)
    op(prefix + "asof invalid pair label", space + "/asof",
       {**base_query, "direction": "nearest", "pair_label": "not a label!"}, status=400)
    op(prefix + "asof empty interval", space + "/asof",
       {**base_query, "start": 5000, "end": 5000, "direction": "nearest"}, status=400)
    return nearest


def request(base, route, payload=None, method=None, status=200):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route, data=body,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        data = response.read()
        if response.status != status:
            raise RuntimeError(f"{route}: {response.status}: {data.decode(errors='replace')}")
        if response.headers.get_content_type() == "application/octet-stream":
            return data, dict(response.headers)
        return json.loads(data)


def launch(binary, database, log):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = f"127.0.0.1:{port}"
    process = subprocess.Popen(
        [str(binary)], env={**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address},
        stdout=log, stderr=log,
    )
    base = "http://" + address
    for _ in range(100):
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
    process.wait(timeout=15)
    if process.returncode != 0:
        raise RuntimeError(f"service exit {process.returncode}")


def exercise(workflow, output):
    seed = json.loads((ROOT / "examples/input.json").read_text())
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-demo-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "source.db", log)
            try:
                space = "spaces/spindle"
                q = {"start": -5000, "end": 10000}

                def op(label, route, payload=None, **options):
                    out = request(base, route, payload, **options)
                    observations.append({"step": label, "request": payload, "response": out})
                    return out

                op("namespace", "spaces", seed["namespace"])
                op("duplicate namespace", "spaces", seed["namespace"], status=409)
                op("ingest", space + "/points", {"now": seed["now"], "points": seed["points"]})
                op("invalid atomic request", space + "/points", {
                    "now": 10000, "points": [seed["points"][0], {**seed["points"][0], "time": 20000}]
                }, status=409)
                op("discovery", space + "/series", {"matchers": [{"label": "rack", "op": "=", "value": ""}]})
                before = op("range", space + "/range", q)
                op("truncated range", space + "/range", {**q, "limit": 2})
                agg = {**q, "step": 1000, "origin": 0, "group_by": ["zone"], "function": "avg"}
                op("windows", space + "/aggregate", agg)
                rate = {**q, "step": 10000, "origin": -5000, "group_by": ["zone"],
                        "function": "rate", "matchers": [{"label": "__name__", "op": "=", "value": "pulse_total"}]}
                op("counters", space + "/aggregate", rate)

                # As-of scenario: dense power observations are correlated with
                # sparse temperature probes sharing the node label.
                asof_identities = [series_id(point["labels"]) for point in
                                   ASOF_POINTS + ASOF_LATE]
                existing = {entry["id"] for entry in
                            request(base, space + "/series",
                                    {"matchers": []}, method="POST")}
                if existing.intersection(asof_identities):
                    raise RuntimeError("as-of scenario collides with existing series identity")
                op("asof ingest", space + "/points", {"now": seed["now"], "points": ASOF_POINTS})
                op("asof late report", space + "/points",
                   {"now": seed["now"], "points": ASOF_LATE})
                discovered = op("asof discovery", space + "/series", {
                    "matchers": [{"label": "__name__", "op": "=", "value": "temperature"}]})
                sensor_ids = {"inlet": set(), "outlet": set()}
                for entry in discovered:
                    sensor = entry["labels"].get("sensor")
                    if sensor in sensor_ids:
                        sensor_ids[sensor].add(entry["id"])
                if len(sensor_ids["inlet"]) != 2 or len(sensor_ids["outlet"]) != 1:
                    raise RuntimeError("unexpected temperature series cardinality")
                sensor_ids = {sensor: next(iter(ids)) for sensor, ids in sensor_ids.items()}
                head_nearest = exercise_asof(base, space, op, sensor_ids)
                # Snapshot every visible series immediately before sealing so
                # head and compressed blocks can be compared on identical data.
                head_snapshot = op("pre-seal snapshot", space + "/range", q)

                op("seal", space + "/seal", {})
                sealed = op("sealed range", space + "/range", q)
                if head_snapshot["series"] != sealed["series"]:
                    raise RuntimeError("seal changed the logical samples")
                # The same logical as-of result must come from compressed blocks.
                sealed_nearest = exercise_asof(base, space, op, sensor_ids, sealed=True)
                if sealed_nearest["pairs"] != head_nearest["pairs"]:
                    raise RuntimeError("as-of result changed after sealing")
                op("blocks", space + "/blocks")
                op("delete", space + "/deletions", {**q, "start": -600, "end": 600,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                op("deletion inventory", space + "/deletions")
                replacement = {"now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 8}]}
                op("correct after delete", space + "/points", replacement)
                op("materialize", space + "/rollups/pulse", agg, method="PUT")
                fresh = op("freshness", space + "/rollups/pulse")
                if fresh["stale"]:
                    raise RuntimeError("new materialization is stale")
                op("new correction", space + "/points", {
                    "now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 9}]
                })
                stale = op("stale materialization", space + "/rollups/pulse")
                if not stale["stale"]:
                    raise RuntimeError("changed namespace should mark materialization stale")
                logical = op("pre-compaction", space + "/range", q)["series"]
                op("compact", space + "/compact", {})
                if op("post-compaction", space + "/range", q)["series"] != logical:
                    raise RuntimeError("compaction changed logical samples")
                op("explain", space + "/explain", q)
                # A corrective temperature write lands in the head while the
                # earlier probes remain in blocks, and the as-of neighbor then
                # exposes the corrected value with its new commit sequence.
                op("asof correction", space + "/points", {"now": 10000, "points": [{
                    "labels": {"__name__": "temperature", "node": "ridge-a", "sensor": "inlet"},
                    "time": 1000, "value": 99.5}]})
                corrected = request(base, space + "/asof", {
                    "start": 0, "end": 5000, "pair_label": "node", "direction": "backward",
                    "left_matchers": [{"label": "__name__", "op": "=", "value": "power"}],
                    "right_matchers": [{"label": "__name__", "op": "=", "value": "temperature"}],
                }, method="POST")
                target = require_pair(asof_pairs(corrected, "ridge-a"), 1500)
                if target["right"]["time"] != 1000 or target["right"]["value"] != 99.5:
                    raise RuntimeError("as-of query did not resolve the corrected temperature")
                observations.append({"step": "asof corrected", "response": corrected})
                op("retention", space + "/retention", {"now": 11000})
                op("retention no resurrection", space + "/points", replacement, status=409)
                op("read-only", space + "/mode", {"read_only": True}, method="PUT")
                op("read-only rejection", space + "/points", {
                    "now": 11000, "points": [{**seed["points"][0], "time": 1500}]
                }, status=409)
                op("resume", space + "/mode", {"read_only": False}, method="PUT")
                source = op("backup source", space + "/range", q)
                image, headers = request(base, "image")
                if hashlib.sha256(image).hexdigest() != headers["X-Ridge-Sha256"]:
                    raise RuntimeError("backup hash mismatch")
                (scratch / "restored.db").write_bytes(image)
                observations.append({"step": "backup", "sequence": headers["X-Ridge-Sequence"],
                                     "sha256": headers["X-Ridge-Sha256"], "bytes": len(image)})
            finally:
                stop(process)
            process, base = launch(binary, scratch / "restored.db", log)
            try:
                restored = request(base, "spaces/spindle/range", q)
                if restored != source:
                    raise RuntimeError("restored image differs from source read")
                observations.append({"step": "restored", "response": restored})
            finally:
                stop(process)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(observations, indent=2) + "\n")
    print(json.dumps({"workflow": workflow, "operations": len(observations), "state": "complete",
                      "coverage": FLOW_NAMES}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    exercise(args.workflow, args.output)
