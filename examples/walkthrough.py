"""Run a bounded production API demonstration in a temporary directory."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
FLOW_NAMES = [
    "namespace", "ingest", "discovery", "range", "windows", "counters", "seal",
    "delete", "retention", "materialize", "freshness", "compact", "backup",
    "explain", "staleness"
]


def close(actual, expected):
    return math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)


def find_cell(cells, node, start):
    for cell in cells:
        if cell["labels"].get("node") == node and cell["start"] == start:
            return cell
    return None


def find_points(result, node):
    for series in result["series"]:
        if series["series"]["labels"].get("node") == node:
            return series["points"]
    return None


def at(points, timestamp):
    for point in points:
        if point["time"] == timestamp:
            return point
    return None


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


def staleness_timeline(op):
    """Retire, correct and resume one series with explicit stale markers.

    Timeline: web-1 reports a counter, is decommissioned (marker), is
    re-reported (recovery), then the marker itself is corrected by late
    writes in both directions. Markers are then sealed, deleted,
    re-admitted, compacted and finally dropped by the retention floor.
    web-2 is a control series that keeps reporting throughout.
    """
    space = "spaces/meridian"
    q = {"start": -1000, "end": 8000}
    rate = {**q, "step": 8000, "origin": 0, "group_by": ["node"],
            "function": "rate",
            "matchers": [{"label": "__name__", "op": "=", "value": "beats_total"}]}
    web1 = {"__name__": "beats_total", "node": "web-1", "zone": "east"}
    web2 = {"__name__": "beats_total", "node": "web-2", "zone": "east"}

    def value(series, t, v):
        return {"labels": series, "time": t, "value": v}

    def marker(series, t):
        return {"labels": series, "time": t, "stale": True}

    op("staleness namespace", "spaces", {
        "name": "meridian", "retention_ms": 100000, "lateness_ms": 100000,
        "future_ms": 1000, "max_series": 50, "max_write": 1000, "max_scan": 100000})
    op("report", space + "/points", {"now": 6000, "points": [
        value(web1, 0, 0), value(web1, 1000, 5), value(web1, 2000, 12),
        value(web1, 3000, 20),
        *[value(web2, t, t // 1000 + 1) for t in range(0, 7000, 1000)]]})

    # Decommission: the last value 20 must not be read as still valid.
    op("decommission", space + "/points", {"now": 6000, "points": [marker(web1, 4000)]})
    seen = op("raw sees marker", space + "/range", q)
    point = at(find_points(seen, "web-1"), 4000)
    if point is None or point["value"] is not None or point["stale"] is not True:
        raise RuntimeError("raw read does not show the invalidation marker")
    if seen["stats"]["stale"] != 1:
        raise RuntimeError("scan statistics do not count the marker")
    avg = op("aggregate skips marker", space + "/aggregate",
             {**q, "step": 1000, "origin": 0, "group_by": ["node"], "function": "avg"})
    if find_cell(avg["cells"], "web-1", 4000) is not None:
        raise RuntimeError("a marker populated an aggregate window")
    if find_cell(avg["cells"], "web-2", 4000)["value"] != 5.0:
        raise RuntimeError("control series lost a window to the marker")
    cells = op("rate stops at marker", space + "/aggregate", rate)["cells"]
    cell = find_cell(cells, "web-1", 0)
    if not close(cell["value"], 20 / 3) or cell["count"] != 4:
        raise RuntimeError("rate chained past the decommission marker")

    # Recovery: reporting resumes, but the rate must not connect across
    # the invalidation point; segments on both sides stay separate.
    op("recovery", space + "/points",
       {"now": 6000, "points": [value(web1, 5000, 25), value(web1, 6000, 31)]})
    cells = op("rate respects marker gap", space + "/aggregate", rate)["cells"]
    cell = find_cell(cells, "web-1", 0)
    if not close(cell["value"], 26 / 4) or cell["count"] != 6:
        raise RuntimeError("rate connected segments across the marker")

    # Late report, first direction: a value at the marker's own timestamp
    # corrects the invalidation; the most recently committed version wins.
    op("late value corrects marker", space + "/points",
       {"now": 6000, "points": [value(web1, 4000, 22)]})
    seen = op("raw marker corrected", space + "/range", q)
    point = at(find_points(seen, "web-1"), 4000)
    if point["value"] != 22 or point["stale"] is not False or seen["stats"]["stale"] != 0:
        raise RuntimeError("late value did not correct the marker")
    cells = op("rate reconnects", space + "/aggregate", rate)["cells"]
    if not close(find_cell(cells, "web-1", 0)["value"], 31 / 6):
        raise RuntimeError("corrected marker should rejoin the counter chain")

    # Late report, second direction: a marker invalidates an old value.
    op("late marker invalidates value", space + "/points",
       {"now": 6000, "points": [marker(web1, 2000)]})
    seen = op("raw late marker", space + "/range", q)
    point = at(find_points(seen, "web-1"), 2000)
    if point["value"] is not None or point["stale"] is not True:
        raise RuntimeError("late marker did not invalidate the value")
    cells = op("rate re-segments", space + "/aggregate", rate)["cells"]
    cell = find_cell(cells, "web-1", 0)
    if not close(cell["value"], 16 / 4) or cell["count"] != 6:
        raise RuntimeError("rate did not re-segment around the late marker")
    counted = op("aggregate count drops", space + "/aggregate",
                 {**q, "step": 8000, "origin": 0, "group_by": ["node"],
                  "function": "count"})["cells"]
    if find_cell(counted, "web-1", 0)["value"] != 6.0:
        raise RuntimeError("invalidated value still counted")

    # Same timestamp, one request: the later point wins regardless of kind.
    op("same request override", space + "/points",
       {"now": 6000, "points": [value(web1, 7000, 35), marker(web1, 7000)]})
    seen = op("raw same request override", space + "/range", q)
    if at(find_points(seen, "web-1"), 7000)["stale"] is not True:
        raise RuntimeError("later point in one request did not win")
    op("committed value overrides marker", space + "/points",
       {"now": 7000, "points": [value(web1, 7000, 35)]})
    seen = op("raw override corrected", space + "/range", q)
    if at(find_points(seen, "web-1"), 7000)["value"] != 35:
        raise RuntimeError("committed value did not override the marker")

    # Markers are sealed into blocks without changing logical samples.
    before = op("pre-seal markers", space + "/range", q)["series"]
    op("seal with markers", space + "/seal", {})
    if op("sealed markers raw", space + "/range", q)["series"] != before:
        raise RuntimeError("seal changed the marker timeline")
    op("marker blocks", space + "/blocks")

    # Deletion hides a marker; a fresh marker at that timestamp is
    # admitted again, and compaction keeps the visible timeline intact.
    op("delete marker", space + "/deletions",
       {"start": 1500, "end": 2500,
        "matchers": [{"label": "node", "op": "=", "value": "web-1"}]})
    seen = op("raw after marker delete", space + "/range", q)
    if at(find_points(seen, "web-1"), 2000) is not None or seen["stats"]["stale"] != 0:
        raise RuntimeError("deletion did not hide the marker")
    cells = op("rate after marker delete", space + "/aggregate", rate)["cells"]
    if not close(find_cell(cells, "web-1", 0)["value"], 35 / 7):
        raise RuntimeError("rate should rejoin once the marker is deleted")
    op("re-admit marker", space + "/points", {"now": 7000, "points": [marker(web1, 2000)]})
    seen = op("raw re-admitted marker", space + "/range", q)
    if at(find_points(seen, "web-1"), 2000)["stale"] is not True:
        raise RuntimeError("marker was not re-admitted after deletion")
    before = op("pre-compact markers", space + "/range", q)["series"]
    op("compact with markers", space + "/compact", {})
    if op("compacted markers raw", space + "/range", q)["series"] != before:
        raise RuntimeError("compaction changed the marker timeline")

    # Retention advances the floor past every marker and rejects a marker
    # below the floor exactly like a value.
    floor = op("retention drops markers", space + "/retention", {"now": 107000})
    if floor["floor"] != 7000:
        raise RuntimeError("unexpected retention floor")
    seen = op("raw after retention", space + "/range", q)
    points = find_points(seen, "web-1")
    if [p["time"] for p in points] != [7000] or seen["stats"]["stale"] != 0:
        raise RuntimeError("retention did not drop old values and markers")
    if find_points(seen, "web-2") is not None:
        raise RuntimeError("control series survived the retention floor")
    op("marker below floor rejected", space + "/points",
       {"now": 7000, "points": [marker(web1, 6000)]}, status=409)


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
                op("seal", space + "/seal", {})
                sealed = op("sealed range", space + "/range", q)
                if before["series"] != sealed["series"]:
                    raise RuntimeError("seal changed the logical samples")
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
                op("retention", space + "/retention", {"now": 11000})
                op("retention no resurrection", space + "/points", replacement, status=409)
                op("read-only", space + "/mode", {"read_only": True}, method="PUT")
                op("read-only rejection", space + "/points", {
                    "now": 11000, "points": [{**seed["points"][0], "time": 1500}]
                }, status=409)
                op("resume", space + "/mode", {"read_only": False}, method="PUT")
                staleness_timeline(op)
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
