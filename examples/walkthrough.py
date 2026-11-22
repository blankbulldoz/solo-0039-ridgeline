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
FLOW_NAMES = [
    "namespace", "ingest", "discovery", "range", "windows", "counters", "edge_rates",
    "seal", "delete", "retention", "materialize", "freshness", "compact", "backup", "explain"
]


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


def edge_rate_scenario(base, space, op):
    """Sparse counters and boundary resets across every window position."""
    sparse_ns = {
        "name": "sparse",
        "retention_ms": 1000000,
        "lateness_ms": 300000,
        "future_ms": 1000,
        "max_series": 100,
        "max_write": 10000,
        "max_scan": 100000,
    }
    op("edge namespace", "spaces", sparse_ns)
    sparse_space = "spaces/sparse"
    # Two sparse series, exactly one sample per 60s window, points 60s apart.
    # steady climbs by 60 per step; resetter wraps from 100 to 5 across the
    # left edge of window [60000, 120000).
    points = []
    for t, v in [(-30000, 0), (30000, 60), (90000, 120), (150000, 180), (210000, 240)]:
        points.append({"labels": {"__name__": "events_total", "host": "steady"},
                       "time": t, "value": v})
    for t, v in [(-30000, 20), (30000, 40), (90000, 5), (150000, 65), (210000, 125)]:
        points.append({"labels": {"__name__": "events_total", "host": "resetter"},
                       "time": t, "value": v})
    # An isolated point whose nearest neighbors are farther than any gap we
    # allow, proving unsupported windows are explained rather than invented.
    points.append({"labels": {"__name__": "events_total", "host": "lonely"},
                   "time": 90000, "value": 7})
    op("edge ingest", sparse_space + "/points", {"now": 220000, "points": points})

    rate_base = {
        "start": 0, "end": 240000, "step": 60000, "origin": 0,
        "function": "rate",
        "matchers": [{"label": "__name__", "op": "=", "value": "events_total"}],
    }

    def cells_of(resp):
        return {c["start"]: c for c in resp["cells"]}

    # Default contract: every window has one in-window point, so no rate.
    default = op("edge default sparse gap", sparse_space + "/aggregate", rate_base)
    if default["cells"]:
        raise RuntimeError("default rate must not invent values for single-point windows")

    edge_q = {**rate_base, "group_by": ["host"],
              "rate_edge_context": True, "rate_max_gap_ms": 30000}
    edge = op("edge context rates", sparse_space + "/aggregate", edge_q)
    by_window = cells_of(edge)
    if set(by_window) != {0, 60000, 120000, 180000}:
        raise RuntimeError(f"unexpected edge windows: {sorted(by_window)}")

    steady = {start: c for start, c in by_window.items()
              if c["labels"].get("host") == "steady"}
    # Interior two-sided windows: the full window is covered, no extrapolation.
    for start in (60000, 120000):
        cell = steady[start]
        if abs(cell["value"] - 1.0) > 1e-12:
            raise RuntimeError(f"steady interior rate wrong at {start}: {cell['value']}")
        ev = cell["rate_evidence"]
        if ev["mode"] != "edge_context" or ev["extrapolated_ms"] != 0 or ev["edge_samples"] != 2:
            raise RuntimeError(f"interior evidence wrong at {start}: {ev}")
        if ev["covered_ms"] != 60000 or ev["support_end"] - ev["support_start"] != 120000:
            raise RuntimeError(f"interior support span wrong at {start}: {ev}")
    # Boundary window 180000 has no right neighbor: half the window rests on
    # the left segment alone, so half the increase is attributed and half is
    # explicitly extrapolated.
    tail = steady[180000]
    if abs(tail["value"] - 0.5) > 1e-12:
        raise RuntimeError(f"one-sided rate wrong: {tail['value']}")
    tail_ev = tail["rate_evidence"]
    if tail_ev["covered_ms"] != 30000 or tail_ev["extrapolated_ms"] != 30000:
        raise RuntimeError(f"one-sided coverage wrong: {tail_ev}")
    if tail_ev["series"][0]["edge_samples"] != 1:
        raise RuntimeError(f"one-sided edge sample count wrong: {tail_ev}")

    # Boundary reset: resetter decreases 40 -> 5 between 30000 and 90000.
    # The left segment is a reset (increase 5, not -35), time-weighted to
    # half a segment; the right segment contributes normally.
    reset_cell = next(c for c in edge["cells"]
                      if c["start"] == 60000 and c["labels"].get("host") == "resetter")
    # left: 5 * 30000/60000 = 2.5 ; right: 60 * 30000/60000 = 30 ; per 60s
    if abs(reset_cell["value"] - 32.5 / 60.0) > 1e-12:
        raise RuntimeError(f"boundary reset rate wrong: {reset_cell['value']}")
    rev = reset_cell["rate_evidence"]
    if not rev["edge_reset"] or not rev["series"][0]["edge_reset"]:
        raise RuntimeError(f"edge reset not flagged: {rev}")

    # A max gap smaller than the 60s sample spacing supports nothing: the
    # lonely series never produces a cell, and no neighbor can be borrowed.
    tight_q = {**edge_q, "rate_max_gap_ms": 10000}
    tight = op("edge gap too small", sparse_space + "/aggregate", tight_q)
    if tight["cells"]:
        raise RuntimeError("windows beyond max gap must not produce rates")

    # Grouping across hosts keeps the unsupported series visible in evidence
    # while the other two still produce a value.
    group_q = {**rate_base, "rate_edge_context": True, "rate_max_gap_ms": 30000}
    grouped = op("edge grouped with unsupported", sparse_space + "/aggregate", group_q)
    gcell = cells_of(grouped)[60000]
    if gcell["rate_evidence"]["unsupported_series"] != 1:
        raise RuntimeError(f"unsupported series not reported: {gcell['rate_evidence']}")
    modes = {s["series_id"]: s["mode"] for s in gcell["rate_evidence"]["series"]}
    if not any(m == "unsupported" for m in modes.values()):
        raise RuntimeError(f"missing unsupported evidence entry: {modes}")

    # Validation: edge context is rate-only and needs a positive gap.
    op("edge context rejects non-rate", sparse_space + "/aggregate",
       {**rate_base, "function": "sum", "rate_edge_context": True, "rate_max_gap_ms": 30000},
       status=400)
    op("edge context requires gap", sparse_space + "/aggregate",
       {**rate_base, "rate_edge_context": True}, status=400)
    op("gap without flag rejected", sparse_space + "/aggregate",
       {**rate_base, "rate_max_gap_ms": 30000}, status=400)

    # Deletion removes a borrowed neighbor: window [120000, 180000) of steady
    # loses its right neighbor at 210000 and becomes one-sided.
    op("edge delete neighbor", sparse_space + "/deletions", {
        "start": 210000, "end": 220000,
        "matchers": [{"label": "host", "op": "=", "value": "steady"}],
    })
    after_delete = cells_of(op("edge rate after deletion", sparse_space + "/aggregate", edge_q))
    deleted_cell = next(c for c in after_delete[120000]["rate_evidence"]["series"]
                        if c["mode"] == "edge_context")
    if deleted_cell["edge_samples"] != 1 or deleted_cell["extrapolated_ms"] != 30000:
        raise RuntimeError(f"deletion did not remove borrowed support: {deleted_cell}")
    # A later correction at the deleted timestamp restores full support.
    op("edge re-admit correction", sparse_space + "/points", {
        "now": 220000,
        "points": [{"labels": {"__name__": "events_total", "host": "steady"},
                    "time": 210000, "value": 240}],
    })
    restored = cells_of(op("edge rate after correction", sparse_space + "/aggregate", edge_q))
    if restored[120000]["rate_evidence"]["extrapolated_ms"] != 0:
        raise RuntimeError("correction did not restore two-sided support")

    # Materialization remembers the chosen rule and the evidence.
    mat = op("edge materialize", sparse_space + "/rollups/sparse_rate", edge_q, method="PUT")
    if not mat["query"].get("rate_edge_context") or mat["query"].get("rate_max_gap_ms") != 30000:
        raise RuntimeError("materialized rollup forgot the edge rule")
    if not any(c.get("rate_evidence") for c in mat["result"]["cells"]):
        raise RuntimeError("materialized rollup dropped rate evidence")
    view = op("edge rollup read", sparse_space + "/rollups/sparse_rate")
    if view["stale"] or view["query"] != mat["query"]:
        raise RuntimeError("stored edge rule differs from the materialized query")
    # A materialization without edge context keeps the old rule and output.
    plain = op("edge materialize default", sparse_space + "/rollups/plain_rate",
               rate_base, method="PUT")
    if plain["result"]["cells"] or plain["query"].get("rate_edge_context"):
        raise RuntimeError("default-rollup semantics changed")

    # Retention physically removes an expired neighbor. Advancing the floor to
    # 0 drops the -30000 samples, so window [0, 60000) loses its left support.
    op("edge retention", sparse_space + "/retention", {"now": 1000000})
    post_retention = cells_of(op("edge rate after retention", sparse_space + "/aggregate", edge_q))
    head = next(c for c in post_retention[0]["rate_evidence"]["series"] if c["mode"] == "edge_context")
    if head["support_start"] == -30000 or head["edge_samples"] != 1:
        raise RuntimeError(f"retention did not remove expired neighbor: {head}")
    if post_retention[0]["rate_evidence"]["support_start"] < 0:
        raise RuntimeError(f"evidence still references expired time: {post_retention[0]['rate_evidence']}")


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
                edge_rate_scenario(base, space, op)
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
