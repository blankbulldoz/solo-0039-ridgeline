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
    "namespace", "ingest", "discovery", "range", "windows", "counters",
    "gap_rate", "seal",
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain"
]


def counter_points(node, schedule):
    """Build monotonic counter samples for one device from (time, value) pairs."""
    return [
        {"labels": {"__name__": "events_total", "group": "watch", "node": node},
         "time": t, "value": v}
        for t, v in schedule
    ]


def gap_rate_demo(base, space, op):
    """Controlled disconnect comparison through the HTTP API.

    Two adjacent windows hold the same two devices and the same per-device
    total increase. Window A concentrates one 5000 ms disconnect on device d2;
    window B makes both devices disconnect together for 4000 ms. Both windows
    end up with identical summed trusted coverage (12000 ms), so group totals
    alone cannot decide whether a window is fit for alert comparison.
    """
    policy = {"name": "gapwatch", "retention_ms": 1000000, "lateness_ms": 300000,
              "future_ms": 1000, "max_series": 100, "max_write": 10000, "max_scan": 100000}
    op("gap namespace", "spaces", policy)
    # Window A: d1 online for 9000 ms; d2 covers 3000 ms over two short runs.
    window_a = (
        counter_points("d1", [(0, 0), (1000, 10), (2000, 20), (3000, 30), (4000, 40),
                              (5000, 50), (6000, 60), (7000, 70), (8000, 80), (9000, 90)])
        + counter_points("d2", [(0, 0), (1000, 20), (6000, 20), (7000, 40), (8000, 60)]))
    # Window B occupies [11000, 22000); both devices are offline together
    # between relative 2000 and 6000, and counters stay frozen at reconnect so
    # each device's total increase is the same as in window A (d1: 90, d2: 60).
    window_b = (
        counter_points("d1", [(11000, 0), (12000, 10), (13000, 20),
                              (17000, 20), (18000, 40), (19000, 60), (20000, 75), (21000, 90)])
        + counter_points("d2", [(11000, 0), (12000, 20), (13000, 40),
                                (17000, 40), (18000, 45), (19000, 50),
                                (20000, 55), (21000, 60)]))
    op("gap ingest", space + "/points", {"now": 22000, "points": window_a + window_b})

    def gap_query(function, max_gap=None):
        query = {"start": 0, "end": 22000, "step": 11000, "origin": 0,
                 "group_by": ["group"], "function": function,
                 "matchers": [{"label": "__name__", "op": "=", "value": "events_total"}]}
        if max_gap is not None:
            query["max_gap_ms"] = max_gap
        return op(function, space + "/aggregate", query)

    legacy = {cell["start"]: cell for cell in gap_query("rate")["cells"]}
    # The old caliber keeps disconnect time in the first-to-last denominator:
    # window A reports 90/9 + 60/8 = 17.5/s, diluting d2's 5000 ms outage into
    # a stable-looking low rate instead of excluding the unmeasured interval.
    if abs(legacy[0]["value"] - 17.5) > 1e-9:
        raise RuntimeError("legacy rate window A did not keep downtime in denominator")

    gap = {cell["start"]: cell for cell in gap_query("rate_gap", 3500)["cells"]}
    a, b = gap[0], gap[11000]
    # Summed trusted coverage is identical (12000 ms) even though only A has a
    # single short-reporting device, so group totals alone cannot judge them.
    if a["coverage_ms"] != 12000 or b["coverage_ms"] != 12000:
        raise RuntimeError("controlled windows must share 12000 ms summed coverage")
    coverage = {cell["start"]: {entry["series"]["labels"]["node"]: entry
                                for entry in cell["series_coverage"]}
                for cell in (a, b)}
    if coverage[0]["d2"]["coverage_ms"] != 3000 or coverage[11000]["d2"]["coverage_ms"] != 6000:
        raise RuntimeError("per-device coverage must expose the different disconnect shapes")
    # Per-series trusted rates summed: A = 90/9 + 60/3 = 30/s;
    # B = 90/6 + 60/6 = 25/s.
    if abs(a["value"] - 30) > 1e-9 or abs(b["value"] - 25) > 1e-9:
        raise RuntimeError("gap-aware group rate mismatch")
    # Alerting policy decided by the caller: every device must cover >= 6000 ms.
    # Equal group totals do not help; only per-device coverage separates them.
    def comparable(cell):
        return all(entry["coverage_ms"] >= 6000 for entry in cell["series_coverage"])
    if comparable(a) or not comparable(b):
        raise RuntimeError("coverage policy must reject window A and accept window B")
    # max_gap_ms is only valid for rate_gap, and rate_gap requires it.
    op("gap rejected on legacy rate", space + "/aggregate",
       {"start": 0, "end": 11000, "step": 11000, "origin": 0, "group_by": ["group"],
        "function": "rate", "max_gap_ms": 3500}, status=400)
    op("rate_gap requires max_gap_ms", space + "/aggregate",
       {"start": 0, "end": 11000, "step": 11000, "origin": 0, "group_by": ["group"],
        "function": "rate_gap"}, status=400)


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
                gap_rate_demo(base, "spaces/gapwatch", op)
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
