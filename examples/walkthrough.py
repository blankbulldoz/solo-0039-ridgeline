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
    "namespace", "ingest", "discovery", "range", "windows", "counters", "seal",
    "delete", "retention", "materialize", "freshness", "compact", "continuous",
    "backup", "explain"
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

                # Continuous aggregate: a defined window query that follows
                # late data through a durable per-window invalidation log.
                cagg_space = "spaces/cagg"
                op("continuous namespace", "spaces", {
                    "name": "cagg", "retention_ms": 20000, "lateness_ms": 100000,
                    "future_ms": 10000, "max_series": 100, "max_write": 10000,
                    "max_scan": 100000,
                })
                load = lambda zone, t, v: {"labels": {"__name__": "load", "zone": zone},
                                           "time": t, "value": v}
                op("continuous ingest", cagg_space + "/points", {"now": 10000, "points": [
                    *[load("east", t, t // 1000) for t in (1000, 2000, 3000, 4000, 5000)],
                    *[load("west", t, t // 100) for t in (1000, 2000, 3000, 4000, 5000)],
                ]})
                cagg_query = {"start": 0, "end": 10000, "step": 1000, "origin": 0,
                              "group_by": ["zone"], "function": "avg",
                              "matchers": [{"label": "__name__", "op": "=", "value": "load"}]}
                defined = op("continuous define", cagg_space + "/continuous/load_avg",
                             cagg_query, method="PUT")
                if defined["pending_windows"] != 0 or len(defined["windows"]) != 5:
                    raise RuntimeError("initial continuous materialization is not current")
                op("continuous duplicate", cagg_space + "/continuous/load_avg",
                   cagg_query, method="PUT", status=409)
                op("continuous missing", cagg_space + "/continuous/absent", status=404)
                op("continuous negative limit",
                   cagg_space + "/continuous/load_avg/refresh", {"max_windows": -1}, status=400)
                # Backfill an old window, overwrite an existing timestamp and
                # report the first point of a not-yet-materialized window.
                op("continuous backfill", cagg_space + "/points", {"now": 10000, "points": [
                    load("east", 1500, 100), load("east", 2000, 200), load("east", 6500, 6),
                ]})
                # A delete and a retention advance invalidate further windows.
                op("continuous delete", cagg_space + "/deletions", {
                    "start": 2500, "end": 3500,
                    "matchers": [{"label": "zone", "op": "=", "value": "east"}]})
                op("continuous retention", cagg_space + "/retention", {"now": 22000})
                lagging = op("continuous pending", cagg_space + "/continuous/load_avg")
                if lagging["pending"] != [1000, 2000, 3000, 6000]:
                    raise RuntimeError("mutations did not invalidate the expected windows")
                flags = {w["start"]: w["pending"] for w in lagging["windows"]}
                if flags != {1000: True, 2000: True, 3000: True, 4000: False, 5000: False}:
                    raise RuntimeError("read view must separate usable from pending windows")
                # Invalidations are committed with each mutation, so a restart
                # cannot lose them.
                stop(process)
                process, base = launch(binary, scratch / "source.db", log)
                resumed = op("continuous pending after restart",
                             cagg_space + "/continuous/load_avg")
                if resumed["pending"] != lagging["pending"]:
                    raise RuntimeError("restart lost invalidated windows")
                # Bounded refresh repairs the backlog oldest first, two
                # windows per call: batch one drops the window the retention
                # floor passed and applies the correction, batch two applies
                # the delete and materializes the brand-new window.
                batch = op("continuous refresh batch 1",
                           cagg_space + "/continuous/load_avg/refresh", {"max_windows": 2})
                if batch["dropped"] != [1000] or batch["remaining"] != 2:
                    raise RuntimeError("first bounded refresh repaired the wrong windows")
                east2000 = [c for w in batch["refreshed"] for c in w["cells"]
                            if w["start"] == 2000 and c["labels"].get("zone") == "east"]
                if east2000 != [{"labels": {"zone": "east"}, "start": 2000, "end": 3000,
                                 "value": 200, "count": 1}]:
                    raise RuntimeError("correction was not applied by refresh")
                batch = op("continuous refresh batch 2",
                           cagg_space + "/continuous/load_avg/refresh", {"max_windows": 2})
                if batch["remaining"] != 0 or len(batch["refreshed"]) != 2:
                    raise RuntimeError("second bounded refresh did not finish the backlog")
                recovered = op("continuous recovered", cagg_space + "/continuous/load_avg")
                if recovered["pending_windows"] != 0:
                    raise RuntimeError("continuous aggregate still pending after refresh")
                direct = op("continuous crosscheck", cagg_space + "/aggregate", cagg_query)
                key = lambda c: (json.dumps(c["labels"], sort_keys=True), c["start"])
                stored = sorted((c for w in recovered["windows"] for c in w["cells"]), key=key)
                if stored != sorted(direct["cells"], key=key):
                    raise RuntimeError("continuous result differs from full aggregation")

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
