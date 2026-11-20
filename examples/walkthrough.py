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
    "delete", "retention", "materialize", "freshness", "compact", "backup",
    "explain", "vector"
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


VECTOR_POINTS = [
    {"labels": {"__name__": "busy_total", "node": "node-a", "rack": "r1"}, "time": 1000, "value": 30},
    {"labels": {"__name__": "busy_total", "node": "node-a", "rack": "r1"}, "time": 2000, "value": 45},
    {"labels": {"__name__": "busy_total", "node": "node-a", "rack": "r1"}, "time": 3000, "value": 60},
    {"labels": {"__name__": "busy_total", "node": "node-b", "rack": "r1"}, "time": 1000, "value": 20},
    {"labels": {"__name__": "busy_total", "node": "node-b", "rack": "r1"}, "time": 2000, "value": 25},
    {"labels": {"__name__": "busy_total", "node": "node-c", "rack": "r2"}, "time": 1000, "value": 10},
    {"labels": {"__name__": "busy_total", "node": "node-c", "rack": "r2"}, "time": 3000, "value": 20},
    {"labels": {"__name__": "capacity", "rack": "r1"}, "time": 1000, "value": 100},
    {"labels": {"__name__": "capacity", "rack": "r1"}, "time": 2000, "value": 100},
    {"labels": {"__name__": "capacity", "rack": "r1"}, "time": 3000, "value": 0},
    {"labels": {"__name__": "capacity", "rack": "r2"}, "time": 1000, "value": 50},
    {"labels": {"__name__": "capacity", "rack": "r2"}, "time": 2000, "value": 50},
    {"labels": {"__name__": "node_capacity", "node": "node-a"}, "time": 1000, "value": 60},
    {"labels": {"__name__": "node_capacity", "node": "node-a"}, "time": 2000, "value": 60},
    {"labels": {"__name__": "node_capacity", "node": "node-b"}, "time": 1000, "value": 40},
    {"labels": {"__name__": "node_capacity", "node": "node-b"}, "time": 2000, "value": 40},
]


def vector_workflow(scratch, binary, observations):
    """Show a server-side vector join, its refusals and a corrected recompute."""
    with (scratch / "vector.log").open("wb") as log:
        process, base = launch(binary, scratch / "vector.db", log)
        try:
            space = "spaces/weave"

            def op(label, route, payload=None, **options):
                out = request(base, route, payload, **options)
                observations.append({"step": label, "request": payload, "response": out})
                return out

            op("vector namespace", "spaces", {
                "name": "weave", "retention_ms": 1000000, "lateness_ms": 30000,
                "future_ms": 1000, "max_series": 100, "max_write": 1000, "max_scan": 10000,
            })
            op("vector ingest", space + "/points", {"now": 10000, "points": VECTOR_POINTS})
            query = {
                "start": 0, "end": 10000,
                "left": {"matchers": [{"label": "__name__", "op": "=", "value": "busy_total"}]},
                "right": {"matchers": [{"label": "__name__", "op": "=", "value": "capacity"}]},
                "on": ["rack"], "op": "/", "relationship": "many_to_one",
            }
            first = op("vector many-to-one", space + "/vector", query)

            rows = first["points"]
            if any(row["value"] is not None for row in rows if row["status"] != "ok"):
                raise RuntimeError("undefined vector row published a numeric value")
            r2 = [row for row in rows if row["key"]["rack"] == "r2"]
            if not any(row["time"] == 2000 and row["status"] == "missing_left"
                       and row["value"] is None for row in r2):
                raise RuntimeError("left gap did not surface as missing_left")
            if not any(row["time"] == 3000 and row["status"] == "missing_right"
                       and row["value"] is None for row in r2):
                raise RuntimeError("right gap did not surface as missing_right")
            if not any(row["key"]["rack"] == "r1" and row["time"] == 3000
                       and row["status"] == "zero_denominator" and row["value"] is None
                       for row in rows):
                raise RuntimeError("zero denominator was not flagged")
            r1_1000 = [row["value"] for row in rows
                       if row["key"]["rack"] == "r1" and row["time"] == 1000]
            if sorted(r1_1000) != [0.2, 0.3]:
                raise RuntimeError("shared capacity was not applied to each instance")
            if first["stats"]["rows"] != 9 or first["stats"]["ok"] != 5:
                raise RuntimeError("vector row counts do not describe the join")

            op("vector one-to-one refusal", space + "/vector",
               {**query, "relationship": "one_to_one"}, status=409)

            one_to_one = {
                "start": 0, "end": 10000,
                "left": {"matchers": [{"label": "__name__", "op": "=", "value": "busy_total"},
                                      {"label": "rack", "op": "=", "value": "r1"}]},
                "right": {"matchers": [{"label": "__name__", "op": "=", "value": "node_capacity"}]},
                "on": ["node"], "op": "/", "relationship": "one_to_one",
            }
            paired = op("vector one-to-one", space + "/vector", one_to_one)
            if len(paired["points"]) != 5 or paired["stats"]["ok"] != 4 or \
                    paired["stats"]["missing_right"] != 1:
                raise RuntimeError("one-to-one join did not pair per node")
            if sorted(row["value"] for row in paired["points"] if row["time"] == 1000) != [0.5, 0.5]:
                raise RuntimeError("one-to-one values do not match per-node capacity")

            duplicate = {"labels": {"__name__": "capacity", "rack": "r2", "scope": "cluster"},
                         "time": 1000, "value": 999}
            op("vector duplicate ingestion", space + "/points",
               {"now": 10000, "points": [duplicate]})
            op("vector duplicate key refusal", space + "/vector", query, status=409)

            op("vector correction", space + "/points", {"now": 10000, "points": [
                {"labels": {"__name__": "busy_total", "node": "node-b", "rack": "r1"},
                 "time": 3000, "value": 30},
                {"labels": {"__name__": "busy_total", "node": "node-c", "rack": "r2"},
                 "time": 2000, "value": 15},
                {"labels": {"__name__": "capacity", "rack": "r2"}, "time": 3000, "value": 40},
                {"labels": {"__name__": "capacity", "rack": "r1"}, "time": 3000, "value": 120},
            ]})
            op("vector duplicate removal", space + "/deletions", {
                "start": -5000, "end": 10000,
                "matchers": [{"label": "__name__", "op": "=", "value": "capacity"},
                             {"label": "scope", "op": "=", "value": "cluster"}],
            })
            corrected = op("vector recompute", space + "/vector", query)
            stats = corrected["stats"]
            if stats["ok"] != stats["rows"] or stats["missing_left"] or \
                    stats["missing_right"] or stats["zero_denominator"]:
                raise RuntimeError("corrected vector still reports undefined rows")
            if [row["status"] for row in corrected["points"]].count("ok") != len(corrected["points"]):
                raise RuntimeError("corrected vector contains a non-ok row")
            if corrected["sequence"] != first["sequence"] + 3:
                raise RuntimeError("vector result does not report one snapshot sequence")
        finally:
            stop(process)


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
        vector_workflow(scratch, binary, observations)
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
