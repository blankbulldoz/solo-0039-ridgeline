"""Run a bounded production API demonstration in a temporary directory."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
FLOW_NAMES = [
    "namespace", "ingest", "discovery", "range", "windows", "counters", "seal",
    "delete", "retention", "materialize", "freshness", "refresh", "overtake",
    "compact", "backup", "explain"
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

                # A failed refresh keeps the last success readable and the
                # attempt definition, source and failure reason queryable.
                ledger = {"name": "ledger", "retention_ms": 100000, "lateness_ms": 30000,
                          "future_ms": 1000, "max_series": 16, "max_write": 4, "max_scan": 8}
                op("refresh namespace", "spaces", ledger)

                def ledger_points(*times):
                    return [{"labels": {"__name__": "pulse_total", "node": "ridge-c"},
                             "time": t, "value": 1} for t in times]

                op("refresh seed", "spaces/ledger/points",
                   {"now": 11000, "points": ledger_points(0, 1000, 2000, 3000)})
                narrow = {"start": 0, "end": 4000, "step": 1000, "origin": 0,
                          "group_by": ["node"], "function": "sum"}
                wide = {**narrow, "end": 12000}
                first = op("refresh materialize", "spaces/ledger/rollups/daily", narrow, method="PUT")
                if first["stale"] or first["attempt"]["status"] != "ok" or first["attempt"]["overtaken"]:
                    raise RuntimeError("first materialization should be fresh and undisputed")
                op("refresh more points", "spaces/ledger/points",
                   {"now": 11000, "points": ledger_points(4000, 5000, 6000, 7000)})
                op("refresh more points", "spaces/ledger/points",
                   {"now": 11000, "points": ledger_points(8000, 9000, 10000, 11000)})
                over_budget = op("refresh over budget", "spaces/ledger/rollups/daily",
                                 wide, method="PUT", status=422)
                if over_budget["code"] != "capacity":
                    raise RuntimeError("expected a capacity rejection")
                kept = op("refresh kept state", "spaces/ledger/rollups/daily")
                if kept["current"]["query"]["end"] != narrow["end"] or \
                        kept["attempt"]["query"]["end"] != wide["end"]:
                    raise RuntimeError("failed definition must not merge into the stored result")
                if kept["current"]["result"] != first["current"]["result"]:
                    raise RuntimeError("failed refresh must keep the last successful result")
                if kept["attempt"]["status"] != "failed" or \
                        kept["attempt"]["failure"]["code"] != "capacity":
                    raise RuntimeError("failed attempt must stay queryable")
                if not kept["stale"]:
                    raise RuntimeError("kept result trails the namespace after later writes")
                retry = op("refresh retry", "spaces/ledger/rollups/daily", narrow, method="PUT")
                if retry["stale"] or retry["attempt"]["status"] != "ok" or "failure" in retry["attempt"]:
                    raise RuntimeError("retry should produce a fresh success")
                unlucky = op("refresh first failure", "spaces/ledger/rollups/everything",
                             wide, method="PUT", status=422)
                if unlucky["code"] != "capacity":
                    raise RuntimeError("expected a capacity rejection")
                empty = op("refresh no success yet", "spaces/ledger/rollups/everything")
                if empty["current"] is not None or empty["attempt"]["status"] != "failed":
                    raise RuntimeError("a failed first attempt has no result but stays queryable")

                # An expensive refresh scans a snapshot: readers keep the old
                # result, writes are not blocked, and the published attempt
                # reports whether new writes overtook it before completion.
                burst = {"name": "burst", "retention_ms": 1000000, "lateness_ms": 300000,
                         "future_ms": 1000, "max_series": 16, "max_write": 10000,
                         "max_scan": 200000}
                op("overtake namespace", "spaces", burst)

                def burst_points(start, stop):
                    return [{"labels": {"__name__": "pulse_total", "node": "ridge-d"},
                             "time": t, "value": 1} for t in range(start, stop)]

                for base_t in range(0, 60000, 10000):
                    receipt = request(base, "spaces/burst/points",
                                      {"now": 100000, "points": burst_points(base_t, base_t + 10000)})
                observations.append({"step": "overtake seed", "points": 60000, "response": receipt})
                total = {"start": 0, "end": 100000, "step": 100000, "origin": 0,
                         "group_by": ["node"], "function": "sum"}
                established = op("overtake materialize", "spaces/burst/rollups/total",
                                 total, method="PUT")
                if established["stale"]:
                    raise RuntimeError("undisturbed materialization should be fresh")
                late_count = 0
                raced = None
                for _ in range(3):
                    before = request(base, "spaces/burst/rollups/total")
                    outcome = {}
                    worker = threading.Thread(target=lambda: outcome.update(view=request(
                        base, "spaces/burst/rollups/total", total, method="PUT")))
                    worker.start()
                    during = request(base, "spaces/burst/rollups/total")
                    observations.append({"step": "overtake read during refresh", "response": during})
                    if during["current"] != before["current"]:
                        raise RuntimeError("readers must keep the previous result during a refresh")
                    time.sleep(0.02)
                    op("overtake late report", "spaces/burst/points", {"now": 100000, "points": [{
                        "labels": {"__name__": "pulse_total", "node": "ridge-d"},
                        "time": 60000 + late_count, "value": 1}]})
                    late_count += 1
                    worker.join()
                    raced = outcome["view"]
                    observations.append({"step": "overtake refresh", "response": raced})
                    if raced["attempt"]["overtaken"]:
                        break
                else:
                    raise RuntimeError("late report never overtook the refresh")
                if not raced["stale"] or raced["attempt"]["status"] != "ok":
                    raise RuntimeError("an overtaken refresh completes already stale")
                caught = op("overtake caught state", "spaces/burst/rollups/total")
                if not caught["stale"] or caught["attempt"]["status"] != "ok":
                    raise RuntimeError("overtaken state should remain readable")
                final = op("overtake re-refresh", "spaces/burst/rollups/total", total, method="PUT")
                if final["stale"] or final["attempt"]["overtaken"]:
                    raise RuntimeError("undisturbed re-refresh should complete current")
                cells = final["current"]["result"]["cells"]
                if len(cells) != 1 or cells[0]["value"] != 60000 + late_count:
                    raise RuntimeError("re-refresh must include the late reports")
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
