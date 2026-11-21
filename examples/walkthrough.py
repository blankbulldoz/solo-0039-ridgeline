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
    "annotations", "seal", "delete", "retention", "materialize", "freshness",
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
                calibration = op("annotate calibration", space + "/annotations", {
                    "start": -1000, "end": 2500, "kind": "calibration",
                    "note": "east analyzer recalibrated; expect offset drift",
                    "matchers": [{"label": "zone", "op": "=", "value": "east"}]})
                maintenance = op("annotate maintenance", space + "/annotations", {
                    "start": 400, "end": 1600, "kind": "maintenance",
                    "note": "link maintenance on ridge-a",
                    "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                annotated = op("annotated range", space + "/range", q)
                if annotated["series"] != before["series"]:
                    raise RuntimeError("annotations changed the logical samples")
                if [a["kind"] for a in annotated["annotations"]] != ["calibration", "maintenance"]:
                    raise RuntimeError("range did not return covering annotations")
                fixed = op("correct annotation", space + "/annotations/" + maintenance["id"], {
                    "start": 400, "end": 2000, "kind": "maintenance",
                    "note": "link maintenance on ridge-a extended after carrier delay"},
                    method="PUT")
                if fixed["revisions"] != 1 or fixed["series"] != maintenance["series"]:
                    raise RuntimeError("correction lost the original series binding")
                windowed = op("annotated windows", space + "/aggregate", agg)
                notes = windowed["annotations"]
                if [a["kind"] for a in notes] != ["calibration", "maintenance"]:
                    raise RuntimeError("aggregate did not return covering annotations")
                east = [c for c in windowed["cells"] if c["labels"].get("zone") == "east"]
                spanned = [c for c in east
                           if c["start"] >= notes[0]["start"] and c["end"] <= notes[0]["end"]]
                if len(spanned) < 3:
                    raise RuntimeError("calibration annotation does not span multiple windows")
                joint = [c for c in east if c["start"] == 1000 and c["end"] == 2000]
                if not joint or not all(a["start"] < 2000 and a["end"] > 1000 for a in notes):
                    raise RuntimeError("overlapping annotations do not jointly cover [1000,2000)")
                op("seal", space + "/seal", {})
                sealed = op("sealed range", space + "/range", q)
                if before["series"] != sealed["series"]:
                    raise RuntimeError("seal changed the logical samples")
                if [a["id"] for a in sealed["annotations"]] != [calibration["id"], fixed["id"]]:
                    raise RuntimeError("seal hid interval annotations")
                op("blocks", space + "/blocks")
                op("delete", space + "/deletions", {**q, "start": -600, "end": 600,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                op("deletion inventory", space + "/deletions")
                replacement = {"now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 8}]}
                op("correct after delete", space + "/points", replacement)
                late_point = {"labels": {"__name__": "pulse_total", "node": "ridge-c", "zone": "east"},
                              "time": 500, "value": 5}
                op("late series", space + "/points", {"now": 10000, "points": [late_point]})
                late = op("annotation non-inheritance", space + "/range",
                          {**q, "matchers": [{"label": "node", "op": "=", "value": "ridge-c"}]})
                if late["annotations"]:
                    raise RuntimeError("late series inherited old annotations")
                sourced = op("annotation source series", space + "/range",
                             {**q, "matchers": [{"label": "zone", "op": "=", "value": "east"}]})
                covered = {sid for a in sourced["annotations"] for sid in a["series"]}
                late_ids = {s["series"]["id"] for s in late["series"]}
                if not covered or covered & late_ids:
                    raise RuntimeError("annotation binding is not frozen at creation")
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
                inventory = op("annotation inventory", space + "/annotations")
                if [a["id"] for a in inventory] != [calibration["id"], fixed["id"]]:
                    raise RuntimeError("compaction dropped interval annotations")
                if inventory[1]["end"] != 2000 or inventory[1]["revisions"] != 1:
                    raise RuntimeError("compaction lost the annotation correction")
                op("explain", space + "/explain", q)
                op("retention", space + "/retention", {"now": 11000})
                op("retention no resurrection", space + "/points", replacement, status=409)
                aged = op("post-retention annotations", space + "/range", q)
                if aged["floor"] != 1000 or len(aged["annotations"]) != 2:
                    raise RuntimeError("retention hid interval annotations")
                op("read-only", space + "/mode", {"read_only": True}, method="PUT")
                op("read-only rejection", space + "/points", {
                    "now": 11000, "points": [{**seed["points"][0], "time": 1500}]
                }, status=409)
                op("read-only annotation rejection", space + "/annotations", {
                    "start": 0, "end": 10, "kind": "audit", "note": "rejected while read-only",
                    "matchers": []}, status=409)
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
