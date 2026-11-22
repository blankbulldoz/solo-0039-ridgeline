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
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain",
    "drain",
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

                # Staged drain: acceptance stops immediately, physical
                # maintenance stays available, queries keep working.
                writable = op("drain preconditions", space)
                op("drain start", space + "/drain", {})
                draining = op("drain status", space + "/drain")
                if draining["state"] != "draining" or draining["accepting_logical"]:
                    raise RuntimeError("drain must stop acceptance")
                if not draining["physical_maintenance"]:
                    raise RuntimeError("drain must keep physical maintenance")
                if draining["started_sequence"] <= writable["last_sequence"]:
                    raise RuntimeError("drain start must have its own sequence")
                during = {"now": 11000, "points": [{**seed["points"][0], "time": 1500, "value": 11}]}
                op("drain write rejection", space + "/points", during, status=409)
                op("drain delete rejection", space + "/deletions",
                   {**q, "start": 1200, "end": 1300}, status=409)
                op("drain retention rejection", space + "/retention", {"now": 12000}, status=409)
                op("drain materialize rejection", space + "/rollups/blocked", agg,
                   method="PUT", status=409)
                op("drain query", space + "/range", q)
                op("drain seal", space + "/seal", {})
                op("drain compact", space + "/compact", {})
                canceled = op("drain cancel", space + "/drain", method="DELETE")
                if canceled["drain"]["state"] != "writable" or not canceled["drain"]["accepting_writes"]:
                    raise RuntimeError("cancel must return to writable")
                if canceled["drain"]["canceled_sequence"] <= draining["started_sequence"]:
                    raise RuntimeError("cancel must have its own sequence")
                op("write after drain cancel", space + "/points", during)

                # Complete drain: the maintainable state rejects logical
                # mutations but still honours physical maintenance.
                op("second drain start", space + "/drain", {})
                done = op("drain complete", space + "/drain/complete", {})
                if done["drain"]["state"] != "read_only" or not done["drain"]["maintainable"]:
                    raise RuntimeError("drain completion must be maintainable")
                if done["drain"]["completed_sequence"] <= done["drain"]["started_sequence"]:
                    raise RuntimeError("completion must have its own sequence")
                op("maintainable write rejection", space + "/points", during, status=409)
                op("maintainable seal", space + "/seal", {})
                op("maintainable compact", space + "/compact", {})
                op("maintainable query", space + "/range", q)
                op("maintainable resume", space + "/drain", method="DELETE")

                # Leave the namespace draining when the image is taken, so the
                # restart inspection below sees a real interrupted drain.
                op("drain before image", space + "/drain", {})
                pending = op("drain status before image", space + "/drain")
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
                recovered = request(base, "spaces/spindle")
                observations.append({"step": "restart drain status", "response": recovered})
                drain = recovered["drain"]
                if drain["state"] != "draining" or drain["completed_sequence"]:
                    raise RuntimeError("restart must preserve the interrupted drain")
                if not drain["restarted"] or drain["in_flight"] != 0:
                    raise RuntimeError("restart must expose an empty in-flight set")
                if drain["started_sequence"] != pending["started_sequence"]:
                    raise RuntimeError("restart must preserve the drain sequence")
                request(base, "spaces/spindle/points", during, status=409)
                restored_done = request(base, "spaces/spindle/drain/complete", {})
                observations.append({"step": "restart drain complete", "response": restored_done})
                if restored_done["drain"]["completed_sequence"] <= drain["started_sequence"]:
                    raise RuntimeError("restart completion must be sequenced")
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
