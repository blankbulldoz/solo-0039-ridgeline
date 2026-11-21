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
    "explain", "resample"
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
                spectra = "spaces/spectra"
                op("resample namespace", "spaces", {
                    "name": "spectra", "retention_ms": 2500, "lateness_ms": 10000,
                    "future_ms": 1000, "max_series": 100, "max_write": 10000,
                    "max_scan": 100000})
                strain_c = {"__name__": "strain", "node": "ridge-c", "zone": "north"}
                strain_d = {"__name__": "strain", "node": "ridge-d", "zone": "north"}
                op("resample ingest base", spectra + "/points", {"now": 5000, "points": [
                    {"labels": strain_c, "time": 0, "value": 0},
                    {"labels": strain_c, "time": 400, "value": 4},
                    {"labels": strain_c, "time": 900, "value": 9},
                    {"labels": strain_c, "time": 2000, "value": 2},
                    {"labels": strain_c, "time": 2600, "value": 6},
                    {"labels": strain_d, "time": 0, "value": 1.5},
                    {"labels": strain_d, "time": 3600, "value": 2.5}]})
                op("resample seal", spectra + "/seal", {})
                op("resample ingest more", spectra + "/points", {"now": 6000, "points": [
                    {"labels": strain_c, "time": 3100, "value": 11},
                    {"labels": strain_c, "time": 3900, "value": 3}]})
                op("resample reseal", spectra + "/seal", {})
                grid = {"start": 0, "step": 100, "steps": 40, "max_gap": 1000,
                        "matchers": [{"label": "node", "op": "=", "value": "ridge-c"}]}
                first = op("resample across blocks", spectra + "/resample", grid)
                cells = first["series"][0]["points"]
                if cells[0]["source"] != "observed" or cells[0]["value"] != 0:
                    raise RuntimeError("grid point on a sample must be observed")
                if cells[5]["source"] != "interpolated" or abs(cells[5]["value"] - 5.0) > 1e-9:
                    raise RuntimeError("grid point inside a small gap must interpolate")
                if cells[10]["source"] != "missing" or cells[10]["value"] is not None:
                    raise RuntimeError("gap wider than max_gap must stay missing")
                # Samples at 2600 and 3100 live in different sealed blocks.
                if cells[28]["source"] != "interpolated" or abs(cells[28]["value"] - 8.0) > 1e-9:
                    raise RuntimeError("interpolation must cross the sealed block boundary")
                if cells[31]["source"] != "observed" or cells[39]["value"] != 3:
                    raise RuntimeError("grid endpoints on samples must be observed")
                op("resample correct", spectra + "/points", {
                    "now": 6000, "points": [{"labels": strain_c, "time": 2000, "value": 12}]})
                corrected = op("resample after correction", spectra + "/resample", grid)
                updated = corrected["series"][0]["points"]
                if updated[20]["source"] != "observed" or updated[20]["value"] != 12:
                    raise RuntimeError("correction must replace the observed value")
                if updated[22]["source"] != "interpolated" or \
                        abs(updated[22]["value"] - 10.0) > 1e-9 or \
                        updated[22]["value"] == cells[22]["value"]:
                    raise RuntimeError("correction must change the adjacent intervals")
                op("resample delete", spectra + "/deletions", {"start": 2000, "end": 2601,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-c"}]})
                holed = op("resample deletion hole", spectra + "/resample", grid)
                hollow = holed["series"][0]["points"]
                if any(c["source"] != "missing" for c in hollow[10:31]):
                    raise RuntimeError("deletion hole wider than max_gap must not be filled")
                if hollow[5]["source"] != "interpolated" or hollow[31]["source"] != "observed":
                    raise RuntimeError("deletion must not disturb surviving intervals")
                op("resample retention", spectra + "/retention", {"now": 5000})
                faded = op("resample below floor", spectra + "/resample", grid)
                below = faded["series"][0]["points"]
                if any(c["source"] != "missing" for c in below[:31]):
                    raise RuntimeError("retention floor must not be interpolated across")
                if below[31]["source"] != "observed" or below[36]["source"] != "interpolated":
                    raise RuntimeError("points above the floor must resample normally")
                dense = {"start": 0, "step": 1, "steps": 100000, "max_gap": 1000}
                rejected = op("resample capacity", spectra + "/resample", dense, status=422)
                if rejected["code"] != "capacity":
                    raise RuntimeError("oversized output shape must fail as capacity")
                again = op("resample capacity again", spectra + "/resample", dense, status=422)
                if again != rejected:
                    raise RuntimeError("capacity rejection must be deterministic")
                op("resample invalid grid", spectra + "/resample",
                   {**dense, "steps": 0}, status=400)
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
