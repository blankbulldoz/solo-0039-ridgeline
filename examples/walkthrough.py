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
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain"
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


def partial_compaction(base, op):
    """Compact one label selection and interval while a straddling block and an
    overlapping deletion survive outside the selection, then prove that a
    full-range read still sees every unchanged point plus the carried edges."""
    space = "spaces/spindle"
    q = {"start": -1000, "end": 1000}
    full = {"start": -5000, "end": 10000}
    pulse = [{"label": "__name__", "op": "=", "value": "pulse_total"}]
    ridge = {"__name__": "pulse_total", "node": "ridge-a", "zone": "east"}
    # The edge block of ridge-a spans -1500..1501, so the selected range
    # [-1000, 1000) is nested inside it: the points at -1500 and 1500 lie out of
    # range and must survive the rewrite. A correction and a head point inside
    # the range are folded in, and the overlapping deletion hides -1500/-500.
    op("partial correction", space + "/points", {
        "now": 10000, "points": [{"labels": ridge, "time": -500, "value": 4}]})
    tomb = op("overlapping delete", space + "/deletions", {
        "start": -500, "end": 0,
        "matchers": pulse + [{"label": "node", "op": "=", "value": "ridge-a"}]})
    op("head correction after delete", space + "/points", {
        "now": 10000, "points": [{"labels": ridge, "time": 0, "value": 5}]})
    before = op("full range before partial compaction", space + "/range", full)
    narrow_before = op("in-range read before partial compaction", space + "/range", q)
    receipt = op("partial compact", space + "/compact",
                 {"start": -1000, "end": 1000, "matchers": pulse})
    if receipt["requested"] != {"start": -1000, "end": 1000}:
        raise RuntimeError("compaction receipt lost the requested interval")
    if receipt["selected_series"] != 2 or len(receipt["processed"]) != 1:
        raise RuntimeError("partial compaction should only rewrite ridge-a")
    entry = receipt["processed"][0]
    if entry["series"]["labels"]["node"] != "ridge-a":
        raise RuntimeError("partial compaction rewrote the wrong series")
    if entry["start"] != -1500 or entry["end"] != 1501:
        raise RuntimeError("partial compaction lost the straddling block edges")
    if entry["preserved_points"] != 2 or entry["hidden_points"] != 1:
        raise RuntimeError("partial compaction did not classify carried points")
    if entry["head_points"] != 2:
        raise RuntimeError("partial compaction did not fold the selected head")
    if receipt["tombstones_pruned"] != 1 or receipt["remaining"]["tombstones"] != 0:
        raise RuntimeError("compaction should prune the fully covered deletion")
    untouched = [b for b in receipt["remaining"]["surviving_blocks"]
                 if b["series_id"] != entry["series"]["id"]]
    if not untouched:
        raise RuntimeError("partial compaction should leave sibling blocks alone")
    after = op("full range after partial compaction", space + "/range", full)
    if after["series"] != before["series"]:
        raise RuntimeError("partial compaction changed the full-range read")
    if after["stats"]["head"] >= before["stats"]["head"]:
        raise RuntimeError("partial compaction should fold head corrections")
    again = op("repeat partial compact", space + "/compact",
               {"start": -1000, "end": 1000, "matchers": pulse})
    if again["processed"] or again["blocks_removed"]:
        raise RuntimeError("repeated compaction should converge without rewrites")
    if again["sequence"] != after["sequence"]:
        raise RuntimeError("no-op compaction must not publish a new sequence")
    surviving = op("blocks after partial compaction", space + "/blocks")
    rewritten = [b for b in surviving if b["series_id"] == entry["series"]["id"]]
    if len(rewritten) != 1 or rewritten[0]["start"] != -1500 or rewritten[0]["end"] != 1501:
        raise RuntimeError("rewritten block dropped the carried edge points")
    narrow = op("in-range read after partial compaction", space + "/range", q)
    if narrow["series"] != narrow_before["series"]:
        raise RuntimeError("compaction changed the in-range read")
    return tomb


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
                partial_compaction(base, op)
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
