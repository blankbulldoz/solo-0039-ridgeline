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


def diagnostics(base, op, observations):
    """Show when a narrower scan helps, when it cannot, and what a rejection proves."""
    def make_space(name, max_write, max_scan):
        return op("create " + name, "spaces", {
            "name": name, "retention_ms": 100000000, "lateness_ms": 100000000,
            "future_ms": 100000, "max_series": 1000, "max_write": max_write,
            "max_scan": max_scan})

    def write(space, points):
        # Bulk diagnostic writes are summarised, not echoed, so the recorded
        # session stays readable while the request still runs verbatim.
        out = request(base, space + "/points", {"now": 100000, "points": points})
        observations.append({"step": "write " + space, "series": sorted(
            {json.dumps(p["labels"], sort_keys=True) for p in points}),
            "points": len(points), "response": out})
        return out

    def every(name, node, start, stop, step, value):
        return [{"labels": {"__name__": name, "node": node}, "time": t, "value": value}
                for t in range(start, stop, step)]

    # A narrow interval that still decodes a whole block, and a request whose
    # budget cannot cover it. The rejection must carry the same view.
    make_space("granite", 100, 200)
    wide = every("wide", "w", 0, 301, 1, 1)
    for offset in range(0, len(wide), 100):
        write("spaces/granite", wide[offset:offset + 100])
    # One seal call packs every head point into a single wide block.
    op("seal granite", "spaces/granite/seal", {})
    narrow = {"start": 100, "end": 110,
              "matchers": [{"label": "__name__", "op": "=", "value": "wide"}]}
    narrow_plan = op("narrow plan in a big block", "spaces/granite/explain", narrow)
    if narrow_plan["totals"]["decode_points"] <= 200:
        raise RuntimeError("a narrow interval should still count the whole block")
    if narrow_plan["narrowing"]["time"]["straddling_block_points"] == 0:
        raise RuntimeError("narrow interval should report a straddling block")
    if narrow_plan["budget"]["within"]:
        raise RuntimeError("narrow interval should exceed the small scan budget")
    failure = request(base, "spaces/granite/range", narrow, status=422)
    observations.append({"step": "narrow rejection", "request": narrow, "response": failure})
    evidence = failure["evidence"]
    if evidence["observed"]["scope"] != "fully read series only":
        raise RuntimeError("a rejection must not project partial series over the query")
    if evidence["partial"]["series"] != narrow_plan["budget"]["rejected_source"]["series"]:
        raise RuntimeError("rejection and plan disagree on the stopping series")
    if evidence["reconcile"]["matches"]:
        raise RuntimeError("a rejected read cannot match the whole plan")
    if evidence["planned"]["decode_points"] != narrow_plan["totals"]["decode_points"]:
        raise RuntimeError("rejection evidence and plan disagree on planned decode")

    # Same interval cost, but older versions stack under the newer ones.
    make_space("atlas", 10000, 100000)
    write("spaces/atlas", every("versions", "v", 0, 100, 10, 5))
    op("seal atlas", "spaces/atlas/seal", {})
    write("spaces/atlas", [{"labels": {"__name__": "versions", "node": "v"},
                            "time": t, "value": 9} for t in range(0, 100, 10)])
    op("seal correction", "spaces/atlas/seal", {})
    stacked = {"start": 0, "end": 100,
               "matchers": [{"label": "__name__", "op": "=", "value": "versions"}]}
    stacked_plan = op("stacked versions plan", "spaces/atlas/explain", stacked)
    if stacked_plan["totals"]["series_with_version_overlap"] == 0:
        raise RuntimeError("stacked blocks should be reported as version overlap")
    if stacked_plan["totals"]["certain_visible_series"] != 0:
        raise RuntimeError("stacked versions must not claim a certain visible count")
    if stacked_plan["series"][0]["certainty"] != "upper_only":
        raise RuntimeError("stacked versions must not claim an exact visible count")
    if not stacked_plan["storage"]["compact_resolves"]:
        raise RuntimeError("stacked versions are resolvable by compaction")
    stacked_read = op("stacked versions read", "spaces/atlas/range", stacked)
    if stacked_read["stats"]["decoded"] != stacked_plan["totals"]["decode_points"]:
        raise RuntimeError("read decode should match the planned decode")
    if stacked_read["evidence"]["observed"]["superseded"] == 0:
        raise RuntimeError("overlap should be visible only after decoding")
    op("compact atlas", "spaces/atlas/compact", {})
    resolved = op("stacked versions after compaction", "spaces/atlas/explain", stacked)
    if resolved["series"][0]["certainty"] != "exact":
        raise RuntimeError("compaction should resolve the version overlap")
    if resolved["totals"]["decode_points"] >= stacked_plan["totals"]["decode_points"]:
        raise RuntimeError("compaction should remove stacked decode work")

    # A large head that the matcher excludes cannot explain the cost.
    write("spaces/atlas", every("chatter", "c", 0, 301, 1, 3))
    write("spaces/atlas", every("quiet", "q", 0, 3, 1, 1))
    quiet = {"start": 0, "end": 400,
             "matchers": [{"label": "__name__", "op": "=", "value": "quiet"},
                          {"label": "node", "op": "=", "value": "q"}]}
    quiet_plan = op("head excluded by matcher", "spaces/atlas/explain", quiet)
    if quiet_plan["totals"]["head_records_namespace"] == 0:
        raise RuntimeError("namespace head should be visible as a size signal")
    if quiet_plan["totals"]["head_records"] != 3:
        raise RuntimeError("only the matching series' head records are decode work")
    if quiet_plan["totals"]["head_records_namespace"] < 300:
        raise RuntimeError("the excluded head should still be visible as a size signal")
    if quiet_plan["totals"]["decode_points"] != 3:
        raise RuntimeError("a selective matcher should avoid almost all head work")


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
                plan = op("explain", space + "/explain", q)
                if plan["totals"]["decode_points"] != plan["budget"]["planned"]:
                    raise RuntimeError("explain decode total disagrees with its budget projection")
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
                diagnostics(base, op, observations)
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
