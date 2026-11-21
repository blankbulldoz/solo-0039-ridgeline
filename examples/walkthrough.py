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
    "rules"
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
                exercise_rules(space, op)
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


def source_rule(matchers, function, group_by):
    return {"kind": "source", "source": {
        "matchers": matchers, "function": function,
        "step": 1000, "origin": 0, "group_by": group_by,
    }}


def exercise_rules(space, op):
    """Show one shared sub-rule and a definition update over the same data view."""
    window = {"start": -5000, "end": 10000}

    op("rule ingest", space + "/points", {
        "now": 10000, "points": [
            {"labels": {"__name__": "cpu", "node": "ridge-a"}, "time": 100, "value": 4},
            {"labels": {"__name__": "cpu", "node": "ridge-b"}, "time": 100, "value": 6},
            {"labels": {"__name__": "memory", "node": "ridge-a"}, "time": 1000, "value": 2},
            {"labels": {"__name__": "memory", "node": "ridge-b"}, "time": 1000, "value": 8},
        ]})
    op("rule cpu", space + "/rules/cpu_rules",
       source_rule([{"label": "__name__", "op": "=", "value": "cpu"}], "sum", ["node"]),
       method="PUT")
    op("rule usage", space + "/rules/usage_rules",
       source_rule([{"label": "__name__", "op": "=", "value": "memory"}], "sum", ["node"]),
       method="PUT")
    # Two derived rules reference the same source rule. The shared node must be
    # computed once and decoded once, not once per referencing rule.
    op("rule per node", space + "/rules/per_node_rules",
       {"kind": "derived", "inputs": ["cpu_rules"], "combine": "sum"}, method="PUT")
    op("rule total", space + "/rules/total_rules",
       {"kind": "derived", "inputs": ["cpu_rules", "usage_rules"], "combine": "sum"},
       method="PUT")
    # The fleet root reaches cpu_rules through two branches, so cpu_rules is a
    # shared dependency of a single requested view.
    op("rule fleet", space + "/rules/fleet_rules",
       {"kind": "derived", "inputs": ["per_node_rules", "total_rules"], "combine": "max"},
       method="PUT")
    op("rule dependents", space + "/rules/cpu_rules")
    first = op("rule run", space + "/rules/fleet_rules/runs", {**window, "record": True},
               method="POST")
    if first["definition_stale"] or first["data_stale"]:
        raise RuntimeError("a fresh rule run should not be stale")
    order = first["order"]
    if order[-1] != "fleet_rules" or order.count("cpu_rules") != 1:
        raise RuntimeError("a shared sub-rule must appear exactly once in the run")
    shared = next(n for n in first["nodes"] if n["name"] == "cpu_rules")
    if not shared["shared"] or shared["references"] != 2:
        raise RuntimeError("the shared sub-rule should be marked shared and referenced twice")
    if first["decoded"] != 2 * shared["decoded"]:
        raise RuntimeError("shared work must be decoded once per run")
    op("rule cycle", space + "/rules/cpu_rules",
       {"kind": "derived", "inputs": ["total_rules"], "combine": "sum"},
       method="PUT", status=409)
    op("rule missing input", space + "/rules/dangling",
       {"kind": "derived", "inputs": ["absent_rules"], "combine": "sum"},
       method="PUT", status=404)
    op("rule budget", space + "/rules/fleet_rules/runs", {**window, "max_points": 1},
       method="POST", status=422)
    # Redefine the shared source rule. The recorded run must keep executing
    # revision 1 and explain that revision 2 has since replaced it.
    op("rule update", space + "/rules/cpu_rules",
       source_rule([{"label": "__name__", "op": "=", "value": "cpu"}], "max", ["node"]),
       method="PUT")
    stored = op("rule run provenance", space + "/runs/" + first["run"])
    changed = {s["name"]: s for s in stored["revisions"]}
    if not changed["cpu_rules"]["changed"] or not stored["definition_stale"]:
        raise RuntimeError("the stored run should report its rule definition changed")
    if changed["cpu_rules"]["executed"] != 1 or changed["cpu_rules"]["current"] != 2:
        raise RuntimeError("the stored run must keep the revision it actually executed")
    frozen = next(d for d in stored["definitions"] if d["name"] == "cpu_rules")
    if frozen["revision"] != 1 or frozen["rule"]["source"]["function"] != "sum":
        raise RuntimeError("the stored run must embed the executed definition")
    op("rule revisions", space + "/rules/cpu_rules/revisions")
    op("rule delete referenced", space + "/rules/cpu_rules", method="DELETE", status=409)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    exercise(args.workflow, args.output)
