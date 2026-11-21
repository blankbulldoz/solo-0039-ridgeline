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
    "partial",
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


def partial_flow(workflow, scratch, binary, corrupt, observations):
    """Query one healthy series and one unreadable series together.

    A sealed namespace holds two series; one block payload is then damaged on
    disk. Strict reads still reject the whole range, while a partial read
    reports complete/failed/not_executed per series and withholds the affected
    aggregate groups instead of presenting an incomplete source as a full set.
    """
    namespace = {
        "name": "ridge-partial", "retention_ms": 10000, "lateness_ms": 30000,
        "future_ms": 1000, "max_series": 100, "max_write": 10000, "max_scan": 100000,
    }
    healthy = {"__name__": "load_ratio", "node": "ridge-a", "zone": "east"}
    healthy_west = {"__name__": "load_ratio", "node": "ridge-d", "zone": "west"}
    damaged = {"__name__": "load_ratio", "node": "ridge-b", "zone": "west"}
    points = [
        {"labels": healthy, "time": 0, "value": 1},
        {"labels": healthy, "time": 1000, "value": 2},
        {"labels": healthy_west, "time": 0, "value": 5},
        {"labels": healthy_west, "time": 1000, "value": 7},
        {"labels": damaged, "time": 0, "value": 10},
        {"labels": damaged, "time": 1000, "value": 20},
    ]
    space = "spaces/" + namespace["name"]
    query = {"start": 0, "end": 2000}
    aggregate = {**query, "step": 1000, "origin": 0, "group_by": ["zone"], "function": "sum"}

    with (scratch / "partial.log").open("wb") as log:
        process, base = launch(binary, scratch / "partial.db", log)
        try:
            def op(label, route, payload=None, **options):
                out = request(base, route, payload, **options)
                observations.append({"step": label, "request": payload, "response": out})
                return out

            op("partial namespace", "spaces", namespace)
            op("partial ingest", space + "/points", {"now": 10000, "points": points})
            op("partial seal", space + "/seal", {})
            damaged_id = next(
                s["series"]["id"] for s in op("partial series", space + "/range", query)["series"]
                if s["series"]["labels"]["node"] == "ridge-b"
            )
        finally:
            stop(process)

        subprocess.run(
            [str(corrupt), "-db", str(scratch / "partial.db"),
             "-space", namespace["name"], "-series", damaged_id],
            check=True,
        )

        process, base = launch(binary, scratch / "partial.db", log)
        try:
            def op(label, route, payload=None, **options):
                out = request(base, route, payload, **options)
                observations.append({"step": label, "request": payload, "response": out})
                return out

            strict = op("strict range fails whole request", space + "/range", query, status=500)
            if strict.get("code") != "corrupt":
                raise RuntimeError(f"strict read should report corrupt sources, got {strict}")

            range_out = op("partial range", space + "/range", {**query, "partial": True})
            statuses = {s["series"]["labels"]["node"]: s["status"] for s in range_out["series"]}
            if statuses != {"ridge-a": "complete", "ridge-b": "failed", "ridge-d": "complete"}:
                raise RuntimeError(f"unexpected partial statuses {statuses}")
            for series in range_out["series"]:
                if series["status"] != "complete" and series["points"]:
                    raise RuntimeError("partial read emitted samples for an incomplete series")

            healthy = op("partial range all complete", space + "/range", {
                **query, "partial": True,
                "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}],
            })
            if healthy.get("partial") or healthy["series"][0]["status"] != "complete":
                raise RuntimeError(f"complete partial read should not flag partial: {healthy}")

            agg_out = op("partial aggregate", space + "/aggregate", {**aggregate, "partial": True})
            if any(cell["labels"] != {"zone": "east"} for cell in agg_out["cells"]):
                raise RuntimeError(f"partial aggregate leaked the west groups: {agg_out['cells']}")
            if [issue["labels"] for issue in agg_out["withheld"]] != [{"zone": "west"}]:
                raise RuntimeError(f"partial aggregate missed withheld group: {agg_out.get('withheld')}")
            if agg_out["withheld"][0]["series_ids"] != [damaged_id]:
                raise RuntimeError(f"withheld group should name only the unreadable series: {agg_out['withheld']}")

            strict = op("strict aggregate fails whole request", space + "/aggregate", aggregate, status=500)
            if strict.get("code") != "corrupt":
                raise RuntimeError(f"strict aggregate should report corrupt sources, got {strict}")

            rejected = op("partial materialization rejected", space + "/rollups/partial",
                          {**aggregate, "partial": True}, method="PUT", status=400)
            if rejected.get("code") != "invalid":
                raise RuntimeError(f"materialization must reject partial reads, got {rejected}")
            strict = op("strict materialization fails whole request", space + "/rollups/partial",
                        aggregate, method="PUT", status=500)
            if strict.get("code") != "corrupt":
                raise RuntimeError(f"strict materialization should fail, got {strict}")

            # A scan budget that one series alone can exhaust must not hide the
            # other devices: the oversized series is reported not executed and
            # the readable one still succeeds.
            budget_space = "spaces/ridge-budget"
            op("partial budget namespace", "spaces", {
                **namespace, "name": "ridge-budget", "max_write": 2, "max_scan": 2,
            })
            op("partial budget ingest", budget_space + "/points", {"now": 10000, "points": [
                {"labels": {"__name__": "load_ratio", "node": "ridge-c"}, "time": 0, "value": 3},
                {"labels": {"__name__": "load_ratio", "node": "ridge-c"}, "time": 500, "value": 4},
            ]})
            op("partial budget small series", budget_space + "/points", {"now": 10000, "points": [
                {"labels": {"__name__": "load_ratio", "node": "ridge-a"}, "time": 0, "value": 1},
            ]})
            op("partial budget seal", budget_space + "/seal", {})
            low = op("partial low budget", budget_space + "/range", {**query, "partial": True})
            by_status = {s["status"]: s for s in low["series"]}
            if sorted(by_status) != ["complete", "not_executed"]:
                raise RuntimeError(f"one oversized series must not hide the other: {list(by_status)}")
            if not by_status["complete"]["points"] or by_status["not_executed"]["points"]:
                raise RuntimeError(f"unexpected budget response: {low['series']}")
            strict = op("strict low budget rejected", budget_space + "/range", query, status=422)
            if strict.get("code") != "capacity":
                raise RuntimeError(f"strict budget should stay a capacity failure, got {strict}")
        finally:
            stop(process)

def exercise(workflow, output):
    seed = json.loads((ROOT / "examples/input.json").read_text())
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-demo-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        corrupt = scratch / "corruptblock"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        subprocess.run(["go", "build", "-o", str(corrupt), "./examples/corruptblock"], cwd=ROOT, check=True)
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
            partial_flow(workflow, scratch, binary, corrupt, observations)
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
