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
    "namespace", "ingest", "discovery", "range", "windows", "counters", "weighted",
    "seal", "delete", "retention", "materialize", "freshness", "compact",
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


def demonstrate_weighted(base, space, op):
    """Contrast plain avg with step-function time-weighted average and integral.

    Two series describe the same underlying load (4 then 6), but one is sampled
    sparsely and the other densely. A plain average shifts with sampling
    density; the time-weighted average recovers the same mean while exposing how
    much of each window was actually known.
    """
    series_labels = {
        "sparse": {"__name__": "host_load", "node": "sparse"},
        "dense": {"__name__": "host_load", "node": "dense"},
    }
    # Sparse: only two observations, 4000ms apart.
    sparse_points = [
        {"labels": series_labels["sparse"], "time": 0, "value": 4},
        {"labels": series_labels["sparse"], "time": 4000, "value": 6},
    ]
    # Dense: the same 4-then-6 shape, sampled every 1000ms.
    dense_points = [{"labels": series_labels["dense"], "time": t, "value": 4}
                    for t in range(0, 4000, 1000)]
    dense_points += [{"labels": series_labels["dense"], "time": t, "value": 6}
                     for t in range(4000, 9000, 1000)]
    op("weighted ingest", space + "/points",
       {"now": 10000, "points": sparse_points + dense_points})

    window = {"start": 0, "end": 10000, "step": 10000, "origin": 0,
              "group_by": ["node"], "matchers": [
                  {"label": "__name__", "op": "=", "value": "host_load"}]}

    def node_cell(result, node):
        for cell in result["cells"]:
            if cell["labels"].get("node") == node:
                return cell
        raise RuntimeError(f"missing cell for {node}: {result}")

    # Plain observation averages disagree because the sample counts differ.
    plain = op("weighted plain avg contrast", space + "/aggregate",
               {**window, "function": "avg"})
    if node_cell(plain, "sparse")["value"] != 5.0:
        raise RuntimeError("sparse plain avg should be (4+6)/2 = 5")
    if abs(node_cell(plain, "dense")["value"] - 46 / 9) > 1e-9:
        raise RuntimeError("dense plain avg is skewed by the extra samples")

    # A short validity horizon: a value held longer than 2000ms goes unknown.
    tw_query = {**window, "function": "twavg", "max_age_ms": 2000}
    strict_query = {**tw_query, "require_coverage": True}

    lenient = op("weighted lenient twavg", space + "/aggregate", tw_query)
    sparse_cell = node_cell(lenient, "sparse")
    dense_cell = node_cell(lenient, "dense")
    # Dense stream stays known for the full window; sparse only covers 4000ms.
    if dense_cell["covered_ms"] != 10000:
        raise RuntimeError("dense stream should cover the whole window")
    if sparse_cell["covered_ms"] != 4000:
        raise RuntimeError("sparse stream should only cover 4000 of 10000ms")
    if sparse_cell["value"] != 5.0:
        raise RuntimeError("sparse twavg over known time should be 5")

    integral = op("weighted integral", space + "/aggregate",
                  {**window, "function": "integral", "max_age_ms": 2000})
    sparse_integral = node_cell(integral, "sparse")
    # Known area: 4 for 2000ms plus 6 for 2000ms => 20000 value-milliseconds.
    if sparse_integral["value"] != 20000 or sparse_integral["covered_ms"] != 4000:
        raise RuntimeError(f"unexpected sparse integral: {sparse_integral}")

    # Strict mode drops the under-covered sparse window but keeps the dense one.
    strict = op("weighted strict coverage", space + "/aggregate", strict_query)
    if strict["dropped_coverage"] != 1:
        raise RuntimeError("strict mode should report one dropped window")
    if node_cell(strict, "dense") is None:
        raise RuntimeError("fully covered dense window must survive strict mode")
    if any(cell["labels"].get("node") == "sparse" for cell in strict["cells"]):
        raise RuntimeError("under-covered sparse window must be dropped")

    # A long validity horizon makes the sparse stream fully known, so strict
    # mode now accepts it and its mean matches the dense mean over real time.
    forgiving = {**tw_query, "max_age_ms": 10000, "require_coverage": True}
    accepted = op("weighted forgiving coverage", space + "/aggregate", forgiving)
    if accepted["dropped_coverage"] != 0:
        raise RuntimeError("long horizon should leave every window covered")
    sparse_now = node_cell(accepted, "sparse")
    if sparse_now["covered_ms"] != 10000 or sparse_now["value"] != 5.2:
        raise RuntimeError(f"forgiving sparse mean wrong: {sparse_now}")

    # Negative timestamps and a non-zero origin still align buckets correctly.
    negative = op("weighted negative origin", space + "/aggregate", {
        "start": -5000, "end": 1000, "step": 2000, "origin": 500,
        "group_by": ["node"], "function": "twavg", "max_age_ms": 2000,
        "matchers": [{"label": "__name__", "op": "=", "value": "host_load"}],
    })
    if not negative["cells"]:
        raise RuntimeError("negative window with an in-range sample must emit cells")
    for cell in negative["cells"]:
        if cell["start"] >= cell["end"] or cell["covered_ms"] <= 0:
            raise RuntimeError(f"invalid negative-window cell: {cell}")

    return {"query": tw_query, "head_cells": lenient["cells"]}


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
                # The weighted demo ingests its own sparse/dense series, so it
                # runs before the pre-seal snapshot that seal must match.
                weighted_baseline = demonstrate_weighted(base, space, op)
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
                # Time-weighted results must read from the same visible data in
                # the head, sealed blocks and compacted blocks.
                weighted_sealed = request(base, space + "/aggregate", weighted_baseline["query"])
                if weighted_sealed["cells"] != weighted_baseline["head_cells"]:
                    raise RuntimeError("seal changed time-weighted result")
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
                weighted_compacted = request(base, space + "/aggregate", weighted_baseline["query"])
                if weighted_compacted["cells"] != weighted_baseline["head_cells"]:
                    raise RuntimeError("compaction changed time-weighted result")
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
