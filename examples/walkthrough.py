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
    "explain", "histograms",
]

# Two bucket layouts describing the same latency distribution. Their bounds
# differ, so merged query results must report incompatibility instead of
# adding bucket counts together.
SCHEME_A = [10, 25, 50, 100]
SCHEME_B = [20, 40, 80, 160]


def hist(node, time_ms, counts, bounds=SCHEME_A, total=None):
    return {
        "labels": {"__name__": "latency_ms", "node": node},
        "time": time_ms,
        "bounds": bounds,
        "counts": counts,
        "sum": float(total if total is not None else counts[-1] * 20),
    }


def run_histograms(base, space, op):
    """Exercise cumulative histogram ingestion, merge and reset semantics."""
    hist_space = "spaces/histlab"
    policy = {
        "name": "histlab", "retention_ms": 1000, "lateness_ms": 30000,
        "future_ms": 1000, "max_series": 100, "max_write": 1000, "max_scan": 100000,
    }
    op("histogram namespace", "spaces", policy)

    # Scalar samples and histograms coexist on the same series catalog.
    op("scalar beside histogram", hist_space + "/points", {
        "now": 30000,
        "points": [{"labels": {"__name__": "scalar_probe", "node": "edge-1"},
                    "time": 30000, "value": 1.5}],
    })

    # Same underlying distribution, two different bucket layouts, two nodes.
    op("histogram ingest layout a", hist_space + "/histograms", {
        "now": 30000,
        "histograms": [
            hist("edge-1", 30000, [10, 40, 80, 100], SCHEME_A),
            hist("edge-2", 30000, [8, 45, 75, 100], SCHEME_B),
        ],
    })
    # Invalid observation: cumulative counts must not decrease within one
    # snapshot and bounds must be strictly increasing.
    op("histogram invalid snapshot", hist_space + "/histograms", {
        "now": 30000,
        "histograms": [hist("edge-1", 30001, [5, 3], [10, 10])],
    }, status=400)

    # Second cumulative snapshot. The window increase is snapshot2 - snapshot1
    # for every bucket, merged across instances sharing a layout.
    op("histogram ingest second snapshot", hist_space + "/histograms", {
        "now": 31000,
        "histograms": [
            hist("edge-1", 31000, [20, 70, 140, 200], SCHEME_A),
            hist("edge-2", 31000, [12, 55, 135, 200], SCHEME_B),
        ],
    })

    query = {
        "start": 30000, "end": 32000, "step": 10000, "origin": 30000,
        "quantiles": [0.5, 0.9], "group_by": ["node"],
    }
    grouped = op("histogram quantiles grouped", hist_space + "/histogram-query", query)
    cell = grouped["cells"][0]
    if not cell["compatible"] or cell["count"] == 0:
        raise RuntimeError("single-node windows with one layout must be compatible")
    if len(cell["quantiles"]) != 2 or cell["quantiles"][0]["value"] is None:
        raise RuntimeError("quantile estimates missing")
    if "bucket_resolution_estimate" not in cell["limitations"]:
        raise RuntimeError("query must disclose bucket-resolution limitation")

    # No group_by: the two layouts meet in one cell and must not be summed.
    merged_query = {k: v for k, v in query.items() if k != "group_by"}
    merged = op("histogram incompatible layouts", hist_space + "/histogram-query", merged_query)
    cell = merged["cells"][0]
    if cell["compatible"] or len(cell["schemes"]) != 2:
        raise RuntimeError("different bucket layouts must yield per-scheme partials")
    if "incompatible_bucket_layouts" not in cell["limitations"]:
        raise RuntimeError("incompatible layouts must be explicitly flagged")
    if any("counts" in scheme and scheme["counts"][-1] == 0 for scheme in cell["schemes"]):
        raise RuntimeError("partial scheme increases must be preserved")

    # Counter restart: cumulative counts drop. The reset snapshot is counted
    # wholesale and the cell reports the reset limitation.
    op("histogram counter reset", hist_space + "/histograms", {
        "now": 31500,
        "histograms": [hist("edge-1", 31500, [5, 25, 40, 50], SCHEME_A)],
    })
    reset_window = {
        "start": 31000, "end": 32000, "step": 10000, "origin": 30000,
        "quantiles": [0.5],
        "matchers": [{"label": "node", "op": "=", "value": "edge-1"}],
    }
    reset = op("histogram reset query", hist_space + "/histogram-query", reset_window)
    cell = reset["cells"][0]
    if cell["resets"] != 1 or "counter_reset_observed" not in cell["limitations"]:
        raise RuntimeError("counter reset not surfaced: " + json.dumps(cell))
    # Increase in this window equals the post-reset snapshot's own counts.
    if cell["counts"] != [5, 25, 40, 50]:
        raise RuntimeError("reset-aware increase wrong: " + json.dumps(cell["counts"]))

    # Corrective write at an existing timestamp replaces the whole snapshot.
    op("histogram correction", hist_space + "/histograms", {
        "now": 31500,
        "histograms": [hist("edge-1", 31500, [6, 26, 41, 51], SCHEME_A)],
    })
    raw = op("histogram raw range", hist_space + "/histogram-range", {
        "start": 31500, "end": 31600,
        "matchers": [{"label": "node", "op": "=", "value": "edge-1"}],
    })
    if raw["series"][0]["points"][0]["counts"] != [6, 26, 41, 51]:
        raise RuntimeError("histogram correction did not replace the snapshot")

    # Sealing moves head snapshots into format-2 compressed blocks while
    # preserving every logical result.
    before_seal = op("histogram pre-seal query", hist_space + "/histogram-query", merged_query)
    receipt = op("histogram seal", hist_space + "/seal", {})
    if receipt["histogram_blocks"] == 0:
        raise RuntimeError("seal produced no histogram blocks")
    inventory = op("histogram block inventory", hist_space + "/histogram-blocks")
    if not inventory or inventory[0]["format"] != 2:
        raise RuntimeError("histogram block format missing from inventory")
    after_seal = op("histogram post-seal query", hist_space + "/histogram-query", merged_query)
    if after_seal["cells"] != before_seal["cells"]:
        raise RuntimeError("sealing changed histogram query results")

    # Deletion hides a snapshot; a later correction at that time is admitted.
    op("histogram delete", hist_space + "/deletions", {
        "start": 30000, "end": 30001,
        "matchers": [{"label": "node", "op": "=", "value": "edge-1"}],
    })
    deleted = op("histogram deleted range", hist_space + "/histogram-range", {
        "start": 30000, "end": 30001,
        "matchers": [{"label": "node", "op": "=", "value": "edge-1"}],
    })
    if deleted["stats"]["visible"] != 0:
        raise RuntimeError("deletion did not hide histogram snapshot")
    op("histogram readmit after delete", hist_space + "/histograms", {
        "now": 32000,
        "histograms": [hist("edge-1", 30000, [1, 2, 3, 4], SCHEME_A)],
    })

    # Compaction must preserve logical results across head and blocks.
    logical = op("histogram pre-compaction", hist_space + "/histogram-query", merged_query)
    op("histogram compact", hist_space + "/compact", {})
    physical = op("histogram post-compaction", hist_space + "/histogram-query", merged_query)
    if physical["cells"] != logical["cells"]:
        raise RuntimeError("compaction changed histogram results")

    # Retention floor physically removes old histograms and blocks resurrection.
    op("histogram retention", hist_space + "/retention", {"now": 33000})
    swept = op("histogram post-retention range", hist_space + "/histogram-range", {
        "start": 0, "end": 40000,
    })
    if swept["stats"]["visible"] != 0:
        raise RuntimeError("retention did not sweep histogram snapshots")
    op("histogram retention rejection", hist_space + "/histograms", {
        "now": 33000,
        "histograms": [hist("edge-1", 31000, [1, 1, 1, 1], SCHEME_A)],
    }, status=409)


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
            # The histogram workflow uses its own namespace and database state,
            # so it runs after the backup/restore scenario of the main space.
            process, base = launch(binary, scratch / "histlab.db", log)
            try:
                run_histograms(base, "spaces/histlab",
                               lambda label, route, payload=None, **options: op(label, route, payload, **options))
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
