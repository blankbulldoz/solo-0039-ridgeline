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
    "namespace", "ingest", "quality", "discovery", "range", "windows", "counters",
    "seal", "delete", "retention", "materialize", "freshness", "compact", "backup",
    "explain"
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
                op("unknown quality rejection", space + "/points", {
                    "now": 10000, "points": [{**seed["points"][0], "quality": "guessed"}]
                }, status=400)

                # Observation quality is stored per point, not encoded in labels.
                # A default raw read keeps the scalar shape with no quality key.
                scalar = op("scalar quality default", space + "/range", q)
                if any("quality" in point for series in scalar["series"] for point in series["points"]):
                    raise RuntimeError("default range output must remain the scalar shape")
                reported = op("quality report", space + "/range", {**q, "report_quality": True})
                timeline = {entry["series_id"]: entry for entry in reported.get("qualities", [])}
                gauge_labels = {"__name__": "gauge_ratio", "node": "ridge-a", "zone": "east"}
                gauge_id = next(entry["series"]["id"] for entry in reported["series"]
                                if entry["series"]["labels"] == gauge_labels)
                if gauge_id not in timeline:
                    raise RuntimeError("mixed-quality series must report a quality timeline")
                if [point["quality"] for point in timeline[gauge_id]["points"]] != \
                        ["trusted", "estimated", "invalid", "trusted"]:
                    raise RuntimeError("quality timeline must follow observation time")
                filtered = op("quality filtered range", space + "/range",
                              {**q, "include_qualities": ["trusted", "estimated"],
                               "matchers": [{"label": "__name__", "op": "=", "value": "gauge_ratio"}]})
                if any(point["quality"] == "invalid" for point in filtered["series"][0]["points"]):
                    raise RuntimeError("explicit quality selection must filter raw points")
                if filtered["quality"]["excluded"]["invalid"] != 1:
                    raise RuntimeError("filtered raw range must report what it excluded")

                # Invalid observations are excluded from plain reductions by
                # default, and the summary explains the exclusion and influence.
                gauge = {"start": -5000, "end": 10000, "step": 20000, "origin": -5000,
                         "function": "avg", "report_quality": True,
                         "matchers": [{"label": "__name__", "op": "=", "value": "gauge_ratio"}]}
                excluded_avg = op("average excludes invalid", space + "/aggregate", gauge)
                if excluded_avg["cells"][0]["value"] != 6:
                    raise RuntimeError("invalid observation must not enter a default average")
                cell_quality = excluded_avg["cells"][0]["quality"]
                if cell_quality["excluded"]["invalid"] != 1 or not cell_quality["spans_excluded"]:
                    raise RuntimeError("excluded observation must be explained")
                included_avg = op("average includes all qualities", space + "/aggregate",
                                  {**gauge, "include_qualities": ["trusted", "estimated", "invalid"]})
                if included_avg["cells"][0]["value"] != 29.25:
                    raise RuntimeError("explicit quality selection must include invalid values")
                contributions = {c["quality"]: c for c in included_avg["cells"][0]["quality"]["contributions"]}
                if contributions["invalid"]["sum"] != 99 or contributions["invalid"]["observations"] != 1:
                    raise RuntimeError("explicit quality query must report each quality's contribution")

                # A counter whose invalid point sits between two usable points:
                # excluding it changes the rate and is reported, never hidden.
                counter = {"start": 0, "end": 10000, "step": 10000, "origin": 0,
                           "function": "rate", "report_quality": True,
                           "matchers": [{"label": "__name__", "op": "=", "value": "cycle_total"}]}
                gap_rate = op("counter spans excluded", space + "/aggregate", counter)
                if gap_rate["cells"][0]["quality"]["excluded_between"] != 1 or \
                        not gap_rate["cells"][0]["quality"]["spans_excluded"]:
                    raise RuntimeError("rate must announce an excluded point between counters")
                spike_rate = op("counter includes invalid", space + "/aggregate",
                                {**counter, "include_qualities": ["trusted", "estimated", "invalid"]})
                if spike_rate["cells"][0]["value"] == gap_rate["cells"][0]["value"]:
                    raise RuntimeError("including the invalid counter point must change the rate")

                # A correction updates value and quality together at one timestamp.
                corrected = op("quality correction", space + "/points", {
                    "now": 10000, "points": [{**seed["points"][9], "time": 300,
                                              "value": 99, "quality": "trusted"}]
                })
                corrected_range = op("quality correction visible", space + "/range",
                                     {**q, "report_quality": True,
                                      "matchers": [{"label": "__name__", "op": "=", "value": "gauge_ratio"}]})
                corrected_timeline = corrected_range["qualities"][0]["points"]
                if corrected_timeline[2]["quality"] != "trusted" or \
                        corrected_timeline[2]["sequence"] != corrected["sequence"]:
                    raise RuntimeError("correction must replace the observation quality with the new commit")

                op("discovery", space + "/series", {"matchers": [{"label": "rack", "op": "=", "value": ""}]})
                before = op("range", space + "/range", q)
                op("truncated range", space + "/range", {**q, "limit": 2})
                agg = {**q, "step": 1000, "origin": 0, "group_by": ["zone"], "function": "avg"}
                op("windows", space + "/aggregate", agg)
                rate = {**q, "step": 10000, "origin": -5000, "group_by": ["zone"],
                        "function": "rate", "matchers": [{"label": "__name__", "op": "=", "value": "pulse_total"}]}
                op("counters", space + "/aggregate", rate)
                quality_before_seal = op("quality before seal", space + "/range",
                                         {**q, "report_quality": True})
                op("seal", space + "/seal", {})
                sealed = op("sealed range", space + "/range", q)
                if before["series"] != sealed["series"]:
                    raise RuntimeError("seal changed the logical samples")
                sealed_quality = op("sealed quality", space + "/range", {**q, "report_quality": True})
                if sealed_quality["series"] != quality_before_seal["series"] or \
                        sealed_quality["qualities"] != quality_before_seal["qualities"]:
                    raise RuntimeError("sealing must preserve observation quality")
                op("blocks", space + "/blocks")
                op("delete", space + "/deletions", {**q, "start": -600, "end": 600,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                op("deletion inventory", space + "/deletions")
                replacement = {"now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 8}]}
                op("correct after delete", space + "/points", replacement)
                op("materialize", space + "/rollups/pulse", {**agg, "report_quality": True}, method="PUT")
                fresh = op("freshness", space + "/rollups/pulse")
                if fresh["stale"]:
                    raise RuntimeError("new materialization is stale")
                if "quality" not in fresh["result"]:
                    raise RuntimeError("materialized aggregate must explain observation quality")
                op("new correction", space + "/points", {
                    "now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 9}]
                })
                stale = op("stale materialization", space + "/rollups/pulse")
                if not stale["stale"]:
                    raise RuntimeError("changed namespace should mark materialization stale")
                logical = op("pre-compaction", space + "/range", q)["series"]
                pre_compaction_quality = op("pre-compaction quality", space + "/range",
                                            {**q, "report_quality": True})
                op("compact", space + "/compact", {})
                if op("post-compaction", space + "/range", q)["series"] != logical:
                    raise RuntimeError("compaction changed logical samples")
                compacted_quality = op("post-compaction quality", space + "/range",
                                       {**q, "report_quality": True})
                if compacted_quality["qualities"] != pre_compaction_quality["qualities"]:
                    raise RuntimeError("compaction changed observation quality")
                op("explain", space + "/explain", q)
                op("retention", space + "/retention", {"now": 11000})
                op("retention no resurrection", space + "/points", replacement, status=409)
                op("read-only", space + "/mode", {"read_only": True}, method="PUT")
                op("read-only rejection", space + "/points", {
                    "now": 11000, "points": [{**seed["points"][0], "time": 1500}]
                }, status=409)
                op("resume", space + "/mode", {"read_only": False}, method="PUT")
                source = op("backup source", space + "/range", q)
                source_quality = op("backup source quality", space + "/range", {**q, "report_quality": True})
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
                restored_quality = request(base, "spaces/spindle/range", {**q, "report_quality": True})
                if restored_quality != source_quality:
                    raise RuntimeError("restore changed observation quality or its timeline")
                observations.append({"step": "restored quality", "response": restored_quality})
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
