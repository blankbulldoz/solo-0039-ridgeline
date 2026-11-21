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
    "explain", "exemplars"
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

                def point(node, at, value, exemplar=None):
                    return {
                        "labels": {"__name__": "pulse_total", "node": node, "zone": "east"},
                        "time": at, "value": value,
                        **({"exemplar": exemplar} if exemplar else {}),
                    }

                def exemplar(trace_id, **context):
                    return {"trace_id": trace_id, "labels": dict(context)}

                # A load spike at t=1500 carries a trace attachment. Two
                # exemplars at the same series-time prove the moment is not
                # a unique key and series identity stays unchanged.
                spike = {
                    "now": 10000, "points": [
                        point("ridge-a", 1500, 96, exemplar("trace-spike-1", route="/checkout", shard="a")),
                        point("ridge-a", 1500, 96, exemplar("trace-spike-2", route="/checkout", shard="b")),
                        point("ridge-b", 1500, 80, exemplar("trace-spike-b1", route="/ingest")),
                    ],
                }
                # Exemplars ride alongside samples; the attachment itself must
                # never synthesize extra samples beyond the points written.
                pre_spike = op("range", space + "/range", q)
                spike_receipt = op("exemplar ingest", space + "/points", spike)
                if spike_receipt["exemplars"] != 3:
                    raise RuntimeError("write receipt must count attached exemplars")
                ex_query = {**q, "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]}
                found = op("exemplar query", space + "/exemplars", ex_query)
                if len(found["exemplars"]) != 2 or found["stats"]["visible"] != 2:
                    raise RuntimeError("both same-moment exemplars must be returned")
                if any(not item["anchor_visible"] for item in found["exemplars"]):
                    raise RuntimeError("new exemplars must anchor visible samples")
                # Observation -> aggregation window -> exemplar -> raw point.
                window = op("spike window", space + "/aggregate",
                            {**q, "step": 1000, "origin": 0, "group_by": ["node"], "function": "max"})
                cell = next(c for c in window["cells"]
                            if c["labels"].get("node") == "ridge-a" and c["start"] == 1000)
                anchor = next(item for item in found["exemplars"] if item["time"] == 1500)
                if cell["value"] != anchor["value"]:
                    raise RuntimeError("exemplar value must match the aggregate window's source point")
                raw = op("anchor raw point", space + "/range",
                         {"start": anchor["time"], "end": anchor["time"] + 1,
                          "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                raw_point = raw["series"][0]["points"][0]
                if raw_point["time"] != anchor["time"] or raw_point["value"] != anchor["value"]:
                    raise RuntimeError("exemplar must cross-check against the original point")
                # Two spike writes correct the existing t=1500 keys and the
                # third adds the ridge-b key, so the range total grows by one;
                # the attachments themselves contribute no samples.
                after = op("range after exemplars", space + "/range", q)
                pre_total = sum(len(s["points"]) for s in pre_spike["series"])
                post_total = sum(len(s["points"]) for s in after["series"])
                if post_total - pre_total != 1:
                    raise RuntimeError("exemplar attachments must not synthesize samples")

                # A dense spike is bounded: representative samples only, with
                # an explicit truncation indicator and omission accounting.
                dense = {"now": 10000, "points": [
                    point("ridge-a", 3000 + i, 70 + i, exemplar(f"trace-dense-{i:02d}"))
                    for i in range(25)
                ]}
                op("dense exemplar ingest", space + "/points", dense)
                bounded = op("bounded exemplars", space + "/exemplars",
                             {**ex_query, "limit": 5})
                if not bounded["truncated"] or len(bounded["exemplars"]) != 5 or \
                        bounded["stats"]["omitted"] != 22:
                    raise RuntimeError("dense exemplar spike must be bounded with omission stats")
                times = [item["time"] for item in bounded["exemplars"]]
                if times[0] != 1500 or times[-1] != 3024:
                    raise RuntimeError("representative selection must span the full spike")
                op("exemplar limit rejected", space + "/exemplars",
                   {**ex_query, "limit": 100000}, status=400)
                op("invalid exemplar rejected", space + "/points",
                    {"now": 10000, "points": [point("ridge-a", 4000, 1,
                                                    exemplar("bad trace id"))]}, status=400)

                # Re-baseline the seal comparison after all exemplar writes;
                # sealing must preserve every logical point it just moved.
                before = op("pre-seal range", space + "/range", q)
                op("seal", space + "/seal", {})
                sealed = op("sealed range", space + "/range", q)
                if before["series"] != sealed["series"]:
                    raise RuntimeError("seal changed the logical samples")
                op("blocks", space + "/blocks")
                op("exemplar before delete", space + "/points",
                    {"now": 10000, "points": [
                        point("ridge-a", 500, 2, exemplar("trace-before-delete", span="old"))]})
                op("delete", space + "/deletions", {**q, "start": -600, "end": 600,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                op("deletion inventory", space + "/deletions")
                detached = op("detached exemplar", space + "/exemplars",
                              {**q, "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                old_ex = next(item for item in detached["exemplars"]
                              if item["trace_id"] == "trace-before-delete")
                if old_ex["anchor_visible"] or detached["stats"]["hidden_tombstone"] < 1:
                    raise RuntimeError("tombstoned exemplar association must be reported hidden")
                replacement = {"now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 8}]}
                op("correct after delete", space + "/points", replacement)
                op("exemplar with corrected anchor", space + "/points",
                    {"now": 10000, "points": [
                        point("ridge-a", 500, 8, exemplar("trace-after-delete", span="new"))]})
                corrected = op("exemplar correction check", space + "/exemplars",
                               {**q, "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                by_trace = {item["trace_id"]: item for item in corrected["exemplars"]}
                if by_trace["trace-before-delete"]["anchor_visible"]:
                    raise RuntimeError("old exemplar must not be resurrected by a correction")
                new_ex = by_trace["trace-after-delete"]
                if not new_ex["anchor_visible"] or new_ex["value"] != 8 or \
                        new_ex["anchor_sequence"] is None:
                    raise RuntimeError("new exemplar must anchor the corrected point")
                corrected_raw = op("corrected raw point", space + "/range",
                                   {"start": 500, "end": 501,
                                    "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                if corrected_raw["series"][0]["points"][0]["value"] != new_ex["value"]:
                    raise RuntimeError("exemplar anchor must match the corrected raw point")
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
                pre_compact = op("pre-compaction exemplars", space + "/exemplars", ex_query)
                pre_all = op("pre-compaction all exemplars", space + "/exemplars", q)
                valid_before = [item for item in pre_compact["exemplars"] if item["anchor_visible"]]
                valid_all = [item for item in pre_all["exemplars"] if item["anchor_visible"]]
                compaction = op("compact", space + "/compact", {})
                if op("post-compaction", space + "/range", q)["series"] != logical:
                    raise RuntimeError("compaction changed logical samples")
                post_compact = op("post-compaction exemplars", space + "/exemplars", ex_query)
                # Valid exemplars survive byte-for-byte; invalid associations
                # (detached/tombstoned) are physically reclaimed and accounted.
                if post_compact["exemplars"] != valid_before:
                    raise RuntimeError("compaction must preserve valid exemplar records")
                if compaction["exemplars_kept"] != len(valid_all):
                    raise RuntimeError("compaction receipt must account for kept exemplars")
                if compaction["exemplars_dropped"] != len(pre_all["exemplars"]) - len(valid_all):
                    raise RuntimeError("compaction receipt must account for reclaimed exemplars")
                op("explain", space + "/explain", q)
                retention = op("retention", space + "/retention", {"now": 11000})
                if retention["exemplars_dropped"] == 0:
                    raise RuntimeError("retention compaction must physically reclaim expired exemplars")
                expired = op("expired exemplars gone", space + "/exemplars",
                             {"start": -4503599627370496, "end": 10000,
                              "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                if any(item["time"] < retention["floor"] for item in expired["exemplars"]):
                    raise RuntimeError("exemplars below the retention floor must not return")
                if expired["stats"]["below_floor"] != 0:
                    raise RuntimeError("reclaimed exemplars should not remain as query-time exclusions")
                op("retention no resurrection", space + "/points", replacement, status=409)
                op("read-only", space + "/mode", {"read_only": True}, method="PUT")
                op("read-only rejection", space + "/points", {
                    "now": 11000, "points": [{**seed["points"][0], "time": 1500}]
                }, status=409)
                op("resume", space + "/mode", {"read_only": False}, method="PUT")
                source = op("backup source", space + "/range", q)
                source_exemplars = op("backup exemplars", space + "/exemplars", ex_query)
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
                restored_exemplars = request(base, "spaces/spindle/exemplars", ex_query)
                if restored_exemplars["exemplars"] != source_exemplars["exemplars"]:
                    raise RuntimeError("restored image differs on exemplar attachments")
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
