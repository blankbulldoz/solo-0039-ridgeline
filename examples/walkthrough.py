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
    "explain", "sessions"
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
                handover = "spaces/handover"
                op("sessions namespace", "spaces", {
                    "name": "handover", "retention_ms": 10000, "lateness_ms": 30000,
                    "future_ms": 1000, "max_series": 100, "max_write": 10000,
                    "max_scan": 100000})

                def tick(device, zone, t):
                    return {"labels": {"__name__": "collect_tick", "device": device,
                                       "zone": zone}, "time": t, "value": 1}

                op("sessions pre-shift", handover + "/points", {"now": 0, "points": [
                    tick("dev-carried", "east", -800),
                    tick("dev-late", "east", -3000), tick("dev-late", "east", -1600),
                    tick("dev-bridge", "east", -500),
                    tick("dev-reaped", "east", -2000), tick("dev-reaped", "east", -500)]})
                op("sessions seal pre-shift", handover + "/seal", {})
                op("sessions early shift", handover + "/points", {"now": 2000, "points": [
                    tick("dev-carried", "east", 700),
                    tick("dev-new", "east", 400), tick("dev-new", "east", 1900),
                    tick("dev-late", "east", 500),
                    tick("dev-bridge", "east", 800),
                    tick("dev-reaped", "east", 1000),
                    tick("dev-west", "west", 100), tick("dev-west", "west", 1600)]})
                op("sessions seal early", handover + "/seal", {})
                op("sessions late shift", handover + "/points", {"now": 3900, "points": [
                    tick("dev-carried", "east", 2200), tick("dev-carried", "east", 3700),
                    tick("dev-late", "east", 2000), tick("dev-late", "east", 3500),
                    tick("dev-bridge", "east", 2100), tick("dev-bridge", "east", 3400),
                    tick("dev-reaped", "east", 2500),
                    tick("dev-west", "west", 3100),
                    tick("dev-spanning", "west", 2000), tick("dev-spanning", "west", 3000),
                    tick("dev-spanning", "west", 4500)]})
                report = {"start": 0, "end": 4000, "gap": 1500, "as_of": 4000,
                          "group_by": ["zone"],
                          "matchers": [{"label": "__name__", "op": "=", "value": "collect_tick"}]}

                def group_of(result, zone):
                    for group in result["groups"]:
                        if group["labels"].get("zone") == zone:
                            return group
                    raise RuntimeError(f"missing session group for {zone}")

                def segment_of(result, device, first):
                    for segment in result["segments"]:
                        if segment["series"]["labels"].get("device") == device \
                                and segment["first"] == first:
                            return segment
                    raise RuntimeError(f"missing segment {device}@{first}")

                def check_invariant(result):
                    for group in result["groups"]:
                        left = group["started"] + group["carried_in"] + group["unknown_left"]
                        right = group["ended"] + group["carried_out"] + group["unknown_right"]
                        if left != group["segments"] or right != group["segments"]:
                            raise RuntimeError(f"session counts do not reconcile: {group}")

                baseline = op("sessions baseline", handover + "/sessions", report)
                check_invariant(baseline)
                east = group_of(baseline, "east")
                if (east["devices"], east["segments"], east["started"], east["carried_in"],
                        east["ended"], east["unknown_right"]) != (5, 5, 2, 3, 2, 3):
                    raise RuntimeError(f"baseline east summary mismatch: {east}")
                carried = segment_of(baseline, "dev-carried", 700)
                if carried["left"] != "carried_in" or carried["points"] != 3:
                    raise RuntimeError("sealed-block neighbor must carry the pre-shift session in")
                if carried["right"] != "unknown_right":
                    raise RuntimeError("tail past the confirmation deadline is not an end")
                fresh = segment_of(baseline, "dev-new", 400)
                if (fresh["left"], fresh["right"]) != ("started", "ended"):
                    raise RuntimeError("in-shift device must show confirmed start and end")
                spanning = segment_of(baseline, "dev-spanning", 2000)
                if (spanning["left"], spanning["right"]) != ("started", "carried_out"):
                    raise RuntimeError("visible successor must carry the session past the shift")
                op("sessions invalid", handover + "/sessions", {**report, "gap": 0}, status=400)
                op("sessions late bridge", handover + "/points",
                   {"now": 3900, "points": [tick("dev-late", "east", -1000)]})
                joined = op("sessions after late point", handover + "/sessions", report)
                check_invariant(joined)
                east_joined = group_of(joined, "east")
                if (east_joined["started"], east_joined["carried_in"]) != \
                        (east["started"] - 1, east["carried_in"] + 1):
                    raise RuntimeError("late point must join the pre-shift session")
                late = segment_of(joined, "dev-late", 500)
                if late["left"] != "carried_in" or late["points"] != 3:
                    raise RuntimeError("late bridge reclassifies the edge, not the point count")
                op("sessions delete bridge", handover + "/deletions", {
                    "start": 2100, "end": 2101,
                    "matchers": [{"label": "device", "op": "=", "value": "dev-bridge"}]})
                split = op("sessions after bridge delete", handover + "/sessions", report)
                check_invariant(split)
                east_split = group_of(split, "east")
                if (east_split["segments"], east_split["started"], east_split["ended"]) != \
                        (east_joined["segments"] + 1, east_joined["started"] + 1,
                         east_joined["ended"] + 1):
                    raise RuntimeError("deleted bridge must split one session into two")
                if segment_of(split, "dev-bridge", 800)["right"] != "ended" or \
                        segment_of(split, "dev-bridge", 3400)["left"] != "started":
                    raise RuntimeError("recorded deletion opens a real, confirmable gap")
                op("sessions retention", handover + "/retention", {"now": 10500})
                reaped = op("sessions after retention", handover + "/sessions", report)
                check_invariant(reaped)
                east_reaped = group_of(reaped, "east")
                if (east_reaped["started"], east_reaped["carried_in"],
                        east_reaped["unknown_left"]) != (1, 0, 5):
                    raise RuntimeError("reaped history must not become new starts")
                if segment_of(reaped, "dev-reaped", 1000)["left"] != "unknown_left":
                    raise RuntimeError("floor inside the lookback interval is unconfirmable")
                spanning = segment_of(reaped, "dev-spanning", 2000)
                if (spanning["left"], spanning["right"]) != ("started", "carried_out"):
                    raise RuntimeError("readable lookback and successor survive retention")
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
