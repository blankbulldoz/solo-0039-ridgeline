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
    "explain", "best_effort",
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


def expect(condition, message):
    if not condition:
        raise RuntimeError(message)


def exercise_best_effort(workflow, output):
    """Best-effort ingestion: per-point receipts, quota/watermark discipline,
    and a client replaying only the points the receipt says to retry."""
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-besteffort-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "effort.db", log)
            try:
                space = "spaces/effort"
                labels_a = {"__name__": "pulse_total", "node": "ridge-a", "zone": "east"}
                labels_b = {"__name__": "pulse_total", "node": "ridge-b", "zone": "east"}
                labels_c = {"__name__": "pulse_total", "node": "ridge-c", "zone": "west"}
                labels_d = {"__name__": "pulse_total", "node": "ridge-d", "zone": "west"}

                def op(label, route, payload=None, **options):
                    out = request(base, route, payload, **options)
                    observations.append({"step": label, "request": payload, "response": out})
                    return out

                # Tight policy makes every rejection reason observable. The
                # three seed series already fill the cardinality quota.
                policy = {
                    "name": "effort", "retention_ms": 10000, "lateness_ms": 2000,
                    "future_ms": 1000, "max_series": 3, "max_write": 10000,
                    "max_scan": 100000,
                }
                op("namespace", "spaces", policy)
                seed = {"now": 10000, "mode": "atomic", "points": [
                    {"labels": labels_a, "time": 9000, "value": 1},
                    {"labels": labels_b, "time": 9000, "value": 2},
                    {"labels": labels_c, "time": 9000, "value": 3},
                ]}
                seed_receipt = op("atomic seed", space + "/points", seed)
                expect(seed_receipt["accepted"] == 3, "seed should store three new points")
                expect(seed_receipt["mode"] == "atomic", "default mode must report atomic")

                # Default atomic write still fails the whole batch when one
                # point violates policy; the valid companion point is not stored.
                op("atomic batch still all-or-nothing", space + "/points", {
                    "now": 10000,
                    "points": [
                        {"labels": labels_a, "time": 9500, "value": 7},
                        {"labels": labels_a, "time": 20000, "value": 8},
                    ],
                }, status=409)
                before = op("range before mixed batch", space + "/range",
                            {"start": 0, "end": 30000})
                stored_before = sum(len(s["points"]) for s in before["series"])
                expect(stored_before == 3, "rejected atomic request must not publish any point")

                # One mixed batch exercising every disposition at once.
                mixed = {"now": 10000, "mode": "best_effort", "points": [
                    {"labels": labels_a, "time": 9500, "value": 10},   # 0 new point
                    {"labels": labels_a, "time": 9500, "value": 11},   # 1 overwritten by index 3
                    {"labels": labels_b, "time": 6000, "value": 4},    # 2 late (wm 9000, lateness 2000)
                    {"labels": labels_a, "time": 9500, "value": 12},   # 3 wins; overwrites index 1
                    {"labels": labels_d, "time": 9500, "value": 5},    # 4 new series over quota
                    {"labels": labels_c, "time": 12000, "value": 6},   # 5 future skew (now 10000)
                    {"labels": labels_a, "time": 9000, "value": 99},   # 6 correction of stored point
                ]}
                receipt = op("mixed best-effort batch", space + "/points", mixed)
                by_index = {r["index"]: r for r in receipt["results"]}
                expect(receipt["requested"] == 7, "requested must count all input rows")
                # Accepted counts only genuinely new stored points: index 0 and
                # index 3 target the same empty slot, so the duplicate winner
                # (3) stores it once while index 0 never becomes a new point.
                expect(receipt["accepted"] == 1,
                       f"only index 3 creates a new stored point, got {receipt['accepted']}")
                expect(receipt["overwritten"] == 3,
                       f"duplicate losers (0,1) and correction (6), got {receipt['overwritten']}")
                expect(receipt["rejected"] == 3,
                       f"indices 2, 4 and 5 are rejects, got {receipt['rejected']}")
                expect(receipt["requested"] == receipt["accepted"] +
                       receipt["overwritten"] + receipt["rejected"],
                       "receipt dispositions must partition every input row")
                expect(receipt["new_series"] == 0, "quota rejection must not create a series")
                expect(by_index[0]["status"] == "overwritten", "index 0 loses the duplicate slot")
                expect(by_index[1]["status"] == "overwritten", "index 1 should be overwritten")
                # Index 3 wins the duplicate: it is the row that actually stores t=9500.
                expect(by_index[3]["status"] == "accepted",
                       "index 3 should be the accepted winning duplicate row")
                expect(by_index[0]["detail"].endswith("(index 3)"),
                       "overwrite detail must name the superseding input position")
                expect(by_index[2]["reason"] == "lateness_exceeded", "index 2 late reason")
                expect(by_index[4]["reason"] == "series_quota_exceeded", "index 4 quota reason")
                expect(by_index[5]["reason"] == "future_skew_exceeded", "index 5 future reason")
                expect(by_index[6]["reason"] == "overwritten", "index 6 correction reason")

                # Watermark and series quota move only on actually accepted data.
                state = op("namespace state after mixed batch", "spaces/effort")
                expect(state["watermark"] == 9500,
                       f"watermark must advance to 9500 via accepted data, got {state['watermark']}")
                series = op("series discovery after quota rejection", space + "/series",
                            {"matchers": []})
                expect(len(series) == 3,
                       f"rejected new-series point must not consume quota, got {len(series)}")

                # Rejected late and over-quota points cannot be rescued by retry
                # under the same policy; the receipt already told the client why.
                replay_rejected = op("replay permanent rejections stays rejected",
                                     space + "/points", {
                                         "now": 10000, "mode": "best_effort", "points": [
                                             mixed["points"][2], mixed["points"][4],
                                         ],
                                     })
                expect(replay_rejected["accepted"] == 0,
                       "late and quota points must remain rejected on replay")
                expect({r["reason"] for r in replay_rejected["results"]} ==
                       {"lateness_exceeded", "series_quota_exceeded"},
                       "replay must preserve stable reason codes")

                # Client-organized补发: send only the retriable future point with
                # a fresh observation time. It is now admitted and moves the
                # watermark again.
                retry_now = 12000
                backfill = op("backfill of retriable future point", space + "/points", {
                    "now": retry_now, "mode": "best_effort",
                    "points": [mixed["points"][5]],
                })
                expect(backfill["accepted"] == 1 and backfill["rejected"] == 0,
                       "backfilled future point should be accepted")
                expect(backfill["watermark"] == 12000,
                       "accepted backfill must advance the watermark")

                # Final observations: request counts never masquerade as new
                # stored points. Four stored points per series a/b/c where they
                # exist, three series, and the duplicate/correction values win.
                final = op("final range", space + "/range", {"start": 0, "end": 30000})
                points = {s["series"]["labels"]["node"]: s["points"] for s in final["series"]}
                expect(len(final["series"]) == 3, "final namespace keeps three series")
                expect({p["time"]: p["value"] for p in points["ridge-a"]} ==
                       {9000: 99, 9500: 12},
                       "ridge-a must show correction 99 and duplicate winner 12")
                expect({p["time"]: p["value"] for p in points["ridge-b"]} == {9000: 2},
                       "late ridge-b point must remain absent")
                expect({p["time"]: p["value"] for p in points["ridge-c"]} ==
                       {9000: 3, 12000: 6},
                       "ridge-c must gain the backfilled point")
                total = sum(len(s["points"]) for s in final["series"])
                expect(total == 5, f"expected five stored points, got {total}")
                summary = {
                    "mixed_request_rows": receipt["requested"],
                    "new_points_from_mixed_request": receipt["accepted"],
                    "overwrites": receipt["overwritten"],
                    "rejections": receipt["rejected"],
                    "backfilled_points": backfill["accepted"],
                    "final_stored_points": total,
                    "final_series": len(final["series"]),
                    "final_watermark": backfill["watermark"],
                }
                observations.append({"step": "final accounting", "response": summary})
            finally:
                stop(process)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(observations, indent=2) + "\n")
    print(json.dumps({"workflow": workflow, "operations": len(observations),
                      "state": "complete", "coverage": ["best_effort"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.workflow == "best_effort":
        exercise_best_effort(args.workflow, args.output)
    else:
        exercise(args.workflow, args.output)
