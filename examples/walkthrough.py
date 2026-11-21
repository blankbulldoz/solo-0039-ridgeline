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
    "seal-job", "delete", "retention", "materialize", "freshness", "compact",
    "backup", "explain",
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


def point_map(series_result):
    """Flatten a range result into {(series_id, time): value}."""
    out = {}
    for group in series_result["series"]:
        sid = group["series"]["id"]
        for sample in group["points"]:
            out[(sid, sample["time"])] = sample["value"]
    return out


def exercise_seal_job(workflow, output):
    """Demonstrate a persistent, batched seal job across a process restart.

    The job snapshots an initial backlog, moves at most ``batch_points`` head
    points per call, keeps running while corrections and new points arrive, and
    resumes from durable state after the process is interrupted. Queries keep
    their head+block merge semantics at every step, so corrections win and no
    point is ever lost or duplicated.
    """
    seed = json.loads((ROOT / "examples/input.json").read_text())
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-sealjob-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        database = scratch / "sealjob.db"
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, database, log)
            try:
                space = "spaces/sealdemo"
                interval = {"start": -5000, "end": 20000}

                def op(label, route, payload=None, **options):
                    out = request(base, route, payload, **options)
                    observations.append({"step": label, "request": payload, "response": out})
                    return out

                op("create namespace", "spaces", {**seed["namespace"], "name": "sealdemo"})
                labels = {"__name__": "pulse_total", "node": "ridge-a", "zone": "east"}
                # Twelve initial points: the backlog the seal job snapshots.
                initial = [
                    {"labels": labels, "time": t, "value": float(t)}
                    for t in range(0, 12000, 1000)
                ]
                op("initial ingest", space + "/points", {"now": 12000, "points": initial})
                before = op("range before seal", space + "/range", interval)
                expected = point_map(before)
                assert len(expected) == 12

                # Start the persistent job with a cap of five points per batch.
                first = op("start seal job", space + "/seal", {"batch_points": 5})
                job = first["job"]["job"]
                assert first["job"]["active"] and first["points"] == 5
                assert job["state"] == "running"
                assert job["progress"]["initial_total"] == 12
                assert job["progress"]["initial_pending"] == 7
                assert job["progress"]["added_pending"] == 0
                assert job["progress"]["remaining"] == 7

                status = op("job status", space + "/seal")
                assert status["active"]
                assert status["job"]["progress"]["initial_pending"] == 7

                # Reads remain identical while sealing is partially complete.
                mid = op("range mid seal", space + "/range", interval)
                assert point_map(mid) == expected

                # Interrupt the process: no in-memory job state may be required.
            finally:
                stop(process)
            process, base = launch(binary, database, log)
            try:
                resumed = op("status after restart", space + "/seal")
                assert resumed["active"], "seal job must survive process restart"
                assert resumed["job"]["progress"]["initial_pending"] == 7
                assert resumed["job"]["batches"] == 1

                # A repeated/duplicate batch advance simply continues the job.
                second = op("second batch", space + "/seal", {"batch_points": 5})
                assert second["points"] == 5
                assert second["job"]["job"]["progress"]["initial_pending"] == 2

                # Writes continue between batches: a same-timestamp correction
                # plus a brand-new point (backfill and new work at once).
                correction = {"labels": labels, "time": 5000, "value": 999.0}
                new_point = {"labels": labels, "time": 12500, "value": 77.0}
                op("correction and backfill", space + "/points",
                   {"now": 13000, "points": [correction, new_point]})
            finally:
                stop(process)

            # Restart again before finishing, proving progress is fully durable.
            process, base = launch(binary, database, log)
            try:
                progress = op("status with in-flight additions", space + "/seal")
                p = progress["job"]["progress"]
                # Two untouched initial keys remain, plus the correction to an
                # already-sealed initial key: three initial-family head keys.
                # The backfill at a brand-new timestamp is the only added key.
                assert p["initial_pending"] == 3
                assert p["added_pending"] == 1
                assert p["remaining"] == 4
                assert p["initial_total"] == 12
                assert p["corrections_sealed"] == 0

                # Final batch drains the head and completes the job.
                final = op("final batch", space + "/seal", {"batch_points": 5})
                assert final["points"] == 4
                assert final["job"]["active"] is False
                assert final["job"]["job"]["state"] == "complete"
                assert final["job"]["job"]["progress"]["initial_sealed"] == 12
                assert final["job"]["job"]["progress"]["added_sealed"] == 1
                assert final["job"]["job"]["progress"]["corrections_sealed"] == 1

                done = op("status after completion", space + "/seal")
                assert done["active"] is False and done["job"] is None

                # A batch call on an empty head starts a job that completes in
                # the same call: zero points, no error and no dangling job.
                idle = op("batch on empty head", space + "/seal", {"batch_points": 5})
                assert idle["points"] == 0 and idle["job"]["active"] is False
                assert op("idle status", space + "/seal")["active"] is False

                # Blocks exist per series/batch; the logical view is unchanged
                # except the corrected value, which must beat the sealed old one.
                blocks = op("block inventory", space + "/blocks")
                assert len(blocks) >= 2
                after = op("range after seal", space + "/range", interval)
                actual = point_map(after)
                # Rebuild the expected map: correction at 5000, new point at 12500.
                sid = before["series"][0]["series"]["id"]
                expected[(sid, 5000)] = 999.0
                expected[(sid, 12500)] = 77.0
                assert actual == expected, "sealed view lost, duplicated or overwrote a point"

                # A later correction to a now-sealed timestamp still wins.
                op("post-seal correction", space + "/points",
                   {"now": 14000, "points": [{"labels": labels, "time": 1000, "value": -5.0}]})
                op("one-shot seal of new head", space + "/seal", {})
                verify = point_map(op("range after second seal", space + "/range", interval))
                assert verify[(sid, 1000)] == -5.0
                assert verify == {**expected, (sid, 1000): -5.0}
                op("compaction while idle", space + "/compact", {})
                assert point_map(op("range after compaction", space + "/range", interval)) == verify
            finally:
                stop(process)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(observations, indent=2) + "\n")
    print(json.dumps({"workflow": workflow, "operations": len(observations), "state": "complete",
                      "coverage": ["seal-job"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.workflow == "seal-job":
        exercise_seal_job(args.workflow, args.output)
    else:
        exercise(args.workflow, args.output)
