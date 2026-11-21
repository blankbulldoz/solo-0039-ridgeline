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
    "lateness"
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


def lateness_scenario(binary, scratch):
    """Interleave a fast and a slow device across a lateness mode switch.

    Policy: lateness_ms=200, retention_ms=6000, future_ms=1000. The fast
    device writes every other second, the slow device every ten milliseconds.
    In namespace mode one fast point at t=5000 makes the slow device's next
    point 3990ms late. In series mode each device is judged only against its
    own watermark, so the slow device resumes inside its 200ms window.
    """
    observations = []

    def record(label, payload, outcome):
        observations.append({"step": label, "request": payload, "response": outcome})

    with (scratch / "lateness.log").open("wb") as log:
        process, base = launch(binary, scratch / "lateness.db", log)
        try:
            space = "spaces/dual"
            fast = {"__name__": "pulse_total", "node": "fast"}
            slow = {"__name__": "pulse_total", "node": "slow"}

            def point(labels, t, value):
                return {"labels": labels, "time": t, "value": value}

            def write(label, now, points, accepted):
                payload = {"now": now, "points": points}
                try:
                    out = request(base, space + "/points", payload)
                    outcome = {"accepted": out["accepted"], "sequence": out["sequence"],
                               "watermark": out["watermark"], "late_mode": out["late_mode"]}
                except RuntimeError as exc:
                    outcome = {"rejected": str(exc)}
                record(label, payload, outcome)
                if accepted != ("accepted" in outcome):
                    raise RuntimeError(f"{label}: unexpected outcome {outcome}")
                return outcome

            policy = {"name": "dual", "retention_ms": 6000, "lateness_ms": 200,
                      "future_ms": 1000, "max_series": 20, "max_write": 100,
                      "max_scan": 100000}
            record("create", policy, request(base, "spaces", policy))
            record("invalid late mode", {"late_mode": "device"},
                   request(base, space + "/late-mode", {"late_mode": "device"},
                           method="PUT", status=400))

            # Both devices start together, so each series has a real watermark.
            write("namespace mode: fast t=1000", 1000, [point(fast, 1000, 1)], True)
            write("namespace mode: slow t=1000", 1000, [point(slow, 1000, 1)], True)

            # The fast device lifts the namespace watermark to 5000 while the
            # slow device has only reached t=1000.
            write("namespace mode: fast t=3000", 3000, [point(fast, 3000, 2)], True)
            write("namespace mode: fast t=5000", 5000, [point(fast, 5000, 3)], True)
            write("namespace mode: slow t=1010 late against t=5000", 5000,
                  [point(slow, 1010, 2)], False)

            # Switching to series mode keeps the namespace watermark. The
            # existing fast series already has a newer per-series record than
            # the slow one, so the slow device regains its own 200ms window.
            record("switch to series", {"late_mode": "series"},
                   request(base, space + "/late-mode", {"late_mode": "series"}, method="PUT"))
            write("series mode: slow t=1020 accepted", 5000, [point(slow, 1020, 2)], True)
            write("series mode: slow t=1030 accepted", 5000, [point(slow, 1030, 3)], True)

            # A correction at an accepted timestamp inside the series window
            # is admitted; one older than the window is still rejected.
            write("series mode: slow correction t=1025 accepted", 5000,
                  [point(slow, 1025, 9)], True)
            write("series mode: slow history t=800 rejected", 5000,
                  [point(slow, 800, 0)], False)

            # A brand new series has no previous timestamp: its first batch is
            # bounded only by future skew and retention.
            fresh = {"__name__": "pulse_total", "node": "fresh"}
            write("series mode: new series t=4000 accepted", 5000,
                  [point(fresh, 4000, 1)], True)
            record("watermarks after new series", None,
                   request(base, space + "/watermarks"))

            # Deleting every sample of a series hides data but keeps catalog
            # identity and watermark: an in-window re-report is visible again,
            # while history older than the window stays rejected.
            record("delete every slow sample", {"start": 1000, "end": 1040},
                   request(base, space + "/deletions", {"start": 1000, "end": 1040,
                           "matchers": [{"label": "node", "op": "=", "value": "slow"}]}))
            write("series mode: slow t=1010 re-report after delete accepted", 5000,
                  [point(slow, 1010, 4)], True)
            write("series mode: slow t=800 after emptying rejected", 5000,
                  [point(slow, 800, 5)], False)

            # Retention outranks lateness in both modes: t=1030 is inside the
            # slow series window but below the floor of 2000.
            record("retention", {"now": 8000},
                   request(base, space + "/retention", {"now": 8000}))
            write("series mode: slow t=1030 before retention floor rejected", 5000,
                  [point(slow, 1030, 6)], False)
            write("both modes: fast t=7000 beyond future skew rejected", 5000,
                  [point(fast, 7000, 4)], False)

            record("switch back to namespace", {"late_mode": "namespace"},
                   request(base, space + "/late-mode", {"late_mode": "namespace"}, method="PUT"))
            # t=3000 is above the retention floor of 2000 but 2000ms behind
            # the namespace watermark of 5000: namespace mode rejects it as
            # late, where series mode would have admitted it for the slow
            # device's own watermark of 1030.
            write("namespace mode: slow t=3000 late against shared t=5000 again", 5000,
                  [point(slow, 3000, 7)], False)
            record("final watermarks", None, request(base, space + "/watermarks"))
        finally:
            stop(process)
    return observations


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
        if workflow in ("all", "lateness"):
            observations.extend(lateness_scenario(binary, scratch))
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
