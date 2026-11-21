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
    "delete", "retention", "materialize", "freshness", "compact", "federation",
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
                # Federation: teams publish the same machine's load_ratio into
                # separate namespaces of this one database file. team-ops keeps
                # its initial floor, team-platform advances a higher floor,
                # team-archive is sealed and read-only, team-idle stays empty,
                # team-burst has a tiny scan budget and team-ghost never exists.
                def policy(name, **overrides):
                    body = {"name": name, "retention_ms": 10000, "lateness_ms": 30000,
                            "future_ms": 1000, "max_series": 100, "max_write": 10000,
                            "max_scan": 100000}
                    body.update(overrides)
                    return body

                shared = {"__name__": "load_ratio", "node": "ridge-a", "zone": "east"}
                for name, extra in [("team-ops", {}), ("team-platform", {"retention_ms": 9500}),
                                    ("team-archive", {}), ("team-idle", {}),
                                    ("team-burst", {"max_write": 4, "max_scan": 6})]:
                    op("federation space " + name, "spaces", policy(name, **extra))
                op("federation ingest ops", "spaces/team-ops/points", {"now": 10000, "points": [
                    {"labels": shared, "time": t, "value": v}
                    for t, v in [(-1500, 1), (-500, 2), (500, 3), (1500, 4)]]})
                op("federation ingest platform", "spaces/team-platform/points", {"now": 10000,
                   "points": [{"labels": shared, "time": t, "value": v}
                              for t, v in [(-1500, 10), (-500, 20), (500, 30), (1500, 40)]]})
                op("federation ingest archive", "spaces/team-archive/points", {"now": 10000,
                   "points": [{"labels": shared, "time": t, "value": v}
                              for t, v in [(-500, 5), (500, 7)]]})
                burst = {"__name__": "load_ratio", "node": "ridge-b", "zone": "west"}
                for batch in (range(0, 4), range(4, 8)):
                    op("federation ingest burst", "spaces/team-burst/points", {"now": 10,
                       "points": [{"labels": burst, "time": t, "value": t} for t in batch]})
                op("federation seal archive", "spaces/team-archive/seal", {})
                op("federation archive read-only", "spaces/team-archive/mode",
                   {"read_only": True}, method="PUT")
                op("federation archive write rejected", "spaces/team-archive/points",
                   {"now": 10000, "points": [{"labels": shared, "time": 1500, "value": 9}]},
                   status=409)
                retention = op("federation platform retention",
                               "spaces/team-platform/retention", {"now": 10000})
                if retention["floor"] != 500:
                    raise RuntimeError("platform retention floor should advance to 500")
                fed = {"spaces": ["team-ops", "team-platform", "team-archive",
                                  "team-idle", "team-burst", "team-ghost"],
                       "start": -5000, "end": 10000,
                       "matchers": [{"label": "__name__", "op": "=", "value": "load_ratio"}]}
                op("federated range no spaces", "federation/range", {**fed, "spaces": []},
                   status=400)
                op("federated range duplicate space", "federation/range",
                   {**fed, "spaces": ["team-ops", "team-ops"]}, status=400)
                joint = op("federated range", "federation/range", fed)
                by_space = {entry["space"]: entry for entry in joint["spaces"]}
                if [entry["space"] for entry in joint["spaces"]] != fed["spaces"]:
                    raise RuntimeError("federated range lost request space order")
                if by_space["team-ghost"]["status"] != "missing":
                    raise RuntimeError("missing namespace not distinguishable")
                if by_space["team-burst"]["status"] != "capacity":
                    raise RuntimeError("over-budget namespace not distinguishable")
                idle = by_space["team-idle"]
                if idle["status"] != "ok" or idle["series"] != [] or idle["stats"]["visible"]:
                    raise RuntimeError("empty namespace not distinguishable")
                if not by_space["team-archive"]["read_only"]:
                    raise RuntimeError("read-only policy not surfaced in federated read")
                if not by_space["team-ops"]["floor"] < by_space["team-platform"]["floor"] == 500:
                    raise RuntimeError("per-space retention floors not preserved")
                shared_ids = {}
                for name in ("team-ops", "team-platform", "team-archive"):
                    entries = by_space[name]["series"]
                    if len(entries) != 1:
                        raise RuntimeError(f"{name} should expose exactly the shared series")
                    shared_ids[name] = entries[0]["series"]["id"]
                if len(set(shared_ids.values())) != 1:
                    raise RuntimeError("identical labels should hash identically per space")
                counts = {name: len(by_space[name]["series"][0]["points"])
                          for name in shared_ids}
                if counts != {"team-ops": 4, "team-platform": 2, "team-archive": 2}:
                    raise RuntimeError(f"per-space floors not applied to joint read: {counts}")
                joint_agg = op("federated aggregate", "federation/aggregate",
                               {**fed, "step": 15000, "origin": -5000,
                                "group_by": ["zone"], "function": "avg"})
                east = [c for c in joint_agg["cells"] if c["labels"] == {"zone": "east"}]
                grouped = {c["space"]: (c["value"], c["count"]) for c in east}
                expected = {"team-ops": (2.5, 4), "team-platform": (35.0, 2),
                            "team-archive": (6.0, 2)}
                if grouped != expected:
                    raise RuntimeError(f"same-label groups merged or wrong: {grouped}")
                if any(not c.get("space") for c in joint_agg["cells"]):
                    raise RuntimeError("aggregate cell lost its source space")
                agg_spaces = {s["space"]: s for s in joint_agg["spaces"]}
                if agg_spaces["team-burst"]["status"] != "capacity" or \
                        agg_spaces["team-ghost"]["status"] != "missing":
                    raise RuntimeError("aggregate did not report per-space status")
                if joint_agg["sequence"] != joint["sequence"]:
                    raise RuntimeError("federated responses not at one committed sequence")
                solo = op("single-space sequence", "spaces/team-ops/range",
                          {"start": -5000, "end": 10000,
                           "matchers": [{"label": "__name__", "op": "=",
                                         "value": "load_ratio"}]})
                if solo["sequence"] != joint["sequence"]:
                    raise RuntimeError("federated sequence differs from single-space read")
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
