"""Demonstrate the namespace label rewrite (host -> node) end to end.

Three independent volumes show the cases the migration must get right:

  absent   - some series never had a `host` label, so their identity must not
             change and they must not gain an empty `node` label;
  empty    - a series carries an explicit empty `host` value, which is a present
             label and must move as empty rather than vanish;
  conflict - a merge where two source series collapse onto one identity and both
             committed a value at the same timestamp. The default policy refuses
             the job; keep_last and deduplicate_identical are shown explicitly.

Every scenario proves that a deletion interval and a materialized rollup still
point at the right object after the rewrite, and that a database image produced
by the previous release (built from the baseline commit, before this feature)
still opens and migrates. The script drives the real HTTP API and prints one
JSON document per scenario to stdout.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]

POLICY = {
    "retention_ms": 100000, "lateness_ms": 100000, "future_ms": 100000,
    "max_series": 1000, "max_write": 10000, "max_scan": 1000000,
}

# Timestamps chosen so the deletion interval (1500,2500) hides the second sample
# of the merged series while leaving later corrections visible.
SCENARIOS = {
    "absent": [
        {"labels": {"__name__": "cpu", "host": "host-1"}, "time": 1000, "value": 1},
        {"labels": {"__name__": "cpu", "host": "host-1"}, "time": 2000, "value": 2},
        # never had host: identity and samples must stay untouched
        {"labels": {"__name__": "disk"}, "time": 1000, "value": 9},
        {"labels": {"__name__": "disk"}, "time": 2000, "value": 8},
    ],
    "empty": [
        {"labels": {"__name__": "cpu", "host": ""}, "time": 1000, "value": 5},
        {"labels": {"__name__": "cpu", "host": ""}, "time": 2000, "value": 6},
        {"labels": {"__name__": "cpu", "host": "host-2"}, "time": 1000, "value": 7},
    ],
    "conflict": [
        # Historic host-labelled series and the already-new node-labelled series
        # describe the same node. Renaming host -> node merges them, and both
        # committed at t=1000 with different values.
        {"labels": {"__name__": "cpu", "host": "host-1", "zone": "east"},
         "time": 1000, "value": 1},
        {"labels": {"__name__": "cpu", "host": "host-1", "zone": "east"},
         "time": 2000, "value": 2},
        {"labels": {"__name__": "cpu", "node": "host-1", "zone": "east"},
         "time": 1000, "value": 40},
        {"labels": {"__name__": "cpu", "node": "host-1", "zone": "east"},
         "time": 3000, "value": 30},
    ],
}

ROLLUP = {
    "start": 0, "end": 10000, "step": 5000, "origin": 0,
    "group_by": ["host"], "function": "sum",
    "matchers": [{"label": "__name__", "op": "=", "value": "cpu"}],
}
RENAME = {"from": "host", "to": "node"}


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
    for _ in range(120):
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


def seed(base):
    """Create namespace, points and both persisted references."""
    space = "spaces/relabel"
    request(base, "spaces", {"name": "relabel", **POLICY})
    return space


def seed_references(base, space, payload):
    request(base, space + "/points", {"now": 100000, "points": payload})
    request(base, space + "/deletions", {
        "start": 1500, "end": 2500,
        "matchers": [{"label": "__name__", "op": "=", "value": "cpu"}],
    })
    request(base, space + "/rollups/cpu_by_host", ROLLUP, method="PUT")


def compact_plan(plan):
    return {
        "blocked": plan["blocked"],
        "work_needed": plan["work_needed"],
        "reasons": plan["reasons"],
        "summary": plan["summary"],
        "changes": [
            {"from": c["series_id"][:12], "to": c["new_series_id"][:12],
             "labels": c["labels"], "new_labels": c["new_labels"],
             "points": c["points"]}
            for c in plan["changes"]
        ],
        "merges": [
            {"target": m["new_series_id"][:12],
             "sources": [s[:12] for s in m["series_ids"]],
             "points": m["points"], "collisions": m["collisions"]}
            for m in plan["merges"]
        ],
        "collisions": [
            {"target": c["new_series_id"][:12], "time": c["time"],
             "candidates": c["candidates"],
             "chosen": (c["chosen"] or "")[:12],
             "conflicting": c["conflicting"]}
            for c in plan["collisions"]
        ],
        "label_conflicts": [
            {"series": c["series_id"][:12], "from": c["from_value"],
             "to": c["to_value"], "resolved": c["resolved"]}
            for c in plan["label_conflicts"]
        ],
        "rollups": plan["rollups"],
    }


def migrate_observations(base, space, scope):
    """Preview, execute (with the right policy) and record the result."""
    out = {"preview": compact_plan(
        request(base, space + "/relabel/preview", {"rename": RENAME}))}
    if scope == "conflict":
        out["refused_default"] = request(
            base, space + "/relabel", {"rename": RENAME}, status=409)
        out["refused_dedup"] = request(base, space + "/relabel", {
            "rename": RENAME, "job": "dedup", "value_conflict": "deduplicate_identical",
        }, status=409)
        out["executed"] = request(base, space + "/relabel", {
            "rename": RENAME, "job": "merge", "value_conflict": "keep_last",
        })
        job = "merge"
    else:
        out["executed"] = request(base, space + "/relabel", {"rename": RENAME, "job": "rename"})
        job = "rename"
    out["state"] = {
        "discovery": request(base, space + "/series", {"matchers": []}),
        "range": request(base, space + "/range", {"start": 0, "end": 10000}),
        "deletions": request(base, space + "/deletions"),
        "rollup": request(base, space + "/rollups/cpu_by_host"),
        "job": request(base, space + f"/relabel/jobs/{job}"),
    }
    # Replaying a completed job is a no-op that returns the same sequence.
    replay = request(base, space + "/relabel", {
        "rename": RENAME, "job": job,
        **({"value_conflict": "keep_last"} if scope == "conflict" else {}),
    })
    out["replay"] = {"state": replay["state"], "result_sequence": replay["result_sequence"]}
    return out


def build_binaries(tmp: Path) -> tuple[Path, Path]:
    server = tmp / "ridgeline"
    drill = tmp / "relabel-drill"
    subprocess.run(["go", "build", "-o", str(drill), "./cmd/relabel-drill"], cwd=ROOT, check=True)
    subprocess.run(["go", "build", "-o", str(server), "./cmd/ridgeline"], cwd=ROOT, check=True)
    return server, drill


def drill(drill_binary, *args):
    out = subprocess.run([str(drill_binary), *map(str, args)], check=True,
                         capture_output=True, text=True)
    return json.loads(out.stdout) if out.stdout.strip() else None


def pause_and_recovery_case(server, drill_binary, scratch, log):
    """A crashed job leaves a pause marker and a running record. Writes must be
    refused while paused, and a restart must fail the job, clear the pause, leave
    data untouched and admit ingestion again."""
    database = scratch / "pause.db"
    process, base = launch(server, database, log)
    space = "spaces/relabel"
    try:
        seed(base)
        seed_references(base, space, SCENARIOS["absent"])
        before = request(base, space + "/range", {"start": 0, "end": 10000})
    finally:
        stop(process)
    drill(drill_binary, "set-pause", database, "relabel", "crashed")
    paused = drill(drill_binary, "inspect", database, "relabel")
    # While paused, the production admission policy refuses ingestion before the
    # server is even restarted.
    paused_admission = drill(drill_binary, "check-admission", database, "relabel")
    process, base = launch(server, database, log)
    try:
        # Recovery runs at open: the pause is gone but the attempt is recorded
        # as failed, and ingestion works again.
        resumed = request(base, "spaces/relabel")
        write = request(base, space + "/points", {"now": 100000, "points": [
            {"labels": {"__name__": "cpu", "host": "host-1"}, "time": 4000, "value": 3},
        ]})
        after = request(base, space + "/range", {"start": 0, "end": 10000})
        job = request(base, space + "/relabel/jobs/crashed")
    finally:
        stop(process)
    recovered = [s for s in after["series"] if any(p["time"] == 4000 for p in s["points"])]
    original = [s for s in after["series"] if any(p["time"] == 1000 for p in s["points"])]
    return {
        "paused": {
            "marker": paused["maintenance"], "job": paused["maintenance_job"],
            "admitted": paused_admission["admitted"],
            "refusal": paused_admission["refusal"],
        },
        "recovered": {
            "marker": resumed.get("maintenance", ""),
            "job_state": job["state"], "job_completed": job["completed"],
            "admitted": write["accepted"],
        },
        # The interrupted job never rewrote anything: original identities still
        # exist and the post-recovery write landed normally.
        "original_series_intact": len(original) == len(before["series"]),
        "new_write_present": len(recovered) == 1,
    }


def baseline_binary(commit: str) -> Path:
    """Build the previous release from a git commit to obtain old-format images."""
    target = Path(tempfile.mkdtemp(prefix="ridge-old-"))
    archive = subprocess.run(["git", "archive", commit], cwd=ROOT, check=True,
                             stdout=subprocess.PIPE).stdout
    tar_path = target / "source.tar"
    tar_path.write_bytes(archive)
    with tarfile.open(tar_path) as bundle:
        bundle.extractall(target / "source")
    binary = target / "ridgeline"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"],
                   cwd=target / "source", check=True)
    return binary


def old_image_case(scope, old_binary, new_binary, scratch, log):
    """Create an image with the previous release, then migrate it with this one."""
    source = scratch / f"{scope}-old-source.db"
    process, base = launch(old_binary, source, log)
    try:
        space = seed(base)
        seed_references(base, space, SCENARIOS[scope])
        image, headers = request(base, "image")
    finally:
        stop(process)
    restored = scratch / f"{scope}-old.db"
    restored.write_bytes(image)
    process, base = launch(new_binary, restored, log)
    try:
        space = "spaces/relabel"
        # The image has no job inventory yet; opening it must still succeed and
        # the new endpoint must answer with an empty list.
        before_jobs = request(base, space + "/relabel/jobs")
        result = migrate_observations(base, space, scope)
        after_jobs = request(base, space + "/relabel/jobs")
    finally:
        stop(process)
    return {
        "image_sha256_matches": hashlib.sha256(image).hexdigest() == headers["X-Ridge-Sha256"],
        "jobs_before": before_jobs,
        "migrated": result,
        "jobs_after": [{"id": j["id"], "state": j["state"], "completed": j["completed"]}
                       for j in after_jobs["jobs"]],
    }


def current_release_case(scope, binary, scratch, log):
    database = scratch / f"{scope}.db"
    process, base = launch(binary, database, log)
    try:
        space = seed(base)
        seed_references(base, space, SCENARIOS[scope])
        before = {
            "discovery": request(base, space + "/series", {"matchers": []}),
            "range": request(base, space + "/range", {"start": 0, "end": 10000}),
            "deletions": request(base, space + "/deletions"),
            "rollup": request(base, space + "/rollups/cpu_by_host"),
        }
        result = migrate_observations(base, space, scope)
        image, headers = request(base, "image")
        result["image"] = {
            "sha256_matches": hashlib.sha256(image).hexdigest() == headers["X-Ridge-Sha256"],
            "sequence": headers["X-Ridge-Sequence"],
        }
    finally:
        stop(process)
    return {"scope": scope, "before": before, **result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline", default="52625b0",
                        help="git commit of the release that predates the relabel feature")
    parser.add_argument("--skip-old-image", action="store_true")
    args = parser.parse_args()
    result = {"scenarios": []}
    with tempfile.TemporaryDirectory(prefix="ridge-relabel-") as scratch:
        scratch = Path(scratch)
        binary, drill_binary = build_binaries(scratch)
        with (scratch / "service.log").open("wb") as log:
            old = None if args.skip_old_image else baseline_binary(args.baseline)
            for scope in ("absent", "empty", "conflict"):
                observation = current_release_case(scope, binary, scratch, log)
                if old is not None:
                    observation["old_format_image"] = old_image_case(
                        scope, old, binary, scratch, log)
                result["scenarios"].append(observation)
            result["pause_recovery"] = pause_and_recovery_case(
                binary, drill_binary, scratch, log)
    data = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(data + "\n")
    print(data)


if __name__ == "__main__":
    main()
