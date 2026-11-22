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
    "explain", "migrate"
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


def launch(binary, database, log, extra_env=None):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = f"127.0.0.1:{port}"
    process = subprocess.Popen(
        [str(binary)], env={**os.environ, "RIDGE_DB": str(database),
                            "RIDGE_ADDR": address, **(extra_env or {})},
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
            q = {"start": -5000, "end": 10000}

            def op(label, route, payload=None, **options):
                out = request(base, route, payload, **options)
                observations.append({"step": label, "request": payload, "response": out})
                return out

            process, base = launch(binary, scratch / "source.db", log)
            try:
                space = "spaces/spindle"

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
                blocks = op("blocks", space + "/blocks")
                if {b["format"] for b in blocks} != {2}:
                    raise RuntimeError("default write format must produce format 2 blocks")
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

            # Migration: build a legacy format 1 database holding overlapping
            # corrections and a deletion, then move it to format 2.
            legacy = scratch / "legacy.db"
            process, base = launch(binary, legacy, log, {"RIDGE_BLOCK_FORMAT": "1"})
            try:
                space = "spaces/relay"
                rate = {**q, "step": 10000, "origin": -5000, "group_by": ["zone"],
                        "function": "rate", "matchers": [{"label": "__name__", "op": "=",
                                                          "value": "pulse_total"}]}
                agg = {**q, "step": 1000, "origin": 0, "group_by": ["zone"], "function": "avg"}
                op("legacy namespace", "spaces", {**seed["namespace"], "name": "relay"})
                op("legacy ingest", space + "/points", {"now": seed["now"], "points": seed["points"]})
                op("legacy seal", space + "/seal", {})
                corrections = [
                    {**seed["points"][2], "value": 5},
                    {**seed["points"][5], "value": 11},
                    {**seed["points"][8], "time": 1500},
                ]
                op("legacy corrections", space + "/points", {"now": 10000, "points": corrections})
                op("legacy reseal", space + "/seal", {})
                op("legacy delete", space + "/deletions", {"start": -600, "end": 600,
                   "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
                op("legacy correct after delete", space + "/points",
                   {"now": 10000, "points": [{**seed["points"][0], "time": 500, "value": 8}]})
                op("legacy materialize", space + "/rollups/pulse", agg, method="PUT")
                legacy_rollup = op("legacy rollup", space + "/rollups/pulse")
                legacy_range = op("legacy range", space + "/range", q)
                legacy_rate = op("legacy rate", space + "/aggregate", rate)
                legacy_avg = op("legacy windows", space + "/aggregate", agg)
                legacy_blocks = op("legacy blocks", space + "/blocks")
                if {b["format"] for b in legacy_blocks} != {1}:
                    raise RuntimeError("legacy input must contain only format 1 blocks")
                legacy_deletions = op("legacy deletions", space + "/deletions")
            finally:
                stop(process)
            process, base = launch(binary, legacy, log, {"RIDGE_BLOCK_FORMAT": "2"})
            try:
                restarted = op("restarted range", space + "/range", q)
                if restarted != legacy_range:
                    raise RuntimeError("format 2 server changed legacy range results")
                if op("restarted rate", space + "/aggregate", rate) != legacy_rate:
                    raise RuntimeError("format 2 server changed legacy rate results")
                if op("restarted windows", space + "/aggregate", agg) != legacy_avg:
                    raise RuntimeError("format 2 server changed legacy window results")
                if op("restarted rollup", space + "/rollups/pulse") != legacy_rollup:
                    raise RuntimeError("restart changed the materialization")
                op("mixed correction", space + "/points",
                   {"now": 10000, "points": [{**seed["points"][0], "time": 1500, "value": 9}]})
                op("mixed seal", space + "/seal", {})
                op("head survivor", space + "/points",
                   {"now": 10000, "points": [{**seed["points"][4], "time": 2500, "value": 4}]})
                mixed_blocks = op("mixed blocks", space + "/blocks")
                if {b["format"] for b in mixed_blocks} != {1, 2}:
                    raise RuntimeError("expected a mixed v1+v2 block library")
                mixed_range = op("mixed range", space + "/range", q)
                mixed_rate = op("mixed rate", space + "/aggregate", rate)
                mixed_avg = op("mixed windows", space + "/aggregate", agg)
                op("mixed materialize", space + "/rollups/pulse", agg, method="PUT")
                pre_migration = op("pre-migration rollup", space + "/rollups/pulse")
                if pre_migration["stale"]:
                    raise RuntimeError("new materialization is stale")
                receipt = op("migrate", space + "/migrate", {})
                rewritten = [b for b in mixed_blocks if b["format"] == 1]
                if (receipt["format"] != 2 or receipt["rewritten"] != len(rewritten)
                        or receipt["points"] != sum(b["count"] for b in rewritten)
                        or receipt["rewritten"] + receipt["current"] != len(mixed_blocks)):
                    raise RuntimeError(f"unexpected migration receipt {receipt}")
                migrated_blocks = op("migrated blocks", space + "/blocks")
                if {b["format"] for b in migrated_blocks} != {2}:
                    raise RuntimeError("migration left non-v2 blocks behind")
                if sorted(b["id"] for b in migrated_blocks) != sorted(b["id"] for b in mixed_blocks):
                    raise RuntimeError("migration changed block identity")
                if sum(b["bytes"] for b in migrated_blocks) >= sum(b["bytes"] for b in mixed_blocks):
                    raise RuntimeError("migration did not shrink payloads")
                post = op("migrated range", space + "/range", q)
                if (post["series"] != mixed_range["series"] or post["floor"] != mixed_range["floor"]
                        or post["stats"] != mixed_range["stats"]
                        or post["truncated"] != mixed_range["truncated"]):
                    raise RuntimeError("migration changed range results")
                if op("migrated rate", space + "/aggregate", rate)["cells"] != mixed_rate["cells"]:
                    raise RuntimeError("migration changed rate results")
                if op("migrated windows", space + "/aggregate", agg)["cells"] != mixed_avg["cells"]:
                    raise RuntimeError("migration changed window results")
                if op("migrated deletions", space + "/deletions") != legacy_deletions:
                    raise RuntimeError("migration changed the deletion inventory")
                stale = op("migrated rollup", space + "/rollups/pulse")
                if not stale["stale"] or stale["result"]["cells"] != pre_migration["result"]["cells"]:
                    raise RuntimeError("migration corrupted the materialization")
                op("rematerialize", space + "/rollups/pulse", agg, method="PUT")
                fresh = op("fresh rollup", space + "/rollups/pulse")
                if fresh["stale"] or fresh["result"]["cells"] != pre_migration["result"]["cells"]:
                    raise RuntimeError("re-materialization after migration diverged")
                before_noop = op("namespace before noop", space)
                noop = op("migrate noop", space + "/migrate", {})
                if noop["rewritten"] != 0 or noop["current"] != len(migrated_blocks):
                    raise RuntimeError(f"unexpected empty migration receipt {noop}")
                if op("namespace after noop", space)["last_sequence"] != before_noop["last_sequence"]:
                    raise RuntimeError("empty migration consumed a sequence")
                source = op("migration backup source", space + "/range", q)
                image, headers = request(base, "image")
                if hashlib.sha256(image).hexdigest() != headers["X-Ridge-Sha256"]:
                    raise RuntimeError("backup hash mismatch")
                (scratch / "migrated-restored.db").write_bytes(image)
                observations.append({"step": "migration backup", "sequence": headers["X-Ridge-Sequence"],
                                     "sha256": headers["X-Ridge-Sha256"], "bytes": len(image)})
            finally:
                stop(process)
            process, base = launch(binary, scratch / "migrated-restored.db", log)
            try:
                restored = request(base, "spaces/relay/range", q)
                if restored != source:
                    raise RuntimeError("restored migrated image differs from source read")
                observations.append({"step": "migrated restored", "response": restored})
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
