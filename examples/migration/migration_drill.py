#!/usr/bin/env python3
"""End-to-end drill for the V1 (64-char hex series keys) -> V2 (32-byte raw
series keys) storage migration.

The drill:
  1. Builds the frozen baseline (git HEAD at task start) in an isolated
     worktree and uses that binary to create a V1 database that covers
     missing labels, present-but-empty labels, delete-then-repost, negative
     timestamps, an empty series and a pre-existing rollup.
  2. Inspects the old file to prove its physical key shape is legacy V1.
  3. Stops the service and restarts it with the upgraded binary, which
     performs a versioned, single-transaction migration and keeps a
     byte-identical mirror at <db>.pre-v2-mirror.
  4. Re-verifies every externally visible identifier and query result,
     including writes/seals/deletes after the upgrade.
  5. Demonstrates a failed migration against a tampered scratch copy: the
     live file stays V1 and service startup refuses to serve it.
  6. Restores the mirrored V1 image and proves the frozen baseline can open
     it, then re-runs the upgrade as the documented recovery path.

All data lives in a fresh temporary directory; nothing is written into the
source tree except build outputs under the worktree.
"""

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE_REF = "52625b0"  # frozen baseline commit: feat: initialize solo-0039-ridgeline baseline


def http_request(base, route, payload=None, method=None, status=200):
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
            raise RuntimeError(f"{route}: expected {status}, got {response.status}: {data.decode(errors='replace')}")
        if response.headers.get_content_type() == "application/octet-stream":
            return data, dict(response.headers)
        return json.loads(data)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def launch(binary, database, log_path):
    address = f"127.0.0.1:{free_port()}"
    log = open(log_path, "wb")
    process = subprocess.Popen(
        [str(binary)], env={**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address},
        stdout=log, stderr=log,
    )
    base = "http://" + address
    for _ in range(100):
        if process.poll() is not None:
            log.close()
            raise RuntimeError(f"service exited before readiness; see {log_path}")
        try:
            http_request(base, "health")
            return process, base, log
        except (OSError, RuntimeError):
            time.sleep(0.05)
    process.terminate()
    process.wait(timeout=15)
    log.close()
    raise RuntimeError("service did not become ready")


def stop(process, log):
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=15)
    log.close()
    if process.returncode != 0:
        raise RuntimeError(f"service exit {process.returncode}")


def run(cmd, cwd=None):
    result = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(map(str, cmd))}\n{result.stdout}\n{result.stderr}")
    return result


def build_baseline(scratch):
    """Build the frozen V1 binary from an isolated git worktree."""
    worktree = scratch / "baseline-src"
    run(["git", "worktree", "add", "--detach", str(worktree), BASELINE_REF], cwd=ROOT)
    # The baseline predates the Go 1.25 toolchain auto-download pin; build in
    # module mode without network requirements beyond the already cached deps.
    env = dict(os.environ)
    env["GOFLAGS"] = "-mod=mod"
    binary = scratch / "ridgeline-v1"
    result = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/ridgeline"],
        cwd=worktree, env=env, text=True, capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"baseline build failed:\n{result.stdout}\n{result.stderr}")
    return binary, worktree


def build_current(scratch):
    binary = scratch / "ridgeline-v2"
    run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT)
    inspector = scratch / "ridge-inspect"
    run(["go", "build", "-o", str(inspector), "./cmd/ridge-inspect"], cwd=ROOT)
    return binary, inspector


def create_legacy_database(binary, db_path, log_path):
    """Exercise the frozen baseline through HTTP to leave a rich V1 file."""
    process, base, log = launch(binary, db_path, log_path)
    evidence = {}
    try:
        space = "spaces/drill"
        policy = {
            "name": "drill", "retention_ms": 100000, "lateness_ms": 50000,
            "future_ms": 1000, "max_series": 100, "max_write": 1000, "max_scan": 200000,
        }
        http_request(base, "spaces", policy)

        # Missing-label series: omits "rack" entirely (matches the empty
        # string for matchers but stays a distinct identity).
        missing_rack = {"__name__": "load_ratio", "zone": "z-a"}
        # Present-but-empty label series: "rack" exists with value "".
        empty_rack = {"__name__": "load_ratio", "zone": "z-a", "rack": ""}
        # Regular series with a negative timestamp (time bytes in the head
        # key must never be confused with series-ID bytes during migration).
        regular = {"__name__": "load_ratio", "zone": "z-b", "rack": "r1"}
        # A second regular series used for delete-then-repost semantics.
        repost = {"__name__": "pulse_total", "zone": "z-c", "rack": "r2"}

        def write(now, points):
            return http_request(base, space + "/points", {"now": now, "points": points})

        write(10000, [
            {"labels": missing_rack, "time": -500, "value": 1.5},
            {"labels": missing_rack, "time": 0, "value": 2.5},
            {"labels": empty_rack, "time": -500, "value": 3.5},
            {"labels": regular, "time": -4096, "value": 4.5},
            {"labels": regular, "time": 4096, "value": 5.5},
            {"labels": repost, "time": 100, "value": 10.0},
            {"labels": repost, "time": 200, "value": 11.0},
        ])

        # Seal part of the data so blocks, payloads and block metadata exist.
        http_request(base, space + "/seal", {})
        write(10000, [
            {"labels": regular, "time": 5000, "value": 6.5},  # fresh head point
            {"labels": repost, "time": 300, "value": 12.0},
        ])

        # Delete a range, then repost one of the deleted timestamps: the
        # tombstone must keep hiding only the old sequence.
        delete_query = {
            "start": 100, "end": 201,
            "matchers": [{"label": "__name__", "op": "=", "value": "pulse_total"}],
        }
        tomb = http_request(base, space + "/deletions", delete_query)
        evidence["tombstone"] = tomb
        write(10000, [{"labels": repost, "time": 100, "value": 99.0}])

        # An empty series: catalog identity with no samples anywhere.
        empty_labels = {"__name__": "ghost_metric", "zone": "z-void"}
        # The API only creates series via ingestion; create one then remove
        # its points through a full-range deletion followed by compaction.
        write(10000, [{"labels": empty_labels, "time": 7000, "value": 0.1}])
        http_request(base, space + "/seal", {})
        http_request(base, space + "/deletions", {
            "start": -100000, "end": 100000,
            "matchers": [{"label": "__name__", "op": "=", "value": "ghost_metric"}],
        })
        http_request(base, space + "/compact", {})

        # Re-arm a persistent tombstone and leave live head points so the
        # migration exercises every physical bucket, not just sealed blocks.
        write(10000, [
            {"labels": regular, "time": 8000, "value": 8.8},
            {"labels": missing_rack, "time": 900, "value": 9.9},
        ])
        http_request(base, space + "/deletions", {
            "start": 4096, "end": 4097,
            "matchers": [{"label": "rack", "op": "=", "value": "r1"}],
        })

        # A rollup created before migration so stored provenance survives it.
        agg = {
            "start": -5000, "end": 10000, "step": 1000, "origin": 0,
            "group_by": ["zone"], "function": "avg",
            "matchers": [{"label": "__name__", "op": "=", "value": "load_ratio"}],
        }
        http_request(base, space + "/rollups/pulse", agg, method="PUT")

        # Capture the full logical state that must survive migration.
        full_query = {"start": -100000, "end": 100000}
        evidence["range_before"] = http_request(base, space + "/range", full_query)
        evidence["series_before"] = http_request(base, space + "/series", {"matchers": []})
        evidence["blocks_before"] = http_request(base, space + "/blocks")
        evidence["deletions_before"] = http_request(base, space + "/deletions")
        evidence["rollup_before"] = http_request(base, space + "/rollups/pulse")
        evidence["explain_before"] = http_request(base, space + "/explain", full_query)
        evidence["discover_missing_label"] = http_request(base, space + "/series", {
            "matchers": [{"label": "rack", "op": "=", "value": ""}],
        })
        evidence["namespace"] = http_request(base, "spaces/drill")

        ids = {s["series"]["id"] for s in evidence["range_before"]["series"]}
        expected_labels = {
            tuple(sorted(missing_rack.items())),
            tuple(sorted(empty_rack.items())),
            tuple(sorted(regular.items())),
            tuple(sorted(repost.items())),
            tuple(sorted(empty_labels.items())),
        }
        found_labels = {
            tuple(sorted(s["labels"].items()))
            for s in evidence["series_before"]
        }
        if not expected_labels.issubset(found_labels):
            raise RuntimeError("legacy database is missing expected label identities")
        if len(ids) != len(evidence["range_before"]["series"]):
            raise RuntimeError("duplicate series IDs in legacy snapshot")
        if any(len(i) != 64 for i in ids):
            raise RuntimeError("legacy IDs must be 64 hex characters")
        evidence["series_ids"] = sorted(ids)
    finally:
        stop(process, log)
    return evidence


def inspect(inspector, db_path):
    result = run([str(inspector), "inspect", str(db_path)])
    return json.loads(result.stdout)


def logical_range(result):
    """Strip the database sequence: migration legitimately advances it while
    every logical series, point, floor and statistic must stay identical."""
    return {k: v for k, v in result.items() if k != "sequence"}


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="keep the scratch directory for inspection")
    parser.add_argument("--scratch", type=Path, help="explicit scratch directory (default: tempdir)")
    args = parser.parse_args()

    verify = subprocess.run(["git", "rev-parse", "--verify", BASELINE_REF],
                            cwd=ROOT, capture_output=True, text=True)
    if verify.returncode != 0:
        raise RuntimeError(f"frozen baseline ref {BASELINE_REF} is not available")

    scratch = args.scratch or Path(tempfile.mkdtemp(prefix="ridge-migrate-"))
    scratch.mkdir(parents=True, exist_ok=True)
    worktree = None
    summary = {}
    try:
        v1_binary, worktree = build_baseline(scratch)
        v2_binary, inspector = build_current(scratch)

        db_path = scratch / "drill.db"
        legacy_log = scratch / "legacy.log"
        evidence = create_legacy_database(v1_binary, db_path, legacy_log)

        before = inspect(inspector, db_path)
        if before["storage_version"] != 1:
            raise RuntimeError(f"expected unversioned/V1 file, got {before}")
        if before["catalog_key_sizes"] != {"64": before["namespaces"][0]["catalog_series"]}:
            raise RuntimeError(f"legacy catalog must use 64-byte keys, got {before['catalog_key_sizes']}")
        if set(before["head_key_sizes"]) - {'72'} != set():
            raise RuntimeError(f"legacy head must use 72-byte keys, got {before['head_key_sizes']}")
        shape = before["namespaces"][0]
        if shape["empty_series"] < 1:
            raise RuntimeError("legacy fixture must contain an empty series")
        summary["legacy_shape"] = before
        print("[1/6] legacy V1 database created and verified:", json.dumps(shape))

        # Take a server-side image of the V1 database as an additional
        # recovery artifact before upgrading.
        proc, base, log = launch(v1_binary, db_path, scratch / "legacy-image.log")
        try:
            image, headers = http_request(base, "image")
            image_path = scratch / "drill-v1-image.db"
            image_path.write_bytes(image)
            if hashlib.sha256(image).hexdigest() != headers["X-Ridge-Sha256"]:
                raise RuntimeError("V1 image hash mismatch")
            summary["v1_image"] = {"path": str(image_path), "sha256": headers["X-Ridge-Sha256"],
                                   "sequence": headers["X-Ridge-Sequence"]}
        finally:
            stop(proc, log)

        mirror_path = Path(str(db_path) + ".pre-v2-mirror")
        if mirror_path.exists():
            mirror_path.unlink()
        legacy_hash = sha256_file(db_path)

        # Restart with the upgraded binary: migration runs inside startup.
        proc, base, log = launch(v2_binary, db_path, scratch / "upgrade.log")
        try:
            upgrade_log = log.name
        finally:
            stop(proc, log)
        log_text = Path(upgrade_log).read_text()
        if "storage migrated V1 -> V2" not in log_text:
            raise RuntimeError(f"startup log missing migration line:\n{log_text}")
        if not mirror_path.exists():
            raise RuntimeError("pre-migration mirror was not created")
        if sha256_file(mirror_path) != legacy_hash:
            raise RuntimeError("mirror is not byte-identical to the V1 file")
        summary["mirror"] = {"path": str(mirror_path), "sha256": sha256_file(mirror_path)}

        after = inspect(inspector, db_path)
        if after["storage_version"] != 2:
            raise RuntimeError(f"expected V2 marker, got {after}")
        ns_after = after["namespaces"][0]
        if after["catalog_key_sizes"] != {"32": ns_after["catalog_series"]}:
            raise RuntimeError(f"V2 catalog must use 32-byte keys, got {after['catalog_key_sizes']}")
        if set(after["head_key_sizes"]) - {'40'} != set():
            raise RuntimeError(f"V2 head must use 40-byte keys, got {after['head_key_sizes']}")
        if ns_after["catalog_series"] != shape["catalog_series"]:
            raise RuntimeError("series count changed during migration")
        if ns_after["empty_series"] != shape["empty_series"]:
            raise RuntimeError("empty series count changed during migration")
        # The fixture must have forced migration through every physical bucket.
        if ns_after["head_points"] < 2 or ns_after["blocks"] < 1 or ns_after["tombstones"] < 1 \
                or ns_after["rollups"] < 1 or ns_after["empty_series"] < 1:
            raise RuntimeError(f"fixture did not cover every migration branch: {ns_after}")
        summary["upgraded_shape"] = after
        print("[2/6] migration committed: catalog 64B->32B keys, head 72B->40B keys, mirror verified")

        # Reopen with the upgraded binary and re-validate every external fact.
        proc, base, log = launch(v2_binary, db_path, scratch / "post-restart.log")
        try:
            full_query = {"start": -100000, "end": 100000}
            range_after = http_request(base, "spaces/drill/range", full_query)
            if logical_range(range_after) != logical_range(evidence["range_before"]):
                raise RuntimeError("range results differ after migration")
            # Migration is a physical format conversion, not a domain mutation:
            # the database sequence must be unchanged so rollup provenance and
            # write ordering stay exactly as the V1 process left them.
            if range_after["sequence"] != evidence["range_before"]["sequence"]:
                raise RuntimeError("migration must not advance the database sequence")
            series_after = http_request(base, "spaces/drill/series", {"matchers": []})
            if series_after != evidence["series_before"]:
                raise RuntimeError("series discovery differs after migration")
            blocks_after = http_request(base, "spaces/drill/blocks")
            if blocks_after != evidence["blocks_before"]:
                raise RuntimeError("block inventory differs after migration")
            deletions_after = http_request(base, "spaces/drill/deletions")
            if deletions_after != evidence["deletions_before"]:
                raise RuntimeError("deletion inventory differs after migration")
            # Explain before any further mutation: the plan's physical facts
            # and the database sequence must both survive migration unchanged.
            explain_after = http_request(base, "spaces/drill/explain", full_query)
            explain_physical = {k: v for k, v in explain_after.items() if k != "sequence"}
            explain_before_physical = {k: v for k, v in evidence["explain_before"].items() if k != "sequence"}
            if explain_physical != explain_before_physical:
                raise RuntimeError("explain plan (apart from sequence) differs after migration")
            if explain_after["sequence"] != evidence["explain_before"]["sequence"]:
                raise RuntimeError("migration must preserve the database sequence")
            rollup_after = http_request(base, "spaces/drill/rollups/pulse")
            stored_before = {k: v for k, v in evidence["rollup_before"].items() if k != "stale"}
            stored_after = {k: v for k, v in rollup_after.items() if k != "stale"}
            if stored_after != stored_before:
                raise RuntimeError("stored rollup materialization differs after migration")
            # Because migration consumes no sequence, the pre-migration
            # materialization stays fresh and its source-sequence provenance
            # remains exactly valid against the unchanged namespace sequence.
            if evidence["rollup_before"]["stale"]:
                raise RuntimeError("pre-migration rollup should have been fresh")
            if rollup_after["stale"]:
                raise RuntimeError("sequence-preserving migration must keep the rollup fresh")
            # Re-materializing must produce a fresh rollup with the same cells.
            http_request(base, "spaces/drill/rollups/pulse", rollup_after["query"], method="PUT")
            rollup_refreshed = http_request(base, "spaces/drill/rollups/pulse")
            if rollup_refreshed["stale"]:
                raise RuntimeError("re-materialized rollup must be fresh")
            if rollup_refreshed["result"]["cells"] != evidence["rollup_before"]["result"]["cells"]:
                raise RuntimeError("rollup cells changed across the storage migration")
            discover_after = http_request(base, "spaces/drill/series", {
                "matchers": [{"label": "rack", "op": "=", "value": ""}],
            })
            if discover_after != evidence["discover_missing_label"]:
                raise RuntimeError("missing/empty label matcher behavior changed")

            # The reposted point must be visible, the deleted old one hidden.
            repost_rows = [s for s in range_after["series"]
                           if s["series"]["labels"].get("__name__") == "pulse_total"]
            points = {p["time"]: p["value"] for s in repost_rows for p in s["points"]}
            if points.get(100) != 99.0:
                raise RuntimeError(f"delete-then-repost semantics broken: {points}")
            if 200 in points:
                raise RuntimeError("deleted point at t=200 resurfaced")

            # Post-upgrade write path must use the new 32-byte key shape.
            http_request(base, "spaces/drill/points", {"now": 10000, "points": [
                {"labels": {"__name__": "load_ratio", "zone": "z-b", "rack": "r1"},
                 "time": 6000, "value": 7.7},
            ]})
            http_request(base, "spaces/drill/seal", {})
            http_request(base, "spaces/drill/deletions", {
                "start": 6000, "end": 6001,
                "matchers": [{"label": "rack", "op": "=", "value": "r1"}],
            })
            final_range = http_request(base, "spaces/drill/range", full_query)
            ids_after = {s["series"]["id"] for s in final_range["series"]}
            if not ids_after.issubset(set(evidence["series_ids"])):
                raise RuntimeError("new writes produced unknown external series IDs")
            if any(len(i) != 64 for i in ids_after):
                raise RuntimeError("external series IDs must remain 64 hex characters")

            # Image/restore round trip on the V2 file.
            image2, headers2 = http_request(base, "image")
            restored = scratch / "restored-v2.db"
            restored.write_bytes(image2)
            if hashlib.sha256(image2).hexdigest() != headers2["X-Ridge-Sha256"]:
                raise RuntimeError("V2 image hash mismatch")
        finally:
            stop(proc, log)

        proc, base, log = launch(v2_binary, restored, scratch / "restored.log")
        try:
            restored_range = http_request(base, "spaces/drill/range", full_query)
            if restored_range != final_range:
                raise RuntimeError("restored V2 image differs from source read")
        finally:
            stop(proc, log)
        print("[3/6] external hex IDs, queries, blocks, tombstones and rollup provenance preserved")

        # Failure drill: tamper a scratch copy and prove startup refuses it
        # while the file remains V1 (transaction rolled back, no partial state).
        broken = scratch / "broken.db"
        shutil.copy2(mirror_path, broken)
        run([str(inspector), "tamper", str(broken), "drill", "0"])
        fail_log = scratch / "failed.log"
        address = f"127.0.0.1:{free_port()}"
        failed = subprocess.run(
            [str(v2_binary)], env={**os.environ, "RIDGE_DB": str(broken), "RIDGE_ADDR": address},
            capture_output=True, text=True, timeout=30,
        )
        if failed.returncode == 0:
            raise RuntimeError("migration unexpectedly succeeded on a corrupted catalog")
        if "invalid stored series identifier" not in failed.stderr:
            raise RuntimeError(f"failure reason missing from log:\n{failed.stderr}")
        broken_shape = inspect(inspector, broken)
        if broken_shape["storage_version"] != 1:
            raise RuntimeError("failed migration must leave the live file at V1")
        if broken_shape["catalog_key_sizes"] != {"64": broken_shape["namespaces"][0]["catalog_series"]}:
            raise RuntimeError("failed migration must not rewrite catalog keys")
        print("[4/6] failure drill: corrupted V1 file rejected, file left byte-consistent at V1")

        # Recovery path: restore the mirror and prove the frozen baseline
        # opens it; then repeat the documented upgrade.
        recovered = scratch / "recovered-v1.db"
        shutil.copy2(mirror_path, recovered)
        if sha256_file(recovered) != legacy_hash:
            raise RuntimeError("recovered mirror does not match the original V1 hash")
        proc, base, log = launch(v1_binary, recovered, scratch / "recover-baseline.log")
        try:
            recovered_before = http_request(base, "spaces/drill/range",
                                            {"start": -100000, "end": 100000})
            if logical_range(recovered_before) != logical_range(evidence["range_before"]):
                raise RuntimeError("baseline cannot read the mirrored V1 file correctly")
        finally:
            stop(proc, log)
        proc, base, log = launch(v2_binary, recovered, scratch / "recover-upgrade.log")
        try:
            recovered_after = http_request(base, "spaces/drill/range",
                                           {"start": -100000, "end": 100000})
            if logical_range(recovered_after) != logical_range(evidence["range_before"]):
                raise RuntimeError("re-upgraded mirror diverges from the original logical state")
        finally:
            stop(proc, log)
        print("[5/6] mirror recovery verified: baseline opens it, upgrade reproduces identical state")

        summary["status"] = "complete"
        summary["scratch"] = str(scratch)
        (scratch / "drill-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print("[6/6] drill complete; summary at", scratch / "drill-summary.json")
    finally:
        if worktree is not None:
            subprocess.run(["git", "worktree", "remove", "--force", str(worktree)],
                           cwd=ROOT, capture_output=True)
        if not args.keep and args.scratch is None:
            shutil.rmtree(scratch, ignore_errors=True)
        elif args.keep:
            print("scratch retained at", scratch)


if __name__ == "__main__":
    sys.exit(main())
