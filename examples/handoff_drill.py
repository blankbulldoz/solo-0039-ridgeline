"""Reproducible rollup/retention handoff drill on disposable databases.

The drill builds the current tree, starts one server over a fresh temporary
database, and keeps two namespaces in that database:

  * ``archive`` - ingests, materializes a handover rollup, is made read-only,
    has its blocks sealed and compacted, is made writable again after the
    drill's approval step, and advances its retention floor.
  * ``live``    - keeps accepting writes for the whole drill.  It is never
    stopped, and no restore is performed over the live database path.

A third namespace (``tight``) is deliberately smaller than its scan budget.
Its seal succeeds but its compact and retention attempts fail with a capacity
error; the drill records that the failure published nothing.

The output is a JSON evidence bundle.  It contains, for the rollup interval
that retention is about to purge, the pre-advance raw export, the matching
aggregate and the stored rollup cells, each with a SHA-256.  A successor can
therefore check the part of the old rollup whose raw points no longer exist.
The bundle also records block/head inventories, file sizes, freshness flags
and recomputability results as four separate observations, plus a restore
drill that proves an old image omits a later ``live`` write.

Run it with the Go toolchain on PATH:

    go build ./...
    python3 examples/handoff_drill.py --output var/handoff-drill/evidence.json

To keep the temporary databases and the evidence together:

    python3 examples/handoff_drill.py --workdir /tmp/ridge-handoff

To re-check a bundle offline:

    python3 examples/handoff_drill.py --verify var/handoff-drill/evidence.json

The drill needs no external service after Go modules are cached.  It binds an
ephemeral loopback port and exits non-zero if any asserted production
behaviour differs from this revision.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MIN_TIME = -(1 << 52)
MAX_TIME = 1 << 52
FULL_RANGE = {"start": MIN_TIME, "end": MAX_TIME + 1}
SCHEMA = "ridgeline-handoff-evidence/1"

ARCHIVE = "archive"
LIVE = "live"
TIGHT = "tight"
OVERSHOOT = "overshoot"
HANDOVER = "handover"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    data = value if isinstance(value, bytes) else canonical(value).encode()
    return hashlib.sha256(data).hexdigest()


def materialization_digest(view):
    """Hash the stored rollup record without its computed ``stale`` flag."""
    record = {key: view[key] for key in ("name", "query", "source_sequence", "result")}
    return digest(record)


def cell_subset(cells, start, end):
    return sorted(
        [cell for cell in cells if start <= cell["start"] < end],
        key=lambda cell: (canonical(cell["labels"]), cell["start"]),
    )


class Drill:
    def __init__(self, base, evidence):
        self.base = base
        self.evidence = evidence
        self.checks = 0

    def check(self, condition, message):
        self.checks += 1
        if not condition:
            raise RuntimeError("check failed: " + message)

    def api(self, route, payload=None, method=None, expect=200):
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base + "/ridge/v1/" + route,
            data=body,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            data = response.read()
            headers = {key.lower(): value for key, value in response.headers.items()}
            is_binary = response.headers.get_content_type() == "application/octet-stream"
            parsed = data if is_binary else json.loads(data)
            if response.status != expect:
                raise RuntimeError(
                    "{}: expected {}, got {}: {}".format(
                        route, expect, response.status,
                        data[:800].decode(errors="replace"),
                    )
                )
            return parsed, data, headers

    def record(self, label, route, request, response, wire, extra=None):
        item = {
            "step": label,
            "route": route,
            "request": request,
            "response": response,
            "wire_sha256": digest(wire),
        }
        if extra:
            item.update(extra)
        self.evidence["operations"].append(item)
        return response

    def call(self, label, route, payload=None, method=None, expect=200, extra=None):
        response, wire, _headers = self.api(route, payload, method, expect)
        return self.record(label, route, payload, response, wire, extra)

    def outcome(self, label, route, payload=None, method=None):
        """Perform a request whose status is itself the evidence."""
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base + "/ridge/v1/" + route,
            data=body,
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            data = response.read()
            status = response.status
            headers = {key.lower(): value for key, value in response.headers.items()}
            parsed = json.loads(data)
            self.record(label, route, payload, parsed, data,
                        {"status": status, "response_headers": {
                            key: headers[key] for key in
                            ("x-ridge-sha256", "x-ridge-sequence") if key in headers}})
            return status, parsed

    def namespace(self, space):
        return self.call("namespace " + space, "spaces/" + space)

    def blocks(self, space):
        blocks = self.call("blocks " + space, "spaces/" + space + "/blocks")
        return sorted(blocks, key=lambda block: block["id"])

    def explain(self, space):
        return self.call("explain " + space, "spaces/" + space + "/explain", FULL_RANGE)

    def deletions(self, space):
        return self.call("deletions " + space, "spaces/" + space + "/deletions")

    def rollup(self, space, name):
        return self.call(
            "rollup {}/{}".format(space, name),
            "spaces/" + space + "/rollups/" + name,
        )

    def raw(self, space, start, end):
        result = self.call(
            "range {} [{},{})".format(space, start, end),
            "spaces/" + space + "/range",
            {"start": start, "end": end},
        )
        self.check(not result["truncated"], "raw export {} must not be truncated".format(space))
        return result

    def aggregate(self, space, query):
        return self.call("aggregate " + space, "spaces/" + space + "/aggregate", query)

    def file_facts(self, database):
        payload, wire, headers = self.api("image")
        digest_ok = hashlib.sha256(wire).hexdigest() == headers.get("x-ridge-sha256")
        return {
            "database_file_bytes": database.stat().st_size,
            "image_bytes": len(payload),
            "image_sha256": hashlib.sha256(wire).hexdigest(),
            "image_header_sha256": headers.get("x-ridge-sha256"),
            "image_header_sequence": int(headers.get("x-ridge-sequence", "0")),
            "image_header_matches_bytes": digest_ok,
        }

    def capture(self, space, phase, database, rollups=()):
        namespace = self.namespace(space)
        blocks = self.blocks(space)
        plan = self.explain(space)
        tombstones = self.deletions(space)
        file_facts = self.file_facts(database)
        rollup_views = {name: self.rollup(space, name) for name in rollups}
        capture = {
            "space": space,
            "phase": phase,
            "namespace": namespace,
            "global_sequence": plan["sequence"],
            "max_decoded": plan["max_decoded"],
            "head_records": plan["head_records"],
            "source_point_hint": plan["source_point_hint"],
            "blocks": blocks,
            "deletions": tombstones,
            "file": file_facts,
            "rollups": {
                name: {
                    "view": view,
                    "materialization_sha256": materialization_digest(view),
                }
                for name, view in rollup_views.items()
            },
        }
        self.evidence["captures"].append(capture)
        return capture


def build(binary):
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)


def launch(binary, database):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = "127.0.0.1:{}".format(port)
    process = subprocess.Popen(
        [str(binary)],
        env={**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base = "http://" + address
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError(
                "service exited before readiness: "
                + process.stderr.read().decode(errors="replace")
            )
        try:
            urllib.request.urlopen(base + "/ridge/v1/health", timeout=2).close()
            return process, base
        except OSError:
            time.sleep(0.05)
    process.terminate()
    raise RuntimeError("service did not become ready")


def stop(process):
    if process.poll() is None:
        process.terminate()
    try:
        _out, err = process.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        _out, err = process.communicate()
    return process.returncode, err.decode(errors="replace")


def policy(name, max_scan=100, max_write=100, retention_ms=4000):
    return {
        "name": name,
        "retention_ms": retention_ms,
        "lateness_ms": 1000000,
        "future_ms": 1000000,
        "max_series": 100,
        "max_write": max_write,
        "max_scan": max_scan,
    }


def series_points(node, start, end, step, offset):
    return [
        {
            "labels": {"__name__": "load", "node": node, "zone": "east"},
            "time": timestamp,
            "value": (timestamp - start) / step + offset,
        }
        for timestamp in range(start, end, step)
    ]


def run_archive(drill, database):
    drill.call("create archive", "spaces", policy(ARCHIVE, max_scan=100))
    drill.call("create live", "spaces", policy(LIVE, retention_ms=1000000))

    points = (series_points("ridge-a", -2000, 9000, 1000, 0)
              + series_points("ridge-b", -2000, 9000, 1000, 10))
    drill.call("ingest archive", "spaces/archive/points", {"now": 8000, "points": points})
    drill.call(
        "ingest live initial",
        "spaces/live/points",
        {"now": 100, "points": [{
            "labels": {"__name__": "live_bytes", "node": "live-a"},
            "time": 100, "value": 1,
        }]},
    )

    drill.call(
        "delete archive ridge-b [3000,4000)",
        "spaces/archive/deletions",
        {"start": 3000, "end": 4000,
         "matchers": [{"label": "node", "op": "=", "value": "ridge-b"}]},
    )
    drill.call(
        "correct archive ridge-b 3000",
        "spaces/archive/points",
        {"now": 8000, "points": [{
            "labels": {"__name__": "load", "node": "ridge-b", "zone": "east"},
            "time": 3000, "value": 42,
        }]},
    )

    handover_query = {"start": -2000, "end": 9000, "step": 1000, "origin": 0,
                      "group_by": ["node"], "function": "avg"}
    materialized = drill.call(
        "materialize handover",
        "spaces/archive/rollups/" + HANDOVER,
        handover_query, method="PUT",
    )
    at_materialization = drill.namespace(ARCHIVE)
    drill.check(
        materialized["source_sequence"] == at_materialization["last_sequence"],
        "materialization source_sequence equals the namespace last_sequence",
    )
    drill.check(
        materialized["result"]["sequence"] >= materialized["source_sequence"],
        "result.sequence is the global sequence, not the namespace sequence",
    )

    # The other namespace keeps writing.  It must not make this rollup stale.
    drill.call(
        "ingest live during handover",
        "spaces/live/points",
        {"now": 200, "points": [{
            "labels": {"__name__": "live_bytes", "node": "live-a"},
            "time": 200, "value": 2,
        }]},
    )
    live_after_write = drill.namespace(LIVE)
    fresh_view = drill.rollup(ARCHIVE, HANDOVER)
    drill.check(not fresh_view["stale"],
                "a write in another namespace does not stale the archive rollup")
    drill.check(live_after_write["last_sequence"] > at_materialization["last_sequence"],
                "the live namespace advanced while archive was materialized")

    before_tidy_raw = drill.raw(ARCHIVE, -2000, 9000)
    before_tidy = drill.capture(ARCHIVE, "before_tidy", database, [HANDOVER])
    drill.check(before_tidy["deletions"], "tombstone inventory is visible before compaction")
    drill.check(before_tidy["rollups"][HANDOVER]["view"]["stale"] is False,
                "handover rollup is fresh before the read-only transition")
    drill.check(before_tidy["head_records"] == 22, "all archive points are still in head")

    drill.call("archive read-only", "spaces/archive/mode", {"read_only": True}, method="PUT")
    for label, route, payload, method in [
        ("read-only rejects points", "spaces/archive/points",
         {"now": 9000, "points": [points[0]]}, None),
        ("read-only rejects deletion", "spaces/archive/deletions",
         {"start": 0, "end": 1000}, None),
        ("read-only rejects retention", "spaces/archive/retention", {"now": 5000}, None),
        ("read-only rejects materialization", "spaces/archive/rollups/handover-2",
         handover_query, "PUT"),
    ]:
        rejected = drill.call(label, route, payload, method=method, expect=409)
        drill.check(rejected["code"] == "conflict", label + " returns conflict")

    sealed = drill.call("seal archive while read-only", "spaces/archive/seal", {})
    drill.check(sealed["blocks"] == 2 and sealed["points"] == 22,
                "seal moves 22 points into two series blocks")
    after_seal = drill.capture(ARCHIVE, "after_seal", database, [HANDOVER])
    sealed_recompute = drill.aggregate(ARCHIVE, handover_query)
    drill.check(after_seal["rollups"][HANDOVER]["view"]["stale"] is True,
                "seal marks the rollup stale by advancing the namespace sequence")
    drill.check(
        canonical(after_seal["rollups"][HANDOVER]["view"]["result"]["cells"])
        == canonical(sealed_recompute["cells"]),
        "a maintenance-caused stale flag coexists with an exactly matching report",
    )
    compacted = drill.call("compact archive while read-only", "spaces/archive/compact", {})
    drill.check(compacted["points"] == 22, "compaction keeps all 22 visible points")
    after_tidy = drill.capture(ARCHIVE, "after_tidy", database, [HANDOVER])

    tidy_raw = drill.raw(ARCHIVE, -2000, 9000)
    drill.check(tidy_raw["series"] == before_tidy_raw["series"],
                "seal and compact preserve the logical raw points")
    drill.check(not after_tidy["deletions"],
                "compaction resolves and removes the tombstone inventory")
    drill.check(after_tidy["head_records"] == 0, "head is empty after sealing")
    drill.check([block["id"] for block in after_tidy["blocks"]]
                != [block["id"] for block in after_seal["blocks"]],
                "compaction replaces block identifiers")
    drill.check(after_tidy["namespace"]["last_sequence"]
                != before_tidy["namespace"]["last_sequence"],
                "maintenance advances the namespace source sequence")
    drill.check(after_tidy["rollups"][HANDOVER]["view"]["stale"] is True,
                "maintenance marks the rollup stale even though raw points are unchanged")
    drill.check(after_tidy["file"]["database_file_bytes"]
                >= before_tidy["file"]["database_file_bytes"],
                "bbolt does not shrink the database file")
    drill.check(after_tidy["file"]["image_header_matches_bytes"],
                "image digest matches the bytes at the tidy capture")

    # Pre-advance evidence: this is the last state in which the doomed interval
    # can still be exported and recomputed.
    pre_advance = drill.capture(ARCHIVE, "pre_advance", database, [HANDOVER])
    pre_advance_image, pre_advance_wire, pre_advance_headers = drill.api("image")
    drill.check(
        hashlib.sha256(pre_advance_wire).hexdigest()
        == pre_advance_headers.get("x-ridge-sha256"),
        "pre-advance image digest matches its header",
    )
    drill.check(
        int(pre_advance_headers.get("x-ridge-sequence", "0"))
        == pre_advance["file"]["image_header_sequence"],
        "pre-advance capture and image come from the same transaction",
    )
    drill.evidence["pre_advance_image"] = {
        "sequence": int(pre_advance_headers.get("x-ridge-sequence", "0")),
        "sha256": hashlib.sha256(pre_advance_wire).hexdigest(),
        "bytes": len(pre_advance_wire),
        "capture_phase": pre_advance["phase"],
        "namespace_last_sequence": pre_advance["namespace"]["last_sequence"],
        "floor": pre_advance["namespace"]["floor"],
        "note": (
            "this image is the same committed state the purged_segment was "
            "exported from; keep it with the bundle to re-derive the segment later"
        ),
    }
    full_recompute = drill.aggregate(ARCHIVE, handover_query)
    stored_cells = pre_advance["rollups"][HANDOVER]["view"]["result"]["cells"]
    drill.check(canonical(full_recompute["cells"]) == canonical(stored_cells),
                "stale rollup cells still equal a full recomputation at the captured state")

    purged_start, purged_end = -2000, 1000
    purged_raw = drill.raw(ARCHIVE, purged_start, purged_end)
    purged_aggregate = drill.aggregate(
        ARCHIVE, {**handover_query, "start": purged_start, "end": purged_end})
    purged_stored = cell_subset(stored_cells, purged_start, purged_end)
    drill.check(canonical(purged_aggregate["cells"]) == canonical(purged_stored),
                "stored rollup cells for the purge boundary equal the pre-advance aggregate")
    drill.check(len(purged_raw["series"]) == 2, "purge boundary export covers both series")
    drill.check(sum(len(item["points"]) for item in purged_raw["series"]) == 6,
                "purge boundary export has six raw points")
    drill.check(len(purged_aggregate["cells"]) == 6,
                "purge boundary aggregate has six cells")
    drill.check(len(stored_cells) == 22, "handover rollup spans 22 cells")
    purged_segment = {
        "interval": {"start": purged_start, "end": purged_end},
        "raw": purged_raw,
        "aggregate": purged_aggregate,
        "stored_cells": purged_stored,
        "raw_sha256": digest(purged_raw),
        "aggregate_sha256": digest(purged_aggregate),
        "stored_cells_sha256": digest(purged_stored),
        "source_state": {
            "capture_phase": pre_advance["phase"],
            "namespace_last_sequence": pre_advance["namespace"]["last_sequence"],
            "global_sequence": pre_advance["global_sequence"],
            "floor": pre_advance["namespace"]["floor"],
            "watermark": pre_advance["namespace"]["watermark"],
            "rollup_source_sequence":
                pre_advance["rollups"][HANDOVER]["view"]["source_sequence"],
            "rollup_result_sequence":
                pre_advance["rollups"][HANDOVER]["view"]["result"]["sequence"],
            "rollup_stale": pre_advance["rollups"][HANDOVER]["view"]["stale"],
            "rollup_materialization_sha256":
                pre_advance["rollups"][HANDOVER]["materialization_sha256"],
            "block_count": len(pre_advance["blocks"]),
            "head_records": pre_advance["head_records"],
            "deletion_ids": [tomb["id"] for tomb in pre_advance["deletions"]],
            "database_file_bytes": pre_advance["file"]["database_file_bytes"],
        },
    }
    drill.evidence["purged_segment"] = purged_segment

    # Approved write restoration, then the irreversible boundary advance.
    drill.call("archive writable after approval", "spaces/archive/mode",
               {"read_only": False}, method="PUT")
    retention = drill.call("advance retention", "spaces/archive/retention", {"now": 5000})
    drill.check(retention["floor"] == 1000,
                "retention floor advances to the approved boundary")
    drill.check(retention["points"] == 16,
                "retention keeps only points at or above the floor")
    after_advance = drill.capture(ARCHIVE, "after_advance", database, [HANDOVER])

    post_purged_raw = drill.raw(ARCHIVE, purged_start, purged_end)
    post_purged = drill.aggregate(
        ARCHIVE, {**handover_query, "start": purged_start, "end": purged_end})
    post_full = drill.aggregate(ARCHIVE, handover_query)
    drill.check(not post_purged_raw["series"], "raw points below the new floor are gone")
    drill.check(not post_purged["cells"], "the purged segment can no longer be recomputed")
    drill.check(canonical(post_full["cells"]) != canonical(stored_cells),
                "full-interval recomputation no longer matches the old rollup")
    drill.check(
        canonical(after_advance["rollups"][HANDOVER]["view"]["result"]["cells"])
        == canonical(stored_cells),
        "advancing retention does not rewrite the old rollup",
    )
    drill.check(after_advance["rollups"][HANDOVER]["view"]["stale"] is True,
                "old rollup remains explicitly stale after advancement")

    refreshed = drill.call(
        "materialize handover-post under a new name",
        "spaces/archive/rollups/handover-post", handover_query, method="PUT",
    )
    refreshed_cells = refreshed["result"]["cells"]
    drill.check(len(refreshed_cells) == 16,
                "post-advance materialization has only retained cells")
    drill.check(
        canonical(cell_subset(refreshed_cells, purged_start, purged_end)) == "[]",
        "post-advance materialization silently omits the purged segment",
    )
    drill.check(
        refreshed["source_sequence"] == after_advance["namespace"]["last_sequence"],
        "refreshed materialization binds the namespace sequence after retention",
    )

    rollup_assessment = {
        "stored_cells": len(stored_cells),
        "stored_cells_sha256": digest(stored_cells),
        "purged_cells": len(purged_stored),
        "purged_cells_sha256": digest(purged_stored),
        "post_advance_cells": len(refreshed_cells),
        "post_advance_cells_sha256": digest(refreshed_cells),
        "source_sequence": after_advance["rollups"][HANDOVER]["view"]["source_sequence"],
        "result_sequence":
            after_advance["rollups"][HANDOVER]["view"]["result"]["sequence"],
        "namespace_last_sequence": after_advance["namespace"]["last_sequence"],
        "floor": after_advance["namespace"]["floor"],
        "stale": after_advance["rollups"][HANDOVER]["view"]["stale"],
        "verdict": (
            "values verified against raw at the pre-advance state; the old "
            "rollup is the sole surviving record for the purged interval"
        ),
    }
    drill.evidence["rollup_assessment"] = rollup_assessment
    drill.evidence["four_facts"] = {
        "physical_placement": {
            "before_tidy": {
                "blocks": len(before_tidy["blocks"]),
                "head_records": before_tidy["head_records"],
            },
            "after_seal": {
                "blocks": len(after_seal["blocks"]),
                "head_records": after_seal["head_records"],
            },
            "after_tidy": {
                "blocks": len(after_tidy["blocks"]),
                "head_records": after_tidy["head_records"],
                "block_ids": [block["id"] for block in after_tidy["blocks"]],
            },
            "logical_raw_unchanged": True,
        },
        "file_size": {
            "before_tidy_bytes": before_tidy["file"]["database_file_bytes"],
            "after_tidy_bytes": after_tidy["file"]["database_file_bytes"],
            "after_advance_bytes": after_advance["file"]["database_file_bytes"],
            "note": "bbolt reuses free pages; a failed or successful tidy need not shrink the file",
        },
        "materialization_freshness": {
            "before_tidy_stale": before_tidy["rollups"][HANDOVER]["view"]["stale"],
            "after_seal_stale": after_seal["rollups"][HANDOVER]["view"]["stale"],
            "after_tidy_stale": after_tidy["rollups"][HANDOVER]["view"]["stale"],
            "after_advance_stale": after_advance["rollups"][HANDOVER]["view"]["stale"],
            "note": "stale is sequence inequality only; it is neither a content check nor a correctness proof",
        },
        "raw_recomputability": {
            "full_interval_before_advance": True,
            "purged_interval_before_advance": True,
            "purged_interval_after_advance": False,
            "full_interval_after_advance_matches_old": False,
            "note": "retention is irreversible; deletions are not, but compaction removes their inventory",
        },
    }
    return {
        "handover_query": handover_query,
        "materialized": materialized,
        "pre_advance": pre_advance,
        "after_advance": after_advance,
        "live_before_final": drill.namespace(LIVE),
    }


def run_drift(drill):
    """A rollup that was already inconsistent before maintenance starts."""
    drill.call("create drift", "spaces", policy("drift", max_scan=100))
    drill.call(
        "ingest drift",
        "spaces/drift/points",
        {"now": 4000, "points": [{
            "labels": {"__name__": "drift_load", "node": "drift-a"},
            "time": timestamp, "value": timestamp / 1000,
        } for timestamp in range(0, 5000, 1000)]},
    )
    query = {"start": 0, "end": 5000, "step": 1000, "origin": 0,
             "group_by": ["node"], "function": "sum"}
    original = drill.call("materialize drift", "spaces/drift/rollups/drift",
                          query, method="PUT")
    drill.call(
        "correct drift at a pre-existing rollup",
        "spaces/drift/points",
        {"now": 4000, "points": [{
            "labels": {"__name__": "drift_load", "node": "drift-a"},
            "time": 2000, "value": 100,
        }]},
    )
    view = drill.rollup("drift", "drift")
    recomputed = drill.aggregate("drift", query)
    current_sequence = drill.namespace("drift")["last_sequence"]
    drill.check(view["stale"] is True, "drift rollup is stale before any maintenance")
    drill.check(canonical(view["result"]["cells"]) != canonical(recomputed["cells"]),
                "drift rollup no longer matches current raw points")
    drill.check(view["source_sequence"] == original["source_sequence"],
                "the stale view keeps the original source sequence")
    drill.check(view["source_sequence"] < current_sequence,
                "the namespace sequence has moved past the stored provenance")
    assessment = {
        "stored_cells_sha256": digest(original["result"]["cells"]),
        "recomputed_cells_sha256": digest(recomputed["cells"]),
        "stored_source_sequence": view["source_sequence"],
        "current_namespace_sequence": current_sequence,
        "stored_result_sequence": view["result"]["sequence"],
        "stale": view["stale"],
        "verdict": (
            "retain the old report as an unverified historical record only; "
            "do not endorse its values and do not present its purged segment as checked"
        ),
    }
    drill.evidence["drift_rollup"] = assessment
    return assessment


def run_boundary_partial(drill):
    """A floor that lands inside a window changes a cell's value, not its presence.

    ``avg`` over [1000,2000) with points at 1000, 1500, 1750 is 1.42; if the
    floor becomes 1250 the same cell still appears, but now averages only
    1500 and 1750.  Comparing "did the cell disappear" is therefore not enough
    to decide whether a stored rollup is still reconcilable.
    """
    space = "boundary"
    drill.call("create boundary", "spaces", policy(space, retention_ms=6000))
    points = [
        {"labels": {"__name__": "boundary_load", "node": "boundary-a"},
         "time": timestamp, "value": timestamp / 1000}
        for timestamp in (0, 500, 1000, 1500, 1750, 2000, 2500, 3000, 4000)
    ]
    drill.call("ingest boundary", "spaces/{}/points".format(space),
               {"now": 4000, "points": points})
    query = {"start": 0, "end": 5000, "step": 1000, "origin": 0,
             "group_by": ["node"], "function": "avg"}
    materialized = drill.call("materialize boundary", "spaces/{}/rollups/boundary".format(space),
                              query, method="PUT")
    stored = materialized["result"]["cells"]

    drill.call("boundary read-only", "spaces/{}/mode".format(space),
               {"read_only": True}, method="PUT")
    drill.call("seal boundary", "spaces/{}/seal".format(space), {})
    drill.call("boundary writable", "spaces/{}/mode".format(space),
               {"read_only": False}, method="PUT")
    retention = drill.call("advance boundary to a misaligned floor",
                           "spaces/{}/retention".format(space), {"now": 7250})
    drill.check(retention["floor"] == 1250,
                "the retention floor lands inside the [1000,2000) window")

    recomputed = drill.aggregate(space, query)
    stored_by_start = {cell["start"]: cell for cell in stored}
    current_by_start = {cell["start"]: cell for cell in recomputed["cells"]}
    drill.check(0 not in current_by_start, "the fully expired window disappears")
    drill.check(1000 in current_by_start,
                "the boundary window still appears after the partial purge")
    drill.check(
        current_by_start[1000]["value"] != stored_by_start[1000]["value"],
        "the boundary window value changes even though the cell still exists",
    )
    drill.check(current_by_start[1000]["count"] < stored_by_start[1000]["count"],
                "the boundary window keeps fewer observations")
    assessment = {
        "space": space,
        "floor": retention["floor"],
        "boundary_window": [1000, 2000],
        "stored_cell": stored_by_start[1000],
        "recomputed_cell": current_by_start[1000],
        "vanished_window": stored_by_start[0],
        "warning": (
            "presence of a cell after advancement is not evidence that its value "
            "is still reconcilable; a partially purged window keeps its bounds "
            "and changes its average and count"
        ),
    }
    drill.evidence["boundary_partial"] = assessment
    return assessment


def run_capacity_failure(drill, database):
    drill.call("create tight", "spaces",
               policy(TIGHT, max_scan=20, max_write=10, retention_ms=5))
    for batch in range(3):
        points = [{
            "labels": {"__name__": "tight_load", "node": "tight-a"},
            "time": batch * 10 + offset, "value": offset,
        } for offset in range(10)]
        drill.call("ingest tight batch {}".format(batch + 1), "spaces/tight/points",
                   {"now": batch * 10 + 9, "points": points})
        if batch < 2:
            readable = drill.raw(TIGHT, 0, 30)
            drill.check(readable["stats"]["decoded"] == (batch + 1) * 10,
                        "tight batches up to the budget stay readable")

    too_large = drill.call("tight range exceeds budget", "spaces/tight/range",
                           {"start": 0, "end": 30}, expect=422)
    drill.check(too_large["code"] == "capacity",
                "a range over 30 points reports capacity")

    drill.call("tight read-only", "spaces/tight/mode", {"read_only": True}, method="PUT")
    sealed = drill.call("seal tight while read-only", "spaces/tight/seal", {})
    drill.check(sealed["points"] == 30, "seal does not spend the scan budget")
    before = drill.capture(TIGHT, "before_failed_tidy", database)
    failed_compact = drill.call("compact tight", "spaces/tight/compact", {}, expect=422)
    after = drill.capture(TIGHT, "after_failed_tidy", database)
    failed_again = drill.call("compact tight identical retry", "spaces/tight/compact", {},
                              expect=422)
    retry = drill.capture(TIGHT, "after_identical_retry", database)

    drill.check(failed_compact["code"] == "capacity", "compact fails on the scan budget")
    drill.check(failed_again["code"] == "capacity",
                "an identical compact retry fails identically")
    drill.check(after["namespace"] == before["namespace"],
                "failed tidy leaves namespace sequence and floor unchanged")
    drill.check(canonical(after["blocks"]) == canonical(before["blocks"]),
                "failed tidy leaves block inventory unchanged")
    drill.check(after["head_records"] == before["head_records"],
                "failed tidy leaves head records unchanged")
    drill.check(retry["namespace"] == after["namespace"],
                "the identical retry leaves state unchanged")
    drill.check(retry["blocks"] == after["blocks"],
                "the identical retry leaves blocks unchanged")

    drill.call("tight writable for retention", "spaces/tight/mode",
               {"read_only": False}, method="PUT")
    retention_before = drill.capture(TIGHT, "before_failed_retention", database)
    failed_retention = drill.call("advance tight retention inside a block",
                                  "spaces/tight/retention", {"now": 20}, expect=422)
    retention_after = drill.capture(TIGHT, "after_failed_retention", database)
    drill.check(failed_retention["code"] == "capacity",
                "retention fails on the scan budget")
    drill.check(
        retention_after["namespace"]["floor"] == retention_before["namespace"]["floor"],
        "failed retention does not move the floor",
    )
    drill.check(
        retention_after["namespace"]["last_sequence"]
        == retention_before["namespace"]["last_sequence"],
        "failed retention does not advance the sequence",
    )
    drill.check(
        canonical(retention_after["blocks"]) == canonical(retention_before["blocks"]),
        "failed retention does not rewrite blocks",
    )
    assessment = {
        "space": TIGHT,
        "policy_max_scan": retention_before["max_decoded"],
        "sealed_points": sealed["points"],
        "compact_error": failed_compact,
        "identical_retry_error": failed_again,
        "retention_error": failed_retention,
        "intended_floor": 15,
        "floor_after_failure": retention_after["namespace"]["floor"],
        "namespace_sequence_before": retention_before["namespace"]["last_sequence"],
        "namespace_sequence_after": retention_after["namespace"]["last_sequence"],
        "state_unchanged": True,
        "decision": (
            "stop: the transaction rolled back, an identical retry is deterministic "
            "and pointless, and the fixed max_scan cannot be raised through the API"
        ),
    }
    drill.evidence["capacity_failure"] = assessment
    return assessment, retention_after


def run_budget_anatomy(drill, database):
    """Show that the scan cost of a retention/compact attempt is structural.

    Three namespaces hold the same 30 points with ``max_scan = 20``:

      * ``head_cost``  - unsealed head points are counted even below the floor.
      * ``straddle``   - one sealed block of 30 straddles the intended floor.
      * ``skip_all``   - one sealed block of 30 lies entirely below the floor,
                         so it is skipped and the advance succeeds by purging it.

    This is why "the budget failed" and "the boundary moved" are separate
    observations, and why raising ``now`` is not a legitimate fix.
    """
    cases = {}
    for name in ("head_cost", "straddle", "skip_all"):
        drill.call("create " + name, "spaces",
                   policy(name, max_scan=20, max_write=10, retention_ms=5))
        for batch in range(3):
            points = [{
                "labels": {"__name__": "anatomy_load", "node": name},
                "time": batch * 10 + offset, "value": offset,
            } for offset in range(10)]
            drill.call("ingest {} batch {}".format(name, batch + 1),
                       "spaces/{}/points".format(name),
                       {"now": batch * 10 + 9, "points": points})
    for name in ("straddle", "skip_all"):
        sealed = drill.call("seal " + name, "spaces/{}/seal".format(name), {})
        drill.check(sealed["points"] == 30, name + " sealed 30 points")
        blocks = drill.blocks(name)
        drill.check(len(blocks) == 1 and blocks[0]["count"] == 30,
                    name + " holds one 30-point block")

    def attempt(name, now):
        before = drill.capture(name, "before_budget_attempt", database)
        status, outcome = drill.outcome(
            "retention " + name, "spaces/{}/retention".format(name), {"now": now})
        after = drill.namespace(name)
        blocks = drill.blocks(name)
        return {
            "now": now,
            "resulting_floor": after["floor"],
            "sequence_before": before["namespace"]["last_sequence"],
            "sequence_after": after["last_sequence"],
            "blocks_before": len(before["blocks"]),
            "blocks_after": len(blocks),
            "outcome": "rejected" if status != 200 else "applied",
            "status": status,
            "error": outcome if status != 200 else None,
            "receipt": outcome if status == 200 else None,
        }

    cases["head_cost"] = attempt("head_cost", 1000)
    cases["straddle"] = attempt("straddle", 15)
    cases["skip_all"] = attempt("skip_all", 1000)

    drill.check(cases["head_cost"]["outcome"] == "rejected",
                "unsealed head points cost budget even below the floor")
    drill.check(cases["straddle"]["outcome"] == "rejected",
                "a block straddling the floor is decoded in full and rejected")
    drill.check(cases["skip_all"]["outcome"] == "applied",
                "a block entirely below the floor is skipped and the advance succeeds")
    drill.check(cases["head_cost"]["sequence_after"] == cases["head_cost"]["sequence_before"],
                "rejected head-cost attempt publishes nothing")
    drill.check(cases["straddle"]["sequence_after"] == cases["straddle"]["sequence_before"],
                "rejected straddle attempt publishes nothing")
    remaining = drill.raw("skip_all", 0, 40)
    drill.check(not remaining["series"],
                "the skipped-block success purges all raw points")

    assessment = {
        "max_scan": 20,
        "cases": cases,
        "rule": [
            "head records are counted regardless of the floor",
            "a sealed block is decoded in full whenever its End is above the floor",
            "a sealed block whose End is at or below the floor costs nothing and disappears",
            "therefore a rejected advance and an over-aggressive advance can look "
            "identical in receipts while meaning opposite things",
        ],
    }
    drill.evidence["budget_anatomy"] = assessment
    return assessment


def run_overshoot(drill):
    """Warn about forcing retention through by inflating ``now``."""
    drill.call("create overshoot", "spaces",
               policy(OVERSHOOT, max_scan=20, max_write=10, retention_ms=5))
    for batch in range(3):
        points = [{
            "labels": {"__name__": "overshoot_load", "node": "overshoot-a"},
            "time": batch * 10 + offset, "value": offset,
        } for offset in range(10)]
        drill.call("ingest overshoot batch {}".format(batch + 1), "spaces/overshoot/points",
                   {"now": batch * 10 + 9, "points": points})
    drill.call("seal overshoot", "spaces/overshoot/seal", {})
    overshoot = drill.call("overshoot retention with inflated now",
                           "spaces/overshoot/retention", {"now": 1000})
    remaining = drill.raw(OVERSHOOT, 0, 30)
    drill.check(overshoot["floor"] == 995,
                "inflating now advances the floor far past the approved boundary")
    drill.check(not remaining["series"], "inflating now purges every raw point")
    assessment = {
        "space": OVERSHOOT,
        "approved_floor": 15,
        "inflated_now": 1000,
        "resulting_floor": overshoot["floor"],
        "raw_points_remaining": 0,
        "warning": (
            "inflating now can make the scan budget pass by skipping whole "
            "blocks below the new floor; this is irreversible data loss, not "
            "an approved boundary advance"
        ),
    }
    drill.evidence["anti_pattern"] = assessment
    return assessment


def run_restore(drill, binary, scratch, live_before_final):
    image, wire, headers = drill.api("image")
    image_sha = hashlib.sha256(wire).hexdigest()
    drill.evidence["operations"].append({
        "step": "image before final live write",
        "route": "image",
        "sequence": int(headers.get("x-ridge-sequence", "0")),
        "sha256": image_sha,
        "bytes": len(wire),
    })
    drill.check(image_sha == headers.get("x-ridge-sha256"),
                "image digest matches its header")

    drill.call(
        "live confirmed write after image",
        "spaces/live/points",
        {"now": 300, "points": [{
            "labels": {"__name__": "live_bytes", "node": "live-a"},
            "time": 300, "value": 3,
        }]},
    )
    live_after_final = drill.namespace(LIVE)
    live_final_raw = drill.raw(LIVE, 0, 1000)
    drill.check(live_after_final["last_sequence"] > live_before_final["last_sequence"],
                "live advanced after the image was captured")

    restored_db = scratch / "restore-old.db"
    restored_db.write_bytes(wire)
    restored_process, restored_base = launch(binary, restored_db)
    restored_evidence = {"operations": [], "captures": []}
    restored = Drill(restored_base, restored_evidence)
    try:
        restored_live = restored.namespace(LIVE)
        restored_live_raw = restored.raw(LIVE, 0, 1000)
        restored_archive = restored.namespace(ARCHIVE)
        drill.check(restored_live["last_sequence"] == live_before_final["last_sequence"],
                    "old image contains only the live state before the final write")
        drill.check(
            canonical(restored_live_raw["series"]) != canonical(live_final_raw["series"]),
            "old image lacks the live write confirmed after the image",
        )
        drill.check(restored_archive["floor"] == 1000,
                    "image carries the archive retention floor at capture time")
    finally:
        stop(restored_process)

    live_still = drill.raw(LIVE, 0, 1000)
    drill.check(canonical(live_still["series"]) == canonical(live_final_raw["series"]),
                "restoring to a separate path does not touch the running live namespace")
    assessment = {
        "image_sequence": int(headers.get("x-ridge-sequence", "0")),
        "image_sha256": image_sha,
        "live_last_sequence_before_final_write": live_before_final["last_sequence"],
        "live_last_sequence_after_final_write": live_after_final["last_sequence"],
        "restored_live_last_sequence": restored_live["last_sequence"],
        "restored_archive_floor": restored_archive["floor"],
        "live_write_present_in_restored_image": False,
        "live_write_present_in_running_database": True,
        "verdict": (
            "restore only to an isolated path; never point RIDGE_DB at the live "
            "path with this image, because it omits the live write confirmed after capture"
        ),
    }
    drill.evidence["restore_drill"] = assessment
    return assessment


def build_facts():
    facts = {
        "go_version": subprocess.run(
            ["go", "version"], capture_output=True, text=True, check=True
        ).stdout.strip(),
    }
    try:
        facts["git_head"] = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True,
            text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        facts["git_head"] = "unavailable"
    return facts


def run(output, workdir, operator, approval):
    evidence = {
        "schema": SCHEMA,
        "drill": "rollup-retention-handoff",
        "build": build_facts(),
        "operator": operator,
        "approval": approval,
        "operations": [],
        "captures": [],
    }
    scratch = None
    if workdir:
        workdir = Path(workdir)
        workdir.mkdir(parents=True, exist_ok=True)
        if (workdir / "source.db").exists():
            raise RuntimeError(
                "{} already contains source.db; choose an empty workdir to keep the run reproducible".format(workdir)
            )
        scratch = workdir
    else:
        scratch = Path(tempfile.mkdtemp(prefix="ridgeline-handoff-"))
    database = scratch / "source.db"
    binary = scratch / "ridgeline"
    build(binary)
    process, base = launch(binary, database)
    drill = Drill(base, evidence)
    live_process = None
    try:
        handover = run_archive(drill, database)
        drift = run_drift(drill)
        capacity, tight_final = run_capacity_failure(drill, database)
        anatomy = run_budget_anatomy(drill, database)
        boundary = run_boundary_partial(drill)
        overshoot = run_overshoot(drill)
        restore = run_restore(drill, binary, scratch, handover["live_before_final"])
        drill.evidence["conclusions"] = {
            "successful_advance": {
                "space": ARCHIVE,
                "floor": 1000,
                "points_kept": 16,
                "rollup_result_unchanged_and_stale": True,
                "purged_segment_preserved_in_evidence": True,
                "continuation": "successor verifies the bundle, then uses a new rollup name for retained data",
            },
            "failed_tidy": {
                "space": TIGHT,
                "error": "capacity",
                "state_unchanged": True,
                "decision": "stop; do not retry identically and do not inflate now",
            },
            "drift": drift["verdict"],
            "budget_anatomy": anatomy["rule"],
            "boundary_partial": boundary["warning"],
            "restore": restore["verdict"],
            "overshoot_warning": overshoot["warning"],
        }
    finally:
        stop(process)
    evidence["checks"] = drill.checks
    evidence_path = Path(output)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return evidence, evidence_path, scratch


REQUIRED_SOURCE_STATE = (
    "namespace_last_sequence",
    "global_sequence",
    "floor",
    "watermark",
    "rollup_source_sequence",
    "rollup_result_sequence",
    "rollup_stale",
    "rollup_materialization_sha256",
    "block_count",
    "head_records",
    "deletion_ids",
    "database_file_bytes",
)


def verify(path):
    bundle = json.loads(Path(path).read_text())
    if bundle.get("schema") != SCHEMA:
        raise RuntimeError("unexpected evidence schema: {}".format(bundle.get("schema")))
    purged = bundle.get("purged_segment")
    if not purged:
        raise RuntimeError("evidence bundle has no purged_segment")
    checks = 0
    for field in ("raw", "aggregate", "stored_cells"):
        expected = purged.get(field + "_sha256")
        if expected is None:
            raise RuntimeError("purged_segment.{}_sha256 is missing".format(field))
        if digest(purged[field]) != expected:
            raise RuntimeError("purged_segment.{} failed its SHA-256 check".format(field))
        checks += 1
    if canonical(purged["aggregate"]["cells"]) != canonical(purged["stored_cells"]):
        raise RuntimeError(
            "stored rollup cells do not match the preserved pre-advance aggregate"
        )
    checks += 1
    subset = cell_subset(purged["stored_cells"],
                         purged["interval"]["start"], purged["interval"]["end"])
    if canonical(subset) != canonical(purged["stored_cells"]):
        raise RuntimeError("stored purged cells exceed the declared purge interval")
    checks += 1
    state = purged.get("source_state", {})
    missing = [name for name in REQUIRED_SOURCE_STATE if name not in state]
    if missing:
        raise RuntimeError("source_state is missing: " + ", ".join(missing))
    checks += 1
    if state["rollup_stale"] is False and (
        state["rollup_source_sequence"] != state["namespace_last_sequence"]
    ):
        raise RuntimeError("a fresh rollup must bind the captured namespace sequence")
    checks += 1
    after = bundle.get("rollup_assessment", {})
    if after:
        if digest(after.get("stored_cells_sha256")) == digest(""):
            raise RuntimeError("rollup assessment is incomplete")
        checks += 1
    four = bundle.get("four_facts", {})
    for name in ("physical_placement", "file_size",
                 "materialization_freshness", "raw_recomputability"):
        if name not in four:
            raise RuntimeError("four_facts is missing " + name)
        checks += 1
    image = bundle.get("pre_advance_image", {})
    if not image or image.get("sequence") != state.get("global_sequence"):
        raise RuntimeError(
            "pre_advance_image must be the same committed state as purged_segment")
    checks += 1
    conclusions = bundle.get("conclusions", {})
    if conclusions.get("failed_tidy", {}).get("state_unchanged") is not True:
        raise RuntimeError("failed tidy must be recorded as publishing nothing")
    checks += 1
    if conclusions.get("successful_advance", {}).get("floor") is None:
        raise RuntimeError("successful advance floor is missing")
    checks += 1
    print(json.dumps({
        "verify": str(path),
        "checks": checks,
        "state": "complete",
        "source_state": state,
    }, indent=2, sort_keys=True))
    return bundle


def main():
    parser = argparse.ArgumentParser(
        description="Run or verify a reproducible rollup/retention handoff drill.")
    parser.add_argument("--output", type=Path,
                        help="evidence bundle path (default: WORKDIR/evidence.json or ./var/handoff-drill/evidence.json)")
    parser.add_argument("--workdir", type=Path,
                        help="directory that keeps source.db, restore-old.db and the binary")
    parser.add_argument("--operator", default="drill",
                        help="recorded in the bundle; use the on-call identifier")
    parser.add_argument("--approval", default="drill-only",
                        help="recorded in the bundle; use the change approval reference")
    parser.add_argument("--verify", type=Path,
                        help="verify an existing evidence bundle offline")
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
        return
    if args.output is None:
        args.output = (args.workdir / "evidence.json") if args.workdir else (
            ROOT / "var" / "handoff-drill" / "evidence.json")
    evidence, evidence_path, scratch = run(
        args.output, args.workdir, args.operator, args.approval)
    print(json.dumps({
        "drill": evidence["drill"],
        "checks": evidence["checks"],
        "evidence": str(evidence_path),
        "workdir": str(scratch),
        "state": "complete",
        "successful_advance_floor": evidence["conclusions"]["successful_advance"]["floor"],
        "failed_tidy": evidence["conclusions"]["failed_tidy"]["error"],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"state": "failed", "detail": str(exc)}), file=sys.stderr)
        raise
