"""Rehearse Ridgeline volume migration, crash fallback and evidence handling.

Topology (all paths are temporary and every database file is independent):

  oldvol/ridgeline.db    production stand-in on the "old" volume
  newvol/candidate.db    consistent image restored on the "new" volume
  quarantine/            byte-preserved crash evidence, never opened by a server
  work/                  disposable inspection working copy

One loopback port plays the FIXED service address for the whole scenario.
Verification, inspection and replay instances always bind separate temporary
addresses, so the fixed address is only ever served by the old database or by
a verified candidate.

Scenario flow (executed twice: second batch committed, and not committed):

  1. Seed the old database on the fixed address: history, seal, a retention
     floor, a deletion, a post-delete correction and a materialized rollup
     that a later write makes stale.
  2. Declare a write freeze, then copy: GET /ridge/v1/image runs in one bbolt
     read transaction, so the candidate is consistent by construction. The
     old instance keeps serving identical reads before, during and after.
  3. Verify the candidate on a temporary address against the live old
     instance: samples (full and truncated ranges), deletions (inventory and
     hidden-point visibility), the retention floor (pre-floor writes are
     rejected) and rollup provenance (source sequence and staleness).
  4. Switch: stop the old server, start the candidate on the fixed address.
  5. Batch one is acknowledged and its receipt kept by the client. Batch two
     mixes same-timestamp corrections with new points; the client never
     receives its receipt. The candidate process is then killed (SIGKILL).
  6. Inspect the crashed database file in isolation on a scratch copy and
     decide whether batch two committed (atomic: every one of its points
     stored under a single sequence) or is entirely absent.
  7. Restore the old directory: replay only the post-image delta (batch one,
     plus batch two iff it actually committed) into the old database on a
     temporary address, verify logical equality with the crashed candidate,
     and only then put the old directory back on the fixed address.
  8. Verify winning values and whole-batch results on the fixed address. In
     the uncommitted scenario the client finally retransmits batch two; both
     scenarios converge to identical visible state.

Data that must move before the fixed address is restored: only the delta
whose database sequence is greater than the migration image sequence (here
the two catch-up batches). Production history dwarfs those batches, so the
replay moves points, not files; the record prints image bytes next to replay
bytes to make the ratio explicit. Evidence that is sufficient to resume
collection, and how the scene is preserved when it is not, is recorded in
OPERATIONAL_NOTES and embedded in the JSON record.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MIN_TIME = -(1 << 52)
MAX_TIME = 1 << 52

OPERATIONAL_NOTES = {
    "data_to_move_before_restoring_fixed_address": (
        "Only the post-image delta crosses back to the old directory: samples "
        "whose database sequence is greater than the migration image sequence "
        "(in this rehearsal the two catch-up batches), grouped by sequence and "
        "replayed in order through the normal ingest API so each batch stays "
        "atomic and last-write-wins ordering is preserved. The production "
        "history never leaves the old volume; copying the whole candidate "
        "file back would move the entire database again for zero logical "
        "gain. The record carries full_image_bytes next to per-batch replay "
        "bytes so the ratio is explicit: in production the image holds the "
        "large history while the two batches are tiny by comparison."
    ),
    "sufficient_evidence_to_resume_collection": [
        "Client-held receipt of batch one (sequence, accepted count, "
        "watermark): a confirmed write that must survive any fallback.",
        "Quarantined candidate file (sha256 recorded at crash time, copy "
        "verified byte-identical) whose isolated inspection shows the "
        "namespace last_sequence advanced past batch one AND every batch-two "
        "point stored under one sequence: atomic-commit proof, so batch two "
        "is preserved through the delta replay and must not be retransmitted.",
        "Inspection showing last_sequence equal to the batch-one receipt "
        "sequence and zero samples above it: atomic-absence proof, so the "
        "client retransmits batch two after the fixed address is restored.",
        "Migration image headers (X-Ridge-Sequence, X-Ridge-SHA256) tying "
        "candidate lineage to the old directory, plus matching namespace "
        "floor/watermark and rollup source_sequence on both sides.",
    ],
    "insufficient_evidence_preserve_scene": (
        "If the candidate file cannot be trusted (checksum mismatch while "
        "copying, bbolt open or decode errors, torn metadata, or inspection "
        "cannot prove batch atomicity): stop writes on both volumes. Leave "
        "the crashed file immutable: record sha256/size/mtime, remove write "
        "permission, keep a byte copy in quarantine and never open the "
        "original with a writer. Keep the old directory untouched. Restore "
        "the fixed address on the OLD directory in read-only mode so reads "
        "continue while producers keep buffering. Reconcile producer logs "
        "against the quarantined file manually. Unreceipted batches are "
        "retransmitted only after reconciliation: ingestion is atomic per "
        "request and last-write-wins makes identical values idempotent, so a "
        "retransmitted batch converges whether or not the original attempt "
        "committed. Do not delete the quarantined file until the "
        "fixed-address service has acknowledged the reconciled state."
    ),
}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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


def launch(binary, database, address, log):
    process = subprocess.Popen(
        [str(binary)], env={**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address},
        stdout=log, stderr=log,
    )
    base = "http://" + address
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(f"service exited before readiness: {address} {database}")
        try:
            request(base, "health")
            return process, base
        except (OSError, RuntimeError):
            time.sleep(0.05)
    process.kill()
    process.wait(timeout=15)
    raise RuntimeError(f"service did not become ready: {address} {database}")


def stop(process):
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=15)
    if process.returncode != 0:
        raise RuntimeError(f"service exit {process.returncode}")


def kill(process):
    """Simulate a candidate process failure: no drain, no clean close."""
    process.kill()
    process.wait(timeout=15)


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def send_without_receipt(address, route, payload):
    """Send a POST and return the open socket without reading any response."""
    host, port = address.rsplit(":", 1)
    body = json.dumps(payload).encode()
    head = (
        f"POST /ridge/v1/{route} HTTP/1.1\r\nHost: {address}\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    sock = socket.create_connection((host, int(port)), timeout=10)
    sock.sendall(head + body)
    return sock


def wait_sequence(base, space, minimum, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        namespace = request(base, f"spaces/{space}")
        if namespace["last_sequence"] >= minimum:
            return namespace
        time.sleep(0.02)
    raise RuntimeError(f"sequence did not reach {minimum}")


def check(checks, name, ok, detail=None):
    entry = {"check": name, "ok": bool(ok)}
    if detail is not None:
        entry["detail"] = detail
    checks.append(entry)
    if not ok:
        raise RuntimeError(f"check failed: {name}: {detail}")


def points_with_sequence(range_result, sequence):
    out = []
    for series in range_result["series"]:
        for point in series["points"]:
            if point["sequence"] == sequence:
                out.append({"labels": series["series"]["labels"], **point})
    return out


def run_scenario(committed, binary, home):
    name = "committed" if committed else "uncommitted"
    record = {"scenario": name, "phases": []}

    def phase(phase_name, **fields):
        entry = {"phase": phase_name, **fields}
        record["phases"].append(entry)
        return entry

    fixed = f"127.0.0.1:{free_port()}"
    oldvol, newvol = home / "oldvol", home / "newvol"
    quarantine, work = home / "quarantine", home / "work"
    for directory in (oldvol, newvol, quarantine, work):
        directory.mkdir()
    old_db = oldvol / "ridgeline.db"
    candidate_db = newvol / "candidate.db"
    phase("topology", fixed_address=fixed, old_volume=str(oldvol), new_volume=str(newvol),
          note="temporary paths; independent database files; verification, "
               "inspection and replay instances bind separate temporary addresses")

    seed = json.loads((ROOT / "examples/input.json").read_text())
    space = "spaces/spindle"
    full = {"start": MIN_TIME, "end": MAX_TIME + 1}
    pulse_a = seed["points"][0]["labels"]   # pulse_total node=ridge-a zone=east
    pulse_b = seed["points"][4]["labels"]   # pulse_total node=ridge-b zone=east
    load_b = seed["points"][8]["labels"]    # load_ratio node=ridge-b zone=west
    load_c = {"__name__": "load_ratio", "node": "ridge-c", "zone": "west"}
    now = 20000
    batch1 = {"now": now, "points": [
        {"labels": pulse_a, "time": 2000, "value": 11},
        {"labels": pulse_a, "time": 2500, "value": 13},
        {"labels": pulse_b, "time": 2000, "value": 17},
    ]}
    batch2 = {"now": now, "points": [
        {"labels": pulse_a, "time": 2000, "value": 110},   # same-time correction
        {"labels": pulse_a, "time": 2500, "value": 130},   # same-time correction
        {"labels": pulse_b, "time": 2500, "value": 19},    # new point
        {"labels": load_c, "time": 2000, "value": 0.4},    # new series + point
    ]}
    batch1_seq, batch2_seq = 9, 10

    logs = {}
    def logfile(tag):
        log = (home / f"{tag}.log").open("wb")
        logs[tag] = log
        return log

    # -- Phase 1: seed the old database on the fixed service address ---------
    checks = []
    process_old, base_old = launch(binary, old_db, fixed, logfile("01-old-seed"))
    request(base_old, "spaces", seed["namespace"])
    receipt = request(base_old, space + "/points", {"now": seed["now"], "points": seed["points"]})
    check(checks, "history ingested", receipt["sequence"] == 2 and receipt["accepted"] == 9, receipt)
    check(checks, "sealed", request(base_old, space + "/seal", {})["sequence"] == 3)
    retention = request(base_old, space + "/retention", {"now": 10500})
    check(checks, "retention floor advanced", retention["floor"] == 500, retention)
    tomb = request(base_old, space + "/deletions", {
        "start": 400, "end": 600,
        "matchers": [{"label": "node", "op": "=", "value": "ridge-a"}]})
    check(checks, "deletion recorded", tomb["sequence"] == 5 and len(tomb["series"]) == 2, tomb)
    receipt = request(base_old, space + "/points",
                      {"now": 10000, "points": [{"labels": pulse_a, "time": 500, "value": 8}]})
    check(checks, "correction after delete admitted", receipt["sequence"] == 6, receipt)
    aggregate_query = {"start": -5000, "end": 10000, "step": 1000, "origin": 0,
                       "group_by": ["zone"], "function": "avg"}
    materialized = request(base_old, space + "/rollups/pulse", aggregate_query, method="PUT")
    check(checks, "rollup materialized at source sequence 6",
          materialized["source_sequence"] == 6, materialized["source_sequence"])
    receipt = request(base_old, space + "/points",
                      {"now": 10000, "points": [{"labels": load_b, "time": 1500, "value": 0.9}]})
    check(checks, "post-rollup write made it stale", receipt["sequence"] == 8, receipt)
    namespace = request(base_old, space)
    check(checks, "seeded state", namespace["last_sequence"] == 8 and
          namespace["floor"] == 500 and namespace["watermark"] == 1500, namespace)
    phase("seed_old_volume", checks=checks)

    # -- Phase 2: consistent copy while the old instance keeps serving reads -
    checks = []
    before = request(base_old, space + "/range", full)
    image, headers = request(base_old, "image")
    digest = hashlib.sha256(image).hexdigest()
    check(checks, "image hash matches header", digest == headers["X-Ridge-Sha256"], digest)
    image_sequence = int(headers["X-Ridge-Sequence"])
    check(checks, "image sequence", image_sequence == 8, image_sequence)
    candidate_db.write_bytes(image)
    after = request(base_old, space + "/range", full)
    check(checks, "old instance served identical reads across the copy", after == before)
    phase("consistent_copy_while_old_serves_reads", checks=checks, write_freeze=True,
          image={"sequence": image_sequence, "sha256": digest, "bytes": len(image)},
          candidate_path=str(candidate_db))

    # -- Phase 3: verify the candidate on a temporary address ----------------
    checks = []
    process_verify, base_verify = launch(binary, candidate_db, f"127.0.0.1:{free_port()}",
                                         logfile("02-candidate-verify"))
    candidate_ns = request(base_verify, space)
    check(checks, "namespace state identical", candidate_ns == request(base_old, space),
          candidate_ns)
    candidate_range = request(base_verify, space + "/range", full)
    check(checks, "samples identical (full range)", candidate_range == request(base_old, space + "/range", full))
    truncated = {"start": MIN_TIME, "end": MAX_TIME + 1, "limit": 2}
    candidate_truncated = request(base_verify, space + "/range", truncated)
    check(checks, "samples identical (truncated range)",
          candidate_truncated == request(base_old, space + "/range", truncated)
          and candidate_truncated["truncated"])
    candidate_deletions = request(base_verify, space + "/deletions")
    check(checks, "deletion inventory identical",
          candidate_deletions == request(base_old, space + "/deletions")
          and len(candidate_deletions) == 1, candidate_deletions)
    hidden_query = {"start": MIN_TIME, "end": MAX_TIME + 1, "matchers": [
        {"label": "__name__", "op": "=", "value": "load_ratio"},
        {"label": "node", "op": "=", "value": "ridge-a"}]}
    check(checks, "deleted points stay hidden on both",
          request(base_verify, space + "/range", hidden_query)["series"] == []
          and request(base_old, space + "/range", hidden_query)["series"] == [])
    pre_floor = {"now": now, "points": [{"labels": pulse_a, "time": 100, "value": 1}]}
    candidate_reject = request(base_verify, space + "/points", pre_floor, status=409)
    check(checks, "retention floor rejects pre-floor writes on both",
          candidate_reject["code"] == "conflict"
          and request(base_old, space + "/points", pre_floor, status=409)["code"] == "conflict",
          candidate_reject)
    candidate_rollup = request(base_verify, space + "/rollups/pulse")
    check(checks, "rollup provenance identical (source sequence, staleness)",
          candidate_rollup == request(base_old, space + "/rollups/pulse")
          and candidate_rollup["source_sequence"] == 6 and candidate_rollup["stale"],
          {"source_sequence": candidate_rollup["source_sequence"], "stale": candidate_rollup["stale"]})
    check(checks, "aggregate results identical",
          request(base_verify, space + "/aggregate", aggregate_query)
          == request(base_old, space + "/aggregate", aggregate_query))
    check(checks, "physical block inventory identical",
          request(base_verify, space + "/blocks") == request(base_old, space + "/blocks"))
    stop(process_verify)
    phase("verify_candidate_on_temporary_address", checks=checks)

    # -- Phase 4: switch the fixed service address to the candidate ----------
    checks = []
    stop(process_old)
    old_hash = sha256_file(old_db)
    process_candidate, base_candidate = launch(binary, candidate_db, fixed,
                                               logfile("03-candidate-fixed"))
    check(checks, "candidate ready on the fixed address",
          request(base_candidate, "health")["state"] == "ready")
    phase("switch_fixed_address_to_candidate", checks=checks,
          old_database_sha256_after_stop=old_hash)

    # -- Phase 5: batch one confirmed, batch two receipt lost, then crash ----
    receipt1 = request(base_candidate, space + "/points", batch1)
    check1 = receipt1["sequence"] == batch1_seq and receipt1["accepted"] == 3
    if not check1:
        raise RuntimeError(f"batch one receipt unexpected: {receipt1}")
    phase("batch_one_confirmed", receipt=receipt1,
          note="client holds this receipt; the write must survive any fallback")

    lost = {"receipt_received": False}
    if committed:
        sock = send_without_receipt(fixed, space + "/points", batch2)
        proof = wait_sequence(base_candidate, "spindle", batch2_seq)
        sock.close()  # the client walks away without ever reading the receipt
        kill(process_candidate)
        lost["client_perspective"] = ("request sent, connection abandoned before any "
                                      "response bytes were read")
        lost["commit_proof_at_crash_time"] = {"last_sequence": proof["last_sequence"]}
    else:
        kill(process_candidate)
        try:
            request(base_candidate, space + "/points", batch2)
            raise RuntimeError("unreachable: the candidate process is dead")
        except OSError as exc:
            lost["client_perspective"] = f"send failed, no receipt: {exc}"
    phase("batch_two_receipt_lost_then_candidate_crash", signal="SIGKILL", **lost)

    # -- Phase 6: isolate the crashed file and inspect it on a scratch copy --
    checks = []
    evidence_hash = sha256_file(candidate_db)
    evidence_size = candidate_db.stat().st_size
    quarantined = quarantine / "candidate-quarantine.db"
    shutil.copyfile(candidate_db, quarantined)
    check(checks, "quarantine copy byte-identical", sha256_file(quarantined) == evidence_hash)
    inspect_db = work / "inspect.db"
    shutil.copyfile(candidate_db, inspect_db)
    process_inspect, base_inspect = launch(binary, inspect_db, f"127.0.0.1:{free_port()}",
                                           logfile("04-isolated-inspection"))
    inspect_ns = request(base_inspect, space)
    inspect_range = request(base_inspect, space + "/range", full)
    # Head keys are (series, time): batch two's same-timestamp corrections
    # replace batch one's entries, so a committed batch two leaves exactly one
    # sequence-9 survivor (the uncorrected point) plus four sequence-10 points.
    batch1_survivors = points_with_sequence(inspect_range, batch1_seq)
    batch2_points = points_with_sequence(inspect_range, batch2_seq)
    if committed:
        check(checks, "batch two committed atomically (all points under one sequence)",
              inspect_ns["last_sequence"] == batch2_seq and len(batch2_points) == 4,
              {"last_sequence": inspect_ns["last_sequence"], "batch_two_points": batch2_points})
        check(checks, "uncorrected batch one point survives; corrections superseded the rest",
              len(batch1_survivors) == 1 and batch1_survivors[0]["value"] == 17,
              batch1_survivors)
        finding = "committed"
    else:
        check(checks, "batch one fully intact",
              len(batch1_survivors) == 3, batch1_survivors)
        check(checks, "batch two atomically absent (no trace above batch one)",
              inspect_ns["last_sequence"] == batch1_seq and len(batch2_points) == 0,
              {"last_sequence": inspect_ns["last_sequence"]})
        finding = "not_committed"
    inspect_rollup = request(base_inspect, space + "/rollups/pulse")
    stop(process_inspect)
    phase("isolated_file_inspection", checks=checks, finding=finding,
          evidence_file={"path": str(candidate_db), "sha256": evidence_hash,
                         "bytes": evidence_size},
          quarantine_file={"path": str(quarantined), "sha256": sha256_file(quarantined)},
          note="the crashed original is never opened by a server; inspection "
               "runs on a disposable working copy, quarantine stays pristine")

    # -- Phase 7: move only the delta back, then restore the fixed address ---
    checks = []
    check(checks, "old directory untouched during candidate episode",
          sha256_file(old_db) == old_hash)
    delta = {}
    for series in inspect_range["series"]:
        for point in series["points"]:
            if point["sequence"] > image_sequence:
                delta.setdefault(point["sequence"], []).append(
                    {"labels": series["series"]["labels"],
                     "time": point["time"], "value": point["value"]})
    expected_sequences = [batch1_seq, batch2_seq] if committed else [batch1_seq]
    check(checks, "delta sequences are exactly the catch-up batches",
          sorted(delta) == expected_sequences, sorted(delta))
    process_replay, base_replay = launch(binary, old_db, f"127.0.0.1:{free_port()}",
                                         logfile("05-replay-old"))
    replayed = []
    for sequence in sorted(delta):
        body = {"now": now, "points": sorted(
            delta[sequence], key=lambda p: (p["time"], json.dumps(p["labels"], sort_keys=True)))}
        receipt = request(base_replay, space + "/points", body)
        check(checks, f"replayed batch at sequence {sequence}",
              receipt["sequence"] == sequence and receipt["accepted"] == len(body["points"]),
              receipt)
        replayed.append({"sequence": receipt["sequence"], "points": len(body["points"]),
                         "request_bytes": len(json.dumps(body).encode())})
    replay_range = request(base_replay, space + "/range", full)
    check(checks, "replayed old database logically equals crashed candidate",
          replay_range["series"] == inspect_range["series"])
    check(checks, "namespace state equals crashed candidate",
          request(base_replay, space) == inspect_ns)
    check(checks, "rollup provenance equals crashed candidate",
          request(base_replay, space + "/rollups/pulse") == inspect_rollup)
    stop(process_replay)
    process_final, base_final = launch(binary, old_db, fixed, logfile("06-restored-fixed"))
    check(checks, "old directory serving the fixed address again",
          request(base_final, "health")["state"] == "ready")
    phase("restore_old_directory_and_fixed_address", checks=checks,
          data_moved_before_fixed_address_restore={
              "method": "delta replay from the quarantined candidate, in sequence order",
              "note": "the delta is extracted from the candidate's final "
                      "committed state, not from client memory: corrected "
                      "points travel only with their winning values, and every "
                      "timestamp batch one wrote remains visible in the "
                      "restored view",
              "full_image_bytes": len(image),
              "delta_batches": replayed,
              "delta_points": sum(b["points"] for b in replayed),
              "delta_request_bytes": sum(b["request_bytes"] for b in replayed),
          })

    # -- Phase 8: verify winning values and whole-batch results --------------
    checks = []
    final_range = request(base_final, space + "/range", full)
    check(checks, "fixed-address view equals crashed candidate",
          final_range["series"] == inspect_range["series"])
    window = request(base_final, space + "/range", {"start": 1500, "end": 3000})
    visible = {}
    for series in window["series"]:
        for point in series["points"]:
            visible[(json.dumps(series["series"]["labels"], sort_keys=True), point["time"])] = point
    key_a = json.dumps(pulse_a, sort_keys=True)
    key_b = json.dumps(pulse_b, sort_keys=True)
    key_c = json.dumps(load_c, sort_keys=True)
    if committed:
        winning = {(key_a, 2000): 110, (key_a, 2500): 130, (key_b, 2000): 17,
                   (key_b, 2500): 19, (key_c, 2000): 0.4}
    else:
        winning = {(key_a, 2000): 11, (key_a, 2500): 13, (key_b, 2000): 17}
    for (key, timestamp), value in winning.items():
        point = visible.get((key, timestamp))
        check(checks, f"winning value at {timestamp} is {value}",
              point is not None and point["value"] == value, point)
    # Batch one's whole-batch result: every timestamp it wrote is visible;
    # where a committed batch-two correction exists, the corrected value wins.
    for key, timestamp in [(key_a, 2000), (key_a, 2500), (key_b, 2000)]:
        check(checks, f"batch one timestamp {timestamp} visible",
              (key, timestamp) in visible)
    present_batch2 = points_with_sequence(final_range, batch2_seq)
    check(checks, "batch two whole-batch result",
          len(present_batch2) == (4 if committed else 0),
          {"points_present": len(present_batch2)})
    final_ns = request(base_final, space)
    check(checks, "final sequence matches finding",
          final_ns["last_sequence"] == (batch2_seq if committed else batch1_seq), final_ns)
    final_rollup = request(base_final, space + "/rollups/pulse")
    check(checks, "rollup provenance preserved through restore",
          final_rollup["source_sequence"] == 6 and final_rollup["stale"])
    phase("final_verification_on_fixed_address", checks=checks,
          winning_values=[{"labels": json.loads(key), "time": timestamp, "value": value}
                          for (key, timestamp), value in sorted(winning.items())])

    terminal_series = final_range["series"]
    if not committed:
        # Evidence proved batch two never landed, so the client retransmits
        # the unreceipted batch; both scenarios then converge.
        receipt2 = request(base_final, space + "/points", batch2)
        checks = []
        check(checks, "retransmitted batch two acknowledged",
              receipt2["sequence"] == batch2_seq and receipt2["accepted"] == 4, receipt2)
        terminal_range = request(base_final, space + "/range", full)
        terminal_series = terminal_range["series"]
        check(checks, "corrections now win",
              len(points_with_sequence(terminal_range, batch2_seq)) == 4)
        phase("resume_collection_retransmission", checks=checks, receipt=receipt2,
              note="safe because ingestion is atomic per request and "
                   "last-write-wins makes identical values idempotent")
    stop(process_final)
    for log in logs.values():
        log.close()
    record["terminal_visible_series"] = terminal_series
    return record


def exercise(output):
    with tempfile.TemporaryDirectory(prefix="ridgeline-migration-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"],
                       cwd=ROOT, check=True)
        scenarios = []
        for committed in (True, False):
            home = scratch / ("committed" if committed else "uncommitted")
            home.mkdir()
            scenarios.append(run_scenario(committed, binary, home))
    converged = scenarios[0]["terminal_visible_series"] == scenarios[1]["terminal_visible_series"]
    record = {
        "rehearsal": "ridgeline volume migration, crash fallback and evidence handling",
        "state": "complete",
        "operational_notes": OPERATIONAL_NOTES,
        "scenarios": scenarios,
        "convergence": {
            "committed_final_equals_uncommitted_after_retransmission": converged,
            "note": "both fallback paths end with identical visible samples, "
                    "winning values and sequences",
        },
    }
    if not converged:
        raise RuntimeError("scenarios did not converge to identical visible state")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({"state": "complete",
                      "scenarios": [s["scenario"] for s in scenarios],
                      "converged": converged, "record": str(output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path,
                        default=ROOT / "var/migration-rehearsal.json")
    args = parser.parse_args()
    exercise(args.output)
