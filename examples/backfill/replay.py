#!/usr/bin/env python3
"""Replayable HTTP examples for docs/backfill-onboarding.md.

Boots a real ridgeline server on a temporary database and replays the
four scenarios of the backfill onboarding manual:

  A  exclusive stream: reconcile after a lost receipt, count from
     read-back instead of "accepted", repair only the missing points
  B  shared stream: a blind resend clobbers a peer correction, so
     unreceipted records are classified and handed back upstream; a
     targeted rewrite happens only after the upstream verdict
  C  deletes and retention: which receipt conclusions stay confirmable
     afterwards (tombstone inventory, re-admission, monotonic floor)
  D  contrast: identical visible values with different commit histories
     prove "readable now" != "this request succeeded"

Every HTTP exchange and client-side decision lands in a transcript
(default examples/backfill/transcript.json). A fresh temporary database
is used per run, so re-running replays the identical conversation.
Set GO=/path/to/go if the toolchain is not on PATH.
"""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
GO = os.environ.get("GO", "go")
MIN_TIME = -(1 << 52)

TRANSCRIPT = []

# ---------------------------------------------------------------------------
# wire helpers


def request(base, route, payload=None, method=None):
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
        return response.status, json.loads(response.read())


def record(entry):
    TRANSCRIPT.append(entry)


def call(base, step, route, payload=None, method=None, expect=200, note=None):
    """One HTTP exchange the collector/operator performs and keeps."""
    status, body = request(base, route, payload, method)
    record({"step": step, "kind": "http", "note": note,
            "request": {"method": method or ("POST" if payload is not None else "GET"),
                        "path": "/ridge/v1/" + route, "body": payload},
            "status": status, "response": body})
    if status != expect:
        raise RuntimeError(f"{step}: {route}: expected {expect}, got {status}: {body}")
    return body


def call_lost(base, step, route, payload, note):
    """A write whose response the client never processes.

    Models a timeout/crash after the server committed: the wire truth is
    recorded, but the collector keeps no receipt and must reconcile.
    """
    status, body = request(base, route, payload, method="POST")
    record({"step": step, "kind": "http-lost", "note": note,
            "request": {"method": "POST", "path": "/ridge/v1/" + route, "body": payload},
            "status": status, "wire_response": body, "client_receipt": None})
    if status != 200:
        raise RuntimeError(f"{step}: unexpected status {status}: {body}")
    return None


def decide(step, note, **fields):
    record({"step": step, "kind": "decision", "note": note, **fields})


def check(condition, message):
    if not condition:
        raise RuntimeError("manual claim violated: " + message)


# ---------------------------------------------------------------------------
# server lifecycle (same pattern as examples/walkthrough.py)


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
            status, _ = request(base, "health")
            if status == 200:
                return process, base
        except OSError:
            pass
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


# ---------------------------------------------------------------------------
# collector reference logic (the manual's rules, executable)


def points_of(result):
    """Flatten a range result to {time: (value, sequence)}; one series expected."""
    check(len(result["series"]) <= 1, "reconcile reads must isolate one series")
    if not result["series"]:
        return {}
    return {p["time"]: (p["value"], p["sequence"])
            for p in result["series"][0]["points"]}


def reconcile_exclusive(expected, observed):
    """Exclusive stream: only this collector writes the series, so a value
    match is its own commit. Returns (confirmed, missing)."""
    confirmed, missing = {}, {}
    for t, value in expected.items():
        if t in observed and observed[t][0] == value:
            confirmed[t] = observed[t]
        else:
            missing[t] = value
    return confirmed, missing


def classify_shared(t, value, observed, my_receipts, tombstones, floor):
    """Shared stream attribution. observed: None or (value, sequence).
    my_receipts: sequences of receipts this collector actually holds."""
    if t < floor:
        return "terminal_floored"
    if observed is None:
        covering = [tb for tb in tombstones if tb["start"] <= t < tb["end"]]
        return "absent_deleted" if covering else "absent_unknown"
    visible_value, visible_seq = observed
    if visible_value == value and visible_seq in my_receipts:
        return "confirmed_yours"
    if visible_value == value:
        return "unattributable"
    return "superseded"


# ---------------------------------------------------------------------------
# scenario fixtures

EA = {"__name__": "pulse_total", "node": "edge-a", "zone": "east"}       # exclusive
EB = {"__name__": "pulse_total", "node": "edge-b", "zone": "east"}       # shared
EBLIND = {"__name__": "pulse_total", "node": "edge-blind", "zone": "east"}
ER = {"__name__": "pulse_total", "node": "edge-r", "zone": "east"}       # retention rig


def ec(hist):
    return {"__name__": "pulse_total", "node": "edge-c", "hist": hist, "zone": "west"}


def match(labels):
    return [{"label": k, "op": "=", "value": v} for k, v in labels.items()
            if k != "__name__" and k != "zone"]


def pt(labels, t, v):
    return {"labels": labels, "time": t, "value": v}


def scenario_a(base, space):
    """Exclusive stream: reconcile and repair, never count 'accepted'."""
    decide("A0", "exclusive stream node=edge-a; collector attempt log kept locally")
    ledger = {"confirmed_increments": 0, "naive_accepted_counter": 0}

    batch1 = [pt(EA, 1000, 1), pt(EA, 2000, 2), pt(EA, 3000, 3)]
    call_lost(base, "A1", space + "/points", {"now": 10000, "points": batch1},
              "batch #1 response lost (timeout after commit): outcome unknown, "
              "collector keeps no receipt and must not count anything")
    ledger["naive_accepted_counter"] += len(batch1)  # old client: resend all, count accepted

    observed = points_of(call(base, "A2", space + "/range",
                              {"start": 1000, "end": 3001, "matchers": match(EA)},
                              note="reconcile read-back over the batch window"))
    confirmed, missing = reconcile_exclusive({1000: 1, 2000: 2, 3000: 3}, observed)
    check(not missing and len(confirmed) == 3, "A2: batch #1 landed, read-back confirms it")
    seq_a1 = observed[1000][1]
    check(all(vs[1] == seq_a1 for vs in confirmed.values()),
          "A2: one commit sequence for the whole batch")
    ledger["confirmed_increments"] += len(confirmed)
    decide("A2", "exclusive rule: value match on an exclusive series is the "
                 "collector's own commit; count from read-back, do NOT resend",
           confirmed=sorted(confirmed), missing=sorted(missing), ledger=dict(ledger))

    before = points_of(call(base, "A3-pre", space + "/range",
                            {"start": 1000, "end": 4001, "matchers": match(EA)}))
    batch2 = [pt(EA, 2000, 2), pt(EA, 4000, 4)]  # mixed: one already committed, one new
    receipt = call(base, "A3", space + "/points", {"now": 10000, "points": batch2},
                   note="mixed batch: t=2000 already committed, t=4000 new")
    check(receipt["accepted"] == 2, "A3: accepted counts every point in the request")
    after = points_of(call(base, "A3-post", space + "/range",
                           {"start": 1000, "end": 4001, "matchers": match(EA)}))
    increments = [t for t in after if t not in before]
    check(increments == [4000], "A3: only t=4000 is a new increment")
    check(after[2000][1] == receipt["sequence"] and after[4000][1] == receipt["sequence"],
          "A3: receipt sequence marks exactly this commit")
    ledger["confirmed_increments"] += len(increments)
    ledger["naive_accepted_counter"] += receipt["accepted"]
    decide("A3", "accepted=2 but read-back diff shows 1 new point; the old "
                 "client's accepted-counter diverges here",
           accepted=receipt["accepted"], real_increments=increments, ledger=dict(ledger))

    dup = [pt(EA, 5000, 5), pt(EA, 5000, 50)]  # same (series,time) twice in one batch
    receipt = call(base, "A4", space + "/points", {"now": 10000, "points": dup},
                   note="intra-batch duplicate timestamp: correction inside one request")
    check(receipt["accepted"] == 2, "A4: accepted counts both duplicates")
    observed = points_of(call(base, "A4-post", space + "/range",
                              {"start": 5000, "end": 5001, "matchers": match(EA)}))
    check(observed == {5000: (50, receipt["sequence"])},
          "A4: last array element wins inside one request")
    ledger["confirmed_increments"] += 1
    ledger["naive_accepted_counter"] += receipt["accepted"]
    decide("A4", "intra-batch duplicate: accepted=2, one visible point (last "
                 "wins); send-side dedup would have made accepted interpretable",
           ledger=dict(ledger))

    pre_seal = points_of(call(base, "A5-pre", space + "/range",
                              {"start": 0, "end": 10000, "matchers": match(EA)}))
    call(base, "A5", space + "/seal", {}, note="maintenance: head -> compressed blocks")
    post_seal = points_of(call(base, "A5-post", space + "/range",
                               {"start": 0, "end": 10000, "matchers": match(EA)}))
    check(pre_seal == post_seal, "A5: seal preserves values and commit sequences")
    decide("A5", "attribution survives seal: per-point sequences unchanged", ledger=dict(ledger))
    return ledger


def scenario_b(base, space):
    """Shared stream: no blind resend; classify and hand back upstream."""
    ledger = {"confirmed_increments": 0, "naive_accepted_counter": 0}

    call_lost(base, "B0a", space + "/points",
              {"now": 10000, "points": [pt(EBLIND, 1000, 20)]},
              "hazard demo on node=edge-blind: batch response lost")
    peer = call(base, "B0b", space + "/points",
                {"now": 10000, "points": [pt(EBLIND, 1000, 99)]},
                note="peer client corrects the same (series,time) meanwhile")
    blind = call(base, "B0c", space + "/points",
                 {"now": 10000, "points": [pt(EBLIND, 1000, 20)]},
                 note="OLD client behavior: blind whole-batch resend after timeout")
    observed = points_of(call(base, "B0d", space + "/range",
                              {"start": 1000, "end": 1001, "matchers": match(EBLIND)}))
    check(observed == {1000: (20, blind["sequence"])} and blind["sequence"] > peer["sequence"],
          "B0: blind resend clobbered the peer's correction (last sequence wins)")
    decide("B0", "the peer's correction 99 is gone, replaced by stale 20; this "
                 "is why shared streams forbid blind resends",
           peer_correction=99, visible_after_blind_resend=observed)

    batch = [pt(EB, 1000, 10), pt(EB, 2000, 20)]
    call_lost(base, "B1", space + "/points", {"now": 10000, "points": batch},
              "shared stream node=edge-b: batch response lost, collector holds no receipt")
    ledger["naive_accepted_counter"] += len(batch)  # old client would resend and count
    peer = call(base, "B2", space + "/points",
                {"now": 10000, "points": [pt(EB, 1000, 10), pt(EB, 2000, 99)]},
                note="peer re-reports 10@1000 identically and corrects 2000 to 99")

    observed = points_of(call(base, "B3", space + "/range",
                              {"start": 1000, "end": 2001, "matchers": match(EB)},
                              note="collector reconciles instead of resending"))
    tombstones = call(base, "B3-evidence", space + "/deletions",
                      note="evidence: no deletion covers the window")
    ns = call(base, "B3-space", space, note="evidence: floor and last_sequence")
    my_receipts = set()  # the B1 receipt was lost
    classifications = {}
    for t, value in ((1000, 10), (2000, 20)):
        classifications[t] = classify_shared(t, value, observed.get(t), my_receipts,
                                             tombstones, ns["floor"])
    check(classifications[1000] == "unattributable",
          "B3: identical value but unknown commit sequence is unattributable")
    check(classifications[2000] == "superseded",
          "B3: different visible value means a peer correction superseded ours")
    handback = {
        "packet_version": 1,
        "space": "backfill",
        "collector": "collector-1",
        "batch_id": "B1",
        "observed_at_sequence": ns["last_sequence"],
        "records": [
            {"labels": EB, "time": 1000, "expected_value": 10,
             "classification": classifications[1000],
             "observed": {"visible": True, "value": observed[1000][0],
                          "sequence": observed[1000][1]},
             "collector_receipts": sorted(my_receipts),
             "evidence": {"tombstones_covering": [], "floor": ns["floor"]}},
            {"labels": EB, "time": 2000, "expected_value": 20,
             "classification": classifications[2000],
             "observed": {"visible": True, "value": observed[2000][0],
                          "sequence": observed[2000][1]},
             "collector_receipts": sorted(my_receipts),
             "evidence": {"tombstones_covering": [], "floor": ns["floor"]}},
        ],
    }
    record({"step": "B4", "kind": "handback",
            "note": "unattributable/superseded records go back to the upstream "
                    "owner; the collector must not resolve them by itself",
            "packet": handback})
    check(ledger["confirmed_increments"] == 0, "B4: nothing counted while unattributed")

    decide("B5", "upstream verdict (authority over both clients): t=1000 the "
                 "peer's identical re-report is the same sample, credit the "
                 "batch upstream, no rewrite; t=2000 authoritative value is 20, "
                 "instruct a targeted rewrite of exactly that point")
    fix = call(base, "B6", space + "/points",
               {"now": 10000, "points": [pt(EB, 2000, 20)]},
               note="targeted rewrite of the single point upstream authorized")
    after = points_of(call(base, "B6-post", space + "/range",
                           {"start": 1000, "end": 2001, "matchers": match(EB)}))
    check(after[2000] == (20, fix["sequence"]), "B6: rewrite confirmed by receipt sequence")
    check(after[1000][1] == peer["sequence"],
          "B6: t=1000 stays the peer's commit; the collector must not count it")
    ledger["confirmed_increments"] += 1
    decide("B6", "collector counts only the rewrite confirmed by its own receipt "
                 "sequence; t=1000 is counted in the upstream ledger, not here",
           ledger=dict(ledger))
    return ledger


def scenario_c(base, space, space_ret):
    """Deletes and retention: which receipt conclusions stay confirmable."""
    tomb = call(base, "C1", space + "/deletions",
                {"start": 1500, "end": 2500, "matchers": match(EA)},
                note="operator deletes (1500,2500)-ish window on the exclusive series")
    inventory = call(base, "C1-inv", space + "/deletions",
                     note="tombstone inventory is queryable evidence")
    check(any(t["id"] == tomb["id"] for t in inventory), "C1: tombstone listed")
    observed = points_of(call(base, "C1-read", space + "/range",
                              {"start": 1000, "end": 4001, "matchers": match(EA)}))
    check(2000 not in observed and observed[1000][0] == 1 and observed[3000][0] == 3,
          "C1: deletion hides committed points with sequence <= tombstone")
    decide("C1", "confirmable now: the tombstone (interval, series, sequence) "
                 "plus the collector's logged receipts prove 'committed, then "
                 "hidden'; the hidden samples themselves are not readable",
           tombstone=tomb)

    repair = call(base, "C2", space + "/points",
                  {"now": 10000, "points": [pt(EA, 2000, 2)]},
                  note="exclusive repair batch: exactly the missing point")
    check(repair["sequence"] > tomb["sequence"], "C2: repair commits after the tombstone")
    observed = points_of(call(base, "C2-read", space + "/range",
                              {"start": 1000, "end": 4001, "matchers": match(EA)}))
    check(observed[2000] == (2, repair["sequence"]),
          "C2: re-admission after a delete works; new sequence is visible")
    decide("C2", "deletion does not advance the floor; re-sending the missing "
                 "point restores it (exclusive stream only)")

    pre = points_of(call(base, "C3-pre", space + "/range",
                         {"start": 0, "end": 10000, "matchers": match(EA)}))
    call(base, "C3", space + "/compact", {},
         note="compaction physically resolves the deletion")
    post = points_of(call(base, "C3-post", space + "/range",
                          {"start": 0, "end": 10000, "matchers": match(EA)}))
    check(pre == post, "C3: compaction preserves visible values and sequences")
    inventory = call(base, "C3-inv", space + "/deletions")
    check(inventory == [], "C3: compaction erases the tombstone inventory")
    decide("C3", "after compact the deletion event itself is gone from the API; "
                 "only the collector's local log still knows it happened")

    # retention rig on a second namespace
    call(base, "C4a", space_ret + "/points",
         {"now": 7000, "points": [pt(ER, 1000, 1), pt(ER, 4500, 4)]},
         note="retention rig: two committed points, receipts logged locally")
    call(base, "C4b", space_ret + "/deletions",
         {"start": 4000, "end": 5000, "matchers": match(ER)},
         note="a deletion whose evidence retention will erase")
    check(len(call(base, "C4b-inv", space_ret + "/deletions")) == 1, "C4b: tombstone listed")
    retention = call(base, "C4c", space_ret + "/retention", {"now": 7000},
                     note="retention_ms=5000 -> floor advances to 2000 and compacts")
    check(retention["floor"] == 2000, "C4c: floor = now - retention_ms")
    ns = call(base, "C4d", space_ret)
    check(ns["floor"] == 2000 and ns["watermark"] == 4500,
          "C4d: floor advanced, watermark (newest accepted timestamp) is kept")
    observed = points_of(call(base, "C4e", space_ret + "/range",
                              {"start": 0, "end": 10000, "matchers": match(ER)}))
    check(observed == {}, "C4e: t=1000 physically reaped, t=4500 deleted; nothing readable")
    check(call(base, "C4f", space_ret + "/deletions") == [],
          "C4f: retention's compaction erased the tombstone inventory too")
    rejection = call(base, "C4g", space_ret + "/points",
                     {"now": 7000, "points": [pt(ER, 1000, 1)]}, expect=409,
                     note="re-sending a floored point is rejected permanently")
    check(rejection == {"code": "conflict", "detail": "point precedes retention floor"},
          "C4g: 409 conflict, terminal for t < floor")
    call(base, "C4h", space_ret + "/points",
         {"now": 7500, "points": [pt(ER, 8000, 8)]},
         note="the namespace still accepts new points at/above the floor")
    decide("C4", "after retention the receipt for t=1000 rests solely on the "
                 "collector's local log; the API can only confirm the terminal "
                 "fact 't < floor: never readable, never writable again' via "
                 "the monotonic floor field")
    return {"floor": ns["floor"]}


def scenario_d(base, space):
    """Same visible value, different commit histories."""
    h1 = call(base, "D1", space + "/points",
              {"now": 10000, "points": [pt(ec("h1"), 500, 10)]},
              note="history 1: collector write, receipt kept")
    call_lost(base, "D2a", space + "/points",
              {"now": 10000, "points": [pt(ec("h2"), 500, 10)]},
              "history 2: collector write, receipt lost ...")
    h2_peer = call(base, "D2b", space + "/points",
                   {"now": 10000, "points": [pt(ec("h2"), 500, 10)]},
                   note="... then a peer writes the identical value")
    h3_mine = call(base, "D3a", space + "/points",
                   {"now": 10000, "points": [pt(ec("h3"), 500, 10)]},
                   note="history 3: collector write, receipt kept ...")
    h3_tomb = call(base, "D3b", space + "/deletions",
                   {"start": 0, "end": 1000, "matchers": match(ec("h3"))},
                   note="... an operator delete hides it ...")
    h3_peer = call(base, "D3c", space + "/points",
                   {"now": 10000, "points": [pt(ec("h3"), 500, 10)]},
                   note="... and a peer re-admits the identical value")

    result = call(base, "D4", space + "/range",
                  {"start": 0, "end": 1000, "matchers": [{"label": "node", "op": "=",
                                                          "value": "edge-c"}]},
                  note="one read, three identical visible values")
    visible = {}
    for series in result["series"]:
        hist = series["series"]["labels"]["hist"]
        check(len(series["points"]) == 1, "D4: one point per history")
        p = series["points"][0]
        check(p["value"] == 10 and p["time"] == 500, "D4: identical visible value")
        visible[hist] = p["sequence"]

    naive = {h: "my write succeeded" for h in visible}  # value matches what was sent
    manual = {
        "h1": "confirmed_yours" if visible["h1"] == h1["sequence"] else "?",
        "h2": "unattributable",   # no receipt held; sequence belongs to someone
        "h3": "not_yours_hidden",  # receipt held, but visible sequence is newer
    }
    check(visible["h1"] == h1["sequence"], "D: h1 visible point is the collector's commit")
    check(visible["h2"] == h2_peer["sequence"], "D: h2 visible point is the peer's commit")
    check(visible["h3"] == h3_peer["sequence"] != h3_mine["sequence"],
          "D: h3 visible point is the peer's re-admission, not the collector's")
    check(h3_mine["sequence"] < h3_tomb["sequence"] < h3_peer["sequence"],
          "D: h3 ordering commit < delete < re-admission")
    inventory = call(base, "D5", space + "/deletions",
                     note="h3's tombstone is still listed (no compact since)")
    check(any(t["id"] == h3_tomb["id"] for t in inventory), "D5: h3 tombstone visible")
    decide("D6", "identical visible values, three different commit histories: "
                 "the naive rule 'readable == my request succeeded' confirms "
                 "all three; the manual confirms only h1, hands h2 back "
                 "upstream (unattributable) and h3 as well (its own commit is "
                 "hidden by a delete, the visible point is a peer's)",
           naive_rule=naive, manual_rule=manual, visible_sequences=visible)
    check(len(set(naive.values())) == 1 and len(set(manual.values())) == 3,
          "D: the manual must not equate 'readable now' with 'this request succeeded'")
    return manual


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path,
                        default=ROOT / "examples/backfill/transcript.json")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="ridgeline-backfill-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run([GO, "build", "-o", str(binary), "./cmd/ridgeline"],
                       cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "backfill.db", log)
            try:
                ns_main = {"name": "backfill", "retention_ms": 3600000,
                           "lateness_ms": 3600000, "future_ms": 60000,
                           "max_series": 100, "max_write": 1000, "max_scan": 100000}
                ns_ret = {**ns_main, "name": "backfill_ret", "retention_ms": 5000}
                call(base, "setup-main", "spaces", ns_main, note="main namespace")
                call(base, "setup-ret", "spaces", ns_ret, note="retention rig namespace")
                space, space_ret = "spaces/backfill", "spaces/backfill_ret"

                ledger_a = scenario_a(base, space)
                ledger_b = scenario_b(base, space)
                scenario_c(base, space, space_ret)
                manual_d = scenario_d(base, space)

                summary = {
                    "exclusive_stream": ledger_a,
                    "shared_stream": ledger_b,
                    "contrast_classification": manual_d,
                    "claims": [
                        "accepted counts request points, not new increments",
                        "lost receipts reconcile by read-back, never by blind resend",
                        "shared-stream conflicts hand back upstream with evidence",
                        "tombstones are evidence only until the next compact/retention",
                        "the retention floor is the only permanent terminal fact",
                        "identical visible values do not imply identical commit history",
                    ],
                }
                record({"step": "summary", "kind": "summary", **summary})
                check(ledger_a["naive_accepted_counter"] > ledger_a["confirmed_increments"],
                      "summary: the old accepted-counter overcounts on the exclusive stream")
            finally:
                stop(process)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(TRANSCRIPT, indent=2) + "\n")
    print(json.dumps({"scenarios": ["A", "B", "C", "D"],
                      "exchanges": sum(1 for e in TRANSCRIPT if e["kind"].startswith("http")),
                      "transcript": str(args.output), "state": "complete"}))


if __name__ == "__main__":
    main()
