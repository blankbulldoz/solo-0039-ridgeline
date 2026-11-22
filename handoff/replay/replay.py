#!/usr/bin/env python3
"""Replay two submission schedules for the two-collector counter handoff.

Runs the real Ridgeline HTTP service against throwaway database files (one
fresh space per scenario) and cross-checks every visible point/count/rate
against an independent expectation model derived from PROJECT_SPEC.md.

Usage:  python3 replay.py --binary /tmp/ridgeline-work/bin/ridgeline --out result.json
"""

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

LABELS = {"__name__": "device_counter_total", "device": "device01"}
NOW = 1700
RETENTION_MS = 600          # now 1000 -> floor 400, matching the committed space
LATENESS_MS = 700
FUTURE_MS = 100
SEED_TIME = 1000            # commits watermark 1000
A_OFFSET = 400              # a local clock is 400 ms fast (confirmed)
RATE_STEP = 2000
RATE_ORIGIN = 0

# Representative feasible offsets for b: local - true in [-300, 300]
# Includes both region boundaries (-101/-100 and +100/+101) for floor and
# future-skew half-open checks.
OFFSETS = [-300, -101, -100, 0, 100, 101, 300]


def request(base, route, payload=None, method=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route, data=body,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as exc:
        with exc:
            return {"_http_status": exc.code, "_error": json.loads(exc.read().decode())}
    with response:
        return json.loads(response.read().decode())


def wait_ready(process, base, log_path):
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError(f"service exited; see {log_path}")
        try:
            request(base, "health")
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("service did not become ready")


def launch(binary, db_path, log_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = f"127.0.0.1:{port}"
    log = open(log_path, "wb")
    process = subprocess.Popen(
        [str(binary)],
        env={**os.environ, "RIDGE_DB": str(db_path), "RIDGE_ADDR": address},
        stdout=log, stderr=log,
    )
    base = "http://" + address
    wait_ready(process, base, log_path)
    return process, base


def stop(process):
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=15)


def point(time, value):
    return {"labels": LABELS, "time": time, "value": value}


def admit_state(time, now, wm, floor=400):
    """Stateful copy of policy.Admit: watermark advances as points commit."""
    if time < floor:
        return False, "point precedes retention floor"
    if time - now > FUTURE_MS:
        return False, "point exceeds future skew policy"
    if wm - time > LATENESS_MS:
        return False, "point exceeds out-of-order time policy"
    return True, "ok"


def _rate_from(visible):
    times = sorted(visible)
    increase = 0.0
    for i in range(1, len(times)):
        d = visible[times[i]] - visible[times[i - 1]]
        increase += d if d >= 0 else visible[times[i]]
    rate = increase / ((times[-1] - times[0]) / 1000.0) if len(times) >= 2 else None
    return rate


def _simulate(commits, delta, split_retry=False):
    """commits: ordered list of (entries, atomic). Tracks watermark and the
    most-recent-wins value map exactly as the engine does."""
    a_points = [(1000 - A_OFFSET, 2, "a1"), (2000 - A_OFFSET, 4, "a2")]
    b_points = [(500 - delta, 3, "b1"), (1500 - delta, 5, "b2")]
    wm, floor = 1000, 400
    visible, verdicts = {}, {}

    def commit(entries, atomic):
        nonlocal wm
        checked = [(t, v, name, *admit_state(t, NOW, wm, floor))
                   for t, v, name in entries]
        if atomic and any(not ok for *_, ok, _detail in checked):
            for t, v, name, ok, detail in checked:
                verdicts[name] = {"admitted": False, "detail": detail,
                                  "reason": "whole atomic request rejected"}
            return
        for t, v, name, ok, detail in checked:
            if ok:
                visible[t] = v
                wm = max(wm, t)
                verdicts[name] = {"admitted": True, "detail": detail}
            else:
                verdicts[name] = {"admitted": False, "detail": detail}

    if commits == "A":
        commit(a_points, atomic=True)              # watermark 1000 -> 1600
        commit(b_points, atomic=True)
        if split_retry:                            # salvage per point
            for entry in b_points:
                commit([entry], atomic=True)
    else:
        commit(a_points + b_points, atomic=True)   # evaluated at watermark 1000
    return {"visible": [[t, visible[t]] for t in sorted(visible)],
            "visible_count": len(visible),
            "rate": _rate_from(visible), "verdicts": verdicts}


def expected_state(plan, delta):
    return _simulate(plan, delta)


def expected_split_retry(delta):
    return _simulate("A", delta, split_retry=True)


def query_state(base, space):
    rng = request(base, f"spaces/{space}/range",
                  {"start": -100, "end": 2200,
                   "matchers": [{"label": "__name__", "op": "=",
                                 "value": "device_counter_total"}]})
    rate = request(base, f"spaces/{space}/aggregate",
                   {"start": 0, "end": RATE_STEP, "step": RATE_STEP,
                    "origin": RATE_ORIGIN, "group_by": [], "function": "rate",
                    "matchers": [{"label": "__name__", "op": "=",
                                  "value": "device_counter_total"}]})
    return rng, rate


def seed_space(base, space):
    ns = {"name": space, "retention_ms": RETENTION_MS,
          "lateness_ms": LATENESS_MS, "future_ms": FUTURE_MS,
          "max_series": 10, "max_write": 1000, "max_scan": 100000}
    create = request(base, "spaces", ns)
    # Commit the pre-existing watermark 1000 with an unrelated series value,
    # then advance retention: floor = 1000 - 600 = 400.
    seed = request(base, f"spaces/{space}/points",
                   {"now": SEED_TIME,
                    "points": [{"labels": {"__name__": "seed_marker",
                                           "device": "device01"},
                                "time": SEED_TIME, "value": 1}]})
    retain = request(base, f"spaces/{space}/retention", {"now": SEED_TIME})
    inspect = request(base, f"spaces/{space}")
    assert inspect["watermark"] == 1000, inspect
    assert inspect["floor"] == 400, inspect
    return {"create": create, "seed": seed, "retention": retain, "inspect": inspect}


def run_scenario(binary, scratch, plan, delta, split_retry=False, run_id=0):
    space = f"dev_{plan.lower()}_{str(delta).replace('-', 'm')}_{run_id}"
    db_path = scratch / f"{space}.db"
    process, base = launch(binary, db_path, scratch / f"{space}.log")
    trace = []
    try:
        trace.append({"step": "seed", "result": seed_space(base, space)})
        a_req = {"now": NOW, "points": [point(600, 2), point(1600, 4)]}
        b_req = {"now": NOW,
                 "points": [point(500 - delta, 3), point(1500 - delta, 5)]}

        if plan == "A":
            trace.append({"step": "planA_submit_a", "request": a_req,
                          "result": request(base, f"spaces/{space}/points", a_req)})
            trace.append({"step": "planA_bundle_b_two_points", "request": b_req,
                          "result": request(base, f"spaces/{space}/points", b_req)})
            if split_retry:
                # After the two-point request fails, retry each point alone so
                # the still-admissible one can land (per-point isolation).
                for idx, name in ((0, "b1"), (1, "b2")):
                    single = {"now": NOW, "points": [b_req["points"][idx]]}
                    trace.append({"step": f"planA_split_retry_{name}",
                                  "request": single,
                                  "result": request(base, f"spaces/{space}/points", single)})
        else:
            bundle = {"now": NOW, "points": a_req["points"] + b_req["points"]}
            trace.append({"step": "planB_bundle_a_and_b_guessed_times",
                          "request": bundle,
                          "result": request(base, f"spaces/{space}/points", bundle)})

        rng, rate = query_state(base, space)
        visible = [[p["time"], p["value"]]
                   for s in rng.get("series", []) for p in s["points"]
                   if s["series"]["labels"] == LABELS]
        cells = rate.get("cells", [])
        actual = {"visible": visible, "visible_count": len(visible),
                  "rate": cells[0]["value"] if cells else None,
                  "rate_count": cells[0]["count"] if cells else 0,
                  "watermark": request(base, f"spaces/{space}")["watermark"]}
        expected = (expected_split_retry(delta) if split_retry
                    else expected_state(plan, delta))
        match = (actual["visible"] == expected["visible"]
                 and actual["visible_count"] == expected["visible_count"]
                 and (actual["rate"] or None) == (expected["rate"] or None))
        return {"space": space, "plan": plan, "delta": delta,
                "split_retry": split_retry, "trace": trace,
                "actual": actual, "expected": expected, "match": match}
    finally:
        stop(process)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    results = []
    run_id = 0
    with tempfile.TemporaryDirectory(prefix="ridgeline-replay-") as d:
        scratch = Path(d)
        for delta in OFFSETS:
            run_id += 1
            results.append(run_scenario(args.binary, scratch, "A", delta, run_id=run_id))
        # Boundary offsets that leave one b point dead: show split retry that
        # salvages the other point instead of losing the whole batch.
        run_id += 1
        results.append(run_scenario(args.binary, scratch, "A", 300,
                                    split_retry=True, run_id=run_id))
        run_id += 1
        results.append(run_scenario(args.binary, scratch, "A", -300,
                                    split_retry=True, run_id=run_id))
        for delta in OFFSETS:
            run_id += 1
            results.append(run_scenario(args.binary, scratch, "B", delta, run_id=run_id))
    summary = [{"plan": r["plan"], "delta": r["delta"],
                "split_retry": r["split_retry"],
                "visible": r["actual"]["visible"],
                "visible_count": r["actual"]["visible_count"],
                "rate": r["actual"]["rate"],
                "expected_visible": r["expected"]["visible"],
                "expected_rate": r["expected"]["rate"],
                "verdicts": r["expected"]["verdicts"],
                "match": r["match"]} for r in results]
    out = {"parameters": {"now": NOW, "retention_ms": RETENTION_MS,
                          "floor": 400, "watermark": 1000,
                          "lateness_ms": LATENESS_MS, "future_ms": FUTURE_MS,
                          "a_true_times": [600, 1600],
                          "b_true_time_bounds": {"b1": [200, 800], "b2": [1200, 1800]}},
           "summary": summary,
           "all_match": all(r["match"] for r in results),
           "scenarios": results}
    Path(args.out).write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps({"scenarios": len(results),
                      "all_match": out["all_match"]}, indent=2))


if __name__ == "__main__":
    main()
