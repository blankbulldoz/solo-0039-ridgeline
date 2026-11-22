"""Reproduce order-dependent rejections of one late backfill batch.

Incident: two agents resend the same four backfill points and get different
errors. This script rebuilds the exact situation on a temporary database and
separates the overlapping causes.

Setup per namespace: retention_ms=2000, lateness_ms=1000, future_ms=500.
Seed a@10000 at now=10000 (watermark becomes 10000), then run retention at
now=10000 (floor becomes 8000). The backfill batch at now=10000 is:
b@8500, b@7900, a@10600, c@9500.

Admission (internal/policy/admit.go) checks every point in request order as
floor -> future -> lateness and rejects the whole request atomically on the
first failure, so point order decides which error surfaces first.

Scenarios:
  s1  same batch, three point orders -> three different first errors
  s2  each point alone at now=10000 -> the only passable point is c@9500
  s3  cause isolation -> b@7900 violates floor AND lateness; lateness needs
      a previously accepted write (watermark), the floor needs the retention run
  s4  split resend at now=10100 -> c@9500 must go before a@10600 (or share one
      atomic request); b@8500/b@7900 can never be rescued by waiting

Every 409 is bracketed by before/after snapshots to prove a rejection leaves
no points, no new series and no watermark/floor/sequence change.

Run: python3 examples/backfill_order.py [--output var/backfill_order.json]
Conclusions: examples/backfill_order.md
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

ROOT = Path(__file__).resolve().parents[1]

POLICY = {
    "retention_ms": 2000,
    "lateness_ms": 1000,
    "future_ms": 500,
    "max_series": 100,
    "max_write": 1000,
    "max_scan": 100000,
}

ERR_FLOOR = "point precedes retention floor"
ERR_FUTURE = "point exceeds future skew policy"
ERR_LATE = "point exceeds out-of-order time policy"


def point(node, moment, value):
    return {"labels": {"__name__": "load_ratio", "node": node},
            "time": moment, "value": value}


SEED = point("a", 10000, 1.0)
B_LATE = point("b", 8500, 2.0)     # 1500 ms behind the watermark
B_STALE = point("b", 7900, 3.0)    # below the floor AND 2100 ms behind the watermark
A_FUTURE = point("a", 10600, 4.0)  # 600 ms ahead of now=10000
C_OK = point("c", 9500, 5.0)       # the only point that passes at now=10000


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


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


class Session:
    def __init__(self, base):
        self.base = base
        self.observations = []
        self.findings = []

    def call(self, label, route, payload=None, method=None, status=200):
        out = request(self.base, route, payload, method, status)
        self.observations.append({
            "step": label, "route": route, "expected_status": status,
            "request": payload, "response": out,
        })
        return out

    def finding(self, scenario, detail):
        self.findings.append({"scenario": scenario, "finding": detail})
        print(f"[{scenario}] {detail}")

    def make_space(self, name, seed=True, retain=True):
        self.call(f"{name}: create", "spaces", {"name": name, **POLICY})
        if seed:
            receipt = self.call(f"{name}: seed a@10000 now=10000",
                                f"spaces/{name}/points", {"now": 10000, "points": [SEED]})
            check(receipt["watermark"] == 10000, f"{name}: seed receipt {receipt}")
        if retain:
            done = self.call(f"{name}: retention now=10000",
                             f"spaces/{name}/retention", {"now": 10000})
            check(done["floor"] == 8000, f"{name}: retention receipt {done}")

    def snapshot(self, name):
        ns = self.call(f"{name}: inspect", f"spaces/{name}")
        series = self.call(f"{name}: series", f"spaces/{name}/series", {"matchers": []})
        raw = self.call(f"{name}: range", f"spaces/{name}/range",
                        {"start": -1000, "end": 20000})
        return {
            "floor": ns["floor"],
            "watermark": ns["watermark"],
            "has_watermark": ns["has_watermark"],
            "last_sequence": ns["last_sequence"],
            "series": sorted(s["labels"]["node"] for s in series),
            "points": {s["series"]["labels"]["node"]: [p["time"] for p in s["points"]]
                       for s in raw["series"]},
            "sequence": raw["sequence"],
        }

    def rejected(self, name, label, now, points, detail):
        before = self.snapshot(name)
        err = self.call(label, f"spaces/{name}/points",
                        {"now": now, "points": points}, status=409)
        check(err["code"] == "conflict", f"{label}: unexpected body {err}")
        check(err["detail"] == detail, f"{label}: {err['detail']!r} != {detail!r}")
        after = self.snapshot(name)
        check(after == before,
              f"{label}: rejection leaked state\nbefore={before}\nafter={after}")
        return err

    def accepted(self, name, label, now, points, accepted, new_series, watermark):
        receipt = self.call(label, f"spaces/{name}/points",
                            {"now": now, "points": points})
        check(receipt["accepted"] == accepted
              and receipt["new_series"] == new_series
              and receipt["watermark"] == watermark,
              f"{label}: unexpected receipt {receipt}")
        return receipt


def scenario_agent_orders(s):
    """Same four points, three submission orders, three different first errors."""
    orders = [
        ("s1-agent-a", [B_LATE, B_STALE, A_FUTURE, C_OK], ERR_LATE),
        ("s1-agent-b", [A_FUTURE, C_OK, B_LATE, B_STALE], ERR_FUTURE),
        ("s1-agent-c", [B_STALE, A_FUTURE, C_OK, B_LATE], ERR_FLOOR),
    ]
    for name, points, detail in orders:
        s.make_space(name)
        order = [p["time"] for p in points]
        s.rejected(name, f"{name}: batch {order} now=10000", 10000, points, detail)
        s.finding(name, f"batch order {order} -> first error {detail!r}; "
                        "whole batch dropped atomically, state unchanged")


def scenario_solo(s):
    """Each backfill point alone at now=10000 (watermark=10000, floor=8000)."""
    cases = [
        ("s2-b-late", B_LATE, ERR_LATE),
        ("s2-b-stale", B_STALE, ERR_FLOOR),
        ("s2-a-future", A_FUTURE, ERR_FUTURE),
    ]
    for name, p, detail in cases:
        s.make_space(name)
        s.rejected(name, f"{name}: solo t={p['time']} now=10000", 10000, [p], detail)
        s.finding(name, f"solo t={p['time']} -> {detail!r}")
    s.make_space("s2-c-ok")
    s.accepted("s2-c-ok", "s2-c-ok: solo t=9500 now=10000", 10000, [C_OK],
               accepted=1, new_series=1, watermark=10000)
    snap = s.snapshot("s2-c-ok")
    check(snap["points"] == {"a": [10000], "c": [9500]}, f"s2-c-ok: {snap}")
    s.finding("s2-c-ok", "solo t=9500 -> accepted; the batch error hid the one valid point")


def scenario_overlap(s):
    """Isolate the two causes that overlap on b@7900 and the watermark dependency."""
    s.make_space("s3-no-retention", seed=True, retain=False)
    s.rejected("s3-no-retention", "s3-no-retention: b@7900 without floor", 10000,
               [B_STALE], ERR_LATE)
    s.finding("s3-no-retention", "without the retention run b@7900 fails lateness instead: "
                                 "the floor error had masked a second, independent cause")

    s.make_space("s3-no-watermark", seed=False, retain=True)
    s.rejected("s3-no-watermark", "s3-no-watermark: b@7900 without watermark", 10000,
               [B_STALE], ERR_FLOOR)
    s.finding("s3-no-watermark", "b@7900 still fails the floor with no watermark: "
                                 "the floor does not depend on accepted writes")
    s.accepted("s3-no-watermark", "s3-no-watermark: b@8500 without watermark", 10000,
               [B_LATE], accepted=1, new_series=1, watermark=8500)
    s.finding("s3-no-watermark", "b@8500 is accepted with no watermark: lateness exists "
                                 "only because a@10000 was accepted earlier")


def scenario_resend(s):
    """The planned split resend once the caller clock reaches 10100."""
    s.make_space("s4-a-first")
    s.accepted("s4-a-first", "s4-a-first: a@10600 now=10100", 10100, [A_FUTURE],
               accepted=1, new_series=0, watermark=10600)
    s.rejected("s4-a-first", "s4-a-first: c@9500 after a@10600", 10100, [C_OK], ERR_LATE)
    s.finding("s4-a-first", "a@10600 accepted first raises the watermark to 10600; "
                            "c@9500 (1100 ms late) now fails although it passed solo")

    s.make_space("s4-c-first")
    s.accepted("s4-c-first", "s4-c-first: c@9500 now=10100", 10100, [C_OK],
               accepted=1, new_series=1, watermark=10000)
    s.accepted("s4-c-first", "s4-c-first: a@10600 after c@9500", 10100, [A_FUTURE],
               accepted=1, new_series=0, watermark=10600)
    snap = s.snapshot("s4-c-first")
    check(snap["points"] == {"a": [10000, 10600], "c": [9500]}, f"s4-c-first: {snap}")
    s.finding("s4-c-first", "c@9500 before a@10600 -> both accepted")

    s.make_space("s4-combined")
    s.accepted("s4-combined", "s4-combined: [a@10600, c@9500] one request now=10100",
               10100, [A_FUTURE, C_OK], accepted=2, new_series=1, watermark=10600)
    s.finding("s4-combined", "one atomic request is judged against the pre-request "
                             "watermark, so intra-request order cannot trip lateness")

    s.make_space("s4-b-points")
    for now in (10100, 20000):
        s.rejected("s4-b-points", f"s4-b-points: b@8500 now={now}", now, [B_LATE], ERR_LATE)
        s.rejected("s4-b-points", f"s4-b-points: b@7900 now={now}", now, [B_STALE], ERR_FLOOR)
    s.finding("s4-b-points", "b@8500/b@7900 stay rejected at now=10100 and now=20000: "
                             "lateness is measured against the watermark and the floor "
                             "only moves forward, so waiting never rescues them")


def exercise(output):
    with tempfile.TemporaryDirectory(prefix="ridgeline-backfill-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"],
                       cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "backfill.db", log)
            try:
                session = Session(base)
                scenario_agent_orders(session)
                scenario_solo(session)
                scenario_overlap(session)
                scenario_resend(session)
                observations, findings = session.observations, session.findings
            finally:
                stop(process)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"findings": findings, "operations": observations},
                                     indent=2) + "\n")
    print(json.dumps({"scenario": "backfill-order", "operations": len(observations),
                      "findings": len(findings), "state": "complete"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    exercise(args.output)
