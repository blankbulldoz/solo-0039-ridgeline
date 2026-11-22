#!/usr/bin/env python3
"""Replay the seeded trajectory against a real ridgeline server and collect
deployment evidence.

Builds ./cmd/ridgeline into a temp dir, launches it on an ephemeral port with
a temp database, then:

  1. creates every namespace from trajectory.spaces()
  2. replays the seeded interleaved batches (sealing at SEAL_MINUTE so the
     correction job lands on sealed blocks, the production case)
  3. checks corrected values, duplicate-merge query cost before/after compact
  4. probes the admission boundaries (lateness / future / maintenance budget)
  5. advances retention and records final namespace state

Usage: python3 deployment/verify.py [--seed 42] [--devices 20] [--minutes 95]
                                    [--out deployment/out/evidence.json]
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import trajectory  # noqa: E402

T0 = trajectory.T0


def find_go() -> str:
    for cand in (os.environ.get("GO"), shutil.which("go"), "/tmp/go/bin/go"):
        if cand and Path(cand).exists():
            return cand
    raise RuntimeError("go toolchain not found (set GO=/path/to/go)")


class Client:
    def __init__(self, address: str):
        self.address = address
        self.reset()

    def reset(self) -> None:
        host, port = self.address.split(":")
        self.conn = http.client.HTTPConnection(host, int(port), timeout=60)

    def call(self, method: str, route: str, payload=None, expect: int = 200):
        body = None if payload is None else json.dumps(payload)
        self.conn.request(method, "/ridge/v1/" + route, body=body,
                          headers={"Content-Type": "application/json"})
        resp = self.conn.getresponse()
        data = resp.read()
        if resp.status != expect:
            raise RuntimeError(
                f"{method} {route}: want {expect}, got {resp.status}: {data[:300]!r}")
        return json.loads(data) if data else {}


def launch(binary: Path, database: Path, log: Path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{sock.getsockname()[1]}"
    env = {**os.environ, "RIDGE_DB": str(database), "RIDGE_ADDR": address}
    process = subprocess.Popen([str(binary)], env=env,
                               stdout=log.open("wb"), stderr=subprocess.STDOUT)
    client = Client(address)
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError("service exited before readiness")
        try:
            client.call("GET", "health")
            return process, client
        except (OSError, http.client.HTTPException):
            client.reset()
            time.sleep(0.05)
    process.terminate()
    raise RuntimeError("service did not become ready")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--devices", type=int, default=20)
    ap.add_argument("--minutes", type=int, default=95)
    ap.add_argument("--out", type=Path,
                    default=ROOT / "deployment/out/evidence.json")
    args = ap.parse_args()

    ev: dict = {"seed": args.seed, "devices": args.devices,
                "minutes": args.minutes, "checks": []}

    def check(name: str, ok: bool, detail) -> None:
        ev["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
        print(f"[{'ok' if ok else 'FAIL'}] {name}: {json.dumps(detail)[:180]}")

    with tempfile.TemporaryDirectory(prefix="ridgeline-deploy-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        env = {**os.environ, "CGO_ENABLED": "0"}
        env.setdefault("GOCACHE", "/tmp/gocache")
        subprocess.run([find_go(), "build", "-o", str(binary), "./cmd/ridgeline"],
                       cwd=ROOT, env=env, check=True)
        process, c = launch(binary, scratch / "ridge.db", scratch / "service.log")
        try:
            t_start = time.time()
            # --- 1. namespaces ------------------------------------------------
            for ns in trajectory.spaces(args.devices):
                c.call("POST", "spaces", ns)
            got = c.call("GET", "spaces/edge_slow")
            check("namespaces created", got["lateness_ms"] == 3_900_500
                  and got["has_watermark"] is False,
                  {"edge_slow": {k: got[k] for k in
                                 ("lateness_ms", "future_ms", "max_write",
                                  "max_scan", "has_watermark")}})
            dup = c.call("POST", "spaces", trajectory.spaces(args.devices)[-1],
                         expect=409)
            check("duplicate namespace rejected", dup["code"] == "conflict", dup)

            # --- 2. replay the interleaved trajectory -------------------------
            batches = trajectory.gen_batches(args.seed, args.devices, args.minutes)
            seal_at = T0 + trajectory.SEAL_MINUTE * 60_000
            sealed = False
            watermarks: dict[str, int] = {}
            monotonic = True
            points = 0
            for b in batches:
                if not sealed and b["now"] >= seal_at:
                    for name in [s["name"] for s in trajectory.spaces(args.devices)]:
                        c.call("POST", f"spaces/{name}/seal", {})
                    sealed = True
                r = c.call("POST", f"spaces/{b['space']}/points",
                           {"now": b["now"], "points": b["points"]})
                points += r["accepted"]
                prev = watermarks.get(b["space"])
                if prev is not None and r["watermark"] < prev:
                    monotonic = False
                watermarks[b["space"]] = r["watermark"]
            ev["replay"] = {"batches": len(batches), "points": points,
                            "sealed_at_minute": trajectory.SEAL_MINUTE,
                            "wall_seconds": round(time.time() - t_start, 2)}
            check("all batches admitted", points == sum(len(b["points"]) for b in batches),
                  ev["replay"])
            check("shared watermark monotonic per namespace", monotonic,
                  {k: v - T0 for k, v in sorted(watermarks.items())})

            # --- 3. discovery + cross-source identity -------------------------
            fast0 = c.call("POST", "spaces/edge_fast_0/series",
                           {"matchers": [{"label": "device", "op": "=", "value": "d-002"}]})
            slow = c.call("POST", "spaces/edge_slow/series",
                          {"matchers": [{"label": "device", "op": "=", "value": "d-002"}]})
            check("same device, two namespaces, disjoint series",
                  len(fast0) == 4 and len(slow) == 10
                  and {s["id"] for s in fast0}.isdisjoint({s["id"] for s in slow}),
                  {"edge_fast_0": len(fast0), "edge_slow": len(slow),
                   "note": "device label is the only join key; two HTTP calls, "
                           "client-side join, no cross-namespace operator"})

            # --- 4. query cost of corrections, before and after compact -------
            win = {"start": T0 + 10 * 60_000, "end": T0 + 70 * 60_000,
                   "matchers": [{"label": "device", "op": "=", "value": "d-002"}]}
            before = c.call("POST", "spaces/edge_slow/range", win)
            plan_before = c.call("POST", "spaces/edge_slow/explain", win)
            check("correction duplicates inflate decoded points",
                  before["stats"]["decoded"] > before["stats"]["visible"] == 600,
                  {"pre_compact": before["stats"],
                   "explain": {"candidates": len(plan_before["candidates"]),
                               "blocks": len(plan_before["blocks"]),
                               "source_point_hint": plan_before["source_point_hint"],
                               "head_records": plan_before["head_records"]}})

            comp = c.call("POST", "spaces/edge_slow/compact", {})
            after = c.call("POST", "spaces/edge_slow/range", win)
            plan_after = c.call("POST", "spaces/edge_slow/explain", win)
            # decoded is block-granular: one compacted block per series holds
            # all 95 live points, so the window decodes 10 * 95 = 950 with no
            # duplicate work left (pre-compact: 1250).
            check("compact resolves duplicates, one block per series",
                  comp["blocks"] == 10 * args.devices
                  and after["stats"]["head"] == 0
                  and after["stats"]["decoded"] == 10 * args.minutes
                  and after["stats"]["visible"] == 600,
                  {"compact": comp, "post_compact": after["stats"],
                   "explain": {"blocks": len(plan_after["blocks"]),
                               "head_records": plan_after["head_records"]}})

            # corrected value wins: minute 30 was backlog-flushed, then fixed
            want = trajectory.corrected(
                trajectory.value(10, 2, T0 + 30 * 60_000))
            got_pt = [p for s in after["series"]
                      if s["series"]["labels"]["__name__"] == "temp_c"
                      for p in s["points"] if p["time"] == T0 + 30 * 60_000]
            check("corrected value visible (sequence wins)",
                  len(got_pt) == 1 and got_pt[0]["value"] == want,
                  {"time_offset_min": 30, "value": got_pt[0]["value"] if got_pt else None,
                   "expected": want})

            # --- 5. admission boundary probes ---------------------------------
            wf = c.call("GET", "spaces/edge_fast_0")["watermark"]
            probe = {"labels": {"__name__": "probe_rt", "device": "d-000",
                                "site": "site-a"}, "value": 1}
            p1 = c.call("POST", "spaces/edge_fast_0/points",
                        {"now": wf, "points": [{**probe, "time": wf - 2_501}]},
                        expect=409)
            check("fast: 2501ms late rejected", p1["code"] == "conflict", p1)
            r = c.call("POST", "spaces/edge_fast_0/points",
                       {"now": wf, "points": [{**probe, "time": wf - 2_500}]})
            check("fast: exactly 2500ms late admitted", r["accepted"] == 1,
                  {"watermark": r["watermark"] - T0})
            p3 = c.call("POST", "spaces/edge_fast_0/points",
                        {"now": wf, "points": [{**probe, "time": wf - 1_800_000}]},
                        expect=409)
            check("fast: correction-age point (30min) rejected", p3["code"] == "conflict",
                  {"note": "corrections cannot live in the fast tier", **p3})
            p4 = c.call("POST", "spaces/edge_fast_0/points",
                        {"now": wf, "points": [{**probe, "time": wf + 600}]},
                        expect=409)
            check("future: 600ms ahead of now rejected", p4["code"] == "conflict", p4)
            r = c.call("POST", "spaces/edge_fast_0/points",
                       {"now": wf, "points": [{**probe, "time": wf + 400}]})
            check("future: 400ms fast client clock admitted", r["accepted"] == 1,
                  {"watermark": r["watermark"] - T0})

            ws = c.call("GET", "spaces/edge_slow")["watermark"]
            fix_pt = {"labels": {"__name__": "temp_c", "device": "d-002",
                                 "site": "site-a"},
                      "time": ws - 1_800_000, "value": -999.0}
            r = c.call("POST", "spaces/edge_slow/points",
                       {"now": ws, "points": [fix_pt]})
            check("slow: 30min-old correction admitted", r["accepted"] == 1,
                  {"watermark": r["watermark"] - T0})
            p7 = c.call("POST", "spaces/edge_slow/points",
                        {"now": ws, "points": [{**probe, "time": ws - 3_900_501}]},
                        expect=409)
            check("slow: beyond 3_900_500ms budget rejected (lateness is finite)",
                  p7["code"] == "conflict", p7)
            rng = c.call("POST", "spaces/edge_slow/range",
                         {"start": ws - 1_800_000, "end": ws - 1_800_000 + 1,
                          "matchers": [{"label": "device", "op": "=", "value": "d-002"},
                                       {"label": "__name__", "op": "=", "value": "temp_c"}]})
            got_fix = [p for s in rng["series"] for p in s["points"]]
            check("latest correction overrides the daily fix",
                  len(got_fix) == 1 and got_fix[0]["value"] == -999.0,
                  {"points": got_fix})

            # --- 6. maintenance budget lockout --------------------------------
            c.call("POST", "spaces", {"name": "probe_maint", "retention_ms": 86_400_000,
                                      "lateness_ms": 3_900_500, "future_ms": 500,
                                      "max_series": 10, "max_write": 100,
                                      "max_scan": 1_000})
            empty = c.call("GET", "spaces/probe_maint")
            for j in range(12):  # 10 series x 120 timestamps = 1200 live points
                c.call("POST", "spaces/probe_maint/points", {
                    "now": T0 + j * 10 + 9,
                    "points": [{"labels": {"__name__": "pm", "device": f"d-p{i}",
                                           "site": "site-a"},
                                "time": T0 + j * 10 + k, "value": 1}
                               for i in range(10) for k in range(10)]})
            m1 = c.call("POST", "spaces/probe_maint/retention", {"now": T0 + 200},
                        expect=422)
            m2 = c.call("POST", "spaces/probe_maint/compact", {}, expect=422)
            w = c.call("POST", "spaces/probe_maint/points",
                       {"now": T0 + 300, "points": [{"labels": {"__name__": "pm",
                        "device": "d-p0", "site": "site-a"}, "time": T0 + 300, "value": 2}]})
            check("maintenance locked when live points exceed max_scan",
                  m1["code"] == m2["code"] == "capacity" and w["accepted"] == 1,
                  {"retention": m1, "compact": m2,
                   "empty_namespace_had_watermark": empty["has_watermark"],
                   "note": "1200 live points > max_scan 1000; writes still work, "
                           "maintenance does not -> sizing is a hard invariant"})

            # --- 7. retention --------------------------------------------------
            rets = {}
            for name in ("edge_fast_0", "edge_slow"):
                rets[name] = c.call("POST", f"spaces/{name}/retention",
                                    {"now": T0 + 96 * 60_000})
            check("retention advances within budget",
                  all(r["sequence"] > 0 for r in rets.values()), rets)

            ev["final"] = {n: c.call("GET", f"spaces/{n}")
                           for n in ("edge_fast_0", "edge_slow")}
        finally:
            process.terminate()
            process.wait(timeout=15)

    ev["ok"] = all(ch["ok"] for ch in ev["checks"])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(ev, indent=2))
    print(json.dumps({"ok": ev["ok"], "checks": len(ev["checks"]),
                      "evidence": str(args.out)}))
    sys.exit(0 if ev["ok"] else 1)


if __name__ == "__main__":
    main()
