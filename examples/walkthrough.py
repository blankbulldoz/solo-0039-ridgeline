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
    "delete", "retention", "retention_preview", "materialize", "freshness",
    "compact", "backup", "explain"
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


def retention_preview_demo(output):
    """Exercise preview/confirm retention across two active namespaces.

    Scenario goals:
      * a preview reports the planned floor, affected visible points, series
        and rollup materializations without changing the database;
      * a point admitted during the preview window invalidates the token, and
        the operator must explicitly choose recompute or continue;
      * a regressing observation time never lowers the monotonic floor;
      * a scan-budget rejection in one namespace leaves the other untouched.
    """
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-retention-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "retention.db", log)
            try:
                def op(label, route, payload=None, **options):
                    out = request(base, route, payload, **options)
                    observations.append({"step": label, "request": payload, "response": out})
                    return out

                def fail(label, route, payload, expected_code, **options):
                    status = {"conflict": 409, "invalid": 400, "capacity": 422}[expected_code]
                    out = op(label, route, payload, status=status, **options)
                    if out.get("code") != expected_code:
                        raise RuntimeError(f"{label}: expected {expected_code}, got {out}")
                    return out

                bravo_policy = {
                    "name": "bravo", "retention_ms": 10000, "lateness_ms": 30000,
                    "future_ms": 1000, "max_series": 100, "max_write": 10000,
                    "max_scan": 100000,
                }
                # alpha has a deliberately tight scan budget so maintenance can
                # be rejected there without any write reaching bravo.
                alpha_policy = {
                    "name": "alpha", "retention_ms": 10000, "lateness_ms": 30000,
                    "future_ms": 1000, "max_series": 100, "max_write": 100,
                    "max_scan": 100,
                }
                op("create bravo", "spaces", bravo_policy)
                op("create alpha", "spaces", alpha_policy)

                def point(name, value):
                    return {"labels": {"__name__": "pulse_total", "node": name},
                            "time": None, "value": value}

                p_a = [point("a", v) for v in range(6)]
                p_b = [point("b", v) for v in range(6)]
                for i, stamp in enumerate([1000, 2000, 3000, 5000, 7000, 9000]):
                    p_a[i]["time"] = stamp
                    p_b[i]["time"] = stamp
                op("ingest bravo", "spaces/bravo/points", {"now": 10000, "points": [*p_a, *p_b]})

                rollup_query = {
                    "start": 0, "end": 10000, "step": 5000, "origin": 0,
                    "group_by": ["node"], "function": "sum",
                    "matchers": [{"label": "__name__", "op": "=", "value": "pulse_total"}],
                }
                op("materialize bravo rollup", "spaces/bravo/rollups/early", rollup_query, method="PUT")
                op("seal bravo", "spaces/bravo/seal", {})

                # 1. Preview is read-only: repeated previews return the same
                # token and the namespace sequence does not move.
                before = op("bravo before preview", "spaces/bravo")
                first = op("preview bravo", "spaces/bravo/retention/preview", {"now": 12000})
                second = op("repeat preview bravo", "spaces/bravo/retention/preview", {"now": 12000})
                after = op("bravo after preview", "spaces/bravo")
                if before["last_sequence"] != after["last_sequence"]:
                    raise RuntimeError("preview consumed a database sequence")
                if first["token"] != second["token"]:
                    raise RuntimeError("identical previews produced different tokens")
                if (first["proposed_floor"] != 2000 or first["current_floor"] != -4503599627370496
                        or not first["advances"] or first["affected_points"] != 2):
                    raise RuntimeError("unexpected preview floor or impact")
                if len(first["series"]) != 2 or {s["removed"] for s in first["series"]} != {1}:
                    raise RuntimeError("preview should report one removed point per affected series")
                if len(first["rollups"]) != 1 or not first["rollups"][0]["overlaps"]:
                    raise RuntimeError("preview should flag the overlapping rollup")
                if first["rollups"][0]["remaining"] != 10:
                    raise RuntimeError("preview should count points remaining in the rollup interval")

                # 2. A late old point arrives while the operator is reviewing.
                # It precedes the proposed floor but is still currently legal.
                late = {"labels": {"__name__": "pulse_total", "node": "a"},
                        "time": 1500, "value": 99}
                op("late old point during preview", "spaces/bravo/points",
                   {"now": 12000, "points": [late]})

                # 3. Confirming the stale token is refused by default; the
                # response tells the operator to recompute or continue.
                fail("stale token rejected", "spaces/bravo/retention/confirm",
                     {"token": first["token"], "on_stale": "reject"}, "conflict")
                refreshed = op("recompute preview", "spaces/bravo/retention/preview", {"now": 12000})
                if refreshed["affected_points"] != 3 or refreshed["token"] == first["token"]:
                    raise RuntimeError("recomputed preview must include the late point and new token")

                # 4. A rollup-only change (no source sequence movement) also
                # invalidates a preview.
                op("second materialization", "spaces/bravo/rollups/late", rollup_query, method="PUT")
                fail("rollup change invalidates token", "spaces/bravo/retention/confirm",
                     {"token": refreshed["token"]}, "conflict")
                current = op("preview after rollup change",
                             "spaces/bravo/retention/preview", {"now": 12000})
                if len(current["rollups"]) != 2:
                    raise RuntimeError("preview should list both rollups")

                # 5. Explicit continue honors the operator's decision and
                # physically removes three points from series a and two from b.
                # Re-presenting the previous token proves staleness is reported.
                committed = op("confirm continue", "spaces/bravo/retention/confirm",
                               {"token": refreshed["token"], "on_stale": "continue"})
                if (not committed["stale_preview"] or committed["no_op"]
                        or committed["removed"] != 3 or committed["affected_series"] != 2
                        or committed["floor"] != 2000):
                    raise RuntimeError("continue confirmation reported an unexpected receipt")
                post = op("bravo after confirm", "spaces/bravo")
                if post["floor"] != 2000:
                    raise RuntimeError("confirmed retention did not persist the floor")
                visible = op("range after confirm", "spaces/bravo/range",
                             {"start": -4503599627370496, "end": 4503599627370497})
                if visible["stats"]["visible"] != 10:
                    raise RuntimeError("10 points should remain visible after retention")
                if any(p["time"] < 2000 for sp in visible["series"] for p in sp["points"]):
                    raise RuntimeError("points below the floor survived compaction")
                fail("resurrection rejected", "spaces/bravo/points",
                     {"now": 12000, "points": [late]}, "conflict")

                # 6. A regressing observation time cannot lower the floor; the
                # confirmation becomes a no-op that writes nothing.
                backward = op("preview with regressing now",
                              "spaces/bravo/retention/preview", {"now": 5000})
                if backward["proposed_floor"] != 2000 or backward["advances"]:
                    raise RuntimeError("regressing now must not propose a lower floor")
                noop = op("confirm regressing now", "spaces/bravo/retention/confirm",
                          {"token": backward["token"], "on_stale": "reject"})
                if not noop["no_op"] or noop["floor"] != 2000 or noop["sequence"] != 0:
                    raise RuntimeError("regressing now should be a no-op confirmation")

                # 7. Forged or tampered tokens are refused before any write,
                # and a valid token signed for another namespace is rejected.
                fail("tampered token rejected", "spaces/bravo/retention/confirm",
                     {"token": current["token"][:-2] + "AA"}, "invalid")
                fail("empty token rejected", "spaces/bravo/retention/confirm",
                     {"token": ""}, "invalid")
                fail("foreign token rejected", "spaces/alpha/retention/confirm",
                     {"token": backward["token"]}, "conflict")

                # 8. Two namespaces are active at once. alpha's tiny budget
                # rejects its own retention preview, and bravo is untouched.
                alpha_points = []
                for stamp in range(0, 200):
                    alpha_points.append({"labels": {"__name__": "pulse_total", "node": "x"},
                                         "time": stamp * 500, "value": stamp})
                op("ingest alpha batch one", "spaces/alpha/points",
                   {"now": 100000, "points": alpha_points[:100]})
                op("ingest alpha batch two", "spaces/alpha/points",
                   {"now": 100000, "points": alpha_points[100:]})
                bravo_seq = op("bravo sequence before alpha failure", "spaces/bravo")["last_sequence"]
                fail("alpha budget exhausted", "spaces/alpha/retention/preview",
                     {"now": 102000}, "capacity")
                bravo_after = op("bravo sequence after alpha failure", "spaces/bravo")
                if bravo_after["last_sequence"] != bravo_seq:
                    raise RuntimeError("alpha budget failure modified bravo")
                alpha_after = op("alpha after its failure", "spaces/alpha")
                if alpha_after["floor"] != -4503599627370496:
                    raise RuntimeError("failed preview moved the alpha floor")

                # 9. The legacy immediate retention path stays available and
                # applies the same monotonic-floor rule.
                immediate = op("immediate retention alpha", "spaces/alpha/retention",
                               {"now": 102000}, status=422)
                if immediate["code"] != "capacity":
                    raise RuntimeError("alpha immediate retention should hit its scan budget")
            finally:
                stop(process)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(observations, indent=2) + "\n")
    print(json.dumps({"workflow": "retention_preview", "operations": len(observations),
                      "state": "complete"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.workflow == "retention_preview":
        retention_preview_demo(args.output)
    else:
        exercise(args.workflow, args.output)
        if args.workflow == "all":
            retention_preview_demo(None)
