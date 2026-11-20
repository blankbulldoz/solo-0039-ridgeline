"""Run a bounded production API demonstration in a temporary directory."""

import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
FLOW_NAMES = [
    "namespace", "ingest", "discovery", "range", "windows", "counters", "seal",
    "delete", "retention", "materialize", "freshness", "compact", "backup",
    "explain", "calendar"
]
NY = ZoneInfo("America/New_York")
HOUR_MS = 3600 * 1000


def ny_ms(year, month, day, hour=0, minute=0, fold=0):
    wall = datetime(year, month, day, hour, minute, tzinfo=NY)
    wall = wall.replace(fold=fold)
    return int(wall.timestamp() * 1000)


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

                # Calendar windows: civil day, week and month buckets in a
                # named IANA timezone, across DST transitions and month ends.
                def require(condition, message):
                    if not condition:
                        raise RuntimeError(message)

                def by_local(result):
                    return {cell["local"]: cell for cell in result["cells"]}

                sky = "spaces/skyline"
                op("calendar namespace", "spaces", {
                    "name": "skyline", "retention_ms": 400 * 24 * HOUR_MS,
                    "lateness_ms": 90 * 24 * HOUR_MS, "future_ms": 10 * 24 * HOUR_MS,
                    "max_series": 100, "max_write": 10000, "max_scan": 100000,
                })

                def sample(when, value):
                    return {"labels": {"__name__": "sample_count", "zone": "metro"},
                            "time": when, "value": value}

                op("calendar ingest", sky + "/points", {"now": ny_ms(2026, 4, 2), "points": [
                    sample(ny_ms(2026, 3, 7, 10, 0), 1),
                    sample(ny_ms(2026, 3, 7, 23, 30), 2),
                    sample(ny_ms(2026, 3, 8, 1, 30), 4),
                    sample(ny_ms(2026, 3, 8, 3, 30), 8),
                    sample(ny_ms(2026, 3, 8, 23, 0), 32),
                    sample(ny_ms(2026, 3, 9, 9, 0), 64),
                    sample(ny_ms(2026, 3, 11, 8, 0), 256),
                    sample(ny_ms(2026, 3, 31, 23, 30), 512),
                    sample(ny_ms(2026, 4, 1, 0, 30), 1024),
                ]})
                op("calendar seal", sky + "/seal", {})
                # Head points merge with sealed blocks in one calendar query;
                # the repeated 03:30 timestamp is a correction and wins.
                op("calendar correction", sky + "/points", {"now": ny_ms(2026, 4, 2), "points": [
                    sample(ny_ms(2026, 3, 9, 21, 0), 128),
                    sample(ny_ms(2026, 3, 8, 3, 30), 16),
                    sample(ny_ms(2026, 4, 1, 12, 0), 2048),
                ]})
                # Fall-back day: two instants share the 01:30 wall time.
                op("calendar fall-back ingest", sky + "/points", {
                    "now": ny_ms(2026, 11, 3), "points": [
                        sample(ny_ms(2026, 10, 31, 23, 0), 4096),
                        sample(ny_ms(2026, 11, 1, 1, 30, fold=0), 8192),
                        sample(ny_ms(2026, 11, 1, 1, 30, fold=1), 16384),
                    ]})

                day_q = {"start": ny_ms(2026, 3, 7), "end": ny_ms(2026, 4, 2),
                         "calendar": {"unit": "day", "timezone": "America/New_York"},
                         "group_by": ["zone"], "function": "count"}
                days = by_local(op("calendar days", sky + "/aggregate", day_q))
                expect = {"2026-03-07": 2, "2026-03-08": 3, "2026-03-09": 2,
                          "2026-03-11": 1, "2026-03-31": 1, "2026-04-01": 2}
                require(set(days) == set(expect),
                        f"day cells {sorted(days)} != {sorted(expect)}: 2026-03-10 is empty")
                for local, count in expect.items():
                    require(days[local]["value"] == count and days[local]["count"] == count,
                            f"day {local} count {days[local]['value']} != {count}")
                spring = days["2026-03-08"]
                require(spring["end"] - spring["start"] == 23 * HOUR_MS,
                        "spring-forward day must span 23 hours")
                require(spring["start"] == ny_ms(2026, 3, 8) and
                        spring["end"] == ny_ms(2026, 3, 9),
                        "spring-forward day boundaries must be local midnights")
                require(days["2026-03-07"]["end"] - days["2026-03-07"]["start"] == 24 * HOUR_MS,
                        "standard day must span 24 hours")

                sums = by_local(op("calendar day sums", sky + "/aggregate",
                                   {**day_q, "function": "sum"}))
                require(sums["2026-03-08"]["value"] == 52,
                        "corrected head value must replace the sealed one: 4+16+32")
                require(sums["2026-03-09"]["value"] == 192,
                        "sealed block and head point must merge: 64+128")

                # The query interval clips the first and last civil days; the
                # cells keep their full natural boundaries and count only
                # points inside the query interval.
                part = by_local(op("calendar partial windows", sky + "/aggregate", {
                    **day_q, "start": ny_ms(2026, 3, 8, 12, 0), "end": ny_ms(2026, 3, 9, 12, 0)}))
                require(set(part) == {"2026-03-08", "2026-03-09"},
                        "clipped query must yield only the two boundary days")
                require(part["2026-03-08"]["value"] == 1 and
                        part["2026-03-08"]["start"] == ny_ms(2026, 3, 8),
                        "first partial day counts in-range points, keeps full boundary")
                require(part["2026-03-09"]["value"] == 1 and
                        part["2026-03-09"]["end"] == ny_ms(2026, 3, 10),
                        "last partial day counts in-range points, keeps full boundary")

                fall = by_local(op("calendar fall-back day", sky + "/aggregate", {
                    **day_q, "start": ny_ms(2026, 10, 31), "end": ny_ms(2026, 11, 3)}))
                require(set(fall) == {"2026-10-31", "2026-11-01"},
                        "2026-11-02 is empty and must not appear")
                require(fall["2026-11-01"]["value"] == 2 and
                        fall["2026-11-01"]["end"] - fall["2026-11-01"]["start"] == 25 * HOUR_MS,
                        "fall-back day must span 25 hours and hold both 01:30 instants")

                week_q = {"start": ny_ms(2026, 3, 7), "end": ny_ms(2026, 3, 16),
                          "calendar": {"unit": "week", "timezone": "America/New_York"},
                          "group_by": ["zone"], "function": "count"}
                monday = by_local(op("calendar weeks monday", sky + "/aggregate", week_q))
                require(set(monday) == {"2026-03-02", "2026-03-09"} and
                        monday["2026-03-02"]["value"] == 5 and monday["2026-03-09"]["value"] == 3,
                        "default week starts on Monday")
                require(monday["2026-03-02"]["end"] - monday["2026-03-02"]["start"] ==
                        7 * 24 * HOUR_MS - HOUR_MS,
                        "DST week keeps seven civil days, not 168 hours")
                sunday = by_local(op("calendar weeks sunday", sky + "/aggregate", {
                    **week_q, "calendar": {"unit": "week", "timezone": "America/New_York",
                                           "week_start": "sunday"}}))
                require(set(sunday) == {"2026-03-01", "2026-03-08"} and
                        sunday["2026-03-01"]["value"] == 2 and sunday["2026-03-08"]["value"] == 6,
                        "week_start moves the week boundary")

                months = by_local(op("calendar months", sky + "/aggregate", {
                    "start": ny_ms(2026, 3, 1), "end": ny_ms(2026, 5, 1),
                    "calendar": {"unit": "month", "timezone": "America/New_York"},
                    "group_by": ["zone"], "function": "sum"}))
                require(set(months) == {"2026-03", "2026-04"}, "month cells")
                march = months["2026-03"]
                require(march["count"] == 9 and march["value"] == 1015,
                        "march must aggregate nine points including the correction")
                require(march["end"] - march["start"] == 31 * 24 * HOUR_MS - HOUR_MS and
                        march["start"] == ny_ms(2026, 3, 1) and march["end"] == ny_ms(2026, 4, 1),
                        "march spans the DST switch: 743 hours ending at local midnight")
                require(months["2026-04"]["value"] == 3072, "april sum")

                op("calendar rollup", sky + "/rollups/daily", day_q, method="PUT")
                stored = op("calendar rollup freshness", sky + "/rollups/daily")
                require(not stored["stale"] and stored["query"]["calendar"]["unit"] == "day",
                        "calendar rollup must store its definition and stay fresh")

                fixed = op("calendar fixed step unchanged", sky + "/aggregate", {
                    "start": day_q["start"], "end": day_q["end"], "step": 24 * HOUR_MS,
                    "origin": 0, "group_by": ["zone"], "function": "count"})
                require(all("local" not in cell for cell in fixed["cells"]),
                        "fixed windows must not gain calendar labels")
                op("calendar rejects step", sky + "/aggregate",
                   {**day_q, "step": 1000}, status=400)
                op("calendar rejects bad zone", sky + "/aggregate",
                   {**day_q, "calendar": {"unit": "day", "timezone": "Mars/Olympus"}}, status=400)
                op("calendar rejects stray week_start", sky + "/aggregate",
                   {**day_q, "calendar": {"unit": "day", "timezone": "America/New_York",
                                          "week_start": "monday"}}, status=400)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    exercise(args.workflow, args.output)
