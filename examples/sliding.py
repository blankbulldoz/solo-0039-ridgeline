"""Replay a dense sliding-window workload against a live Ridgeline instance.

The scenario models a shift dashboard that reads the trailing hour of load at a
ten second cadence. Thirty devices in one group report one sample per second for
two continuous hours. Each output time t reduces [t-width, t); the same
observation is reused by many overlapping windows, so the demonstration reports
both the naive membership count and the bounded reduction work actually done.

The script also shows a silent device (empty windows), a negative-time window,
and that sealing head points does not change sliding results.
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

DEVICES = 30
WIDTH = 3600 * 1000
STEP = 10 * 1000
HOURS = 2
START = 0
END = HOURS * 3600 * 1000
SILENT = "ridge-silent"
SILENT_FROM = 45 * 60 * 1000  # device reports for 45 minutes, then stops


def request(base, route, payload=None, method=None, status=200):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route, data=body,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        response = urllib.request.urlopen(req, timeout=60)
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


def device_labels(index, silent=False):
    return {"__name__": "load_ratio", "zone": "north" if silent else "east",
            "device": SILENT if silent else f"ridge-{index:02d}"}


def stream(silent=False):
    """One point per second for the replay span, with a warm-up pre-roll."""
    points = []
    step = 1000
    for t in range(-WIDTH, END, step):
        if silent and t >= SILENT_FROM:
            break
        value = ((t // step) % 100) / 100.0
        points.append({"labels": device_labels(0, silent), "time": t, "value": value})
    return points


def batch(base, space, points, now=END):
    for offset in range(0, len(points), 9000):
        request(base, space + "/points",
                {"now": now, "points": points[offset:offset + 9000]})


def aggregate(base, space, function, start=START, end=END, width=WIDTH, origin=0,
              matchers=None):
    return request(base, space + "/aggregate", {
        "start": start, "end": end, "step": STEP, "origin": origin,
        "width": width, "group_by": ["zone"], "function": function,
        "matchers": matchers or [],
    })


def demo(output):
    observations = []
    with tempfile.TemporaryDirectory(prefix="ridgeline-sliding-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/ridgeline"], cwd=ROOT, check=True)
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "sliding.db", log)
            try:
                space = "spaces/shift"
                request(base, "spaces", {
                    "name": "shift", "retention_ms": 10 * 365 * 24 * 3600 * 1000,
                    "lateness_ms": 30 * 24 * 3600 * 1000, "future_ms": 1000,
                    "max_series": 100, "max_write": 9000, "max_scan": 1000000,
                })
                # Ingest the warm-up pre-roll so the first output window can read
                # samples that precede the requested output interval.
                for index in range(DEVICES):
                    points = stream()
                    for p in points:
                        p["labels"] = device_labels(index)
                    batch(base, space, points)
                batch(base, space, stream(silent=True))

                dense = [{"label": "zone", "op": "=", "value": "east"}]
                avg = aggregate(base, space, "avg", matchers=dense)
                total = aggregate(base, space, "sum", matchers=dense)
                peak = aggregate(base, space, "max", matchers=dense)
                observations.append({
                    "step": "dense-sliding-avg",
                    "cells": len(avg["cells"]), "first": avg["cells"][0],
                    "last": avg["cells"][-1], "sliding": avg["sliding"],
                    "scan": avg["stats"],
                })
                expected_groups = 1
                dense_windows = avg["sliding"]["outputs"]
                dense_cells = dense_windows * expected_groups
                if len(avg["cells"]) != dense_cells:
                    raise RuntimeError(
                        f"dense replay lost populated windows: {len(avg['cells'])} != {dense_cells}")
                if avg["sliding"]["groups"] != expected_groups:
                    raise RuntimeError("dense replay mixed groups")
                if avg["sliding"]["reductions"] != avg["sliding"]["observations"]:
                    raise RuntimeError("reduction work scaled with window reuse")
                if avg["sliding"]["memberships"] <= avg["sliding"]["reductions"] * 2:
                    raise RuntimeError("reuse factor too small for a meaningful demo")
                total_first = total["cells"][0]
                if total_first["count"] != avg["cells"][0]["count"]:
                    raise RuntimeError("sum and avg disagreed on observation count")
                if peak["cells"][0]["value"] < avg["cells"][0]["value"]:
                    raise RuntimeError("max fell below avg")

                # The silent device reports for 45 minutes, then stops. Once the
                # trailing hour no longer contains one of its samples its group
                # is empty: the window is omitted and counted, not fabricated.
                silent = aggregate(base, space, "count", matchers=[
                    {"label": "zone", "op": "=", "value": "north"}])
                if silent["sliding"]["groups"] != 1:
                    raise RuntimeError("silent device group was not isolated")
                if silent["sliding"]["gap_cells"] < 1:
                    raise RuntimeError("silent device produced no empty window")
                if silent["sliding"]["empty_outputs"] != silent["sliding"]["gap_cells"]:
                    raise RuntimeError("empty-window accounting disagrees")
                last_live_output = SILENT_FROM + WIDTH
                if any(cell["end"] > last_live_output for cell in silent["cells"]):
                    raise RuntimeError("silent device still reported after its window emptied")
                observations.append({
                    "step": "silent-device",
                    "silent_stop": SILENT_FROM,
                    "window_width": WIDTH,
                    "last_live_output": last_live_output,
                    "gap_cells": silent["sliding"]["gap_cells"],
                    "empty_outputs": silent["sliding"]["empty_outputs"],
                    "outputs": silent["sliding"]["outputs"],
                    "north_cells": len(silent["cells"]),
                })

                # Negative time: output times align to origin 0 and the first
                # window reads before the requested start. Output time 0 is the
                # last output because the interval is half-open [-step, step).
                negative = aggregate(base, space, "avg", start=-STEP, end=STEP, matchers=dense)
                observations.append({"step": "negative-boundary", "result": negative})
                neg_cells = negative["cells"]
                if len(neg_cells) != 2:
                    raise RuntimeError("negative span did not yield exactly two aligned outputs")
                if neg_cells[0]["start"] != -WIDTH-STEP or neg_cells[0]["end"] != -STEP:
                    raise RuntimeError("first negative window is not [t-width, t)")
                if neg_cells[1]["start"] != -WIDTH or neg_cells[1]["end"] != 0:
                    raise RuntimeError("cross-epoch window is not [t-width, t)")
                if neg_cells[0]["count"] <= 0 or neg_cells[1]["count"] <= 0:
                    raise RuntimeError("negative window read no samples")

                # Seal: moving head points into blocks must preserve the answer.
                before = aggregate(base, space, "avg", matchers=dense)
                planned = request(base, space + "/explain", {
                    "start": START, "end": END, "step": STEP, "origin": 0,
                    "width": WIDTH, "group_by": ["zone"], "function": "avg",
                    "matchers": dense,
                })
                if planned["sliding"]["source_points"] != before["sliding"]["observations"]:
                    raise RuntimeError(
                        f"explain source span {planned['sliding']['source_points']} disagrees "
                        f"with the executed scan {before['sliding']['observations']}")
                if planned["sliding"]["read_start"] != -WIDTH:
                    raise RuntimeError("explain omits the first-window pre-roll")
                observations.append({"step": "explain", "sliding": planned["sliding"]})
                request(base, space + "/seal", {})
                after = aggregate(base, space, "avg", matchers=dense)
                if before["cells"] != after["cells"]:
                    raise RuntimeError("seal changed sliding results")
                observations.append({
                    "step": "seal",
                    "blocks": len(request(base, space + "/blocks")),
                    "cells_equal": True,
                    "sliding": after["sliding"],
                })

                # Delete a slice of history that only the first output window can
                # read. It falls before every later window, so only the first
                # window changes: window membership, not mutation order, decides.
                first = avg["cells"][0]
                request(base, space + "/deletions", {
                    "start": first["start"], "end": first["start"] + STEP,
                    "matchers": dense,
                })
                changed = aggregate(base, space, "avg", matchers=dense)
                if len(changed["cells"]) != len(avg["cells"]):
                    raise RuntimeError("a pre-roll deletion changed the window count")
                for a, b in zip(avg["cells"][1:], changed["cells"][1:]):
                    # Floating-point summation order may differ between runs;
                    # the observations and therefore the counts must not.
                    if a["count"] != b["count"] or abs(a["value"] - b["value"]) > 1e-9:
                        raise RuntimeError(f"a pre-roll deletion changed a later window: {a} -> {b}")
                if changed["cells"][0]["count"] >= first["count"]:
                    raise RuntimeError("pre-roll deletion did not change the first window")
                observations.append({
                    "step": "first-window-mutation",
                    "deleted": [first["start"], first["start"] + STEP],
                    "first_before": {"count": first["count"], "value": first["value"]},
                    "first_after": {"count": changed["cells"][0]["count"],
                                    "value": changed["cells"][0]["value"]},
                    "later_windows_equal": True,
                })
            finally:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=20)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(observations, indent=2) + "\n")
    print(json.dumps({"workflow": "sliding", "operations": len(observations), "state": "complete"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    demo(args.output)
