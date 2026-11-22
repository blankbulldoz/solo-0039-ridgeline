"""Run a bounded production API demonstration in a temporary directory."""

import argparse
import hashlib
import http.client
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
    "delete", "retention", "materialize", "freshness", "compact", "backup",
    "explain", "incremental"
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


def stream(base, route, payload, headers, close_after=None):
    """Read server-sent events; optionally hang up after N window events."""
    host, port = base.removeprefix("http://").split(":")
    connection = http.client.HTTPConnection(host, int(port), timeout=20)
    connection.request("POST", "/ridge/v1/" + route, body=json.dumps(payload),
                       headers={"Content-Type": "application/json", **headers})
    response = connection.getresponse()
    events, event, data, windows = [], None, [], 0
    while True:
        line = response.readline()
        if not line:
            break
        line = line.decode().rstrip("\n")
        if line.startswith("event: "):
            event = line[7:]
        elif line.startswith("data: "):
            data.append(line[6:])
        elif line == "" and event:
            events.append({"event": event, "data": json.loads("".join(data))})
            windows += event == "window"
            event, data = None, []
            if close_after is not None and windows >= close_after:
                connection.close()
                return events, response.status, dict(response.headers)
    connection.close()
    return events, response.status, dict(response.headers)


def exercise_incremental(base, space, observations, op):
    """Deliver final windows early, correct mid-stream, and survive a hang-up.

    The query span is split across two sealed blocks and a head correction so
    the first window depends on several sources and the later block is only
    decoded after the first window has already been delivered.
    """
    headers = {"Accept": "text/event-stream"}
    query = {"start": 0, "end": 6000, "step": 1000, "origin": 0, "group_by": ["zone"],
             "function": "sum", "matchers": [{"label": "__name__", "op": "=", "value": "load_ratio"}]}

    def ingest(points, now=10000):
        op("incremental ingest", space + "/points", {"now": now, "points": points})

    def labels(node, zone):
        return {"__name__": "load_ratio", "node": node, "zone": zone}

    # First window: one contribution from an early block, one from the head,
    # and one late-but-overlapping correction block.
    ingest([{"labels": labels("ridge-a", "east"), "time": 100, "value": 2.0}])
    op("incremental seal", space + "/seal", {})
    ingest([{"labels": labels("ridge-b", "east"), "time": 200, "value": 4.0},
            {"labels": labels("ridge-a", "east"), "time": 300, "value": 99.0},
            {"labels": labels("ridge-c", "west"), "time": 400, "value": 1.0}])
    op("incremental seal correction", space + "/seal", {})
    ingest([{"labels": labels("ridge-a", "east"), "time": 300, "value": 6.0}])
    # Second window lives in a block that the client never needs to wait for.
    ingest([{"labels": labels("ridge-a", "east"), "time": 5100, "value": 8.0},
            {"labels": labels("ridge-b", "east"), "time": 5900, "value": 10.0}])
    op("incremental seal late", space + "/seal", {})

    events, status, response_headers = stream(base, space + "/aggregate", query, headers)
    observations.append({"step": "incremental windows", "request": query,
                         "status": status, "events": events})
    if response_headers.get("Content-Type") != "text/event-stream; charset=utf-8":
        raise RuntimeError("incremental aggregate did not stream")
    sequence = events[0]["data"]["sequence"]
    if any(event["data"].get("sequence", sequence) != sequence for event in events):
        raise RuntimeError("streamed windows used more than one read view")
    windows = [event["data"] for event in events if event["event"] == "window"]
    if [window["start"] for window in windows] != [0, 5000]:
        raise RuntimeError(f"windows were not delivered once each in time order: {windows}")
    if events[-1]["event"] != "done" or not events[-1]["data"]["complete"]:
        raise RuntimeError("completed stream must end with a complete marker")
    first = {(cell["labels"]["zone"], cell["start"]): cell for cell in windows[0]["cells"]}
    # 2 + 4 + corrected 6 for east, 1 for west: the correction replaced the
    # earlier contribution instead of being added to it.
    if (first[("east", 0)]["value"], first[("east", 0)]["count"]) != (12.0, 3):
        raise RuntimeError("first window did not resolve the correction")
    if first[("west", 0)]["value"] != 1.0:
        raise RuntimeError("grouping across nodes was not preserved")

    # An event-stream request changes representation, not arithmetic: the same
    # query over ordinary JSON still returns one document.
    plain = request(base, space + "/aggregate", query)
    if "cells" not in plain:
        raise RuntimeError("ordinary JSON aggregate behaviour changed")

    # Stop reading after the first window: the server must observe the hang-up,
    # release the read transaction and stay healthy.
    partial, _, _ = stream(base, space + "/aggregate", query, headers, close_after=1)
    observations.append({"step": "incremental disconnect", "windows_read":
                         sum(event["event"] == "window" for event in partial)})
    if [event["event"] for event in partial] != ["open", "window"]:
        raise RuntimeError("client should observe the first final window before hanging up")
    if request(base, "health")["state"] != "ready":
        raise RuntimeError("service unhealthy after client disconnected")
    if request(base, space + "/aggregate", query)["cells"] != plain["cells"]:
        raise RuntimeError("disconnect left the database changed")

    # A namespace whose scan budget covers only the early block: the first
    # window must be delivered, then the stream must fail closed instead of
    # pretending the query completed or retracting what was delivered.
    tight = {"name": "feed", "retention_ms": 100000, "lateness_ms": 30000,
             "future_ms": 1000, "max_series": 10, "max_write": 6, "max_scan": 6}
    op("incremental tight namespace", "spaces", tight)
    def feed(times):
        op("incremental tight ingest", "spaces/feed/points", {
            "now": 10000,
            "points": [{"labels": labels("ridge-a", "east"), "time": t, "value": float(t)} for t in times]})
    feed([100, 300, 500, 700])
    op("incremental tight seal", "spaces/feed/seal", {})
    feed([5100, 5300, 5500, 5700])
    op("incremental tight seal late", "spaces/feed/seal", {})
    fed = {"start": 0, "end": 7000, "step": 1000, "origin": 0, "group_by": ["zone"],
           "function": "count", "matchers": [{"label": "__name__", "op": "=", "value": "load_ratio"}]}
    events, _, _ = stream(base, "spaces/feed/aggregate", fed, headers)
    observations.append({"step": "incremental budget cut", "events": events})
    delivered = [event["data"] for event in events if event["event"] == "window"]
    if [window["start"] for window in delivered] != [0]:
        raise RuntimeError("early window must be delivered before the late block is scanned")
    failure = events[-1]
    if failure["event"] != "error" or failure["data"]["code"] != "capacity":
        raise RuntimeError(f"budget cut must end with an error, not completion: {failure}")
    if failure["data"]["complete"] or failure["data"]["windows"] != 1:
        raise RuntimeError("budget cut must not claim completion or lose delivered windows")
    # The same query without streaming still fails as one typed response.
    request(base, "spaces/feed/aggregate", fed, status=422)
    return sequence


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
                if workflow in ("all", "incremental"):
                    exercise_incremental(base, space, observations, op)
                if workflow != "incremental":
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
            if workflow != "incremental":
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
