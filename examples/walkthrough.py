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
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain"
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


def preview_workflow(seed, space, op):
    """Show deletion previews bound to catalog identity and visible points.

    The selector matches a present-empty label and an absent label at once:
    rack="" is stored explicitly on ridge-a while ridge-b has no rack label.
    Both canonical series match because absent labels compare as empty, yet
    they remain distinct identities in the preview and the tombstone.
    """
    query = {"start": -5000, "end": 10000, "matchers": [
        {"label": "__name__", "op": "=", "value": "load_ratio"},
        {"label": "rack", "op": "=", "value": ""},
    ]}
    present_empty = {"__name__": "load_ratio", "node": "ridge-a", "zone": "east", "rack": ""}
    absent = {"__name__": "load_ratio", "node": "ridge-b", "zone": "west"}
    op("late load observations", space + "/points", {"now": 10000, "points": [
        {"labels": present_empty, "time": 2500, "value": 0.4},
        {"labels": present_empty, "time": 4000, "value": 0.6},
        {"labels": absent, "time": 3000, "value": 0.5},
    ]})

    made = op("delete preview", space + "/deletion-previews", query)
    if made["state"] != "pending" or made["visible"] != 4:
        raise RuntimeError("preview should expose pending visibility")
    if sorted(s["series"]["labels"]["node"] for s in made["series"]) != ["ridge-a", "ridge-b"]:
        raise RuntimeError("absent and present-empty labels must both be selected")
    labels_by_node = {s["series"]["labels"]["node"]: s["series"]["labels"] for s in made["series"]}
    if "rack" not in labels_by_node["ridge-a"] or "rack" in labels_by_node["ridge-b"]:
        raise RuntimeError("preview must preserve the absent label identity")
    for entry in made["series"]:
        if entry["visible"] and not entry["coverage"]:
            raise RuntimeError("visible series require time coverage")
    if op("preview before confirm", space + "/deletion-previews/" + made["id"])["state"] != "pending":
        raise RuntimeError("unchanged preview should remain pending")
    confirmed = op("confirm preview", space + "/deletion-previews/" + made["id"] + "/confirm",
                   {"fingerprint": made["fingerprint"]})
    if confirmed["hidden"] != 4 or len(confirmed["tombstone"]["series"]) != 2:
        raise RuntimeError("confirmation must hide exactly the previewed samples")
    done = op("confirmed preview view", space + "/deletion-previews/" + made["id"])
    if done["state"] != "confirmed" or done["tombstone_id"] != confirmed["tombstone"]["id"]:
        raise RuntimeError("confirmed preview must expose its tombstone binding")
    op("re-report after confirmed delete", space + "/points", {"now": 10000, "points": [
        {"labels": present_empty, "time": 2500, "value": 0.9},
    ]})
    visible = op("later report visible", space + "/range", query)
    ridge_a = [row for row in visible["series"] if row["series"]["labels"]["node"] == "ridge-a"]
    if len(ridge_a) != 1 or [p["time"] for p in ridge_a[0]["points"]] != [2500]:
        raise RuntimeError("later report at a deleted timestamp should be visible")

    cancelled = op("delete preview to cancel", space + "/deletion-previews", query)
    op("cancel preview", space + "/deletion-previews/" + cancelled["id"] + "/cancel",
       {"fingerprint": cancelled["fingerprint"]})
    op("confirm cancelled preview", space + "/deletion-previews/" + cancelled["id"] + "/confirm",
       {"fingerprint": cancelled["fingerprint"]}, status=409)
    if op("cancelled preview view", space + "/deletion-previews/" + cancelled["id"])["state"] != "cancelled":
        raise RuntimeError("cancelled preview must stay cancelled")

    drifting = op("delete preview before correction", space + "/deletion-previews", query)
    op("same-time correction", space + "/points", {"now": 10000, "points": [
        {"labels": present_empty, "time": 2500, "value": 0.7},
    ]})
    drifted = op("drifted preview", space + "/deletion-previews/" + drifting["id"])
    if drifted["state"] != "invalidated" or "visible_points_replaced" not in drifted["drift"]["reasons"]:
        raise RuntimeError("same-time correction must invalidate the preview")
    if not any(change["replaced_times"] == [2500] for change in drifted["drift"]["series"]):
        raise RuntimeError("drift should name the corrected timestamp")
    op("confirm drifted preview", space + "/deletion-previews/" + drifting["id"] + "/confirm",
       {"fingerprint": drifting["fingerprint"]}, status=409)
    op("cancel drifted preview", space + "/deletion-previews/" + drifting["id"] + "/cancel",
       {"fingerprint": drifting["fingerprint"]})

    growing = op("delete preview before new series", space + "/deletion-previews", query)
    op("report new series", space + "/points", {"now": 10000, "points": [
        {"labels": {"__name__": "load_ratio", "node": "ridge-c", "zone": "west"},
         "time": 6000, "value": 0.3},
    ]})
    grown = op("preview with new series", space + "/deletion-previews/" + growing["id"])
    if grown["state"] != "pending" or grown["drift"]["invalidated"]:
        raise RuntimeError("unselected new series must not invalidate the preview")
    if [entry["series"]["labels"]["node"] for entry in grown["drift"]["new_series"]] != ["ridge-c"]:
        raise RuntimeError("new matching series should be reported separately")
    grown_confirm = op("confirm preview with new series",
                       space + "/deletion-previews/" + growing["id"] + "/confirm",
                       {"fingerprint": growing["fingerprint"]})
    if grown_confirm["hidden"] != 1:
        raise RuntimeError("confirmation must ignore series outside the preview")
    after = op("new series stays visible", space + "/range", query)
    if [row["series"]["labels"]["node"] for row in after["series"]] != ["ridge-c"]:
        raise RuntimeError("only the later reported series should remain visible")

    shadowed = op("delete preview before legacy delete", space + "/deletion-previews", query)
    op("legacy immediate delete", space + "/deletions", {
        "start": 0, "end": 7000, "matchers": [{"label": "__name__", "op": "=", "value": "load_ratio"}],
    })
    overlap = op("preview after legacy delete", space + "/deletion-previews/" + shadowed["id"])
    if overlap["state"] != "invalidated" or "overlapping_deletion" not in overlap["drift"]["reasons"]:
        raise RuntimeError("overlapping deletion must invalidate the preview")
    op("confirm shadowed preview", space + "/deletion-previews/" + shadowed["id"] + "/confirm",
       {"fingerprint": shadowed["fingerprint"]}, status=409)
    op("cancel shadowed preview", space + "/deletion-previews/" + shadowed["id"] + "/cancel",
       {"fingerprint": shadowed["fingerprint"]})


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
                preview_workflow(seed, space, op)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", choices=["all", *FLOW_NAMES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    exercise(args.workflow, args.output)
