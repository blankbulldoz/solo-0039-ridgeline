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
    "delete", "retention", "materialize", "freshness", "compact", "backup", "explain",
    "settle"
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
                # Settlement: 16-bit link counters settled against boot evidence.
                links = "spaces/links"
                op("settle namespace", "spaces", {
                    "name": "links", "retention_ms": 4000000, "lateness_ms": 4000000,
                    "future_ms": 1000, "max_series": 100, "max_write": 10000,
                    "max_scan": 100000})
                counts, boots = [], []

                def sample(store, name, device, at, value):
                    store.append({"labels": {"__name__": name, "device": device},
                                  "time": at, "value": value})

                # Identical 65530 -> 4 transitions under different boot evidence.
                for device in ("alpha", "beta", "gamma"):
                    sample(counts, "if_octets_total", device, 10000, 65530)
                    sample(counts, "if_octets_total", device, 20000, 4)
                sample(boots, "boot_id", "alpha", 0, 7)
                sample(boots, "boot_id", "alpha", 30000, 7)    # continuous run
                sample(boots, "boot_id", "beta", 0, 7)
                sample(boots, "boot_id", "beta", 30000, 8)     # rebooted
                # gamma reports no boot identifier at all.
                # A long interval where several wraps fit the growth rate.
                sample(counts, "if_octets_total", "delta", 0, 100)
                sample(counts, "if_octets_total", "delta", 100000, 200)
                sample(boots, "boot_id", "delta", 0, 5)
                sample(boots, "boot_id", "delta", 100000, 5)
                # A jump even the minimum wrap cannot explain under the rate limit.
                sample(counts, "if_octets_total", "epsilon", 10000, 0)
                sample(counts, "if_octets_total", "epsilon", 20000, 60000)
                sample(boots, "boot_id", "epsilon", 0, 3)
                sample(boots, "boot_id", "epsilon", 30000, 3)
                receipt = op("settle ingest", links + "/points",
                             {"now": 100000, "points": counts + boots})
                view = op("settle namespace view", links)
                settle_q = {
                    "start": 0, "end": 100001,
                    "count_matchers": [{"label": "__name__", "op": "=",
                                        "value": "if_octets_total"}],
                    "boot_matchers": [{"label": "__name__", "op": "=", "value": "boot_id"}],
                    "join": ["device"], "modulus": 65536, "max_rate": 1500,
                    "reconcile": 131172}
                first = op("settle contrast", links + "/settle", settle_q)
                if first["sequence"] != view["last_sequence"] or \
                        first["sequence"] != receipt["sequence"]:
                    raise RuntimeError("settlement must read one committed view")

                def interval(device, result):
                    found = {g["labels"]["device"]: g for g in result["groups"]}
                    intervals = found[device]["intervals"]
                    if len(intervals) != 1:
                        raise RuntimeError(f"{device}: expected one interval: {intervals}")
                    return intervals[0]

                alpha, beta, gamma = (interval(d, first) for d in ("alpha", "beta", "gamma"))
                delta, epsilon = interval("delta", first), interval("epsilon", first)
                if (alpha["status"], alpha["increment"]) != ("settled", 10):
                    raise RuntimeError(f"continuous boot must settle one wrap: {alpha}")
                if alpha["reconcile_match"] is not False:
                    raise RuntimeError(f"131172 is not the settled increment: {alpha}")
                if beta["status"] != "rebooted" or "reconcile_match" in beta:
                    raise RuntimeError(f"boot change must not settle: {beta}")
                if gamma["status"] != "unknown" or "reconcile_match" in gamma:
                    raise RuntimeError(f"missing boot evidence must not settle: {gamma}")
                if (delta["status"], delta["min"], delta["max"]) != ("ambiguous", 100, 131172):
                    raise RuntimeError(f"long interval must bound wraps: {delta}")
                if delta["reconcile_match"] is not True:
                    raise RuntimeError(f"131172 is a plausible wrap candidate: {delta}")
                if epsilon["status"] != "exceeded" or "reconcile_match" in epsilon:
                    raise RuntimeError(f"rate violation must not settle: {epsilon}")
                by_device = {g["labels"]["device"]: g for g in first["groups"]}
                if by_device["alpha"]["settled_total"] != 10 or \
                        by_device["delta"]["settled_total"] != 0:
                    raise RuntimeError("settled totals only include unique increments")
                miss = op("settle reconcile miss", links + "/settle",
                          {**settle_q, "reconcile": 50000})
                if interval("delta", miss)["reconcile_match"] is not False:
                    raise RuntimeError("50000 is not a plausible wrap candidate")
                op("settle boot correction", links + "/points", {"now": 100000, "points": [
                    {"labels": {"__name__": "boot_id", "device": "beta"},
                     "time": 30000, "value": 7}]})
                corrected = op("settle corrected", links + "/settle", settle_q)
                if (interval("beta", corrected)["status"],
                        interval("beta", corrected)["increment"]) != ("settled", 10):
                    raise RuntimeError("corrected boot identifier did not settle")
                op("settle boot deletion", links + "/deletions", {
                    "start": 25000, "end": 35000,
                    "matchers": [{"label": "__name__", "op": "=", "value": "boot_id"},
                                 {"label": "device", "op": "=", "value": "alpha"}]})
                deleted = op("settle deleted evidence", links + "/settle", settle_q)
                if interval("alpha", deleted)["status"] != "unknown":
                    raise RuntimeError("deleted boot evidence must read as unknown")
                op("settle seal", links + "/seal", {})
                sealed = op("settle sealed", links + "/settle", settle_q)
                if sealed["groups"] != deleted["groups"]:
                    raise RuntimeError("sealing changed settlement results")
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
