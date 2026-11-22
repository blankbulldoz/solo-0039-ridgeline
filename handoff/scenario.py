"""Build the Ridgeline forked-restore scenario from examples/input.json.

Topology (all images are bbolt files produced through the real HTTP API):

    base-seed (shared backup, one source sequence namespace)
      |
      | restore the same image bytes three times into separate temp instances
      v
    branch-a      observation fork (same corrected timestamp, value 8.0)
    branch-b      observation fork (same corrected timestamp, value 9.0)
    branch-c      physical-only fork (no logical writes, seal+compact only)

Every branch materializes the SAME two rollup names with the SAME query text.
branch-a and branch-b finish with identical source_sequence values while their
stored rollup values differ. branch-c has identical logical observations to the
seed but different block IDs, checksums and image hash.

Run with -I; all paths are passed explicitly, nothing is imported from the
data directories.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SPACE = "spindle"
COMMON_QUERY = {
    "start": -5000, "end": 10000, "step": 1000, "origin": 0,
    "group_by": ["zone"], "function": "avg",
    "matchers": [{"label": "__name__", "op": "=", "value": "pulse_total"}],
}
SUM_QUERY = dict(COMMON_QUERY, function="sum")
# Same rollup name on every branch, but branch-b narrows the half-open
# interval, so the pair demonstrates a query-caliber (口径) divergence even
# though both reports honestly describe their own image.
WIDE_CALIBER_QUERY = dict(COMMON_QUERY)
NARROW_CALIBER_QUERY = dict(COMMON_QUERY, start=-2000, end=2000)
CORRECTION_SERIES = {"__name__": "pulse_total", "node": "ridge-a", "zone": "east"}


def http_request(base, route, payload=None, method=None, expect=200):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route, data=body, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        response = urllib.request.urlopen(req, timeout=20)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        data = response.read()
        if response.status != expect:
            raise RuntimeError(
                f"{route} returned {response.status}, wanted {expect}: "
                f"{data.decode(errors='replace')}")
        if response.headers.get_content_type() == "application/octet-stream":
            return data, dict(response.headers)
        return json.loads(data), dict(response.headers)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, binary, database, log_path):
        self.binary = binary
        self.database = database
        self.log_path = log_path
        self.process = None
        self.base = None

    def __enter__(self):
        port = free_port()
        self.base = f"http://127.0.0.1:{port}"
        log = open(self.log_path, "ab")
        self.process = subprocess.Popen(
            [str(self.binary)],
            env={**os.environ, "RIDGE_DB": str(self.database),
                 "RIDGE_ADDR": f"127.0.0.1:{port}"},
            stdout=log, stderr=log,
        )
        for _ in range(100):
            if self.process.poll() is not None:
                raise RuntimeError(f"service exited early; see {self.log_path}")
            try:
                http_request(self.base, "health")
                return self
            except (OSError, RuntimeError):
                time.sleep(0.05)
        raise RuntimeError("service did not become ready")

    def __exit__(self, *exc):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=15)
            if self.process.returncode not in (0, None):
                raise RuntimeError(f"service exit code {self.process.returncode}")

    def call(self, label, route, payload=None, method=None, expect=200):
        started = time.time_ns()
        out, headers = http_request(self.base, route, payload, method, expect)
        record = {
            "step": label, "route": route, "method": method or ("GET" if payload is None else "POST"),
            "request": payload, "response": out,
            "elapsed_ns": time.time_ns() - started,
        }
        if isinstance(out, dict) and "sequence" in out:
            record["sequence"] = out["sequence"]
        return out, headers, record

    def image(self, label):
        data, headers = http_request(self.base, "image")
        digest = hashlib.sha256(data).hexdigest()
        if digest != headers["X-Ridge-Sha256"]:
            raise RuntimeError("image header hash does not match payload")
        return data, {
            "step": label, "sha256": digest, "bytes": len(data),
            "sequence": int(headers["X-Ridge-Sequence"]),
        }


def build_binary(scratch):
    binary = scratch / "ridgeline"
    go = os.environ.get("GO_BIN", "go")
    subprocess.run([go, "build", "-o", str(binary), "./cmd/ridgeline"],
                   cwd=ROOT, check=True)
    return binary


def seed_instance(server, seed, journal):
    server.call("create namespace", "spaces", seed["namespace"])
    out, _, rec = server.call(
        "ingest seed observations", f"spaces/{SPACE}/points",
        {"now": seed["now"], "points": seed["points"]})
    journal.append(rec)
    server.call("seal seed head", f"spaces/{SPACE}/seal", {})
    ns, _, _ = server.call("inspect seed namespace", f"spaces/{SPACE}")
    blocks, _, _ = server.call("seed block inventory", f"spaces/{SPACE}/blocks")
    raw, _, _ = server.call("seed logical range", f"spaces/{SPACE}/range",
                            {"start": -5000, "end": 10000})
    return ns, blocks, raw


def branch_with_correction(server, value, journal, branch):
    payload = {"now": 10000, "points": [
        {"labels": CORRECTION_SERIES, "time": 500, "value": value}]}
    _, _, rec = server.call(
        f"{branch} corrective write at t=500", f"spaces/{SPACE}/points", payload)
    journal.append(rec)
    # Rollup metadata advances the global sequence but does not touch the
    # namespace's last_sequence, so source_sequence stays pinned to the write.
    r1, _, rec1 = server.call(
        f"{branch} materialize rollup pulse-avg",
        f"spaces/{SPACE}/rollups/pulse-avg", COMMON_QUERY, method="PUT")
    r2, _, rec2 = server.call(
        f"{branch} materialize rollup pulse-sum",
        f"spaces/{SPACE}/rollups/pulse-sum", SUM_QUERY, method="PUT")
    caliber = NARROW_CALIBER_QUERY if branch == "branch-b" else WIDE_CALIBER_QUERY
    r3, _, rec3 = server.call(
        f"{branch} materialize rollup pulse-caliber",
        f"spaces/{SPACE}/rollups/pulse-caliber", caliber, method="PUT")
    journal.extend([rec1, rec2, rec3])
    seqs = {r1["source_sequence"], r2["source_sequence"], r3["source_sequence"]}
    if len(seqs) != 1:
        raise RuntimeError(f"{branch}: rollup source sequences diverged")
    return r1, r2


def physical_only_branch(server, journal, branch):
    # No points are written. The head is already empty after the seed seal,
    # so this seal only publishes a maintenance sequence; the subsequent
    # compaction rewrites every physical block with new seq-prefixed IDs.
    server.call(f"{branch} layout-only seal", f"spaces/{SPACE}/seal", {})
    server.call(f"{branch} layout-only compact", f"spaces/{SPACE}/compact", {})
    r1, _, rec1 = server.call(
        f"{branch} materialize rollup pulse-avg",
        f"spaces/{SPACE}/rollups/pulse-avg", COMMON_QUERY, method="PUT")
    r2, _, rec2 = server.call(
        f"{branch} materialize rollup pulse-sum",
        f"spaces/{SPACE}/rollups/pulse-sum", SUM_QUERY, method="PUT")
    r3, _, rec3 = server.call(
        f"{branch} materialize rollup pulse-caliber",
        f"spaces/{SPACE}/rollups/pulse-caliber", WIDE_CALIBER_QUERY, method="PUT")
    journal.extend([rec1, rec2, rec3])
    seqs = {r1["source_sequence"], r2["source_sequence"], r3["source_sequence"]}
    if len(seqs) != 1:
        raise RuntimeError(f"{branch}: rollup source sequences diverged")
    return r1, r2


def collect_final_state(server, branch, journal):
    for label, route, payload in [
        (f"{branch} namespace", f"spaces/{SPACE}", None),
        (f"{branch} blocks", f"spaces/{SPACE}/blocks", None),
        (f"{branch} range", f"spaces/{SPACE}/range",
         {"start": -5000, "end": 10000}),
        (f"{branch} rollup pulse-avg", f"spaces/{SPACE}/rollups/pulse-avg", None),
        (f"{branch} rollup pulse-sum", f"spaces/{SPACE}/rollups/pulse-sum", None),
        (f"{branch} rollup pulse-caliber",
         f"spaces/{SPACE}/rollups/pulse-caliber", None),
        (f"{branch} explain", f"spaces/{SPACE}/explain",
         {"start": -5000, "end": 10000}),
    ]:
        out, _, rec = server.call(label, route, payload)
        journal.append(rec)
        yield label, out


def run(scratch, dist):
    scratch = Path(scratch)
    images = dist / "images"
    images.mkdir(parents=True, exist_ok=True)
    binary = build_binary(scratch)
    seed = json.loads((ROOT / "examples" / "input.json").read_text())
    journal = []

    # ---- shared backup -----------------------------------------------------
    seed_db = scratch / "seed.db"
    with Server(binary, seed_db, scratch / "seed.log") as server:
        ns, seed_blocks, seed_raw = seed_instance(server, seed, journal)
        image_bytes, image_meta = server.image("export base-seed image")
    seed_image = images / "base-seed.db"
    seed_image.write_bytes(image_bytes)
    image_meta["image"] = seed_image.name
    images_meta = [image_meta]

    branches = {
        "branch-a": {"kind": "observation_fork", "correction": 8.0},
        "branch-b": {"kind": "observation_fork", "correction": 9.0},
        "branch-c": {"kind": "physical_only", "correction": None},
    }
    summaries = {}
    for branch, meta in branches.items():
        db_path = scratch / f"{branch}.db"
        shutil.copyfile(seed_image, db_path)
        with Server(binary, db_path, scratch / f"{branch}.log") as server:
            if meta["kind"] == "observation_fork":
                r_avg, r_sum = branch_with_correction(
                    server, meta["correction"], journal, branch)
            else:
                r_avg, r_sum = physical_only_branch(server, journal, branch)
            state = dict(collect_final_state(server, branch, journal))
            image_bytes, image_meta = server.image(f"export {branch} image")
        out_image = images / f"{branch}.db"
        out_image.write_bytes(image_bytes)
        image_meta["image"] = out_image.name
        images_meta.append(image_meta)
        summaries[branch] = {
            "meta": meta,
            "namespace": state[f"{branch} namespace"],
            "blocks": state[f"{branch} blocks"],
            "range": state[f"{branch} range"],
            "rollups": {
                "pulse-avg": state[f"{branch} rollup pulse-avg"],
                "pulse-sum": state[f"{branch} rollup pulse-sum"],
                "pulse-caliber": state[f"{branch} rollup pulse-caliber"],
            },
            "explain": state[f"{branch} explain"],
            "image": image_meta,
        }

    # Restore check: every exported image must re-open and answer identically.
    restore_checks = []
    for image in [seed_image, *(images / f"{b}.db" for b in branches)]:
        restored = scratch / f"restore-{image.name}"
        shutil.copyfile(image, restored)
        with Server(binary, restored, scratch / "restore.log") as server:
            out, _, _ = server.call(
                f"verify restore {image.name}", f"spaces/{SPACE}/range",
                {"start": -5000, "end": 10000})
        name = image.stem
        if name == "base-seed":
            expected = seed_raw
        else:
            expected = summaries[name]["range"]
        ok = out == expected
        restore_checks.append({"image": image.name, "range_matches_source": ok})
        if not ok:
            raise RuntimeError(f"restored range mismatch for {image.name}")

    evidence = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input": "examples/input.json",
        "input_sha256": hashlib.sha256(
            (ROOT / "examples" / "input.json").read_bytes()).hexdigest(),
        "namespace": ns,
        "queries": {"pulse-avg": COMMON_QUERY, "pulse-sum": SUM_QUERY,
                    "pulse-caliber": {"wide": WIDE_CALIBER_QUERY,
                                      "narrow": NARROW_CALIBER_QUERY}},
        "seed": {"namespace": ns, "blocks": seed_blocks, "range": seed_raw,
                 "image": images_meta[0]},
        "branches": summaries,
        "images": images_meta,
        "restore_checks": restore_checks,
        "api_journal": journal,
    }
    out = dist / "evidence.json"
    out.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", required=True,
                        help="empty working directory for temp databases/logs")
    parser.add_argument("--dist", required=True,
                        help="delivery directory receiving evidence and images")
    args = parser.parse_args()
    scratch = Path(args.scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    if any(scratch.iterdir()):
        raise SystemExit(f"scratch directory must be empty: {scratch}")
    evidence = run(scratch, Path(args.dist))
    print(json.dumps({
        "images": [(i["image"], i["sha256"][:16], i["sequence"])
                   for i in evidence["images"]],
        "branch_source_sequences": {
            b: evidence["branches"][b]["rollups"]["pulse-avg"]["source_sequence"]
            for b in evidence["branches"]},
    }, indent=2))


if __name__ == "__main__":
    sys.exit(main())
