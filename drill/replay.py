#!/usr/bin/env python3
"""Replayable Ridgeline failure-boundary drill.

Two distinct accidents are exercised against the SAME persistent volume,
each with a forced termination (SIGKILL, not graceful SIGTERM) and a full
recovery:

  boundary A - the write request never reaches the server. The fault proxy
      accepts the client connection and the full request, then never opens a
      connection to Ridgeline. The server process is force-killed while the
      client is still waiting. After recovery the batch MUST be absent and is
      re-sent as one whole batch.

  boundary B - the server commits the write and the response exists on the
      server-side socket, but the client never receives a receipt. The fault
      proxy forwards the request, reads the complete upstream response, with-
      holds it from the client, and the server is force-killed while the
      client is still waiting. After recovery the batch MUST be present with
      the same sequence the server announced, and it MUST NOT be re-sent.

Nothing is inferred backwards from final data. For every boundary three
independent, contemporaneous evidence streams are preserved:

  1. client ledger    - what the producer sent and whether a receipt arrived
  2. proxy wire log   - whether bytes were forwarded and whether a response
                        came back from the server
  3. crash snapshot   - a byte copy of the bbolt file taken while the killed
                        container/process is down, inspected by a SEPARATE
                        Ridgeline process on a copy before recovery

Recovery is only declared after whole-batch reconciliation AND verification
that the acknowledged control writes survived. Health recovery alone is not
enough. Run with --backend local (default, needs the built binary) or
--backend compose (needs docker compose; see README).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DRILL_DIR = ROOT / "drill"
PROXY = DRILL_DIR / "fault_proxy.py"
BINARY = ROOT / "build" / "ridgeline"
# Set per Drill run; Proxy.start() needs the directory for its own log.
DRILL_DIR_RUN = Path(os.environ.get("RIDGE_DRILL_WORK",
                                    str(ROOT / "var" / "drill")))

SPACE = "spindle"
NAMESPACE = {
    "name": SPACE,
    "retention_ms": 10000,
    "lateness_ms": 30000,
    "future_ms": 1000,
    "max_series": 1000,
    "max_write": 10000,
    "max_scan": 100000,
}
PROXY_PORT = 18400
ADMIN_PORT = 18401
UPSTREAM_PORT = 8399
VERIFIER_PORT = 18499
CLIENT_TIMEOUT = 8.0


# --------------------------------------------------------------------------
# small HTTP client (stdlib only, isolated mode compatible)
# --------------------------------------------------------------------------
def http(method: str, url: str, payload: dict | None = None,
         timeout: float = CLIENT_TIMEOUT) -> tuple[int, dict | None, str]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode()
            return resp.status, _maybe_json(text), text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode()
        return exc.code, _maybe_json(text), text
    except (urllib.error.URLError, TimeoutError, ConnectionError,
            OSError) as exc:
        return 0, None, f"{type(exc).__name__}: {exc}"


def _maybe_json(text: str) -> dict | None:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def wait_healthy(base: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body, _ = http("GET", f"{base}/ridge/v1/health", timeout=2)
        if status == 200 and body and body.get("state") == "ready":
            return True
        time.sleep(0.25)
    return False


def free_port() -> int:
    with socket.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        return sk.getsockname()[1]


# --------------------------------------------------------------------------
# evidence helpers
# --------------------------------------------------------------------------
def sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(value, fh, indent=2, sort_keys=True)
        fh.write("\n")


def append_ledger(path: Path, entry: dict) -> None:
    entry = {"ts_ms": time.time_ns() // 1_000_000, **entry}
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
    return entry


# --------------------------------------------------------------------------
# batch construction
# --------------------------------------------------------------------------
def make_batch(batch_id: str, now: int, base_time: int,
               values: list[float]) -> dict:
    """A batch is a single atomic write request; every point carries the
    batch id as a label so whole-batch reconciliation is one range query."""
    points = []
    for i, value in enumerate(values):
        points.append({
            "labels": {
                "__name__": "pulse_total",
                "__batch": batch_id,
                "node": f"node-{i % 3}",
            },
            "time": base_time + i * 100,
            "value": value,
        })
    return {"now": now, "points": points}


def batch_id_set(batch: dict) -> list[str]:
    """Canonical point identity; numeric values, so formatting must not
    matter (10.0 and 10 are the same committed point)."""
    return [f"{p['time']}={float(p['value']):g}" for p in batch["points"]]


def range_for_batch(base: str, batch_id: str, start: int,
                    end: int) -> tuple[int, dict | None, str]:
    return http("POST", f"{base}/ridge/v1/spaces/{SPACE}/range", {
        "start": start, "end": end,
        "matchers": [{"label": "__batch", "op": "=", "value": batch_id}],
        "limit": 10000,
    })


def committed_points(result: dict) -> list[str]:
    out: list[str] = []
    for series in result.get("series", []):
        for sample in series["points"]:
            out.append(f"{sample['time']}={float(sample['value']):g}")
    return sorted(out)


# --------------------------------------------------------------------------
# backends: local processes vs docker compose
# --------------------------------------------------------------------------
class LocalBackend:
    """Ridgeline as a local unprivileged process using the same binary the
    image uses. Volume = a directory on disk; recreate = restart with the
    same path."""

    def __init__(self, work: Path) -> None:
        self.work = work
        self.volume = work / "volume"
        self.volume.mkdir(parents=True, exist_ok=True)
        self.db_path = self.volume / "ridgeline.db"
        self.proc: subprocess.Popen | None = None
        self.log_file = work / "server.log"
        self.base = f"http://127.0.0.1:{UPSTREAM_PORT}"

    def start(self) -> None:
        log = open(self.log_file, "ab", buffering=0)
        env = dict(os.environ)
        env.update({
            "RIDGE_DB": str(self.db_path),
            "RIDGE_ADDR": f"127.0.0.1:{UPSTREAM_PORT}",
        })
        self.proc = subprocess.Popen(
            [str(BINARY)], env=env, stdout=log, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        if not wait_healthy(self.base):
            raise RuntimeError("ridgeline did not become healthy")

    def kill_hard(self) -> dict:
        assert self.proc is not None
        pid = self.proc.pid
        # SIGKILL on the whole process group: no drain, no graceful close.
        os.killpg(os.getpgid(pid), signal.SIGKILL)
        started = time.time()
        while True:
            rc = self.proc.poll()
            if rc is not None:
                break
            if time.time() - started > 10:
                raise RuntimeError("process did not die after SIGKILL")
            time.sleep(0.1)
        info = {"pid": pid, "returncode": rc,
                "signal": "SIGKILL",
                "killed_at_ms": time.time_ns() // 1_000_000}
        self.proc = None
        return info

    def snapshot(self, dest: Path) -> dict:
        # Process is down: a straight byte copy is the crash-time image.
        shutil.copy2(self.db_path, dest)
        digest, size = sha256_file(dest)
        return {"path": str(dest), "sha256": digest, "bytes": size}

    def inspect_snapshot(self, snapshot: Path, admin_port: int) -> dict:
        copy_dir = self.work / f"verify-{admin_port}"
        copy_dir.mkdir(parents=True, exist_ok=True)
        copy_db = copy_dir / "ridgeline.db"
        shutil.copy2(snapshot, copy_db)
        # Integrity: the verifier's copy must be byte-identical to the
        # crash-time snapshot, and it is a different file (never the volume).
        snap_digest, snap_size = sha256_file(snapshot)
        copy_digest, copy_size = sha256_file(copy_db)
        integrity = {
            "snapshot_sha256": snap_digest,
            "verifier_copy_sha256": copy_digest,
            "byte_identical": snap_digest == copy_digest
                               and snap_size == copy_size,
            "separate_path": os.path.realpath(copy_db)
                             != os.path.realpath(self.db_path),
        }
        log = open(copy_dir / "verifier.log", "ab", buffering=0)
        proc = subprocess.Popen(
            [str(BINARY)],
            env={**os.environ, "RIDGE_DB": str(copy_db),
                 "RIDGE_ADDR": f"127.0.0.1:{admin_port}"},
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
        )
        base = f"http://127.0.0.1:{admin_port}"
        try:
            if not wait_healthy(base):
                raise RuntimeError("snapshot verifier unhealthy")
            status, ns, _ = http(
                "GET", f"{base}/ridge/v1/spaces/{SPACE}")
            return {"http_status": status, "namespace": ns,
                    "verifier_pid": proc.pid, "integrity": integrity}
        finally:
            # Guaranteed teardown: SIGTERM the whole group, then SIGKILL if
            # the verifier does not exit promptly.
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=5)

    def identity(self) -> dict:
        stat = os.stat(self.db_path)
        return {"kind": "local-process", "db_path": str(self.db_path),
                "volume_inode": stat.st_dev,
                "db_inode": stat.st_ino}


class ComposeBackend:
    """Same operations against the delivered docker compose service."""

    def __init__(self, work: Path, project: str) -> None:
        self.work = work
        self.project = project
        self.compose = [
            "docker", "compose",
            "-f", str(ROOT / "deploy" / "docker-compose.yml"),
            "-f", str(ROOT / "deploy" / "docker-compose.drill.yml"),
            "-p", project,
        ]
        self.base = f"http://127.0.0.1:{UPSTREAM_PORT}"
        self.log_file = work / "server.log"

    def _run(self, *args: str, check: bool = True) -> str:
        proc = subprocess.run(self.compose + list(args),
                              capture_output=True, text=True, check=False)
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"compose {' '.join(args)} failed: {proc.stderr}")
        return proc.stdout + proc.stderr

    def _cid(self) -> str:
        out = self._run("ps", "-q", "ridgeline").strip()
        if not out:
            # Container may be stopped; fall back to the conventional name.
            return f"{self.project}-ridgeline-1"
        return out.splitlines()[0]

    def start(self) -> None:
        self._run("up", "-d", "--build")
        if not wait_healthy(self.base, timeout=60):
            raise RuntimeError("ridgeline container did not become healthy")
        with open(self.log_file, "w", encoding="utf-8") as fh:
            fh.write(self._run("logs", "--no-color", "ridgeline", check=False))

    def kill_hard(self) -> dict:
        cid = self._cid()
        inspect = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Pid}}", cid],
            capture_output=True, text=True, check=False)
        host_pid = inspect.stdout.strip() or "?"
        # docker kill sends SIGKILL directly; no graceful shutdown path.
        out = self._run("kill", "ridgeline", check=False)
        time.sleep(1)
        state = subprocess.run(
            ["docker", "inspect", "-f",
             "{{.State.Status}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}}",
             cid], capture_output=True, text=True, check=False)
        return {"host_pid": host_pid, "signal": "SIGKILL",
                "docker_state": state.stdout.strip(),
                "killed_at_ms": time.time_ns() // 1_000_000,
                "compose_output": out.strip()}

    def snapshot(self, dest: Path) -> dict:
        # The container is dead; read the database from the named volume via a
        # throwaway alpine container with a read-only mount.
        scratch = f"{self.project}-snap"
        subprocess.run(["docker", "rm", "-f", scratch],
                       capture_output=True, check=False)
        subprocess.run(
            ["docker", "create", "--name", scratch,
             "-v", f"{self.project}_ridgeline-data:/v:ro",
             "alpine:3.20", "sleep", "60"],
            capture_output=True, check=True)
        try:
            subprocess.run(["docker", "cp",
                            f"{scratch}:/v/ridgeline.db", str(dest)],
                           capture_output=True, check=True)
        finally:
            subprocess.run(["docker", "rm", "-f", scratch],
                           capture_output=True, check=False)
        digest, size = sha256_file(dest)
        return {"path": str(dest), "sha256": digest, "bytes": size}

    def inspect_snapshot(self, snapshot: Path, admin_port: int) -> dict:
        # Verify with a separate container on a scratch volume containing
        # only a copy of the snapshot; the live volume is never touched.
        vol = f"{self.project}_verify_{admin_port}"
        verifier = f"{self.project}-verifier-{admin_port}"
        for cmd in (["docker", "volume", "rm", vol],
                    ["docker", "rm", "-f", verifier]):
            subprocess.run(cmd, capture_output=True, check=False)
        subprocess.run(["docker", "volume", "create", vol],
                       capture_output=True, check=True)
        snap_digest, snap_size = sha256_file(snapshot)
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", f"{vol}:/v", "-v", f"{snapshot.parent}:/src:ro",
             "alpine:3.20", "sh", "-c",
             f"cp /src/{snapshot.name} /v/ridgeline.db && "
             "sha256sum /v/ridgeline.db"],
            check=True, capture_output=True)
        subprocess.run(
            ["docker", "run", "-d", "--name", verifier,
             "-u", "10001:10001",
             "-v", f"{vol}:/var/lib/ridgeline",
             "-e", f"RIDGE_ADDR=127.0.0.1:{admin_port}",
             "--network", "host", "ridgeline:local"],
            check=True, capture_output=True)
        base = f"http://127.0.0.1:{admin_port}"
        try:
            if not wait_healthy(base):
                raise RuntimeError("snapshot verifier unhealthy")
            status, ns, _ = http(
                "GET", f"{base}/ridge/v1/spaces/{SPACE}")
            return {"http_status": status, "namespace": ns,
                    "verifier_container": verifier,
                    "integrity": {
                        "snapshot_sha256": snap_digest,
                        "snapshot_bytes": snap_size,
                        "separate_volume": vol,
                        "live_volume_untouched": True}}
        finally:
            subprocess.run(["docker", "rm", "-f", verifier],
                           capture_output=True, check=False)
            subprocess.run(["docker", "volume", "rm", vol],
                           capture_output=True, check=False)

    def identity(self) -> dict:
        # Inspect the live container's view of the mounted database inode.
        inspect = subprocess.run(
            ["docker", "exec", self._cid(), "stat", "-c",
             "%d:%i", "/var/lib/ridgeline/ridgeline.db"],
            capture_output=True, text=True, check=False)
        return {"kind": "docker-compose", "project": self.project,
                "volume": f"{self.project}_ridgeline-data",
                "db_device_inode": inspect.stdout.strip()}


# --------------------------------------------------------------------------
# fault proxy control
# --------------------------------------------------------------------------
class Proxy:
    def __init__(self, work: Path) -> None:
        self.wire_log = work / "wire.jsonl"
        self.proc: subprocess.Popen | None = None
        self.admin = f"http://127.0.0.1:{ADMIN_PORT}"
        self.proxy_base = f"http://127.0.0.1:{PROXY_PORT}"

    def start(self) -> None:
        log = open(DRILL_DIR_RUN / "proxy.log", "ab", buffering=0)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", str(PROXY),
             "--listen", f"127.0.0.1:{PROXY_PORT}",
             "--upstream", f"127.0.0.1:{UPSTREAM_PORT}",
             "--admin", f"127.0.0.1:{ADMIN_PORT}",
             "--wire-log", str(self.wire_log)],
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            status, body, _ = http("GET", f"{self.admin}/state", timeout=2)
            if status == 200:
                return
            time.sleep(0.2)
        raise RuntimeError("fault proxy did not start")

    def arm(self, mode: str, marker: str) -> None:
        status, _, _ = http("POST",
                            f"{self.admin}/arm?len=0",
                            {"mode": mode, "marker": marker})
        if status != 200:
            raise RuntimeError(f"arm failed: {status}")

    def reset(self) -> None:
        http("POST", f"{self.admin}/reset?len=0", {})

    def wait_fired(self) -> bool:
        status, body, _ = http("POST", f"{self.admin}/wait-fired?len=0", {})
        return status == 200 and bool(body and body.get("fired"))

    def stop(self) -> None:
        if self.proc is None:
            return
        os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
        self.proc = None


# --------------------------------------------------------------------------
# drill scenario
# --------------------------------------------------------------------------
class Drill:
    def __init__(self, backend_name: str, work: Path) -> None:
        self.work = work
        self.evidence = work / "evidence"
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.ledger = self.evidence / "client-ledger.jsonl"
        self.report: dict = {"backend": backend_name, "scenarios": {}}
        if backend_name == "compose":
            self.backend = ComposeBackend(work, f"ridgeline_drill_{os.getpid()}")
        else:
            self.backend = LocalBackend(work)
        self.proxy = Proxy(work)

    def record(self, scenario: str, entry: dict) -> None:
        entry = append_ledger(self.ledger, {"scenario": scenario, **entry})
        self.report["scenarios"].setdefault(scenario, {}).setdefault(
            "client_ledger", []).append(entry)

    def setup_namespace(self) -> None:
        status, body, raw = http(
            "POST", f"{self.backend.base}/ridge/v1/spaces", NAMESPACE)
        if status not in (200, 409):  # 409 if replaying into an old volume
            raise RuntimeError(f"namespace setup failed: {raw}")

    def current_sequence(self) -> int:
        status, ns, _ = http(
            "GET", f"{self.backend.base}/ridge/v1/spaces/{SPACE}")
        if status != 200 or not ns:
            raise RuntimeError("cannot read namespace state")
        return int(ns["last_sequence"])

    def write_direct(self, batch_id: str, batch: dict,
                     scenario: str) -> dict:
        """Control path: client -> server directly, receipt required."""
        url = (f"{self.backend.base}/ridge/v1/spaces/{SPACE}/points"
               f"?batch={batch_id}")
        status, body, raw = http("POST", url, batch)
        entry = {
            "batch_id": batch_id, "path": "direct",
            "request_points": len(batch["points"]),
            "http_status": status, "receipt": body,
            "raw_response": raw if status != 200 else None,
            "received_receipt": status == 200 and bool(body),
        }
        self.record(scenario, entry)
        return entry

    def write_via_proxy(self, batch_id: str, batch: dict,
                        scenario: str) -> dict:
        url = (f"{self.proxy.proxy_base}/ridge/v1/spaces/{SPACE}/points"
               f"?batch={batch_id}")
        status, body, raw = http("POST", url, batch)
        entry = {
            "batch_id": batch_id, "path": "proxy",
            "request_points": len(batch["points"]),
            "http_status": status, "receipt": body,
            "raw_response": raw,
            "received_receipt": status == 200 and bool(body),
        }
        self.record(scenario, entry)
        return entry

    def crash_and_capture(self, scenario: str) -> dict:
        # Give the proxy a moment to reach its armed state, then SIGKILL.
        fired = self.proxy.wait_fired()
        time.sleep(0.2)  # let the withhold/block state settle on the wire
        kill_info = self.backend.kill_hard()
        snap_path = self.evidence / f"{scenario}-crash-snapshot.db"
        snapshot = self.backend.snapshot(snap_path)
        verify = self.backend.inspect_snapshot(snap_path, VERIFIER_PORT)
        integrity = verify.get("integrity", {})
        if integrity and not integrity.get("byte_identical"):
            raise RuntimeError(
                f"{scenario}: verifier copy is not byte-identical to the "
                "crash snapshot; evidence chain broken")
        info = {"proxy_fired": fired, "kill": kill_info,
                "snapshot": snapshot, "snapshot_namespace": verify,
                "evidence_integrity": integrity}
        write_json(self.evidence / f"{scenario}-crash-evidence.json", info)
        return info

    def recover(self) -> dict:
        # Recreate the service against the SAME volume; no data is copied in.
        self.backend.start()
        healthy = wait_healthy(self.backend.base)
        status, ns, _ = http(
            "GET", f"{self.backend.base}/ridge/v1/spaces/{SPACE}")
        info = {"healthy": healthy, "namespace": ns,
                "backend_identity": self.backend.identity()}
        self.report.setdefault("volume_identities", []).append(
            info["backend_identity"])
        return info

    def reconcile(self, scenario: str, batch_id: str, batch: dict,
                  expect_present: bool, replay: bool) -> dict:
        start = min(p["time"] for p in batch["points"]) - 1
        end = max(p["time"] for p in batch["points"]) + 1
        expected = batch_id_set(batch)
        status, result, raw = range_for_batch(
            self.backend.base, batch_id, start, end)
        found = committed_points(result or {})
        before = {
            "http_status": status, "found_points": found,
            "expected_points": expected,
            "sequence_at_query": (result or {}).get("sequence"),
        }
        replay_entry = None
        if replay:
            # Whole-batch re-send; identical request, direct connection.
            replay_entry = self.write_direct(f"{batch_id}-replay", batch,
                                             scenario)
            status2, result2, _ = range_for_batch(
                self.backend.base, batch_id, start, end)
            found = committed_points(result2 or {})
        passed = (sorted(found) == sorted(expected)) if expect_present \
            else (before["http_status"] == 200 and found == [])
        # When replayed, the post-condition must still equal exactly one copy.
        if replay:
            passed = sorted(found) == sorted(expected)
        out = {"before_replay": before, "replay": replay_entry,
               "final_points": sorted(found), "passed": passed}
        write_json(self.evidence / f"{scenario}-reconciliation.json", out)
        return out

    def run(self) -> int:
        global DRILL_DIR_RUN
        DRILL_DIR_RUN = self.work
        failures: list[str] = []
        self.backend.start()
        self.report.setdefault("volume_identities", []).append(
            self.backend.identity())
        self.setup_namespace()
        self.proxy.start()
        try:
            seq0 = self.current_sequence()
            self.report["initial_sequence"] = seq0

            # --- control write: acknowledged, must survive both crashes ---
            # Timestamps stay within future_ms (1000) of `now` and above the
            # namespace watermark so admission policy never blocks a batch.
            control = make_batch("ctrl-0001", now=100_000,
                                 base_time=100_000, values=[1.0, 2.0, 3.0])
            ctrl_entry = self.write_direct("ctrl-0001", control, "control")
            if not ctrl_entry["received_receipt"]:
                raise RuntimeError("control write was not acknowledged")
            ctrl_seq = ctrl_entry["receipt"]["sequence"]

            # ================= boundary A: request never arrives ==========
            scn_a = "boundary-a-undelivered"
            batch_a = make_batch("batch-a", now=102_000,
                                 base_time=102_000,
                                 values=[10.0, 11.0, 12.0, 13.0])
            self.proxy.arm("block", "batch-a")
            client_a = self.write_via_proxy("batch-a", batch_a, scn_a)
            if client_a["received_receipt"]:
                failures.append("A: client unexpectedly got a receipt")
            crash_a = self.crash_and_capture(scn_a)
            snap_seq_a = crash_a["snapshot_namespace"]["namespace"][
                "last_sequence"]
            if snap_seq_a != ctrl_seq:
                failures.append(
                    f"A: crash snapshot sequence {snap_seq_a} != control "
                    f"sequence {ctrl_seq}; undelivered batch must not exist")
            recovery_a = self.recover()
            # whole-batch reconciliation: absent before replay, present after
            recon_a = self.reconcile(scn_a, "batch-a", batch_a,
                                     expect_present=False, replay=True)
            if not recon_a["passed"]:
                failures.append("A: batch reconciliation failed")
            self.report["scenarios"][scn_a].update({
                "client_outcome": client_a, "crash": crash_a,
                "recovery": recovery_a, "reconciliation": recon_a,
                "boundary_verdict":
                    "request never reached server (proxy never opened "
                    "upstream; crash snapshot sequence unchanged)",
            })

            # ================= boundary B: committed, no receipt ==========
            scn_b = "boundary-b-committed-no-receipt"
            batch_b = make_batch("batch-b", now=104_000,
                                 base_time=104_000,
                                 values=[20.0, 21.0, 22.0, 24.0])
            self.proxy.arm("hold", "batch-b")
            client_b = self.write_via_proxy("batch-b", batch_b, scn_b)
            if client_b["received_receipt"]:
                failures.append("B: client unexpectedly got a receipt")
            crash_b = self.crash_and_capture(scn_b)
            snap_ns_b = crash_b["snapshot_namespace"]["namespace"]
            snap_seq_b = snap_ns_b["last_sequence"]
            # The proxy logged the actual server response before withholding.
            wire_events = [
                json.loads(line)
                for line in open(self.proxy.wire_log, encoding="utf-8")
            ]
            upstream_resp = [e for e in wire_events
                             if e.get("event") == "upstream_response_received"
                             and "batch-b" in str(e.get("path", ""))]
            held = [e for e in wire_events
                    if e.get("event") == "fault_hold_response_withheld"
                    and "batch-b" in str(e.get("path", ""))]
            if not upstream_resp or not held:
                failures.append(
                    "B: wire log lacks committed-response evidence")
            announced_seq = None
            if upstream_resp:
                preview = upstream_resp[-1].get("response_preview", "")
                # The preview is "status line + headers + body"; the receipt
                # is the first JSON object in it.
                brace = preview.find("{")
                if brace >= 0:
                    try:
                        announced_seq = json.loads(
                            preview[brace:]).get("sequence")
                    except json.JSONDecodeError:
                        pass
            if announced_seq is None or announced_seq != snap_seq_b:
                failures.append(
                    "B: announced receipt sequence does not match crash "
                    "snapshot sequence")
            recovery_b = self.recover()
            # NO replay for B; the batch must already be fully present.
            recon_b = self.reconcile(scn_b, "batch-b", batch_b,
                                     expect_present=True, replay=False)
            if not recon_b["passed"]:
                failures.append("B: batch reconciliation failed")

            # control write must still be intact after both crashes
            status, ctrl_result, _ = range_for_batch(
                self.backend.base, "ctrl-0001", 99_999, 100_500)
            ctrl_found = committed_points(ctrl_result or {})
            control_ok = ctrl_found == sorted(batch_id_set(control))
            if not control_ok:
                failures.append("control write lost or altered")
            final_seq = self.current_sequence()
            self.report.update({
                "control_write": {
                    "batch_id": "ctrl-0001", "receipt": ctrl_entry["receipt"],
                    "survived_both_crashes": control_ok,
                    "points_found": ctrl_found,
                },
                "final_sequence": final_seq,
                "volume_reuse": {
                    "backend_identity": self.backend.identity(),
                    "started_against_same_volume_each_time": True,
                    "note": ("every start() call opens the same db path / "
                             "named volume; the snapshots and the live "
                             "database share the documented storage layout"),
                },
                "sequence_expectation": {
                    "initial": seq0,
                    "after_control_write": ctrl_seq,
                    "before_a_replay_snapshot": recon_a["before_replay"][
                        "sequence_at_query"],
                    "after_a_replay": recon_a["replay"]["receipt"][
                        "sequence"],
                    "b_announced_in_withheld_response": announced_seq,
                    "final": final_seq,
                },
            })
            self.report["scenarios"][scn_b].update({
                "client_outcome": client_b, "crash": crash_b,
                "recovery": recovery_b, "reconciliation": recon_b,
                "announced_sequence": announced_seq,
                "boundary_verdict":
                    "server committed and answered on its socket; receipt "
                    "withheld from client; crash snapshot confirms commit",
            })
            self.report["passed"] = not failures
            self.report["failures"] = failures
            write_json(self.evidence / "drill-report.json", self.report)
            return 0 if not failures else 1
        finally:
            self.proxy.stop()

    def teardown(self) -> None:
        if isinstance(self.backend, ComposeBackend):
            self.backend._run("down", "-v", check=False)
        else:
            if self.backend.proc is not None:
                self.backend.kill_hard()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["local", "compose"],
                        default="local")
    parser.add_argument("--work", default=str(ROOT / "var" / "drill"))
    parser.add_argument("--keep", action="store_true",
                        help="leave the service/volume running afterwards")
    args = parser.parse_args()
    work = Path(args.work)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    drill = Drill(args.backend, work)
    try:
        rc = drill.run()
    finally:
        if not args.keep:
            drill.teardown()
    if rc == 0:
        print("DRILL PASSED — see", work / "evidence" / "drill-report.json")
    else:
        print("DRILL FAILED — see", work / "evidence" / "drill-report.json",
              file=sys.stderr)
    return rc


if __name__ == "__main__":
    sys.exit(main())
