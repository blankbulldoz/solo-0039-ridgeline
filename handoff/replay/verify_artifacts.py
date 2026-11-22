#!/usr/bin/env python3
"""Verify the handoff batch artifacts against a fresh throwaway Ridgeline.

- 01-submit-source-a.json is POSTed byte-for-byte and must be accepted.
- 03-backfill-template-source-b.json is POSTed as stored and must be rejected
  (placeholder timestamps are intentionally non-numeric / non-submittable).
"""

import argparse
import json
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

BAT = Path(__file__).resolve().parents[1] / "batches"


def request(base, route, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + "/ridge/v1/" + route, data=body,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        with exc:
            raw = exc.read().decode()
            try:
                return exc.code, json.loads(raw)
            except json.JSONDecodeError:
                return exc.code, {"raw": raw}


def healthy(base):
    try:
        status, _ = request(base, "health")
        return status == 200
    except (OSError, urllib.error.HTTPError):
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="rl-verify-") as d:
        scratch = Path(d)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        log = open(scratch / "svc.log", "wb")
        proc = subprocess.Popen(
            [args.binary], env={"RIDGE_DB": str(scratch / "v.db"),
                                "RIDGE_ADDR": f"127.0.0.1:{port}"},
            stdout=log, stderr=log)
        base = f"http://127.0.0.1:{port}"
        try:
            for _ in range(120):
                if healthy(base):
                    break
                time.sleep(0.05)
            space = "device01"
            request(base, "spaces", {"name": space, "retention_ms": 600,
                                     "lateness_ms": 700, "future_ms": 100,
                                     "max_series": 10, "max_write": 1000,
                                     "max_scan": 100000})
            request(base, f"spaces/{space}/points",
                    {"now": 1000, "points": [
                        {"labels": {"__name__": "seed_marker", "device": "device01"},
                         "time": 1000, "value": 1}]})
            request(base, f"spaces/{space}/retention", {"now": 1000})

            # 1) The confirmed-a artifact must be accepted exactly as stored.
            artifact = json.loads((BAT / "01-submit-source-a.json").read_text())
            status, body = request(base, f"spaces/{space}/points", artifact)
            print("01-submit-source-a.json ->", status, body)
            assert status == 200 and body["accepted"] == 2 and body["watermark"] == 1600

            # 2) The backfill template as stored must NOT be submittable.
            template = json.loads((BAT / "03-backfill-template-source-b.json").read_text())
            status, body = request(base, f"spaces/{space}/points", template)
            print("03-backfill-template (as stored) ->", status, body)
            assert status == 400
            print("ARTIFACT CHECKS PASSED")
        finally:
            proc.terminate()
            proc.wait(timeout=15)


if __name__ == "__main__":
    main()
