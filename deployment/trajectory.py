#!/usr/bin/env python3
"""Seeded interleaved-arrival trajectory for the three-source ridgeline deployment.

Sources (see deployment/README.md for the full plan):
  fast  - 1s cadence, at most 2s late                -> edge_fast_<shard>
  slow  - 60s cadence, up to 10min store-and-forward -> edge_slow
  fix   - daily job re-sending the most recent hour  -> edge_slow (same series)

Usage:
  python3 deployment/trajectory.py plan --out deployment/out
      [--seed 42] [--devices 20] [--minutes 95]

Outputs written to --out:
  spaces.json    namespace policies, ready for POST /ridge/v1/spaces
  series.json    metric -> namespace attribution and label schema
  batches.jsonl  commit schedule: one write request per line, in arrival
                 order; each line is {"space", "now", "points"} and the
                 {"now","points"} body is directly POSTable to
                 /ridge/v1/spaces/{space}/points

The trajectory is a pure function of (seed, devices, minutes): the same
command always regenerates the same interleaved arrival schedule.
"""
from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

# --- clock and contract constants (milliseconds) ---------------------------
T0 = 1_700_000_000_000          # virtual anchor of the simulated day
SKEW_MS = 500                   # client "now" may lead server time by 500ms
FAST_LATE_MS = 2_000            # fast source contract: at most 2s late
SLOW_BACKLOG_MS = 600_000       # slow source: up to 10 minutes of backlog
FIX_WINDOW_MS = 3_600_000       # correction job re-sends the most recent hour
FIX_JOB_BUDGET_MS = 300_000     # ...and must finish within this budget
DAY_MS = 86_400_000             # 24h retention for every namespace

FAST_METRICS = ["cpu_util", "mem_util", "net_rx_bps", "net_tx_bps"]
SLOW_METRICS = ["temp_c", "humidity_pct", "power_w", "fan_rpm", "voltage_v",
                "current_a", "disk_iops", "disk_used_pct", "fan_duty_pct", "psu_w"]

# --- shard sizing (maintenance invariant, see README.md) --------------------
# compact()/Retain() run one full scan bounded by max_scan (<= 2_000_000), so
# live points per namespace must stay <= max_scan forever.  A 1s series holds
# 86_400 live points per 24h, hence a fast namespace holds at most
# floor(2_000_000 / 86_400) = 23 fast series; we run 20 and keep the rest as
# duplicate/growth headroom.
SHARD_DEVICES = 5
FAST_PER_SHARD = SHARD_DEVICES * len(FAST_METRICS)          # 20 series
MAX_SCAN_CAP = 2_000_000

# --- demo event anchors (virtual time) --------------------------------------
BURST_SECOND = 1_800            # fast tick held and merged into the next batch
BACKLOG_MINUTE = 30             # slow gateway holds minutes [30, 40)
FIX_MINUTE = 70                 # correction job starts at minute 70
SEAL_MINUTE = 65                # verify.py seals the namespaces here
SKEWED_DEVICE = 4               # d-004 stamps points 500ms ahead (fast client)


def fast_namespaces(devices: int) -> list[str]:
    return [f"edge_fast_{k}" for k in range(math.ceil(devices / SHARD_DEVICES))]


def spaces(devices: int) -> list[dict]:
    """Namespace policies for POST /ridge/v1/spaces (reference deployment)."""
    out = [{
        "name": name,
        "retention_ms": DAY_MS,
        # 2s lateness contract + 500ms cross-clock watermark slack.
        "lateness_ms": FAST_LATE_MS + SKEW_MS,            # 2_500
        # points may be stamped up to 500ms ahead of the caller's now.
        "future_ms": SKEW_MS,                             # 500
        # shard-full at 20 series + 2 strays; hard maintenance ceiling is 23.
        "max_series": FAST_PER_SHARD + 2,                 # 22
        # 20 points/s steady; a <=2s catch-up burst merges at most 3 ticks.
        "max_write": 100,
        # maintenance must decode 20 * 86_400 = 1_728_000 live points.
        "max_scan": MAX_SCAN_CAP,
    } for name in fast_namespaces(devices)]
    out.append({
        "name": "edge_slow",
        "retention_ms": DAY_MS,
        # correction window 1h + job runtime budget 5min + 500ms clock slack;
        # also covers the 10min slow backlog.  Finite on purpose.
        "lateness_ms": FIX_WINDOW_MS + FIX_JOB_BUDGET_MS + SKEW_MS,  # 3_900_500
        "future_ms": SKEW_MS,
        "max_series": 300,
        # one full 10-minute backlog flush (10 * 200 = 2_000) fits in a
        # single atomic request.
        "max_write": 2_500,
        # live set ~302k incl. correction duplicates; maintenance-safe to
        # ~660 series, which is above max_series.
        "max_scan": 1_000_000,
    })
    return out


def series_map(devices: int) -> dict:
    shards = fast_namespaces(devices)
    metrics = []
    for name in FAST_METRICS:
        metrics.append({
            "name": name, "cadence_ms": 1_000, "source": "fast",
            "namespaces": shards, "series_total": devices,
            "series_per_namespace": SHARD_DEVICES,
        })
    for name in SLOW_METRICS:
        metrics.append({
            "name": name, "cadence_ms": 60_000, "source": "slow+fix",
            "namespaces": ["edge_slow"], "series_total": devices,
            "series_per_namespace": devices,
        })
    return {
        "label_schema": {
            "required": ["__name__", "device", "site"],
            "identity": "series = (namespace, canonical label set); the fix "
                        "source must reuse the slow source's exact label set "
                        "or it would create a new series instead of correcting",
            "shard_rule": f"fast shard = device_index // {SHARD_DEVICES}",
        },
        "metrics": metrics,
    }


def labels(metric: str, device: int) -> dict:
    return {"__name__": metric, "device": f"d-{device:03d}",
            "site": "site-a" if device % 2 == 0 else "site-b"}


def value(metric_idx: int, device: int, t_ms: int) -> float:
    return round((metric_idx + 1) * 10 + device
                 + ((t_ms // 1000 + device * 7 + metric_idx * 13) % 100) / 10, 4)


def corrected(v: float) -> float:
    return round(v * 1.01, 4)


def fast_points(devices: int, shard: int, tick: int) -> list[dict]:
    lo, hi = shard * SHARD_DEVICES, min((shard + 1) * SHARD_DEVICES, devices)
    pts = []
    for d in range(lo, hi):
        # d-004's clock runs 500ms fast: it stamps points into the "future".
        ts = tick + (SKEW_MS if d == SKEWED_DEVICE and shard == 0 else 0)
        for mi, m in enumerate(FAST_METRICS):
            pts.append({"labels": labels(m, d), "time": ts,
                        "value": value(mi, d, tick)})
    return pts


def slow_points(devices: int, minute: int, fix: bool = False) -> list[dict]:
    tick = T0 + minute * 60_000
    pts = []
    for d in range(devices):
        for mi, m in enumerate(SLOW_METRICS):
            v = value(10 + mi, d, tick)
            pts.append({"labels": labels(m, d), "time": tick,
                        "value": corrected(v) if fix else v})
    return pts


def gen_batches(seed: int, devices: int, minutes: int) -> list[dict]:
    """Interleaved arrival schedule, deterministic for (seed, devices, minutes)."""
    rng = random.Random(seed)
    batches: list[tuple[int, int, dict]] = []
    order = 0

    def emit(arrival: int, space: str, points: list[dict]) -> None:
        nonlocal order
        now = arrival + rng.randint(0, SKEW_MS)   # client now may be 500ms fast
        batches.append((arrival, order,
                        {"space": space, "now": now, "points": points}))
        order += 1

    # fast: one request per shard per second, jitter 0..2000ms.
    for s in range(minutes * 60):
        tick = T0 + s * 1_000
        for k in range(math.ceil(devices / SHARD_DEVICES)):
            if k == 0 and s == BURST_SECOND:
                continue  # tick held by the collector, merged into the next batch
            ticks = [tick]
            jitter = rng.randint(0, FAST_LATE_MS)
            if k == 0 and s == BURST_SECOND + 1:
                ticks = [tick - 1_000, tick]      # catch-up burst, 2x points
                jitter = rng.randint(0, 900)      # still arrives before tick s+2
            pts = [p for t in ticks for p in fast_points(devices, k, t)]
            emit(tick + jitter, f"edge_fast_{k}", pts)

    # slow: one request per minute; minutes [BACKLOG_MINUTE, +10) are held by
    # the store-and-forward gateway and flushed in a single request.
    held: list[dict] = []
    for m in range(minutes):
        tick = T0 + m * 60_000
        if BACKLOG_MINUTE <= m < BACKLOG_MINUTE + 10:
            held.extend(slow_points(devices, m))
            continue
        if m == BACKLOG_MINUTE + 10 and held:
            emit(tick + rng.randint(0, SKEW_MS), "edge_slow", held)  # oldest first
            held = []
        emit(tick + rng.randint(0, 30_000), "edge_slow", slow_points(devices, m))

    # fix: daily job re-sends the most recent hour of the same slow series,
    # one request per minute-slice, oldest first, within the runtime budget.
    if minutes >= FIX_MINUTE + 25:
        job_start = T0 + FIX_MINUTE * 60_000 + 5_000
        for k in range(FIX_WINDOW_MS // 60_000):
            minute = FIX_MINUTE - 60 + k
            emit(job_start + k * 2_000, "edge_slow",
                 slow_points(devices, minute, fix=True))

    batches.sort(key=lambda b: (b[0], b[1]))
    return [b for _, _, b in batches]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=["plan"])
    ap.add_argument("--out", type=Path, default=Path("deployment/out"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--devices", type=int, default=20)
    ap.add_argument("--minutes", type=int, default=95,
                    help="simulated horizon; >=95 includes the correction job")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "spaces.json").write_text(json.dumps(spaces(args.devices), indent=2))
    (args.out / "series.json").write_text(json.dumps(series_map(args.devices), indent=2))
    batches = gen_batches(args.seed, args.devices, args.minutes)
    with (args.out / "batches.jsonl").open("w") as fh:
        for b in batches:
            fh.write(json.dumps(b) + "\n")

    per_space: dict[str, list[int]] = {}
    for b in batches:
        per_space.setdefault(b["space"], []).append(len(b["points"]))
    print(json.dumps({
        "seed": args.seed, "devices": args.devices, "minutes": args.minutes,
        "batches": len(batches),
        "points": sum(len(b["points"]) for b in batches),
        "per_space": {k: {"batches": len(v), "points": sum(v),
                          "max_batch": max(v)} for k, v in sorted(per_space.items())},
    }, indent=2))


if __name__ == "__main__":
    main()
