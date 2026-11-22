"""Read-only evidence capture before a retention boundary advance.

Use this against a *running* Ridgeline server when you need to preserve the
proof for the interval that retention is about to purge.  It performs only
read operations (GET, and the read-only POST bodies ``range``, ``aggregate``
and ``explain``); it never ingests, deletes, seals, compacts, changes
read-only mode or advances retention.

It writes a directory with:

  * ``capture.json``    - source state, exports, checks and verdicts
  * ``image.ridge.db``  - one consistent database image taken during the capture

The capture is accepted only if the namespace sequence is identical before
and after every read and the image sequence matches.  If something else
mutated the namespace in between, the run is marked inconsistent and the
operator must repeat it (or fall back to the image).

Exit status: 0 capture is consistent and the rollup verifies against raw;
2 capture is consistent but the rollup cannot be endorsed (drift, partial
purge or already-expired raw); 1 the capture failed or was inconsistent.

    python3 examples/handoff_capture.py \
        --base http://127.0.0.1:8399 --space archive --rollup handover \
        --purge-start -2000 --purge-end 1000 --output var/handoff-capture
"""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

SCHEMA = "ridgeline-capture/1"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    data = value if isinstance(value, bytes) else canonical(value).encode()
    return hashlib.sha256(data).hexdigest()


def cell_subset(cells, start, end):
    return sorted(
        [cell for cell in cells if start <= cell["start"] < end],
        key=lambda cell: (canonical(cell["labels"]), cell["start"]),
    )


class Client:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def get(self, route):
        request = urllib.request.Request(self.base + "/ridge/v1/" + route)
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read(), dict(response.headers)

    def get_json(self, route):
        payload, _headers = self.get(route)
        return json.loads(payload)

    def post(self, route, payload):
        request = urllib.request.Request(
            self.base + "/ridge/v1/" + route,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())


def capture(base, space, rollup_name, purge_start, purge_end, output):
    client = Client(base)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    capture_path = output / "capture.json"
    image_path = output / "image.ridge.db"
    if capture_path.exists() or image_path.exists():
        raise RuntimeError("{} already has a capture; use a fresh directory".format(output))

    state_before = client.get_json("spaces/" + space)
    image, image_headers = client.get("image")
    image_headers = {key.lower(): value for key, value in image_headers.items()}
    image_sha = hashlib.sha256(image).hexdigest()
    image_sequence = int(image_headers.get("x-ridge-sequence", "-1"))

    # A stored rollup is read with GET only; it is never re-materialized here.
    rollup = client.get_json("spaces/{}/rollups/{}".format(space, rollup_name))

    blocks = client.get_json("spaces/{}/blocks".format(space))
    tombstones = client.get_json("spaces/{}/deletions".format(space))
    plan = client.post("spaces/{}/explain".format(space),
                       {"start": -(1 << 52), "end": (1 << 52) + 1})

    query = dict(rollup["query"])
    purged_query = dict(query)
    purged_query["start"] = purge_start
    purged_query["end"] = purge_end

    purged_raw = client.post("spaces/{}/range".format(space),
                             {"start": purge_start, "end": purge_end})
    purged_aggregate = client.post("spaces/{}/aggregate".format(space), purged_query)
    full_aggregate = client.post("spaces/{}/aggregate".format(space), query)

    state_after = client.get_json("spaces/" + space)

    # Namespace and global sequences are different facts: materializing a
    # rollup advances the global sequence without moving last_sequence.  A
    # consistent capture therefore needs both stable namespace records *and*
    # one shared global sequence across the image and every read.
    export_sequences = {
        purged_raw["sequence"],
        purged_aggregate["sequence"],
        full_aggregate["sequence"],
        plan["sequence"],
        image_sequence,
    }
    consistent = (
        state_before["last_sequence"] == state_after["last_sequence"]
        and len(export_sequences) == 1
        and image_sequence >= state_after["last_sequence"]
    )

    stored_cells = rollup["result"]["cells"]
    stored_purged = cell_subset(stored_cells, purge_start, purge_end)
    recomputed_purged = cell_subset(purged_aggregate["cells"], purge_start, purge_end)
    full_matches = canonical(stored_cells) == canonical(full_aggregate["cells"])
    purged_matches = canonical(stored_purged) == canonical(recomputed_purged)
    floor = state_after["floor"]
    below_floor = floor > purge_start
    purged_has_raw = bool(purged_raw["series"])

    if not consistent:
        verdict = "inconsistent"
        endorsement = (
            "do not use this bundle; the namespace changed during the capture. "
            "Repeat against a quiescent namespace or use the image alone."
        )
    elif not purged_has_raw and stored_purged:
        verdict = "expired"
        endorsement = (
            "no raw point inside the purge interval survives at this source "
            "state. Retain the stored cells as the only record, but do not "
            "claim they were verified here: the interval cannot be recomputed."
        )
    elif not purged_has_raw:
        verdict = "empty-interval"
        endorsement = (
            "the rollup stored no cells inside the interval and raw has none. "
            "There is nothing to endorse for this interval."
        )
    elif not purged_matches and below_floor and purged_has_raw:
        verdict = "partial"
        endorsement = (
            "the floor cuts through the purge interval, so only part of its "
            "raw points survive. Retain the report; do not claim its purged "
            "segment was verified at this state."
        )
    elif not purged_matches:
        verdict = "drift"
        endorsement = (
            "raw inside the purge interval disagrees with the stored cells. "
            "Retain the old report as a historical record only; its purged "
            "segment cannot be endorsed."
        )
    elif not full_matches:
        verdict = "drift-purged-ok"
        endorsement = (
            "the stored purged segment still matches raw, but another window of "
            "the report does not. Retain the report as historical; endorse only "
            "the preserved segment."
        )
    else:
        verdict = "verified"
        endorsement = (
            "the stored report matches a full recomputation at the captured "
            "source state; the preserved segment is verified against raw."
        )

    bundle = {
        "schema": SCHEMA,
        "base": base,
        "space": space,
        "rollup": rollup_name,
        "purge_interval": {"start": purge_start, "end": purge_end},
        "consistent": consistent,
        "image": {
            "file": image_path.name,
            "bytes": len(image),
            "sha256": image_sha,
            "header_sha256": image_headers.get("x-ridge-sha256"),
            "header_matches_bytes": image_sha == image_headers.get("x-ridge-sha256"),
            "sequence": image_sequence,
        },
        "source_state": {
            "namespace_last_sequence": state_after["last_sequence"],
            "global_sequence": image_sequence,
            "floor": floor,
            "watermark": state_after["watermark"],
            "read_only": state_after["read_only"],
            "rollup_source_sequence": rollup["source_sequence"],
            "rollup_result_sequence": rollup["result"]["sequence"],
            "rollup_stale": rollup["stale"],
            "rollup_materialization_sha256": digest(
                {key: rollup[key] for key in
                 ("name", "query", "source_sequence", "result")}),
            "block_count": len(blocks),
            "head_records": plan["head_records"],
            "deletion_ids": [tomb["id"] for tomb in tombstones],
            "namespace_sequence_before_capture": state_before["last_sequence"],
            "namespace_sequence_after_capture": state_after["last_sequence"],
            "export_global_sequences": sorted(export_sequences),
            "max_scan": plan["max_decoded"],
        },
        "stored_rollup_cells": stored_cells,
        "stored_rollup_cells_sha256": digest(stored_cells),
        "purged_segment": {
            "raw": purged_raw,
            "aggregate": purged_aggregate,
            "stored_cells": stored_purged,
            "raw_sha256": digest(purged_raw),
            "aggregate_sha256": digest(purged_aggregate),
            "stored_cells_sha256": digest(stored_purged),
        },
        "checks": {
            "capture_consistent": consistent,
            "image_header_matches_bytes": image_sha == image_headers.get("x-ridge-sha256"),
            "rollup_level_with_namespace": (
                rollup["source_sequence"] == state_after["last_sequence"]
            ),
            "stale_flag": rollup["stale"],
            "stored_rollup_equals_full_recomputation": full_matches,
            "stored_purged_equals_recomputation": purged_matches,
            "purged_raw_present": purged_has_raw,
            "floor_inside_or_above_purge_interval": below_floor,
            "purged_raw_truncated": purged_raw["truncated"],
            "purged_aggregate_stats": purged_aggregate["stats"],
        },
        "verdict": verdict,
        "endorsement": endorsement,
    }

    image_path.write_bytes(image)
    capture_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n")
    return bundle, capture_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8399")
    parser.add_argument("--space", required=True)
    parser.add_argument("--rollup", required=True)
    parser.add_argument("--purge-start", type=int, required=True)
    parser.add_argument("--purge-end", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    bundle, path = capture(args.base, args.space, args.rollup,
                           args.purge_start, args.purge_end, args.output)
    print(json.dumps({
        "capture": str(path),
        "verdict": bundle["verdict"],
        "consistent": bundle["consistent"],
        "source_sequence": bundle["source_state"]["namespace_last_sequence"],
        "floor": bundle["source_state"]["floor"],
        "checks": bundle["checks"],
    }, indent=2, sort_keys=True))
    if not bundle["consistent"]:
        return 1
    if bundle["verdict"] == "verified":
        return 0
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"state": "failed", "detail": str(exc)}), file=sys.stderr)
        raise
