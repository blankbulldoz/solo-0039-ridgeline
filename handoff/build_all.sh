#!/usr/bin/env bash
# One-shot rebuild of the whole handoff deliverable.
# Usage: GO_BIN=/path/to/go handoff/build_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

GO_BIN="${GO_BIN:-go}"
SCRATCH="handoff/var/rebuild"
DIST="handoff/dist"

rm -rf "$SCRATCH"
mkdir -p "$SCRATCH"
GO_BIN="$GO_BIN" python3 -I handoff/scenario.py --scratch "$SCRATCH" --dist "$DIST"
python3 -I handoff/build_relational.py --evidence "$DIST/evidence.json" --dist "$DIST"
python3 -I handoff/redact.py --dist "$DIST"
python3 -I handoff/verify.py "$DIST/relational/ridgeline_lineage.sqlite"
python3 -I handoff/verify.py "$DIST/relational/ridgeline_lineage_redacted.sqlite"
