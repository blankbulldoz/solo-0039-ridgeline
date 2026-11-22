#!/usr/bin/env sh
# Replay the Ridgeline failure-boundary drill.
#   ./drill/run-drill.sh            # local process backend (needs build/ridgeline)
#   ./drill/run-drill.sh compose    # docker compose backend
set -eu
cd "$(dirname "$0")/.."
BACKEND="${1:-local}"
if [ "$BACKEND" = "local" ] && [ ! -x build/ridgeline ]; then
  CGO_ENABLED=0 go build -o build/ridgeline ./cmd/ridgeline
fi
exec python3 -I drill/replay.py --backend "$BACKEND" --work var/drill
