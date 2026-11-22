#!/usr/bin/env bash
# Reproduce every float64-reduction experiment without touching production code.
# Requires Go 1.25+ (CGO is disabled so no C toolchain is needed).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/experiments/numerics/out"
mkdir -p "$OUT"

: "${GO:=go}"
export CGO_ENABLED=0

echo "== building experiment binaries =="
( cd "$ROOT" && "$GO" build -o /tmp/ridge-numerics   ./experiments/numerics )
( cd "$ROOT" && "$GO" build -o /tmp/ridge-e2e         ./experiments/numerics/e2e )
( cd "$ROOT" && "$GO" build -o /tmp/ridge-sweep       ./experiments/numerics/sweep )
( cd "$ROOT" && "$GO" build -o /tmp/ridge-roundtrip   ./experiments/numerics/roundtrip )

echo "== 1/4 strategy comparison and ordering/overflow probes (a few minutes) =="
/tmp/ridge-numerics --out "$OUT"

echo "== 2/4 end-to-end engine: rounding vs business-definition vs visibility =="
/tmp/ridge-e2e --out "$OUT"

echo "== 3/4 error-budget sweep and representability ceiling =="
/tmp/ridge-sweep "$OUT"

echo "== 4/4 storage round-trip bit-exactness =="
/tmp/ridge-roundtrip "$OUT"

echo
echo "reports written to $OUT:"
ls -1 "$OUT"
