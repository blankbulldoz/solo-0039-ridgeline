// Command roundtrip verifies the two persistence boundaries every reducer
// result crosses: JSON float64 encoding (head points, blocks, rollups) and
// the gzip+JSON block format (chunk.Encode/Decode). Both must preserve the
// exact IEEE-754 bit pattern, otherwise reproducibility would depend on
// storage layout rather than arithmetic.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
)

func main() {
	outDir := "experiments/numerics/out"
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	failures := 0

	// 1) Pure JSON round-trip across magnitudes, including subnormals.
	probe := []float64{
		0.1, 0.3, 1e15, 1e16, 1e308, 1e-308, math.MaxFloat64,
		math.SmallestNonzeroFloat64, 1.0 / 3.0, 123456789.123456789,
		-0.0, 9007199254740992.0, 9007199254740993.0,
	}
	for _, v := range probe {
		data, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		var back float64
		if err := json.Unmarshal(data, &back); err != nil {
			panic(err)
		}
		if math.Float64bits(back) != math.Float64bits(v) {
			failures++
			fmt.Printf("JSON mismatch: in=%g out=%g\n", v, back)
		}
	}

	// 2) Full block format round-trip (gzip + JSON + SHA-256 verification),
	//    which is exactly what seal/compact write and queries decode.
	points := make([]model.Sample, 0, len(probe))
	for i, v := range probe {
		points = append(points, model.Sample{Time: int64(i), Value: v, Sequence: 1})
	}
	payload, checksum, err := chunk.Encode(points)
	if err != nil {
		panic(err)
	}
	block := model.Block{
		ID: "probe", SeriesID: "s", Start: 0, End: int64(len(points)),
		Count: len(points), Checksum: checksum, Format: chunk.Format,
	}
	decoded, err := chunk.Decode(payload, block)
	if err != nil {
		panic(err)
	}
	for i, p := range decoded {
		if math.Float64bits(p.Value) != math.Float64bits(points[i].Value) {
			failures++
			fmt.Printf("block mismatch at %d: in=%g out=%g\n", i, points[i].Value, p.Value)
		}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}
	status := map[string]any{
		"json_roundtrip_bit_exact":  failures == 0,
		"block_roundtrip_bit_exact": len(decoded) == len(points) && failures == 0,
		"probe_count":               len(probe),
	}
	data, _ := json.MarshalIndent(status, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "roundtrip_report.json"), data, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("roundtrip: %v\n", status)
	if failures != 0 {
		os.Exit(1)
	}
}
