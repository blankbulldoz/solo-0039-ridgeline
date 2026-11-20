package histogram

import (
	"math"
	"testing"

	"ridgeline/internal/model"
)

func schemeA() []float64 {
	return []float64{10, 25, 50, 100}
}

func schemeB() []float64 {
	return []float64{20, 40, 80, 160}
}

func snapshot(t int64, seq uint64, bounds, values []float64, counts []uint64, sum float64) model.HistogramSample {
	if values == nil {
		values = bounds
	}
	return model.HistogramSample{Time: t, Bounds: bounds, Counts: counts, Sum: sum, Sequence: seq}
}

func seriesOf(id string, points ...model.HistogramSample) model.SeriesHistograms {
	return model.SeriesHistograms{Series: model.Series{ID: id, Labels: model.Labels{"__name__": "latency_ms", "node": id}}, Points: points}
}

func TestReduceSeriesMergesIdenticalLayoutsAndDetectsReset(t *testing.T) {
	// Cumulative snapshots: three observations, the third is a process
	// restart whose cumulative counts are smaller than the second.
	points := []model.HistogramSample{
		snapshot(1000, 1, schemeA(), nil, []uint64{10, 40, 80, 100}, 3000),
		snapshot(2000, 2, schemeA(), nil, []uint64{20, 70, 140, 200}, 7000),
		snapshot(3000, 3, schemeA(), nil, []uint64{5, 25, 40, 50}, 1500),
	}
	segments, err := ReduceSeries(points)
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("expected one layout segment, got %d", len(segments))
	}
	seg := segments[0]
	// Increase 1->2: [10,30,60,100]; after reset the whole snapshot 3 counts.
	want := []uint64{15, 55, 100, 150}
	for i := range want {
		if seg.Counts[i] != want[i] {
			t.Fatalf("bucket %d: got %d want %d", i, seg.Counts[i], want[i])
		}
	}
	if seg.Resets != 1 {
		t.Fatalf("expected one reset, got %d", seg.Resets)
	}
	if seg.Sum != (7000-3000)+1500 {
		t.Fatalf("reset-aware sum mismatch: %v", seg.Sum)
	}
	if seg.Fingerprint != Fingerprint(schemeA()) {
		t.Fatal("fingerprint does not match layout")
	}
}

func TestLayoutChangeOpensNewSegment(t *testing.T) {
	points := []model.HistogramSample{
		snapshot(1000, 1, schemeA(), nil, []uint64{1, 2, 3, 4}, 100),
		snapshot(2000, 2, schemeB(), nil, []uint64{1, 2, 3, 4}, 100),
	}
	segments, err := ReduceSeries(points)
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if len(segments) != 2 {
		t.Fatalf("expected two segments for distinct layouts, got %d", len(segments))
	}
	if segments[0].Fingerprint == segments[1].Fingerprint {
		t.Fatal("distinct layouts must have distinct fingerprints")
	}
	// Layout change is not a counter reset.
	if segments[0].Resets != 0 || segments[1].Resets != 0 {
		t.Fatal("layout change must not be counted as a reset")
	}
}

func TestAggregateCompatibleInstancesEstimateQuantiles(t *testing.T) {
	// Two instances emit the same distribution through identical buckets.
	query := model.HistogramQuery{
		Start: 0, End: 10000, Step: 10000, Origin: 0,
		Quantiles: []float64{0.5}, GroupBy: []string{},
	}
	mk := func(id string, shift int64) model.SeriesHistograms {
		return seriesOf(id,
			snapshot(1000+shift, 1, schemeA(), nil, []uint64{10, 40, 80, 100}, 3000),
			snapshot(2000+shift, 2, schemeA(), nil, []uint64{20, 70, 140, 200}, 7000),
		)
	}
	cells, err := Aggregate([]model.SeriesHistograms{mk("a", 0), mk("b", 100)}, query)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(cells) != 1 {
		t.Fatalf("expected one window cell, got %d", len(cells))
	}
	cell := cells[0]
	if !cell.Compatible {
		t.Fatal("identical layouts must be compatible")
	}
	// Each instance contributes increase [10,30,60,100]; merged doubles it.
	want := []uint64{20, 60, 120, 200}
	for i := range want {
		if cell.Counts[i] != want[i] {
			t.Fatalf("merged bucket %d: got %d want %d", i, cell.Counts[i], want[i])
		}
	}
	if cell.Count != 200 || cell.Series != 2 {
		t.Fatalf("unexpected cell totals: %+v", cell)
	}
	if len(cell.Quantiles) != 1 || cell.Quantiles[0].Value == nil {
		t.Fatalf("missing quantile estimate: %+v", cell.Quantiles)
	}
	// Rank 100 falls in the (25,50] bucket, merged cum 60..120, fraction
	// (100-60)/60 = 2/3 -> 25 + 2/3*25.
	wantQ := 25.0 + (100.0-60.0)/60.0*25.0
	if math.Abs(*cell.Quantiles[0].Value-wantQ) > 1e-9 {
		t.Fatalf("p50 estimate got %v want %v", *cell.Quantiles[0].Value, wantQ)
	}
	for _, code := range []string{LimitationResolution, LimitationBaseline} {
		if !contains(cell.Limitations, code) {
			t.Fatalf("missing limitation %q in %v", code, cell.Limitations)
		}
	}
}

func TestAggregateIncompatibleLayoutsAreNeverSummed(t *testing.T) {
	query := model.HistogramQuery{
		Start: 0, End: 10000, Step: 10000, Origin: 0,
		Quantiles: []float64{0.5},
	}
	// Both instances describe the same observations but with different
	// bounds. A second snapshot per series supplies a positive increase so
	// the partial scheme totals reflect window activity rather than baseline.
	a := seriesOf("a",
		snapshot(1000, 1, schemeA(), nil, []uint64{10, 40, 80, 100}, 3000),
		snapshot(2000, 2, schemeA(), nil, []uint64{12, 44, 84, 104}, 3200),
	)
	b := seriesOf("b",
		snapshot(1000, 1, schemeB(), nil, []uint64{8, 45, 75, 100}, 3000),
		snapshot(2000, 2, schemeB(), nil, []uint64{9, 48, 79, 105}, 3300),
	)
	cells, err := Aggregate([]model.SeriesHistograms{a, b}, query)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	cell := cells[0]
	if cell.Compatible {
		t.Fatal("different bucket layouts must be incompatible")
	}
	if len(cell.Schemes) != 2 {
		t.Fatalf("expected two partial schemes, got %d", len(cell.Schemes))
	}
	if cell.Counts != nil || len(cell.Quantiles) != 0 {
		t.Fatal("incompatible cell must not present merged counts or estimates")
	}
	if !contains(cell.Limitations, LimitationIncompatible) {
		t.Fatalf("missing incompatibility limitation: %v", cell.Limitations)
	}
	// Partials preserve each scheme's own per-window increase.
	totals := map[string]uint64{}
	for _, partial := range cell.Schemes {
		totals[partial.Fingerprint] = partial.Count
	}
	if totals[Fingerprint(schemeA())] != 4 || totals[Fingerprint(schemeB())] != 5 {
		t.Fatalf("partial totals corrupted: %v", totals)
	}
}

func TestResetSequenceCarriesResetLimitationAndIncrease(t *testing.T) {
	query := model.HistogramQuery{
		Start: 0, End: 10000, Step: 10000, Origin: 0,
		Quantiles: []float64{0.5},
	}
	s := seriesOf("a",
		snapshot(1000, 1, schemeA(), nil, []uint64{10, 40, 80, 100}, 3000),
		snapshot(2000, 2, schemeA(), nil, []uint64{20, 70, 140, 200}, 7000),
		snapshot(3000, 3, schemeA(), nil, []uint64{5, 25, 40, 50}, 1500),
	)
	cells, err := Aggregate([]model.SeriesHistograms{s}, query)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	cell := cells[0]
	if cell.Resets != 1 {
		t.Fatalf("expected one reset, got %d", cell.Resets)
	}
	if !contains(cell.Limitations, LimitationReset) {
		t.Fatalf("missing reset limitation: %v", cell.Limitations)
	}
	want := []uint64{15, 55, 100, 150}
	for i := range want {
		if cell.Counts[i] != want[i] {
			t.Fatalf("bucket %d: got %d want %d", i, cell.Counts[i], want[i])
		}
	}
	// Rank 75 lands in (25,50]: cumulative 55..100, fraction (75-55)/45.
	got := *cell.Quantiles[0].Value
	wantQ := 25 + (75.0-55.0)/45.0*25
	if math.Abs(got-wantQ) > 1e-9 {
		t.Fatalf("p50 got %v want %v", got, wantQ)
	}
}

func TestQuantileLowEdgeIsCappedAtFirstBound(t *testing.T) {
	// All observations sit in the open-ended first bucket.
	estimates, capped, err := Estimate(schemeA(), []uint64{5, 5, 5, 5}, []float64{0.01, 0.5})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if !capped {
		t.Fatal("low-edge estimates must be flagged as capped")
	}
	for _, estimate := range estimates {
		if estimate.Value == nil || *estimate.Value != schemeA()[0] || !estimate.Capped {
			t.Fatalf("unexpected low-edge estimate: %+v", estimate)
		}
	}
}

func TestZeroCountHasNoEstimate(t *testing.T) {
	estimates, _, err := Estimate(schemeA(), []uint64{0, 0, 0, 0}, []float64{0.5})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if estimates[0].Value != nil {
		t.Fatal("empty histogram must return a null estimate")
	}
}

func TestEstimateRejectsMismatchedLengths(t *testing.T) {
	if _, _, err := Estimate([]float64{1, 2}, []uint64{1}, []float64{0.5}); err == nil {
		t.Fatal("mismatched bounds and counts must be rejected")
	}
}

func TestEstimatePinsRankToFinalBucket(t *testing.T) {
	// A total near the uint64 limit rounds in float space; estimation must
	// stay inside the stored buckets instead of indexing past the end.
	huge := []uint64{1 << 62, 1 << 62, 1 << 62, math.MaxUint64}
	estimates, _, err := Estimate(schemeA(), huge, []float64{0.9999999999})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if estimates[0].Value == nil || *estimates[0].Value > schemeA()[3] {
		t.Fatalf("rank must be pinned inside the final bucket: %+v", estimates[0])
	}
}

func TestOnlyPopulatedWindowsAppearAndAlignToOrigin(t *testing.T) {
	query := model.HistogramQuery{
		Start: 0, End: 10000, Step: 1000, Origin: -500,
	}
	s := seriesOf("a",
		snapshot(100, 1, schemeA(), nil, []uint64{1, 1, 1, 1}, 10),
		snapshot(600, 2, schemeA(), nil, []uint64{2, 2, 2, 2}, 20),
	)
	cells, err := Aggregate([]model.SeriesHistograms{s}, query)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(cells) != 2 {
		t.Fatalf("expected two populated windows, got %d", len(cells))
	}
	// Origin -500 with step 1000: windows start at -500 and 500.
	if cells[0].Start != -500 || cells[1].Start != 500 {
		t.Fatalf("windows not origin aligned: %d %d", cells[0].Start, cells[1].Start)
	}
}

func contains(codes []string, want string) bool {
	for _, code := range codes {
		if code == want {
			return true
		}
	}
	return false
}
