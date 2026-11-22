package window_test

import (
	"encoding/json"
	"math/big"
	"math/rand"
	"testing"

	"ridgeline/internal/model"
	"ridgeline/internal/testutil/oracle"
	"ridgeline/internal/window"
)

func points(list ...[2]float64) []model.Sample {
	out := make([]model.Sample, 0, len(list))
	for _, p := range list {
		out = append(out, model.Sample{Time: int64(p[0]), Value: p[1]})
	}
	return out
}

// TestBucketHalfOpenLattice verifies the defining properties of bucket
// attribution exhaustively over signed times, signed origins and steps:
// half-open containment, lattice alignment, and agreement with the
// independent Euclidean-division oracle. The oracle itself is validated by
// the same two invariants, so the implementation and the oracle cannot
// share a systematic error without violating containment.
func TestBucketHalfOpenLattice(t *testing.T) {
	origins := []int64{-14, -7, -3, 0, 3, 7, 14}
	steps := []int64{1, 2, 3, 5, 7, 10, 16}
	for _, origin := range origins {
		for _, step := range steps {
			for ts := int64(-60); ts <= 60; ts++ {
				start := window.Bucket(ts, origin, step)
				if !(start <= ts && ts < start+step) {
					t.Fatalf("Bucket(%d, %d, %d) = %d: t not in half-open [%d, %d)",
						ts, origin, step, start, start, start+step)
				}
				if (start-origin)%step != 0 {
					t.Fatalf("Bucket(%d, %d, %d) = %d: start not congruent to origin mod step",
						ts, origin, step, start)
				}
				want := oracle.BucketStart(ts, origin, step)
				if !(want <= ts && ts < want+step) || (want-origin)%step != 0 {
					t.Fatalf("oracle self-check failed for t=%d origin=%d step=%d", ts, origin, step)
				}
				if start != want {
					t.Fatalf("Bucket(%d, %d, %d) = %d, oracle expects %d", ts, origin, step, start, want)
				}
				// Translation coherence: shifting t by exactly one step
				// must shift the window by exactly one step.
				if next := window.Bucket(ts+step, origin, step); next != start+step {
					t.Fatalf("Bucket(%d+%d, %d, %d) = %d, expected %d",
						ts, step, origin, step, next, start+step)
				}
			}
		}
	}
}

// TestBucketBoundaryTable pins hand-computed boundary cases: lattice points
// on the negative side of the origin (where truncated and floor division
// silently agree) and points just beside them (where they disagree).
func TestBucketBoundaryTable(t *testing.T) {
	cases := []struct {
		t, origin, step, want int64
	}{
		{-13, 7, 10, -13}, // exactly on the lattice, negative side
		{-14, 7, 10, -23}, // one millisecond below the lattice point
		{-4, 7, 10, -13},  // truncated division would answer -3
		{-3, 7, 10, -3},   // lattice point
		{7, 7, 10, 7},     // the origin itself
		{16, 7, 10, 7},    // last millisecond of the window
		{17, 7, 10, 17},   // first millisecond of the next window
		{-1, 0, 10, -10},  // negative side of the epoch
		{-10, 0, 10, -10}, // lattice point on the epoch lattice
		{-11, 0, 10, -20}, // one below it
		{0, -7, 5, -2},    // negative origin, positive time
		{-8, -7, 5, -12},  // negative origin, one below its lattice point
		{-7, -7, 5, -7},   // the negative origin itself
		{0, 0, 1, 0},      // degenerate step of one millisecond
		{-4503599627370496, 0, 1 << 40, -4503599627370496}, // MinTime is on any power-of-two lattice
	}
	for _, c := range cases {
		if got := window.Bucket(c.t, c.origin, c.step); got != c.want {
			t.Errorf("Bucket(%d, %d, %d) = %d, hand-computed expectation %d",
				c.t, c.origin, c.step, got, c.want)
		}
	}
}

// windowDataset exercises negative times, a positive origin, uneven
// observation counts per series, same-timestamp ties across series and a
// series that lacks the grouping label.
func windowDataset() []model.SeriesPoints {
	return []model.SeriesPoints{
		{Series: model.Series{ID: "series-a", Labels: model.Labels{"__name__": "m", "zone": "east"}},
			Points: points([2]float64{-23, 1.5}, [2]float64{-4, 0.1}, [2]float64{3, -1}, [2]float64{6, 0.5})},
		{Series: model.Series{ID: "series-b", Labels: model.Labels{"__name__": "m", "zone": "east"}},
			Points: points([2]float64{-4, 2}, [2]float64{3, 4.5}, [2]float64{6, 8})},
		{Series: model.Series{ID: "series-c", Labels: model.Labels{"__name__": "m", "zone": "west"}},
			Points: points([2]float64{-4, 1}, [2]float64{3, 3})},
		{Series: model.Series{ID: "series-d", Labels: model.Labels{"__name__": "m"}},
			Points: points([2]float64{3, 10})},
	}
}

// TestAggregateMatchesRationalOracle runs every reducer over the dataset
// and compares all cells against the exact rational oracle.
func TestAggregateMatchesRationalOracle(t *testing.T) {
	q := model.Query{
		Start: -100, End: 100, Step: 10, Origin: 7,
		GroupBy: []string{"zone"},
	}
	series := windowDataset()
	for _, function := range []string{"count", "sum", "avg", "min", "max", "first", "last"} {
		q.Function = function
		got, err := window.Aggregate(series, q)
		if err != nil {
			t.Fatalf("function %s: unexpected error %v", function, err)
		}
		oracle.CompareCells(t, got, oracle.Aggregate(series, q), function)
	}
}

// TestAggregateHandComputedSpotChecks pins a few cells with values computed
// by hand, as a second independent anchor next to the Go oracle.
func TestAggregateHandComputedSpotChecks(t *testing.T) {
	q := model.Query{
		Start: -100, End: 100, Step: 10, Origin: 7,
		GroupBy: []string{"zone"}, Function: "avg",
	}
	got, err := window.Aggregate(windowDataset(), q)
	if err != nil {
		t.Fatal(err)
	}
	// East window [-3,7): values -1, 0.5, 4.5, 8 → sum 12, count 4,
	// average exactly 3 (12/4 is exact in binary64).
	for _, c := range got {
		if c.Labels["zone"] == "east" && c.Start == -3 {
			if c.Count != 4 || c.Value != 3 {
				t.Errorf("east [-3,7): got count=%d avg=%v, hand-computed count=4 avg=3", c.Count, c.Value)
			}
		}
	}
	// First/last tie at t=-4 in east window [-13,-3): the smaller series
	// ID wins first, the larger wins last.
	q.Function = "first"
	firsts, err := window.Aggregate(windowDataset(), q)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range firsts {
		if c.Labels["zone"] == "east" && c.Start == -13 && c.Value != 0.1 {
			t.Errorf("east [-13,-3) first: got %v, hand-computed 0.1 (series-a wins the t=-4 tie)", c.Value)
		}
	}
	q.Function = "last"
	lasts, err := window.Aggregate(windowDataset(), q)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range lasts {
		if c.Labels["zone"] == "east" && c.Start == -13 && c.Value != 2 {
			t.Errorf("east [-13,-3) last: got %v, hand-computed 2 (series-b wins the t=-4 tie)", c.Value)
		}
	}
}

// TestAggregatePermutationInvariance shuffles series order and point order
// with a fixed seed. Values are small integers, so every sum is exact in
// binary64 and the output must be bit-identical for every permutation: any
// dependence on arrival order (for example a first/last tie broken by
// ingestion order instead of series ID) shows up as a diff.
func TestAggregatePermutationInvariance(t *testing.T) {
	base := []model.SeriesPoints{
		{Series: model.Series{ID: "perm-1", Labels: model.Labels{"__name__": "m", "zone": "a"}},
			Points: points([2]float64{-20, 3}, [2]float64{-9, -2}, [2]float64{2, 7}, [2]float64{5, 1}, [2]float64{11, -4})},
		{Series: model.Series{ID: "perm-2", Labels: model.Labels{"__name__": "m", "zone": "a"}},
			Points: points([2]float64{-20, 6}, [2]float64{-4, 2}, [2]float64{2, -3}, [2]float64{5, 5}, [2]float64{12, 4})},
		{Series: model.Series{ID: "perm-3", Labels: model.Labels{"__name__": "m", "zone": "b"}},
			Points: points([2]float64{-17, 1}, [2]float64{2, 9}, [2]float64{8, -6})},
		{Series: model.Series{ID: "perm-4", Labels: model.Labels{"__name__": "m"}},
			Points: points([2]float64{-11, 4}, [2]float64{2, 2})},
	}
	q := model.Query{Start: -30, End: 30, Step: 7, Origin: -3, GroupBy: []string{"zone"}}
	rng := rand.New(rand.NewSource(20261122))
	for _, function := range []string{"count", "sum", "avg", "min", "max", "first", "last"} {
		q.Function = function
		var reference string
		for trial := 0; trial < 25; trial++ {
			shuffled := make([]model.SeriesPoints, len(base))
			for i, s := range base {
				pts := make([]model.Sample, len(s.Points))
				copy(pts, s.Points)
				rng.Shuffle(len(pts), func(a, b int) { pts[a], pts[b] = pts[b], pts[a] })
				shuffled[i] = model.SeriesPoints{Series: s.Series, Points: pts}
			}
			rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
			got, err := window.Aggregate(shuffled, q)
			if err != nil {
				t.Fatalf("function %s trial %d: %v", function, trial, err)
			}
			data, _ := json.Marshal(got)
			if trial == 0 {
				reference = string(data)
				oracle.CompareCells(t, got, oracle.Aggregate(shuffled, q), function)
				continue
			}
			if string(data) != reference {
				t.Fatalf("function %s: permutation changed the result\nfirst: %s\ntrial %d: %s",
					function, reference, trial, data)
			}
		}
	}
}

// TestRateMatchesRationalOracle verifies reset-aware counter rates against
// exact rational expectations, including a group that sums per-series rates
// and a series whose windows never reach two points.
func TestRateMatchesRationalOracle(t *testing.T) {
	series := []model.SeriesPoints{
		{Series: model.Series{ID: "counter-1", Labels: model.Labels{"__name__": "requests_total", "zone": "east"}},
			Points: points([2]float64{0, 0}, [2]float64{1000, 10}, [2]float64{2000, 3}, [2]float64{3000, 8},
				[2]float64{15000, 20}, [2]float64{16000, 25})},
		{Series: model.Series{ID: "counter-2", Labels: model.Labels{"__name__": "requests_total", "zone": "east"}},
			Points: points([2]float64{500, 100}, [2]float64{2500, 130})},
		{Series: model.Series{ID: "counter-3", Labels: model.Labels{"__name__": "requests_total", "zone": "west"}},
			Points: points([2]float64{0, 5})},
	}
	q := model.Query{Start: -1000, End: 30000, Step: 10000, Origin: 0, GroupBy: []string{"zone"}, Function: "rate"}
	got, err := window.Aggregate(series, q)
	if err != nil {
		t.Fatal(err)
	}
	oracle.CompareCells(t, got, oracle.Aggregate(series, q), "rate")
	// Hand-computed anchor: east [0,10000) = 18/3 (counter-1, reset at
	// t=2000) + 30/2 (counter-2) = 6 + 15 = 21 per second, count 6.
	found := false
	for _, c := range got {
		if c.Labels["zone"] == "east" && c.Start == 0 {
			found = true
			if c.Value != 21 || c.Count != 6 {
				t.Errorf("east [0,10000): got rate=%v count=%d, hand-computed rate=21 count=6", c.Value, c.Count)
			}
		}
	}
	if !found {
		t.Error("east [0,10000) rate cell missing")
	}
}

// TestRateRejectsNegativeValues pins the precondition that counter rates
// require nonnegative samples.
func TestRateRejectsNegativeValues(t *testing.T) {
	series := []model.SeriesPoints{
		{Series: model.Series{ID: "counter-1", Labels: model.Labels{"__name__": "requests_total"}},
			Points: points([2]float64{0, 5}, [2]float64{1, -1})},
	}
	q := model.Query{Start: -10, End: 10, Step: 10, Origin: 0, Function: "rate"}
	if _, err := window.Aggregate(series, q); err == nil {
		t.Fatal("rate over a negative sample must be rejected")
	}
}

// TestDecimalTenthsSeparateRoundingFromAttribution demonstrates the
// tolerance contract on the classic 0.1+0.1+0.1 example: the exact
// expectation is 3/10, binary64 sequential summation deterministically
// lands one ulp away, and that gap is float rounding — not an attribution
// error. A test that demanded exact equality with decimal 0.3 would be
// broken, not the implementation.
func TestDecimalTenthsSeparateRoundingFromAttribution(t *testing.T) {
	series := []model.SeriesPoints{
		{Series: model.Series{ID: "s", Labels: model.Labels{"__name__": "m"}},
			Points: points([2]float64{0, 0.1}, [2]float64{1, 0.1}, [2]float64{2, 0.1})},
	}
	q := model.Query{Start: -10, End: 10, Step: 10, Origin: 0, Function: "sum"}
	got, err := window.Aggregate(series, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one cell, got %d", len(got))
	}
	exact := new(big.Rat).SetInt64(3)
	exact.Quo(exact, new(big.Rat).SetInt64(10))
	tolerance := oracle.FloatTolerance(3, new(big.Rat).SetFloat64(0.3))
	if !oracle.Close(got[0].Value, exact, tolerance) {
		t.Fatalf("sum %v not within %v of 3/10", got[0].Value, tolerance)
	}
	if got[0].Value == 0.3 {
		t.Fatal("0.1+0.1+0.1 unexpectedly equals decimal 0.3 exactly; " +
			"this test documents binary64 rounding and must be revisited")
	}
	// The misattribution margin: moving any one 0.1 sample to a neighbor
	// window changes the sum by 0.1, which is ~1e14 times the tolerance.
	if 0.1/tolerance < 1e12 {
		t.Fatalf("tolerance %v too large to distinguish attribution errors", tolerance)
	}
}
