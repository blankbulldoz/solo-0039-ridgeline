// Package oracle is an independent reference implementation of Ridgeline's
// window aggregation semantics. It exists for tests only and deliberately
// shares no code with internal/window:
//
//   - Bucket attribution uses Euclidean division on math/big.Int, and tests
//     validate it through brute-force half-open containment invariants, not
//     through the truncated-division-with-correction formula that
//     window.Bucket uses.
//   - Reducers accumulate exact rational numbers (math/big.Rat) built from
//     the exact binary values of the float64 inputs, so the IEEE-754
//     binary64 rounding of the implementation under test is measured
//     against a known-exact expectation instead of a golden value captured
//     from that same implementation.
//
// The tolerance derivation and the counterexample catalog that accompany
// this package live in docs/window-testing.md.
package oracle

import (
	"encoding/json"
	"math"
	"math/big"
	"sort"
	"strconv"
	"testing"

	"ridgeline/internal/model"
)

// unitRoundoff is the unit roundoff u of IEEE-754 binary64: half the
// distance between 1 and the next representable value.
const unitRoundoff = 1.0 / (1 << 53)

// rat returns the exact rational value of the float64 f. The implementation
// under test receives f as input, so expectations built on rat(f) are exact
// with respect to what the implementation actually sees.
func rat(f float64) *big.Rat {
	return new(big.Rat).SetFloat64(f)
}

// BucketStart returns the start of the half-open window
// [start, start+step) that contains t, where every valid start satisfies
// start ≡ origin (mod step). big.Int.Div is Euclidean division: for a
// positive divisor the quotient rounds toward negative infinity, which is
// exactly floor division on the signed delta t-origin.
func BucketStart(t, origin, step int64) int64 {
	delta := new(big.Int).Sub(big.NewInt(t), big.NewInt(origin))
	index := new(big.Int).Div(delta, big.NewInt(step))
	start := index.Mul(index, big.NewInt(step))
	return start.Add(start, big.NewInt(origin)).Int64()
}

// Cell is the exact expectation for one output cell of a window
// aggregation. Which fields are meaningful depends on the function; see
// Expected.
type Cell struct {
	Labels model.Labels
	Start  int64
	End    int64
	Count  int
	// Sum and Rate are exact rational accumulations.
	Sum  *big.Rat
	Rate *big.Rat
	// AbsSum is the exact sum of magnitudes of every accumulated term
	// (|value| for reducers, |per-series rate| for rate). It is the
	// magnitude base for FloatTolerance.
	AbsSum *big.Rat
	// Terms bounds the number of binary64 additions behind Sum or Rate.
	Terms int
	// First, Last, Min and Max are selected input values, never computed
	// ones: implementations must reproduce them bit-exactly.
	First float64
	Last  float64
	Min   float64
	Max   float64

	obs []observation
}

// observation is one (time, series ID, value) tuple inside a cell, kept so
// first/last can be derived by sorting instead of by the running-comparison
// formulation used by the implementation under test.
type observation struct {
	time  int64
	id    string
	value float64
}

// Expected returns the exact rational expectation of one aggregation
// function for this cell, and whether the implementation must reproduce it
// bit-exactly. Counts and selected values (min, max, first, last) involve
// no floating-point arithmetic, so exact=true. Sums, averages and rates
// involve binary64 rounding, so exact=false and FloatTolerance applies.
func (c Cell) Expected(function string) (want *big.Rat, exact bool) {
	switch function {
	case "count":
		return new(big.Rat).SetInt64(int64(c.Count)), true
	case "sum":
		return c.Sum, false
	case "avg":
		return new(big.Rat).Quo(c.Sum, new(big.Rat).SetInt64(int64(c.Count))), false
	case "min":
		return rat(c.Min), true
	case "max":
		return rat(c.Max), true
	case "first":
		return rat(c.First), true
	case "last":
		return rat(c.Last), true
	case "rate":
		return c.Rate, false
	}
	return nil, true
}

// Tolerance returns the largest absolute error that binary64 rounding may
// legitimately produce for this cell's sum/average/rate. See FloatTolerance.
func (c Cell) Tolerance() float64 {
	return FloatTolerance(c.Terms, c.AbsSum)
}

// FloatTolerance returns the largest absolute error attributable to IEEE-754
// binary64 rounding for a reduction of `terms` ordered additions whose exact
// term magnitudes sum to absBase, followed by at most one correctly rounded
// division (the average's divide-by-count or the rate's divide-by-elapsed).
//
// Sequential summation of n terms has a worst-case absolute error of
// (n-1)·u·Σ|xᵢ| (Higham, Accuracy and Stability of Numerical Algorithms,
// chap. 4); one final division adds at most u·|result| ≤ u·absBase. The
// factor 4 is headroom for the rounding of the expectation itself when it
// is converted to float64 for comparison. A zero absBase means every term
// is exactly zero and the result must be exactly zero.
func FloatTolerance(terms int, absBase *big.Rat) float64 {
	if absBase.Sign() == 0 {
		return 0
	}
	base, _ := absBase.Float64()
	return 4 * float64(terms+1) * unitRoundoff * base
}

// Close reports whether got is within tolerance of the exact expectation
// want. The expectation's own conversion to float64 rounds by at most one
// unit roundoff, which FloatTolerance's headroom absorbs.
func Close(got float64, want *big.Rat, tolerance float64) bool {
	w, _ := want.Float64()
	return math.Abs(got-w) <= tolerance
}

// Exact reports whether got is bit-identical to the nearest float64 of the
// exact expectation want. Use it for selections and counts, where any
// deviation is a discrete (attribution or selection) error, never rounding.
func Exact(got float64, want *big.Rat) bool {
	w, _ := want.Float64()
	return got == w
}

// groupKey projects labels onto the group_by names, dropping absent labels,
// and returns a canonical JSON key plus the projected set.
func groupKey(labels model.Labels, names []string) (string, model.Labels) {
	out := model.Labels{}
	for _, name := range names {
		if value, present := labels[name]; present {
			out[name] = value
		}
	}
	data, _ := json.Marshal(out)
	return string(data), out
}

// Aggregate computes exact expectations for every cell that the given
// function should produce. Points within one series are used in the given
// order for reducers and sorted by time for rate, mirroring the engine's
// contract of feeding time-sorted points.
func Aggregate(series []model.SeriesPoints, q model.Query) []Cell {
	if q.Function == "rate" {
		return rateCells(series, q)
	}
	return reduceCells(series, q)
}

func reduceCells(series []model.SeriesPoints, q model.Query) []Cell {
	groups := map[string]*Cell{}
	for _, s := range series {
		key, labels := groupKey(s.Series.Labels, q.GroupBy)
		for _, p := range s.Points {
			start := BucketStart(p.Time, q.Origin, q.Step)
			k := key + "/" + strconv.FormatInt(start, 10)
			c := groups[k]
			if c == nil {
				c = &Cell{
					Labels: labels, Start: start, End: start + q.Step,
					Sum: new(big.Rat), AbsSum: new(big.Rat),
					Min: math.Inf(1), Max: math.Inf(-1),
				}
				groups[k] = c
			}
			v := rat(p.Value)
			c.Sum.Add(c.Sum, v)
			c.AbsSum.Add(c.AbsSum, new(big.Rat).Abs(v))
			c.Terms++
			c.Count++
			if p.Value < c.Min {
				c.Min = p.Value
			}
			if p.Value > c.Max {
				c.Max = p.Value
			}
			c.obs = append(c.obs, observation{time: p.Time, id: s.Series.ID, value: p.Value})
		}
	}
	out := sortedCells(groups)
	for i := range out {
		// First is the value at the lexicographically smallest
		// (time, series ID); Last at the largest. Sorting is an
		// independent formulation of the tie-break contract.
		obs := out[i].obs
		sort.Slice(obs, func(a, b int) bool {
			if obs[a].time != obs[b].time {
				return obs[a].time < obs[b].time
			}
			return obs[a].id < obs[b].id
		})
		out[i].First = obs[0].value
		out[i].Last = obs[len(obs)-1].value
	}
	return out
}

func rateCells(series []model.SeriesPoints, q model.Query) []Cell {
	type seriesWindow struct {
		group string
		start int64
		id    string
	}
	points := map[seriesWindow][]model.Sample{}
	labelsOf := map[string]model.Labels{}
	for _, s := range series {
		key, labels := groupKey(s.Series.Labels, q.GroupBy)
		labelsOf[key] = labels
		for _, p := range s.Points {
			start := BucketStart(p.Time, q.Origin, q.Step)
			k := seriesWindow{key, start, s.Series.ID}
			points[k] = append(points[k], p)
		}
	}
	cells := map[string]*Cell{}
	for k, pts := range points {
		if len(pts) < 2 {
			// A window observed once per series contributes nothing.
			continue
		}
		ordered := make([]model.Sample, len(pts))
		copy(ordered, pts)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].Time < ordered[j].Time })
		increase := new(big.Rat)
		for i := 1; i < len(ordered); i++ {
			d := new(big.Rat).Sub(rat(ordered[i].Value), rat(ordered[i-1].Value))
			if d.Sign() < 0 {
				// A decrease is a reset to zero: the counter
				// restarted, so the whole new value is increase.
				d = rat(ordered[i].Value)
			}
			increase.Add(increase, d)
		}
		elapsed := ordered[len(ordered)-1].Time - ordered[0].Time
		if elapsed <= 0 {
			continue
		}
		// rate = increase / (elapsed milliseconds / 1000)
		r := new(big.Rat).Mul(increase, big.NewRat(1000, elapsed))
		key := k.group + "/" + strconv.FormatInt(k.start, 10)
		c := cells[key]
		if c == nil {
			c = &Cell{
				Labels: labelsOf[k.group], Start: k.start, End: k.start + q.Step,
				Rate: new(big.Rat), AbsSum: new(big.Rat),
			}
			cells[key] = c
		}
		c.Rate.Add(c.Rate, r)
		c.AbsSum.Add(c.AbsSum, new(big.Rat).Abs(r))
		c.Count += len(ordered)
		c.Terms += len(ordered) + 1
	}
	return sortedCells(cells)
}

// sortedCells flattens the cell map into the canonical output order:
// group labels JSON, then window start.
func sortedCells(groups map[string]*Cell) []Cell {
	out := make([]Cell, 0, len(groups))
	for _, c := range groups {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := json.Marshal(out[i].Labels)
		b, _ := json.Marshal(out[j].Labels)
		if string(a) != string(b) {
			return string(a) < string(b)
		}
		return out[i].Start < out[j].Start
	})
	return out
}

// CompareCells asserts that got (produced by the implementation under test)
// matches the oracle expectation want cell by cell, in canonical order.
// Counts, windows and selected values must match exactly; sums, averages
// and rates must match within each cell's FloatTolerance.
func CompareCells(t *testing.T, got []model.Cell, want []Cell, function string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("function %s: got %d cells, oracle expects %d\ngot:  %v\nwant: %v",
			function, len(got), len(want), got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		gotLabels, _ := json.Marshal(g.Labels)
		wantLabels, _ := json.Marshal(w.Labels)
		if string(gotLabels) != string(wantLabels) {
			t.Errorf("function %s cell %d: labels %s, oracle expects %s", function, i, gotLabels, wantLabels)
			continue
		}
		if g.Start != w.Start || g.End != w.End {
			t.Errorf("function %s cell %d: window [%d,%d), oracle expects [%d,%d)",
				function, i, g.Start, g.End, w.Start, w.End)
			continue
		}
		if g.Count != w.Count {
			t.Errorf("function %s cell %d [%d,%d): count %d, oracle expects %d",
				function, i, g.Start, g.End, g.Count, w.Count)
		}
		expectation, exact := w.Expected(function)
		if exact {
			if !Exact(g.Value, expectation) {
				t.Errorf("function %s cell %d [%d,%d): value %v, oracle expects exactly %v",
					function, i, g.Start, g.End, g.Value, expectation)
			}
			continue
		}
		if !Close(g.Value, expectation, w.Tolerance()) {
			t.Errorf("function %s cell %d [%d,%d): value %v, oracle expects %v ± %v",
				function, i, g.Start, g.End, g.Value, expectation, w.Tolerance())
		}
	}
}
