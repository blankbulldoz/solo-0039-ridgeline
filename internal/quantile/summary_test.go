package quantile

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func exactQuantile(sorted []float64, phi float64) float64 {
	rank := int(math.Ceil(phi*float64(len(sorted)))) - 1
	return sorted[rank]
}

// trueRankSpan returns the inclusive 1-based rank interval of values equal to v.
func trueRankSpan(all []float64, v float64) (int, int) {
	lo, hi := 0, 0
	for _, x := range all {
		if x < v {
			lo++
		}
		if x <= v {
			hi++
		}
	}
	return lo + 1, hi
}

// checkStructure verifies ordering, rank bookkeeping and the GK per-tuple
// invariant g_i + delta_i <= 2*epsilon*n for interior tuples.
func checkStructure(t *testing.T, s *Summary) {
	t.Helper()
	s.flush()
	period := int(2 * s.epsilon * float64(s.n))
	cumulative := 0
	for i, tup := range s.tuples {
		if tup.Width <= 0 || tup.Delta < 0 {
			t.Fatalf("tuple %d invalid: %+v", i, tup)
		}
		if i > 0 && tup.Value < s.tuples[i-1].Value {
			t.Fatalf("tuples not ordered at %d", i)
		}
		if i > 0 && i < len(s.tuples)-1 && tup.Width+tup.Delta > period {
			t.Fatalf("tuple %d violates GK invariant: %+v period=%d", i, tup, period)
		}
		cumulative += tup.Width
	}
	if cumulative != s.n {
		t.Fatalf("rank span %d != n %d", cumulative, s.n)
	}
}

// checkEstimate verifies the symmetric GK rank guarantee and the bracket.
func checkEstimate(t *testing.T, all []float64, s *Summary, phi float64) Estimate {
	t.Helper()
	est, err := s.Quantile(phi)
	if err != nil {
		t.Fatalf("quantile %v: %v", phi, err)
	}
	target := int(math.Ceil(phi * float64(len(all))))
	bound := int(math.Ceil(s.epsilon * float64(len(all))))
	lo, hi := trueRankSpan(all, est.Value)
	// Some rank of the returned value lies within bound of the target rank.
	if hi < target-bound || lo > target+bound {
		t.Fatalf("phi=%v estimate=%v true rank=[%d,%d] target=%d bound=%d",
			phi, est.Value, lo, hi, target, bound)
	}
	if est.RankError != bound {
		t.Fatalf("rank error %d != bound %d", est.RankError, bound)
	}
	if est.Lower > est.Value || est.Value > est.Upper {
		t.Fatalf("bracket does not contain estimate: %+v", est)
	}
	trueQ := exactQuantile(all, phi)
	if trueQ < est.Lower || trueQ > est.Upper {
		t.Fatalf("true quantile %v outside bracket [%v,%v]", trueQ, est.Lower, est.Upper)
	}
	return est
}

func TestUniformStreamGuarantee(t *testing.T) {
	const n = 20000
	rng := rand.New(rand.NewSource(42))
	s, err := New(0.01)
	if err != nil {
		t.Fatal(err)
	}
	all := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		v := rng.Float64() * 1000
		s.Observe(v)
		all = append(all, v)
	}
	sort.Float64s(all)
	checkStructure(t, s)
	for _, phi := range []float64{0.5, 0.9, 0.95, 0.99, 0.999} {
		checkEstimate(t, all, s, phi)
	}
	if len(s.tuples) > n/4 {
		t.Fatalf("summary did not compress: %d tuples", len(s.tuples))
	}
	if s.tuples[0].Value != all[0] || s.tuples[len(s.tuples)-1].Value != all[n-1] {
		t.Fatalf("extrema not exact")
	}
}

func TestSkewedDistribution(t *testing.T) {
	const n = 30000
	rng := rand.New(rand.NewSource(7))
	s, _ := New(0.005)
	all := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		v := math.Pow(rng.Float64(), 8) * 10000
		s.Observe(v)
		all = append(all, v)
	}
	sort.Float64s(all)
	checkStructure(t, s)
	for _, phi := range []float64{0.5, 0.95, 0.99} {
		est := checkEstimate(t, all, s, phi)
		exact := exactQuantile(all, phi)
		if math.Abs(est.Value-exact) > math.Abs(all[n-1]-all[0]) {
			t.Fatalf("skew estimate wildly off: est=%v exact=%v", est.Value, exact)
		}
	}
}

func TestDuplicateValues(t *testing.T) {
	values := []float64{1, 1, 2, 2, 2, 2, 5, 5, 9, 9, 9, 9, 9, 9, 100, 100}
	s, _ := New(0.05)
	all := make([]float64, 0, 10000)
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 10000; i++ {
		v := values[rng.Intn(len(values))]
		s.Observe(v)
		all = append(all, v)
	}
	sort.Float64s(all)
	checkStructure(t, s)
	for _, phi := range []float64{0.01, 0.5, 0.95, 0.99} {
		checkEstimate(t, all, s, phi)
	}
	single, _ := New(0.01)
	for i := 0; i < 5000; i++ {
		single.Observe(42)
	}
	est, err := single.Quantile(0.95)
	if err != nil || est.Value != 42 || est.Lower != 42 || est.Upper != 42 {
		t.Fatalf("identical-stream quantile wrong: %+v err=%v", est, err)
	}
}

func TestOrderedStream(t *testing.T) {
	s, _ := New(0.01)
	all := make([]float64, 0, 20000)
	for i := 0; i < 20000; i++ {
		v := float64(i)
		s.Observe(v)
		all = append(all, v)
	}
	checkStructure(t, s)
	for _, phi := range []float64{0.5, 0.95, 0.99} {
		checkEstimate(t, all, s, phi)
	}
}

func TestMergeDisjointBlocks(t *testing.T) {
	const perBlock = 8000
	build := func(seed int64, offset float64) (*Summary, []float64) {
		r := rand.New(rand.NewSource(seed))
		s, _ := NewBlockOperand(0.01, 2)
		all := make([]float64, 0, perBlock)
		for i := 0; i < perBlock; i++ {
			v := r.Float64()*100 + offset
			s.Observe(v)
			all = append(all, v)
		}
		return s, all
	}
	a, allA := build(1, 0)
	b, allB := build(2, 5000)
	merged, err := Merge(a, b)
	if err != nil {
		t.Fatal(err)
	}
	all := append(allA, allB...)
	sort.Float64s(all)
	checkStructure(t, a)
	checkStructure(t, b)
	checkStructure(t, merged)
	if merged.Len() != 2*perBlock {
		t.Fatalf("merged n=%d want %d", merged.Len(), 2*perBlock)
	}
	if merged.Epsilon() != 0.01 {
		t.Fatalf("merge changed epsilon: %v", merged.Epsilon())
	}
	for _, phi := range []float64{0.5, 0.95, 0.99} {
		checkEstimate(t, all, merged, phi)
	}
	if merged.tuples[0].Value != a.tuples[0].Value {
		t.Fatalf("merge lost minimum")
	}
}

func TestMergeOverlappingBlocks(t *testing.T) {
	const blocks = 5
	const perBlock = 4000
	rng := rand.New(rand.NewSource(123))
	var merged *Summary
	all := make([]float64, 0, blocks*perBlock)
	for k := 0; k < blocks; k++ {
		s, _ := NewBlockOperand(0.01, blocks)
		for i := 0; i < perBlock; i++ {
			v := math.Floor(rng.Float64()*50) / 10
			s.Observe(v)
			all = append(all, v)
		}
		if merged == nil {
			merged = s
		} else {
			merged, _ = Merge(merged, s)
		}
	}
	sort.Float64s(all)
	checkStructure(t, merged)
	for _, phi := range []float64{0.25, 0.5, 0.75, 0.95, 0.99} {
		checkEstimate(t, all, merged, phi)
	}
}

func TestMergeValidation(t *testing.T) {
	a, _ := New(0.01)
	b, _ := New(0.02)
	for i := 0; i < 1000; i++ {
		a.Observe(float64(i))
		b.Observe(float64(i) + 0.5)
	}
	if _, err := Merge(a, b); err == nil {
		t.Fatal("merging different epsilon should fail")
	}
	empty, _ := New(0.01)
	merged, err := Merge(empty, a)
	if err != nil || merged.Len() != 1000 {
		t.Fatalf("empty merge: %v n=%d", err, merged.Len())
	}
	if _, err := Merge(nil, a); err == nil {
		t.Fatal("nil merge should fail")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s, _ := New(0.01)
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 5000; i++ {
		s.Observe(rng.Float64() * 100)
	}
	snap := s.Marshal()
	restored, err := Unmarshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Len() != s.Len() || restored.Epsilon() != s.Epsilon() ||
		len(restored.tuples) != len(s.tuples) {
		t.Fatalf("snapshot mismatch")
	}
	want, _ := s.Quantile(0.95)
	got, _ := restored.Quantile(0.95)
	if got != want {
		t.Fatalf("restored quantile %+v != %+v", got, want)
	}
	empty, _ := New(0.1)
	if _, err := Unmarshal(empty.Marshal()); err != nil {
		t.Fatalf("empty snapshot: %v", err)
	}
	if _, err := Unmarshal(Snapshot{Epsilon: 0.01, N: 3,
		Tuples: []Tuple{{Value: 1, Width: 1, Delta: 0}, {Value: 2, Width: 5, Delta: 0}}}); err == nil {
		t.Fatal("corrupt snapshot should be rejected")
	}
	if _, err := Unmarshal(Snapshot{Epsilon: 2, N: 0}); err == nil {
		t.Fatal("invalid epsilon snapshot should be rejected")
	}
}

func TestValidationAndBoundaries(t *testing.T) {
	for _, eps := range []float64{0, -0.1, 0.5, math.NaN(), math.Inf(1)} {
		if _, err := New(eps); err == nil {
			t.Fatalf("epsilon %v should be rejected", eps)
		}
	}
	s, _ := New(0.01)
	if _, err := s.Quantile(0.95); err == nil {
		t.Fatal("empty summary must reject queries")
	}
	for i := 0; i < 100; i++ {
		s.Observe(float64(i))
	}
	for _, phi := range []float64{0, 1, -0.5, math.NaN()} {
		if _, err := s.Quantile(phi); err == nil {
			t.Fatalf("phi %v should be rejected", phi)
		}
	}
}

// TestBoundedMemory confirms tuple count grows logarithmically, not linearly.
func TestBoundedMemory(t *testing.T) {
	s, _ := New(0.01)
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 100000; i++ {
		s.Observe(rng.Float64())
	}
	s.flush()
	// 1/eps * log2(eps*n) is the GK space bound; allow a generous constant.
	bound := int(100.0 * math.Log2(0.01*100000))
	if len(s.tuples) > bound {
		t.Fatalf("tuple count %d exceeds bounded-memory expectation %d", len(s.tuples), bound)
	}
}
