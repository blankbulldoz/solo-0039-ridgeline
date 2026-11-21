// Package quantile provides mergeable, bounded-memory approximate quantile
// summaries.
//
// The implementation follows Greenwald and Khanna's deterministic
// epsilon-summary ("Space-Efficient Online Computation of Quantile Summaries",
// 2001). A summary built with parameter epsilon observes a stream of n values
// without retaining them all: it keeps O((1/epsilon) log(epsilon n)) tuples.
// For any requested rank r the returned observed value v has an observed value
// whose true rank r' satisfies
//
//	r - epsilon*n <= r' <= r + epsilon*n.
//
// This is a symmetric, absolute rank guarantee expressed in observations. The
// summaries are mergeable: combining summaries of disjoint value sets yields a
// summary whose rank uncertainty is the sum of the operands' uncertainties.
// Building the operands with NewBlockOperand(epsilon, blockCount) tightens each
// one to epsilon/blockCount, so merging up to blockCount of them still honors
// the requested epsilon over the union rather than relaxing it.
package quantile

import (
	"fmt"
	"math"
	"sort"
)

// flushCapacity bounds the insertion buffer before it is merged into the
// compressed summary.
const flushCapacity = 500

// Tuple is one stored GK entry. Values are non-decreasing across tuples. Width
// (the GK "g") is the gap between this tuple's minimum rank and the previous
// tuple's minimum rank; Delta is the maximum additional rank uncertainty.
type Tuple struct {
	Value float64 `json:"value"`
	Width int     `json:"width"`
	Delta int     `json:"delta"`
}

// Snapshot is the serializable representation of a Summary.
type Snapshot struct {
	Epsilon float64 `json:"epsilon"`
	N       int     `json:"n"`
	Tuples  []Tuple `json:"tuples"`
}

// Summary is a Greenwald-Khanna epsilon-summary. It is not safe for concurrent
// use; callers confine one summary to a single build/merge stream. Incoming
// values are buffered; at capacity the buffer is sorted, merged into the main
// tuple list and compressed.
type Summary struct {
	epsilon float64
	// internalEpsilon drives the 2*internalEpsilon*n tuple invariant. It equals
	// epsilon for a single-stream summary but is tightened when a summary is
	// built as a merge operand so that fan-in merging still honors the
	// user-requested epsilon over the union.
	internalEpsilon float64
	n               int
	tuples          []Tuple
	buffer          []float64
	sorted          bool
}

// New creates an empty summary that keeps absolute rank error within epsilon
// times the observed count n.
func New(epsilon float64) (*Summary, error) {
	return newAt(epsilon, epsilon)
}

// NewBlockOperand creates a summary intended to be merged with up to blockCount
// sibling block summaries (blockCount >= 1) covering disjoint value sets. Each
// operand tightens its internal rank invariant to epsilon/blockCount so that
// the additive uncertainty across all blocks still fits within epsilon times
// the union count once merged. A standalone query of an operand is therefore at
// least as accurate as a normal epsilon summary.
func NewBlockOperand(epsilon float64, blockCount int) (*Summary, error) {
	if blockCount < 1 {
		return nil, fmt.Errorf("blockCount must be positive")
	}
	return newAt(epsilon, epsilon/float64(blockCount))
}

// newAt builds a summary whose advertised guarantee is epsilon but whose tuple
// invariant uses internalEpsilon (internalEpsilon <= epsilon).
func newAt(epsilon, internalEpsilon float64) (*Summary, error) {
	if math.IsNaN(epsilon) || math.IsInf(epsilon, 0) || epsilon <= 0 || epsilon >= 0.5 {
		return nil, fmt.Errorf("epsilon must lie within (0, 0.5)")
	}
	if internalEpsilon <= 0 || internalEpsilon > epsilon {
		return nil, fmt.Errorf("internal epsilon must lie within (0, epsilon]")
	}
	return &Summary{
		epsilon:         epsilon,
		internalEpsilon: internalEpsilon,
		buffer:          make([]float64, 0, flushCapacity),
		sorted:          true,
	}, nil
}

// Epsilon reports the summary's rank-error parameter.
func (s *Summary) Epsilon() float64 { return s.epsilon }

// Len reports how many values the summary has observed.
func (s *Summary) Len() int { return s.n + len(s.buffer) }

// Tuples returns a copy of the compressed tuples after flushing the buffer.
func (s *Summary) Tuples() []Tuple {
	s.flush()
	s.compress()
	out := make([]Tuple, len(s.tuples))
	copy(out, s.tuples)
	return out
}

// Observe feeds one value into the summary. Values need not arrive ordered and
// duplicate values are fully supported.
func (s *Summary) Observe(v float64) {
	s.buffer = append(s.buffer, v)
	s.sorted = false
	if len(s.buffer) == cap(s.buffer) {
		s.flush()
		s.compress()
	}
}

func (s *Summary) maybeSort() {
	if !s.sorted {
		sort.Float64s(s.buffer)
		s.sorted = true
	}
}

// flush sorts the buffer and merges its values into the main tuple list.
func (s *Summary) flush() {
	if len(s.buffer) == 0 {
		return
	}
	s.maybeSort()
	rank := 0
	for _, v := range s.buffer {
		s.n++
		// First tuple whose value exceeds v is the insertion point; equal
		// values stay ordered and are inserted as distinct tuples.
		idx := sort.Search(len(s.tuples), func(i int) bool { return s.tuples[i].Value > v })
		switch {
		case len(s.tuples) == 0 || idx == len(s.tuples):
			// Running maximum: exact tuple with delta 0.
			s.tuples = append(s.tuples, Tuple{Value: v, Width: 1, Delta: 0})
		case idx == 0:
			// New global minimum: exact tuple, the previous minimum becomes an
			// interior tuple but keeps its own delta.
			s.tuples = append(s.tuples, Tuple{})
			copy(s.tuples[1:], s.tuples)
			s.tuples[0] = Tuple{Value: v, Width: 1, Delta: 0}
		default:
			delta := int(2*s.internalEpsilon*float64(s.n)) - 1
			if delta < 0 {
				delta = 0
			}
			s.tuples = append(s.tuples, Tuple{})
			copy(s.tuples[idx+1:], s.tuples[idx:])
			s.tuples[idx] = Tuple{Value: v, Width: 1, Delta: delta}
		}
		rank++
	}
	s.buffer = s.buffer[:0]
	s.sorted = true
}

// band is the GK band index: 0 for the exact extrema, otherwise
// 1 + floor(log2(period - delta)). A smaller delta means a larger band.
func band(delta, period int) int {
	if delta == 0 {
		return 0
	}
	b := 1
	span := period - delta
	for span > 1 {
		span /= 2
		b++
	}
	return b
}

// compress removes redundant interior tuples while preserving the GK
// guarantee. It walks backwards, folding tuple c into its successor x when c's
// band is no higher than x's band and the combined span stays below
// 2*epsilon*n. The successor keeps its value and delta and absorbs c's width.
func (s *Summary) compress() {
	if len(s.tuples) < 2 {
		return
	}
	period := int(2 * s.internalEpsilon * float64(s.n))
	if period <= 1 {
		return
	}
	x := s.tuples[len(s.tuples)-1]
	xi := len(s.tuples) - 1
	rank := s.n - x.Width
	for i := len(s.tuples) - 2; i >= 0; i-- {
		c := s.tuples[i]
		if band(c.Delta, period) > band(x.Delta, period) &&
			c.Width+x.Width+x.Delta < period {
			x.Width += c.Width
			s.tuples[xi] = x
			copy(s.tuples[i:], s.tuples[i+1:])
			s.tuples = s.tuples[:len(s.tuples)-1]
			xi--
		} else {
			x = c
			xi = i
		}
		rank -= c.Width
	}
}

// rankBounds computes the inclusive min/max ranks of every tuple.
func rankBounds(tuples []Tuple) (rMin, rMax []int) {
	rMin = make([]int, len(tuples))
	rMax = make([]int, len(tuples))
	cumulative := 0
	for i, t := range tuples {
		cumulative += t.Width
		rMin[i] = cumulative
		rMax[i] = cumulative + t.Delta
	}
	return rMin, rMax
}

// Merge combines summaries of disjoint value sets into a new summary. The
// operands are not modified and must share one advertised epsilon. The merge
// interleaves the operand tuples in value order while preserving each tuple's
// width and rank uncertainty: widths are summed and the larger delta is kept
// at an equal value. The combined uncertainty is the sum of the operands'
// uncertainties, so operands created with NewBlockOperand(epsilon, blockCount)
// merge into a result that stays within epsilon of the union count. No
// lossy re-compression is applied because that would discard the provenance
// needed to state the merged error honestly.
func Merge(a, b *Summary) (*Summary, error) {
	if a == nil || b == nil {
		return nil, fmt.Errorf("cannot merge a nil summary")
	}
	if math.Abs(a.epsilon-b.epsilon) > 1e-12 {
		return nil, fmt.Errorf("cannot merge summaries with different epsilon")
	}
	if a.Len() == 0 {
		return clone(b), nil
	}
	if b.Len() == 0 {
		return clone(a), nil
	}
	ta := a.Tuples()
	tb := b.Tuples()
	out := &Summary{
		epsilon:         a.epsilon,
		internalEpsilon: minEpsilon(a.internalEpsilon, b.internalEpsilon),
		buffer:          make([]float64, 0, flushCapacity),
		sorted:          true,
		tuples:          make([]Tuple, 0, len(ta)+len(tb)),
	}
	i, j := 0, 0
	for i < len(ta) || j < len(tb) {
		switch {
		case j == len(tb) || (i < len(ta) && ta[i].Value < tb[j].Value):
			out.tuples = append(out.tuples, ta[i])
			out.n += ta[i].Width
			i++
		case i == len(ta) || tb[j].Value < ta[i].Value:
			out.tuples = append(out.tuples, tb[j])
			out.n += tb[j].Width
			j++
		default:
			out.tuples = append(out.tuples, Tuple{
				Value: ta[i].Value,
				Width: ta[i].Width + tb[j].Width,
				Delta: maxInt(ta[i].Delta, tb[j].Delta),
			})
			out.n += ta[i].Width + tb[j].Width
			i++
			j++
		}
	}
	return out, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minEpsilon(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func clone(s *Summary) *Summary {
	s.flush()
	out := &Summary{
		epsilon:         s.epsilon,
		internalEpsilon: s.internalEpsilon,
		n:               s.n,
		tuples:          make([]Tuple, len(s.tuples)),
		buffer:          make([]float64, 0, flushCapacity),
		sorted:          true,
	}
	copy(out.tuples, s.tuples)
	return out
}

// Estimate is the result of a quantile query.
type Estimate struct {
	// Phi is the requested quantile in (0, 1).
	Phi float64 `json:"phi"`
	// Value is the approximate phi quantile.
	Value float64 `json:"value"`
	// Lower and Upper bracket an observed value equal to the true quantile:
	// the true quantile lies within [Lower, Upper].
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
	// RankError is the absolute rank guarantee epsilon*n rounded up.
	RankError int `json:"rank_error"`
	// Epsilon is the summary's rank-error parameter.
	Epsilon float64 `json:"epsilon"`
	// N is the number of observed values represented by the summary.
	N int `json:"n"`
}

// Quantile returns the approximate phi quantile together with its error
// meaning. phi lies in (0, 1); e.g. 0.95 requests the p95.
func (s *Summary) Quantile(phi float64) (Estimate, error) {
	if math.IsNaN(phi) || math.IsInf(phi, 0) || phi <= 0 || phi >= 1 {
		return Estimate{}, fmt.Errorf("phi must lie within (0, 1)")
	}
	if s.Len() == 0 {
		return Estimate{}, fmt.Errorf("cannot query an empty summary")
	}
	s.flush()
	n := s.n
	target := int(math.Ceil(phi * float64(n)))
	rankErr := int(math.Ceil(s.epsilon * float64(n)))
	if rankErr < 1 {
		rankErr = 1
	}

	// GK query: the first tuple whose guaranteed maximum rank reaches the
	// target rank is the estimate; the invariant keeps its true rank within
	// epsilon*n of the target.
	_, rMax := rankBounds(s.tuples)
	idx := sort.Search(len(rMax), func(i int) bool { return rMax[i] >= target })
	if idx >= len(s.tuples) {
		idx = len(s.tuples) - 1
	}
	est := Estimate{
		Phi: phi, Value: s.tuples[idx].Value, RankError: rankErr,
		Epsilon: s.epsilon, N: n,
	}
	// Bracket the true quantile from the guaranteed rank bounds: values whose
	// rank interval can contain target +/- rankErr.
	rMin, _ := rankBounds(s.tuples)
	lo := sort.Search(len(rMax), func(i int) bool { return rMax[i] >= target-rankErr })
	hi := sort.Search(len(rMin), func(i int) bool { return rMin[i] > target+rankErr })
	if lo >= len(s.tuples) {
		lo = len(s.tuples) - 1
	}
	if hi == 0 {
		hi = 0
	} else {
		hi--
		if hi >= len(s.tuples) {
			hi = len(s.tuples) - 1
		}
	}
	est.Lower = s.tuples[lo].Value
	est.Upper = s.tuples[hi].Value
	return est, nil
}

// Marshal converts the summary to its serializable snapshot.
func (s *Summary) Marshal() Snapshot {
	s.flush()
	return Snapshot{Epsilon: s.epsilon, N: s.n, Tuples: s.Tuples()}
}

// Unmarshal restores a summary previously produced with Marshal.
func Unmarshal(snap Snapshot) (*Summary, error) {
	s, err := New(snap.Epsilon)
	if err != nil {
		return nil, err
	}
	if snap.N < 0 {
		return nil, fmt.Errorf("invalid quantile snapshot")
	}
	if snap.N == 0 && len(snap.Tuples) == 0 {
		return s, nil
	}
	observed := 0
	for i, t := range snap.Tuples {
		if t.Width <= 0 || t.Delta < 0 {
			return nil, fmt.Errorf("invalid quantile tuple at index %d", i)
		}
		if i > 0 && t.Value < snap.Tuples[i-1].Value {
			return nil, fmt.Errorf("quantile tuples must be ordered")
		}
		observed += t.Width
	}
	if observed != snap.N {
		return nil, fmt.Errorf("quantile snapshot rank span %d != n %d", observed, snap.N)
	}
	s.n = snap.N
	s.tuples = make([]Tuple, len(snap.Tuples))
	copy(s.tuples, snap.Tuples)
	return s, nil
}
