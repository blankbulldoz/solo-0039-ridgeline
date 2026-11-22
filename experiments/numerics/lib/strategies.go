// Package lib contains read-only numerical experiments for the
// Ridgeline float64 reducers. Nothing in this tree is imported by production
// code; it exists to quantify rounding behavior and compare strategies.
package lib

import (
	"math/big"
	"sort"
)

// SeqSum reproduces internal/window.Accumulate: values are added to one
// float64 register in the order the caller supplies.
func SeqSum(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s
}

// PairwiseSum uses a recursive balanced tree (same binary64 rounding,
// different association). Leaf threshold 64 keeps overhead bounded.
func PairwiseSum(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	if len(xs) <= 64 {
		var s float64
		for _, x := range xs {
			s += x
		}
		return s
	}
	m := len(xs) / 2
	return PairwiseSum(xs[:m]) + PairwiseSum(xs[m:])
}

// NeumaierSum is the improved Kahan compensated summation (Kahan-Babuška),
// which stays accurate even when the running sum loses magnitude to terms.
// Returned value is still a single float64; the compensation is internal.
func NeumaierSum(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := xs[0]
	var c float64
	for _, x := range xs[1:] {
		t := s + x
		if abs(s) >= abs(x) {
			c += (s - t) + x
		} else {
			c += (x - t) + s
		}
		s = t
	}
	return s + c
}

// KahanSum is the classic compensated summation; kept for comparison because
// it is weaker than Neumaier when the running sum is small relative to terms.
func KahanSum(xs []float64) float64 {
	var s, c float64
	for _, x := range xs {
		y := x - c
		t := s + y
		c = (t - s) - y
		s = t
	}
	return s
}

// ExactSum evaluates the mathematically exact real sum in big.Float and
// rounds once to binary64. Precision 128 is far beyond any input combination
// reachable under the 2,000,000-point scan budget (2e6 * 53 bits < 75 bits).
func ExactSum(xs []float64) float64 {
	acc := new(big.Float).SetPrec(128)
	tmp := new(big.Float).SetPrec(128)
	for _, x := range xs {
		tmp.SetFloat64(x)
		acc.Add(acc, tmp)
	}
	f, _ := acc.Float64()
	return f
}

// SortedSeqSum sorts ascending by magnitude first, then sums sequentially.
// It is the cheapest ordering-based improvement and bounds term growth.
func SortedSeqSum(xs []float64) float64 {
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys) // ascending signed value; |order| implied for positive data
	return SeqSum(ys)
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// Reorder returns a permuted copy that interleaves the slice deterministically
// (reverse order and a fixed stride permutation) so order-sensitivity of each
// strategy can be measured without any randomness in the harness.
func Reorder(xs []float64, mode string) []float64 {
	ys := append([]float64(nil), xs...)
	switch mode {
	case "reverse":
		for i, j := 0, len(ys)-1; i < j; i, j = i+1, j-1 {
			ys[i], ys[j] = ys[j], ys[i]
		}
	case "stride":
		// Deterministic perfect-shuffle-like permutation via coprime stride.
		n := len(ys)
		out := make([]float64, n)
		stride := 1
		if n > 1 {
			stride = n/2 + 1
		}
		for i := 0; i < n; i++ {
			out[i] = ys[(i*stride)%n]
		}
		ys = out
	}
	return ys
}

// ExactAvg divides the exact sum by the integer count and rounds once.
func ExactAvg(xs []float64) float64 {
	sum := new(big.Float).SetPrec(128)
	tmp := new(big.Float).SetPrec(128)
	for _, x := range xs {
		tmp.SetFloat64(x)
		sum.Add(sum, tmp)
	}
	cnt := new(big.Float).SetPrec(128).SetInt64(int64(len(xs)))
	sum.Quo(sum, cnt)
	f, _ := sum.Float64()
	return f
}
