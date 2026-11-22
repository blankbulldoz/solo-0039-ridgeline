package lib

import "math"

// Scenario is one fixed, deterministic input distribution. Every generator is
// pure so the harness is fully reproducible across runs and machines.
type Scenario struct {
	Name string
	N    int
	Gen  func(n int) []float64
	Note string
}

// Scenarios returns the sum/avg experiment matrix. Sizes stay within the
// production per-query scan ceiling (MaxScan <= 2,000,000).
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name: "small_increments_0p1", N: 2_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				for i := range xs {
					xs[i] = 0.1 // not representable exactly: classic drift probe
				}
				return xs
			},
			Note: "2e6 identical 0.1 increments (largest supported scan budget)",
		},
		{
			Name: "small_increments_unit", N: 2_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				for i := range xs {
					xs[i] = 1.0
				}
				return xs
			},
			Note: "2e6 exact unit increments; result stays within 2^53 exact-int range",
		},
		{
			Name: "cancellation_pairs", N: 2_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				for i := 0; i+1 < n; i += 2 {
					xs[i] = 1e15
					xs[i+1] = -1e15
				}
				if n%2 == 1 {
					xs[n-1] = 1.0
				}
				return xs
			},
			Note: "1e15/-1e15 cancelling pairs; exact sum is 0 (or 1), relative error undefined",
		},
		{
			Name: "catastrophic_cancellation_residual", N: 2_000_000,
			Gen: func(n int) []float64 {
				// Every pair (1e15 + 0.1, -1e15) has exact residual 0.1. In
				// binary64, 1e15+0.1 rounds to 1e15 (0.1 is below half an ulp),
				// so each pair actually contributes 0 after subtraction: the
				// information is lost in the stored inputs, not in the summation.
				xs := make([]float64, n)
				for i := 0; i+1 < n; i += 2 {
					xs[i] = 1e15 + 0.1
					xs[i+1] = -1e15
				}
				return xs
			},
			Note: "true residual exists over reals but vanishes in stored float64 values",
		},
		{
			Name: "cancellation_with_survivor", N: 1_000_000,
			Gen: func(n int) []float64 {
				// 999,999 cancelling pairs plus a 123.0 survivor: exact sum
				// 123 is tiny relative to the 1e15 terms, so relative error is
				// finite but dominated by a handful of ulps at 1e15 scale.
				xs := make([]float64, n)
				for i := 0; i+1 < n-1; i += 2 {
					xs[i] = 1e15
					xs[i+1] = -1e15
				}
				xs[n-1] = 123.0
				return xs
			},
			Note: "large cancelling terms with a small surviving real sum (123.0)",
		},
		{
			Name: "large_plus_tiny", N: 1_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				xs[0] = 1e16 // absorbent running sum: 1.0 < 0.5 ulp at this magnitude
				for i := 1; i < n; i++ {
					xs[i] = 1.0
				}
				return xs
			},
			Note: "one 1e16 anchor followed by 1e6 unit increments that vanish in addition",
		},
		{
			Name: "mixed_magnitudes", N: 2_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				// Deterministic LCG, no math/rand global state.
				seed := uint64(0x9e3779b97f4a7c15)
				for i := range xs {
					seed = seed*6364136223846793005 + 1442695040888963407
					// magnitudes spanning 1e-6 .. 1e12 with alternating sign
					mag := math.Pow(10, -6+18*float64(seed>>40)/(1<<24))
					if seed&1 == 1 {
						mag = -mag
					}
					xs[i] = mag
				}
				return xs
			},
			Note: "deterministic 18-decade signed mix; stresses association order",
		},
		{
			Name: "near_overflow_chains", N: 1_000_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				for i := range xs {
					if i%2 == 0 {
						xs[i] = 1e308
					} else {
						xs[i] = -1e308
					}
				}
				return xs
			},
			Note: "alternating ±1e308: intermediate inf can appear depending on association",
		},
		{
			Name: "realistic_counters", N: 500_000,
			Gen: func(n int) []float64 {
				xs := make([]float64, n)
				// sub-second jittery request counts around 100..1000 per sample
				seed := uint64(0x2545F4914F6CDD1D)
				for i := range xs {
					seed = seed*6364136223846793005 + 1
					xs[i] = 100 + float64(seed%901)
				}
				return xs
			},
			Note: "bounded positive operational counters (typical production shape)",
		},
	}
}
