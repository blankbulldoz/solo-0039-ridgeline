package lib

import "math/big"

// Point mirrors model.Sample for the rate experiments.
type Point struct {
	Time  int64
	Value float64
}

// RateInput is one counter series inside one grouped window. Production
// requires len(Points) >= 2 and nonnegative values; the reset rule turns a
// negative delta into "restart at the current value" (delta = current).
type RateInput struct {
	Points []Point
}

// PerSeriesRate reproduces internal/window.Rates for one series/window:
// sum of reset-aware positive deltas divided by elapsed seconds between
// the first and last observed point. All arithmetic is binary64, in
// timestamp order (scan.go sorts points by time before the reducer runs).
func PerSeriesRate(in RateInput) float64 {
	if len(in.Points) < 2 {
		return 0
	}
	increase := 0.0
	for i := 1; i < len(in.Points); i++ {
		delta := in.Points[i].Value - in.Points[i-1].Value
		if delta < 0 {
			delta = in.Points[i].Value
		}
		increase += delta
	}
	elapsed := float64(in.Points[len(in.Points)-1].Time-in.Points[0].Time) / 1000
	return increase / elapsed
}

// PerSeriesRateCompensated applies Neumaier summation to the same deltas;
// deltas, reset rule and division are otherwise identical to production.
func PerSeriesRateCompensated(in RateInput) float64 {
	if len(in.Points) < 2 {
		return 0
	}
	deltas := make([]float64, 0, len(in.Points)-1)
	for i := 1; i < len(in.Points); i++ {
		delta := in.Points[i].Value - in.Points[i-1].Value
		if delta < 0 {
			delta = in.Points[i].Value
		}
		deltas = append(deltas, delta)
	}
	increase := NeumaierSum(deltas)
	elapsed := float64(in.Points[len(in.Points)-1].Time-in.Points[0].Time) / 1000
	return increase / elapsed
}

// PerSeriesRateExact computes the increase as an exact big.Float sum of the
// float64 deltas, then performs a single binary64 division. It isolates
// accumulation error from the (already tiny) denominator conversion error.
func PerSeriesRateExact(in RateInput) float64 {
	if len(in.Points) < 2 {
		return 0
	}
	acc := new(big.Float).SetPrec(128)
	tmp := new(big.Float).SetPrec(128)
	for i := 1; i < len(in.Points); i++ {
		delta := in.Points[i].Value - in.Points[i-1].Value
		if delta < 0 {
			delta = in.Points[i].Value
		}
		tmp.SetFloat64(delta)
		acc.Add(acc, tmp)
	}
	elapsedMS := new(big.Float).SetPrec(128).SetInt64(
		in.Points[len(in.Points)-1].Time - in.Points[0].Time)
	thousand := new(big.Float).SetPrec(128).SetInt64(1000)
	elapsed := new(big.Float).SetPrec(128).Quo(elapsedMS, thousand)
	acc.Quo(acc, elapsed)
	f, _ := acc.Float64()
	return f
}

// GroupedRate reproduces the production grouped reducer: per-series rates are
// computed first, then added into one cell in the order given.
func GroupedRate(inputs []RateInput) float64 {
	v := 0.0
	for _, in := range inputs {
		v += PerSeriesRate(in)
	}
	return v
}

// GroupedRateExact sums exact per-series rates (big.Float) and rounds once.
func GroupedRateExact(inputs []RateInput) float64 {
	acc := new(big.Float).SetPrec(128)
	tmp := new(big.Float).SetPrec(128)
	for _, in := range inputs {
		tmp.SetFloat64(PerSeriesRateExact(in))
		acc.Add(acc, tmp)
	}
	f, _ := acc.Float64()
	return f
}
