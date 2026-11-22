package main

import (
	"fmt"
	"math"
	"runtime"

	"ridgeline/experiments/numerics/lib"
)

func runtimeVersion() string { return runtime.Version() }

// rateCases builds deterministic counter scenarios. Timestamps are one second
// apart and stay well inside the [-2^52, 2^52] ms envelope.
func rateCases() []RateCase {
	cases := []RateCase{}

	// 1) Many small exact increments per series, many series in one group.
	build := func(series, pts int, startVal, step float64, withResets bool) []lib.RateInput {
		ins := make([]lib.RateInput, series)
		for s := range ins {
			points := make([]lib.Point, pts)
			v := startVal
			for i := 0; i < pts; i++ {
				points[i] = lib.Point{Time: int64(i) * 1000, Value: v}
				if withResets && i > 0 && i%97 == 0 {
					v = step // reset: next value is small, delta becomes negative -> restart rule
				} else {
					v += step
				}
			}
			ins[s] = lib.RateInput{Points: points}
		}
		return ins
	}

	add := func(name string, ins []lib.RateInput, note string) {
		prod := lib.GroupedRate(ins)
		comp := 0.0
		for _, in := range ins {
			comp += lib.PerSeriesRateCompensated(in)
		}
		exact := lib.GroupedRateExact(ins)
		rel, _, _ := lib.RelErr(prod, exact)
		cases = append(cases, RateCase{
			Name:   name,
			Series: len(ins), PointsPerSery: len(ins[0].Points),
			Production: prod, Compensated: comp, Exact: exact,
			RelErr: rel, ULP: lib.ULP(prod, exact),
			NonFinite: math.IsInf(prod, 0) || math.IsNaN(prod), Note: note,
		})
	}

	add("rate_small_steps_200x10k", build(200, 10_000, 0, 0.1, false),
		"200 series x 10k points, +0.1/s per series; classic non-exact decimal drift")
	add("rate_exact_unit_200x10k", build(200, 10_000, 0, 1, false),
		"same shape with integer steps; increase stays exactly representable up to 2^53")
	add("rate_with_resets_100x20k", build(100, 20_000, 1e9, 1, true),
		"large base counters with periodic resets exercising the restart rule")

	// 2) Absorbent counter: huge plateau values plus tiny per-second growth.
	{
		pts := 50_000
		ins := []lib.RateInput{{Points: make([]lib.Point, pts)}}
		for i := range ins[0].Points {
			// 1e16 base; +1 each second. Above 2^53 the unit steps vanish in
			// storage arithmetic itself, so deltas are exactly 0: a visibility/
			// representability limit, not an accumulation-order artifact.
			ins[0].Points[i] = lib.Point{Time: int64(i) * 1000, Value: 1e16 + float64(i)}
		}
		add("rate_absorbent_counter_1e16", ins,
			"unit increments at 1e16 magnitude disappear in float64 subtraction")
	}

	// 3) Rate cancellation analogue: resets interleave with large increases so
	//    the increase sum mixes magnitudes inside one series.
	{
		pts := 100_000
		ins := []lib.RateInput{{Points: make([]lib.Point, pts)}}
		v := 0.0
		for i := range ins[0].Points {
			ins[0].Points[i] = lib.Point{Time: int64(i) * 1000, Value: v}
			if i%2 == 0 {
				v += 1e12
			} else {
				v = 1 // reset every other step
			}
		}
		add("rate_alternating_huge_increase_and_reset", ins,
			"1e12 increases alternate with resets; reset deltas added to a large running sum")
	}

	return cases
}

// overflowCases probes the fail-closed IsInf/IsNaN check in reduce.go:76 and
// rate.go:44 and the stickiness of intermediate infinities.
func overflowCases() []OverflowCase {
	cases := []OverflowCase{
		{
			Name:       "two_max_then_subtract",
			Inputs:     []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64},
			Production: "aggregation overflow -> 422 (intermediate +inf is sticky)",
			Note:       "mathematically finite (+MaxFloat64), but sequential order overflows first",
		},
		{
			Name:       "subtract_first_then_max",
			Inputs:     []float64{-math.MaxFloat64, math.MaxFloat64, math.MaxFloat64},
			Production: "aggregation overflow -> 422 (same sticky +inf)",
			Note:       "same multiset, different association; exact result is +MaxFloat64",
		},
		{
			Name:       "true_divergence",
			Inputs:     []float64{math.MaxFloat64, math.MaxFloat64},
			Production: "aggregation overflow -> 422 (correct: true sum exceeds range)",
			Note:       "no finite float64 answer exists; rejection is the only safe outcome",
		},
		{
			// This is an ingest-validation probe only; no real store can contain
			// +Inf, so the "input" is recorded symbolically via Production/Note.
			Name:       "inf_inputs_rejected_at_ingest",
			Inputs:     []float64{math.MaxFloat64, 1},
			Production: "400 at write time (ValidateObservation rejects non-finite)",
			Note: "infinity can only arise inside arithmetic, never from stored points; " +
				"ValidateObservation (model/validate.go) rejects math.IsInf/IsNaN",
		},
	}
	for i := range cases {
		c := &cases[i]
		c.Seq = lib.SeqSum(c.Inputs)
		c.Pairwise = lib.PairwiseSum(c.Inputs)
		c.Neumaier = lib.NeumaierSum(c.Inputs)
		c.Exact = lib.ExactSum(c.Inputs)
	}
	return cases
}

func printSummary(r Report, path string) {
	fmt.Printf("Ridgeline float64 reduction experiments (%s)\n", r.GoVersion)
	fmt.Printf("full machine-readable report: %s\n\n", path)
	fmt.Printf("%-34s %8s %14s %12s %10s %10s\n", "scenario", "n", "seq rel.err", "seq ULP", "neumaier", "exact cost")
	for _, s := range r.SumScenarios {
		var seq, neu float64
		var ulp float64
		var exactNanos int64
		for _, st := range s.Strategies {
			switch st.Strategy {
			case "sequential_production":
				seq, ulp = st.RelErr, st.ULP
			case "neumaier":
				neu = st.RelErr
				if st.ExactZero {
					neu = math.NaN()
				}
			case "bigfloat_exact_then_round":
				exactNanos = st.NanosPerOp
			}
		}
		seqS := fmt.Sprintf("%.2e", seq)
		if math.IsInf(seq, 1) {
			seqS = "undef(0)"
		}
		ulpS := fmt.Sprintf("%.1f", ulp)
		if math.IsInf(ulp, 1) {
			ulpS = "inf"
		}
		neuS := fmt.Sprintf("%.2e", neu)
		if math.IsNaN(neu) {
			neuS = "undef(0)"
		}
		fmt.Printf("%-34s %8d %14s %12s %10s %8dms\n", s.Scenario, s.N, seqS, ulpS, neuS,
			exactNanos/1_000_000)
	}
	fmt.Println()
	fmt.Printf("%-38s %8s %8s %14s %10s\n", "rate case", "series", "pts/s", "prod rel.err", "ULP")
	for _, c := range r.RateCases {
		rel := fmt.Sprintf("%.2e", c.RelErr)
		if math.IsInf(c.RelErr, 1) {
			rel = "undef(0)"
		}
		fmt.Printf("%-38s %8d %8d %14s %10.1f\n", c.Name, c.Series, c.PointsPerSery, rel, c.ULP)
	}
	fmt.Println()
	fmt.Println("overflow / non-finite handling:")
	for _, c := range r.OverflowCases {
		fmt.Printf("  %-30s seq=%v pairwise=%v neumaier=%v exact=%v\n", c.Name, c.Seq, c.Pairwise, c.Neumaier, c.Exact)
	}
}
