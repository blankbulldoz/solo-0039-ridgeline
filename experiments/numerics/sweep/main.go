package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"ridgeline/experiments/numerics/lib"
)

// SweepPoint records one (n, condition) cell of the empirical error envelope.
type SweepPoint struct {
	N                int     `json:"n"`
	SequentialRelErr any     `json:"sequential_rel_err"`
	SequentialULP    float64 `json:"sequential_ulp"`
	PairwiseRelErr   any     `json:"pairwise_rel_err"`
	NeumaierRelErr   any     `json:"neumaier_rel_err"`
	TheoryBound      float64 `json:"theory_bound_sequential"`
	AbsorbedFraction float64 `json:"absorbed_fraction_of_increments"`
}

type SweepReport struct {
	Description string       `json:"description"`
	GrowthSweep []SweepPoint `json:"growth_sweep"`
	// AbsorptionSweep holds the running-sum magnitude at which a unit
	// increment first stops changing the sum (0.5 ulp threshold).
	AbsorptionSweep []AbsorptionPoint `json:"absorption_sweep"`
}

type AbsorptionPoint struct {
	RunningSum   float64 `json:"running_sum"`
	ULP          float64 `json:"ulp_at_that_magnitude"`
	UnitSurvives bool    `json:"unit_increment_survives_addition"`
}

func main() {
	outDir := "experiments/numerics/out"
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	rep := SweepReport{
		Description: "Empirical sequential-summation error envelope for the " +
			"production reducer. Inputs are identical 0.1 increments; theory bound " +
			"is (n-1)*eps/(1-(n-1)*eps) with eps=2^-53.",
	}

	// 1) Error growth vs n on a log grid, up to the 2,000,000 scan ceiling.
	for _, n := range []int{10, 100, 1000, 10000, 100000, 500000, 1000000, 2000000} {
		xs := make([]float64, n)
		for i := range xs {
			xs[i] = 0.1
		}
		exact := lib.ExactSum(xs)
		seq := lib.SeqSum(xs)
		pair := lib.PairwiseSum(xs)
		neu := lib.NeumaierSum(xs)
		rep.GrowthSweep = append(rep.GrowthSweep, SweepPoint{
			N:                n,
			SequentialRelErr: finite(math.Abs(seq-exact) / math.Abs(exact)),
			SequentialULP:    lib.ULP(seq, exact),
			PairwiseRelErr:   finite(math.Abs(pair-exact) / math.Abs(exact)),
			NeumaierRelErr:   finite(math.Abs(neu-exact) / math.Abs(exact)),
			TheoryBound:      lib.BoundSequential(n),
			AbsorbedFraction: 0,
		})
	}

	// 2) Absorption threshold: at what running-sum magnitude does adding 1.0
	//    become a no-op? This is the representability ceiling that no summation
	//    strategy can defeat, and it drives the rate/avg "data range" promise.
	for _, s := range []float64{1e12, 1e13, 1e14, 1e15, 9.007199254740992e15 /*2^53*/, 1e16, 1e17} {
		ulp := math.Ldexp(1.0, int(math.Floor(math.Log2(s)))-52)
		survives := (s + 1.0) != s
		rep.AbsorptionSweep = append(rep.AbsorptionSweep, AbsorptionPoint{
			RunningSum: s, ULP: ulp, UnitSurvives: survives,
		})
	}

	// 3) Absorbed-fraction sweep for the "large anchor + many ones" shape.
	for i := range rep.GrowthSweep {
		n := rep.GrowthSweep[i].N
		xs := make([]float64, n)
		xs[0] = 1e16
		for j := 1; j < n; j++ {
			xs[j] = 1.0
		}
		exact := lib.ExactSum(xs)
		got := lib.SeqSum(xs)
		lost := math.Abs(exact-got) / float64(n-1)
		rep.GrowthSweep[i].AbsorbedFraction = lost
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "sweep_report.json"), data, 0o644); err != nil {
		panic(err)
	}

	printSweep(rep)
}

func finite(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return v
}

func printSweep(r SweepReport) {
	fmt.Println("error growth vs point count (0.1 increments):")
	fmt.Println("        n   seq rel.err   seq ULP   pair rel.err   neumaier   theory bound")
	for _, p := range r.GrowthSweep {
		fmt.Printf("%9d %13v %9.0f %14v %10v %13.2e\n", p.N, p.SequentialRelErr,
			p.SequentialULP, p.PairwiseRelErr, p.NeumaierRelErr, p.TheoryBound)
	}
	fmt.Println()
	fmt.Println("absorption thresholds (when does +1.0 stop changing the sum?):")
	for _, a := range r.AbsorptionSweep {
		fmt.Printf("sum=%-22g ulp=%-12g unit_survives=%v\n", a.RunningSum, a.ULP, a.UnitSurvives)
	}
	fmt.Println()
	fmt.Println("fraction of unit increments absorbed by a 1e16 anchor:")
	for _, p := range r.GrowthSweep {
		fmt.Printf("n=%9d absorbed_fraction=%.4f\n", p.N, p.AbsorbedFraction)
	}
}
