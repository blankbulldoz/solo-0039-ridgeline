// Command numerics runs the float64 reduction experiments for sum, avg and
// grouped rate and emits machine-readable results plus a human summary.
// It never touches production packages; the production ordering is mirrored
// in strategies.go and cross-checked against the live engine in the e2e
// subcommand (see experiments/numerics/e2e).
package main

import (
	"encoding/json"
	"flag"
	"math"
	"os"
	"path/filepath"
	"time"

	"ridgeline/experiments/numerics/lib"
)

type StrategyResult struct {
	Strategy    string  `json:"strategy"`
	Value       float64 `json:"value"`
	Exact       float64 `json:"exact"`
	AbsErr      float64 `json:"abs_err"`
	RelErr      float64 `json:"rel_err"`
	ULP         float64 `json:"ulp"`
	ExactZero   bool    `json:"exact_zero"`
	NonFinite   bool    `json:"non_finite"`
	NanosPerOp  int64   `json:"nanos_per_op"`
	BytesPerEl  int     `json:"extra_bytes_per_element"`
	OrderChange bool    `json:"value_changes_under_reorder"`
}

type ScenarioReport struct {
	Scenario      string           `json:"scenario"`
	N             int              `json:"n"`
	Note          string           `json:"note"`
	TheoryBound   float64          `json:"sequential_rel_bound_theory"`
	Ordering      []OrderingProbe  `json:"ordering_probes"`
	Strategies    []StrategyResult `json:"strategies"`
	AvgStrategies []StrategyResult `json:"avg_strategies"`
}

type OrderingProbe struct {
	Order       string  `json:"order"`
	SeqValue    float64 `json:"seq_value"`
	PairValue   float64 `json:"pairwise_value"`
	NeumaierVal float64 `json:"neumaier_value"`
	Exact       float64 `json:"exact"`
	SeqRelErr   float64 `json:"seq_rel_err"`
	SeqULP      float64 `json:"seq_ulp"`
	NonFinite   bool    `json:"non_finite"`
}

type RateCase struct {
	Name          string  `json:"name"`
	Series        int     `json:"series"`
	PointsPerSery int     `json:"points_per_series"`
	Production    float64 `json:"production_value"`
	Compensated   float64 `json:"neumaier_value"`
	Exact         float64 `json:"exact_value"`
	RelErr        float64 `json:"production_rel_err"`
	ULP           float64 `json:"production_ulp"`
	NonFinite     bool    `json:"non_finite"`
	Note          string  `json:"note"`
}

type OverflowCase struct {
	Name       string    `json:"name"`
	Inputs     []float64 `json:"inputs"`
	Seq        float64   `json:"seq"`
	Pairwise   float64   `json:"pairwise"`
	Neumaier   float64   `json:"neumaier"`
	Exact      float64   `json:"exact"`
	Production string    `json:"production_outcome"`
	Note       string    `json:"note"`
}

type Report struct {
	GeneratedAt   string           `json:"generated_at_utc"`
	GoVersion     string           `json:"go_version"`
	ScanBudgetMax int              `json:"scan_budget_max"`
	SumScenarios  []ScenarioReport `json:"sum_scenarios"`
	RateCases     []RateCase       `json:"rate_cases"`
	OverflowCases []OverflowCase   `json:"overflow_cases"`
}

func timeit(fn func()) int64 {
	// One warm-up, then timed single pass over the full slice (data sizes are
	// already large; repeated iterations would add minutes for little gain).
	fn()
	start := time.Now()
	fn()
	return time.Since(start).Nanoseconds()
}

func evaluate(name string, xs []float64, exact float64, fn func([]float64) float64,
	extraBytes int, nanos int64, reorders bool) StrategyResult {
	got := fn(xs)
	rel, abs, zero := lib.RelErr(got, exact)
	r := StrategyResult{
		Strategy: name, Value: got, Exact: exact, AbsErr: abs, RelErr: rel,
		ULP: lib.ULP(got, exact), ExactZero: zero,
		NonFinite:  math.IsInf(got, 0) || math.IsNaN(got),
		NanosPerOp: nanos, BytesPerEl: extraBytes, OrderChange: reorders,
	}
	return r
}

func main() {
	outDir := flag.String("out", "experiments/numerics/out", "output directory")
	flag.Parse()

	report := Report{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		GoVersion:     goVersion(),
		ScanBudgetMax: 2_000_000,
	}

	for _, sc := range lib.Scenarios() {
		xs := sc.Gen(sc.N)
		exact := lib.ExactSum(xs)
		sr := ScenarioReport{
			Scenario: sc.Name, N: sc.N, Note: sc.Note,
			TheoryBound: lib.BoundSequential(sc.N),
		}

		// Ordering probes: same multiset, three deterministic associations.
		for _, mode := range []string{"as_generated", "reverse", "stride"} {
			var reordered []float64
			if mode == "as_generated" {
				reordered = xs
			} else {
				reordered = lib.Reorder(xs, mode)
			}
			seq := lib.SeqSum(reordered)
			pair := lib.PairwiseSum(reordered)
			neu := lib.NeumaierSum(reordered)
			rel, _, _ := lib.RelErr(seq, exact)
			sr.Ordering = append(sr.Ordering, OrderingProbe{
				Order: mode, SeqValue: seq, PairValue: pair, NeumaierVal: neu,
				Exact: exact, SeqRelErr: rel, SeqULP: lib.ULP(seq, exact),
				NonFinite: math.IsInf(seq, 0) || math.IsNaN(seq),
			})
		}

		// Strategy comparison with wall-clock cost.
		nsSeq := timeit(func() { sink = lib.SeqSum(xs) })
		nsPair := timeit(func() { sink = lib.PairwiseSum(xs) })
		nsKahan := timeit(func() { sink = lib.KahanSum(xs) })
		nsNeu := timeit(func() { sink = lib.NeumaierSum(xs) })
		nsSort := timeit(func() { sink = lib.SortedSeqSum(xs) })
		nsExact := timeit(func() { sink = lib.ExactSum(xs) })

		base := lib.SeqSum(xs)
		rev := lib.SeqSum(lib.Reorder(xs, "reverse"))
		stride := lib.SeqSum(lib.Reorder(xs, "stride"))
		reorderChanges := base != rev || base != stride

		sr.Strategies = []StrategyResult{
			evaluate("sequential_production", xs, exact, lib.SeqSum, 0, nsSeq, reorderChanges),
			evaluate("pairwise_tree", xs, exact, lib.PairwiseSum, 0, nsPair,
				lib.PairwiseSum(xs) != lib.PairwiseSum(lib.Reorder(xs, "reverse"))),
			evaluate("kahan", xs, exact, lib.KahanSum, 16, nsKahan, false),
			evaluate("neumaier", xs, exact, lib.NeumaierSum, 16, nsNeu, false),
			evaluate("sorted_then_sequential", xs, exact, lib.SortedSeqSum, 8, nsSort, false),
			evaluate("bigfloat_exact_then_round", xs, exact, lib.ExactSum, 0, nsExact, false),
		}

		// Avg uses the SAME accumulator as sum, then one division.
		exactAvg := lib.ExactAvg(xs)
		avgEval := func(name string, sum float64, nanos int64) StrategyResult {
			got := sum / float64(len(xs))
			rel, abs, zero := lib.RelErr(got, exactAvg)
			return StrategyResult{
				Strategy: name, Value: got, Exact: exactAvg, AbsErr: abs, RelErr: rel,
				ULP: lib.ULP(got, exactAvg), ExactZero: zero,
				NonFinite: math.IsInf(got, 0) || math.IsNaN(got), NanosPerOp: nanos,
			}
		}
		// The big.Float value is already the exact average, so report it
		// directly instead of dividing it by n a second time.
		avgDirect := func(name string, got float64, nanos int64) StrategyResult {
			rel, abs, zero := lib.RelErr(got, exactAvg)
			return StrategyResult{
				Strategy: name, Value: got, Exact: exactAvg, AbsErr: abs, RelErr: rel,
				ULP: lib.ULP(got, exactAvg), ExactZero: zero,
				NonFinite: math.IsInf(got, 0) || math.IsNaN(got), NanosPerOp: nanos,
			}
		}
		sr.AvgStrategies = []StrategyResult{
			avgEval("sum_then_divide_production", lib.SeqSum(xs), nsSeq),
			avgEval("neumaier_sum_then_divide", lib.NeumaierSum(xs), nsNeu),
			avgDirect("bigfloat_exact_then_divide", exactAvg, nsExact),
		}
		report.SumScenarios = append(report.SumScenarios, sr)
	}

	report.RateCases = rateCases()
	report.OverflowCases = overflowCases()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	path := filepath.Join(*outDir, "numerics_report.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		panic(err)
	}
	printSummary(report, path)
}

var sink float64

func goVersion() string {
	return runtimeVersion()
}
