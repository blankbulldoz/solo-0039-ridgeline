// Command e2e drives the real Ridgeline engine (in-process bbolt, real
// window.Aggregate/window.Rates) to separate three causes of query-result
// changes:
//
//   - rounding drift:     same visible data, compare the live cell to a
//     big.Float reference computed from the exact points the engine returned;
//   - business semantics: same visible data, compare function definitions
//     (sum vs avg weighting; rate reset rule vs plain difference quotient);
//   - data visibility:    change what is visible (delete / retention /
//     correction / seal+compact) and re-run the identical query.
//
// It also asserts that physical layout (head vs sealed vs compacted blocks)
// never changes a cell, which is the reproducibility guarantee operators need.
package main

import (
	"encoding/json"
	"flag"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
)

type Point struct {
	Labels model.Labels `json:"-"`
	Time   int64
	Value  float64
}

type Finding struct {
	ID          string  `json:"id"`
	Class       string  `json:"class"` // rounding | business_definition | visibility | reproducibility | overflow
	Description string  `json:"description"`
	CellValue   float64 `json:"cell_value"`
	Reference   float64 `json:"reference_value"`
	AbsErr      float64 `json:"abs_err"`
	RelErr      any     `json:"rel_err"`
	ULP         any     `json:"ulp"`
	Detail      string  `json:"detail"`
}

type Report struct {
	GoVersion string    `json:"go_version"`
	Findings  []Finding `json:"findings"`
}

const space = "lab"

func main() {
	outDir := flag.String("out", "experiments/numerics/out", "output directory")
	flag.Parse()

	dir, err := os.MkdirTemp("", "ridge-e2e-*")
	must(err)
	defer os.RemoveAll(dir)
	e, err := engine.Open(filepath.Join(dir, "db"))
	must(err)
	defer e.Close()

	mustNS(e.Create(model.Namespace{
		Name: "lab", RetentionMS: model.MaxTime, LatenessMS: model.MaxTime,
		FutureMS: model.MaxTime, MaxSeries: 1000, MaxWrite: 50000, MaxScan: 2_000_000,
	}))

	rep := Report{GoVersion: goVer}
	f := &rep.Findings

	// ---- workload: two zones, many small decimal increments -----------------
	labelsA := model.Labels{"__name__": "metric", "zone": "a"}
	labelsB := model.Labels{"__name__": "metric", "zone": "b"}
	write := func(pts []Point, now int64) {
		obs := make([]model.Observation, len(pts))
		for i, p := range pts {
			obs[i] = model.Observation{Labels: p.Labels, Time: p.Time, Value: p.Value}
		}
		_, err := e.Write(space, model.WriteRequest{Now: now, Points: obs})
		must(err)
	}

	pts := make([]Point, 0, 2000)
	for i := 0; i < 1000; i++ {
		pts = append(pts, Point{labelsA, int64(i), 0.1})
	}
	for i := 0; i < 1000; i++ {
		pts = append(pts, Point{labelsB, int64(i), 0.3})
	}
	// chunk writes under MaxWrite=50000 (single batch here is fine)
	write(pts, 1000)

	aggQuery := model.Query{
		Start: 0, End: 1000, Step: 1000, Origin: 0,
		Function: "sum", GroupBy: []string{"zone"},
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "metric"}},
	}
	avgQuery := aggQuery
	avgQuery.Function = "avg"

	// ---------- F1 rounding drift, live engine vs exact reference -------------
	sumRes, err := e.Aggregate(space, aggQuery)
	must(err)
	avgRes, err := e.Aggregate(space, avgQuery)
	must(err)
	raw, err := e.Range(space, model.Query{
		Start: 0, End: 1000, Matchers: aggQuery.Matchers,
	})
	must(err)

	exactByZone := map[string]*big.Float{}
	countByZone := map[string]int{}
	for _, sp := range raw.Series {
		zone := sp.Series.Labels["zone"]
		acc := big.NewFloat(0).SetPrec(128)
		tmp := big.NewFloat(0).SetPrec(128)
		for _, p := range sp.Points {
			tmp.SetFloat64(p.Value)
			acc.Add(acc, tmp)
		}
		exactByZone[zone] = acc
		countByZone[zone] = len(sp.Points)
	}
	for _, cell := range sumRes.Cells {
		zone := cell.Labels["zone"]
		exact, _ := exactByZone[zone].Float64()
		*f = append(*f, mkFinding("F1-sum-"+zone, "rounding",
			"live sum cell vs big.Float sum of the same visible points",
			cell.Value, exact))
	}
	for _, cell := range avgRes.Cells {
		zone := cell.Labels["zone"]
		avgExact := new(big.Float).SetPrec(128).Quo(exactByZone[zone],
			big.NewFloat(float64(countByZone[zone])))
		exact, _ := avgExact.Float64()
		*f = append(*f, mkFinding("F1-avg-"+zone, "rounding",
			"live avg cell (observation-weighted) vs exact mean of the same points",
			cell.Value, exact))
	}

	// ---------- F2 business-definition differences, identical visible data ----
	// avg is NOT sum/series; with unequal series populations it differs from
	// any per-series mean. Here both zones have 1000 points so avg = sum/1000;
	// add a third zone with a different count to expose weighting semantics.
	labelsC := model.Labels{"__name__": "metric", "zone": "c"}
	cpts := []Point{}
	for i := 0; i < 10; i++ { // only 10 observations
		cpts = append(cpts, Point{labelsC, int64(i), 1.0})
	}
	write(cpts, 1000)
	// Re-snapshot every visible metric series (now including zone c) so the
	// reference uses exactly the same rows as the aggregate under test.
	raw, err = e.Range(space, model.Query{
		Start: 0, End: 1000, Matchers: aggQuery.Matchers,
	})
	must(err)
	groupAll := aggQuery
	groupAll.GroupBy = nil // one group over every selected series
	groupAllSum, err := e.Aggregate(space, groupAll)
	must(err)
	groupAllAvg := groupAll
	groupAllAvg.Function = "avg"
	avgAll, err := e.Aggregate(space, groupAllAvg)
	must(err)
	sumCell := groupAllSum.Cells[0].Value
	avgCell := avgAll.Cells[0].Value
	// observation-weighted mean: total sum / total points (2010)
	naiveSeriesMean := func() float64 {
		// equally weighting each series mean instead of each observation;
		// raw now contains all three metric series including zone c.
		means := 0.0
		for _, sp := range raw.Series {
			s := 0.0
			for _, p := range sp.Points {
				s += p.Value
			}
			means += s / float64(len(sp.Points))
		}
		return means / float64(len(raw.Series))
	}()
	*f = append(*f, Finding{
		ID: "F2-avg-weighting", Class: "business_definition",
		Description: "avg weights observations (sum/count=2010), not series; " +
			"the equal-series-weight mean is a different metric",
		CellValue: avgCell, Reference: naiveSeriesMean,
		AbsErr: math.Abs(avgCell - naiveSeriesMean),
		RelErr: finite(math.Abs(avgCell-naiveSeriesMean) / math.Abs(naiveSeriesMean)),
		ULP:    nil,
		Detail: "sum cell=" + fp(sumCell) + " avg cell=" + fp(avgCell) +
			" count=" + itoa(avgAll.Cells[0].Count),
	})

	// rate reset rule vs plain difference quotient on a decreasing counter
	counter := model.Labels{"__name__": "hits", "zone": "a"}
	write([]Point{
		{counter, 0, 1000}, {counter, 1000, 1100},
		{counter, 2000, 50}, // reset: plain quotient goes negative, rate restarts
		{counter, 3000, 120},
	}, 4000)
	rateQ := model.Query{
		Start: 0, End: 4000, Step: 4000, Origin: 0, Function: "rate",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "hits"}},
	}
	rateRes, err := e.Aggregate(space, rateQ)
	must(err)
	// production: (1100-1000) + 50 + (120-50) = 220 over 3s = 73.333...
	prodRate := rateRes.Cells[0].Value
	plainQuotient := (120.0 - 1000.0) / 3.0 // negative: meaningless for a counter
	*f = append(*f, Finding{
		ID: "F3-rate-reset-rule", Class: "business_definition",
		Description: "rate treats a decrease as reset-to-zero (adds current value); " +
			"plain end-minus-start quotient would report a negative rate",
		CellValue: prodRate, Reference: plainQuotient,
		AbsErr: math.Abs(prodRate - plainQuotient),
		RelErr: nil, ULP: nil,
		Detail: "production increase=220.0 / 3s; negative value would 400 the rate call",
	})

	// rate on a negative stored value must be rejected
	write([]Point{{model.Labels{"__name__": "hits", "zone": "neg"}, 0, -1}}, 4000)
	negQ := rateQ
	negQ.Matchers = []model.Matcher{{Label: "zone", Op: "=", Value: "neg"}}
	if _, err := e.Aggregate(space, negQ); err == nil {
		panic("expected negative-value rate rejection")
	} else {
		*f = append(*f, Finding{
			ID: "F4-rate-negative-rejected", Class: "business_definition",
			Description: "rate rejects any negative stored value with invalid_request",
			Detail:      err.Error(),
		})
	}

	// grouped rate: three counter series collapse into one cell; the engine
	// sums per-series rates in catalog-ID order. Compare against an exact
	// big.Float evaluation of the same reset-aware deltas over visible points.
	grp := []model.Labels{
		{"__name__": "grp", "shard": "1"},
		{"__name__": "grp", "shard": "2"},
		{"__name__": "grp", "shard": "3"},
	}
	gpts := []Point{}
	for si, lb := range grp {
		v := float64(si * 1000)
		for i := 0; i < 2000; i++ {
			if i > 0 && i%500 == 0 {
				v = float64(si) // periodic reset
			}
			v += 0.1
			gpts = append(gpts, Point{lb, int64(i) * 1000, v})
		}
	}
	write(gpts, 5000)
	grpQ := model.Query{
		Start: 0, End: 2000 * 1000, Step: 2000 * 1000, Origin: 0, Function: "rate",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "grp"}},
	}
	grpRes, err := e.Aggregate(space, grpQ)
	must(err)
	grpRaw, err := e.Range(space, model.Query{
		Start: 0, End: 2000 * 1000, Matchers: grpQ.Matchers,
	})
	must(err)
	exactRate := big.NewFloat(0).SetPrec(128)
	tmp := big.NewFloat(0).SetPrec(128)
	for _, sp := range grpRaw.Series {
		inc := big.NewFloat(0).SetPrec(128)
		for i := 1; i < len(sp.Points); i++ {
			d := sp.Points[i].Value - sp.Points[i-1].Value
			if d < 0 {
				d = sp.Points[i].Value
			}
			tmp.SetFloat64(d)
			inc.Add(inc, tmp)
		}
		elapsed := big.NewFloat(float64(
			sp.Points[len(sp.Points)-1].Time-sp.Points[0].Time) / 1000).SetPrec(128)
		exactRate.Add(exactRate, new(big.Float).SetPrec(128).Quo(inc, elapsed))
	}
	exactRateF, _ := exactRate.Float64()
	*f = append(*f, mkFinding("F4b-grouped-rate-rounding", "rounding",
		"grouped rate cell (3 series x 2000 points, reset-aware) vs exact reference",
		grpRes.Cells[0].Value, exactRateF))

	// ---------- F5 visibility: deletion changes the cell, layout does not -----
	before, err := e.Aggregate(space, aggQuery)
	must(err)
	// seal: head -> blocks, identical query must return identical cells
	_, err = e.Seal(space)
	must(err)
	afterSeal, err := e.Aggregate(space, aggQuery)
	must(err)
	*f = append(*f, compareCells("F5-seal-invariance", "reproducibility",
		"sealing head points into gzip blocks must not change any cell",
		before.Cells, afterSeal.Cells))

	// delete half of zone a; cell must change by exactly the visible removal
	_, err = e.Delete(space, model.Query{
		Start: 0, End: 500, Matchers: []model.Matcher{
			{Label: "__name__", Op: "=", Value: "metric"},
			{Label: "zone", Op: "=", Value: "a"},
		},
	})
	must(err)
	afterDelete, err := e.Aggregate(space, aggQuery)
	must(err)
	del := diffCell(before.Cells, afterDelete.Cells, "a")
	*f = append(*f, Finding{
		ID: "F6-delete-visibility", Class: "visibility",
		Description: "tombstone hides 500 zone-a points; sum drops by exactly 500*0.1",
		CellValue:   del.after, Reference: del.before, AbsErr: math.Abs(del.after - del.before),
		RelErr: finite(math.Abs(del.after-del.before) / math.Abs(del.before)), ULP: nil,
		Detail: "expected delta=-50 stored-as-float64; observed delta=" + fp(del.after-del.before),
	})

	// correction at an existing timestamp: latest sequence wins, sum changes once
	write([]Point{{labelsA, 600, 5.0}}, 5000)
	afterCorrect, err := e.Aggregate(space, aggQuery)
	must(err)
	dc := diffCell(afterDelete.Cells, afterCorrect.Cells, "a")
	*f = append(*f, Finding{
		ID: "F7-correction-visibility", Class: "visibility",
		Description: "re-writing t=600 replaces 0.1 with 5.0 (highest sequence wins)",
		CellValue:   dc.after, Reference: dc.before, AbsErr: math.Abs(dc.after - dc.before),
		RelErr: finite(math.Abs(dc.after-dc.before) / math.Abs(dc.before)), ULP: nil,
		Detail: "expected delta=4.9; observed delta=" + fp(dc.after-dc.before),
	})

	// compact: merges blocks, applies tombstones, rebuilds; cells must be stable
	_, err = e.Compact(space)
	must(err)
	afterCompact, err := e.Aggregate(space, aggQuery)
	must(err)
	*f = append(*f, compareCells("F8-compact-invariance", "reproducibility",
		"compaction resolves blocks+tombstones but must preserve every visible cell",
		afterCorrect.Cells, afterCompact.Cells))

	// retention: a dedicated namespace with a short retention window lets the
	// monotonic floor move past every point; the original namespace cannot,
	// because its RetentionMS is MaxTime and the floor can never exceed 0.
	mustNS(e.Create(model.Namespace{
		Name: "labret", RetentionMS: 1000, LatenessMS: model.MaxTime,
		FutureMS: model.MaxTime, MaxSeries: 100, MaxWrite: 50000, MaxScan: 2_000_000,
	}))
	retLab := model.Labels{"__name__": "metric", "zone": "a"}
	rpts := make([]Point, 100)
	for i := range rpts {
		rpts[i] = Point{retLab, int64(i), 0.1}
	}
	robs := make([]model.Observation, len(rpts))
	for i, p := range rpts {
		robs[i] = model.Observation{Labels: p.Labels, Time: p.Time, Value: p.Value}
	}
	_, err = e.Write("labret", model.WriteRequest{Now: 100, Points: robs})
	must(err)
	retQ := model.Query{
		Start: 0, End: 100, Step: 100, Origin: 0, Function: "sum",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "metric"}},
	}
	retBefore, err := e.Aggregate("labret", retQ)
	must(err)
	// floor = now - 1000; now=2000 -> floor=1000, strictly above every sample.
	_, err = e.Retain("labret", 2000)
	must(err)
	retAfter, err := e.Aggregate("labret", retQ)
	must(err)
	*f = append(*f, Finding{
		ID: "F9-retention-visibility", Class: "visibility",
		Description: "monotonic retention floor physically removes old points; cells disappear",
		CellValue:   float64(len(retAfter.Cells)), Reference: float64(len(retBefore.Cells)),
		AbsErr: float64(len(retBefore.Cells) - len(retAfter.Cells)), RelErr: nil, ULP: nil,
		Detail: "cells before=" + itoa(len(retBefore.Cells)) +
			" after=" + itoa(len(retAfter.Cells)),
	})

	// ---------- F10 overflow fail-closed through the real engine ---------------
	huge := model.Labels{"__name__": "big"}
	bigPts := make([]Point, 0, 3000)
	for i := 0; i < 3000; i++ {
		bigPts = append(bigPts, Point{huge, int64(i), math.MaxFloat64 / 1000})
	}
	// split into batches <= MaxWrite
	for start := 0; start < len(bigPts); start += 1000 {
		end := start + 1000
		if end > len(bigPts) {
			end = len(bigPts)
		}
		write(bigPts[start:end], int64(10000+start))
	}
	bigQ := model.Query{
		Start: 0, End: 3000, Step: 3000, Origin: 0, Function: "sum",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "big"}},
	}
	if _, err := e.Aggregate(space, bigQ); err == nil {
		panic("expected overflow rejection from engine")
	} else {
		*f = append(*f, Finding{
			ID: "F10-engine-overflow", Class: "overflow",
			Description: "engine rejects a cell whose running sum reaches +/-Inf " +
				"with capacity/422 instead of emitting non-finite JSON",
			Detail: err.Error(),
		})
	}

	must(os.MkdirAll(*outDir, 0o755))
	path := filepath.Join(*outDir, "e2e_report.json")
	data, err := json.MarshalIndent(rep, "", "  ")
	must(err)
	must(os.WriteFile(path, data, 0o644))
	printFindings(rep, path)
}

// ---- helpers ---------------------------------------------------------------

type cellDiff struct{ before, after float64 }

func diffCell(before, after []model.Cell, zone string) cellDiff {
	get := func(cells []model.Cell) float64 {
		for _, c := range cells {
			if c.Labels["zone"] == zone {
				return c.Value
			}
		}
		return math.NaN()
	}
	return cellDiff{get(before), get(after)}
}

func compareCells(id, class, desc string, a, b []model.Cell) Finding {
	sort.Slice(a, func(i, j int) bool { return keyOf(a[i]) < keyOf(a[j]) })
	sort.Slice(b, func(i, j int) bool { return keyOf(b[i]) < keyOf(b[j]) })
	maxDiff := 0.0
	detail := ""
	if len(a) != len(b) {
		detail = "cell count differs"
	}
	for i := range a {
		d := math.Abs(a[i].Value - b[i].Value)
		if d > maxDiff {
			maxDiff = d
		}
		if d != 0 {
			detail += fp(a[i].Value) + "!=" + fp(b[i].Value) + " "
		}
	}
	return Finding{
		ID: id, Class: class, Description: desc,
		CellValue: 0, Reference: 0, AbsErr: maxDiff,
		RelErr: finite(maxDiff), ULP: nil,
		Detail: ternary(detail == "", "all cells bit-identical", detail),
	}
}

func keyOf(c model.Cell) string {
	b, _ := json.Marshal(c.Labels)
	return string(b) + "|" + itoa64(c.Start)
}

func mkFinding(id, class, desc string, got, exact float64) Finding {
	rel, ulp := finite(math.Abs(got-exact)/math.Abs(exact)), finite(ulpBetween(got, exact))
	if exact == 0 {
		rel, ulp = nil, nil
	}
	return Finding{
		ID: id, Class: class, Description: desc,
		CellValue: got, Reference: exact, AbsErr: math.Abs(got - exact),
		RelErr: rel, ULP: ulp,
	}
}

func ulpBetween(got, exact float64) float64 {
	if exact == 0 || math.Signbit(got) != math.Signbit(exact) {
		if got == exact {
			return 0
		}
		return math.Inf(1)
	}
	exp := math.Floor(math.Log2(math.Abs(exact)))
	return math.Abs(got-exact) / math.Ldexp(1, int(exp)-52)
}

func finite(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return v
}

func fp(v float64) string { return strconvF(v) }
func itoa(n int) string   { return itoa64(int64(n)) }

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
