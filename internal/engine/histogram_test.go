package engine

import (
	"math"
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "ridge.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	namespace := model.Namespace{
		Name: "lab", RetentionMS: 1_000_000, LatenessMS: 1_000_000,
		FutureMS: 1_000, MaxSeries: 50, MaxWrite: 1000, MaxScan: 100000,
	}
	if _, err := e.Create(namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return e
}

func labels(node string) model.Labels {
	return model.Labels{"__name__": "latency_ms", "node": node}
}

func hist(node string, time int64, counts []uint64) model.HistogramObservation {
	return model.HistogramObservation{
		Labels: labels(node), Time: time,
		Bounds: []float64{10, 25, 50, 100},
		Counts: counts, Sum: float64(counts[len(counts)-1]) * 20,
	}
}

func writeHist(t *testing.T, e *Engine, now int64, observations ...model.HistogramObservation) {
	t.Helper()
	if _, err := e.WriteHistograms("lab",
		model.HistogramWriteRequest{Now: now, Histograms: observations}); err != nil {
		t.Fatalf("write histograms: %v", err)
	}
}

func histQuery() model.HistogramQuery {
	return model.HistogramQuery{
		Start: 0, End: 100000, Step: 100000, Origin: 0,
		Quantiles: []float64{0.5, 0.9},
	}
}

func TestHistogramCorrectionSealAndCompactPreserveResults(t *testing.T) {
	e := testEngine(t)
	writeHist(t, e, 1000,
		hist("a", 1000, []uint64{10, 40, 80, 100}),
		hist("b", 1000, []uint64{5, 20, 60, 90}),
	)
	// Corrective write at an existing timestamp: newest sequence wins.
	writeHist(t, e, 1000, hist("a", 1000, []uint64{12, 42, 82, 102}))

	head, err := e.HistogramRange("lab",
		model.HistogramQuery{Start: 0, End: 100000})
	if err != nil {
		t.Fatalf("histogram range: %v", err)
	}
	if len(head.Series) != 2 || head.Stats.Visible != 2 {
		t.Fatalf("unexpected head series: %+v", head)
	}
	for _, s := range head.Series {
		if s.Series.Labels["node"] == "a" && s.Points[0].Counts[3] != 102 {
			t.Fatalf("correction not visible: %+v", s.Points)
		}
	}

	sealed, err := e.Seal("lab")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if sealed.HistogramBlock != 2 || sealed.Histograms != 2 {
		t.Fatalf("seal receipt wrong: %+v", sealed)
	}
	blocks, err := e.HistogramBlocks("lab")
	if err != nil {
		t.Fatalf("blocks: %v", err)
	}
	if len(blocks) != 2 || blocks[0].Format != 2 {
		t.Fatalf("histogram block inventory wrong: %+v", blocks)
	}

	before, err := e.Histograms("lab", histQuery())
	if err != nil {
		t.Fatalf("query before compact: %v", err)
	}
	// Snapshot-only windows have zero increase: baselines are excluded.
	if len(before.Cells) != 1 || before.Cells[0].Count != 0 {
		t.Fatalf("baseline-only cell should have zero increase: %+v", before.Cells)
	}

	// Add a second snapshot so a positive, mergeable increase exists.
	writeHist(t, e, 2000,
		hist("a", 2000, []uint64{22, 72, 142, 202}),
		hist("b", 2000, []uint64{15, 50, 110, 190}),
	)
	logical, err := e.Histograms("lab", histQuery())
	if err != nil {
		t.Fatalf("logical query: %v", err)
	}
	if logical.Cells[0].Count != (202-102)+(190-90) {
		t.Fatalf("merged increase wrong: %+v", logical.Cells[0])
	}
	if logical.Stats.Blocks != 2 || logical.Stats.Head != 2 {
		t.Fatalf("query should span blocks and head: %+v", logical.Stats)
	}
	if len(logical.Cells[0].Quantiles) != 2 {
		t.Fatalf("quantile estimates missing: %+v", logical.Cells[0].Quantiles)
	}

	if _, err := e.Compact("lab"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	physical, err := e.Histograms("lab", histQuery())
	if err != nil {
		t.Fatalf("query after compact: %v", err)
	}
	if physical.Stats.Blocks != 2 || physical.Stats.Head != 0 {
		t.Fatalf("compaction should relocate head into blocks: %+v", physical.Stats)
	}
	if !equalCells(logical.Cells, physical.Cells) {
		t.Fatalf("compaction changed histogram results\n%+v\n%+v", logical.Cells, physical.Cells)
	}
}

func TestHistogramDeletionAndRetention(t *testing.T) {
	e := testEngine(t)
	// Short-retention namespace for the floor portion of this scenario.
	if _, err := e.Create(model.Namespace{
		Name: "lab-retention", RetentionMS: 1000, LatenessMS: 1_000_000,
		FutureMS: 1000, MaxSeries: 50, MaxWrite: 1000, MaxScan: 100000,
	}); err != nil {
		t.Fatalf("create retention namespace: %v", err)
	}
	writeHist(t, e, 5000,
		hist("a", 1000, []uint64{10, 40, 80, 100}),
		hist("a", 2000, []uint64{20, 70, 140, 200}),
	)
	// The short-retention namespace shares the same catalog/series identity.
	retSpace := "lab-retention"
	if _, err := e.WriteHistograms(retSpace, model.HistogramWriteRequest{Now: 5000, Histograms: []model.HistogramObservation{
		hist("a", 1000, []uint64{10, 40, 80, 100}),
		hist("a", 2000, []uint64{20, 70, 140, 200}),
	}}); err != nil {
		t.Fatalf("seed retention namespace: %v", err)
	}
	if _, err := e.Delete("lab", model.Query{
		Start: 1500, End: 2500, Matchers: []model.Matcher{
			{Label: "node", Op: "=", Value: "a"},
		},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	visible, err := e.HistogramRange("lab",
		model.HistogramQuery{Start: 0, End: 100000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(visible.Series[0].Points) != 1 || visible.Series[0].Points[0].Time != 1000 {
		t.Fatalf("deletion did not hide snapshot: %+v", visible.Series)
	}
	// A newer correction at the deleted timestamp is admitted again.
	writeHist(t, e, 5000, hist("a", 2000, []uint64{5, 10, 15, 20}))
	visible, err = e.HistogramRange("lab",
		model.HistogramQuery{Start: 0, End: 100000})
	if err != nil {
		t.Fatalf("range after correction: %v", err)
	}
	if len(visible.Series[0].Points) != 2 {
		t.Fatalf("post-delete correction must be visible: %+v", visible.Series)
	}

	// Retention advances the irreversible floor and physically removes data.
	// Floor becomes 5000 - 1000 = 4000, hiding both earlier snapshots.
	if _, err := e.Retain(retSpace, 5000); err != nil {
		t.Fatalf("retention: %v", err)
	}
	empty, err := e.HistogramRange(retSpace,
		model.HistogramQuery{Start: 0, End: 100000})
	if err != nil {
		t.Fatalf("range after retention: %v", err)
	}
	if empty.Stats.Visible != 0 {
		t.Fatalf("retention floor must hide old histograms: %+v", empty.Stats)
	}
	if _, err := e.WriteHistograms(retSpace, model.HistogramWriteRequest{
		Now: 5000, Histograms: []model.HistogramObservation{hist("a", 2000, []uint64{1, 2, 3, 4})},
	}); err == nil {
		t.Fatal("histogram before retention floor must be rejected")
	}
}

func TestHistogramValidationFailures(t *testing.T) {
	e := testEngine(t)
	cases := []model.HistogramObservation{
		{Labels: labels("a"), Time: 1000, Bounds: []float64{10, 10}, Counts: []uint64{1, 2}, Sum: 1},
		{Labels: labels("a"), Time: 1000, Bounds: []float64{10, 20}, Counts: []uint64{2, 1}, Sum: 1},
		{Labels: labels("a"), Time: 1000, Bounds: []float64{10}, Counts: []uint64{}, Sum: 1},
		{Labels: labels("a"), Time: 1000, Bounds: []float64{10}, Counts: []uint64{1}, Sum: math.NaN()},
	}
	for i, observation := range cases {
		_, err := e.WriteHistograms("lab",
			model.HistogramWriteRequest{Now: 1000, Histograms: []model.HistogramObservation{observation}})
		if err == nil {
			t.Fatalf("case %d expected validation error", i)
		}
	}
}

func equalCells(a, b []model.HistogramCell) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Count != b[i].Count || a[i].Sum != b[i].Sum ||
			a[i].Start != b[i].Start || a[i].Resets != b[i].Resets {
			return false
		}
	}
	return true
}
