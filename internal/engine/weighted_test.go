package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func weightedNamespace() model.Namespace {
	return model.Namespace{
		Name: "probe", RetentionMS: model.MaxTime, LatenessMS: model.MaxTime,
		FutureMS: 100000, MaxSeries: 100, MaxWrite: 10000, MaxScan: 200000,
	}
}

func weightedQuery(function string) model.Query {
	return model.Query{
		Start: 0, End: 10000, Step: 10000, Origin: 0,
		Function: function, MaxAgeMS: 2000, GroupBy: []string{"node"},
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "load"}},
	}
}

func labels(node string) model.Labels {
	return model.Labels{"__name__": "load", "node": node}
}

func TestWeightedQueryAcrossSealCompactAndDelete(t *testing.T) {
	engine := openTestEngine(t)
	defer engine.Close()
	if _, err := engine.Create(weightedNamespace()); err != nil {
		t.Fatal(err)
	}
	// Sparse stream: holds 4 on [0,2000), unknown on [2000,4000),
	// holds 6 on [4000,6000), unknown thereafter.
	points := []model.Observation{
		{Labels: labels("a"), Time: 0, Value: 4},
		{Labels: labels("a"), Time: 4000, Value: 6},
		// Predecessor before the window for a second series.
		{Labels: labels("b"), Time: -1000, Value: 2},
	}
	if _, err := engine.Write("probe", model.WriteRequest{Now: 10000, Points: points}); err != nil {
		t.Fatal(err)
	}

	head, err := engine.Aggregate("probe", weightedQuery("twavg"))
	if err != nil {
		t.Fatal(err)
	}
	if head.DroppedCoverage != 0 || len(head.Cells) != 2 {
		t.Fatalf("head aggregate mismatch: %+v", head)
	}
	a := cellForNode(head.Cells, "a")
	if a == nil || a.CoveredMS != 4000 || a.Value != 5 {
		t.Fatalf("series a weighted result wrong: %+v", a)
	}
	b := cellForNode(head.Cells, "b")
	if b == nil || b.CoveredMS != 1000 || b.Value != 2 {
		t.Fatalf("predecessor hold wrong: %+v", b)
	}

	// Sealing moves head points into blocks without changing logical results.
	if _, err := engine.Seal("probe"); err != nil {
		t.Fatal(err)
	}
	sealed, err := engine.Aggregate("probe", weightedQuery("twavg"))
	if err != nil {
		t.Fatal(err)
	}
	if !cellsEqual(sealed.Cells, head.Cells) {
		t.Fatalf("seal changed weighted result: %+v vs %+v", sealed.Cells, head.Cells)
	}

	// Compaction rewrites blocks; the same visible data must drive the result.
	if _, err := engine.Compact("probe"); err != nil {
		t.Fatal(err)
	}
	compacted, err := engine.Aggregate("probe", weightedQuery("twavg"))
	if err != nil {
		t.Fatal(err)
	}
	if !cellsEqual(compacted.Cells, head.Cells) {
		t.Fatalf("compact changed weighted result: %+v vs %+v", compacted.Cells, head.Cells)
	}

	// Deleting series a leaves only the predecessor-driven series b.
	if _, err := engine.Delete("probe", model.Query{
		Start: -5000, End: 10000,
		Matchers: []model.Matcher{{Label: "node", Op: "=", Value: "a"}},
	}); err != nil {
		t.Fatal(err)
	}
	afterDelete, err := engine.Aggregate("probe", weightedQuery("twavg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDelete.Cells) != 1 || cellForNode(afterDelete.Cells, "a") != nil {
		t.Fatalf("deletion did not hide series a: %+v", afterDelete.Cells)
	}
	if cellForNode(afterDelete.Cells, "b") == nil {
		t.Fatalf("series b must remain visible: %+v", afterDelete.Cells)
	}
}

func TestWeightedPredecessorAcrossInterleavedSeries(t *testing.T) {
	engine := openTestEngine(t)
	defer engine.Close()
	if _, err := engine.Create(weightedNamespace()); err != nil {
		t.Fatal(err)
	}
	// Several series, some only before the window and some with in-window
	// points, so head keys interleave across series identities.
	if _, err := engine.Write("probe", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labels("pre1"), Time: -1500, Value: 7},
		{Labels: labels("pre2"), Time: -500, Value: 9},
		{Labels: labels("inside"), Time: 2000, Value: 3},
		{Labels: labels("inside"), Time: 4000, Value: 5},
	}}); err != nil {
		t.Fatal(err)
	}
	res, err := engine.Aggregate("probe", weightedQuery("twavg"))
	if err != nil {
		t.Fatal(err)
	}
	pre1 := cellForNode(res.Cells, "pre1")
	pre2 := cellForNode(res.Cells, "pre2")
	inside := cellForNode(res.Cells, "inside")
	// pre1 at -1500 holds to 500 (max age 2000): 500ms of known time.
	if pre1 == nil || pre1.CoveredMS != 500 || pre1.Value != 7 {
		t.Fatalf("pre1 predecessor wrong: %+v", pre1)
	}
	// pre2 at -500 holds to 1500: 1500ms of known time.
	if pre2 == nil || pre2.CoveredMS != 1500 || pre2.Value != 9 {
		t.Fatalf("pre2 predecessor wrong: %+v", pre2)
	}
	// inside has ordinary in-window holds.
	if inside == nil || inside.CoveredMS != 4000 {
		t.Fatalf("inside series wrong: %+v", inside)
	}
}

func TestWeightedStrictCoverageThroughStorage(t *testing.T) {
	engine := openTestEngine(t)
	defer engine.Close()
	if _, err := engine.Create(weightedNamespace()); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Write("probe", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labels("a"), Time: 0, Value: 4},
		{Labels: labels("a"), Time: 4000, Value: 6},
	}}); err != nil {
		t.Fatal(err)
	}
	lenient := weightedQuery("twavg")
	strict := lenient
	strict.RequireCoverage = true

	loose, err := engine.Aggregate("probe", lenient)
	if err != nil {
		t.Fatal(err)
	}
	if len(loose.Cells) != 1 {
		t.Fatalf("lenient mode should retain the partial window: %+v", loose)
	}
	tight, err := engine.Aggregate("probe", strict)
	if err != nil {
		t.Fatal(err)
	}
	if len(tight.Cells) != 0 || tight.DroppedCoverage != 1 {
		t.Fatalf("strict mode should drop the under-covered window: %+v", tight)
	}
}

func TestWeightedCorrectionAndIntegral(t *testing.T) {
	engine := openTestEngine(t)
	defer engine.Close()
	if _, err := engine.Create(weightedNamespace()); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Write("probe", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labels("a"), Time: 0, Value: 4},
		{Labels: labels("a"), Time: 5000, Value: 8},
	}}); err != nil {
		t.Fatal(err)
	}
	integral, err := engine.Aggregate("probe", weightedQuery("integral"))
	if err != nil {
		t.Fatal(err)
	}
	c := cellForNode(integral.Cells, "a")
	// Holds [0,2000) value 4 and [5000,7000) value 8, each capped at max age.
	if c == nil || c.CoveredMS != 4000 || c.Value != 4*2000+8*2000 {
		t.Fatalf("integral wrong: %+v", c)
	}
	// Correct the first timestamp; the same visible data now yields a new area.
	if _, err := engine.Write("probe", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labels("a"), Time: 0, Value: 10},
	}}); err != nil {
		t.Fatal(err)
	}
	integral, err = engine.Aggregate("probe", weightedQuery("integral"))
	if err != nil {
		t.Fatal(err)
	}
	c = cellForNode(integral.Cells, "a")
	if c == nil || c.Value != 10*2000+8*2000 {
		t.Fatalf("corrected integral wrong: %+v", c)
	}
}

func TestWeightedRequiresMaxAge(t *testing.T) {
	engine := openTestEngine(t)
	defer engine.Close()
	if _, err := engine.Create(weightedNamespace()); err != nil {
		t.Fatal(err)
	}
	bad := weightedQuery("twavg")
	bad.MaxAgeMS = 0
	if _, err := engine.Aggregate("probe", bad); err == nil {
		t.Fatal("expected rejection of twavg without max_age_ms")
	}
}

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func cellForNode(cells []model.Cell, node string) *model.Cell {
	for i := range cells {
		if cells[i].Labels["node"] == node {
			return &cells[i]
		}
	}
	return nil
}

func cellsEqual(a, b []model.Cell) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Start != b[i].Start || a[i].CoveredMS != b[i].CoveredMS ||
			a[i].Value != b[i].Value || a[i].Count != b[i].Count {
			return false
		}
	}
	return true
}
