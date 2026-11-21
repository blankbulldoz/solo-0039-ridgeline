package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func testEngine(t *testing.T) (*Engine, func()) {
	t.Helper()
	dir := t.TempDir()
	e, err := Open(filepath.Join(dir, "ridge.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return e, func() { e.Close() }
}

func testNamespace() model.Namespace {
	return model.Namespace{
		Name: "spindle", RetentionMS: 10000, LatenessMS: 30000, FutureMS: 1000,
		MaxSeries: 100, MaxWrite: 10000, MaxScan: 100000,
	}
}

func labels(node string) model.Labels {
	return model.Labels{"__name__": "pulse_total", "node": node, "zone": "east"}
}

func writePoint(t *testing.T, e *Engine, node string, at int64, value float64, ex *model.ExemplarInput) model.WriteReceipt {
	t.Helper()
	receipt, err := e.Write("spindle", model.WriteRequest{
		Now: 10000,
		Points: []model.Observation{{
			Labels: labels(node), Time: at, Value: value, Exemplar: ex,
		}},
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	return receipt
}

func exQuery() model.ExemplarQuery {
	return model.ExemplarQuery{Start: -5000, End: 10000}
}

func findExemplar(t *testing.T, res model.ExemplarResult, trace string) model.Exemplar {
	t.Helper()
	for _, ex := range res.Exemplars {
		if ex.TraceID == trace {
			return ex
		}
	}
	t.Fatalf("exemplar %q not present", trace)
	return model.Exemplar{}
}

func TestExemplarLifecycleAndIdentityIsolation(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}

	receipt := writePoint(t, e, "ridge-a", 1000, 42,
		&model.ExemplarInput{TraceID: "trace-1", Labels: model.Labels{"route": "/checkout"}})
	if receipt.Exemplars != 1 {
		t.Fatalf("expected 1 exemplar in receipt, got %d", receipt.Exemplars)
	}
	// A point without an exemplar must not synthesize one.
	writePoint(t, e, "ridge-a", 2000, 43, nil)

	res, err := e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Exemplars) != 1 || res.Stats.Visible != 1 || res.Stats.Matched != 1 {
		t.Fatalf("unexpected exemplar result: %+v", res)
	}
	ex := res.Exemplars[0]
	if ex.TraceID != "trace-1" || !ex.AnchorVisible || ex.Value != 42 ||
		ex.Labels["route"] != "/checkout" || ex.AnchorSequence == nil {
		t.Fatalf("exemplar content wrong: %+v", ex)
	}

	// Exemplars never participate in raw points or aggregation.
	rng, err := e.Range("spindle", model.Query{Start: -5000, End: 10000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	for _, sp := range rng.Series {
		for _, p := range sp.Points {
			if p.Value == 0 && p.Time == 0 {
				t.Fatalf("unexpected zero sample")
			}
		}
		if len(sp.Points) != 2 {
			t.Fatalf("range should expose two samples, got %d", len(sp.Points))
		}
	}
	agg, err := e.Aggregate("spindle", model.Query{
		Start: -5000, End: 10000, Step: 15000, Origin: 0,
		GroupBy: []string{"zone"}, Function: "count",
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(agg.Cells) != 1 || agg.Cells[0].Count != 2 {
		t.Fatalf("aggregate must count samples only: %+v", agg.Cells)
	}
}

func TestMultipleExemplarsAtSameMoment(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, trace := range []string{"trace-a", "trace-b", "trace-c"} {
		writePoint(t, e, "ridge-a", 1000, 42,
			&model.ExemplarInput{TraceID: trace})
	}
	res, err := e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Exemplars) != 3 {
		t.Fatalf("expected three exemplars at one moment, got %d", len(res.Exemplars))
	}
	seen := map[string]bool{}
	for _, ex := range res.Exemplars {
		if ex.Time != 1000 || !ex.AnchorVisible {
			t.Fatalf("unexpected exemplar: %+v", ex)
		}
		seen[ex.TraceID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("distinct traces missing: %v", seen)
	}
}

func TestExemplarDensityBound(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 25; i++ {
		writePoint(t, e, "ridge-a", int64(1000+i), float64(i),
			&model.ExemplarInput{TraceID: "trace"})
	}
	q := exQuery()
	q.Limit = 10
	res, err := e.Exemplars("spindle", q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !res.Truncated || len(res.Exemplars) != 10 || res.Stats.Omitted != 15 {
		t.Fatalf("density bound not enforced: %+v", res.Stats)
	}
	// Representative sampling must retain the first and last moments.
	if res.Exemplars[0].Time != 1000 || res.Exemplars[9].Time != 1024 {
		t.Fatalf("representative endpoints wrong: %d..%d", res.Exemplars[0].Time, res.Exemplars[9].Time)
	}

	// The hard cap rejects pathological limits rather than failing open.
	bad := exQuery()
	bad.Limit = model.MaxExemplarLimit + 1
	if _, err := e.Exemplars("spindle", bad); err == nil {
		t.Fatal("expected over-cap limit to be rejected")
	}
}

func TestExemplarDeleteCorrectionAndRetention(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	writePoint(t, e, "ridge-a", 500, 2, &model.ExemplarInput{TraceID: "before-delete"})
	del, err := e.Delete("spindle", model.Query{
		Start: -600, End: 600,
		Matchers: []model.Matcher{{Label: "node", Op: "=", Value: "ridge-a"}},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Exemplar attached before the tombstone loses its anchor.
	res, err := e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	old := findExemplar(t, res, "before-delete")
	if old.AnchorVisible || old.AnchorSequence != nil {
		t.Fatalf("tombstoned exemplar must be detached: %+v", old)
	}
	if res.Stats.HiddenTombstone != 1 {
		t.Fatalf("expected one tombstoned exemplar, got %+v", res.Stats)
	}

	// A later correction at the same moment is visible, and a new exemplar
	// attached after the deletion stays valid; the old one stays detached.
	writePoint(t, e, "ridge-a", 500, 9, &model.ExemplarInput{TraceID: "after-delete"})
	res, err = e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	old = findExemplar(t, res, "before-delete")
	newEx := findExemplar(t, res, "after-delete")
	if old.AnchorVisible {
		t.Fatal("old exemplar must not be resurrected by a correction")
	}
	if !newEx.AnchorVisible || newEx.Value != 9 || newEx.AnchorSequence == nil ||
		*newEx.AnchorSequence <= del.Sequence {
		t.Fatalf("new exemplar should anchor the corrected point: %+v", newEx)
	}
	// Raw range confirms the corrected value and single logical point.
	rng, err := e.Range("spindle", model.Query{Start: -5000, End: 10000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(rng.Series[0].Points) != 1 || rng.Series[0].Points[0].Value != 9 {
		t.Fatalf("correction semantics broken: %+v", rng.Series[0].Points)
	}

	// A later exemplar above the upcoming retention floor separates kept
	// from reclaimed associations during physical maintenance.
	writePoint(t, e, "ridge-a", 5000, 5, &model.ExemplarInput{TraceID: "survivor"})

	// Retention advances the floor and compacts; expired exemplars are
	// physically reclaimed and cannot return.
	maint, err := e.Retain("spindle", 11000)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if maint.Floor != 1000 || maint.ExemplarsDropped != 2 || maint.ExemplarsKept != 1 {
		t.Fatalf("expected compaction to reclaim expired exemplars: %+v", maint)
	}
	res, err = e.Exemplars("spindle", model.ExemplarQuery{Start: model.MinTime, End: 10000})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.Stats.Matched != 1 || res.Stats.BelowFloor != 0 {
		t.Fatalf("expired exemplars must be physically gone, stats=%+v", res.Stats)
	}
	if res.Exemplars[0].TraceID != "survivor" || !res.Exemplars[0].AnchorVisible {
		t.Fatalf("surviving exemplar wrong: %+v", res.Exemplars[0])
	}
}

func TestExemplarSurvivesSealAndCompaction(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	writePoint(t, e, "ridge-a", 1000, 7, &model.ExemplarInput{TraceID: "seal-me"})
	writePoint(t, e, "ridge-b", 2000, 8, &model.ExemplarInput{TraceID: "other-series"})

	if _, err := e.Seal("spindle"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	res, err := e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query after seal: %v", err)
	}
	if res.Stats.Visible != 2 || len(res.Exemplars) != 2 {
		t.Fatalf("seal must preserve exemplars: %+v", res.Stats)
	}
	for _, ex := range res.Exemplars {
		if !ex.AnchorVisible {
			t.Fatalf("sealed exemplar lost anchor: %+v", ex)
		}
	}

	maint, err := e.Compact("spindle")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if maint.ExemplarsKept != 2 || maint.ExemplarsDropped != 0 {
		t.Fatalf("compaction should keep valid exemplars: %+v", maint)
	}
	res, err = e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query after compact: %v", err)
	}
	if res.Stats.Visible != 2 {
		t.Fatalf("visible count changed after compaction: %+v", res.Stats)
	}
	survivor := findExemplar(t, res, "seal-me")
	if !survivor.AnchorVisible || survivor.AnchorSequence == nil {
		t.Fatalf("block-backed anchor must resolve: %+v", survivor)
	}
	// Anchors now live in blocks, not head, and still resolve.
	for _, ex := range res.Exemplars {
		if !ex.AnchorVisible || ex.AnchorSequence == nil {
			t.Fatalf("block-backed anchor must resolve: %+v", ex)
		}
	}
}

func TestExemplarMatchersFilterBySeries(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	writePoint(t, e, "ridge-a", 1000, 1, &model.ExemplarInput{TraceID: "a"})
	writePoint(t, e, "ridge-b", 1000, 2, &model.ExemplarInput{TraceID: "b"})

	q := model.ExemplarQuery{
		Start: -5000, End: 10000,
		Matchers: []model.Matcher{{Label: "node", Op: "=", Value: "ridge-a"}},
	}
	res, err := e.Exemplars("spindle", q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Exemplars) != 1 || res.Exemplars[0].TraceID != "a" {
		t.Fatalf("matcher did not filter exemplars: %+v", res.Exemplars)
	}

	// Time bounds are half-open, matching the rest of the API.
	early, err := e.Exemplars("spindle", model.ExemplarQuery{Start: 1001, End: 10000})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if early.Stats.Matched != 0 {
		t.Fatalf("half-open start bound violated: %+v", early.Stats)
	}
	if _, err := e.Exemplars("spindle", model.ExemplarQuery{Start: 1000, End: 1000}); err == nil {
		t.Fatal("empty interval must be rejected")
	}
}

func TestExemplarValidation(t *testing.T) {
	e, closeFn := testEngine(t)
	defer closeFn()
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create: %v", err)
	}
	cases := []model.ExemplarInput{
		{TraceID: ""},
		{TraceID: "bad space"},
		{TraceID: "ok", Labels: model.Labels{"9bad": "x"}},
	}
	for i, bad := range cases {
		_, err := e.Write("spindle", model.WriteRequest{Now: 10000, Points: []model.Observation{{
			Labels: labels("ridge-a"), Time: 1000, Value: 1, Exemplar: &bad,
		}}})
		if err == nil {
			t.Fatalf("case %d expected rejection", i)
		}
	}
	// Rejected writes publish nothing, including no partial exemplar.
	res, err := e.Exemplars("spindle", exQuery())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.Stats.Scanned != 0 {
		t.Fatalf("failed writes must leave no exemplars: %+v", res.Stats)
	}
}
