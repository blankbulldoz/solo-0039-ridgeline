package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func createSpace(t *testing.T, e *Engine, retention int64) {
	t.Helper()
	_, err := e.Create(model.Namespace{
		Name: "space", RetentionMS: retention, LatenessMS: 100000, FutureMS: 1000,
		MaxSeries: 100, MaxWrite: 10000, MaxScan: 100000,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
}

func writePoints(t *testing.T, e *Engine, now int64, points ...model.Observation) model.WriteReceipt {
	t.Helper()
	receipt, err := e.Write("space", model.WriteRequest{Now: now, Points: points})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	return receipt
}

func point(name, zone string, at int64, value float64) model.Observation {
	return model.Observation{
		Labels: model.Labels{"__name__": name, "zone": zone},
		Time:   at, Value: value,
	}
}

func rollupQuery(name string) model.Query {
	return model.Query{
		Start: -10000, End: 10000, Step: 1000, Origin: 0, GroupBy: []string{"zone"},
		Function: "avg",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: name}},
	}
}

func view(t *testing.T, e *Engine) model.RollupView {
	t.Helper()
	out, err := e.Rollup("space", "cpu")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	return out
}

func assertFresh(t *testing.T, e *Engine) {
	t.Helper()
	out := view(t, e)
	if out.Stale || out.Freshness != model.FreshnessFresh {
		t.Fatalf("expected fresh, got %+v", out)
	}
}

func assertStale(t *testing.T, e *Engine) {
	t.Helper()
	out := view(t, e)
	if !out.Stale || out.Freshness != model.FreshnessStale {
		t.Fatalf("expected stale, got %+v", out)
	}
}

func TestFreshnessIgnoresUnrelatedSeries(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1), point("cpu", "east", 1000, 2))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 0, point("disk", "east", 500, 99))
	assertFresh(t, e)
}

func TestFreshnessIgnoresOutOfRangeWrite(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	q := rollupQuery("cpu")
	q.Start, q.End = 0, 1000
	if _, err := e.Materialize("space", "cpu", q); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 5000, point("cpu", "east", 5000, 99))
	assertFresh(t, e)
}

func TestFreshnessDetectsCorrection(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1), point("cpu", "east", 1000, 2))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 0, point("cpu", "east", 1000, 3))
	assertStale(t, e)
}

func TestFreshnessDetectsDeletion(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1), point("cpu", "east", 1000, 2))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	del := rollupQuery("cpu")
	del.Start, del.End = 500, 1500
	if _, err := e.Delete("space", del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	assertStale(t, e)
}

func TestFreshnessIgnoresDeletionOutsideRange(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	q := rollupQuery("cpu")
	q.Start, q.End = 0, 1000
	if _, err := e.Materialize("space", "cpu", q); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	del := rollupQuery("cpu")
	del.Start, del.End = 5000, 6000
	if _, err := e.Delete("space", del); err != nil {
		t.Fatalf("delete: %v", err)
	}
	assertFresh(t, e)
}

func TestFreshnessIgnoresPhysicalSealAndCompaction(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1), point("cpu", "east", 1000, 2))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := e.Seal("space"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	assertFresh(t, e)
	if _, err := e.Compact("space"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	assertFresh(t, e)
}

func TestFreshnessIgnoresReadOnlyToggle(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := e.ReadOnly("space", true); err != nil {
		t.Fatalf("read-only: %v", err)
	}
	assertFresh(t, e)
}

func TestFreshnessRetentionBoundary(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 1000)
	writePoints(t, e, 9000, point("cpu", "east", 1000, 1), point("cpu", "east", 9000, 2))

	// A query below the advancing floor loses observed samples: stale.
	low := rollupQuery("cpu")
	low.Start, low.End = 0, 2000
	if _, err := e.Materialize("space", "cpu", low); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := e.Retain("space", 5000); err != nil { // floor becomes 4000
		t.Fatalf("retain: %v", err)
	}
	assertStale(t, e)

	// A query above the new floor retains all observations: fresh.
	high := rollupQuery("cpu")
	high.Start, high.End = 5000, 10000
	if _, err := e.Materialize("space", "cpu", high); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := e.Retain("space", 6000); err != nil { // floor becomes 5000
		t.Fatalf("retain: %v", err)
	}
	// The retained samples at 9000 remain visible even though floor rose to 5000.
	assertFresh(t, e)
}

func TestFreshnessLaterMatchSeries(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	q := rollupQuery("cpu")
	q.Matchers = []model.Matcher{{Label: "zone", Op: "=", Value: "east"}}
	if _, err := e.Materialize("space", "cpu", q); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 0, point("mem", "east", 500, 4))
	assertStale(t, e)
}

func TestLegacyMaterializationIndeterminate(t *testing.T) {
	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 0, point("disk", "east", 500, 1))
	// Simulate a record written before provenance tracking.
	if err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, "space")
		var m model.Materialization
		if err := volume.GetJSON(b.Bucket(volume.Rollups), []byte("cpu"), &m); err != nil {
			return err
		}
		m.Provenance = 0
		return volume.PutJSON(b.Bucket(volume.Rollups), []byte("cpu"), m)
	}); err != nil {
		t.Fatal(err)
	}
	out := view(t, e)
	if out.Freshness != model.FreshnessIndeterminate || !out.Stale {
		t.Fatalf("expected indeterminate and conservatively stale, got %+v", out)
	}
}

func TestJournalPruningMakesOldSourcesIndeterminate(t *testing.T) {
	previous := maxJournalEntries
	maxJournalEntries = 4
	t.Cleanup(func() { maxJournalEntries = previous })

	e := newTestEngine(t)
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for i := int64(1); i <= 6; i++ {
		writePoints(t, e, 0, point("cpu", "east", i*10, float64(i)))
	}
	out := view(t, e)
	if out.Freshness != model.FreshnessIndeterminate || !out.Stale {
		t.Fatalf("pruned journal should be indeterminate and stale, got %+v", out)
	}
	// A fresh materialization at the current sequence is precisely judgeable.
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("re-materialize: %v", err)
	}
	assertFresh(t, e)
}

func TestFreshnessSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	createSpace(t, e, 100000)
	writePoints(t, e, 0, point("cpu", "east", 0, 1), point("cpu", "east", 1000, 2))
	if _, err := e.Materialize("space", "cpu", rollupQuery("cpu")); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	writePoints(t, e, 0, point("disk", "east", 500, 1))
	// Unrelated write after reopening is still judged precisely, which requires
	// both the journal and the tracking boundary to have been persisted.
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	e, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	if view := mustView(t, e); view.Stale {
		t.Fatalf("unrelated change after reopen marked stale: %+v", view)
	}
	if _, err := e.Write("space", model.WriteRequest{Now: 0, Points: []model.Observation{point("cpu", "east", 1000, 9)}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if view := mustView(t, e); !view.Stale || view.Freshness != model.FreshnessStale {
		t.Fatalf("correction after reopen not stale: %+v", view)
	}
}

func mustView(t *testing.T, e *Engine) model.RollupView {
	t.Helper()
	out, err := e.Rollup("space", "cpu")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	return out
}
