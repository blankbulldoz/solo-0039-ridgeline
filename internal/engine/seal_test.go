package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func testNamespace(name string) model.Namespace {
	return model.Namespace{
		Name: name, RetentionMS: 1 << 40, LatenessMS: 1 << 40, FutureMS: 1 << 40,
		MaxSeries: 100, MaxWrite: 1000, MaxScan: 200000,
	}
}

func ingest(t *testing.T, e *Engine, space string, now int64, points ...model.Observation) {
	t.Helper()
	if _, err := e.Write(space, model.WriteRequest{Now: now, Points: points}); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func allValues(t *testing.T, e *Engine, space string) map[int64]float64 {
	t.Helper()
	res, err := e.Range(space, model.Query{Start: model.MinTime, End: model.MaxTime + 1})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	out := map[int64]float64{}
	for _, group := range res.Series {
		for _, p := range group.Points {
			if _, dup := out[p.Time]; dup {
				t.Fatalf("duplicate timestamp %d across series", p.Time)
			}
			out[p.Time] = p.Value
		}
	}
	return out
}

// TestBatchedSealLifecycle drives several batches interleaved with engine
// reopens (simulating process interruption), corrective and new writes, and
// asserts no point is lost or duplicated and corrections always win.
func TestBatchedSealLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ridgeline.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	labels := model.Labels{"__name__": "pulse_total", "node": "a"}
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatal(err)
	}
	points := make([]model.Observation, 0, 12)
	for ts := int64(0); ts < 12000; ts += 1000 {
		points = append(points, model.Observation{Labels: labels, Time: ts, Value: float64(ts)})
	}
	ingest(t, e, "spindle", 12000, points...)
	expected := map[int64]float64{}
	for ts := int64(0); ts < 12000; ts += 1000 {
		expected[ts] = float64(ts)
	}

	receipt, err := e.StartSealJob("spindle", 5)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Points != 5 || !receipt.Job.Active {
		t.Fatalf("unexpected first batch: %+v", receipt)
	}
	if got := allValues(t, e, "spindle"); len(got) != 12 {
		t.Fatalf("mid-seal read changed row count: %d", len(got))
	}

	// Reopen while the job is partially complete: progress must be durable.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	view, err := e.SealJobStatus("spindle")
	if err != nil || !view.Active || view.Job.Batches != 1 {
		t.Fatalf("job did not survive restart: %+v err=%v", view, err)
	}
	if view.Job.Progress.InitialPending != 7 {
		t.Fatalf("initial pending after restart = %d, want 7", view.Job.Progress.InitialPending)
	}

	if _, err := e.SealBatch("spindle", 5); err != nil {
		t.Fatal(err)
	}

	// Correction to an already-sealed key plus a brand-new timestamp.
	ingest(t, e, "spindle", 13000,
		model.Observation{Labels: labels, Time: 5000, Value: 999},
		model.Observation{Labels: labels, Time: 12500, Value: 77},
	)
	expected[5000], expected[12500] = 999, 77
	view, err = e.SealJobStatus("spindle")
	if err != nil {
		t.Fatal(err)
	}
	if view.Job.Progress.AddedPending != 1 || view.Job.Progress.InitialPending != 3 {
		t.Fatalf("unexpected split after correction write: %+v", view.Job.Progress)
	}

	// Reopen again before draining the rest.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	final, err := e.SealBatch("spindle", 5)
	if err != nil {
		t.Fatal(err)
	}
	if final.Job.Active || final.Job.Job.State != sealStateComplete {
		t.Fatalf("job did not complete: %+v", final.Job)
	}
	if final.Job.Job.InitialSealed != 12 || final.Job.Job.AddedSealed != 1 ||
		final.Job.Job.CorrectionsSealed != 1 {
		t.Fatalf("work accounting wrong: %+v", final.Job.Job)
	}
	if got := allValues(t, e, "spindle"); !mapsEqual(got, expected) {
		t.Fatalf("sealed data mismatch\n got %v\nwant %v", got, expected)
	}
	if count := len(mustBlocks(t, e)); count < 3 {
		t.Fatalf("expected multiple batch blocks, got %d", count)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSealCorrectionAfterCompaction proves a post-seal correction survives a
// second seal and compaction: the newer head sequence must never be hidden by
// an older block value.
func TestSealCorrectionAfterCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ridgeline.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	labels := model.Labels{"__name__": "load", "node": "a"}
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatal(err)
	}
	ingest(t, e, "spindle", 1000,
		model.Observation{Labels: labels, Time: 100, Value: 1},
		model.Observation{Labels: labels, Time: 200, Value: 2},
	)
	if _, err := e.StartSealJob("spindle", 10); err != nil {
		t.Fatal(err)
	}
	ingest(t, e, "spindle", 2000, model.Observation{Labels: labels, Time: 100, Value: 42})
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatal(err)
	}
	if got := allValues(t, e, "spindle"); got[100] != 42 || got[200] != 2 {
		t.Fatalf("correction lost after seal: %v", got)
	}
	if _, err := e.Compact("spindle"); err != nil {
		t.Fatal(err)
	}
	if got := allValues(t, e, "spindle"); got[100] != 42 || got[200] != 2 {
		t.Fatalf("correction lost after compaction: %v", got)
	}
}

// TestSealJobConflictAndGuards checks mutual exclusion between the two seal
// paths and maintenance that rebuilds storage.
func TestSealJobConflictAndGuards(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ridgeline.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	labels := model.Labels{"__name__": "load", "node": "a"}
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatal(err)
	}
	ingest(t, e, "spindle", 1000,
		model.Observation{Labels: labels, Time: 100, Value: 1},
		model.Observation{Labels: labels, Time: 200, Value: 2},
		model.Observation{Labels: labels, Time: 300, Value: 3},
	)
	if _, err := e.StartSealJob("spindle", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.StartSealJob("spindle", 1); err == nil {
		t.Fatal("expected conflict starting a second active job")
	}
	if _, err := e.Seal("spindle"); err == nil {
		t.Fatal("expected one-shot seal to refuse while a job runs")
	}
	if _, err := e.Compact("spindle"); err == nil {
		t.Fatal("expected compaction to refuse while a job runs")
	}
	if _, err := e.Retain("spindle", 1000); err == nil {
		t.Fatal("expected retention to refuse while a job runs")
	}
	if _, err := e.SealBatch("spindle", 1); err != nil {
		t.Fatal(err)
	}
	// The next advance drains the final point and completes the job (the
	// start call itself was the first batch).
	if done, err := e.SealBatch("spindle", 1); err != nil || done.Job.Active {
		t.Fatalf("job should be complete: %+v err=%v", done, err)
	}
	if _, err := e.Compact("spindle"); err != nil {
		t.Fatalf("compaction after job completion failed: %v", err)
	}
}

func mustBlocks(t *testing.T, e *Engine) []model.Block {
	t.Helper()
	blocks, err := e.Blocks("spindle")
	if err != nil {
		t.Fatal(err)
	}
	return blocks
}

func mapsEqual(a, b map[int64]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
