package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func testNamespace() model.Namespace {
	return model.Namespace{
		Name: "space", RetentionMS: 10000, LatenessMS: 2000, FutureMS: 1000,
		MaxSeries: 2, MaxWrite: 100, MaxScan: 1000,
	}
}

var labelsA = model.Labels{"__name__": "metric", "node": "a"}
var labelsB = model.Labels{"__name__": "metric", "node": "b"}
var labelsC = model.Labels{"__name__": "metric", "node": "c"}

func TestAtomicWriteRemainsAllOrNothing(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatal(err)
	}
	_, err := e.Write("space", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labelsA, Time: 9000, Value: 1},
		{Labels: labelsA, Time: 30000, Value: 2}, // future skew
	}})
	if err == nil {
		t.Fatal("atomic write with a future point must fail")
	}
	got, err := e.Range("space", model.Query{Start: 0, End: 40000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Series) != 0 {
		t.Fatalf("rejected atomic request published data: %+v", got.Series)
	}
}

func TestBestEffortMixedBatchReceipt(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Write("space", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labelsA, Time: 9000, Value: 1},
		{Labels: labelsB, Time: 9000, Value: 2},
	}}); err != nil {
		t.Fatal(err)
	}

	receipt, err := e.Write("space", model.WriteRequest{
		Now: 10000, Mode: model.WriteBestEffort, Points: []model.Observation{
			{Labels: labelsA, Time: 9500, Value: 10}, // 0 accepted (new point)
			{Labels: labelsA, Time: 9500, Value: 11}, // 1 overwritten by 3
			{Labels: labelsB, Time: 6000, Value: 4},  // 2 late
			{Labels: labelsA, Time: 9500, Value: 12}, // 3 duplicate winner, accepted
			{Labels: labelsC, Time: 9500, Value: 5},  // 4 over series quota
			{Labels: labelsB, Time: 12000, Value: 6}, // 5 future skew
			{Labels: labelsA, Time: 9000, Value: 99}, // 6 correction, overwritten
		},
	})
	if err != nil {
		t.Fatalf("best effort must not return an error: %v", err)
	}
	if receipt.Requested != 7 || receipt.Accepted != 1 ||
		receipt.Overwritten != 3 || receipt.Rejected != 3 {
		t.Fatalf("unexpected counts: %+v", receipt)
	}
	if receipt.NewSeries != 0 {
		t.Fatalf("quota rejection created a series: %d", receipt.NewSeries)
	}
	want := map[int]string{
		0: model.PointOverwritten, 1: model.PointOverwritten, 2: model.PointRejected,
		3: model.PointAccepted, 4: model.PointRejected, 5: model.PointRejected,
		6: model.PointOverwritten,
	}
	for _, r := range receipt.Results {
		if r.Status != want[r.Index] {
			t.Fatalf("index %d: got %s want %s", r.Index, r.Status, want[r.Index])
		}
	}
	// Watermark only moved through the accepted t=9500 point.
	if receipt.Watermark != 9500 {
		t.Fatalf("watermark = %d, want 9500", receipt.Watermark)
	}
}

func TestBestEffortWatermarkAndQuotaOnlyAdvanceOnAcceptedData(t *testing.T) {
	e := openTestEngine(t)
	ns := testNamespace()
	ns.MaxSeries = 1 // the single seed series fills the quota
	if _, err := e.Create(ns); err != nil {
		t.Fatal(err)
	}
	// Seed one series at t=9000; quota allows one more.
	if _, err := e.Write("space", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labelsA, Time: 9000, Value: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	receipt, err := e.Write("space", model.WriteRequest{
		Now: 10000, Mode: model.WriteBestEffort, Points: []model.Observation{
			{Labels: labelsB, Time: 5000, Value: 2}, // late -> rejected
			{Labels: labelsC, Time: 9500, Value: 3}, // quota -> rejected
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Accepted != 0 || receipt.Rejected != 2 {
		t.Fatalf("both points must reject: %+v", receipt)
	}
	ns, err = e.Namespace("space")
	if err != nil {
		t.Fatal(err)
	}
	if ns.Watermark != 9000 {
		t.Fatalf("rejected points moved the watermark to %d", ns.Watermark)
	}
	series, err := e.Discover("space", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("quota rejection created series identity: %d", len(series))
	}
	// A request that admits nothing must not consume a database sequence.
	if ns.LastSequence != receipt.Sequence {
		t.Fatalf("no-op best effort changed sequence: %d -> %d",
			ns.LastSequence, receipt.Sequence)
	}
}

func TestBestEffortBackfillThenObservations(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Write("space", model.WriteRequest{Now: 10000, Points: []model.Observation{
		{Labels: labelsA, Time: 9000, Value: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	first, err := e.Write("space", model.WriteRequest{
		Now: 10000, Mode: model.WriteBestEffort,
		Points: []model.Observation{{Labels: labelsA, Time: 12000, Value: 6}},
	})
	if err != nil || first.Rejected != 1 ||
		first.Results[0].Reason != "future_skew_exceeded" {
		t.Fatalf("future point should reject: %+v err=%v", first, err)
	}
	// Client replays only the rejected point once observation time catches up.
	backfill, err := e.Write("space", model.WriteRequest{
		Now: 12000, Mode: model.WriteBestEffort,
		Points: []model.Observation{{Labels: labelsA, Time: 12000, Value: 6}},
	})
	if err != nil || backfill.Accepted != 1 || backfill.Watermark != 12000 {
		t.Fatalf("backfill failed: %+v err=%v", backfill, err)
	}
	got, err := e.Range("space", model.Query{Start: 0, End: 20000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Series) != 1 || len(got.Series[0].Points) != 2 {
		t.Fatalf("expected two stored points, got %+v", got.Series)
	}
}

func TestInvalidWriteModeRejected(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Write("space", model.WriteRequest{
		Now: 10000, Mode: "speculative",
		Points: []model.Observation{{Labels: labelsA, Time: 9000, Value: 1}},
	}); err == nil {
		t.Fatal("unknown write mode must be rejected")
	}
}
