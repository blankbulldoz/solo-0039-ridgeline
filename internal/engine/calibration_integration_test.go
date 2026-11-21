package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	e, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func labNamespace() model.Namespace {
	return model.Namespace{
		Name: "lab", RetentionMS: 100000, LatenessMS: 300000, FutureMS: 1000,
		MaxSeries: 100, MaxWrite: 10000, MaxScan: 100000,
	}
}

func probeLabels() model.Labels {
	return model.Labels{"__name__": "probe_celsius", "node": "thermo-1", "zone": "bench"}
}

func factoryView(factor, offset float64) model.Calibration {
	return model.Calibration{
		Name: "factory",
		Rules: []model.CalibrationRule{{
			Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe_celsius"}},
			Segments: []model.CalibrationSegment{
				{Start: 0, End: 2000, Factor: factor, Offset: offset},
				{Start: 2000, End: 10000, Factor: 1.1, Offset: 2.0},
			},
		}},
	}
}

func TestCalibrationLifecycleAndCorrection(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(labNamespace()); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := e.Write("lab", model.WriteRequest{Now: 2000, Points: []model.Observation{
		{Labels: probeLabels(), Time: 1000, Value: 10},
		{Labels: probeLabels(), Time: 2000, Value: 30},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	view, err := e.PutCalibration("lab", factoryView(1.0, 0.0))
	if err != nil {
		t.Fatalf("put view: %v", err)
	}
	if view.Revision != 1 || view.Sequence == 0 {
		t.Fatalf("unexpected initial view: %+v", view)
	}

	q := model.Query{
		Start: 0, End: 3000, Calibration: "factory",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe_celsius"}},
	}
	cal, err := e.CalibratedRange("lab", q)
	if err != nil {
		t.Fatalf("calibrated range: %v", err)
	}
	got := map[int64]model.CalibratedPoint{}
	for _, sp := range cal.Series {
		for _, p := range sp.Points {
			got[p.Time] = p
		}
	}
	if got[1000].Value != 10 || got[2000].Value != 35 || !got[2000].Calibrated {
		t.Fatalf("unexpected calibrated values: %+v", got)
	}

	// Raw reads remain untouched and expose no calibration fields.
	rawQ := model.Query{Start: 0, End: 3000, Matchers: q.Matchers}
	raw, err := e.Range("lab", rawQ)
	if err != nil {
		t.Fatalf("raw range: %v", err)
	}
	if raw.Series[0].Points[0].Value != 10 {
		t.Fatal("raw value must stay equal to the original observation")
	}

	// A materialized calibrated rollup is fresh, then stale after correction.
	aggQ := model.Query{
		Start: 0, End: 3000, Step: 3000, Origin: 0, GroupBy: []string{"zone"},
		Function: "avg", Matchers: q.Matchers, Calibration: "factory",
	}
	if _, err := e.Materialize("lab", "bench_cal", aggQ); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	rollup, err := e.Rollup("lab", "bench_cal")
	if err != nil {
		t.Fatalf("read rollup: %v", err)
	}
	if rollup.Stale || rollup.Calibration != "factory" {
		t.Fatalf("new rollup must be fresh and name its view: %+v", rollup)
	}

	corrected, err := e.PutCalibration("lab", factoryView(1.2, 1.0))
	if err != nil {
		t.Fatalf("correct view: %v", err)
	}
	if corrected.Revision != 2 {
		t.Fatalf("correction must bump revision, got %d", corrected.Revision)
	}
	rollup, err = e.Rollup("lab", "bench_cal")
	if err != nil {
		t.Fatalf("read rollup after correction: %v", err)
	}
	if !rollup.Stale {
		t.Fatal("rollup must be stale after coefficient correction")
	}

	cal2, err := e.CalibratedRange("lab", q)
	if err != nil {
		t.Fatalf("calibrated range after correction: %v", err)
	}
	got2 := map[int64]model.CalibratedPoint{}
	for _, sp := range cal2.Series {
		for _, p := range sp.Points {
			got2[p.Time] = p
		}
	}
	// 1.2*10+1 = 13 for the corrected era; the other segment stays 35.
	if got2[1000].Value != 13 || got2[1000].Factor != 1.2 || got2[1000].Offset != 1 {
		t.Fatalf("corrected history mismatch: %+v", got2[1000])
	}
	if got2[2000].Value != 35 {
		t.Fatalf("untouched segment changed: %+v", got2[2000])
	}

	// Refreshing the rollup recomputes against the corrected coefficients.
	if _, err := e.Materialize("lab", "bench_cal", aggQ); err != nil {
		t.Fatalf("refresh rollup: %v", err)
	}
	rollup, _ = e.Rollup("lab", "bench_cal")
	if rollup.Stale {
		t.Fatal("refreshed rollup must be fresh")
	}
	wantAvg := (13.0 + 35.0) / 2.0
	if rollup.Result.Cells[0].Value != wantAvg {
		t.Fatalf("refreshed rollup avg = %v, want %v", rollup.Result.Cells[0].Value, wantAvg)
	}
}

func TestCalibrationAcrossSealAndCompact(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(labNamespace()); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := e.Write("lab", model.WriteRequest{Now: 2000, Points: []model.Observation{
		{Labels: probeLabels(), Time: 1000, Value: 10},
		{Labels: probeLabels(), Time: 2000, Value: 30},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := e.PutCalibration("lab", factoryView(1.0, 0.0)); err != nil {
		t.Fatalf("put view: %v", err)
	}
	q := model.Query{
		Start: 0, End: 3000, Calibration: "factory",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe_celsius"}},
	}
	fromHead, err := e.CalibratedRange("lab", q)
	if err != nil {
		t.Fatalf("head calibrated range: %v", err)
	}
	if _, err := e.Seal("lab"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	fromBlocks, err := e.CalibratedRange("lab", q)
	if err != nil {
		t.Fatalf("block calibrated range: %v", err)
	}
	if !calibratedRangesEqual(fromHead, fromBlocks) {
		t.Fatal("sealing head into blocks changed calibrated output")
	}
	if _, err := e.Compact("lab"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	afterCompact, err := e.CalibratedRange("lab", q)
	if err != nil {
		t.Fatalf("post-compact calibrated range: %v", err)
	}
	if !calibratedRangesEqual(fromHead, afterCompact) {
		t.Fatal("compaction changed calibrated output")
	}
}

func calibratedRangesEqual(a, b model.CalibratedRangeResult) bool {
	if len(a.Series) != len(b.Series) {
		return false
	}
	for i := range a.Series {
		if a.Series[i].Series.ID != b.Series[i].Series.ID ||
			len(a.Series[i].Points) != len(b.Series[i].Points) {
			return false
		}
		for j := range a.Series[i].Points {
			if a.Series[i].Points[j] != b.Series[i].Points[j] {
				return false
			}
		}
	}
	return true
}

func TestCalibrationValidationAndUnknownView(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(labNamespace()); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := e.Write("lab", model.WriteRequest{Now: 1000, Points: []model.Observation{
		{Labels: probeLabels(), Time: 500, Value: 1},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	overlap := factoryView(1, 0)
	overlap.Rules[0].Segments[0].End = 2500
	if _, err := e.PutCalibration("lab", overlap); err == nil {
		t.Fatal("overlapping segments must be rejected")
	}
	cross := model.Calibration{
		Name: "cross",
		Rules: []model.CalibrationRule{
			factoryView(1, 0).Rules[0],
			{Matchers: []model.Matcher{{Label: "node", Op: "=", Value: "thermo-1"}},
				Segments: []model.CalibrationSegment{{Start: 0, End: 10000, Factor: 2, Offset: 0}}},
		},
	}
	if _, err := e.PutCalibration("lab", cross); err == nil {
		t.Fatal("rules matching the same series must be rejected")
	}

	q := model.Query{
		Start: 0, End: 1000, Calibration: "ghost",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe_celsius"}},
	}
	if _, err := e.CalibratedRange("lab", q); err == nil {
		t.Fatal("unknown calibration view must be an explicit error")
	}
	if _, err := e.Aggregate("lab", q); err == nil {
		t.Fatal("unknown calibration view on aggregate must be an explicit error")
	}
}

func TestCalibrationRejectsReadOnlyNamespace(t *testing.T) {
	e := openTestEngine(t)
	if _, err := e.Create(labNamespace()); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := e.ReadOnly("lab", true); err != nil {
		t.Fatalf("mark read-only: %v", err)
	}
	if _, err := e.PutCalibration("lab", factoryView(1, 0)); err == nil {
		t.Fatal("read-only namespace must reject calibration view writes")
	}
}

func TestCalibrationNamespaceIsolation(t *testing.T) {
	e := openTestEngine(t)
	other := labNamespace()
	other.Name = "other"
	if _, err := e.Create(labNamespace()); err != nil {
		t.Fatalf("create lab: %v", err)
	}
	if _, err := e.Create(other); err != nil {
		t.Fatalf("create other: %v", err)
	}
	if _, err := e.Write("lab", model.WriteRequest{Now: 1000, Points: []model.Observation{
		{Labels: probeLabels(), Time: 500, Value: 1},
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := e.PutCalibration("lab", factoryView(2, 0)); err != nil {
		t.Fatalf("put view: %v", err)
	}
	// The same view name must not leak into another namespace.
	if _, err := e.GetCalibration("other", "factory"); err == nil {
		t.Fatal("calibration view must be scoped to its namespace")
	}
	views, err := e.ListCalibrations("other")
	if err != nil || len(views) != 0 {
		t.Fatalf("other namespace should see no views, got %v (%v)", views, err)
	}
}
