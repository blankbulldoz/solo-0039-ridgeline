package engine

import (
	"os"
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

// TestQuantileViewSurvivesImageRestore confirms a materialized quantile view
// is part of the durable database image: the backup snapshot includes the
// quantiles bucket and a reopened engine sees the same summaries and freshness.
func TestQuantileViewSurvivesImageRestore(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	points := make([]model.Observation, 0, 800)
	for i := 0; i < 800; i++ {
		points = append(points, model.Observation{
			Labels: labels("lat", "n"), Time: int64(i), Value: float64(i * 3),
		})
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		Epsilon: 0.02, Phis: []float64{0.95},
	}
	mat, err := e.MaterializeQuantile("spindle", "persist", q)
	if err != nil {
		t.Fatal(err)
	}
	image, err := e.DB.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(restoredPath, image.Data, 0600); err != nil {
		t.Fatal(err)
	}
	re, err := Open(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	view, err := re.QuantileView("spindle", "persist")
	if err != nil {
		t.Fatal(err)
	}
	if view.Stale {
		t.Fatal("restored view should not be stale with no intervening writes")
	}
	if view.SourceSequence != mat.SourceSequence {
		t.Fatalf("sequence %d != %d", view.SourceSequence, mat.SourceSequence)
	}
	if len(view.Summaries) != len(mat.Summaries) {
		t.Fatalf("summary count %d != %d", len(view.Summaries), len(mat.Summaries))
	}
	if view.Result.Cells[0].Estimates[0].Value !=
		mat.Result.Cells[0].Estimates[0].Value {
		t.Fatal("restored quantile estimate differs")
	}
}
