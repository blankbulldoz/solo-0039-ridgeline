package engine

import (
	"math"
	"path/filepath"
	"sort"
	"testing"

	"ridgeline/internal/model"
)

func newTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "quantile.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, path
}

func baseNamespace() model.Namespace {
	return model.Namespace{
		Name: "spindle", RetentionMS: 100000, LatenessMS: 1000000, FutureMS: 100000,
		MaxSeries: 1000, MaxWrite: 10000, MaxScan: 100000,
	}
}

func labels(name, node string) model.Labels {
	return model.Labels{"__name__": name, "node": node}
}

// exactP95 computes the true 95th percentile over a value slice.
func exactP95(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := int(math.Ceil(0.95*float64(len(sorted)))) - 1
	return sorted[rank]
}

func TestQuantileSkewedAcrossBlocks(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		GroupBy: []string{"node"}, Epsilon: 0.01, Phis: []float64{0.95},
	}
	// Two distinct series (different group_by node) with skewed values, sealed
	// into separate compressed blocks across two seal cycles.
	var allA, allB []float64
	pts := func(node string, base float64) []model.Observation {
		out := make([]model.Observation, 0, 2000)
		for i := 0; i < 2000; i++ {
			// Heavy skew: a few large values dominate the tail.
			v := math.Pow(float64(i+1)/2000.0, 6)*1000 + base
			out = append(out, model.Observation{Labels: labels("latency", node), Time: int64(i), Value: v})
		}
		return out
	}
	a := pts("a", 0)
	b := pts("b", 5000)
	for _, p := range a {
		allA = append(allA, p.Value)
	}
	for _, p := range b {
		allB = append(allB, p.Value)
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: a}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: b}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatal(err)
	}
	res, err := e.Quantile("spindle", q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 2 {
		t.Fatalf("want 2 grouped cells, got %d", len(res.Cells))
	}
	exact := map[string]float64{"a": exactP95(allA), "b": exactP95(allB)}
	for _, cell := range res.Cells {
		if len(cell.Estimates) != 1 || cell.Count != 2000 {
			t.Fatalf("unexpected cell: %+v", cell)
		}
		est := cell.Estimates[0]
		if est.Phi != 0.95 || est.RankError <= 0 {
			t.Fatalf("bad estimate: %+v", est)
		}
		want := exact[cell.Labels["node"]]
		if want < est.Lower || want > est.Upper {
			t.Fatalf("node %s exact p95 %v outside bracket [%v,%v]",
				cell.Labels["node"], want, est.Lower, est.Upper)
		}
		if est.Lower > est.Value || est.Value > est.Upper {
			t.Fatalf("estimate outside its own bracket: %+v", est)
		}
	}
}

func TestQuantileDuplicates(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	// 90% of values are identical; p95 must still resolve to a real observed
	// value and the bracket must contain the exact quantile.
	var points []model.Observation
	var values []float64
	for i := 0; i < 2000; i++ {
		v := 7.0
		if i >= 1800 {
			v = float64(90 + i)
		}
		points = append(points, model.Observation{Labels: labels("dup", "x"), Time: int64(i), Value: v})
		values = append(values, v)
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		Epsilon: 0.02, Phis: []float64{0.5, 0.95, 0.99},
	}
	res, err := e.Quantile("spindle", q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 1 {
		t.Fatalf("want 1 cell, got %d", len(res.Cells))
	}
	exact := map[float64]float64{}
	for _, phi := range []float64{0.5, 0.95, 0.99} {
		sorted := append([]float64(nil), values...)
		sort.Float64s(sorted)
		exact[phi] = sorted[int(math.Ceil(phi*float64(len(sorted))))-1]
	}
	for _, est := range res.Cells[0].Estimates {
		if exact[est.Phi] < est.Lower || exact[est.Phi] > est.Upper {
			t.Fatalf("phi %v exact %v outside bracket [%v,%v]", est.Phi, exact[est.Phi], est.Lower, est.Upper)
		}
	}
}

func TestQuantileLateOverwriteInvalidatesMaterialization(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	mk := func(v float64, ts int64) model.Observation {
		return model.Observation{Labels: labels("lat", "n"), Time: ts, Value: v}
	}
	points := make([]model.Observation, 0, 1000)
	for i := 0; i < 1000; i++ {
		points = append(points, mk(float64(i), int64(i)))
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		Epsilon: 0.05, Phis: []float64{0.95},
	}
	if _, err := e.MaterializeQuantile("spindle", "p95view", q); err != nil {
		t.Fatal(err)
	}
	view, err := e.QuantileView("spindle", "p95view")
	if err != nil || view.Stale {
		t.Fatalf("fresh materialization reported stale: %+v err=%v", view, err)
	}
	before := view.Result.Cells[0].Estimates[0].Value
	beforeSeq := view.SourceSequence
	beforeSnapshotN := view.Summaries[0].Snapshot.N

	// A batch of late corrections overwrites the upper tail. The p95 region of
	// the data changes substantially even though the point count is unchanged.
	corrections := make([]model.Observation, 0, 60)
	for ts := int64(940); ts < 1000; ts++ {
		corrections = append(corrections, mk(90000+float64(ts), ts))
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: corrections}); err != nil {
		t.Fatal(err)
	}
	view, err = e.QuantileView("spindle", "p95view")
	if err != nil {
		t.Fatal(err)
	}
	if !view.Stale {
		t.Fatal("materialization must be stale after a late overwrite")
	}
	// The stale stored value and source sequence must not change until refresh.
	if view.Result.Cells[0].Estimates[0].Value != before {
		t.Fatal("stale view must retain its previous estimates until refreshed")
	}
	if view.SourceSequence != beforeSeq {
		t.Fatal("stale view must retain its source sequence until refreshed")
	}
	refreshed, err := e.RefreshQuantile("spindle", "p95view")
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Stale {
		t.Fatal("refreshed view must be current")
	}
	if refreshed.SourceSequence <= beforeSeq {
		t.Fatal("refresh must advance the source sequence")
	}
	// Recompute rebuilt the summary over the same number of points.
	if refreshed.Summaries[0].Snapshot.N != beforeSnapshotN {
		t.Fatalf("recomputed summary n=%d want %d",
			refreshed.Summaries[0].Snapshot.N, beforeSnapshotN)
	}
	// The corrected exact p95 (index ceil(.95*1000)-1 = 949 -> timestamp 949,
	// overwritten to 90949) must lie within the refreshed bracket.
	est := refreshed.Result.Cells[0].Estimates[0]
	const exactP95 = 90949.0
	if exactP95 < est.Lower || exactP95 > est.Upper {
		t.Fatalf("corrected exact p95 %v outside refreshed bracket [%v,%v]",
			exactP95, est.Lower, est.Upper)
	}
	// A live ad-hoc query reflects the correction immediately: both the
	// refreshed materialization and the fresh ad-hoc summary bracket the exact
	// corrected p95. Two independent GK summaries may pick different observed
	// values inside the same error envelope, so they are compared on the
	// guarantee rather than point equality.
	live, err := e.Quantile("spindle", q)
	if err != nil {
		t.Fatal(err)
	}
	liveEst := live.Cells[0].Estimates[0]
	if exactP95 < liveEst.Lower || exactP95 > liveEst.Upper {
		t.Fatalf("live exact p95 %v outside bracket [%v,%v]",
			exactP95, liveEst.Lower, liveEst.Upper)
	}
}

func TestQuantileDeletionRecomputeBoundary(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	points := make([]model.Observation, 0, 1000)
	for i := 0; i < 1000; i++ {
		points = append(points, model.Observation{
			Labels: labels("lat", "n"), Time: int64(i), Value: float64(i),
		})
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		Epsilon: 0.01, Phis: []float64{0.95},
	}
	if _, err := e.MaterializeQuantile("spindle", "delview", q); err != nil {
		t.Fatal(err)
	}
	// Delete the top 100 timestamps; the p95 must move down after refresh.
	if _, err := e.Delete("spindle", model.Query{Start: 900, End: 1000, Matchers: []model.Matcher{
		{Label: "__name__", Op: "=", Value: "lat"},
	}}); err != nil {
		t.Fatal(err)
	}
	view, err := e.QuantileView("spindle", "delview")
	if err != nil || !view.Stale {
		t.Fatalf("view must be stale after deletion: %+v err=%v", view, err)
	}
	refreshed, err := e.RefreshQuantile("spindle", "delview")
	if err != nil {
		t.Fatal(err)
	}
	// 900 points remain; their exact p95 is index ceil(.95*900)-1 = 854 -> 854.
	if refreshed.Result.Cells[0].Count != 900 {
		t.Fatalf("refreshed count %d, want 900", refreshed.Result.Cells[0].Count)
	}
	est := refreshed.Result.Cells[0].Estimates[0]
	if 854.0 < est.Lower || 854.0 > est.Upper {
		t.Fatalf("post-delete exact p95 854 outside bracket [%v,%v]", est.Lower, est.Upper)
	}
}

func TestQuantileCompactionRecomputesViews(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	points := make([]model.Observation, 0, 1500)
	for i := 0; i < 1500; i++ {
		points = append(points, model.Observation{
			Labels: labels("lat", "n"), Time: int64(i), Value: float64(i * 2),
		})
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatal(err)
	}
	q := model.QuantileQuery{
		Start: model.MinTime, End: model.MaxTime + 1, Step: 1 << 52, Origin: 0,
		Epsilon: 0.01, Phis: []float64{0.95},
	}
	mat, err := e.MaterializeQuantile("spindle", "compactview", q)
	if err != nil {
		t.Fatal(err)
	}
	oldSeq := mat.SourceSequence
	if _, err := e.Compact("spindle"); err != nil {
		t.Fatal(err)
	}
	view, err := e.QuantileView("spindle", "compactview")
	if err != nil {
		t.Fatal(err)
	}
	if view.Stale {
		t.Fatal("compaction must recompute quantile views in the same transaction")
	}
	if view.SourceSequence <= oldSeq {
		t.Fatalf("recomputed view sequence not advanced: %d <= %d", view.SourceSequence, oldSeq)
	}
	// The recomputed estimates must still bracket the exact p95.
	var values []float64
	for i := 0; i < 1500; i++ {
		values = append(values, float64(i*2))
	}
	want := exactP95(values)
	est := view.Result.Cells[0].Estimates[0]
	if want < est.Lower || want > est.Upper {
		t.Fatalf("post-compaction exact p95 %v outside bracket [%v,%v]", want, est.Lower, est.Upper)
	}
}

func TestQuantilePrecisionValidation(t *testing.T) {
	e, _ := newTestEngine(t)
	if _, err := e.Create(baseNamespace()); err != nil {
		t.Fatal(err)
	}
	valid := model.QuantileQuery{
		Start: 0, End: 1000, Step: 100, Origin: 0, Epsilon: 0.01, Phis: []float64{0.95},
	}
	if _, err := e.Quantile("spindle", valid); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	for _, bad := range []model.QuantileQuery{
		{Start: 0, End: 1000, Step: 100, Origin: 0, Epsilon: 0, Phis: []float64{0.95}},
		{Start: 0, End: 1000, Step: 100, Origin: 0, Epsilon: 0.01, Phis: nil},
		{Start: 0, End: 1000, Step: 100, Origin: 0, Epsilon: 0.01, Phis: []float64{1.5}},
		{Start: 0, End: 1000, Step: 0, Origin: 0, Epsilon: 0.01, Phis: []float64{0.95}},
	} {
		if _, err := e.Quantile("spindle", bad); err == nil {
			t.Fatalf("invalid query accepted: %+v", bad)
		}
	}
}
