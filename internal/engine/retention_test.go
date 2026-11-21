package engine

import (
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
	return e
}

func mustCreate(t *testing.T, e *Engine, name string, maxScan int) {
	t.Helper()
	maxWrite := 1000
	if maxScan < maxWrite {
		maxWrite = maxScan
	}
	n := model.Namespace{
		Name: name, RetentionMS: 10000, LatenessMS: 30000, FutureMS: 1000,
		MaxSeries: 100, MaxWrite: maxWrite, MaxScan: maxScan,
	}
	if _, err := e.Create(n); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

func obs(node string, at int64, v float64) model.Observation {
	return model.Observation{Labels: model.Labels{"__name__": "pulse_total", "node": node}, Time: at, Value: v}
}

func visible(t *testing.T, e *Engine, space string) int {
	t.Helper()
	res, err := e.Range(space, model.Query{Start: model.MinTime, End: model.MaxTime + 1})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	return res.Stats.Visible
}

func TestRetentionPreviewIsReadOnlyAndCountsImpact(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	if _, err := e.Write("bravo", model.WriteRequest{Now: 10000, Points: []model.Observation{
		obs("a", 1000, 1), obs("a", 5000, 2), obs("b", 1000, 3),
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	before, _ := e.Namespace("bravo")
	preview, err := e.RetentionPreview("bravo", 12000)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	after, _ := e.Namespace("bravo")
	if before.LastSequence != after.LastSequence || before.Floor != after.Floor {
		t.Fatal("preview mutated the namespace")
	}
	if preview.ProposedFloor != 2000 || preview.CurrentFloor != model.MinTime || !preview.Advances {
		t.Fatalf("unexpected floor proposal: %+v", preview)
	}
	if preview.AffectedPoints != 2 || len(preview.Series) != 2 || preview.VisiblePoints != 3 {
		t.Fatalf("unexpected impact: %+v", preview)
	}
	if preview.Token == "" {
		t.Fatal("preview must return a confirm token")
	}
	// Identical previews are deterministic.
	again, err := e.RetentionPreview("bravo", 12000)
	if err != nil || again.Token != preview.Token {
		t.Fatal("identical previews must yield the same token")
	}
}

func TestRetentionConfirmRejectsStalePreviewAfterLatePoint(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	if _, err := e.Write("bravo", model.WriteRequest{Now: 10000, Points: []model.Observation{
		obs("a", 1000, 1), obs("a", 5000, 2),
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	preview, err := e.RetentionPreview("bravo", 12000)
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	// An old-but-still-legal point arrives during the review window.
	if _, err := e.Write("bravo", model.WriteRequest{Now: 12000, Points: []model.Observation{
		obs("a", 1500, 9),
	}}); err != nil {
		t.Fatalf("late write: %v", err)
	}
	if _, err := e.RetentionConfirm("bravo", preview.Token, "reject"); err == nil {
		t.Fatal("stale token must be rejected by default")
	}
	refreshed, err := e.RetentionPreview("bravo", 12000)
	if err != nil {
		t.Fatalf("recompute: %v", err)
	}
	if refreshed.AffectedPoints != 2 {
		t.Fatalf("recomputed preview must count the late point, got %d", refreshed.AffectedPoints)
	}
	// Continuing with the stale token still works and removes current data.
	result, err := e.RetentionConfirm("bravo", preview.Token, "continue")
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if !result.StalePreview || result.Removed != 2 || result.Floor != 2000 {
		t.Fatalf("unexpected continue receipt: %+v", result)
	}
	if got := visible(t, e, "bravo"); got != 1 {
		t.Fatalf("one point should survive, got %d", got)
	}
	// Resurrection below the new floor is forbidden.
	if _, err := e.Write("bravo", model.WriteRequest{Now: 12000, Points: []model.Observation{
		obs("a", 1500, 9),
	}}); err == nil {
		t.Fatal("point below the retention floor must be rejected")
	}
}

func TestRetentionFloorNeverMovesBackward(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	if _, err := e.Write("bravo", model.WriteRequest{Now: 10000, Points: []model.Observation{
		obs("a", 5000, 1),
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	preview, _ := e.RetentionPreview("bravo", 12000)
	if _, err := e.RetentionConfirm("bravo", preview.Token, "reject"); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	back, err := e.RetentionPreview("bravo", 5000)
	if err != nil {
		t.Fatalf("backward preview: %v", err)
	}
	if back.ProposedFloor != 2000 || back.Advances {
		t.Fatalf("regressing now must not lower the floor: %+v", back)
	}
	result, err := e.RetentionConfirm("bravo", back.Token, "reject")
	if err != nil {
		t.Fatalf("backward confirm: %v", err)
	}
	if !result.NoOp || result.Floor != 2000 || result.Sequence != 0 {
		t.Fatalf("regressing now should be a no-op: %+v", result)
	}
}

func TestRetentionTokenValidation(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	mustCreate(t, e, "alpha", 100000)
	if _, err := e.Write("bravo", model.WriteRequest{Now: 10000, Points: []model.Observation{
		obs("a", 5000, 1),
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	preview, _ := e.RetentionPreview("bravo", 12000)
	for _, tc := range []struct {
		name  string
		space string
		token string
	}{
		{"empty", "bravo", ""},
		{"garbage", "bravo", "not-a-token"},
		{"tampered", "bravo", preview.Token[:max(1, len(preview.Token)-2)] + "AA"},
		{"foreign namespace", "alpha", preview.Token},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := e.RetentionConfirm(tc.space, tc.token, "reject"); err == nil {
				t.Fatal("expected token rejection")
			}
		})
	}
	if _, err := e.RetentionConfirm("bravo", preview.Token, "pause"); err == nil {
		t.Fatal("invalid on_stale must be rejected")
	}
}

func TestRetentionBudgetFailureIsNamespaceScoped(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	mustCreate(t, e, "alpha", 100)
	points := make([]model.Observation, 200)
	for i := range points {
		points[i] = obs("x", int64(i)*500, float64(i))
	}
	if _, err := e.Write("alpha", model.WriteRequest{Now: 100000, Points: points[:100]}); err != nil {
		t.Fatalf("alpha write one: %v", err)
	}
	if _, err := e.Write("alpha", model.WriteRequest{Now: 100000, Points: points[100:]}); err != nil {
		t.Fatalf("alpha write two: %v", err)
	}
	if _, err := e.Write("bravo", model.WriteRequest{Now: 100000, Points: points[:10]}); err != nil {
		t.Fatalf("bravo write: %v", err)
	}
	bravoBefore, _ := e.Namespace("bravo")
	if _, err := e.RetentionPreview("alpha", 102000); err == nil {
		t.Fatal("alpha preview must exceed its scan budget")
	}
	alphaAfter, _ := e.Namespace("alpha")
	if alphaAfter.Floor != model.MinTime {
		t.Fatal("failed preview must not move the alpha floor")
	}
	bravoAfter, _ := e.Namespace("bravo")
	if bravoAfter.LastSequence != bravoBefore.LastSequence {
		t.Fatal("alpha budget failure must not modify bravo")
	}
	preview, err := e.RetentionPreview("bravo", 102000)
	if err != nil {
		t.Fatalf("bravo preview must still succeed: %v", err)
	}
	if preview.BudgetExhausted {
		t.Fatal("bravo has ample headroom and must not report budget exhaustion")
	}
	if _, err := e.RetentionConfirm("bravo", preview.Token, "reject"); err != nil {
		t.Fatalf("bravo confirm: %v", err)
	}
}

func TestRetainImmediatePathPhysicallyDropsPoints(t *testing.T) {
	e := testEngine(t)
	mustCreate(t, e, "bravo", 100000)
	if _, err := e.Write("bravo", model.WriteRequest{Now: 10000, Points: []model.Observation{
		obs("a", 1000, 1), obs("a", 5000, 2),
	}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	receipt, err := e.Retain("bravo", 12000)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if receipt.Floor != 2000 || receipt.Points != 1 {
		t.Fatalf("unexpected immediate receipt: %+v", receipt)
	}
	if got := visible(t, e, "bravo"); got != 1 {
		t.Fatalf("one point should survive, got %d", got)
	}
	blocks, _ := e.Blocks("bravo")
	if len(blocks) != 1 || blocks[0].Count != 1 {
		t.Fatalf("block should contain only the surviving point: %+v", blocks)
	}
}
