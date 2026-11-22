package engine

import (
	"errors"
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
)

func openTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(dir, "ridge.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func testNamespace(name string) model.Namespace {
	return model.Namespace{
		Name: name, RetentionMS: 10000, LatenessMS: 30000, FutureMS: 1000,
		MaxSeries: 100, MaxWrite: 100, MaxScan: 1000,
	}
}

func points(times ...int64) []model.Observation {
	out := make([]model.Observation, 0, len(times))
	for _, tm := range times {
		out = append(out, model.Observation{
			Labels: model.Labels{"__name__": "load"},
			Time:   tm,
			Value:  float64(tm),
		})
	}
	return out
}

func conflictCode(err error) string {
	var domain *model.Error
	if errors.As(err, &domain) {
		return domain.Code
	}
	return ""
}

func TestDrainStopsAcceptanceButKeepsReads(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 10, Points: points(1, 2)}); err != nil {
		t.Fatalf("write: %v", err)
	}

	view, err := e.StartDrain("spindle")
	if err != nil {
		t.Fatalf("start drain: %v", err)
	}
	if view.Drain.State != model.StateDraining || view.Drain.AcceptingLogical {
		t.Fatalf("unexpected drain view: %+v", view.Drain)
	}
	if !view.Drain.PhysicalMaintenance {
		t.Fatalf("physical maintenance must stay available: %+v", view.Drain)
	}

	if _, err := e.Write("spindle", model.WriteRequest{Now: 10, Points: points(3)}); conflictCode(err) != "conflict" {
		t.Fatalf("write during drain: got %v", err)
	}
	if _, err := e.Delete("spindle", model.Query{Start: 0, End: 10}); conflictCode(err) != "conflict" {
		t.Fatalf("delete during drain: got %v", err)
	}
	if _, err := e.Retain("spindle", 10); conflictCode(err) != "conflict" {
		t.Fatalf("retention during drain: got %v", err)
	}
	if _, err := e.Materialize("spindle", "r", model.Query{
		Start: 0, End: 10, Step: 1, Function: "count",
	}); conflictCode(err) != "conflict" {
		t.Fatalf("materialize during drain: got %v", err)
	}
	result, err := e.Range("spindle", model.Query{Start: 0, End: 10})
	if err != nil || len(result.Series) != 1 {
		t.Fatalf("range during drain: %+v %v", result, err)
	}

	// Physical maintenance must still work while acceptance is stopped.
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatalf("seal during drain: %v", err)
	}
	if _, err := e.Compact("spindle"); err != nil {
		t.Fatalf("compact during drain: %v", err)
	}
}

func TestCompleteDrainWaitsForInFlightWrites(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Simulate an admitted logical request that has not committed yet.
	release, err := e.beginLogical("spindle")
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	view, err := e.StartDrain("spindle")
	if err != nil {
		t.Fatalf("start drain: %v", err)
	}
	if view.Drain.InFlight != 1 || view.Drain.Phase != "stopping" {
		t.Fatalf("in-flight request not visible: %+v", view.Drain)
	}

	if _, err := e.CompleteDrain("spindle"); conflictCode(err) != "conflict" {
		t.Fatalf("completion with in-flight work must conflict, got %v", err)
	}
	release()

	view, err = e.CompleteDrain("spindle")
	if err != nil {
		t.Fatalf("complete drain: %v", err)
	}
	if view.Drain.State != model.StateReadOnly || !view.Drain.Maintainable || view.Drain.InFlight != 0 {
		t.Fatalf("unexpected maintainable view: %+v", view.Drain)
	}
	if view.Drain.CompletedSequence <= view.Drain.StartedSequence {
		t.Fatalf("completion must have its own later sequence: %+v", view.Drain)
	}
	if _, err := e.Seal("spindle"); err != nil {
		t.Fatalf("seal in read-only: %v", err)
	}
}

func TestCancelDrainRestoresWritesAndIsObservable(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.StartDrain("spindle"); err != nil {
		t.Fatalf("start drain: %v", err)
	}
	view, err := e.CancelDrain("spindle")
	if err != nil {
		t.Fatalf("cancel drain: %v", err)
	}
	if view.Drain.State != model.StateWritable || !view.Drain.AcceptingWrites {
		t.Fatalf("unexpected cancel view: %+v", view.Drain)
	}
	if view.Drain.CanceledSequence == 0 || view.Drain.CanceledSequence <= view.Drain.StartedSequence {
		t.Fatalf("cancel must be sequenced: %+v", view.Drain)
	}
	if _, err := e.Write("spindle", model.WriteRequest{Now: 10, Points: points(1)}); err != nil {
		t.Fatalf("write after cancel: %v", err)
	}
}

func TestCompleteRequiresStartedDrainAndOrdersSequences(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e.CompleteDrain("spindle"); conflictCode(err) != "conflict" {
		t.Fatalf("complete before start must conflict, got %v", err)
	}
	started, err := e.StartDrain("spindle")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	before := started.Namespace.LastSequence
	if _, err := e.Write("spindle", model.WriteRequest{Now: 10, Points: points(1)}); err == nil {
		t.Fatalf("logical mutation must be rejected while draining")
	}
	after, err := e.Namespace("spindle")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if after.Namespace.LastSequence != before {
		t.Fatalf("rejected mutation changed the sequence")
	}
	done, err := e.CompleteDrain("spindle")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	for _, err := range []error{
		(func() error { _, err := e.Write("spindle", model.WriteRequest{Now: 10, Points: points(2)}); return err })(),
		(func() error { _, err := e.Delete("spindle", model.Query{Start: 0, End: 1}); return err })(),
		(func() error { _, err := e.Retain("spindle", 10); return err })(),
		(func() error {
			_, err := e.Materialize("spindle", "r", model.Query{Start: 0, End: 10, Step: 1, Function: "count"})
			return err
		})(),
	} {
		if conflictCode(err) != "conflict" {
			t.Fatalf("logical mutation after completion: got %v", err)
		}
	}
	if _, err := e.Compact("spindle"); err != nil {
		t.Fatalf("physical maintenance after completion: %v", err)
	}
	if done.Drain.CompletedSequence == 0 || done.Drain.CompletedSequence <= done.Drain.StartedSequence {
		t.Fatalf("completion sequence: %+v", done.Drain)
	}
}

func TestSetModeDrainingAndWritable(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	view, err := e.SetMode("spindle", model.StateDraining)
	if err != nil || view.Drain.State != model.StateDraining {
		t.Fatalf("mode draining: %+v %v", view.Drain, err)
	}
	view, err = e.SetMode("spindle", model.StateWritable)
	if err != nil || view.Drain.State != model.StateWritable {
		t.Fatalf("mode writable: %+v %v", view.Drain, err)
	}
	if _, err := e.SetMode("spindle", "broken"); conflictCode(err) != "invalid" {
		t.Fatalf("invalid mode: %v", err)
	}
}

func TestDrainIsolationBetweenNamespaces(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	for _, name := range []string{"alpha", "beta"} {
		if _, err := e.Create(testNamespace(name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := e.Write(name, model.WriteRequest{Now: 10, Points: points(1)}); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if _, err := e.StartDrain("alpha"); err != nil {
		t.Fatalf("start drain: %v", err)
	}
	if _, err := e.Write("alpha", model.WriteRequest{Now: 10, Points: points(2)}); conflictCode(err) != "conflict" {
		t.Fatalf("alpha must reject writes, got %v", err)
	}
	if _, err := e.Write("beta", model.WriteRequest{Now: 10, Points: points(2)}); err != nil {
		t.Fatalf("beta must keep accepting writes: %v", err)
	}
	beta, err := e.Namespace("beta")
	if err != nil {
		t.Fatalf("beta status: %v", err)
	}
	if beta.Drain.State != model.StateWritable || beta.Drain.Restarted {
		t.Fatalf("beta lifecycle changed: %+v", beta.Drain)
	}
}

func TestLegacyReadOnlyRecordNormalizes(t *testing.T) {
	var n model.Namespace
	n.ReadOnly = true
	n.Normalize()
	if n.State != model.StateReadOnly {
		t.Fatalf("legacy read_only did not normalize: %+v", n)
	}
}

func TestSetReadOnlyWithInFlightWorkLeavesDraining(t *testing.T) {
	e := openTestEngine(t, t.TempDir())
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	release, err := e.beginLogical("spindle")
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := e.SetReadOnly("spindle", true); conflictCode(err) != "conflict" {
		t.Fatalf("legacy mode must not silently finish a busy drain, got %v", err)
	}
	view, err := e.Namespace("spindle")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if view.Drain.State != model.StateDraining || view.Drain.InFlight != 1 {
		t.Fatalf("namespace must remain draining: %+v", view.Drain)
	}
	release()
	view, err = e.SetReadOnly("spindle", true)
	if err != nil {
		t.Fatalf("retry read-only: %v", err)
	}
	if view.Drain.State != model.StateReadOnly {
		t.Fatalf("retry must complete: %+v", view.Drain)
	}
}

func TestRestartExposesDrainWithoutCompletingIt(t *testing.T) {
	dir := t.TempDir()
	e := openTestEngine(t, dir)
	if _, err := e.Create(testNamespace("spindle")); err != nil {
		t.Fatalf("create: %v", err)
	}
	started, err := e.StartDrain("spindle")
	if err != nil {
		t.Fatalf("start drain: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A restart removes any process-local in-flight work. It must not decide
	// the lifecycle on the operator's behalf.
	reopened := openTestEngine(t, dir)
	view, err := reopened.Namespace("spindle")
	if err != nil {
		t.Fatalf("namespace after restart: %v", err)
	}
	if view.Drain.State != model.StateDraining {
		t.Fatalf("durable state must remain draining: %+v", view.Drain)
	}
	if !view.Drain.Restarted || view.Drain.InFlight != 0 || view.Drain.Maintainable {
		t.Fatalf("restart must be visible without completing the drain: %+v", view.Drain)
	}
	if view.Namespace.LastSequence != started.Namespace.LastSequence {
		t.Fatalf("recovery must not commit a lifecycle change: %+v", view.Namespace)
	}
	if _, err := reopened.Write("spindle", model.WriteRequest{Now: 10, Points: points(1)}); conflictCode(err) != "conflict" {
		t.Fatalf("write after restart must stay rejected, got %v", err)
	}
	completed, err := reopened.CompleteDrain("spindle")
	if err != nil {
		t.Fatalf("complete after restart: %v", err)
	}
	if !completed.Drain.Maintainable || completed.Drain.Restarted {
		t.Fatalf("completion after restart: %+v", completed.Drain)
	}
}
