package engine

import (
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// openTestEngine builds an engine on a fresh temporary database and registers
// cleanup on the test.
func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "ridgeline.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func testNamespace() model.Namespace {
	return model.Namespace{
		Name:        "probe",
		RetentionMS: 10000,
		LatenessMS:  30000,
		FutureMS:    1000,
		MaxSeries:   100,
		MaxWrite:    2,
		// A two-point scan budget lets the two-point "east" block selected by
		// the rollup read decode exactly, while the namespace-wide retention
		// rebuild fails when it reaches the second two-point series. Policy
		// requires max_scan >= max_write, so the four seed points are ingested
		// in two two-point requests.
		MaxScan: 2,
	}
}

// Four points across two series: the two selected "east" points fit the scan
// budget, while decoding both two-point series forces a namespace-wide rebuild
// past it.
func testPoints() []model.Observation {
	east := model.Labels{"__name__": "pulse_total", "zone": "east"}
	west := model.Labels{"__name__": "pulse_total", "zone": "west"}
	return []model.Observation{
		{Labels: east, Time: 1000, Value: 10},
		{Labels: east, Time: 2000, Value: 14},
		{Labels: west, Time: 2000, Value: 5},
		{Labels: west, Time: 3000, Value: 7},
	}
}

func rollupQuery() model.Query {
	return model.Query{
		Start: 0, End: 10000, Step: 1000, Origin: 0,
		Function: "sum", GroupBy: []string{"zone"},
		Matchers: []model.Matcher{{Label: "zone", Op: "=", Value: "east"}},
	}
}

func eastRange() model.Query {
	return model.Query{
		Start: 0, End: 10000,
		Matchers: []model.Matcher{{Label: "zone", Op: "=", Value: "east"}},
	}
}

// TestMaintenanceBoundaryRollup follows one rollup through:
//  1. a read-only physical maintenance (seal),
//  2. a materialization whose publication must not move the source version,
//  3. a retention maintenance that fails after staging its new floor.
//
// Receipts, reads and the source state after reopening are all asserted.
func TestMaintenanceBoundaryRollup(t *testing.T) {
	e := openTestEngine(t)
	space := "probe"
	if _, err := e.Create(testNamespace()); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	globalAt := func() uint64 {
		var current uint64
		if err := e.DB.Raw.View(func(tx *bolt.Tx) error {
			current = volume.Current(tx)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return current
	}

	// Two logical mutations establish source sequence 3 (creation took 1).
	points := testPoints()
	for i := range points {
		if i%2 != 0 {
			continue
		}
		if _, err := e.Write(space, model.WriteRequest{Now: 10000, Points: points[i : i+2]}); err != nil {
			t.Fatalf("ingest batch %d: %v", i/2, err)
		}
	}
	write, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("load namespace after ingest: %v", err)
	}
	if write.LastSequence != 3 {
		t.Fatalf("namespace source after ingest = %d, want 3", write.LastSequence)
	}
	if globalAt() != 3 {
		t.Fatalf("global sequence after ingest = %d, want 3", globalAt())
	}

	// Read-only admission: logical mutations are rejected, physical ones run.
	if _, err := e.ReadOnly(space, true); err != nil {
		t.Fatalf("enter read-only mode: %v", err)
	}
	if _, err := e.Write(space, model.WriteRequest{
		Now:    10000,
		Points: []model.Observation{{Labels: testPoints()[0].Labels, Time: 4000, Value: 1}},
	}); err == nil {
		t.Fatal("ingest on read-only namespace must be rejected")
	}

	// 1. Read-only maintenance: sealing still runs and publishes a new source.
	sealed, err := e.Seal(space)
	if err != nil {
		t.Fatalf("seal on read-only namespace: %v", err)
	}
	if sealed.Sequence != 5 || sealed.Blocks != 2 || sealed.Points != 4 {
		t.Fatalf("seal receipt = %+v, want sequence 5, 2 blocks, 4 points", sealed)
	}
	if blocks, err := e.Blocks(space); err != nil || len(blocks) != 2 {
		t.Fatalf("block inventory after seal = %d blocks, err %v; want 2", len(blocks), err)
	}
	// Reads inside read-only mode still resolve the sealed physical samples.
	read, err := e.Range(space, eastRange())
	if err != nil {
		t.Fatalf("range on read-only namespace: %v", err)
	}
	visible := 0
	for _, s := range read.Series {
		visible += len(s.Points)
	}
	if visible != 2 {
		t.Fatalf("visible east points after seal = %d, want 2", visible)
	}
	if _, err := e.ReadOnly(space, false); err != nil {
		t.Fatalf("leave read-only mode: %v", err)
	}

	// 2. Materialize with an unchanged source: provenance equals the source
	// version that was read, and publication does not advance that version.
	before, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("load namespace before materialization: %v", err)
	}
	mat, err := e.Materialize(space, "zone_sum", rollupQuery())
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if mat.SourceSequence != before.LastSequence {
		t.Fatalf("materialization source = %d, want namespace source %d",
			mat.SourceSequence, before.LastSequence)
	}
	if len(mat.Result.Cells) != 2 {
		t.Fatalf("materialized cells = %d, want 2 populated east windows", len(mat.Result.Cells))
	}
	after, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("load namespace after materialization: %v", err)
	}
	if after.LastSequence != before.LastSequence {
		t.Fatalf("materialization moved source version %d -> %d",
			before.LastSequence, after.LastSequence)
	}
	// Publication still consumed a database-wide receipt sequence.
	if globalAt() != 7 {
		t.Fatalf("global sequence after materialization = %d, want 7", globalAt())
	}
	view, err := e.Rollup(space, "zone_sum")
	if err != nil {
		t.Fatalf("read rollup: %v", err)
	}
	if view.Stale {
		t.Fatal("rollup materialized at the current source must not be stale")
	}

	// 3. Failing retention: the floor decision is staged before the physical
	// rebuild, and that rebuild exceeds the two-point scan budget.
	preFail, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("load namespace before failed retention: %v", err)
	}
	globalBeforeFailure := globalAt()
	if _, err := e.Retain(space, 11000); err == nil {
		t.Fatal("retention with over-budget rebuild must fail")
	}
	postFail, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("load namespace after failed retention: %v", err)
	}
	if postFail.Floor != preFail.Floor {
		t.Fatalf("failed retention left floor %d, want unchanged %d",
			postFail.Floor, preFail.Floor)
	}
	if postFail.LastSequence != preFail.LastSequence {
		t.Fatalf("failed retention left source version %d, want unchanged %d",
			postFail.LastSequence, preFail.LastSequence)
	}
	if globalAt() != globalBeforeFailure {
		t.Fatalf("failed retention advanced global sequence %d -> %d",
			globalBeforeFailure, globalAt())
	}
	// The pre-failure physical layout survives: blocks remain and rows read.
	if blocks, err := e.Blocks(space); err != nil || len(blocks) != 2 {
		t.Fatalf("blocks after failed retention = %d, err %v; want 2 intact blocks",
			len(blocks), err)
	}
	if _, err := e.Range(space, eastRange()); err != nil {
		t.Fatalf("range after failed retention: %v", err)
	}
	// A rollup created before the failed maintenance still pairs with the same
	// source and therefore remains fresh.
	view, err = e.Rollup(space, "zone_sum")
	if err != nil {
		t.Fatalf("reread rollup after failed retention: %v", err)
	}
	if view.SourceSequence != mat.SourceSequence || view.Stale {
		t.Fatalf("rollup after failed retention = source %d stale %v, want source %d fresh",
			view.SourceSequence, view.Stale, mat.SourceSequence)
	}

	// Reopen the database: provenance and the un-mutated source must survive.
	path := e.DB.Raw.Path()
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedNS, err := reopened.Namespace(space)
	if err != nil {
		t.Fatalf("namespace after reopen: %v", err)
	}
	if reopenedNS.Floor != preFail.Floor || reopenedNS.LastSequence != preFail.LastSequence {
		t.Fatalf("state after reopen = floor %d source %d, want floor %d source %d",
			reopenedNS.Floor, reopenedNS.LastSequence, preFail.Floor, preFail.LastSequence)
	}
	reopenedView, err := reopened.Rollup(space, "zone_sum")
	if err != nil {
		t.Fatalf("rollup after reopen: %v", err)
	}
	if reopenedView.SourceSequence != mat.SourceSequence {
		t.Fatalf("rollup provenance after reopen = %d, want %d",
			reopenedView.SourceSequence, mat.SourceSequence)
	}
	if reopenedView.Stale {
		t.Fatal("rollup must stay fresh after reopen because no source mutation committed")
	}
}

// TestReadOnlyPhysicalMaintenance confirms both physical entry points are
// admitted under read-only mode while logical mutations stay rejected.
func TestReadOnlyPhysicalMaintenance(t *testing.T) {
	e := openTestEngine(t)
	space := "probe"
	ns := testNamespace()
	// This test needs a successful namespace-wide rebuild, so relax the scan
	// budget deliberately tight only for the composite-failure test.
	ns.MaxScan = 1000
	if _, err := e.Create(ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	points := testPoints()
	for i := range points {
		if i%2 != 0 {
			continue
		}
		if _, err := e.Write(space, model.WriteRequest{Now: 10000, Points: points[i : i+2]}); err != nil {
			t.Fatalf("ingest batch %d: %v", i/2, err)
		}
	}
	if _, err := e.ReadOnly(space, true); err != nil {
		t.Fatalf("enter read-only mode: %v", err)
	}
	// Seal first so the namespace-wide compaction exercises block decoding.
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal on read-only namespace: %v", err)
	}
	if _, err := e.Compact(space); err != nil {
		t.Fatalf("compact must run on read-only namespace: %v", err)
	}
	if _, err := e.Materialize(space, "zone_sum", rollupQuery()); err == nil {
		t.Fatal("rollup publication must be rejected on read-only namespace")
	}
	if _, err := e.Delete(space, eastRange()); err == nil {
		t.Fatal("deletion must be rejected on read-only namespace")
	}
	if _, err := e.Retain(space, 11000); err == nil {
		t.Fatal("retention must be rejected on read-only namespace")
	}
}
