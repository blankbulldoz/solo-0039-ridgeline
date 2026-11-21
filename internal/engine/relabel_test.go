package engine

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	if _, err := e.Create(model.Namespace{
		Name: "space", RetentionMS: 100000, LatenessMS: 100000, FutureMS: 100000,
		MaxSeries: 100, MaxWrite: 1000, MaxScan: 100000,
	}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return e
}

func write(t *testing.T, e *Engine, points ...model.Observation) {
	t.Helper()
	if _, err := e.Write("space", model.WriteRequest{Now: 100000, Points: points}); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func observe(name string, labels model.Labels, at int64, value float64) model.Observation {
	out := model.Labels{"__name__": name}
	for k, v := range labels {
		out[k] = v
	}
	return model.Observation{Labels: out, Time: at, Value: value}
}

func rename() model.RelabelRequest {
	return model.RelabelRequest{Rename: model.RelabelRename{From: "host", To: "node"}}
}

func allSeries(t *testing.T, e *Engine) []model.Series {
	t.Helper()
	out, err := e.Discover("space", nil)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	return out
}

func hasSeries(series []model.Series, labels model.Labels) bool {
	want := model.SeriesID(labels)
	for _, s := range series {
		if s.ID == want {
			return true
		}
	}
	return false
}

func TestRelabelKeepsAbsentAndMovesEmpty(t *testing.T) {
	e := testEngine(t)
	write(t, e,
		observe("cpu", model.Labels{"host": "a"}, 1000, 1),
		observe("disk", nil, 1000, 9),
		observe("mem", model.Labels{"host": ""}, 1000, 5),
	)
	plan, err := e.PreviewRelabel("space", rename())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if plan.Blocked {
		t.Fatalf("plan should not be blocked: %v", plan.Reasons)
	}
	if plan.Summary.SeriesMissing != 1 || plan.Summary.SeriesEmpty != 1 {
		t.Fatalf("unexpected summary: %+v", plan.Summary)
	}
	if _, err := e.ExecuteRelabel("space", rename()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	series := allSeries(t, e)
	if !hasSeries(series, model.Labels{"__name__": "disk"}) {
		t.Error("series without host must keep its original identity")
	}
	if !hasSeries(series, model.Labels{"__name__": "mem", "node": ""}) {
		t.Error("explicit empty host must become an explicit empty node")
	}
	if !hasSeries(series, model.Labels{"__name__": "cpu", "node": "a"}) {
		t.Error("host must be renamed to node")
	}
	if hasSeries(series, model.Labels{"__name__": "disk", "node": ""}) {
		t.Error("absent label must not become an empty target label")
	}
	if len(series) != 3 {
		t.Fatalf("expected 3 series, got %d", len(series))
	}
}

func TestRelabelRefusesCollisionWithoutPolicy(t *testing.T) {
	e := testEngine(t)
	write(t, e,
		observe("cpu", model.Labels{"host": "a"}, 1000, 1),
		observe("cpu", model.Labels{"node": "a"}, 1000, 40),
	)
	plan, err := e.PreviewRelabel("space", rename())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !plan.Blocked || len(plan.Collisions) != 1 {
		t.Fatalf("expected a reported blocking collision, got %+v", plan)
	}
	if plan.Collisions[0].Chosen != "" || !plan.Collisions[0].Conflicting {
		t.Fatalf("collision detail must show candidates and no choice: %+v", plan.Collisions[0])
	}
	if _, err := e.ExecuteRelabel("space", rename()); err == nil {
		t.Fatal("execute must refuse an unresolved collision")
	}
	// The refusal must not have changed any data.
	series := allSeries(t, e)
	if len(series) != 2 {
		t.Fatalf("refused job changed the catalog: %d series", len(series))
	}
	jobs, err := e.RelabelJobs("space")
	if err != nil {
		t.Fatalf("jobs: %v", err)
	}
	if len(jobs.Jobs) != 1 || jobs.Jobs[0].Completed {
		t.Fatalf("refusal must be recorded as incomplete: %+v", jobs.Jobs)
	}
}

func TestRelabelResolvesCollisions(t *testing.T) {
	cases := []struct {
		policy string
		value  float64
	}{
		{CollisionKeepFirst, 1},
		{CollisionKeepLast, 40},
		{CollisionDedupSame, 40},
	}
	for _, tc := range cases {
		t.Run(tc.policy, func(t *testing.T) {
			e := testEngine(t)
			// Separate commits give the two samples distinct sequences, so
			// keep_first/keep_last have an unambiguous meaning.
			write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
			if tc.policy == CollisionDedupSame {
				write(t, e, observe("cpu", model.Labels{"node": "a"}, 1000, 1))
				tc.value = 1
			} else {
				write(t, e, observe("cpu", model.Labels{"node": "a"}, 1000, 40))
			}
			req := rename()
			req.ValueConflict = tc.policy
			job, err := e.ExecuteRelabel("space", req)
			if err != nil {
				t.Fatalf("execute %s: %v", tc.policy, err)
			}
			if !job.Completed {
				t.Fatalf("job did not complete: %+v", job)
			}
			result, err := e.Range("space", model.Query{Start: 0, End: 10000})
			if err != nil {
				t.Fatalf("range: %v", err)
			}
			if len(result.Series) != 1 || len(result.Series[0].Points) != 1 {
				t.Fatalf("merge must yield one series with one point: %+v", result.Series)
			}
			if got := result.Series[0].Points[0].Value; got != tc.value {
				t.Fatalf("%s chose %v, want %v", tc.policy, got, tc.value)
			}
		})
	}
}

func TestRelabelDedupRejectsDifferingValues(t *testing.T) {
	e := testEngine(t)
	write(t, e,
		observe("cpu", model.Labels{"host": "a"}, 1000, 1),
		observe("cpu", model.Labels{"node": "a"}, 1000, 40),
	)
	req := rename()
	req.ValueConflict = CollisionDedupSame
	if _, err := e.ExecuteRelabel("space", req); err == nil {
		t.Fatal("differing values must not be deduplicated")
	}
}

func TestRelabelLabelConflictPolicies(t *testing.T) {
	cases := []struct {
		policy string
		want   model.Labels
	}{
		{LabelConflictKeepTarget, model.Labels{"__name__": "cpu", "node": "old"}},
		{LabelConflictPreferSource, model.Labels{"__name__": "cpu", "node": "new"}},
	}
	for _, tc := range cases {
		t.Run(tc.policy, func(t *testing.T) {
			e := testEngine(t)
			write(t, e, observe("cpu", model.Labels{"host": "new", "node": "old"}, 1000, 1))
			req := rename()
			req.LabelConflict = tc.policy
			if _, err := e.ExecuteRelabel("space", req); err != nil {
				t.Fatalf("execute: %v", err)
			}
			series := allSeries(t, e)
			if !hasSeries(series, tc.want) {
				t.Fatalf("%s produced %+v, want %+v", tc.policy, series, tc.want)
			}
		})
	}
}

func TestRelabelRepointsDeletionsAndRollups(t *testing.T) {
	e := testEngine(t)
	write(t, e,
		observe("cpu", model.Labels{"host": "a"}, 1000, 1),
		observe("cpu", model.Labels{"host": "a"}, 2000, 2),
	)
	if _, err := e.Delete("space", model.Query{Start: 1500, End: 2500,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "cpu"}}}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	query := model.Query{Start: 0, End: 10000, Step: 5000, GroupBy: []string{"host"},
		Function: "sum", Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "cpu"}}}
	if _, err := e.Materialize("space", "by_host", query); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if _, err := e.ExecuteRelabel("space", rename()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	rollup, err := e.Rollup("space", "by_host")
	if err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if len(rollup.Query.GroupBy) != 1 || rollup.Query.GroupBy[0] != "node" {
		t.Fatalf("rollup group_by not repointed: %+v", rollup.Query.GroupBy)
	}
	if !rollup.Stale {
		t.Fatal("migration must leave the old materialization explicitly stale")
	}
	for _, cell := range rollup.Result.Cells {
		if _, ok := cell.Labels["host"]; ok {
			t.Fatalf("stored cell label still says host: %+v", cell.Labels)
		}
	}
	// The hidden sample stays hidden and the visible one survives.
	result, err := e.Range("space", model.Query{Start: 0, End: 10000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(result.Series) != 1 || len(result.Series[0].Points) != 1 ||
		result.Series[0].Points[0].Time != 1000 {
		t.Fatalf("deletion visibility changed: %+v", result.Series)
	}
	tombstones, err := e.Deletions("space")
	if err != nil {
		t.Fatalf("deletions: %v", err)
	}
	if len(tombstones) != 1 || !tombstones[0].Resolved || len(tombstones[0].Series) != 0 {
		t.Fatalf("tombstone still targets a removed identity: %+v", tombstones)
	}
}

func TestRelabelIdempotentAndPausesWrites(t *testing.T) {
	e := testEngine(t)
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	first, err := e.ExecuteRelabel("space", rename())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	second, err := e.ExecuteRelabel("space", rename())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second.ResultSequence != first.ResultSequence {
		t.Fatalf("replay must not advance: %d != %d", second.ResultSequence, first.ResultSequence)
	}
	// Confirm the pause marker is cleared and ingestion resumes with the new key.
	write(t, e, observe("cpu", model.Labels{"node": "a"}, 2000, 2))
	result, err := e.Range("space", model.Query{Start: 0, End: 10000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(result.Series) != 1 || len(result.Series[0].Points) != 2 {
		t.Fatalf("post-migration ingestion failed: %+v", result.Series)
	}
}

func TestRelabelPreservesCorrectionSequence(t *testing.T) {
	e := testEngine(t)
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	if _, err := e.Seal("space"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 7))
	if _, err := e.ExecuteRelabel("space", rename()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	result, err := e.Range("space", model.Query{Start: 0, End: 10000})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(result.Series) != 1 || len(result.Series[0].Points) != 1 ||
		result.Series[0].Points[0].Value != 7 {
		t.Fatalf("newest correction must survive the migration: %+v", result.Series)
	}
}

func TestOldImageWithoutJobsBucketOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := volume.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = db.Raw.Update(func(tx *bolt.Tx) error {
		ns, err := volume.CreateNamespace(tx, "space")
		if err != nil {
			return err
		}
		// Simulate an image written before this feature existed.
		if err := ns.DeleteBucket(volume.Jobs); err != nil {
			return err
		}
		n := model.Namespace{Name: "space", RetentionMS: 100000, LatenessMS: 100000,
			FutureMS: 100000, MaxSeries: 10, MaxWrite: 100, MaxScan: 1000,
			Floor: model.MinTime, LastSequence: 1}
		if err := volume.PutJSON(ns, volume.Config, n); err != nil {
			return err
		}
		id := model.SeriesID(model.Labels{"__name__": "cpu", "host": "a"})
		if err := volume.PutJSON(ns.Bucket(volume.Catalog), []byte(id),
			model.Series{ID: id, Labels: model.Labels{"__name__": "cpu", "host": "a"}}); err != nil {
			return err
		}
		payload, err := json.Marshal(model.Sample{Time: 1000, Value: 5, Sequence: 1})
		if err != nil {
			return err
		}
		return ns.Bucket(volume.Head).Put(volume.PointKey(id, 1000), payload)
	})
	if err != nil {
		t.Fatalf("prepare old image: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e, err := Open(path)
	if err != nil {
		t.Fatalf("old image must open: %v", err)
	}
	defer e.Close()
	if _, err := e.RelabelJobs("space"); err != nil {
		t.Fatalf("jobs inventory on a migrated image: %v", err)
	}
	if _, err := e.ExecuteRelabel("space", rename()); err != nil {
		t.Fatalf("migration on an old image: %v", err)
	}
	if !hasSeries(allSeries(t, e), model.Labels{"__name__": "cpu", "node": "a"}) {
		t.Fatal("old-image series was not migrated")
	}
}

func TestRecoverMaintenanceClearsInterruptedJob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recover.db")
	e, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := e.Create(model.Namespace{Name: "space", RetentionMS: 100000,
		LatenessMS: 100000, FutureMS: 100000, MaxSeries: 10, MaxWrite: 100, MaxScan: 1000}); err != nil {
		t.Fatalf("create: %v", err)
	}
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	// Simulate dying after intent but before the rewrite.
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		ns := volume.Namespace(tx, "space")
		var n model.Namespace
		if err := volume.GetJSON(ns, volume.Config, &n); err != nil {
			return err
		}
		job := model.RelabelJob{ID: "crashed", State: jobRunning,
			Rename: model.RelabelRename{From: "host", To: "node"}}
		if err := volume.PutJSON(ns.Bucket(volume.Jobs), []byte(job.ID), job); err != nil {
			return err
		}
		n.Maintenance, n.MaintenanceJob = "relabel", job.ID
		return volume.PutJSON(ns, volume.Config, n)
	})
	if err != nil {
		t.Fatalf("simulate crash: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	n, err := reopened.Namespace("space")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if n.Maintenance != "" {
		t.Fatalf("pause marker survived recovery: %q", n.Maintenance)
	}
	job, err := reopened.RelabelJob("space", "crashed")
	if err != nil {
		t.Fatalf("job: %v", err)
	}
	if job.State != jobFailed || job.Completed {
		t.Fatalf("interrupted job must be recorded as failed: %+v", job)
	}
	// Data is untouched and writes are admitted again.
	if !hasSeries(allSeries(t, reopened), model.Labels{"__name__": "cpu", "host": "a"}) {
		t.Fatal("interrupted job must not rewrite data")
	}
	write(t, reopened, observe("cpu", model.Labels{"host": "a"}, 2000, 2))
}

func TestRelabelSecondMigrationGetsDistinctDefaultJob(t *testing.T) {
	e := testEngine(t)
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	first, err := e.ExecuteRelabel("space", rename())
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// New legacy-labelled data arrives between migrations. The default job
	// identifier must not silently reuse the completed record and skip it.
	write(t, e, observe("disk", model.Labels{"host": "b"}, 1000, 2))
	second, err := e.ExecuteRelabel("space", rename())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.ID == first.ID || !second.Completed {
		t.Fatalf("second migration reused job %q: %+v", second.ID, second)
	}
	if !hasSeries(allSeries(t, e), model.Labels{"__name__": "disk", "node": "b"}) {
		t.Fatal("newly written legacy series was not migrated")
	}
}

func TestRelabelRejectsStalePreviewSequence(t *testing.T) {
	e := testEngine(t)
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	plan, err := e.PreviewRelabel("space", rename())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 2000, 2))
	req := rename()
	req.SourceSequence = plan.SourceSequence
	if _, err := e.ExecuteRelabel("space", req); err == nil {
		t.Fatal("execution must reject a preview taken before later writes")
	}
	// The namespace is unchanged and a fresh preview succeeds.
	if !hasSeries(allSeries(t, e), model.Labels{"__name__": "cpu", "host": "a"}) {
		t.Fatal("a rejected stale preview must not migrate")
	}
	req.SourceSequence = 0
	if _, err := e.ExecuteRelabel("space", req); err != nil {
		t.Fatalf("fresh execution: %v", err)
	}
}

func TestRelabelRejectsNameAndSameKeys(t *testing.T) {
	e := testEngine(t)
	write(t, e, observe("cpu", model.Labels{"host": "a"}, 1000, 1))
	bad := []model.RelabelRename{
		{From: "host", To: "host"},
		{From: "__name__", To: "job"},
		{From: "1bad", To: "node"},
	}
	for _, r := range bad {
		if _, err := e.PreviewRelabel("space", model.RelabelRequest{Rename: r}); err == nil {
			t.Fatalf("rename %+v should be rejected", r)
		}
	}
}
