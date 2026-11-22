package visibility_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	visibility "ridgeline/internal/visibility"
)

// artifactDir is where failing sequences are preserved. It is inside the
// repository so a failed run leaves the exact reproducer behind.
func artifactDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := filepath.Join(wd, "testdata", "failures")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("prepare artifact directory: %v", err)
	}
	return dir
}

func newModelStore(t *testing.T, space string, policy visibility.Policy) *visibility.Store {
	t.Helper()
	store := visibility.NewStore()
	if err := store.Create(space, policy); err != nil {
		t.Fatalf("create model space: %v", err)
	}
	return store
}

func newAdapter(t *testing.T, path string, space string, policy visibility.Policy) *engineAdapter {
	t.Helper()
	a := openAdapter(t, path)
	if err := a.Create(space, policy); err != nil {
		a.Close()
		t.Fatalf("create engine space: %v", err)
	}
	return a
}

// replaySequence runs ops through the engine and the independent model and
// returns the first divergence. If it diverges, the full and shrunk sequences
// are written under testdata/failures so they can be re-run directly.
func replaySequence(t *testing.T, name string, f visibility.Fixture, ops []visibility.Operation) {
	t.Helper()
	space := "space"
	start, end := f.Start, f.End
	makeReplay := func() visibility.Replayer {
		return visibility.DependencyReplayer(space, f.Policy, start, end, f.Matchers,
			freshAdapterFactory(f.Policy))
	}
	div, err := makeReplay()(ops)
	if err != nil {
		t.Fatalf("%s: replay error: %v", name, err)
	}
	if div == nil {
		return
	}
	persistFailure(t, name, ops, div, makeReplay())
	t.Fatalf("%s: %v", name, div)
}

func persistFailure(t *testing.T, name string, ops []visibility.Operation, div *visibility.Divergence, replay visibility.Replayer) {
	t.Helper()
	dir := artifactDir(t)
	full, err := visibility.MarshalSequence(ops)
	if err != nil {
		t.Fatalf("marshal failing sequence: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".full.json"), full, 0o644); err != nil {
		t.Fatalf("save failing sequence: %v", err)
	}
	shrunk, minimal, err := visibility.Shrink(ops, replay, 0)
	if err != nil {
		t.Logf("shrink failed: %v", err)
		return
	}
	saved, err := visibility.MarshalSequence(shrunk)
	if err != nil {
		t.Fatalf("marshal shrunk sequence: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".min.json"), saved, 0o644); err != nil {
		t.Fatalf("save shrunk sequence: %v", err)
	}
	t.Logf("divergence saved to %s (full) and %s (minimal, %d of %d ops); minimal divergence: %v",
		filepath.Join(dir, name+".full.json"), filepath.Join(dir, name+".min.json"),
		len(shrunk), len(ops), minimal)
}

// TestDirectedVisibilityCombinations walks the named combinations one
// operation at a time, checking the observable snapshot after every accepted
// mutation against the model.
func TestDirectedVisibilityCombinations(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)

	dir := t.TempDir()
	a := newAdapter(t, filepath.Join(dir, "directed.db"), "space", f.Policy)
	defer a.Close()
	store := newModelStore(t, "space", f.Policy)
	rec := visibility.NewRecorder(a, store, "space", f.Start, f.End, f.Matchers)
	for i, op := range ops {
		d, err := rec.Step(op)
		if err != nil {
			t.Fatalf("operation %d %s: %v", i, visibility.Describe(op), err)
		}
		if d != nil {
			persistFailure(t, "directed", ops[:i+1], d,
				visibility.DependencyReplayer("space", f.Policy, f.Start, f.End, f.Matchers,
					freshAdapterFactory(f.Policy)))
			t.Fatalf("%v", d)
		}
	}
}

// TestRandomVisibilitySequences is the property test: seeded random sequences
// of writes, same-batch duplicates, corrections, deletes, seal and compact.
func TestRandomVisibilitySequences(t *testing.T) {
	f := visibility.DefaultFixture()
	for seed := int64(1); seed <= 40; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			ops := visibility.RandomSequence(f, seed, 45)
			replaySequence(t, fmt.Sprintf("random-%02d", seed), f, ops)
		})
	}
}

// TestRandomRetentionAndCompaction inserts a randomly placed monotonic
// retention advance and verifies that the floor transition is reproduced.
func TestRandomRetentionAndCompaction(t *testing.T) {
	f := visibility.DefaultFixture()
	rng := rand.New(rand.NewSource(99))
	for seed := int64(1); seed <= 20; seed++ {
		seed := seed
		ops := visibility.RandomSequence(f, seed+1000, 35)
		insert := rng.Intn(len(ops))
		retain := visibility.Operation{Kind: visibility.RetainKind, Now: f.Now + int64(rng.Intn(3001))}
		ops = append(ops[:insert], append([]visibility.Operation{retain}, ops[insert:]...)...)
		// Follow retention with physical rewrites, which must preserve the
		// same visible set.
		ops = append(ops, visibility.Operation{Kind: visibility.SealKind}, visibility.Operation{Kind: visibility.CompactKind})
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			replaySequence(t, fmt.Sprintf("retention-%02d", seed), f, ops)
		})
	}
}

// TestReopenAndImageRestorePreserveVisibility applies a sequence, rejects any
// drift against the model, then checks both recovery paths: reopening the same
// database file and opening an exported image as an independent database. Both
// must keep the model's conclusion and the persisted floor/watermark.
func TestReopenAndImageRestorePreserveVisibility(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)
	// Add a random tail so recovery is checked against a non-trivial mix of
	// head points, sealed blocks, tombstones and a compacted namespace.
	ops = append(ops, visibility.RandomSequence(f, 7, 25)...)
	ops = append(ops, visibility.Operation{Kind: visibility.RetainKind, Space: "space", Now: 3500})
	ops = append(ops, visibility.Operation{Kind: visibility.CompactKind, Space: "space"})

	dir := t.TempDir()
	path := filepath.Join(dir, "source.db")
	source := newAdapter(t, path, "space", f.Policy)
	store := newModelStore(t, "space", f.Policy)
	rec := visibility.NewRecorder(source, store, "space", f.Start, f.End, f.Matchers)
	for i, op := range ops {
		d, err := rec.Step(op)
		if err != nil {
			t.Fatalf("operation %d %s: %v", i, visibility.Describe(op), err)
		}
		if d != nil {
			persistFailure(t, "recovery", ops[:i+1], d,
				visibility.DependencyReplayer("space", f.Policy, f.Start, f.End, f.Matchers,
					freshAdapterFactory(f.Policy)))
			t.Fatalf("%v", d)
		}
	}
	floorBefore, watermarkBefore, hasBefore, err := source.State("space")
	if err != nil {
		t.Fatalf("state before close: %v", err)
	}
	imagePath := filepath.Join(dir, "restored.db")
	imageSequence := imageToFile(t, source, imagePath)
	if err := source.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	checkRecovered := func(label, dbPath string) {
		t.Helper()
		recovered := openAdapter(t, dbPath)
		defer recovered.Close()
		floor, watermark, has, err := recovered.State("space")
		if err != nil {
			t.Fatalf("%s state: %v", label, err)
		}
		if floor != floorBefore || watermark != watermarkBefore || has != hasBefore {
			t.Fatalf("%s state changed: floor %d->%d watermark %d->%d has %v->%v",
				label, floorBefore, floor, watermarkBefore, watermark, hasBefore, has)
		}
		if floor != mustModelFloor(t, store, label) {
			t.Fatalf("%s floor disagrees with model", label)
		}
		compareSnapshot(t, recovered, store, f, label)
		// A write below the persisted floor must still be refused.
		code, err := recovered.Apply(visibility.Operation{Kind: visibility.WriteKind,
			Space: "space", Now: 5000,
			Points: []visibility.Observation{{Labels: visibility.Labels{"__name__": "load"}, Time: 0, Value: 99}}})
		if err != nil {
			t.Fatalf("%s post-recovery write: %v", label, err)
		}
		if code != visibility.CodeConflict {
			t.Fatalf("%s: expected persisted floor to reject time 0, got %q", label, code)
		}
	}
	checkRecovered("reopen", path)
	checkRecovered("image", imagePath)
	if imageSequence == 0 {
		t.Fatal("image advertised sequence 0 after mutations")
	}
}

func mustModelFloor(t *testing.T, store *visibility.Store, label string) int64 {
	t.Helper()
	floor, _, _, err := store.State("space")
	if err != nil {
		t.Fatalf("%s model state: %v", label, err)
	}
	return floor
}

// freshAdapterFactory opens a new, empty temporary database and namespace.
func freshAdapterFactory(policy visibility.Policy) func() (visibility.Adapter, func() error, error) {
	return func() (visibility.Adapter, func() error, error) {
		dir, err := os.MkdirTemp("", "ridgeline-replay-")
		if err != nil {
			return nil, nil, err
		}
		a, err := openAdapterErr(filepath.Join(dir, "replay.db"))
		if err != nil {
			os.RemoveAll(dir)
			return nil, nil, err
		}
		if err := a.Create("space", policy); err != nil {
			a.Close()
			os.RemoveAll(dir)
			return nil, nil, err
		}
		return a, func() error { err := a.Close(); os.RemoveAll(dir); return err }, nil
	}
}

// compareSnapshot checks an engine adapter against the model for the fixture
// interval. It never computes expected values through the engine scan path.
func compareSnapshot(t *testing.T, a *engineAdapter, store *visibility.Store, f visibility.Fixture, label string) {
	t.Helper()
	want, err := store.Snapshot("space", f.Start, f.End, nil)
	if err != nil {
		t.Fatalf("%s: model snapshot: %v", label, err)
	}
	got, err := a.Range("space", f.Start, f.End, nil)
	if err != nil {
		t.Fatalf("%s: engine range: %v", label, err)
	}
	normalizedWant := normalizeSnapshot(want, a)
	if !equalSnapshots(got, normalizedWant) {
		t.Fatalf("%s snapshot mismatch:\nengine=%s\nmodel =%s", label, render(got), render(normalizedWant))
	}
}

func normalizeSnapshot(s visibility.Snapshot, a *engineAdapter) visibility.Snapshot {
	out := visibility.Snapshot{Floor: s.Floor, Series: make([]visibility.SeriesView, 0, len(s.Series))}
	for _, sv := range s.Series {
		out.Series = append(out.Series, visibility.SeriesView{
			Key: a.Identity(sv.Labels), Labels: sv.Labels, Points: sv.Points,
		})
	}
	sortViews(out.Series)
	return out
}

func sortViews(series []visibility.SeriesView) {
	for i := 1; i < len(series); i++ {
		for j := i; j > 0 && series[j].Key < series[j-1].Key; j-- {
			series[j], series[j-1] = series[j-1], series[j]
		}
	}
}

func equalSnapshots(a, b visibility.Snapshot) bool {
	if a.Floor != b.Floor || len(a.Series) != len(b.Series) {
		return false
	}
	for i := range a.Series {
		x, y := a.Series[i], b.Series[i]
		if x.Key != y.Key || len(x.Points) != len(y.Points) {
			return false
		}
		for j := range x.Points {
			if x.Points[j] != y.Points[j] {
				return false
			}
		}
	}
	return true
}

func render(s visibility.Snapshot) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "floor=%d", s.Floor)
	for _, sv := range s.Series {
		fmt.Fprintf(&buf, "\n  %s %v", sv.Key, sv.Points)
	}
	return buf.String()
}
