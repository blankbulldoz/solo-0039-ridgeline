package visibility_test

import (
	"os"
	"path/filepath"
	"testing"

	visibility "ridgeline/internal/visibility"
)

// faultyAdapter injects a visibility bug into the engine side by translating a
// public mutation or by post-processing a read result. It exists to prove the
// harness detects a real behavioral difference, shrinks the sequence and can
// replay the saved artifact. Production code is never modified.
type faultyAdapter struct {
	*engineAdapter
	mode    string
	deletes []recordedDelete
}

const (
	faultHideReposted  = "hide-reposted"   // reads ignore commit sequence for deletions
	faultDropRangeEdge = "drop-range-edge" // range result loses its last point
)

type recordedDelete struct {
	start, end int64
	matchers   []visibility.Matcher
}

func (f *faultyAdapter) Apply(op visibility.Operation) (string, error) {
	code, err := f.engineAdapter.Apply(op)
	if err == nil && code == "" && op.Kind == visibility.DeleteKind {
		f.deletes = append(f.deletes, recordedDelete{
			start: op.Start, end: op.End, matchers: op.Matchers,
		})
	}
	return code, err
}

// Range simulates a reader that treats every deletion as covering all commits,
// so a value re-reported after the deletion is hidden too. This is exactly the
// behavior the model distinguishes from a retention floor.
func (f *faultyAdapter) Range(space string, start, end int64, matchers []visibility.Matcher) (visibility.Snapshot, error) {
	snap, err := f.engineAdapter.Range(space, start, end, matchers)
	if err != nil {
		return snap, err
	}
	switch f.mode {
	case faultDropRangeEdge:
		for i := range snap.Series {
			if points := snap.Series[i].Points; len(points) > 0 {
				snap.Series[i].Points = points[:len(points)-1]
			}
		}
	case faultHideReposted:
		for i := range snap.Series {
			if !matchesLabels(snap.Series[i].Labels, matchers) {
				continue
			}
			kept := snap.Series[i].Points[:0]
			for _, p := range snap.Series[i].Points {
				if !f.deletedAt(snap.Series[i].Labels, p.Time) {
					kept = append(kept, p)
				}
			}
			snap.Series[i].Points = kept
		}
	}
	return snap, nil
}

func (f *faultyAdapter) deletedAt(labels visibility.Labels, t int64) bool {
	for _, d := range f.deletes {
		if t >= d.start && t < d.end && matchesLabels(labels, d.matchers) {
			return true
		}
	}
	return false
}

func matchesLabels(labels visibility.Labels, matchers []visibility.Matcher) bool {
	for _, m := range matchers {
		value := labels[m.Label]
		switch m.Op {
		case "=":
			if value != m.Value {
				return false
			}
		case "!=":
			if value == m.Value {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// faultyFactory builds an independent faulty engine per replay. Each call must
// use a fresh database file; Shrink invokes it many times.
func faultyFactory(policy visibility.Policy, mode string) func() (visibility.Adapter, func() error, error) {
	return func() (visibility.Adapter, func() error, error) {
		dir, err := os.MkdirTemp("", "ridgeline-fault-")
		if err != nil {
			return nil, nil, err
		}
		base, err := openAdapterErr(filepath.Join(dir, "faulty.db"))
		if err != nil {
			os.RemoveAll(dir)
			return nil, nil, err
		}
		if err := base.Create("space", policy); err != nil {
			base.Close()
			os.RemoveAll(dir)
			return nil, nil, err
		}
		a := &faultyAdapter{engineAdapter: base, mode: mode}
		return a, func() error {
			err := a.Close()
			os.RemoveAll(dir)
			return err
		}, nil
	}
}

// TestHarnessDetectsAndShrinksSequenceFault drives a deliberately faulty
// adapter through a long sequence, requires a divergence, shrinks it and
// verifies the saved minimal sequence still reproduces the failure.
func TestHarnessDetectsAndShrinksSequenceFault(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)

	replay := visibility.DependencyReplayer("space", f.Policy, f.Start, f.End, f.Matchers,
		faultyFactory(f.Policy, faultHideReposted))
	div, err := replay(ops)
	if err != nil {
		t.Fatalf("fault replay: %v", err)
	}
	if div == nil {
		t.Fatal("faulty adapter was not detected")
	}
	shrunk, minimal, err := visibility.Shrink(ops, replay, 0)
	if err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if len(shrunk) == 0 || len(shrunk) >= len(ops) {
		t.Fatalf("shrink did not reduce sequence: %d -> %d", len(ops), len(shrunk))
	}
	if minimal == nil {
		t.Fatal("shrunk sequence no longer diverges")
	}
	// The minimal reproducer must retain the delete/re-report interaction: a
	// deletion followed by a write to the same range.
	var sawDelete, sawWriteAfterDelete bool
	for _, op := range shrunk {
		if op.Kind == visibility.DeleteKind {
			sawDelete = true
		} else if sawDelete && op.Kind == visibility.WriteKind && len(op.Points) > 0 {
			sawWriteAfterDelete = true
		}
	}
	if !sawDelete || !sawWriteAfterDelete {
		t.Fatalf("minimal reproducer lost the delete/re-report interaction: %+v", shrinkKinds(shrunk))
	}
	if len(shrunk) > 12 {
		t.Fatalf("shrunk sequence is not small: %d ops: %+v", len(shrunk), shrinkKinds(shrunk))
	}

	saved, err := visibility.MarshalSequence(shrunk)
	if err != nil {
		t.Fatalf("marshal minimal: %v", err)
	}
	loaded, err := visibility.UnmarshalSequence(saved)
	if err != nil {
		t.Fatalf("unmarshal minimal: %v", err)
	}
	if again, err := replay(loaded); err != nil || again == nil {
		t.Fatalf("saved artifact did not reproduce: err=%v divergence=%v", err, again)
	}
}

// TestHarnessDetectsRangeBoundaryFault verifies a reader-side defect is also
// caught; the model is not allowed to use the engine to produce expectations.
func TestHarnessDetectsRangeBoundaryFault(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)
	replay := visibility.DependencyReplayer("space", f.Policy, f.Start, f.End, f.Matchers,
		faultyFactory(f.Policy, faultDropRangeEdge))
	div, err := replay(ops)
	if err != nil {
		t.Fatalf("fault replay: %v", err)
	}
	if div == nil {
		t.Fatal("range boundary fault was not detected")
	}
	if div.Index != 0 || div.Engine.Code != div.Model.Code {
		t.Fatalf("unexpected first divergence shape: %+v", div)
	}
	if div.Engine.Accepted != div.Model.Accepted {
		t.Fatalf("range fault should show up as a snapshot mismatch, not acceptance: %+v", div)
	}
}

func shrinkKinds(ops []visibility.Operation) []string {
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		out = append(out, visibility.Describe(op))
	}
	return out
}

// TestExtremeTimeBoundary places samples exactly at the supported limits and
// checks that they survive seal, compaction, corrections and full-interval
// queries. Lateness is widened so the watermark does not legally reject the
// boundary corrections.
func TestExtremeTimeBoundary(t *testing.T) {
	// Lateness is the widest legal value. Boundary corrections therefore have
	// to happen while the watermark is still close enough to be admissible:
	// correcting MinTime after the watermark reached MaxTime is refusable by
	// policy, not a visibility question, so the sequence avoids it.
	policy := visibility.Policy{
		RetentionMS: visibility.MaxTime, LatenessMS: visibility.MaxTime, FutureMS: visibility.MaxTime,
		MaxSeries: 8, MaxWrite: 16, MaxScan: 1000,
	}
	labels := visibility.Labels{"__name__": "edge"}
	ops := []visibility.Operation{
		{Kind: visibility.WriteKind, Now: 0, Points: []visibility.Observation{
			{Labels: labels, Time: visibility.MinTime, Value: 1},
			{Labels: labels, Time: -1, Value: 2},
			{Labels: labels, Time: 0, Value: 3},
		}},
		{Kind: visibility.WriteKind, Now: 0, Points: []visibility.Observation{
			{Labels: labels, Time: visibility.MinTime, Value: 5},
		}},
		{Kind: visibility.SealKind},
		{Kind: visibility.CompactKind},
		{Kind: visibility.WriteKind, Now: visibility.MaxTime, Points: []visibility.Observation{
			{Labels: labels, Time: visibility.MaxTime, Value: 4},
		}},
		{Kind: visibility.WriteKind, Now: visibility.MaxTime, Points: []visibility.Observation{
			{Labels: labels, Time: visibility.MaxTime, Value: 6},
		}},
		{Kind: visibility.SealKind},
	}
	dir := t.TempDir()
	a := newAdapter(t, filepath.Join(dir, "edge.db"), "space", policy)
	defer a.Close()
	store := newModelStore(t, "space", policy)
	rec := visibility.NewRecorder(a, store, "space", visibility.MinTime, visibility.MaxTime+1, nil)
	for i, op := range ops {
		if d, err := rec.Step(op); err != nil {
			t.Fatalf("operation %d %s: %v", i, visibility.Describe(op), err)
		} else if d != nil {
			t.Fatalf("%v", d)
		}
	}
	snap, err := a.Range("space", visibility.MinTime, visibility.MaxTime+1, nil)
	if err != nil {
		t.Fatalf("full-interval range: %v", err)
	}
	if len(snap.Series) != 1 || len(snap.Series[0].Points) != 4 {
		t.Fatalf("expected the four boundary points, got %+v", snap.Series)
	}
	want := map[int64]float64{
		visibility.MinTime: 5, -1: 2, 0: 3, visibility.MaxTime: 6,
	}
	for _, p := range snap.Series[0].Points {
		if want[p.Time] != p.Value {
			t.Fatalf("point %d = %v, want %v (all points %+v)", p.Time, p.Value, want[p.Time], snap.Series[0].Points)
		}
	}
}

// TestReadOnlyModeIsReplayed covers read-only admission: point writes and
// deletions are refused, seal and compact remain allowed, and toggling back
// restores writes. The mode flip is an operation in the replayed sequence.
func TestReadOnlyModeIsReplayed(t *testing.T) {
	f := visibility.DefaultFixture()
	labels := visibility.Labels{"__name__": "load"}
	ops := []visibility.Operation{
		{Kind: visibility.WriteKind, Space: "space", Now: 0,
			Points: []visibility.Observation{{Labels: labels, Time: 10, Value: 1}}},
		{Kind: visibility.ModeKind, Space: "space", ReadOnly: true},
		{Kind: visibility.WriteKind, Space: "space", Now: 0,
			Points: []visibility.Observation{{Labels: labels, Time: 20, Value: 2}}},
		{Kind: visibility.DeleteKind, Space: "space", Start: 0, End: 100},
		{Kind: visibility.SealKind, Space: "space"},
		{Kind: visibility.CompactKind, Space: "space"},
		{Kind: visibility.ModeKind, Space: "space", ReadOnly: false},
		{Kind: visibility.WriteKind, Space: "space", Now: 0,
			Points: []visibility.Observation{{Labels: labels, Time: 20, Value: 3}}},
	}
	dir := t.TempDir()
	a := newAdapter(t, filepath.Join(dir, "readonly.db"), "space", f.Policy)
	defer a.Close()
	store := newModelStore(t, "space", f.Policy)
	rec := visibility.NewRecorder(a, store, "space", f.Start, f.End, nil)
	for i, op := range ops {
		d, err := rec.Step(op)
		if err != nil {
			t.Fatalf("operation %d %s: %v", i, visibility.Describe(op), err)
		}
		if d != nil {
			t.Fatalf("%v", d)
		}
	}
}
