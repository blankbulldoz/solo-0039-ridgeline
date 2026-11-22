package visibility_test

import (
	"testing"

	visibility "ridgeline/internal/visibility"
)

// These tests pin the oracle itself. If the model changes, they document that
// the change was intentional rather than an accident of a replayed fixture.

func TestModelBatchOverwriteKeepsLastValue(t *testing.T) {
	store := visibility.NewStore()
	mustCreate(t, store)
	labels := visibility.Labels{"__name__": "load"}
	mustApply(t, store, visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{
			{Labels: labels, Time: 5, Value: 1},
			{Labels: labels, Time: 5, Value: 2},
			{Labels: labels, Time: 5, Value: 3},
		}})
	snapshot := mustSnapshot(t, store, -10, 10, nil)
	assertPoints(t, snapshot, labels, []visibility.Point{{Time: 5, Value: 3}})
}

func TestModelDeleteAllowsNewValueButFloorDoesNot(t *testing.T) {
	store := visibility.NewStore()
	mustCreate(t, store)
	labels := visibility.Labels{"__name__": "load"}
	mustApply(t, store, visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{{Labels: labels, Time: 100, Value: 1}}})
	mustApply(t, store, visibility.Operation{Kind: visibility.DeleteKind, Space: "space",
		Start: 50, End: 150})
	assertPoints(t, mustSnapshot(t, store, -10, 1000, nil), labels, nil)

	// Deletion permits a later commit at the same timestamp.
	mustApply(t, store, visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{{Labels: labels, Time: 100, Value: 2}}})
	assertPoints(t, mustSnapshot(t, store, -10, 1000, nil), labels,
		[]visibility.Point{{Time: 100, Value: 2}})

	// Retention floor is absolute: old values may not return at all.
	mustApply(t, store, visibility.Operation{Kind: visibility.RetainKind, Space: "space", Now: 3100})
	if floor, _, _, _ := store.State("space"); floor != 100 {
		t.Fatalf("expected floor 100, got %d", floor)
	}
	assertPoints(t, mustSnapshot(t, store, -10, 1000, nil), labels,
		[]visibility.Point{{Time: 100, Value: 2}})
	err := store.Apply(visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 3100,
		Points: []visibility.Observation{{Labels: labels, Time: 99, Value: 3}}})
	if code := codeOf(t, err); code != visibility.CodeConflict {
		t.Fatalf("expected conflict below floor, got %q", code)
	}

	// A lower retention observation must not lower the floor.
	mustApply(t, store, visibility.Operation{Kind: visibility.RetainKind, Space: "space", Now: 1})
	if floor, _, _, _ := store.State("space"); floor != 100 {
		t.Fatalf("floor moved backwards to %d", floor)
	}
}

func TestModelIdentitySeparatesAbsentAndEmptyLabel(t *testing.T) {
	store := visibility.NewStore()
	mustCreate(t, store)
	absent := visibility.Labels{"__name__": "load"}
	present := visibility.Labels{"__name__": "load", "note": ""}
	if visibility.Identity(absent) == visibility.Identity(present) {
		t.Fatal("absent and present-empty labels must differ in identity")
	}
	matcher := []visibility.Matcher{{Label: "note", Op: "=", Value: ""}}
	if visibility.Identity(absent) == visibility.Identity(present) {
		t.Fatal("identity must not collapse")
	}
	// Both match the absent-as-empty matcher.
	mustApply(t, store, visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{
			{Labels: absent, Time: 1, Value: 1},
			{Labels: present, Time: 1, Value: 2},
		}})
	snapshot := mustSnapshot(t, store, 0, 10, matcher)
	if len(snapshot.Series) != 2 {
		t.Fatalf("expected both identities selected, got %d", len(snapshot.Series))
	}
}

func TestModelRejectedWriteDoesNotChangeVisibility(t *testing.T) {
	store := visibility.NewStore()
	mustCreate(t, store)
	labels := visibility.Labels{"__name__": "load"}
	before := mustSnapshot(t, store, -10, 1000, nil)
	err := store.Apply(visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{
			{Labels: labels, Time: 1, Value: 1},
			{Labels: labels, Time: 10000, Value: 2},
		}})
	if code := codeOf(t, err); code != visibility.CodeConflict {
		t.Fatalf("expected future-skew conflict, got %q", code)
	}
	after := mustSnapshot(t, store, -10, 1000, nil)
	if len(after.Series) != len(before.Series) {
		t.Fatalf("rejected batch changed visibility: %d -> %d series", len(before.Series), len(after.Series))
	}
	if _, _, has, _ := store.State("space"); has {
		t.Fatal("rejected batch must not set a watermark")
	}
}

func mustCreate(t *testing.T, store *visibility.Store) {
	t.Helper()
	if err := store.Create("space", visibility.Policy{
		RetentionMS: 3000, LatenessMS: 9000, FutureMS: 9000,
		MaxSeries: 32, MaxWrite: 64, MaxScan: 10000,
	}); err != nil {
		t.Fatalf("create model namespace: %v", err)
	}
}

func mustApply(t *testing.T, store *visibility.Store, op visibility.Operation) {
	t.Helper()
	if op.Space == "" {
		op.Space = "space"
	}
	if err := store.Apply(op); err != nil {
		t.Fatalf("model apply %s: %v", visibility.Describe(op), err)
	}
}

func mustSnapshot(t *testing.T, store *visibility.Store, start, end int64, matchers []visibility.Matcher) visibility.Snapshot {
	t.Helper()
	snap, err := store.Snapshot("space", start, end, matchers)
	if err != nil {
		t.Fatalf("model snapshot: %v", err)
	}
	return snap
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return ""
	}
	domain, ok := err.(*visibility.ModelError)
	if !ok {
		t.Fatalf("expected model error, got %T: %v", err, err)
	}
	return domain.Code
}

func assertPoints(t *testing.T, snap visibility.Snapshot, labels visibility.Labels, want []visibility.Point) {
	t.Helper()
	key := visibility.Identity(labels)
	var found *visibility.SeriesView
	for i := range snap.Series {
		if snap.Series[i].Key == key {
			found = &snap.Series[i]
		}
	}
	if len(want) == 0 {
		if found != nil && len(found.Points) > 0 {
			t.Fatalf("expected no visible points, got %+v", found.Points)
		}
		return
	}
	if found == nil {
		t.Fatalf("series %v not visible in %+v", labels, snap.Series)
	}
	if len(found.Points) != len(want) {
		t.Fatalf("points = %+v, want %+v", found.Points, want)
	}
	for i := range want {
		if found.Points[i] != want[i] {
			t.Fatalf("points = %+v, want %+v", found.Points, want)
		}
	}
}
