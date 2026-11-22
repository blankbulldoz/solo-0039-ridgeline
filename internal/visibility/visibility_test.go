package visibility_test

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	visibility "ridgeline/internal/visibility"
)

// TestDivergenceArtifactRoundTrips guarantees that a saved failure can be
// loaded and replayed, which is what lets CI keep a minimised reproducer.
func TestDivergenceArtifactRoundTrips(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)
	data, err := json.Marshal(struct {
		Ops []visibility.Operation `json:"ops"`
	}{Ops: ops})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		Ops []visibility.Operation `json:"ops"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Ops) != len(ops) {
		t.Fatalf("op count changed: %d -> %d", len(ops), len(decoded.Ops))
	}
	for i := range ops {
		if decoded.Ops[i].Kind != ops[i].Kind || decoded.Ops[i].Now != ops[i].Now ||
			decoded.Ops[i].Start != ops[i].Start || decoded.Ops[i].End != ops[i].End ||
			len(decoded.Ops[i].Points) != len(ops[i].Points) {
			t.Fatalf("op %d changed across JSON: %+v -> %+v", i, ops[i], decoded.Ops[i])
		}
	}
}

// TestFloorNeverDecreasesUnderRepeatedRetention uses the model directly to pin
// the monotonicity invariant across many advances and re-observations.
func TestFloorNeverDecreasesUnderRepeatedRetention(t *testing.T) {
	store := visibility.NewStore()
	if err := store.Create("space", visibility.Policy{
		RetentionMS: 1000, LatenessMS: 1000, FutureMS: 1000,
		MaxSeries: 4, MaxWrite: 4, MaxScan: 100,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	floor := int64(visibility.MinTime)
	for _, now := range []int64{5000, 4000, 6000, 6000, 4999, 9000} {
		if err := store.Apply(visibility.Operation{Kind: visibility.RetainKind, Space: "space", Now: now}); err != nil {
			t.Fatalf("retain %d: %v", now, err)
		}
		next, _, _, err := store.State("space")
		if err != nil {
			t.Fatalf("state: %v", err)
		}
		if next < floor {
			t.Fatalf("floor decreased from %d to %d after retention at %d", floor, next, now)
		}
		if next != maxInt64(floor, now-1000) {
			t.Fatalf("floor after retention at %d = %d, want %d", now, next, maxInt64(floor, now-1000))
		}
		floor = next
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestDeleteSelectsSeriesAtCommitTime verifies the deletion stores matched
// identities at commit: a series created later in the same range is not
// retroactively hidden.
func TestDeleteSelectsSeriesAtCommitTime(t *testing.T) {
	store := visibility.NewStore()
	if err := store.Create("space", visibility.Policy{
		RetentionMS: 1000, LatenessMS: 1000, FutureMS: 1000,
		MaxSeries: 8, MaxWrite: 8, MaxScan: 100,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	early := visibility.Labels{"__name__": "load", "rack": "a"}
	late := visibility.Labels{"__name__": "load", "rack": "b"}
	if err := store.Apply(visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{{Labels: early, Time: 10, Value: 1}}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := store.Apply(visibility.Operation{Kind: visibility.DeleteKind, Space: "space",
		Start: 0, End: 100, Matchers: []visibility.Matcher{{Label: "rack", Op: "=", Value: "a"}}}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Apply(visibility.Operation{Kind: visibility.WriteKind, Space: "space", Now: 0,
		Points: []visibility.Observation{{Labels: late, Time: 10, Value: 2}}}); err != nil {
		t.Fatalf("write late series: %v", err)
	}
	snap, err := store.Snapshot("space", 0, 100, nil)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Series) != 1 || snap.Series[0].Labels["rack"] != "b" {
		t.Fatalf("expected only the later series to be visible, got %+v", snap.Series)
	}
}

// TestFailureArtifactsAreWrittenAndReplayable proves the failure-capture path
// itself: a divergence produces a full and a minimised sequence on disk, the
// minimised sequence loads, and replaying it still diverges. The artifacts are
// removed at the end so a passing suite leaves no state behind.
func TestFailureArtifactsAreWrittenAndReplayable(t *testing.T) {
	f := visibility.DefaultFixture()
	ops := visibility.EdgeSequence(f)
	// The predicate is intentionally simple: it stands in for a discovered
	// defect so the artifact pipeline is exercised without touching production.
	// Candidate diverges only while it still contains a deletion, so the
	// shrinker has a real predicate to satisfy and cannot collapse to one op.
	replay := func(candidate []visibility.Operation) (*visibility.Divergence, error) {
		for i, op := range candidate {
			if op.Kind == visibility.DeleteKind {
				return &visibility.Divergence{Index: i, Op: op, Engine: visibility.Effect{Accepted: true},
					Model: visibility.Effect{Accepted: true, Code: "forced"}}, nil
			}
		}
		return nil, nil
	}
	persistFailure(t, "artifact-smoke", ops, &visibility.Divergence{Index: 0, Op: ops[0]}, replay)
	dir := artifactDir(t)
	t.Cleanup(func() {
		for _, name := range []string{"artifact-smoke.full.json", "artifact-smoke.min.json"} {
			_ = os.Remove(filepath.Join(dir, name))
		}
	})
	full, err := os.ReadFile(filepath.Join(dir, "artifact-smoke.full.json"))
	if err != nil {
		t.Fatalf("full artifact missing: %v", err)
	}
	loadedFull, err := visibility.UnmarshalSequence(full)
	if err != nil || len(loadedFull) != len(ops) {
		t.Fatalf("full artifact does not round-trip: %v (len %d)", err, len(loadedFull))
	}
	minimal, err := os.ReadFile(filepath.Join(dir, "artifact-smoke.min.json"))
	if err != nil {
		t.Fatalf("minimal artifact missing: %v", err)
	}
	loadedMin, err := visibility.UnmarshalSequence(minimal)
	if err != nil {
		t.Fatalf("minimal artifact does not parse: %v", err)
	}
	if len(loadedMin) >= len(ops) {
		t.Fatalf("minimal artifact was not reduced: %d of %d", len(loadedMin), len(ops))
	}
	if div, err := replay(loadedMin); err != nil || div == nil {
		t.Fatalf("minimal artifact no longer replays: err=%v div=%v", err, div)
	}
}

// TestGeneratedSequencesReachInterestingState guards the property tests against
// vacuity: sequences must mostly be accepted, must contain same-batch
// duplicate timestamps, and must produce at least one sample that a deletion
// hid and a later commit made visible again.
func TestGeneratedSequencesReachInterestingState(t *testing.T) {
	f := visibility.DefaultFixture()
	accepted, total, reposted, sameBatchDuplicates := 0, 0, 0, 0
	for seed := int64(1); seed <= 40; seed++ {
		ops := visibility.RandomSequence(f, seed, 45)
		store := visibility.NewStore()
		if err := store.Create("space", f.Policy); err != nil {
			t.Fatalf("create: %v", err)
		}
		previous, err := store.Snapshot("space", f.Start, f.End, nil)
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		for _, op := range ops {
			total++
			if op.Kind == visibility.WriteKind {
				seen := map[int64]bool{}
				for _, p := range op.Points {
					if seen[p.Time] {
						sameBatchDuplicates++
					}
					seen[p.Time] = true
				}
			}
			if err := store.Apply(op); err != nil {
				continue
			}
			accepted++
			next, err := store.Snapshot("space", f.Start, f.End, nil)
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			if op.Kind == visibility.WriteKind {
				reposted += countReappeared(previous, next)
			}
			previous = next
		}
	}
	if rate := float64(accepted) / float64(total); rate < 0.7 {
		t.Fatalf("only %.0f%% of generated operations were admissible (%d/%d); the property test would not reach deep state",
			rate*100, accepted, total)
	}
	if sameBatchDuplicates == 0 {
		t.Fatal("generator never produced a same-batch duplicate timestamp")
	}
	if reposted == 0 {
		t.Fatal("no sample was hidden by a deletion and later re-reported")
	}
}

func countReappeared(before, after visibility.Snapshot) int {
	index := func(s visibility.Snapshot) map[string]map[int64]bool {
		out := map[string]map[int64]bool{}
		for _, series := range s.Series {
			out[series.Key] = map[int64]bool{}
			for _, p := range series.Points {
				out[series.Key][p.Time] = true
			}
		}
		return out
	}
	b, a := index(before), index(after)
	count := 0
	for key, times := range a {
		for time := range times {
			if !b[key][time] {
				count++
			}
		}
	}
	return count
}

// TestOracleDoesNotImportProductionPackages enforces the central rule of this
// package: the model may not derive expectations from the implementation under
// test. It inspects the non-test sources of the oracle package itself.
func TestOracleDoesNotImportProductionPackages(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	entries, err := os.ReadDir(wd)
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(wd, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imported := range f.Imports {
			path := strings.Trim(imported.Path.Value, `"`)
			if strings.HasPrefix(path, "ridgeline/") {
				t.Fatalf("%s imports production package %q; the oracle must stay independent", name, path)
			}
		}
	}
}
