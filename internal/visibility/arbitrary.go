package visibility

import (
	"fmt"
	"math/rand"
)

// Series describes one synthetic series identity. Extra is a label that only
// some series carry, so absent and present-empty labels can be exercised.
type Series struct {
	Name     string
	Rack     string
	Extra    string
	HasExtra bool
}

func (s Series) Labels() Labels {
	labels := Labels{"__name__": s.Name}
	if s.Rack != "" {
		labels["rack"] = s.Rack
	}
	if s.HasExtra {
		labels["note"] = s.Extra
	}
	return labels
}

// Fixture is a deterministic, contract-valid binding used by both property
// and directed tests. Times cluster around zero and around the retention
// boundary; identities repeat across series names.
type Fixture struct {
	Space    string
	Policy   Policy
	Start    int64
	End      int64
	Now      int64
	Times    []int64
	Series   []Series
	Matchers []Matcher
}

// DefaultFixture builds a fixture whose timestamps cross the epoch and sit on
// both sides of a retention floor reached by retainNow.
func DefaultFixture() Fixture {
	return Fixture{
		Space: "space",
		Policy: Policy{
			RetentionMS: 3000, LatenessMS: 9000, FutureMS: 9000,
			MaxSeries: 32, MaxWrite: 64, MaxScan: 10000,
		},
		Start: -4000,
		End:   4000,
		Now:   0,
		Times: []int64{
			-3999, -3000, -2999, -1500, -1, 0, 1, 500, 999,
			1000, 1001, 1500, 2000, 2999, 3000, 3999,
		},
		Series: []Series{
			{Name: "load", Rack: "a"},
			{Name: "load", Rack: "b"},
			{Name: "load", Rack: "a", Extra: "", HasExtra: true},
			{Name: "pulse_total", Rack: "a"},
			{Name: "pulse_total", Rack: "a", Extra: "", HasExtra: true},
		},
		Matchers: nil,
	}
}

// bind fills the fixture namespace into generated operations so they can be
// applied to the model directly, not only through the Recorder.
func (f Fixture) bind(ops []Operation) []Operation {
	for i := range ops {
		if ops[i].Space == "" {
			ops[i].Space = f.Space
		}
	}
	return ops
}

// RandomOperation draws one operation from a seeded source. The generator
// keeps every emitted operation admissible: it never asks for a point below
// the model floor or beyond future/lateness bounds that the fixture's own
// history would violate with high probability. Rejected-operation coverage is
// provided by the directed tests instead, because the generator's job is to
// reach deep accepted state.
func (f Fixture) RandomOperation(rng *rand.Rand) Operation {
	switch rng.Intn(10) {
	case 0, 1, 2, 3, 4, 5, 6:
		count := 1 + rng.Intn(4)
		points := make([]Observation, 0, count)
		now := f.Now + int64(rng.Intn(2001))
		for i := 0; i < count; i++ {
			s := f.Series[rng.Intn(len(f.Series))]
			t := f.Times[rng.Intn(len(f.Times))]
			points = append(points, Observation{Labels: s.Labels(), Time: t, Value: float64(rng.Intn(20)) - 5})
		}
		return Operation{Kind: WriteKind, Space: f.Space, Now: now, Points: points}
	case 7:
		start := f.Times[rng.Intn(len(f.Times))]
		end := start + int64(1+rng.Intn(1500))
		var matchers []Matcher
		switch rng.Intn(3) {
		case 0:
			matchers = nil
		case 1:
			matchers = []Matcher{{Label: "rack", Op: "=", Value: f.Series[rng.Intn(len(f.Series))].Rack}}
		default:
			matchers = []Matcher{{Label: "note", Op: "=", Value: ""}}
		}
		return Operation{Kind: DeleteKind, Space: f.Space, Start: start, End: end, Matchers: matchers}
	case 8:
		return Operation{Kind: SealKind, Space: f.Space}
	default:
		return Operation{Kind: CompactKind, Space: f.Space}
	}
}

// RandomSequence builds a sequence of write/delete/seal/compact operations.
// Retention is intentionally excluded here; the property tests add a single
// monotonic retention step at a random point in a separate pass so the floor
// transition stays reproducible.
func RandomSequence(f Fixture, seed int64, length int) []Operation {
	rng := rand.New(rand.NewSource(seed))
	ops := make([]Operation, 0, length+2)
	ops = append(ops, seedWrites(f)...)
	for i := 0; i < length; i++ {
		ops = append(ops, f.RandomOperation(rng))
	}
	return f.bind(ops)
}

// seedWrites places all points for one series and fixture timestamp in a
// single batch, so the same commit sequence covers every identity and the
// random tail is free to correct, delete and physically rewrite them.
func seedWrites(f Fixture) []Operation {
	var ops []Operation
	for i, t := range f.Times {
		points := make([]Observation, 0, len(f.Series))
		for _, s := range f.Series {
			points = append(points, Observation{Labels: s.Labels(), Time: t, Value: float64(i + 1)})
		}
		ops = append(ops, Operation{Kind: WriteKind, Space: f.Space, Now: t, Points: points})
	}
	return ops
}

// EdgeSequence is a deterministic directed sequence covering the combinations
// named in the task: batch-internal overwrite, same-batch duplication, delete
// followed by a later re-report, monotonic retention, seal and compaction.
func EdgeSequence(f Fixture) []Operation {
	load := Series{Name: "load", Rack: "a"}.Labels()
	loadB := Series{Name: "load", Rack: "b"}.Labels()
	noteEmpty := Series{Name: "load", Rack: "a", Extra: "", HasExtra: true}.Labels()
	noteValue := Series{Name: "load", Rack: "a", Extra: "x", HasExtra: true}.Labels()

	return f.bind([]Operation{
		// Within one request the last occurrence wins.
		{Kind: WriteKind, Now: 0, Points: []Observation{
			{Labels: load, Time: 100, Value: 1},
			{Labels: load, Time: 100, Value: 2},
			{Labels: load, Time: 100, Value: 3},
		}},
		// Distinct identities with absent vs present-empty labels.
		{Kind: WriteKind, Now: 0, Points: []Observation{
			{Labels: load, Time: 200, Value: 4},
			{Labels: noteEmpty, Time: 200, Value: 5},
			{Labels: noteValue, Time: 200, Value: 6},
			{Labels: loadB, Time: 200, Value: 7},
		}},
		// Correction of one timestamp only; neighbours untouched.
		{Kind: WriteKind, Now: 0, Points: []Observation{
			{Labels: load, Time: 200, Value: 8},
		}},
		// Delete hides committed samples in a range for matched series.
		{Kind: DeleteKind, Start: 150, End: 250, Matchers: []Matcher{{Label: "rack", Op: "=", Value: "a"}}},
		// New value inside the deleted range is admitted again.
		{Kind: WriteKind, Now: 0, Points: []Observation{
			{Labels: load, Time: 200, Value: 9},
		}},
		// Sealing must not change visibility (deleted series remains hidden,
		// re-reported value remains visible).
		{Kind: SealKind},
		// Correction after sealing is visible too.
		{Kind: WriteKind, Now: 0, Points: []Observation{
			{Labels: load, Time: 200, Value: 10},
		}},
		{Kind: CompactKind},
		// Retention floor advances irreversibly; point at the exact boundary
		// is retained, one millisecond below is not.
		{Kind: RetainKind, Now: 3000},
		// A point exactly at the new floor (0) is admitted.
		{Kind: WriteKind, Now: 3000, Points: []Observation{
			{Labels: load, Time: 0, Value: 11},
		}},
		{Kind: SealKind},
		{Kind: CompactKind},
		// A point below the floor is permanently refused even though a
		// deletion at that timestamp would have allowed a re-report.
		{Kind: WriteKind, Now: 3000, Points: []Observation{
			{Labels: load, Time: -1, Value: 12},
		}},
		// Retention can only move forward; a lower Now is a no-op for the floor.
		{Kind: RetainKind, Now: 1000},
		{Kind: CompactKind},
	})
}

// Describe renders an operation compactly for test failure messages.
func Describe(op Operation) string {
	switch op.Kind {
	case WriteKind:
		return fmt.Sprintf("write(now=%d, points=%d)", op.Now, len(op.Points))
	case DeleteKind:
		return fmt.Sprintf("delete([%d,%d), matchers=%d)", op.Start, op.End, len(op.Matchers))
	case RetainKind:
		return fmt.Sprintf("retain(now=%d)", op.Now)
	default:
		return op.Kind
	}
}
