package engine_test

import (
	"encoding/json"
	"math/big"
	"path/filepath"
	"sort"
	"testing"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
	"ridgeline/internal/testutil/oracle"
)

// This test pins a storage-level contract: window aggregation statistics
// must not depend on the physical layout of the samples. The same logical
// dataset is built four ways — entirely in the mutable head, split across
// sealed blocks, compacted into one block per series, and interleaved with
// corrections, a deletion and re-admissions — and every aggregate query
// must return bit-identical cells for every layout.
//
// Expected values come from the independent rational oracle fed by a small
// visibility model written directly from PROJECT_SPEC semantics ("repeated
// timestamps use the most recently committed value"; "a deletion hides
// samples committed at or before the deletion"). Nothing here is derived
// from window.Aggregate, window.Bucket or engine scan output.

var (
	labelsA = model.Labels{"__name__": "load_ratio", "zone": "east", "host": "a"}
	labelsB = model.Labels{"__name__": "load_ratio", "zone": "east", "host": "b"}
	labelsC = model.Labels{"__name__": "load_ratio", "zone": "west", "host": "c"}
	labelsD = model.Labels{"__name__": "requests_total", "zone": "east", "host": "d"}
)

func obs(labels model.Labels, t int64, v float64) model.Observation {
	return model.Observation{Labels: labels, Time: t, Value: v}
}

// layoutOp is one physical operation in a layout script.
type layoutOp struct {
	kind   string // "write", "seal", "compact", "delete"
	points []model.Observation
	start  int64
	end    int64
	match  model.Labels
}

// visibility is an independent model of which samples are logically
// visible, built from the specification rather than from engine code.
// Seal and compact are physical maintenance and change no visibility.
type visibility struct {
	seq     uint64
	series  map[string]model.Labels
	samples map[string]map[int64]model.Sample
	tombs   []model.Tombstone
}

func newVisibility() *visibility {
	return &visibility{
		series:  map[string]model.Labels{},
		samples: map[string]map[int64]model.Sample{},
	}
}

func (v *visibility) apply(op layoutOp) {
	switch op.kind {
	case "write":
		v.seq++
		for _, p := range op.points {
			id := model.SeriesID(p.Labels)
			v.series[id] = p.Labels
			m := v.samples[id]
			if m == nil {
				m = map[int64]model.Sample{}
				v.samples[id] = m
			}
			// Most recently committed value wins at a repeated timestamp.
			m[p.Time] = model.Sample{Time: p.Time, Value: p.Value, Sequence: v.seq}
		}
	case "delete":
		v.seq++
		tomb := model.Tombstone{Start: op.start, End: op.end, Sequence: v.seq, Series: []string{}}
		for id, labels := range v.series {
			matched := true
			for name, want := range op.match {
				if labels[name] != want {
					matched = false
					break
				}
			}
			if matched {
				tomb.Series = append(tomb.Series, id)
			}
		}
		v.tombs = append(v.tombs, tomb)
	}
}

// visible returns the logically visible samples: a tombstone hides samples
// of matched series in [start, end) committed at or before its sequence.
func (v *visibility) visible() []model.SeriesPoints {
	out := []model.SeriesPoints{}
	for id, byTime := range v.samples {
		pts := []model.Sample{}
		for _, p := range byTime {
			hidden := false
			for _, tomb := range v.tombs {
				if p.Sequence > tomb.Sequence || p.Time < tomb.Start || p.Time >= tomb.End {
					continue
				}
				for _, sid := range tomb.Series {
					if sid == id {
						hidden = true
						break
					}
				}
				if hidden {
					break
				}
			}
			if !hidden {
				pts = append(pts, p)
			}
		}
		if len(pts) == 0 {
			continue
		}
		sort.Slice(pts, func(a, b int) bool { return pts[a].Time < pts[b].Time })
		out = append(out, model.SeriesPoints{
			Series: model.Series{ID: id, Labels: v.series[id]},
			Points: pts,
		})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Series.ID < out[b].Series.ID })
	return out
}

// rawPoint is the layout-independent projection of a raw query result:
// sequences and physical statistics are layout details, identity and
// values are not.
type rawPoint struct {
	Series string  `json:"series"`
	Time   int64   `json:"time"`
	Value  float64 `json:"value"`
}

func flatten(series []model.SeriesPoints) []rawPoint {
	out := []rawPoint{}
	for _, s := range series {
		for _, p := range s.Points {
			out = append(out, rawPoint{Series: s.Series.ID, Time: p.Time, Value: p.Value})
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Series != out[b].Series {
			return out[a].Series < out[b].Series
		}
		return out[a].Time < out[b].Time
	})
	return out
}

// matched restricts series to those selected by equality matchers, so the
// oracle sees exactly the source set the engine's selector admits.
func matched(series []model.SeriesPoints, matchers []model.Matcher) []model.SeriesPoints {
	out := []model.SeriesPoints{}
	for _, s := range series {
		ok := true
		for _, m := range matchers {
			if m.Op != "=" || s.Series.Labels[m.Label] != m.Value {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, s)
		}
	}
	return out
}

func layouts() map[string][]layoutOp {
	loadRatio := model.Labels{"__name__": "load_ratio"}
	headOnly := []layoutOp{
		{kind: "write", points: []model.Observation{
			obs(labelsA, -12000, 1.5), obs(labelsA, -9500, 2.25), obs(labelsA, -4000, 0.1),
			obs(labelsA, -500, 3.5), obs(labelsA, 1200, 4.25),
			obs(labelsB, -12000, 7), obs(labelsB, -3000, 0.1), obs(labelsB, -500, 1.25),
			obs(labelsB, 1200, 2.5), obs(labelsB, 6500, 5),
			obs(labelsC, -11000, 4), obs(labelsC, 500, 2.5),
			obs(labelsD, -10000, 0), obs(labelsD, -9000, 4), obs(labelsD, -8000, 2), obs(labelsD, -7000, 9),
		}},
		{kind: "write", points: []model.Observation{
			obs(labelsA, 8800, 0.1), obs(labelsA, 14000, 2),
			obs(labelsB, 14000, 0.1),
			obs(labelsC, 9500, 1.5),
			obs(labelsD, 2000, 10), obs(labelsD, 3000, 11), obs(labelsD, 11000, 12),
		}},
	}
	sealed := []layoutOp{
		// History: values that will be corrected, deleted or re-admitted.
		{kind: "write", points: []model.Observation{
			obs(labelsA, -4000, 9.9),  // corrected to 0.1 below
			obs(labelsA, -6000, 99),   // deleted below, never re-admitted
			obs(labelsA, -9500, 2.25), // deleted below, then re-admitted
			obs(labelsB, 6500, 4),     // corrected to 5 below
			obs(labelsD, 3000, 100),   // corrected to 11 below
			obs(labelsA, -12000, 1.5), obs(labelsB, -12000, 7), obs(labelsC, -11000, 4),
			obs(labelsD, -10000, 0), obs(labelsD, -9000, 4),
		}},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsA, -4000, 0.1), // correction lands in the head over a sealed block
			obs(labelsD, -8000, 2), obs(labelsD, -7000, 9),
			obs(labelsB, -3000, 0.1), obs(labelsC, 500, 2.5),
			obs(labelsA, -500, 3.5), obs(labelsB, -500, 1.25),
		}},
		{kind: "delete", start: -10000, end: -5000, match: loadRatio},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsA, -9500, 2.25), // re-admitted after the deletion
			obs(labelsD, 3000, 11),    // correction of the sealed 100
			obs(labelsB, 6500, 5),     // correction of the sealed 4
			obs(labelsA, 1200, 4.25), obs(labelsB, 1200, 2.5),
			obs(labelsD, 2000, 10),
		}},
		{kind: "write", points: []model.Observation{
			obs(labelsA, 8800, 0.1), obs(labelsA, 14000, 2),
			obs(labelsB, 14000, 0.1), obs(labelsC, 9500, 1.5),
			obs(labelsD, 11000, 12),
		}},
	}
	compacted := append(append([]layoutOp{}, sealed...), layoutOp{kind: "compact"})
	interleaved := []layoutOp{
		{kind: "write", points: []model.Observation{
			obs(labelsD, -10000, 0), obs(labelsD, -9000, 4),
			obs(labelsA, -12000, 1.5), obs(labelsA, -6000, 99),
		}},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsA, -9500, 2.25), obs(labelsB, -12000, 7),
			obs(labelsC, -11000, 4), obs(labelsD, -8000, 2),
		}},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsA, -4000, 9.9), obs(labelsB, -3000, 0.1),
			obs(labelsD, -7000, 9), obs(labelsC, 500, 2.5),
		}},
		{kind: "delete", start: -10000, end: -5000, match: loadRatio},
		{kind: "write", points: []model.Observation{
			obs(labelsA, -9500, 2.25), obs(labelsA, -4000, 0.1),
			obs(labelsA, -500, 3.5), obs(labelsB, -500, 1.25),
		}},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsA, 1200, 4.25), obs(labelsB, 1200, 2.5),
			obs(labelsB, 6500, 4), obs(labelsD, 2000, 10), obs(labelsD, 3000, 100),
		}},
		{kind: "seal"},
		{kind: "write", points: []model.Observation{
			obs(labelsB, 6500, 5), obs(labelsD, 3000, 11),
			obs(labelsA, 8800, 0.1), obs(labelsC, 9500, 1.5),
		}},
		{kind: "write", points: []model.Observation{
			obs(labelsA, 14000, 2), obs(labelsB, 14000, 0.1), obs(labelsD, 11000, 12),
		}},
	}
	return map[string][]layoutOp{
		"head-only":   headOnly,
		"sealed":      sealed,
		"compacted":   compacted,
		"interleaved": interleaved,
	}
}

func aggregateQueries() []model.Query {
	loadMatcher := []model.Matcher{{Label: "__name__", Op: "=", Value: "load_ratio"}}
	counterMatcher := []model.Matcher{{Label: "__name__", Op: "=", Value: "requests_total"}}
	queries := []model.Query{}
	for _, function := range []string{"count", "sum", "avg", "min", "max", "first", "last"} {
		queries = append(queries, model.Query{
			Start: -20000, End: 20000, Step: 1000, Origin: 7,
			GroupBy: []string{"zone"}, Function: function, Matchers: loadMatcher,
		})
	}
	queries = append(queries,
		model.Query{
			Start: -20000, End: 20000, Step: 5000, Origin: 0,
			GroupBy: []string{"zone"}, Function: "rate", Matchers: counterMatcher,
		},
		// Ungrouped average over a negative origin.
		model.Query{
			Start: -20000, End: 20000, Step: 3000, Origin: -2,
			Function: "avg", Matchers: loadMatcher,
		},
	)
	return queries
}

func TestAggregateInvariantAcrossPhysicalLayouts(t *testing.T) {
	queries := aggregateQueries()
	var referenceRaw string
	var referenceCells []string
	for _, layoutName := range []string{"head-only", "sealed", "compacted", "interleaved"} {
		layoutName := layoutName
		ops := layouts()[layoutName]
		t.Run(layoutName, func(t *testing.T) {
			e, err := engine.Open(filepath.Join(t.TempDir(), "ridge.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { e.Close() })
			if _, err := e.Create(model.Namespace{
				Name: "layout", RetentionMS: model.MaxTime, LatenessMS: model.MaxTime,
				FutureMS: model.MaxTime, MaxSeries: 100, MaxWrite: 5000, MaxScan: 2000000,
			}); err != nil {
				t.Fatal(err)
			}
			sim := newVisibility()
			for i, op := range ops {
				sim.apply(op)
				var err error
				switch op.kind {
				case "write":
					_, err = e.Write("layout", model.WriteRequest{Now: 0, Points: op.points})
				case "seal":
					_, err = e.Seal("layout")
				case "compact":
					_, err = e.Compact("layout")
				case "delete":
					matchers := []model.Matcher{}
					for name, value := range op.match {
						matchers = append(matchers, model.Matcher{Label: name, Op: "=", Value: value})
					}
					_, err = e.Delete("layout", model.Query{Start: op.start, End: op.end, Matchers: matchers})
				}
				if err != nil {
					t.Fatalf("op %d (%s): %v", i, op.kind, err)
				}
			}

			// Stage 1: the engine's visible raw points must equal the
			// independent visibility model, for this layout.
			raw, err := e.Range("layout", model.Query{Start: -20000, End: 20000})
			if err != nil {
				t.Fatal(err)
			}
			gotRaw, _ := json.Marshal(flatten(raw.Series))
			wantRaw, _ := json.Marshal(flatten(sim.visible()))
			if string(gotRaw) != string(wantRaw) {
				t.Fatalf("visible raw points differ from visibility model\ngot:  %s\nwant: %s", gotRaw, wantRaw)
			}
			// Sanity that the fixture is non-vacuous: 24 distinct
			// (series, time) pairs were written, one is hidden by the
			// deletion, so 23 remain.
			if got := len(flatten(raw.Series)); got != 23 {
				t.Fatalf("expected 23 visible raw points, got %d", got)
			}

			// Stage 2: every aggregate query must match the rational
			// oracle computed from the visibility model, restricted to
			// the series the query's equality matchers select.
			visible := sim.visible()
			cellsJSON := make([]string, len(queries))
			for qi, q := range queries {
				result, err := e.Aggregate("layout", q)
				if err != nil {
					t.Fatalf("query %d (%s): %v", qi, q.Function, err)
				}
				oracle.CompareCells(t, result.Cells, oracle.Aggregate(matched(visible, q.Matchers), q), q.Function)
				data, _ := json.Marshal(result.Cells)
				cellsJSON[qi] = string(data)
			}

			// Stage 3: bit-identical statistics across physical layouts.
			// Accumulation order is deterministic (series sorted by ID,
			// points sorted by time), so layout changes must not even
			// perturb floating-point summation order.
			if referenceRaw == "" {
				referenceRaw = string(gotRaw)
				referenceCells = cellsJSON
				return
			}
			if string(gotRaw) != referenceRaw {
				t.Errorf("raw visibility differs across layouts\ngot:  %s\nwant: %s", gotRaw, referenceRaw)
			}
			for qi := range queries {
				if cellsJSON[qi] != referenceCells[qi] {
					t.Errorf("query %d (%s): cells differ across layouts\ngot:  %s\nwant: %s",
						qi, queries[qi].Function, cellsJSON[qi], referenceCells[qi])
				}
			}
		})
	}
}

// TestLayoutHandComputedAnchors pins two hand-computed cells through the
// full engine, as an anchor that does not depend on the Go oracle.
func TestLayoutHandComputedAnchors(t *testing.T) {
	e, err := engine.Open(filepath.Join(t.TempDir(), "ridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	if _, err := e.Create(model.Namespace{
		Name: "layout", RetentionMS: model.MaxTime, LatenessMS: model.MaxTime,
		FutureMS: model.MaxTime, MaxSeries: 100, MaxWrite: 5000, MaxScan: 2000000,
	}); err != nil {
		t.Fatal(err)
	}
	for _, op := range layouts()["interleaved"] {
		switch op.kind {
		case "write":
			if _, err := e.Write("layout", model.WriteRequest{Now: 0, Points: op.points}); err != nil {
				t.Fatal(err)
			}
		case "seal":
			if _, err := e.Seal("layout"); err != nil {
				t.Fatal(err)
			}
		case "delete":
			if _, err := e.Delete("layout", model.Query{
				Start: op.start, End: op.end,
				Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: op.match["__name__"]}},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Average of zone=east in window [-993,7) with origin=7 step=1000:
	// samples 3.5 (A@-500) and 1.25 (B@-500) → 4.75/2 = 2.375 exactly.
	avg, err := e.Aggregate("layout", model.Query{
		Start: -20000, End: 20000, Step: 1000, Origin: 7,
		GroupBy: []string{"zone"}, Function: "avg",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "load_ratio"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range avg.Cells {
		if c.Labels["zone"] == "east" && c.Start == -993 {
			found = true
			if c.Value != 2.375 || c.Count != 2 {
				t.Errorf("east [-993,7): got avg=%v count=%d, hand-computed avg=2.375 count=2", c.Value, c.Count)
			}
		}
	}
	if !found {
		t.Error("east [-993,7) avg cell missing")
	}

	// Reset-aware rate of requests_total in window [-10000,-5000):
	// counter 0,4,2,9 → increase 4+2+7=13 over 3s → 13/3 per second.
	rate, err := e.Aggregate("layout", model.Query{
		Start: -20000, End: 20000, Step: 5000, Origin: 0,
		GroupBy: []string{"zone"}, Function: "rate",
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "requests_total"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, c := range rate.Cells {
		if c.Labels["zone"] == "east" && c.Start == -10000 {
			found = true
			want := big.NewRat(13, 3)
			if !oracle.Close(c.Value, want, oracle.FloatTolerance(4, new(big.Rat).SetFloat64(13.0/3.0))) {
				t.Errorf("east [-10000,-5000): got rate=%v, hand-computed 13/3", c.Value)
			}
			if c.Count != 4 {
				t.Errorf("east [-10000,-5000): got count=%d, hand-computed 4", c.Count)
			}
		}
	}
	if !found {
		t.Error("east [-10000,-5000) rate cell missing")
	}
}
