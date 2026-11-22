package window_test

import (
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"testing"

	"ridgeline/internal/model"
	"ridgeline/internal/testutil/oracle"
	"ridgeline/internal/window"
)

// counterexample is one discriminating case from testdata/counterexamples.json.
type counterexample struct {
	Name    string
	Comment string
	Query   model.Query
	Series  []struct {
		ID     string            `json:"id"`
		Labels map[string]string `json:"labels"`
		Points []model.Sample    `json:"points"`
	}
	Expect         []expectedCell `json:"expect"`
	PlausibleWrong []struct {
		Rule    string         `json:"rule"`
		Compare string         `json:"compare"`
		Cells   []expectedCell `json:"cells"`
	} `json:"plausible_wrong"`
}

// expectedCell carries a hand-computed value as a rational literal ("6",
// "3/10", "8/3") so expectations stay exact and interpretable.
type expectedCell struct {
	Labels map[string]string `json:"labels"`
	Start  int64             `json:"start"`
	End    int64             `json:"end"`
	Count  int               `json:"count"`
	Value  string            `json:"value"`
}

type counterexampleFile struct {
	Comment string
	Cases   []counterexample `json:"cases"`
}

// compareMode decides how a cell value is matched: "exact" for selections
// and counts, "rational" for sums, averages and rates.
func compareMode(function string) string {
	switch function {
	case "sum", "avg", "rate":
		return "rational"
	default:
		return "exact"
	}
}

// matchCells reports whether got matches the hand-computed cells. Rational
// tolerances come from the independent oracle run over the same input.
func matchCells(got []model.Cell, want []expectedCell, mode string, oracleCells []oracle.Cell) bool {
	if len(got) != len(want) {
		return false
	}
	ordered := make([]expectedCell, len(want))
	copy(ordered, want)
	sort.Slice(ordered, func(i, j int) bool {
		a, _ := json.Marshal(ordered[i].Labels)
		b, _ := json.Marshal(ordered[j].Labels)
		if string(a) != string(b) {
			return string(a) < string(b)
		}
		return ordered[i].Start < ordered[j].Start
	})
	for i, w := range ordered {
		g := got[i]
		gotLabels, _ := json.Marshal(g.Labels)
		wantLabels, _ := json.Marshal(w.Labels)
		if string(gotLabels) != string(wantLabels) || g.Start != w.Start || g.End != w.End || g.Count != w.Count {
			return false
		}
		expectation, ok := new(big.Rat).SetString(w.Value)
		if !ok {
			return false
		}
		switch mode {
		case "exact":
			if !oracle.Exact(g.Value, expectation) {
				return false
			}
		default:
			tolerance := oracleCells[i].Tolerance()
			if !oracle.Close(g.Value, expectation, tolerance) {
				return false
			}
		}
	}
	return true
}

// TestCounterexamples runs the hand-computed catalog. Every case is checked
// three ways: the implementation must match the hand-computed expectation,
// the implementation must match the independent rational oracle, and the
// implementation must not match any plausible-but-wrong output.
func TestCounterexamples(t *testing.T) {
	data, err := os.ReadFile("testdata/counterexamples.json")
	if err != nil {
		t.Fatal(err)
	}
	var catalog counterexampleFile
	if err := json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Cases) == 0 {
		t.Fatal("counterexample catalog is empty")
	}
	for _, tc := range catalog.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			series := make([]model.SeriesPoints, 0, len(tc.Series))
			for _, s := range tc.Series {
				pts := make([]model.Sample, len(s.Points))
				copy(pts, s.Points)
				// The engine always feeds time-sorted points per series.
				sort.Slice(pts, func(a, b int) bool { return pts[a].Time < pts[b].Time })
				series = append(series, model.SeriesPoints{
					Series: model.Series{ID: s.ID, Labels: model.Labels(s.Labels)},
					Points: pts,
				})
			}
			got, err := window.Aggregate(series, tc.Query)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			oracleCells := oracle.Aggregate(series, tc.Query)

			// The hand-computed expectation and the Go oracle must agree
			// with each other and with the implementation.
			if !matchCells(got, tc.Expect, compareMode(tc.Query.Function), oracleCells) {
				t.Errorf("implementation does not match hand-computed expectation\ngot:  %v\nwant: %v", got, tc.Expect)
			}
			oracle.CompareCells(t, got, oracleCells, tc.Query.Function)

			// The data must discriminate: none of the plausible buggy
			// rules may describe the implementation's output.
			for _, wrong := range tc.PlausibleWrong {
				mode := wrong.Compare
				if mode == "" {
					mode = compareMode(tc.Query.Function)
				}
				if matchCells(got, wrong.Cells, mode, oracleCells) {
					t.Errorf("implementation exhibits plausible bug: %s", wrong.Rule)
				}
			}
		})
	}
}
