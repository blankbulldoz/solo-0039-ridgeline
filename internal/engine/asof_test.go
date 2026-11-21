package engine

import (
	"testing"

	"ridgeline/internal/model"
)

func sample(t int64, v float64, seq uint64) model.Sample {
	return model.Sample{Time: t, Value: v, Sequence: seq}
}

func series(id string) model.Series {
	return model.Series{ID: id, Labels: model.Labels{"__name__": "temperature", "node": "n1"}}
}

func TestPickNeighborDirections(t *testing.T) {
	rights := []candidate{
		{series: series("s1"), points: []model.Sample{
			sample(100, 1, 1), sample(300, 3, 1), sample(500, 5, 1),
		}},
	}
	cases := []struct {
		dir       string
		left      int64
		wantTime  int64
		wantDist  int64
		wantMatch bool
	}{
		{model.AsOfBackward, 250, 100, 150, true},
		{model.AsOfBackward, 50, 0, 0, false},
		{model.AsOfForward, 250, 300, 50, true},
		{model.AsOfForward, 600, 0, 0, false},
		{model.AsOfNearest, 250, 300, 50, true},
		{model.AsOfNearest, 400, 300, 100, true},
	}
	for _, tc := range cases {
		q := model.AsOfQuery{Direction: tc.dir}
		got, ok := pickNeighbor(q, rights, tc.left)
		if ok != tc.wantMatch {
			t.Fatalf("dir %s left %d: match=%v want %v", tc.dir, tc.left, ok, tc.wantMatch)
		}
		if ok && (got.point.Time != tc.wantTime || got.distance != tc.wantDist) {
			t.Fatalf("dir %s left %d: got (%d,%d) want (%d,%d)",
				tc.dir, tc.left, got.point.Time, got.distance, tc.wantTime, tc.wantDist)
		}
	}
}

func TestPickNeighborMaxDistanceIsInclusive(t *testing.T) {
	rights := []candidate{
		{series: series("s1"), points: []model.Sample{sample(100, 1, 1)}},
	}
	q := model.AsOfQuery{Direction: model.AsOfNearest, MaxDistanceMS: 100}
	if _, ok := pickNeighbor(q, rights, 200); !ok {
		t.Fatal("boundary distance must be accepted")
	}
	q.MaxDistanceMS = 99
	if _, ok := pickNeighbor(q, rights, 200); ok {
		t.Fatal("distance beyond bound must be rejected")
	}
	q.MaxDistanceMS = 0
	if _, ok := pickNeighbor(q, rights, 200); !ok {
		t.Fatal("zero bound means unbounded")
	}
}

func TestPickNeighborCrossSeriesTieUsesSeriesID(t *testing.T) {
	// s2 sits before the left point and s1 after it, both exactly 50ms away.
	rights := []candidate{
		{series: series("s1"), points: []model.Sample{sample(150, 1, 1)}},
		{series: series("s2"), points: []model.Sample{sample(50, 2, 1)}},
	}
	q := model.AsOfQuery{Direction: model.AsOfNearest}
	got, ok := pickNeighbor(q, rights, 100)
	if !ok || got.seriesID != "s1" || got.distance != 50 {
		t.Fatalf("equal-distance tie must resolve to lower series ID, got %+v ok=%v", got, ok)
	}
	// Backward cannot see the later series even with an earlier ID ordering.
	back, ok := pickNeighbor(model.AsOfQuery{Direction: model.AsOfBackward}, rights, 100)
	if !ok || back.seriesID != "s2" {
		t.Fatalf("backward must only choose earlier points, got %+v", back)
	}
}

func TestPickNeighborSameSeriesTieUsesEarlierTime(t *testing.T) {
	rights := []candidate{
		{series: series("s1"), points: []model.Sample{
			sample(100, 1, 1), sample(300, 3, 1),
		}},
	}
	got, ok := pickNeighbor(model.AsOfQuery{Direction: model.AsOfNearest}, rights, 200)
	if !ok || got.point.Time != 100 {
		t.Fatalf("same-series equal distance must choose earlier time, got %+v", got)
	}
}

func TestRightBounds(t *testing.T) {
	q := model.AsOfQuery{Start: 1000, End: 2000, MaxDistanceMS: 300}
	start, end := rightBounds(q)
	if start != 700 || end != 2300 {
		t.Fatalf("widened bounds = (%d,%d), want (700,2300)", start, end)
	}
	clamp := model.AsOfQuery{Start: model.MinTime, End: model.MaxTime + 1, MaxDistanceMS: 10}
	start, end = rightBounds(clamp)
	if start != model.MinTime || end != model.MaxTime+1 {
		t.Fatalf("bounds must clamp to supported range, got (%d,%d)", start, end)
	}
	noBound := model.AsOfQuery{Start: 1000, End: 2000}
	start, end = rightBounds(noBound)
	if start != 1000 || end != 2000 {
		t.Fatalf("zero distance must not widen bounds, got (%d,%d)", start, end)
	}
}

func TestValidateAsOf(t *testing.T) {
	good := model.AsOfQuery{
		Start: 0, End: 10, PairLabel: "node", Direction: model.AsOfNearest,
		LeftMatchers:  []model.Matcher{{Label: "__name__", Op: "=", Value: "power"}},
		RightMatchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "temperature"}},
	}
	if err := model.ValidateAsOf(good); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	mutate := func(fn func(*model.AsOfQuery)) model.AsOfQuery {
		q := good
		fn(&q)
		return q
	}
	for name, q := range map[string]model.AsOfQuery{
		"direction":  mutate(func(q *model.AsOfQuery) { q.Direction = "sideways" }),
		"pair label": mutate(func(q *model.AsOfQuery) { q.PairLabel = "bad label" }),
		"distance":   mutate(func(q *model.AsOfQuery) { q.MaxDistanceMS = -1 }),
		"interval":   mutate(func(q *model.AsOfQuery) { q.Start, q.End = 10, 10 }),
		"limit":      mutate(func(q *model.AsOfQuery) { q.Limit = -1 }),
	} {
		if err := model.ValidateAsOf(q); err == nil {
			t.Fatalf("%s mutation must be rejected", name)
		}
	}
}
