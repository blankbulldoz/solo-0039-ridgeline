package window

import (
	"reflect"
	"testing"

	"ridgeline/internal/model"
)

func sampleSeries() []model.SeriesPoints {
	a := model.Series{ID: "a", Labels: model.Labels{"__name__": "x", "zone": "east"}}
	b := model.Series{ID: "b", Labels: model.Labels{"__name__": "x", "zone": "east"}}
	c := model.Series{ID: "c", Labels: model.Labels{"__name__": "y", "zone": "west"}}
	return []model.SeriesPoints{
		{Series: a, Points: []model.Sample{{Time: -500, Value: 1, Sequence: 1}, {Time: 500, Value: 3, Sequence: 2}}},
		{Series: b, Points: []model.Sample{{Time: -500, Value: 2, Sequence: 1}, {Time: 500, Value: 6, Sequence: 2}}},
		{Series: c, Points: []model.Sample{{Time: 500, Value: 9, Sequence: 1}}},
	}
}

// TestReducerMatchesBatchAggregate ensures the incremental reducer produces the
// same cells as feeding the whole slice at once for every function.
func TestReducerMatchesBatchAggregate(t *testing.T) {
	for _, function := range []string{"count", "sum", "min", "max", "avg", "first", "last", "rate"} {
		q := model.Query{
			Start: -1000, End: 1000, Step: 1000, Origin: 0,
			GroupBy: []string{"zone"}, Function: function,
		}
		batch, err := Aggregate(sampleSeries(), q)
		if err != nil {
			t.Fatalf("%s batch: %v", function, err)
		}
		reducer, err := NewReducer(q)
		if err != nil {
			t.Fatalf("%s reducer: %v", function, err)
		}
		for _, s := range sampleSeries() {
			if err := reducer.Add(s); err != nil {
				t.Fatalf("%s add: %v", function, err)
			}
		}
		streamed, err := reducer.Cells()
		if err != nil {
			t.Fatalf("%s cells: %v", function, err)
		}
		if !reflect.DeepEqual(batch, streamed) {
			t.Fatalf("%s streaming differs from batch:\nbatch    %+v\nstreamed %+v", function, batch, streamed)
		}
	}
}

func TestReducerRateRejectsNegative(t *testing.T) {
	q := model.Query{Start: -1000, End: 1000, Step: 1000, Origin: 0,
		GroupBy: []string{"zone"}, Function: "rate"}
	r, err := NewReducer(q)
	if err != nil {
		t.Fatalf("new reducer: %v", err)
	}
	err = r.Add(model.SeriesPoints{
		Series: model.Series{ID: "a", Labels: model.Labels{"zone": "east"}},
		Points: []model.Sample{{Time: 0, Value: -1}},
	})
	if err == nil {
		t.Fatal("negative counters must be rejected")
	}
}
