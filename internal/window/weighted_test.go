package window

import (
	"math"
	"testing"

	"ridgeline/internal/model"
)

func series(id string, points ...model.Sample) model.SeriesPoints {
	return model.SeriesPoints{Series: model.Series{ID: id, Labels: model.Labels{"__name__": "load", "node": id}}, Points: points}
}

func baseQuery(function string) model.Query {
	return model.Query{
		Start: 0, End: 10000, Step: 10000, Origin: 0,
		Function: function, MaxAgeMS: 2000,
	}
}

func approx(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func cellByStart(cells []model.Cell, start int64) *model.Cell {
	for i := range cells {
		if cells[i].Start == start {
			return &cells[i]
		}
	}
	return nil
}

func TestTimeWeightedHoldsUntilNextObservation(t *testing.T) {
	// Sparse samples: value 4 for 0..4000, then 6 for 4000..10000, but each
	// hold is capped at 2000ms of known time.
	s := series("a",
		model.Sample{Time: 0, Value: 4},
		model.Sample{Time: 4000, Value: 6},
	)
	q := baseQuery("twavg")
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 1 {
		t.Fatalf("want 1 cell, got %d", len(res.Cells))
	}
	c := res.Cells[0]
	// Known intervals: [0,2000) value 4 and [4000,6000) value 6.
	if c.CoveredMS != 4000 {
		t.Fatalf("covered = %d, want 4000", c.CoveredMS)
	}
	want := (4*2000 + 6*2000) / 4000.0
	if !approx(c.Value, want) {
		t.Fatalf("twavg = %v, want %v", c.Value, want)
	}
}

func TestIntegralIsValueTimesMilliseconds(t *testing.T) {
	s := series("a",
		model.Sample{Time: 0, Value: 4},
		model.Sample{Time: 4000, Value: 6},
	)
	q := baseQuery("integral")
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	c := res.Cells[0]
	want := 4*2000 + 6*2000.0
	if !approx(c.Value, want) {
		t.Fatalf("integral = %v, want %v", c.Value, want)
	}
	if c.CoveredMS != 4000 {
		t.Fatalf("covered = %d, want 4000", c.CoveredMS)
	}
}

func TestStaleGapBecomesUnknownAfterMaxAge(t *testing.T) {
	// Dense samples stay known; a sparse one with a long gap goes unknown.
	// They are queried separately because an empty group_by merges all series.
	dense := series("dense",
		model.Sample{Time: 0, Value: 4}, model.Sample{Time: 1000, Value: 4},
		model.Sample{Time: 2000, Value: 4}, model.Sample{Time: 3000, Value: 4},
		model.Sample{Time: 4000, Value: 6}, model.Sample{Time: 5000, Value: 6},
		model.Sample{Time: 6000, Value: 6}, model.Sample{Time: 8000, Value: 6},
	)
	sparse := series("sparse",
		model.Sample{Time: 0, Value: 4},
		model.Sample{Time: 4000, Value: 6},
	)
	q := baseQuery("twavg")
	denseRes, err := WeightedAggregate([]model.SeriesPoints{dense}, q)
	if err != nil {
		t.Fatal(err)
	}
	sparseRes, err := WeightedAggregate([]model.SeriesPoints{sparse}, q)
	if err != nil {
		t.Fatal(err)
	}
	denseCell := denseRes.Cells[0]
	sparseCell := sparseRes.Cells[0]
	// Dense stream is known across the whole window.
	if denseCell.CoveredMS != 10000 {
		t.Fatalf("dense covered = %d, want 10000", denseCell.CoveredMS)
	}
	if !approx(denseCell.Value, (4*4000+6*6000)/10000.0) {
		t.Fatalf("dense twavg = %v", denseCell.Value)
	} // Sparse stream only knows 4000ms despite identical underlying steps.
	if sparseCell.CoveredMS != 4000 {
		t.Fatalf("sparse covered = %d, want 4000", sparseCell.CoveredMS)
	}
	if !approx(sparseCell.Value, 5.0) {
		t.Fatalf("sparse twavg = %v, want 5", sparseCell.Value)
	}
}

func TestPredecessorSeedsWindowOpening(t *testing.T) {
	// No sample inside the window, but a fresh predecessor holds into it.
	s := series("a", model.Sample{Time: -500, Value: 3})
	q := baseQuery("twavg")
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 1 {
		t.Fatalf("want 1 cell from predecessor hold, got %d", len(res.Cells))
	}
	c := res.Cells[0]
	// Hold covers [0, 1500), then unknown.
	if c.CoveredMS != 1500 {
		t.Fatalf("covered = %d, want 1500", c.CoveredMS)
	}
	if !approx(c.Value, 3.0) || c.Count != 0 {
		t.Fatalf("value=%v count=%d, want 3 and 0 in-window samples", c.Value, c.Count)
	}
}

func TestExpiredPredecessorContributesNothing(t *testing.T) {
	// Predecessor is older than max_age_ms, so the opening is unknown.
	s := series("a", model.Sample{Time: -5000, Value: 3})
	q := baseQuery("twavg")
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 0 {
		t.Fatalf("want no cell, got %+v", res.Cells)
	}
}

func TestRequireCoverageDropsPartialWindow(t *testing.T) {
	s := series("a",
		model.Sample{Time: 0, Value: 4},
		model.Sample{Time: 4000, Value: 6},
	)
	lenient := baseQuery("twavg")
	strict := lenient
	strict.RequireCoverage = true
	loose, err := WeightedAggregate([]model.SeriesPoints{s}, lenient)
	if err != nil {
		t.Fatal(err)
	}
	if len(loose.Cells) != 1 || loose.DroppedCoverage != 0 {
		t.Fatalf("lenient should keep partial cell: %+v", loose)
	}
	tight, err := WeightedAggregate([]model.SeriesPoints{s}, strict)
	if err != nil {
		t.Fatal(err)
	}
	if len(tight.Cells) != 0 || tight.DroppedCoverage != 1 {
		t.Fatalf("strict should drop one partial cell: %+v", tight)
	}
}

func TestFullyCoveredWindowPassesStrictMode(t *testing.T) {
	// A sample at end-max_age keeps the closing hold known right to the end.
	s := series("a",
		model.Sample{Time: 0, Value: 1}, model.Sample{Time: 1000, Value: 2},
		model.Sample{Time: 5000, Value: 3}, model.Sample{Time: 8000, Value: 4},
	)
	q := baseQuery("twavg")
	q.MaxAgeMS = 5000 // every hold reaches the next sample or the end
	q.RequireCoverage = true
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Cells) != 1 {
		t.Fatalf("fully covered window must survive strict mode: %+v", res)
	}
	if res.Cells[0].CoveredMS != 10000 {
		t.Fatalf("covered = %d, want 10000", res.Cells[0].CoveredMS)
	}
}

func TestNegativeTimesAndNonZeroOrigin(t *testing.T) {
	// Window [-5000, 1000), step 2000, origin 500 => buckets at ...,-1500,500.
	s := series("a",
		model.Sample{Time: -6000, Value: 9}, // stale before window starts
		model.Sample{Time: -3000, Value: 2},
	)
	q := model.Query{
		Start: -5000, End: 1000, Step: 2000, Origin: 500,
		Function: "twavg", MaxAgeMS: 2000,
	}
	res, err := WeightedAggregate([]model.SeriesPoints{s}, q)
	if err != nil {
		t.Fatal(err)
	}
	// Sample -3000 holds to -1000, crossing the -1500 bucket boundary.
	first := cellByStart(res.Cells, -3500)
	second := cellByStart(res.Cells, -1500)
	if first == nil || second == nil {
		t.Fatalf("expected buckets -3500 and -1500, got %+v", res.Cells)
	}
	// [-3000,-1500) in bucket -3500..500 wide; [-1500,-1000) in next bucket.
	if first.CoveredMS != 1500 || !approx(first.Value, 2) {
		t.Fatalf("first bucket covered=%d value=%v", first.CoveredMS, first.Value)
	}
	if second.CoveredMS != 500 || !approx(second.Value, 2) {
		t.Fatalf("second bucket covered=%d value=%v", second.CoveredMS, second.Value)
	}
}

func TestGroupedStrictCoverageCountsSeries(t *testing.T) {
	// Two series in one group; one fully covered, one partial. The group is
	// not fully covered and must be dropped in strict mode.
	full := series("a",
		model.Sample{Time: 0, Value: 1}, model.Sample{Time: 5000, Value: 1},
		model.Sample{Time: 8000, Value: 1},
	)
	partial := series("b", model.Sample{Time: 0, Value: 2})
	full.Series.Labels = model.Labels{"__name__": "load", "zone": "east", "node": "a"}
	partial.Series.Labels = model.Labels{"__name__": "load", "zone": "east", "node": "b"}
	q := baseQuery("twavg")
	q.GroupBy = []string{"zone"}
	q.MaxAgeMS = 5000
	strict := q
	strict.RequireCoverage = true
	loose, err := WeightedAggregate([]model.SeriesPoints{full, partial}, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(loose.Cells) != 1 || loose.Cells[0].CoveredMS != 15000 {
		t.Fatalf("lenient group covered=%v cells=%+v", loose.Cells, loose.Cells)
	}
	tight, err := WeightedAggregate([]model.SeriesPoints{full, partial}, strict)
	if err != nil {
		t.Fatal(err)
	}
	if len(tight.Cells) != 0 || tight.DroppedCoverage != 1 {
		t.Fatalf("partial series must make the group under-covered: %+v", tight)
	}
}

func TestValidationRejectsWeightedWithoutMaxAge(t *testing.T) {
	q := baseQuery("twavg")
	q.MaxAgeMS = 0
	if err := Validate(q); err == nil {
		t.Fatal("expected rejection of twavg without max_age_ms")
	}
	q = baseQuery("avg")
	q.RequireCoverage = true
	if err := Validate(q); err == nil {
		t.Fatal("expected rejection of coverage option on plain avg")
	}
}
