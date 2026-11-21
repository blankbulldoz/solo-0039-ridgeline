package window

import (
	"testing"

	"ridgeline/internal/model"
)

func series(points ...model.Sample) []model.SeriesPoints {
	return []model.SeriesPoints{{
		Series: model.Series{ID: "s", Labels: model.Labels{"__name__": "m"}},
		Points: points,
	}}
}

func point(t int64, value float64, quality model.Quality) model.Sample {
	return model.Sample{Time: t, Value: value, Quality: quality}
}

func TestDefaultAverageExcludesOnlyInvalid(t *testing.T) {
	cells, quality, err := Aggregate(series(
		point(0, 2, ""),
		point(100, 100, model.QualityInvalid),
		point(200, 4, model.QualityEstimated),
	), model.Query{Start: 0, End: 1000, Step: 1000, Function: "avg"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || cells[0].Value != 3 || cells[0].Count != 2 {
		t.Fatalf("unexpected cell: %+v", cells)
	}
	if quality == nil {
		t.Fatal("an exclusion must be explained")
	}
	if quality.Excluded.Invalid != 1 {
		t.Fatalf("unexpected excluded tally: %+v", quality.Excluded)
	}
	if quality.ExcludedBetween != 1 || !quality.SpansExcluded {
		t.Fatalf("excluded point between included ones must be reported: %+v", quality)
	}
}

func TestDefaultReductionUnchangedWhenNothingExcluded(t *testing.T) {
	cells, quality, err := Aggregate(series(point(0, 2, ""), point(100, 4, "")),
		model.Query{Start: 0, End: 1000, Step: 1000, Function: "avg"})
	if err != nil {
		t.Fatal(err)
	}
	if quality != nil || cells[0].Quality != nil {
		t.Fatalf("a reduction that excluded nothing must keep the scalar shape")
	}
	if cells[0].Value != 3 || cells[0].Count != 2 {
		t.Fatalf("unexpected cell: %+v", cells[0])
	}
}

func TestExplicitQualitySelection(t *testing.T) {
	q := model.Query{Start: 0, End: 1000, Step: 1000, Function: "sum", ReportQuality: true,
		IncludeQualities: []model.Quality{model.QualityInvalid}}
	cells, quality, err := Aggregate(series(
		point(0, 2, ""),
		point(100, 5, model.QualityInvalid),
	), q)
	if err != nil {
		t.Fatal(err)
	}
	if cells[0].Value != 5 || cells[0].Count != 1 {
		t.Fatalf("explicit selection must include invalid: %+v", cells[0])
	}
	if len(quality.Contributions) != 1 || quality.Contributions[0].Quality != model.QualityInvalid ||
		quality.Contributions[0].Sum != 5 {
		t.Fatalf("unexpected contributions: %+v", quality.Contributions)
	}
	if quality.Excluded.Trusted != 1 {
		t.Fatalf("trusted should be excluded here: %+v", quality.Excluded)
	}
}

func TestRateSkipsExcludedCounterPoint(t *testing.T) {
	points := series(
		point(0, 0, ""),
		point(100, 10, ""),
		point(200, 999, model.QualityInvalid),
		point(300, 20, ""),
	)
	q := model.Query{Start: 0, End: 1000, Step: 1000, Function: "rate", ReportQuality: true}
	cells, quality, err := Aggregate(points, q)
	if err != nil {
		t.Fatal(err)
	}
	// Included sequence 0,10,20 over 0.3s yields 20/0.3, not a reset spike.
	const want = 20.0 / 0.3
	if len(cells) != 1 || cells[0].Value != want {
		t.Fatalf("unexpected rate: %+v (want %v)", cells, want)
	}
	if quality.ExcludedBetween != 1 || !quality.SpansExcluded {
		t.Fatalf("the excluded middle point must be reported: %+v", quality)
	}
}

func TestRateSuppressedWhenFilterStarvesCell(t *testing.T) {
	q := model.Query{Start: 0, End: 1000, Step: 1000, Function: "rate", ReportQuality: true}
	cells, quality, err := Aggregate(series(
		point(0, 1, ""),
		point(100, 5, model.QualityInvalid),
	), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 0 {
		t.Fatalf("one included observation yields no rate: %+v", cells)
	}
	if quality.Suppressed != 1 {
		t.Fatalf("starved cell must be reported as suppressed: %+v", quality)
	}
}

func TestSuppressedCountsOutputCellsNotSeries(t *testing.T) {
	// Two series share the group, so one survives while the other is starved.
	// The output cell still has a value and must not be reported as suppressed.
	series := []model.SeriesPoints{
		{Series: model.Series{ID: "a", Labels: model.Labels{"__name__": "m"}},
			Points: []model.Sample{point(0, 1, ""), point(100, 2, "")}},
		{Series: model.Series{ID: "b", Labels: model.Labels{"__name__": "m"}},
			Points: []model.Sample{point(0, 1, ""), point(100, 5, model.QualityInvalid)}},
	}
	q := model.Query{Start: 0, End: 1000, Step: 1000, Function: "rate", ReportQuality: true}
	cells, quality, err := Aggregate(series, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || quality.Suppressed != 0 {
		t.Fatalf("a surviving output cell must not be suppressed: cells=%+v quality=%+v", cells, quality)
	}
}

func TestSuppressedWhenWholeGroupStarved(t *testing.T) {
	// Both series are starved in the same window, so the output cell is absent
	// and must be reported as suppressed exactly once.
	series := []model.SeriesPoints{
		{Series: model.Series{ID: "a", Labels: model.Labels{"__name__": "m"}},
			Points: []model.Sample{point(0, 1, ""), point(100, 5, model.QualityInvalid)}},
		{Series: model.Series{ID: "b", Labels: model.Labels{"__name__": "m"}},
			Points: []model.Sample{point(0, 2, ""), point(150, 9, model.QualityInvalid)}},
	}
	q := model.Query{Start: 0, End: 1000, Step: 1000, Function: "rate", ReportQuality: true}
	cells, quality, err := Aggregate(series, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 0 || quality.Suppressed != 1 {
		t.Fatalf("a starved output cell must be reported once: cells=%+v quality=%+v", cells, quality)
	}
}

func TestValidationRejectsUnknownQuality(t *testing.T) {
	q := model.Query{Start: 0, End: 1, Step: 1, Function: "avg",
		IncludeQualities: []model.Quality{"guessed"}}
	if _, _, err := Aggregate(series(point(0, 1, "")), q); err == nil {
		t.Fatal("unknown include quality must be rejected")
	}
	if _, err := model.ParseQuality("guessed"); err == nil {
		t.Fatal("unknown observation quality must be rejected")
	}
}
