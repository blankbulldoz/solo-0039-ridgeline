package engine

import (
	"math"
	"testing"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
)

func resolverWith(t *testing.T, series model.Series, view model.Calibration) *calibrationResolver {
	t.Helper()
	rules := make([]compiledRule, 0, len(view.Rules))
	for _, rule := range view.Rules {
		selector, err := selectseries.Compile(rule.Matchers)
		if err != nil {
			t.Fatalf("compile matchers: %v", err)
		}
		compiled := compiledRule{series: map[string]bool{}, segments: rule.Segments}
		if selector.Matches(series.Labels) {
			compiled.series[series.ID] = true
		}
		rules = append(rules, compiled)
	}
	return &calibrationResolver{view: view.Name, rules: rules}
}

func sampleView() model.Calibration {
	return model.Calibration{
		Name: "v1",
		Rules: []model.CalibrationRule{{
			Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe"}},
			Segments: []model.CalibrationSegment{
				{Start: 0, End: 2000, Factor: 1.0, Offset: 0.0},
				{Start: 2000, End: 10000, Factor: 1.1, Offset: 2.0},
			},
		}},
	}
}

func TestResolveHalfOpenBoundaryAndGap(t *testing.T) {
	s := model.Series{ID: "s1", Labels: model.Labels{"__name__": "probe"}}
	r := resolverWith(t, s, sampleView())

	cases := []struct {
		time       int64
		wantFactor float64
		covered    bool
	}{
		{-1, 0, false},    // before first segment
		{0, 1.0, true},    // inclusive start
		{1999, 1.0, true}, // last ms of first segment
		{2000, 1.1, true}, // boundary belongs to later segment
		{9999, 1.1, true}, // last ms of second segment
		{10000, 0, false}, // exclusive end, gap thereafter
		{50000, 0, false}, // far outside any segment
	}
	for _, tc := range cases {
		seg, ok := r.resolve(s.ID, tc.time)
		if ok != tc.covered {
			t.Fatalf("t=%d coverage = %v, want %v", tc.time, ok, tc.covered)
		}
		if ok && seg.Factor != tc.wantFactor {
			t.Fatalf("t=%d factor = %v, want %v", tc.time, seg.Factor, tc.wantFactor)
		}
	}
}

func TestApplyProvenanceAndPassThrough(t *testing.T) {
	s := model.Series{ID: "s1", Labels: model.Labels{"__name__": "probe"}}
	r := resolverWith(t, s, sampleView())

	inside, err := r.apply(s.ID, model.Sample{Time: 2000, Value: 30, Sequence: 7})
	if err != nil {
		t.Fatalf("apply inside segment: %v", err)
	}
	if inside.Value != 35 || inside.RawValue != 30 || inside.Factor != 1.1 ||
		inside.Offset != 2.0 || !inside.Calibrated || inside.Sequence != 7 {
		t.Fatalf("unexpected calibrated point: %+v", inside)
	}

	gap, err := r.apply(s.ID, model.Sample{Time: 20000, Value: 9, Sequence: 8})
	if err != nil {
		t.Fatalf("apply in gap: %v", err)
	}
	if gap.Value != 9 || gap.RawValue != 9 || gap.Factor != 1 || gap.Offset != 0 ||
		gap.Calibrated {
		t.Fatalf("gap must pass raw through with identity coefficients: %+v", gap)
	}
}

func TestApplyRejectsNonFiniteResult(t *testing.T) {
	s := model.Series{ID: "s1", Labels: model.Labels{"__name__": "probe"}}
	view := model.Calibration{
		Name: "huge",
		Rules: []model.CalibrationRule{{
			Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe"}},
			Segments: []model.CalibrationSegment{
				{Start: 0, End: 10, Factor: math.MaxFloat64, Offset: 0},
			},
		}},
	}
	r := resolverWith(t, s, view)
	if _, err := r.apply(s.ID, model.Sample{Time: 1, Value: 2, Sequence: 1}); err == nil {
		t.Fatal("non-finite calibrated value must be reported as an error")
	}
}

func TestResolveIndependentOfArrivalOrder(t *testing.T) {
	s := model.Series{ID: "s1", Labels: model.Labels{"__name__": "probe"}}
	r := resolverWith(t, s, sampleView())
	// A late sample and an on-time sample at the same observation time must
	// resolve to the identical coefficient regardless of commit sequence.
	late, err := r.apply(s.ID, model.Sample{Time: 500, Value: 40, Sequence: 99})
	if err != nil {
		t.Fatalf("apply late point: %v", err)
	}
	early, err := r.apply(s.ID, model.Sample{Time: 500, Value: 40, Sequence: 1})
	if err != nil {
		t.Fatalf("apply early point: %v", err)
	}
	if late.Factor != early.Factor || late.Offset != early.Offset ||
		late.Value != early.Value {
		t.Fatalf("late and early points at the same observation time diverged: %+v vs %+v",
			late, early)
	}
}

func TestValidateCalibrationRejectsOverlap(t *testing.T) {
	view := model.Calibration{
		Name: "bad",
		Rules: []model.CalibrationRule{{
			Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe"}},
			Segments: []model.CalibrationSegment{
				{Start: 0, End: 2500, Factor: 1, Offset: 0},
				{Start: 2000, End: 10000, Factor: 2, Offset: 0},
			},
		}},
	}
	if err := model.ValidateCalibration(view); err == nil {
		t.Fatal("overlapping half-open intervals must be rejected")
	}

	// Touching intervals [0,2000) + [2000,10000) are a valid seam.
	view.Rules[0].Segments[0].End = 2000
	if err := model.ValidateCalibration(view); err != nil {
		t.Fatalf("adjacent half-open intervals should be accepted: %v", err)
	}
}

func TestValidateCalibrationRejectsBadCoefficientsAndNames(t *testing.T) {
	base := func() model.Calibration {
		return model.Calibration{
			Name: "ok",
			Rules: []model.CalibrationRule{{
				Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "probe"}},
				Segments: []model.CalibrationSegment{{Start: 0, End: 1, Factor: 1, Offset: 0}},
			}},
		}
	}
	badName := base()
	badName.Name = "bad name"
	if err := model.ValidateCalibration(badName); err == nil {
		t.Fatal("invalid view name must be rejected")
	}
	noRules := base()
	noRules.Rules = nil
	if err := model.ValidateCalibration(noRules); err == nil {
		t.Fatal("view without rules must be rejected")
	}
	nan := base()
	nan.Rules[0].Segments[0].Factor = math.NaN()
	if err := model.ValidateCalibration(nan); err == nil {
		t.Fatal("NaN factor must be rejected")
	}
	empty := base()
	empty.Rules[0].Segments[0].Start = 5
	empty.Rules[0].Segments[0].End = 5
	if err := model.ValidateCalibration(empty); err == nil {
		t.Fatal("empty interval must be rejected")
	}
}
