package window

import (
	"math"
	"testing"

	"ridgeline/internal/model"
)

func seriesOf(node string) model.Series {
	labels := model.Labels{"__name__": "events_total", "group": "g", "node": node}
	return model.Series{ID: model.SeriesID(labels), Labels: labels}
}

func pt(t int64, v float64) model.Sample {
	return model.Sample{Time: t, Value: v}
}

func points(samples ...model.Sample) []model.Sample {
	return samples
}

func baseQuery(function string, maxGap int64) model.Query {
	return model.Query{
		Start: 0, End: 11000, Step: 11000, Origin: 0,
		GroupBy: []string{"group"}, Function: function, MaxGapMS: maxGap,
	}
}

func nearly(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func findCell(t *testing.T, cells []model.Cell) model.Cell {
	t.Helper()
	if len(cells) != 1 {
		t.Fatalf("expected one cell, got %d: %+v", len(cells), cells)
	}
	return cells[0]
}

func coverageByNode(cell model.Cell) map[string]model.SeriesCoverage {
	out := map[string]model.SeriesCoverage{}
	for _, c := range cell.SeriesCoverage {
		out[c.Series.Labels["node"]] = c
	}
	return out
}

// TestGapRatesDisconnectDistribution builds the controlled comparison from the
// requirement: the same group, the same per-device total increase, but the
// disconnect is concentrated on one device in case A and simultaneous on both
// devices in case B. Both group windows then share the same summed coverage,
// yet only per-device coverage reveals whether the window is fit for alerting.
func TestGapRatesDisconnectDistribution(t *testing.T) {
	const maxGap int64 = 3500

	// Online devices report every 1000 ms; a 5000/4000 ms outage exceeds the
	// trusted gap and breaks the trusted run. Values rise at 10 per covered
	// second, so total increase depends only on covered time: both cases give
	// each device the same total increase (d1: 90, d2: 60).
	// Case A: high-load d1 reports the whole 9000 ms; d2 covers two short runs
	// totalling 3000 ms and is offline across the middle 5000 ms.
	caseA := []model.SeriesPoints{
		{Series: seriesOf("d1"), Points: points(
			pt(0, 0), pt(1000, 10), pt(2000, 20), pt(3000, 30), pt(4000, 40),
			pt(5000, 50), pt(6000, 60), pt(7000, 70), pt(8000, 80), pt(9000, 90))},
		{Series: seriesOf("d2"), Points: points(
			pt(0, 0), pt(1000, 20),
			pt(6000, 20), pt(7000, 40), pt(8000, 60))},
	}
	// Case B: both devices go offline at the same time (2000..6000, a 4000 ms
	// untrusted interval) and each covers two trusted runs of 6000 ms total.
	caseB := []model.SeriesPoints{
		{Series: seriesOf("d1"), Points: points(
			pt(0, 0), pt(1000, 10), pt(2000, 20),
			pt(6000, 20), pt(7000, 40), pt(8000, 60), pt(9000, 75), pt(10000, 90))},
		{Series: seriesOf("d2"), Points: points(
			pt(0, 0), pt(1000, 20), pt(2000, 40),
			pt(6000, 40), pt(7000, 45), pt(8000, 50), pt(9000, 55), pt(10000, 60))},
	}

	legacyA, err := Rates(caseA, baseQuery("rate", 0))
	if err != nil {
		t.Fatal(err)
	}
	legacyB, err := Rates(caseB, baseQuery("rate", 0))
	if err != nil {
		t.Fatal(err)
	}
	// Legacy denominator spans first to last observed point, so the missing
	// 5000 ms in A and the shared 4000 ms outage in B stay inside the
	// denominator: A: 90/9 (d1) + 60/8 (d2) = 17.5/s; B: (90 + 60)/10 = 15/s.
	// Both dilute unmeasured downtime instead of excluding it.
	nearly(t, "legacy A group rate", findCell(t, legacyA).Value, 17.5)
	nearly(t, "legacy B group rate", findCell(t, legacyB).Value, 15)

	gapA, err := GapRates(caseA, baseQuery("rate_gap", maxGap))
	if err != nil {
		t.Fatal(err)
	}
	gapB, err := GapRates(caseB, baseQuery("rate_gap", maxGap))
	if err != nil {
		t.Fatal(err)
	}
	cellA, cellB := findCell(t, gapA), findCell(t, gapB)

	// Trusted coverage: A = 9000 + 3000, B = 6000 + 6000 — identical totals.
	if cellA.CoverageMS != 12000 || cellB.CoverageMS != 12000 {
		t.Fatalf("coverage A=%d B=%d, want 12000 for both", cellA.CoverageMS, cellB.CoverageMS)
	}
	// Identical total coverage must not mean identical comparability.
	covA, covB := coverageByNode(cellA), coverageByNode(cellB)
	if covA["d1"].CoverageMS != 9000 || covA["d2"].CoverageMS != 3000 {
		t.Fatalf("case A per-device coverage = %d/%d, want 9000/3000",
			covA["d1"].CoverageMS, covA["d2"].CoverageMS)
	}
	if covB["d1"].CoverageMS != 6000 || covB["d2"].CoverageMS != 6000 {
		t.Fatalf("case B per-device coverage = %d/%d, want 6000/6000",
			covB["d1"].CoverageMS, covB["d2"].CoverageMS)
	}

	// Caller policy: every alerting device must cover at least 6000 ms of the
	// window. Case A fails on d2; case B passes for both devices.
	perDeviceOK := func(cell model.Cell, minCoverage int64) bool {
		for _, c := range cell.SeriesCoverage {
			if c.CoverageMS < minCoverage {
				return false
			}
		}
		return true
	}
	if perDeviceOK(cellA, 6000) {
		t.Fatal("case A must be rejected by a 6000 ms per-device coverage policy")
	}
	if !perDeviceOK(cellB, 6000) {
		t.Fatal("case B must satisfy a 6000 ms per-device coverage policy")
	}
	// A group-total-only policy cannot tell them apart.
	if cellA.CoverageMS != cellB.CoverageMS {
		t.Fatal("group total coverage differs despite the controlled design")
	}

	// Group values: per-series trusted rates, summed — not total increase over
	// total coverage. A: 90/9 + 60/3 = 30/s. B: 90/6 + 60/6 = 25/s.
	nearly(t, "gap A group rate", cellA.Value, 30)
	nearly(t, "gap B group rate", cellB.Value, 25)
	nearly(t, "gap A d1 increase", covA["d1"].Increase, 90)
	nearly(t, "gap A d2 increase", covA["d2"].Increase, 60)
	nearly(t, "gap B d1 increase", covB["d1"].Increase, 90)
	nearly(t, "gap B d2 increase", covB["d2"].Increase, 60)

	// d2 in A has two trusted runs [0,1000] and [6000,8000]; the 5000 ms
	// untrusted interval contributes no segment and no coverage.
	d2segments := covA["d2"].Segments
	if len(d2segments) != 2 || d2segments[0] != (model.TrustedSegment{Start: 0, End: 1000}) ||
		d2segments[1] != (model.TrustedSegment{Start: 6000, End: 8000}) {
		t.Fatalf("unexpected d2 segments: %+v", d2segments)
	}
	// B has two trusted runs per device split by the shared 4000 ms outage.
	d1B := covB["d1"]
	if len(d1B.Segments) != 2 || d1B.Segments[0] != (model.TrustedSegment{Start: 0, End: 2000}) ||
		d1B.Segments[1] != (model.TrustedSegment{Start: 6000, End: 10000}) {
		t.Fatalf("unexpected case B d1 segments: %+v", d1B.Segments)
	}
	if cellA.Count != 15 || cellB.Count != 16 {
		t.Fatalf("sample counts A=%d B=%d, want 15/16", cellA.Count, cellB.Count)
	}
	if cellA.MaxGapMS != maxGap || cellB.MaxGapMS != maxGap {
		t.Fatal("cells must echo the trusted gap")
	}
}

// TestGapRatesResetAndResiduals checks that reset handling stays inside
// trusted pairs, and that a single isolated sample (residual segment) is
// visible but contributes no coverage or rate.
func TestGapRatesResetAndResiduals(t *testing.T) {
	// Trusted pair 0->1000 rises 30; pair 1000->2000 drops 80->10, which is a
	// reset contributing the new value 10. A later point at 8000 is separated
	// by a 6000 ms untrusted gap.
	input := []model.SeriesPoints{{
		Series: seriesOf("d1"),
		Points: points(pt(0, 0), pt(1000, 30), pt(2000, 10), pt(8000, 90)),
	}}
	cells, err := GapRates(input, baseQuery("rate_gap", 3500))
	if err != nil {
		t.Fatal(err)
	}
	cell := findCell(t, cells)
	cov := coverageByNode(cell)["d1"]
	if cov.CoverageMS != 2000 {
		t.Fatalf("coverage = %d, want 2000", cov.CoverageMS)
	}
	// Increase 30 + reset-to-zero 10 = 40 over 2 seconds => 20/s.
	nearly(t, "increase across reset", cov.Increase, 40)
	nearly(t, "rate across reset", cell.Value, 20)
	if cov.Points != 4 || cell.Count != 4 {
		t.Fatalf("all observed points stay visible: cov.Points=%d cell.Count=%d", cov.Points, cell.Count)
	}
	if len(cov.Segments) != 1 || cov.Segments[0] != (model.TrustedSegment{Start: 0, End: 2000}) {
		t.Fatalf("unexpected segments: %+v", cov.Segments)
	}

	// Only one point in the window: no trusted pair, so no coverage or rate,
	// but the device is still reported rather than silently treated as zero.
	single := []model.SeriesPoints{{Series: seriesOf("d9"), Points: points(pt(500, 7))}}
	cells, err = GapRates(single, baseQuery("rate_gap", 3500))
	if err != nil {
		t.Fatal(err)
	}
	cell = findCell(t, cells)
	if cell.Value != 0 || cell.CoverageMS != 0 || cell.Count != 1 {
		t.Fatalf("single residual point: value=%v coverage=%d count=%d", cell.Value, cell.CoverageMS, cell.Count)
	}
	cov = coverageByNode(cell)["d9"]
	if cov.CoverageMS != 0 || cov.Points != 1 || len(cov.Segments) != 0 {
		t.Fatalf("residual point must have zero coverage: %+v", cov)
	}
}

func TestGapRatesValidationAndNegatives(t *testing.T) {
	input := []model.SeriesPoints{{Series: seriesOf("d1"), Points: points(pt(0, 0), pt(1000, -1))}}
	if _, err := GapRates(input, baseQuery("rate_gap", 3500)); err == nil {
		t.Fatal("negative counter values must be rejected")
	}
	if err := Validate(baseQuery("rate_gap", 0)); err == nil {
		t.Fatal("rate_gap without a positive max_gap_ms must be rejected")
	}
	if err := Validate(baseQuery("rate_gap", model.MaxTime+1)); err == nil {
		t.Fatal("max_gap_ms above the supported time range must be rejected")
	}
	legacy := baseQuery("rate", 0)
	legacy.MaxGapMS = 1000
	if err := Validate(legacy); err == nil {
		t.Fatal("max_gap_ms must be rejected for the legacy rate function")
	}
	if err := Validate(baseQuery("sum", 0)); err != nil {
		t.Fatalf("legacy functions remain valid: %v", err)
	}
}

// TestLegacyRateUnchanged guards the old caliber: disconnect time stays in the
// denominator and cells expose no coverage metadata.
func TestLegacyRateUnchanged(t *testing.T) {
	input := []model.SeriesPoints{{
		Series: seriesOf("d1"),
		Points: points(pt(0, 0), pt(1000, 20), pt(8000, 20)),
	}}
	cells, err := Rates(input, baseQuery("rate", 0))
	if err != nil {
		t.Fatal(err)
	}
	cell := findCell(t, cells)
	nearly(t, "legacy rate keeps full first-to-last denominator", cell.Value, 20.0/8.0)
	if cell.CoverageMS != 0 || cell.SeriesCoverage != nil || cell.MaxGapMS != 0 {
		t.Fatalf("legacy rate must not expose gap metadata: %+v", cell)
	}
}
