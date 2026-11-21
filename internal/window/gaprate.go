package window

import (
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
)

// trustedSpan is one run of points connected by trusted adjacent intervals.
type trustedSpan struct {
	start    int64
	end      int64
	increase float64
}

// seriesRate is the trusted contribution of one series to one window.
type seriesRate struct {
	series   model.Series
	points   int
	coverage int64
	increase float64
	spans    []trustedSpan
}

// spanRate walks an ordered, deduplicated run of points and keeps only the
// adjacent pairs whose interval does not exceed the trusted gap. A decrease
// inside a trusted pair is still treated as a counter reset to zero, exactly
// like the legacy rate. Untrusted pairs break the run: the increments across
// them are discarded, and their gap adds no covered time. A run with a single
// point contributes no coverage; it only stays visible through the points
// count and the zero-coverage series entry.
func spanRate(points []model.Sample, maxGap int64) (int64, float64, []trustedSpan) {
	var coverage int64
	var increase float64
	spans := make([]trustedSpan, 0)
	for i := 1; i < len(points); i++ {
		gap := points[i].Time - points[i-1].Time
		if gap <= 0 || gap > maxGap {
			continue
		}
		delta := points[i].Value - points[i-1].Value
		if delta < 0 {
			delta = points[i].Value
		}
		if n := len(spans); n > 0 && spans[n-1].end == points[i-1].Time {
			spans[n-1].end = points[i].Time
			spans[n-1].increase += delta
		} else {
			spans = append(spans, trustedSpan{
				start: points[i-1].Time, end: points[i].Time, increase: delta,
			})
		}
		coverage += gap
		increase += delta
	}
	return coverage, increase, spans
}

// gapAccumulator collects the per-series trusted contributions into one cell.
type gapAccumulator struct {
	cell     model.Cell
	contribs []seriesRate
	coverage int64
}

// GapRates computes reset-aware counter rates while excluding disconnect
// intervals. An adjacent pair is trusted only when its interval is at most
// q.MaxGapMS. Increments and covered time are summed over trusted pairs only;
// counter resets inside trusted pairs follow the same reset-to-zero rule as
// Rates. Single-point residual runs contribute no coverage. The grouped Value
// keeps the legacy group semantics — per-series trusted rates summed across
// the group — while the returned coverage fields state which devices and
// trusted time spans that value actually represents.
func GapRates(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	cells := map[string]*gapAccumulator{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		windows := map[int64][]model.Sample{}
		for _, point := range s.Points {
			if point.Value < 0 {
				return nil, model.Bad("counter rate requires nonnegative values")
			}
			start := Bucket(point.Time, q.Origin, q.Step)
			windows[start] = append(windows[start], point)
		}
		for start, points := range windows {
			sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
			coverage, increase, spans := spanRate(points, q.MaxGapMS)
			k := fmt.Sprintf("%s/%d", key, start)
			acc := cells[k]
			if acc == nil {
				acc = &gapAccumulator{
					cell: model.Cell{
						Labels: labels, Start: start, End: start + q.Step,
						MaxGapMS: q.MaxGapMS,
					},
				}
				cells[k] = acc
			}
			rate := 0.0
			if coverage > 0 {
				rate = increase / (float64(coverage) / 1000)
			}
			if math.IsInf(rate, 0) || math.IsNaN(rate) {
				return nil, model.Capacity("counter calculation overflow")
			}
			acc.cell.Value += rate
			acc.cell.Count += len(points)
			acc.coverage += coverage
			acc.contribs = append(acc.contribs, seriesRate{
				series:   s.Series,
				points:   len(points),
				coverage: coverage,
				increase: increase,
				spans:    spans,
			})
		}
	}
	out := make([]model.Cell, 0, len(cells))
	for _, acc := range cells {
		acc.cell.CoverageMS = acc.coverage
		acc.cell.SeriesCoverage = make([]model.SeriesCoverage, 0, len(acc.contribs))
		for _, c := range acc.contribs {
			segments := make([]model.TrustedSegment, 0, len(c.spans))
			for _, span := range c.spans {
				segments = append(segments, model.TrustedSegment{Start: span.start, End: span.end})
			}
			acc.cell.SeriesCoverage = append(acc.cell.SeriesCoverage, model.SeriesCoverage{
				Series:     c.series,
				CoverageMS: c.coverage,
				Increase:   c.increase,
				Segments:   segments,
				Points:     c.points,
			})
		}
		if math.IsInf(acc.cell.Value, 0) || math.IsNaN(acc.cell.Value) {
			return nil, model.Capacity("counter calculation overflow")
		}
		out = append(out, acc.cell)
	}
	SortCells(out)
	// Keep per-device coverage deterministic within each cell regardless of
	// map iteration order.
	for i := range out {
		coverage := out[i].SeriesCoverage
		sort.Slice(coverage, func(a, b int) bool { return coverage[a].Series.ID < coverage[b].Series.ID })
	}
	return out, nil
}
