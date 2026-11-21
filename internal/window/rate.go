package window

import (
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
)

// Rates computes reset-aware counter rates. Observations excluded by quality
// filtering do not participate; a reset-aware increase is attributed to the
// quality of the observation whose value produced it. When an excluded
// observation lies strictly between the first and last included observation of
// a series in a window, the rate spans that gap and this is reported rather than
// silently interpolated.
func Rates(series []model.SeriesPoints, q model.Query) ([]model.Cell, *model.QualitySummary, error) {
	filter, err := newSelection(q)
	if err != nil {
		return nil, nil, err
	}
	report := reportRequested(q)
	cells := map[string]*model.Cell{}
	order := map[string]struct {
		group string
		start int64
	}{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		windows := map[int64][]model.Sample{}
		for _, point := range s.Points {
			start := Bucket(point.Time, q.Origin, q.Step)
			ck := cellKey{series: s.Series.ID, group: key, start: start}
			if !filter.wants(qualityOf(point)) {
				filter.excludeObservation(ck, point)
				continue
			}
			// Validation applies only to the observations that are actually
			// reduced, so an invalid sample cannot veto a filtered reduction.
			if point.Value < 0 {
				return nil, nil, model.Bad("counter rate requires nonnegative values")
			}
			filter.includeObservation(ck, point)
			windows[start] = append(windows[start], point)
		}
		for start, points := range windows {
			sort.SliceStable(points, func(i, j int) bool { return points[i].Time < points[j].Time })
			ck := cellKey{series: s.Series.ID, group: key, start: start}
			if len(points) < 2 {
				// A cell with a single included observation contributes no rate.
				// Its suppression is derived from the excluded tally in the
				// summary, so no explicit mark is needed here.
				continue
			}
			increase := 0.0
			for i := 1; i < len(points); i++ {
				delta := points[i].Value - points[i-1].Value
				if delta < 0 {
					delta = points[i].Value
				}
				increase += delta
				filter.contribute(ck, qualityOf(points[i]), delta)
			}
			elapsed := float64(points[len(points)-1].Time-points[0].Time) / 1000
			value := increase / elapsed
			k := fmt.Sprintf("%s/%d", key, start)
			cell := cells[k]
			if cell == nil {
				cell = &model.Cell{Labels: labels, Start: start, End: start + q.Step}
				cells[k] = cell
				order[k] = struct {
					group string
					start int64
				}{group: key, start: start}
			}
			cell.Value += value
			cell.Count += len(points)
			if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
				return nil, nil, model.Capacity("counter calculation overflow")
			}
		}
	}
	out := make([]model.Cell, 0, len(cells))
	produced := map[groupStart]bool{}
	for k, cell := range cells {
		id := order[k]
		produced[groupStart{id.group, id.start}] = true
		if report && (filter.full || filter.cellHasExcluded(id.group, id.start)) {
			cell.Quality = filter.groupSummary(id.group, id.start)
		}
		out = append(out, *cell)
	}
	if report {
		// A rate cell needs two included observations; a series starved below
		// that is explained instead of silently omitted.
		filter.markSuppressed(produced, 2)
	}
	SortCells(out)
	if !report && !filter.needsExplanation() {
		return out, nil, nil
	}
	return out, filter.resultSummary(), nil
}
