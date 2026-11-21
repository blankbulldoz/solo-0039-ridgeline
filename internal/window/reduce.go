package window

import (
	"fmt"
	"math"

	"ridgeline/internal/model"
)

type accumulator struct {
	cell     model.Cell
	sum      float64
	firstT   int64
	lastT    int64
	firstKey string
	lastKey  string
	first    float64
	last     float64
	min      float64
	max      float64
}

// Aggregate reduces included observations into origin-aligned cells. Invalid
// observations are excluded unless explicitly included; any other quality is
// included. With no quality fields the reduction is exactly the previous scalar
// one. When report_quality or include_qualities is present, a quality summary
// accompanies each cell and the result.
func Aggregate(series []model.SeriesPoints, q model.Query) ([]model.Cell, *model.QualitySummary, error) {
	if err := Validate(q); err != nil {
		return nil, nil, err
	}
	if q.Function == "rate" {
		return Rates(series, q)
	}
	filter, err := newSelection(q)
	if err != nil {
		return nil, nil, err
	}
	report := reportRequested(q)
	// groupKey -> accumulator, with the (group, start) pair retained so the
	// per-cell quality summary can be merged after the pass.
	type groupID struct {
		group string
		start int64
	}
	groups := map[string]*accumulator{}
	ids := map[string]groupID{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		for _, p := range s.Points {
			start := Bucket(p.Time, q.Origin, q.Step)
			ck := cellKey{series: s.Series.ID, group: key, start: start}
			quality := qualityOf(p)
			if !filter.wants(quality) {
				filter.excludeObservation(ck, p)
				continue
			}
			filter.includeObservation(ck, p)
			filter.contribute(ck, quality, p.Value)
			k := fmt.Sprintf("%s/%d", key, start)
			a := groups[k]
			if a == nil {
				a = &accumulator{
					cell:   model.Cell{Labels: labels, Start: start, End: start + q.Step},
					firstT: p.Time, lastT: p.Time, first: p.Value, last: p.Value,
					firstKey: s.Series.ID, lastKey: s.Series.ID,
					min: p.Value, max: p.Value,
				}
				groups[k] = a
				ids[k] = groupID{group: key, start: start}
			}
			a.cell.Count++
			a.sum += p.Value
			a.min = math.Min(a.min, p.Value)
			a.max = math.Max(a.max, p.Value)
			if p.Time < a.firstT || (p.Time == a.firstT && s.Series.ID < a.firstKey) {
				a.firstT, a.firstKey, a.first = p.Time, s.Series.ID, p.Value
			}
			if p.Time > a.lastT || (p.Time == a.lastT && s.Series.ID > a.lastKey) {
				a.lastT, a.lastKey, a.last = p.Time, s.Series.ID, p.Value
			}
		}
	}
	out := make([]model.Cell, 0, len(groups))
	produced := map[groupStart]bool{}
	for k, a := range groups {
		id := ids[k]
		produced[groupStart{id.group, id.start}] = true
		switch q.Function {
		case "count":
			a.cell.Value = float64(a.cell.Count)
		case "sum":
			a.cell.Value = a.sum
		case "avg":
			a.cell.Value = a.sum / float64(a.cell.Count)
		case "min":
			a.cell.Value = a.min
		case "max":
			a.cell.Value = a.max
		case "first":
			a.cell.Value = a.first
		case "last":
			a.cell.Value = a.last
		}
		if math.IsInf(a.cell.Value, 0) || math.IsNaN(a.cell.Value) {
			return nil, nil, model.Capacity("aggregation overflow")
		}
		if report && (filter.full || filter.cellHasExcluded(id.group, id.start)) {
			a.cell.Quality = filter.groupSummary(id.group, id.start)
		}
		out = append(out, a.cell)
	}
	if report {
		// Any reducer here needs one included observation; a group with only
		// excluded observations is explained rather than silently absent.
		filter.markSuppressed(produced, 1)
	}
	SortCells(out)
	if !report && !filter.needsExplanation() {
		// With no quality request and nothing excluded the original scalar
		// response is returned exactly.
		return out, nil, nil
	}
	return out, filter.resultSummary(), nil
}
