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

func newAccumulator(labels model.Labels, start, step int64, p model.Sample, seriesID string) *accumulator {
	return &accumulator{
		cell:     model.Cell{Labels: labels, Start: start, End: start + step},
		firstT:   p.Time,
		lastT:    p.Time,
		first:    p.Value,
		last:     p.Value,
		firstKey: seriesID,
		lastKey:  seriesID,
		min:      p.Value,
		max:      p.Value,
	}
}

func (a *accumulator) add(p model.Sample, seriesID string) {
	a.cell.Count++
	a.sum += p.Value
	a.min = math.Min(a.min, p.Value)
	a.max = math.Max(a.max, p.Value)
	if p.Time < a.firstT || (p.Time == a.firstT && seriesID < a.firstKey) {
		a.firstT, a.firstKey, a.first = p.Time, seriesID, p.Value
	}
	if p.Time > a.lastT || (p.Time == a.lastT && seriesID > a.lastKey) {
		a.lastT, a.lastKey, a.last = p.Time, seriesID, p.Value
	}
}

func (a *accumulator) reduction() *model.Reduction {
	return &model.Reduction{Count: a.cell.Count, Sum: a.sum, Min: a.min, Max: a.max}
}

// value derives a display value from accumulated state. Average is sum/count,
// a weighted mean over raw observations rather than over series.
func (a *accumulator) value(function string) float64 {
	switch function {
	case "count":
		return float64(a.cell.Count)
	case "sum":
		return a.sum
	case "avg":
		return a.sum / float64(a.cell.Count)
	case "min":
		return a.min
	case "max":
		return a.max
	case "first":
		return a.first
	case "last":
		return a.last
	}
	return math.NaN()
}

func Aggregate(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	if err := Validate(q); err != nil {
		return nil, err
	}
	if q.Function == "rate" {
		return Rates(series, q)
	}
	groups := map[string]*accumulator{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		for _, p := range s.Points {
			start := Bucket(p.Time, q.Origin, q.Step)
			k := fmt.Sprintf("%s/%d", key, start)
			a := groups[k]
			if a == nil {
				a = newAccumulator(labels, start, q.Step, p, s.Series.ID)
				groups[k] = a
			}
			a.add(p, s.Series.ID)
		}
	}
	out := make([]model.Cell, 0, len(groups))
	for _, a := range groups {
		cell := a.cell
		cell.Value = a.value(q.Function)
		// Reducible functions retain their full state so a coarser tier can be
		// built without re-reading raw points. Non-reducible functions keep
		// only the displayed value and cannot seed a derived tier.
		if Reducible(q.Function) {
			cell.Reduction = a.reduction()
		}
		if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
			return nil, model.Capacity("aggregation overflow")
		}
		out = append(out, cell)
	}
	SortCells(out)
	return out, nil
}
