package window

import (
	"fmt"
	"math"

	"ridgeline/internal/model"
)

// Rates sums per-segment counter increases over per-segment elapsed time.
// A staleness marker closes the current segment: no change is ever summed
// across a marker, and the invalidation gap contributes neither increase
// nor elapsed time. Segments with fewer than two points contribute nothing.
func Rates(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	cells := map[string]*model.Cell{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		windows := map[int64][]model.Sample{}
		for _, point := range s.Points {
			if !point.Stale && *point.Value < 0 {
				return nil, model.Bad("counter rate requires nonnegative values")
			}
			start := Bucket(point.Time, q.Origin, q.Step)
			windows[start] = append(windows[start], point)
		}
		for start, points := range windows {
			increase, elapsed, count := 0.0, 0.0, 0
			segment := []model.Sample{}
			flush := func() {
				if len(segment) >= 2 {
					for i := 1; i < len(segment); i++ {
						delta := *segment[i].Value - *segment[i-1].Value
						if delta < 0 {
							delta = *segment[i].Value
						}
						increase += delta
					}
					elapsed += float64(segment[len(segment)-1].Time-segment[0].Time) / 1000
					count += len(segment)
				}
				segment = segment[:0]
			}
			for _, point := range points {
				if point.Stale {
					flush()
					continue
				}
				segment = append(segment, point)
			}
			flush()
			if count == 0 {
				continue
			}
			k := fmt.Sprintf("%s/%d", key, start)
			cell := cells[k]
			if cell == nil {
				cell = &model.Cell{Labels: labels, Start: start, End: start + q.Step}
				cells[k] = cell
			}
			cell.Value += increase / elapsed
			cell.Count += count
			if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
				return nil, model.Capacity("counter calculation overflow")
			}
		}
	}
	out := make([]model.Cell, 0, len(cells))
	for _, cell := range cells {
		out = append(out, *cell)
	}
	SortCells(out)
	return out, nil
}
