package window

import (
	"fmt"
	"math"

	"ridgeline/internal/model"
)

// weightedCell accumulates the held (step-function) contribution of one
// origin-aligned window across every series in a label group.
type weightedCell struct {
	cell     model.Cell
	integral float64
	covered  int64
	// seriesCovered is the covered duration contributed by each distinct
	// series, used to decide whether the group window is fully covered.
	seriesCovered map[string]int64
}

// addDuration sums two non-negative durations and rejects overflow so a
// single response cannot silently wrap the covered time.
func addDuration(a, b int64) (int64, error) {
	if b < 0 || a > model.MaxTime-b {
		return 0, model.Capacity("covered duration overflow")
	}
	return a + b, nil
}

// WeightedAggregate reduces irregular samples under step-function semantics.
//
// Each sample value holds from its timestamp until the next observation of the
// same series, but never longer than q.MaxAgeMS. Only the portion of a hold
// intersecting [q.Start, q.End) is known time; everything else in a window is
// treated as unknown. twavg divides the integral by the covered duration;
// integral returns the area (value*milliseconds). Points may include one
// predecessor strictly before q.Start, which only seeds the opening hold.
func WeightedAggregate(series []model.SeriesPoints, q model.Query) (model.AggregateResult, error) {
	result := model.AggregateResult{Cells: []model.Cell{}}
	cells := map[string]*weightedCell{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		points := s.Points
		for i, p := range points {
			// The next observation bounds the hold; absent one, the caller
			// supplied validity horizon bounds it instead.
			holdEnd := q.End
			if i+1 < len(points) {
				holdEnd = points[i+1].Time
			}
			holdEnd = min64(holdEnd, p.Time+q.MaxAgeMS)
			holdStart := max64(p.Time, q.Start)
			if holdStart >= holdEnd {
				continue
			}
			// Split the held interval at every origin-aligned bucket boundary.
			for cursor := holdStart; cursor < holdEnd; {
				bucketStart := Bucket(cursor, q.Origin, q.Step)
				fragmentEnd := min64(holdEnd, bucketStart+q.Step)
				duration := fragmentEnd - cursor
				k := fmt.Sprintf("%s/%d", key, bucketStart)
				c := cells[k]
				if c == nil {
					c = &weightedCell{
						cell:          model.Cell{Labels: labels, Start: bucketStart, End: bucketStart + q.Step},
						seriesCovered: map[string]int64{},
					}
					cells[k] = c
				}
				area, err := areaContribution(p.Value, duration)
				if err != nil {
					return result, err
				}
				c.integral += area
				if math.IsInf(c.integral, 0) || math.IsNaN(c.integral) {
					return result, model.Capacity("time-weighted aggregation overflow")
				}
				c.covered, err = addDuration(c.covered, duration)
				if err != nil {
					return result, err
				}
				c.seriesCovered[s.Series.ID], err =
					addDuration(c.seriesCovered[s.Series.ID], duration)
				if err != nil {
					return result, err
				}
				if p.Time >= q.Start && p.Time < q.End {
					c.cell.Count++
				}
				cursor = fragmentEnd
			}
		}
	}
	out := make([]model.Cell, 0, len(cells))
	for _, c := range cells {
		if q.RequireCoverage && !fullyCovered(c, q.Step) {
			result.DroppedCoverage++
			continue
		}
		if c.covered > 0 {
			if q.Function == "twavg" {
				c.cell.Value = c.integral / float64(c.covered)
			} else {
				c.cell.Value = c.integral
			}
		}
		if math.IsInf(c.cell.Value, 0) || math.IsNaN(c.cell.Value) {
			return result, model.Capacity("time-weighted aggregation overflow")
		}
		c.cell.CoveredMS = c.covered
		out = append(out, c.cell)
	}
	SortCells(out)
	result.Cells = out
	return result, nil
}

// fullyCovered requires every series that contributed to the window to have
// been known for the whole window length. A series with no data leaves the
// window absent entirely, so it cannot produce a falsely "covered" result.
func fullyCovered(c *weightedCell, step int64) bool {
	want := int64(0)
	for _, covered := range c.seriesCovered {
		if covered != step {
			return false
		}
		want += covered
	}
	return want == c.covered && want > 0
}

func areaContribution(value float64, duration int64) (float64, error) {
	area := value * float64(duration)
	if math.IsInf(area, 0) || math.IsNaN(area) {
		return 0, model.Capacity("time-weighted aggregation overflow")
	}
	return area, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
