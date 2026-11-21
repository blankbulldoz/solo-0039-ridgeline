package window

import (
	"math"

	"ridgeline/internal/model"
)

// Resample evaluates sorted, correction-resolved samples on the caller's even
// grid. A grid point coinciding with a sample is observed; a point between two
// neighboring samples is linearly interpolated only when their gap does not
// exceed the caller's maximum; anything else is missing. Points outside the
// sample range, inside gaps wider than the maximum, and below the retention
// floor (already invisible to the caller) therefore stay missing.
func Resample(points []model.Sample, q model.ResampleQuery) ([]model.GridPoint, error) {
	out := make([]model.GridPoint, 0, q.Steps)
	next := 0
	for i := 0; i < q.Steps; i++ {
		t := q.Start + int64(i)*q.Step
		for next < len(points) && points[next].Time < t {
			next++
		}
		cell := model.GridPoint{Time: t, Source: model.GridMissing}
		switch {
		case next < len(points) && points[next].Time == t:
			value := points[next].Value
			cell.Source, cell.Value = model.GridObserved, &value
		case next > 0 && next < len(points) && points[next].Time-points[next-1].Time <= q.MaxGap:
			before, after := points[next-1], points[next]
			share := float64(t-before.Time) / float64(after.Time-before.Time)
			value := before.Value*(1-share) + after.Value*share
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, model.Capacity("interpolation overflow")
			}
			cell.Source, cell.Value = model.GridInterpolated, &value
		}
		out = append(out, cell)
	}
	return out, nil
}
