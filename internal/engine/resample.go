package engine

import (
	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// resampleSpan is the source interval a grid can depend on. Only samples
// within max_gap of the grid can bracket a grid point: a sample farther away
// would need a neighbor beyond max_gap to participate, which the gap rule
// forbids, so widening by exactly max_gap on both sides loses nothing.
func resampleSpan(q model.ResampleQuery) (int64, int64) {
	lo := q.Start - q.MaxGap
	if lo < model.MinTime {
		lo = model.MinTime
	}
	last := q.Start + int64(q.Steps-1)*q.Step
	hi := last + q.MaxGap
	if hi > model.MaxTime {
		hi = model.MaxTime
	}
	return lo, hi + 1
}

func (e *Engine) Resample(name string, q model.ResampleQuery) (model.ResampleResult, error) {
	out := model.ResampleResult{
		Start: q.Start, Step: q.Step, Steps: q.Steps, MaxGap: q.MaxGap,
		Series: []model.ResampleSeries{},
	}
	if err := model.ValidateResample(q); err != nil {
		return out, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		selector, err := selectseries.Compile(q.Matchers)
		if err != nil {
			return err
		}
		all, err := catalog(b)
		if err != nil {
			return err
		}
		selected := selectseries.Pick(all, selector)
		// The output shape is fixed by the request: every selected series
		// yields exactly Steps cells. Rejecting oversized shapes before the
		// scan keeps failure independent of the data that would be read.
		if len(selected)*q.Steps > model.MaxResampleCells {
			return model.Capacity("resample output cell budget exceeded")
		}
		lo, hi := resampleSpan(q)
		raw, err := scan(b, n, model.Query{Start: lo, End: hi, Matchers: q.Matchers}, volume.Current(tx))
		if err != nil {
			return err
		}
		visible := map[string][]model.Sample{}
		for _, s := range raw.Series {
			visible[s.Series.ID] = s.Points
		}
		out.Sequence, out.Floor, out.Stats = raw.Sequence, raw.Floor, raw.Stats
		for _, s := range selected {
			points, err := window.Resample(visible[s.ID], q)
			if err != nil {
				return err
			}
			out.Series = append(out.Series, model.ResampleSeries{Series: s, Points: points})
		}
		return nil
	})
	return out, err
}
