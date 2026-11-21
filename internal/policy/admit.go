package policy

import "ridgeline/internal/model"

// Admission carries the committed state that lateness decisions depend on.
// Known lists catalog series, so a series that existed before a per-series
// watermark record was introduced cannot be mistaken for a brand new series.
// Series holds the stored per-series watermarks, keyed by series ID.
type Admission struct {
	Known  map[string]bool
	Series map[string]model.SeriesWatermark
}

func EffectiveLateMode(n model.Namespace) string {
	if n.LateMode == "" {
		return model.LateModeNamespace
	}
	return n.LateMode
}

// watermarkFor chooses the newest previously accepted timestamp for one
// series. In series mode a catalog series without a stored record falls back
// to the namespace watermark: a missing record must never widen the window
// enough to admit history. A series absent from the catalog is genuinely new
// and has no previous timestamp to compare with.
func watermarkFor(n model.Namespace, series string, a Admission) (int64, bool) {
	if EffectiveLateMode(n) == model.LateModeSeries {
		if w, ok := a.Series[series]; ok {
			return w.Time, true
		}
		if a.Known[series] && n.HasWatermark {
			return n.Watermark, true
		}
		return 0, false
	}
	return n.Watermark, n.HasWatermark
}

func Admit(n model.Namespace, request model.WriteRequest, a Admission) error {
	if n.ReadOnly {
		return model.Conflict("namespace is read-only")
	}
	if !model.ValidTime(request.Now) {
		return model.Bad("invalid observation time")
	}
	if len(request.Points) == 0 || len(request.Points) > n.MaxWrite {
		return model.Capacity("write point count exceeds namespace policy")
	}
	for _, p := range request.Points {
		if err := model.ValidateObservation(p); err != nil {
			return err
		}
		// Retention is irreversible and takes precedence over lateness in
		// both modes, so expired timestamps cannot return through either.
		if p.Time < n.Floor {
			return model.Conflict("point precedes retention floor")
		}
		// Future skew is measured against the caller observation time, not
		// against a watermark, and is unchanged by the lateness mode.
		if p.Time-request.Now > n.FutureMS {
			return model.Conflict("point exceeds future skew policy")
		}
		// All points are judged against the state before this request, so
		// one atomic batch cannot retroactively make its own members late.
		if watermark, ok := watermarkFor(n, model.SeriesID(p.Labels), a); ok &&
			watermark-p.Time > n.LatenessMS {
			return model.Conflict("point exceeds out-of-order time policy")
		}
	}
	return nil
}

func RetentionFloor(n model.Namespace, now int64) (int64, error) {
	if !model.ValidTime(now) {
		return 0, model.Bad("invalid retention observation time")
	}
	floor := now - n.RetentionMS
	if floor < model.MinTime {
		floor = model.MinTime
	}
	if floor < n.Floor {
		floor = n.Floor
	}
	return floor, nil
}
