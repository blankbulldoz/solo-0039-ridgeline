package policy

import "ridgeline/internal/model"

// Stable per-point rejection reason codes returned in best-effort receipts.
const (
	ReasonReadOnly       = "read_only"
	ReasonBadObservation = "invalid_observation"
	ReasonRetention      = "before_retention_floor"
	ReasonFutureSkew     = "future_skew_exceeded"
	ReasonLateness       = "lateness_exceeded"
	ReasonSeriesQuota    = "series_quota_exceeded"
)

// PointError pairs a stable reason code with its human-readable detail.
type PointError struct {
	Reason string
	Detail string
}

func (e *PointError) Error() string {
	return e.Reason + ": " + e.Detail
}

func reject(reason, detail string) *PointError {
	return &PointError{Reason: reason, Detail: detail}
}

// RequestError validates request-level facts that apply to every write mode.
// Read-only state and the caller-supplied observation time cannot be repaired
// per point, so they always fail the whole request.
func RequestError(n model.Namespace, request model.WriteRequest) error {
	if n.ReadOnly {
		return model.Conflict("namespace is read-only")
	}
	if !model.ValidTime(request.Now) {
		return model.Bad("invalid observation time")
	}
	if len(request.Points) == 0 || len(request.Points) > n.MaxWrite {
		return model.Capacity("write point count exceeds namespace policy")
	}
	return nil
}

// Evaluate applies every per-point admission rule except series cardinality,
// which the engine tracks as new series become accepted. Lateness is measured
// against the committed watermark at request start: rejected points never
// advance it, so one late point cannot push other points out of the window.
func Evaluate(n model.Namespace, now int64, p model.Observation) *PointError {
	if err := model.ValidateObservation(p); err != nil {
		return reject(ReasonBadObservation, err.Error())
	}
	if p.Time < n.Floor {
		return reject(ReasonRetention, "point precedes retention floor")
	}
	if p.Time-now > n.FutureMS {
		return reject(ReasonFutureSkew, "point exceeds future skew policy")
	}
	if n.HasWatermark && n.Watermark-p.Time > n.LatenessMS {
		return reject(ReasonLateness, "point exceeds out-of-order time policy")
	}
	return nil
}

// Admit preserves the historical atomic, all-or-nothing admission path.
func Admit(n model.Namespace, request model.WriteRequest) error {
	if err := RequestError(n, request); err != nil {
		return err
	}
	for _, p := range request.Points {
		if perr := Evaluate(n, request.Now, p); perr != nil {
			return model.Conflict(perr.Detail)
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
