package policy

import "ridgeline/internal/model"

// Validate rejects a structurally inadmissible request before any observation
// state is resolved. It covers read-only policy, observation time, point count
// and per-observation shape.
func Validate(n model.Namespace, request model.WriteRequest) error {
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
	}
	return nil
}

// Admit applies event-time admission windows. It runs after conditional
// corrections so an expectation can report a retention-expired observation
// distinctly from an absent one.
func Admit(n model.Namespace, request model.WriteRequest) error {
	for _, p := range request.Points {
		if p.Time < n.Floor {
			return model.Conflict("point precedes retention floor")
		}
		if p.Time-request.Now > n.FutureMS {
			return model.Conflict("point exceeds future skew policy")
		}
		if n.HasWatermark && n.Watermark-p.Time > n.LatenessMS {
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
