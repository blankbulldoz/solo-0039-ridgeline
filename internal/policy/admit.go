package policy

import "ridgeline/internal/model"

// AdmitState rejects logical mutations for a namespace whose lifecycle has
// stopped accepting them.
func AdmitState(n model.Namespace) error {
	switch n.EffectiveState() {
	case model.StateReadOnly:
		return model.Conflict("namespace is read-only")
	case model.StateDraining:
		return model.Conflict("namespace is draining; logical mutations are stopped")
	}
	return nil
}

// Admit applies the lifecycle state and the event-time policy. Direct callers
// use this; the engine admits a request once through its drain gate and then
// calls AdmitPoints so an already-admitted request can finish during a drain.
func Admit(n model.Namespace, request model.WriteRequest) error {
	if err := AdmitState(n); err != nil {
		return err
	}
	return AdmitPoints(n, request)
}

// AdmitPoints evaluates point count, age, future skew and retention without
// consulting lifecycle state.
func AdmitPoints(n model.Namespace, request model.WriteRequest) error {
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
