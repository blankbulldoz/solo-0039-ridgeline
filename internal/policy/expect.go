package policy

import "ridgeline/internal/model"

// Matches reports whether a current observation state satisfies the author's
// expectation. A correction that carries no expectation always matches; the
// unconditional write path keeps its original newest-committed-wins meaning.
func Matches(e *model.Expectation, actual model.ObservationState) bool {
	if e == nil {
		return true
	}
	state := e.State
	if state == "" {
		state = model.StatePresent
	}
	if state != actual.State {
		return false
	}
	if e.Sequence != nil && (actual.Sequence != *e.Sequence || actual.Sequence == 0) {
		return false
	}
	if e.Value != nil {
		if actual.Value == nil || *actual.Value != *e.Value {
			return false
		}
	}
	return true
}
