package engine

import (
	"fmt"
	"sync"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// spaceGate serializes the namespace lifecycle with logical mutations.
//
// state is the process-local copy of the durable namespace state. It is only
// changed while holding mu, and always after the matching database commit.
// transition serializes lifecycle commits (start, complete, cancel) so two
// callers cannot interleave their database transactions and in-memory state.
//
// mu is never held while a database transaction is open: a logical mutation
// blocked on bbolt must still be able to release its in-flight slot.
//
// startedHere distinguishes a drain begun by this process from one that was
// already durable when the process opened the database. In-flight counters are
// process-local, so a reopened draining namespace necessarily has no admitted
// logical work; exposing that fact is what lets an operator decide whether it
// is safe to complete.
type spaceGate struct {
	mu          sync.Mutex
	state       string
	inFlight    int
	completing  bool
	startedHere bool
	lastError   string
	transition  sync.Mutex
}

func (e *Engine) gate(name string) *spaceGate {
	e.gateMu.RLock()
	g := e.gates[name]
	e.gateMu.RUnlock()
	return g
}

// gateFor loads the namespace state on first use. Open registers every
// namespace, so the database fallback only covers the window between a
// creation commit and its registration.
func (e *Engine) gateFor(name string) (*spaceGate, error) {
	if g := e.gate(name); g != nil {
		return g, nil
	}
	var n model.Namespace
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		_, loaded, err := loadNamespace(tx, name)
		n = loaded
		return err
	})
	if err != nil {
		return nil, err
	}
	n.Normalize()
	e.gateMu.Lock()
	defer e.gateMu.Unlock()
	if g := e.gates[name]; g != nil {
		return g, nil
	}
	g := &spaceGate{state: n.EffectiveState()}
	e.gates[name] = g
	return g, nil
}

func (e *Engine) registerGate(name, state string, startedHere bool) {
	e.gateMu.Lock()
	e.gates[name] = &spaceGate{state: state, startedHere: startedHere}
	e.gateMu.Unlock()
}

// beginLogical admits a logical mutation only while the namespace accepts
// them, holding an in-flight slot until release. The slot is what makes drain
// completion safe: completion cannot commit while any admitted logical
// mutation is still able to commit.
func (e *Engine) beginLogical(name string) (func(), error) {
	g, err := e.gateFor(name)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	switch g.state {
	case model.StateWritable:
		g.inFlight++
		g.mu.Unlock()
		return g.release, nil
	case model.StateDraining:
		g.mu.Unlock()
		return nil, model.Conflict("namespace is draining; logical mutations are stopped")
	default:
		g.mu.Unlock()
		return nil, model.Conflict("namespace is read-only")
	}
}

func (g *spaceGate) release() {
	g.mu.Lock()
	if g.inFlight > 0 {
		g.inFlight--
	}
	g.mu.Unlock()
}

func (e *Engine) viewOf(n model.Namespace, g *spaceGate) model.NamespaceView {
	n.Normalize()
	state := n.EffectiveState()
	status := model.DrainStatus{
		State:               state,
		AcceptingWrites:     state == model.StateWritable,
		AcceptingLogical:    state == model.StateWritable,
		PhysicalMaintenance: true,
		Maintainable:        state == model.StateReadOnly,
		StartedSequence:     n.DrainSequence,
		CompletedSequence:   n.DrainCompletedSequence,
		CanceledSequence:    n.DrainCanceledSequence,
	}
	if g != nil {
		g.mu.Lock()
		status.InFlight = g.inFlight
		completing := g.completing
		status.Restarted = g.state == model.StateDraining && !g.startedHere
		status.CompletionError = g.lastError
		g.mu.Unlock()
		switch {
		case state == model.StateWritable:
			status.Phase = "accepting"
		case state == model.StateReadOnly:
			status.Phase = "maintainable"
		case completing:
			status.Phase = "completing"
		case status.InFlight > 0:
			status.Phase = "stopping"
		default:
			status.Phase = "quiesced"
		}
	} else {
		switch state {
		case model.StateWritable:
			status.Phase = "accepting"
		case model.StateReadOnly:
			status.Phase = "maintainable"
		default:
			status.Phase = "quiesced"
			status.Restarted = true
		}
	}
	status.Drained = state != model.StateWritable && status.InFlight == 0
	return model.NamespaceView{Namespace: n, Drain: status}
}

func (e *Engine) namespaceView(tx *bolt.Tx, name string, g *spaceGate) (model.NamespaceView, error) {
	_, n, err := loadNamespace(tx, name)
	if err != nil {
		return model.NamespaceView{}, err
	}
	return e.viewOf(n, g), nil
}

// Namespace returns the durable policy plus the process-local drain status.
func (e *Engine) Namespace(name string) (model.NamespaceView, error) {
	var out model.NamespaceView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		var err error
		out, err = e.namespaceView(tx, name, e.gate(name))
		return err
	})
	return out, err
}

// DrainStatus reports only the lifecycle portion of a namespace.
func (e *Engine) DrainStatus(name string) (model.DrainStatus, error) {
	var out model.DrainStatus
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		_, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		out = e.viewOf(n, e.gate(name)).Drain
		return nil
	})
	return out, err
}

// StartDrain stops accepting logical mutations immediately. Admitted logical
// requests keep their slots and are allowed to commit; physical maintenance
// (seal, compact) remains available. Starting a drain on an already draining
// or maintainable namespace is idempotent and returns the current view.
func (e *Engine) StartDrain(name string) (model.NamespaceView, error) {
	g, err := e.gateFor(name)
	if err != nil {
		return model.NamespaceView{}, err
	}
	g.transition.Lock()
	defer g.transition.Unlock()

	g.mu.Lock()
	if g.state != model.StateWritable {
		g.mu.Unlock()
		return e.Namespace(name)
	}
	// Flip admission before the durable commit: a logical request either
	// holds a slot before the boundary or is rejected after it. Nothing is
	// admitted between the in-memory flip and the committed sequence.
	g.state = model.StateDraining
	g.startedHere = true
	g.lastError = ""
	g.mu.Unlock()

	var n model.Namespace
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		n = loaded
		n.Normalize()
		switch n.EffectiveState() {
		case model.StateReadOnly, model.StateDraining:
			return nil
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		n.State = model.StateDraining
		n.ReadOnly = false
		n.DrainSequence = seq
		n.DrainCompletedSequence = 0
		n.DrainCanceledSequence = 0
		n.LastSequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})
	if err != nil {
		g.mu.Lock()
		g.state = model.StateWritable
		g.startedHere = false
		g.lastError = err.Error()
		g.mu.Unlock()
		return model.NamespaceView{}, err
	}
	return e.Namespace(name)
}

// CompleteDrain commits the maintainable state. It refuses while any admitted
// logical request is still in flight, so the completion sequence is a hard
// boundary: no logical mutation can commit after it.
func (e *Engine) CompleteDrain(name string) (model.NamespaceView, error) {
	g, err := e.gateFor(name)
	if err != nil {
		return model.NamespaceView{}, err
	}
	g.transition.Lock()
	defer g.transition.Unlock()

	g.mu.Lock()
	switch {
	case g.state == model.StateReadOnly:
		g.mu.Unlock()
		return e.Namespace(name)
	case g.state != model.StateDraining:
		g.mu.Unlock()
		return model.NamespaceView{}, model.Conflict("drain has not started")
	case g.inFlight > 0:
		remaining := g.inFlight
		g.mu.Unlock()
		return model.NamespaceView{}, model.Conflict(fmt.Sprintf(
			"cannot complete drain: %d in-flight logical request(s) remain", remaining))
	case g.completing:
		g.mu.Unlock()
		return model.NamespaceView{}, model.Conflict("drain completion already in progress")
	}
	g.completing = true
	g.mu.Unlock()

	var n model.Namespace
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		n = loaded
		n.Normalize()
		switch n.EffectiveState() {
		case model.StateReadOnly:
			return nil
		case model.StateDraining:
		default:
			return model.Conflict("drain has not started")
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		n.State = model.StateReadOnly
		n.ReadOnly = true
		n.DrainCompletedSequence = seq
		n.LastSequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})

	g.mu.Lock()
	g.completing = false
	if err != nil {
		g.lastError = err.Error()
	} else {
		g.state = n.EffectiveState()
		g.lastError = ""
	}
	g.mu.Unlock()
	if err != nil {
		return model.NamespaceView{}, err
	}
	return e.Namespace(name)
}

// CancelDrain returns a draining or maintainable namespace to writable. It is
// the explicit "cancel drain" transition; admitted requests are unaffected.
func (e *Engine) CancelDrain(name string) (model.NamespaceView, error) {
	g, err := e.gateFor(name)
	if err != nil {
		return model.NamespaceView{}, err
	}
	g.transition.Lock()
	defer g.transition.Unlock()

	g.mu.Lock()
	state := g.state
	g.mu.Unlock()
	if state == model.StateWritable {
		return e.Namespace(name)
	}

	var n model.Namespace
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		n = loaded
		n.Normalize()
		if n.EffectiveState() == model.StateWritable {
			return nil
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		n.State = model.StateWritable
		n.ReadOnly = false
		n.DrainCanceledSequence = seq
		n.LastSequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})
	if err != nil {
		return model.NamespaceView{}, err
	}
	g.mu.Lock()
	g.state = n.EffectiveState()
	g.startedHere = false
	g.lastError = ""
	g.mu.Unlock()
	return e.Namespace(name)
}

// SetReadOnly keeps the original boolean mode contract: true stops acceptance
// and completes the drain in one step when nothing is in flight. If admitted
// logical work is still running it fails with a conflict and leaves the
// namespace draining, so the caller must either retry or use the staged drain
// endpoints. false cancels the drain and accepts writes again.
func (e *Engine) SetReadOnly(name string, readOnly bool) (model.NamespaceView, error) {
	if !readOnly {
		return e.CancelDrain(name)
	}
	view, err := e.StartDrain(name)
	if err != nil {
		return view, err
	}
	if view.Drain.State != model.StateDraining {
		return view, nil
	}
	if view.Drain.InFlight > 0 {
		return view, model.Conflict(fmt.Sprintf(
			"namespace is draining with %d in-flight logical request(s); retry completion",
			view.Drain.InFlight))
	}
	return e.CompleteDrain(name)
}

// SetMode is the state-oriented lifecycle entry point.
func (e *Engine) SetMode(name, state string) (model.NamespaceView, error) {
	switch state {
	case model.StateWritable:
		return e.CancelDrain(name)
	case model.StateDraining:
		return e.StartDrain(name)
	case model.StateReadOnly:
		return e.SetReadOnly(name, true)
	default:
		return model.NamespaceView{}, model.Bad("invalid namespace state")
	}
}
