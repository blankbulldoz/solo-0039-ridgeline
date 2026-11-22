package visibility

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Result is one replayed operation observed from the engine and from the
// model. Only the exported facts that the public contract promises are
// compared: acceptance, failure code and the visible range snapshot.
type Result struct {
	Op     Operation `json:"op"`
	Engine Effect    `json:"engine"`
	Model  Effect    `json:"model"`
}

// Effect is the externally observable outcome of one mutation.
type Effect struct {
	Accepted bool      `json:"accepted"`
	Code     string    `json:"code,omitempty"`
	Snapshot *Snapshot `json:"snapshot,omitempty"`
}

// Divergence identifies the first operation whose engine and model results
// differ, together with the replay prefix up to and including that operation as
// JSON so a failure can be stored and minimized.
type Divergence struct {
	Index    int       `json:"index"`
	Op       Operation `json:"op"`
	Engine   Effect    `json:"engine"`
	Model    Effect    `json:"model"`
	Sequence []byte    `json:"sequence"`
}

func (d *Divergence) Error() string {
	return fmt.Sprintf("visibility divergence at operation %d (%s): engine=%s model=%s",
		d.Index, d.Op.Kind, describe(d.Engine), describe(d.Model))
}

func describe(e Effect) string {
	if !e.Accepted {
		return "rejected/" + e.Code
	}
	if e.Snapshot == nil {
		return "accepted"
	}
	return fmt.Sprintf("accepted/snapshot=%s", string(mustJSON(e.Snapshot)))
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// Recorder couples an engine adapter with the independent model. It replays
// operations in order, comparing one observable effect at a time.
type Recorder struct {
	Model *Store
	Space string
	// Start and End bound the range query observed after each operation.
	Start, End int64
	// Matchers select the series observed after each operation.
	Matchers []Matcher

	adapter Adapter
	history []Operation
}

// Adapter is the engine side of the comparison. Implementations must only use
// public mutation/query entry points (Write/Delete/Seal/Compact/Retain/Range)
// and must never call the model to derive an expected value.
type Adapter interface {
	// Apply performs the mutation and returns its public error code, or "" on
	// success.
	Apply(op Operation) (code string, err error)
	// Range returns the engine's visible samples for the interval. It must be
	// a fresh read transaction, not a cached projection.
	Range(space string, start, end int64, matchers []Matcher) (Snapshot, error)
	// State returns floor and watermark so restart tests can compare them.
	State(space string) (floor, watermark int64, hasWatermark bool, err error)
	// Identity maps labels to the engine's series identity so snapshots can be
	// compared without depending on the model's identity encoding.
	Identity(labels Labels) string
}

// NewRecorder prepares a recorder with an already-created model namespace.
func NewRecorder(adapter Adapter, model *Store, space string, start, end int64, matchers []Matcher) *Recorder {
	return &Recorder{
		Model: model, Space: space, Start: start, End: end, Matchers: matchers,
		adapter: adapter,
	}
}

// Apply replays one operation against both sides and returns their effects.
// The returned error is non-nil only when the engine adapter itself failed
// (a harness problem); semantic mismatches are reported through Result.
func (r *Recorder) Apply(op Operation) (Result, error) {
	if op.Space == "" {
		op.Space = r.Space
	}
	res := Result{Op: op}
	code, err := r.adapter.Apply(op)
	if err != nil {
		return res, fmt.Errorf("engine adapter: %w", err)
	}
	res.Engine = Effect{Accepted: code == "", Code: code}

	modelCode := ""
	if err := r.Model.Apply(op); err != nil {
		var domain *ModelError
		if !asModelError(err, &domain) {
			return res, fmt.Errorf("model: %w", err)
		}
		modelCode = domain.Code
	}
	res.Model = Effect{Accepted: modelCode == "", Code: modelCode}

	r.history = append(r.history, op)
	if res.Engine.Accepted && res.Model.Accepted {
		// Reads are only compared after accepted mutations. A rejected write
		// must leave the visible set unchanged; checking that is the job of
		// the next snapshot.
		engineSnap, err := r.adapter.Range(op.Space, r.Start, r.End, r.Matchers)
		if err != nil {
			return res, fmt.Errorf("engine range: %w", err)
		}
		modelSnap, err := r.Model.Snapshot(op.Space, r.Start, r.End, r.Matchers)
		if err != nil {
			return res, fmt.Errorf("model snapshot: %w", err)
		}
		engineSnap = r.normalize(engineSnap)
		modelSnap = r.normalize(modelSnap)
		res.Engine.Snapshot = &engineSnap
		res.Model.Snapshot = &modelSnap
	}
	return res, nil
}

// Compare returns a *Divergence when the two effects differ. Refused
// mutations must agree on refusal and on the failure category.
func (r *Recorder) Compare(index int, res Result) *Divergence {
	diverge := res.Engine.Accepted != res.Model.Accepted
	if !diverge && !res.Engine.Accepted {
		diverge = res.Engine.Code != res.Model.Code
	}
	if !diverge && res.Engine.Accepted {
		diverge = string(mustJSON(res.Engine.Snapshot)) != string(mustJSON(res.Model.Snapshot))
	}
	if !diverge {
		return nil
	}
	return &Divergence{
		Index: index, Op: res.Op, Engine: res.Engine, Model: res.Model,
		Sequence: r.Sequence(),
	}
}

// Sequence renders every operation applied so far as pretty JSON.
func (r *Recorder) Sequence() []byte {
	data, err := json.MarshalIndent(r.history, "", "  ")
	if err != nil {
		panic(err)
	}
	return data
}

// Step applies one operation, compares it and returns a divergence if any.
func (r *Recorder) Step(op Operation) (*Divergence, error) {
	index := len(r.history)
	res, err := r.Apply(op)
	if err != nil {
		return nil, err
	}
	return r.Compare(index, res), nil
}

// normalize rewrites series keys to the engine identity. Both sides use the
// same mapping so the oracle's own encoding never leaks into a comparison.
func (r *Recorder) normalize(s Snapshot) Snapshot {
	return Snapshot{Floor: s.Floor, Series: normalizeSeries(s.Series, r.adapter)}
}

// normalizeSeries rewrites series keys to the engine identity so the two
// snapshots are comparable. The model's own identity is structural; tests
// never rely on its spelling, and the adapter never supplies point values.
func normalizeSeries(series []SeriesView, adapter Adapter) []SeriesView {
	out := make([]SeriesView, 0, len(series))
	for _, s := range series {
		key := s.Key
		if adapter != nil {
			key = adapter.Identity(Labels(s.Labels))
		}
		out = append(out, SeriesView{Key: key, Labels: s.Labels, Points: s.Points})
	}
	// Re-sort because identity mapping can reorder series.
	sortSeries(out)
	return out
}

func sortSeries(series []SeriesView) {
	sort.Slice(series, func(i, j int) bool { return series[i].Key < series[j].Key })
}

func asModelError(err error, out **ModelError) bool {
	domain, ok := err.(*ModelError)
	if ok {
		*out = domain
	}
	return ok
}
