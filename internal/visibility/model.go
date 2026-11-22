// Package visibility is an independent executable model of Ridgeline sample
// visibility. It deliberately imports only the Go standard library and never
// calls the production query or scan path. Tests use it as an oracle: they
// replay the same mutation sequence through the engine and through this model
// and compare the visible samples.
//
// The model keeps the logical state that the public contract describes:
// committed samples by series identity and timestamp, sequence-aware deletion
// tombstones, a monotonic retention floor and the admission watermark. Sealing
// and compaction are physical operations and therefore do not alter logical
// visibility; a correctly implemented engine must preserve the same result.
package visibility

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Time bounds from the public contract. They are intentionally redeclared so
// the oracle does not depend on production packages.
const (
	MinTime int64 = -(1 << 52)
	MaxTime int64 = 1 << 52
)

// Operation kinds accepted by Apply.
const (
	WriteKind   = "write"
	DeleteKind  = "delete"
	SealKind    = "seal"
	CompactKind = "compact"
	RetainKind  = "retain"
	ModeKind    = "mode"
)

// Error codes used by the model. They mirror the public failure categories
// without importing the production error package.
const (
	CodeInvalid  = "invalid"
	CodeMissing  = "missing"
	CodeConflict = "conflict"
	CodeCapacity = "capacity"
)

// Labels is a label set. Presence is significant: an absent label and a
// present label with an empty value identify different series.
type Labels map[string]string

// Observation is one measurement in a write batch.
type Observation struct {
	Labels Labels  `json:"labels"`
	Time   int64   `json:"time"`
	Value  float64 `json:"value"`
}

// Matcher is one label predicate. Absent labels compare as the empty string,
// while series identity still preserves absence.
type Matcher struct {
	Label string `json:"label"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

// Operation is one public mutation in a replay sequence.
type Operation struct {
	Kind     string        `json:"kind"`
	Space    string        `json:"space"`
	Now      int64         `json:"now"`
	Points   []Observation `json:"points,omitempty"`
	Start    int64         `json:"start,omitempty"`
	End      int64         `json:"end,omitempty"`
	Matchers []Matcher     `json:"matchers,omitempty"`
	ReadOnly bool          `json:"read_only,omitempty"`
}

// Policy is the subset of namespace policy that affects admission.
type Policy struct {
	ReadOnly    bool  `json:"read_only"`
	RetentionMS int64 `json:"retention_ms"`
	LatenessMS  int64 `json:"lateness_ms"`
	FutureMS    int64 `json:"future_ms"`
	MaxSeries   int   `json:"max_series"`
	MaxWrite    int   `json:"max_write"`
	MaxScan     int   `json:"max_scan"`
}

// ModelError describes a rejected operation using the public code categories.
type ModelError struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *ModelError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Detail) }

func modelErr(code, detail string) error { return &ModelError{Code: code, Detail: detail} }

type storedSample struct {
	value float64
	seq   uint64
}

type tombstone struct {
	series []string
	start  int64
	end    int64
	seq    uint64
}

type space struct {
	policy       Policy
	floor        int64
	watermark    int64
	hasWatermark bool
	seq          uint64
	series       map[string]Labels
	samples      map[string]map[int64]storedSample
	tombstones   []tombstone
}

// Store holds any number of namespaces, mirroring the database rather than a
// single namespace. Tests usually create one space per scenario.
type Store struct {
	spaces map[string]*space
}

// NewStore returns an empty model.
func NewStore() *Store {
	return &Store{spaces: map[string]*space{}}
}

// Create adds a namespace with an initial floor and no watermark.
func (s *Store) Create(name string, p Policy) error {
	if _, ok := s.spaces[name]; ok {
		return modelErr(CodeConflict, "namespace already exists")
	}
	s.spaces[name] = &space{
		policy:  p,
		floor:   MinTime,
		series:  map[string]Labels{},
		samples: map[string]map[int64]storedSample{},
	}
	return nil
}

// State exposes the logical namespace counters for restart and image tests.
func (s *Store) State(name string) (floor, watermark int64, hasWatermark bool, err error) {
	sp := s.spaces[name]
	if sp == nil {
		return 0, 0, false, modelErr(CodeMissing, "namespace does not exist")
	}
	return sp.floor, sp.watermark, sp.hasWatermark, nil
}

// Apply executes one public mutation against the model. It returns a
// *ModelError when the production API would reject the operation.
func (s *Store) Apply(op Operation) error {
	sp := s.spaces[op.Space]
	if sp == nil {
		return modelErr(CodeMissing, "namespace does not exist")
	}
	switch op.Kind {
	case WriteKind:
		return sp.write(op)
	case DeleteKind:
		return sp.delete(op)
	case SealKind, CompactKind:
		// Physical relocation only. Read-only namespaces still permit it.
		sp.seq++
		return nil
	case ModeKind:
		sp.policy.ReadOnly = op.ReadOnly
		sp.seq++
		return nil
	case RetainKind:
		return sp.retain(op)
	default:
		return modelErr(CodeInvalid, "unknown operation kind")
	}
}

func (sp *space) write(op Operation) error {
	if sp.policy.ReadOnly {
		return modelErr(CodeConflict, "namespace is read-only")
	}
	if !validTime(op.Now) {
		return modelErr(CodeInvalid, "invalid observation time")
	}
	if len(op.Points) == 0 || len(op.Points) > sp.policy.MaxWrite {
		return modelErr(CodeCapacity, "write point count exceeds namespace policy")
	}
	for _, p := range op.Points {
		if err := validateObservation(p); err != nil {
			return err
		}
		if p.Time < sp.floor {
			return modelErr(CodeConflict, "point precedes retention floor")
		}
		if p.Time-op.Now > sp.policy.FutureMS {
			return modelErr(CodeConflict, "point exceeds future skew policy")
		}
		if sp.hasWatermark && sp.watermark-p.Time > sp.policy.LatenessMS {
			return modelErr(CodeConflict, "point exceeds out-of-order time policy")
		}
	}

	newSeries := map[string]Labels{}
	for _, p := range op.Points {
		id := Identity(p.Labels)
		if _, ok := sp.series[id]; !ok {
			newSeries[id] = p.Labels
		}
	}
	if len(sp.series)+len(newSeries) > sp.policy.MaxSeries {
		return modelErr(CodeCapacity, "series cardinality policy exceeded")
	}

	sp.seq++
	seq := sp.seq
	for id, labels := range newSeries {
		sp.series[id] = labels
	}
	for _, p := range op.Points {
		id := Identity(p.Labels)
		if sp.samples[id] == nil {
			sp.samples[id] = map[int64]storedSample{}
		}
		// Later entries in one batch share the commit sequence and win
		// because they are applied last.
		sp.samples[id][p.Time] = storedSample{value: p.Value, seq: seq}
		if !sp.hasWatermark || p.Time > sp.watermark {
			sp.watermark, sp.hasWatermark = p.Time, true
		}
	}
	return nil
}

func (sp *space) delete(op Operation) error {
	// Query validation precedes the read-only check, as in the public API.
	if err := validateInterval(op.Start, op.End); err != nil {
		return err
	}
	ms, err := compileMatchers(op.Matchers)
	if err != nil {
		return err
	}
	if sp.policy.ReadOnly {
		return modelErr(CodeConflict, "namespace is read-only")
	}
	sp.seq++
	seq := sp.seq
	tomb := tombstone{start: op.Start, end: op.End, seq: seq}
	for id, labels := range sp.series {
		if matches(labels, ms) {
			tomb.series = append(tomb.series, id)
		}
	}
	sort.Strings(tomb.series)
	sp.tombstones = append(sp.tombstones, tomb)
	return nil
}

func (sp *space) retain(op Operation) error {
	if sp.policy.ReadOnly {
		return modelErr(CodeConflict, "namespace is read-only")
	}
	if !validTime(op.Now) {
		return modelErr(CodeInvalid, "invalid retention observation time")
	}
	floor := op.Now - sp.policy.RetentionMS
	if floor < MinTime {
		floor = MinTime
	}
	if floor < sp.floor {
		floor = sp.floor
	}
	sp.floor = floor
	sp.seq++ // retention compacts in the same successful mutation
	return nil
}

// Identity serializes a label set so that presence is significant. The
// encoding is injective; unlike production it is intentionally not a hash.
func Identity(labels Labels) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		value := labels[key]
		b.WriteString(strconv.Itoa(len(key)))
		b.WriteByte(':')
		b.WriteString(key)
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
	}
	return b.String()
}

// Point is one visible sample.
type Point struct {
	Time  int64   `json:"time"`
	Value float64 `json:"value"`
}

// SeriesView is one visible series with its points sorted by time.
type SeriesView struct {
	Key    string  `json:"key"`
	Labels Labels  `json:"labels"`
	Points []Point `json:"points"`
}

// Snapshot is the expected result of a range query.
type Snapshot struct {
	Floor  int64        `json:"floor"`
	Series []SeriesView `json:"series"`
}

type compiledMatcher struct {
	matcher Matcher
	regex   *regexp.Regexp
}

func compileMatchers(ms []Matcher) ([]compiledMatcher, error) {
	if len(ms) > 32 {
		return nil, modelErr(CodeInvalid, "invalid matcher count")
	}
	out := make([]compiledMatcher, 0, len(ms))
	for _, m := range ms {
		if !identifier.MatchString(m.Label) {
			return nil, modelErr(CodeInvalid, "invalid matcher label")
		}
		c := compiledMatcher{matcher: m}
		switch m.Op {
		case "=", "!=":
		case "=~", "!~":
			if len(m.Value) > 4096 {
				return nil, modelErr(CodeInvalid, "matcher expression is too long")
			}
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				return nil, modelErr(CodeInvalid, "invalid matcher expression")
			}
			c.regex = re
		default:
			return nil, modelErr(CodeInvalid, "unknown matcher operation")
		}
		out = append(out, c)
	}
	return out, nil
}

func matches(labels Labels, ms []compiledMatcher) bool {
	for _, c := range ms {
		value := labels[c.matcher.Label]
		matched := value == c.matcher.Value
		if c.regex != nil {
			matched = c.regex.MatchString(value)
		}
		if c.matcher.Op == "!=" || c.matcher.Op == "!~" {
			matched = !matched
		}
		if !matched {
			return false
		}
	}
	return true
}

// Snapshot returns the samples a range query [start,end) would expose. It is
// computed directly from committed samples, tombstones and the floor; no
// production code is consulted.
func (s *Store) Snapshot(name string, start, end int64, matchers []Matcher) (Snapshot, error) {
	sp := s.spaces[name]
	if sp == nil {
		return Snapshot{}, modelErr(CodeMissing, "namespace does not exist")
	}
	if err := validateInterval(start, end); err != nil {
		return Snapshot{}, err
	}
	ms, err := compileMatchers(matchers)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Floor: sp.floor, Series: []SeriesView{}}
	for id, byTime := range sp.samples {
		labels := sp.series[id]
		if !matches(labels, ms) {
			continue
		}
		points := []Point{}
		for t, sample := range byTime {
			if t < start || t >= end || t < sp.floor {
				continue
			}
			if hidden(id, t, sample.seq, sp.tombstones) {
				continue
			}
			points = append(points, Point{Time: t, Value: sample.value})
		}
		if len(points) == 0 {
			continue
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		snap.Series = append(snap.Series, SeriesView{Key: id, Labels: labels, Points: points})
	}
	sort.Slice(snap.Series, func(i, j int) bool { return snap.Series[i].Key < snap.Series[j].Key })
	return snap, nil
}

func hidden(id string, t int64, seq uint64, tombstones []tombstone) bool {
	for _, tomb := range tombstones {
		if t < tomb.start || t >= tomb.end || seq > tomb.seq {
			continue
		}
		for _, series := range tomb.series {
			if series == id {
				return true
			}
		}
	}
	return false
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:-]{0,95}$`)

func validTime(t int64) bool { return t >= MinTime && t <= MaxTime }

func validateInterval(start, end int64) error {
	if !validTime(start) || end < MinTime || end > MaxTime+1 || start >= end {
		return modelErr(CodeInvalid, "query requires a supported nonempty half-open interval")
	}
	return nil
}

func validateObservation(p Observation) error {
	if len(p.Labels) == 0 || len(p.Labels) > 32 {
		return modelErr(CodeInvalid, "labels must contain 1..32 entries")
	}
	for key, value := range p.Labels {
		if !identifier.MatchString(key) || len(value) > 512 {
			return modelErr(CodeInvalid, "invalid label name or value")
		}
	}
	if p.Labels["__name__"] == "" {
		return modelErr(CodeInvalid, "__name__ label is required")
	}
	if !validTime(p.Time) || math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
		return modelErr(CodeInvalid, "time outside supported range or value is not finite")
	}
	return nil
}
