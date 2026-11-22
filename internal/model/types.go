package model

type Labels map[string]string

const (
	StateWritable = "writable"
	StateDraining = "draining"
	StateReadOnly = "read_only"
)

type Namespace struct {
	Name         string `json:"name"`
	ReadOnly     bool   `json:"read_only"`
	RetentionMS  int64  `json:"retention_ms"`
	LatenessMS   int64  `json:"lateness_ms"`
	FutureMS     int64  `json:"future_ms"`
	MaxSeries    int    `json:"max_series"`
	MaxWrite     int    `json:"max_write"`
	MaxScan      int    `json:"max_scan"`
	Floor        int64  `json:"floor"`
	Watermark    int64  `json:"watermark"`
	HasWatermark bool   `json:"has_watermark"`
	LastSequence uint64 `json:"last_sequence"`

	// State is the durable namespace lifecycle state. ReadOnly is retained in
	// the wire contract and is true exactly when State is StateReadOnly.
	State string `json:"state"`
	// DrainSequence records the commit that stopped accepting logical
	// mutations. DrainCompletedSequence records the commit that made the
	// namespace maintenance-ready. DrainCanceledSequence records a cancel.
	DrainSequence          uint64 `json:"drain_sequence,omitempty"`
	DrainCompletedSequence uint64 `json:"drain_completed_sequence,omitempty"`
	DrainCanceledSequence  uint64 `json:"drain_canceled_sequence,omitempty"`
}

// EffectiveState normalizes legacy namespace records that predate the
// lifecycle state field.
func (n Namespace) EffectiveState() string {
	switch n.State {
	case StateWritable, StateDraining, StateReadOnly:
		return n.State
	}
	if n.ReadOnly {
		return StateReadOnly
	}
	return StateWritable
}

// Normalize keeps the legacy read_only flag and the lifecycle state in sync.
func (n *Namespace) Normalize() {
	n.State = n.EffectiveState()
	n.ReadOnly = n.State == StateReadOnly
}

// DrainStatus is the observable lifecycle view of a namespace.
//
// State and the three sequences are durable: they survive a restart and are
// only written by a committed lifecycle transaction. Acceptance stops
// process-locally just before the start sequence commits, so a caller may
// briefly observe state "writable" alongside a rejected request.
//
// In-flight logical requests are process-local. After a restart the field is
// zero for every namespace and restarted is true, which is the signal that a
// draining namespace's earlier in-flight work no longer exists in this
// process. The durable State and the three drain sequences survive the
// restart, so the lifecycle decision remains auditable.
type DrainStatus struct {
	State string `json:"state"`
	// Phase is "accepting", "stopping" (drain started but logical requests
	// remain in flight), "completing" (completion is committing), "quiesced"
	// (no in-flight logical work, completion not committed yet) or
	// "maintainable" (read_only).
	Phase               string `json:"phase"`
	AcceptingWrites     bool   `json:"accepting_writes"`
	AcceptingLogical    bool   `json:"accepting_logical"`
	PhysicalMaintenance bool   `json:"physical_maintenance"`
	InFlight            int    `json:"in_flight"`
	Maintainable        bool   `json:"maintainable"`
	Drained             bool   `json:"drained"`
	Restarted           bool   `json:"restarted"`
	StartedSequence     uint64 `json:"started_sequence"`
	CompletedSequence   uint64 `json:"completed_sequence"`
	CanceledSequence    uint64 `json:"canceled_sequence"`
	CompletionError     string `json:"completion_error,omitempty"`
}

// ValidState reports whether s is a lifecycle state accepted by the mode API.
func ValidState(s string) bool {
	return s == StateWritable || s == StateDraining || s == StateReadOnly
}

type NamespaceView struct {
	Namespace
	Drain DrainStatus `json:"drain"`
}

type Series struct {
	ID     string `json:"id"`
	Labels Labels `json:"labels"`
}

type Sample struct {
	Time     int64   `json:"time"`
	Value    float64 `json:"value"`
	Sequence uint64  `json:"sequence"`
}

type Observation struct {
	Labels Labels  `json:"labels"`
	Time   int64   `json:"time"`
	Value  float64 `json:"value"`
}

type WriteRequest struct {
	Now    int64         `json:"now"`
	Points []Observation `json:"points"`
}

type WriteReceipt struct {
	Sequence  uint64 `json:"sequence"`
	Accepted  int    `json:"accepted"`
	NewSeries int    `json:"new_series"`
	Watermark int64  `json:"watermark"`
}

type Matcher struct {
	Label string `json:"label"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

type Query struct {
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	Matchers []Matcher `json:"matchers"`
	Limit    int       `json:"limit"`
	Step     int64     `json:"step"`
	Origin   int64     `json:"origin"`
	GroupBy  []string  `json:"group_by"`
	Function string    `json:"function"`
}

type SeriesPoints struct {
	Series Series   `json:"series"`
	Points []Sample `json:"points"`
}

type ScanStats struct {
	Series     int `json:"series"`
	Blocks     int `json:"blocks"`
	Head       int `json:"head"`
	Decoded    int `json:"decoded"`
	Visible    int `json:"visible"`
	Tombstones int `json:"tombstones"`
}

type RangeResult struct {
	Sequence  uint64         `json:"sequence"`
	Floor     int64          `json:"floor"`
	Series    []SeriesPoints `json:"series"`
	Stats     ScanStats      `json:"stats"`
	Truncated bool           `json:"truncated"`
}

type Cell struct {
	Labels Labels  `json:"labels"`
	Start  int64   `json:"start"`
	End    int64   `json:"end"`
	Value  float64 `json:"value"`
	Count  int     `json:"count"`
}

type AggregateResult struct {
	Sequence uint64    `json:"sequence"`
	Cells    []Cell    `json:"cells"`
	Stats    ScanStats `json:"stats"`
}

type Tombstone struct {
	ID       string   `json:"id"`
	Series   []string `json:"series"`
	Start    int64    `json:"start"`
	End      int64    `json:"end"`
	Sequence uint64   `json:"sequence"`
}

type Block struct {
	ID       string `json:"id"`
	SeriesID string `json:"series_id"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Count    int    `json:"count"`
	Bytes    int    `json:"bytes"`
	Checksum string `json:"checksum"`
	Format   int    `json:"format"`
}

type Materialization struct {
	Name           string          `json:"name"`
	Query          Query           `json:"query"`
	SourceSequence uint64          `json:"source_sequence"`
	Result         AggregateResult `json:"result"`
}

type RollupView struct {
	Materialization
	Stale bool `json:"stale"`
}

type Maintenance struct {
	Sequence uint64 `json:"sequence"`
	Blocks   int    `json:"blocks"`
	Points   int    `json:"points"`
	Floor    int64  `json:"floor"`
}
