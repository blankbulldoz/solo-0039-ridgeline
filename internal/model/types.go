package model

type Labels map[string]string

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

// Continuous is a named window aggregation that follows source mutations.
// Its output is materialized per window and repaired incrementally.
type Continuous struct {
	Name           string `json:"name"`
	Query          Query  `json:"query"`
	SourceSequence uint64 `json:"source_sequence"`
}

// WindowRecord is the materialized output of one time window.
type WindowRecord struct {
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Sequence uint64 `json:"sequence"`
	Cells    []Cell `json:"cells"`
}

// Invalidation records that one window must be recomputed. Entries are
// written in the same transaction as the mutation that caused them.
type Invalidation struct {
	Start    int64  `json:"start"`
	Sequence uint64 `json:"sequence"`
}

type WindowView struct {
	WindowRecord
	Pending bool `json:"pending"`
}

// ContinuousView separates usable windows from windows awaiting refresh.
// Pending lists window starts with a recorded invalidation, including
// windows that have no materialized record yet.
type ContinuousView struct {
	Continuous
	Sequence     uint64       `json:"sequence"`
	Windows      []WindowView `json:"windows"`
	Pending      []int64      `json:"pending"`
	PendingCount int          `json:"pending_windows"`
}

type RefreshRequest struct {
	MaxWindows int `json:"max_windows"`
}

// RefreshResult reports one bounded repair batch: windows recomputed and
// stored, windows dropped because they no longer have visible source
// points, and the invalidation backlog still remaining.
type RefreshResult struct {
	Name      string         `json:"name"`
	Sequence  uint64         `json:"sequence"`
	Refreshed []WindowRecord `json:"refreshed"`
	Dropped   []int64        `json:"dropped"`
	Remaining int            `json:"remaining"`
}
