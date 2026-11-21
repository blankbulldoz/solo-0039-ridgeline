package model

import "ridgeline/internal/quantile"

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

// QuantileQuery defines a bounded, mergeable approximate quantile query. It
// selects and groups points exactly like an aggregate query (same matchers,
// interval, origin/step windowing and group_by labels), but instead of an exact
// reducer it builds epsilon-approximate quantile summaries. Epsilon is the
// caller-specified relative rank precision and Phis the requested quantiles
// (e.g. 0.95 for p95).
type QuantileQuery struct {
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	Matchers []Matcher `json:"matchers"`
	Step     int64     `json:"step"`
	Origin   int64     `json:"origin"`
	GroupBy  []string  `json:"group_by"`
	Epsilon  float64   `json:"epsilon"`
	Phis     []float64 `json:"phis"`
}

// QuantileEstimate is one returned quantile with its value and error meaning.
// Lower/Upper bracket an observed value equal to the true quantile; RankError
// is the absolute rank distance the estimate may be off by, and Epsilon the
// relative rank precision. The guarantee is |trueRank - targetRank| <=
// RankError over N observed values in that window.
type QuantileEstimate struct {
	Phi       float64 `json:"phi"`
	Value     float64 `json:"value"`
	Lower     float64 `json:"lower"`
	Upper     float64 `json:"upper"`
	RankError int     `json:"rank_error"`
}

// QuantileCell is one window's quantile output across one label group.
type QuantileCell struct {
	Labels    Labels             `json:"labels"`
	Start     int64              `json:"start"`
	End       int64              `json:"end"`
	Count     int                `json:"count"`
	Epsilon   float64            `json:"epsilon"`
	Estimates []QuantileEstimate `json:"estimates"`
}

// QuantileResult is the query-time output of the streaming quantile engine.
type QuantileResult struct {
	Sequence uint64         `json:"sequence"`
	Cells    []QuantileCell `json:"cells"`
	Stats    ScanStats      `json:"stats"`
}

// QuantileMaterialization is a durable, mergeable summary built once and
// reused for later queries. It stores the serialized summaries per window/group
// so repeated quantile queries never sort all values in memory.
type QuantileMaterialization struct {
	Name           string          `json:"name"`
	Query          QuantileQuery   `json:"query"`
	SourceSequence uint64          `json:"source_sequence"`
	Result         QuantileResult  `json:"result"`
	Summaries      []StoredSummary `json:"summaries"`
}

// StoredSummary is one materialized window/group summary with its serialized
// Greenwald-Khanna tuples.
type StoredSummary struct {
	Key      string            `json:"key"`
	Labels   Labels            `json:"labels"`
	Start    int64             `json:"start"`
	End      int64             `json:"end"`
	Count    int               `json:"count"`
	Snapshot quantile.Snapshot `json:"snapshot"`
}

// QuantileView is a stored materialization plus its freshness flag.
type QuantileView struct {
	QuantileMaterialization
	Stale bool `json:"stale"`
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
