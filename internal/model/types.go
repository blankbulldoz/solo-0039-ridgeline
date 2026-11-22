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
	// RateEdgeContext opts rate queries into bounded outside-window
	// neighbor support for windows that contain a single sample. It is
	// rejected for every other function and leaves the default rate
	// contract untouched when false.
	RateEdgeContext bool `json:"rate_edge_context,omitempty"`
	// RateMaxGapMS is the largest permitted distance between a window
	// boundary and a borrowed neighbor sample, in milliseconds.
	RateMaxGapMS int64 `json:"rate_max_gap_ms,omitempty"`
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
	Edges      int `json:"edges,omitempty"`
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
	// RateEvidence is populated only for rate queries that enable edge
	// context. It records how much of the rate rests on samples actually
	// observed inside the window and how much is extrapolated from bounded
	// outside neighbors.
	Evidence *RateEvidence `json:"rate_evidence,omitempty"`
}

// SeriesRateEvidence explains one series' contribution to an edge-context
// rate cell. Times are the actual sample timestamps supporting the value;
// covered/extrapolated durations are milliseconds of the window that the
// supporting span fills versus leaves to be extrapolated.
type SeriesRateEvidence struct {
	SeriesID        string  `json:"series_id"`
	Mode            string  `json:"mode"`
	SupportStart    int64   `json:"support_start,omitempty"`
	SupportEnd      int64   `json:"support_end,omitempty"`
	InWindowSamples int     `json:"in_window_samples"`
	EdgeSamples     int     `json:"edge_samples"`
	CoveredMS       int64   `json:"covered_ms"`
	ExtrapolatedMS  int64   `json:"extrapolated_ms"`
	Increase        float64 `json:"increase"`
	EdgeReset       bool    `json:"edge_reset,omitempty"`
}

// RateEvidence aggregates the per-series support behind a rate cell.
type RateEvidence struct {
	Mode              string               `json:"mode"`
	MaxGapMS          int64                `json:"max_gap_ms"`
	SupportStart      int64                `json:"support_start,omitempty"`
	SupportEnd        int64                `json:"support_end,omitempty"`
	CoveredMS         int64                `json:"covered_ms"`
	ExtrapolatedMS    int64                `json:"extrapolated_ms"`
	EdgeSamples       int                  `json:"edge_samples"`
	EdgeReset         bool                 `json:"edge_reset,omitempty"`
	UnsupportedSeries int                  `json:"unsupported_series"`
	Series            []SeriesRateEvidence `json:"series"`
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
