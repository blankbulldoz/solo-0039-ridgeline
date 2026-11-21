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
	Quality  Quality `json:"quality,omitempty"`
}

type Observation struct {
	Labels  Labels  `json:"labels"`
	Time    int64   `json:"time"`
	Value   float64 `json:"value"`
	Quality Quality `json:"quality,omitempty"`
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
	// IncludeQualities selects which observation qualities participate in a
	// reduction. Omitted means trusted and estimated, preserving scalar
	// behavior; invalid observations are excluded by default.
	IncludeQualities []Quality `json:"include_qualities,omitempty"`
	// ReportQuality asks reductions to attach a per-cell quality summary.
	ReportQuality bool `json:"report_quality,omitempty"`
}

type SeriesPoints struct {
	Series Series   `json:"series"`
	Points []Sample `json:"points"`
}

// QualityPoint is one element of a mixed-quality timeline: the quality carried
// by the visible observation at a timestamp and the sequence that committed it.
type QualityPoint struct {
	Time     int64   `json:"time"`
	Value    float64 `json:"value"`
	Quality  Quality `json:"quality"`
	Sequence uint64  `json:"sequence"`
}

// QualityTimeline reports the ordered quality of every visible observation for
// one series when that series mixes more than one quality in the queried range.
type QualityTimeline struct {
	SeriesID string         `json:"series_id"`
	Points   []QualityPoint `json:"points"`
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
	Sequence  uint64          `json:"sequence"`
	Floor     int64           `json:"floor"`
	Series    []SeriesPoints  `json:"series"`
	Stats     ScanStats       `json:"stats"`
	Truncated bool            `json:"truncated"`
	Quality   *QualitySummary `json:"quality,omitempty"`
	// Qualities reports the ordered quality of every visible observation for
	// each series that mixes more than one quality in the queried range. It is
	// present only when the request sets report_quality.
	Qualities []QualityTimeline `json:"qualities,omitempty"`
}

type Cell struct {
	Labels  Labels          `json:"labels"`
	Start   int64           `json:"start"`
	End     int64           `json:"end"`
	Value   float64         `json:"value"`
	Count   int             `json:"count"`
	Quality *QualitySummary `json:"quality,omitempty"`
}

type AggregateResult struct {
	Sequence uint64          `json:"sequence"`
	Cells    []Cell          `json:"cells"`
	Stats    ScanStats       `json:"stats"`
	Quality  *QualitySummary `json:"quality,omitempty"`
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
