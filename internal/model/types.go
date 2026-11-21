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
	// MaxGapMS selects the largest trusted interval between two adjacent
	// samples for the gap-aware "rate_gap" function. Longer intervals are
	// treated as device disconnects and contribute neither increments nor
	// covered time. It is only valid with function "rate_gap".
	MaxGapMS int64 `json:"max_gap_ms,omitempty"`
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

// TrustedSegment is one continuous run of adjacent samples whose intervals
// were all within the configured maximum trusted sampling gap. Intervals of a
// segment are half-open [Start, End); a single isolated sample cannot form a
// segment and therefore never covers any time.
type TrustedSegment struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// SeriesCoverage states which device one contribution to a rate_gap cell came
// from and over which trusted time that contribution was actually measured.
// A device that reported in the window but never had a trusted adjacent pair
// appears with CoverageMS and Increase equal to zero and no segments, so
// missing measurements stay distinguishable from measured zero load.
type SeriesCoverage struct {
	Series     Series           `json:"series"`
	CoverageMS int64            `json:"coverage_ms"`
	Increase   float64          `json:"increase"`
	Segments   []TrustedSegment `json:"segments"`
	Points     int              `json:"points"`
}

type Cell struct {
	Labels Labels  `json:"labels"`
	Start  int64   `json:"start"`
	End    int64   `json:"end"`
	Value  float64 `json:"value"`
	Count  int     `json:"count"`
	// The following fields are populated only by function "rate_gap".
	// CoverageMS sums the trusted covered time of every contributing series;
	// SeriesCoverage explains which devices and time spans that covers.
	MaxGapMS       int64            `json:"max_gap_ms,omitempty"`
	CoverageMS     int64            `json:"coverage_ms,omitempty"`
	SeriesCoverage []SeriesCoverage `json:"series_coverage,omitempty"`
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
