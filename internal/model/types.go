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

type ExemplarInput struct {
	TraceID string `json:"trace_id"`
	Labels  Labels `json:"labels"`
}

type Observation struct {
	Labels   Labels         `json:"labels"`
	Time     int64          `json:"time"`
	Value    float64        `json:"value"`
	Exemplar *ExemplarInput `json:"exemplar,omitempty"`
}

type WriteRequest struct {
	Now    int64         `json:"now"`
	Points []Observation `json:"points"`
}

type WriteReceipt struct {
	Sequence  uint64 `json:"sequence"`
	Accepted  int    `json:"accepted"`
	NewSeries int    `json:"new_series"`
	Exemplars int    `json:"exemplars"`
	Watermark int64  `json:"watermark"`
}

// StoredExemplar is the durable record. It never participates in series
// identity, head points or block payloads; the anchor fields are provenance
// for visibility checks rather than sample state.
type StoredExemplar struct {
	ID        string  `json:"id"`
	SeriesID  string  `json:"series_id"`
	Time      int64   `json:"time"`
	TraceID   string  `json:"trace_id"`
	Labels    Labels  `json:"labels"`
	Sequence  uint64  `json:"sequence"`
	Offset    int     `json:"offset"`
	AnchorVal float64 `json:"anchor_value"`
}

// Exemplar is the query-facing attachment. Anchor fields describe the sample
// at the same series-time at query time, so deletion, correction and
// retention effects remain directly inspectable.
type Exemplar struct {
	ID             string  `json:"id"`
	Series         Series  `json:"series"`
	Time           int64   `json:"time"`
	Value          float64 `json:"value"`
	TraceID        string  `json:"trace_id"`
	Labels         Labels  `json:"labels"`
	Sequence       uint64  `json:"sequence"`
	AnchorVisible  bool    `json:"anchor_visible"`
	AnchorSequence *uint64 `json:"anchor_sequence,omitempty"`
}

type ExemplarQuery struct {
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	Matchers []Matcher `json:"matchers"`
	Limit    int       `json:"limit"`
}

type ExemplarStats struct {
	Scanned         int `json:"scanned"`
	Matched         int `json:"matched"`
	Visible         int `json:"visible"`
	Omitted         int `json:"omitted"`
	HiddenTombstone int `json:"hidden_tombstone"`
	BelowFloor      int `json:"below_floor"`
	Detached        int `json:"detached"`
}

type ExemplarResult struct {
	Sequence  uint64        `json:"sequence"`
	Floor     int64         `json:"floor"`
	Limit     int           `json:"limit"`
	Truncated bool          `json:"truncated"`
	Exemplars []Exemplar    `json:"exemplars"`
	Stats     ExemplarStats `json:"stats"`
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
	Sequence         uint64 `json:"sequence"`
	Blocks           int    `json:"blocks"`
	Points           int    `json:"points"`
	ExemplarsKept    int    `json:"exemplars_kept"`
	ExemplarsDropped int    `json:"exemplars_dropped"`
	Floor            int64  `json:"floor"`
}
