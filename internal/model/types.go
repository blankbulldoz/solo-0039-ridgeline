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

// Write modes. The empty string and WriteAtomic preserve the historical
// all-or-nothing request semantics; WriteBestEffort admits each point on its
// own merits and reports every rejected point in the receipt.
const (
	WriteAtomic     = "atomic"
	WriteBestEffort = "best_effort"
)

// Per-point disposition codes used by best-effort receipts.
const (
	PointAccepted    = "accepted"
	PointOverwritten = "overwritten"
	PointRejected    = "rejected"
)

type WriteRequest struct {
	Now    int64         `json:"now"`
	Mode   string        `json:"mode,omitempty"`
	Points []Observation `json:"points"`
}

// PointResult records the disposition of one input point at its original
// position in the request. Reasons are stable machine-readable codes; Detail
// carries the human-readable explanation.
type PointResult struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
	Time   int64  `json:"time,omitempty"`
	Series string `json:"series_id,omitempty"`
}

type WriteReceipt struct {
	Sequence uint64 `json:"sequence"`
	Mode     string `json:"mode"`
	// Requested is the number of input points, distinct from Accepted which
	// counts only points that became stored samples.
	Requested    int           `json:"requested"`
	Accepted     int           `json:"accepted"`
	Overwritten  int           `json:"overwritten"`
	Rejected     int           `json:"rejected"`
	NewSeries    int           `json:"new_series"`
	Watermark    int64         `json:"watermark"`
	HasWatermark bool          `json:"has_watermark"`
	Results      []PointResult `json:"results,omitempty"`
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
