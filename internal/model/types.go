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
	// Width selects sliding output: when positive each aligned output time t
	// reports [t-Width, t). A zero width keeps tumbling [bucket, bucket+step).
	Width int64 `json:"width,omitempty"`
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

// SlidingStats describes the bounded reduction work of a sliding aggregation.
// Observations is the source point count read once; Memberships is the number
// of (observation, output) pairs the windows would contain if counted naively;
// Reductions is the actual reduction work (equal to Observations).
type SlidingStats struct {
	Width        int64 `json:"width"`
	Step         int64 `json:"step"`
	Groups       int   `json:"groups"`
	Outputs      int   `json:"outputs"`
	Observations int64 `json:"observations"`
	Memberships  int64 `json:"memberships"`
	Reductions   int64 `json:"reductions"`
	MaxReuse     int64 `json:"max_reuse"`
	EmptyOutputs int   `json:"empty_outputs"`
	GapCells     int   `json:"gap_cells"`
	Cells        int   `json:"cells"`
}

// SlidingBounds previews the aligned output and source spans of a sliding
// aggregate, including the pre-roll the first window may read.
type SlidingBounds struct {
	Width        int64 `json:"width"`
	Step         int64 `json:"step"`
	FirstOutput  int64 `json:"first_output"`
	LastOutput   int64 `json:"last_output"`
	Outputs      int64 `json:"outputs"`
	ReadStart    int64 `json:"read_start"`
	ReadEnd      int64 `json:"read_end"`
	SourcePoints int64 `json:"source_points"`
}

type AggregateResult struct {
	Sequence uint64        `json:"sequence"`
	Cells    []Cell        `json:"cells"`
	Stats    ScanStats     `json:"stats"`
	Sliding  *SlidingStats `json:"sliding,omitempty"`
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
