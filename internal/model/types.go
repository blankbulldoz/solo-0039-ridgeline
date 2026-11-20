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

type VectorSelector struct {
	Matchers []Matcher `json:"matchers"`
}

type VectorQuery struct {
	Start        int64           `json:"start"`
	End          int64           `json:"end"`
	Left         *VectorSelector `json:"left"`
	Right        *VectorSelector `json:"right"`
	On           []string        `json:"on"`
	Op           string          `json:"op"`
	Relationship string          `json:"relationship"`
}

const (
	VectorOK              = "ok"
	VectorMissingLeft     = "missing_left"
	VectorMissingRight    = "missing_right"
	VectorZeroDenominator = "zero_denominator"
)

// VectorPoint is one aligned observation of a vector expression. Left, Right
// and Value are null whenever the corresponding side is absent or the
// arithmetic result is undefined; a missing operand never becomes zero.
type VectorPoint struct {
	Key         Labels   `json:"key"`
	Time        int64    `json:"time"`
	LeftSeries  string   `json:"left_series"`
	RightSeries string   `json:"right_series"`
	Left        *float64 `json:"left"`
	Right       *float64 `json:"right"`
	Value       *float64 `json:"value"`
	Status      string   `json:"status"`
}

type VectorStats struct {
	LeftSeries      int `json:"left_series"`
	RightSeries     int `json:"right_series"`
	JoinedKeys      int `json:"joined_keys"`
	LeftOnlyKeys    int `json:"left_only_keys"`
	RightOnlyKeys   int `json:"right_only_keys"`
	MisalignedKeys  int `json:"misaligned_keys"`
	Rows            int `json:"rows"`
	OK              int `json:"ok"`
	MissingLeft     int `json:"missing_left"`
	MissingRight    int `json:"missing_right"`
	ZeroDenominator int `json:"zero_denominator"`
	Decoded         int `json:"decoded"`
}

type VectorResult struct {
	Sequence     uint64        `json:"sequence"`
	Op           string        `json:"op"`
	On           []string      `json:"on"`
	Relationship string        `json:"relationship"`
	Points       []VectorPoint `json:"points"`
	Stats        VectorStats   `json:"stats"`
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
