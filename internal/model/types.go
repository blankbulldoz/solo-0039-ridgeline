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

// As-of join directions. Backward only selects neighbor points at or before
// the left observation, forward only at or after it, and nearest considers
// both sides.
const (
	AsOfBackward = "backward"
	AsOfForward  = "forward"
	AsOfNearest  = "nearest"
)

type AsOfQuery struct {
	Start         int64     `json:"start"`
	End           int64     `json:"end"`
	LeftMatchers  []Matcher `json:"left_matchers"`
	RightMatchers []Matcher `json:"right_matchers"`
	PairLabel     string    `json:"pair_label"`
	Direction     string    `json:"direction"`
	// MaxDistanceMS zero means no distance bound; a positive value rejects
	// neighbors farther than the bound, the boundary itself being accepted.
	MaxDistanceMS int64 `json:"max_distance_ms"`
	Limit         int   `json:"limit"`
}

// AsOfPoint preserves the identity of the original sample that was selected.
type AsOfPoint struct {
	Time     int64   `json:"time"`
	Value    float64 `json:"value"`
	SeriesID string  `json:"series_id"`
	Sequence uint64  `json:"sequence"`
}

type AsOfPair struct {
	PairKey     string     `json:"pair_key"`
	LeftSeries  Series     `json:"left_series"`
	RightSeries *Series    `json:"right_series"`
	Left        AsOfPoint  `json:"left"`
	Right       *AsOfPoint `json:"right"`
	DistanceMS  int64      `json:"distance_ms"`
	Matched     bool       `json:"matched"`
}

type AsOfStats struct {
	Left  ScanStats `json:"left"`
	Right ScanStats `json:"right"`
}

type AsOfResult struct {
	Sequence  uint64     `json:"sequence"`
	Floor     int64      `json:"floor"`
	PairLabel string     `json:"pair_label"`
	Direction string     `json:"direction"`
	Pairs     []AsOfPair `json:"pairs"`
	Stats     AsOfStats  `json:"stats"`
	Truncated bool       `json:"truncated"`
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
