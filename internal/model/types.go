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

// RetentionSeriesImpact is one series whose currently visible points fall
// below the floor a retention preview would advance to.
type RetentionSeriesImpact struct {
	Series  Series `json:"series"`
	Removed int    `json:"removed"`
	Kept    int    `json:"kept"`
}

// RetentionRollupImpact describes a stored materialization whose definition
// overlaps the interval a retention preview would drop.
type RetentionRollupImpact struct {
	Name      string `json:"name"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Overlaps  bool   `json:"overlaps"`
	Stale     bool   `json:"stale"`
	Remaining int    `json:"remaining"`
}

// RetentionPreview is the read-only effect estimate of advancing retention.
type RetentionPreview struct {
	Namespace       string                  `json:"namespace"`
	Now             int64                   `json:"now"`
	CurrentFloor    int64                   `json:"current_floor"`
	ProposedFloor   int64                   `json:"proposed_floor"`
	Advances        bool                    `json:"advances"`
	SourceSequence  uint64                  `json:"source_sequence"`
	AffectedPoints  int                     `json:"affected_points"`
	VisiblePoints   int                     `json:"visible_points"`
	Series          []RetentionSeriesImpact `json:"series"`
	Rollups         []RetentionRollupImpact `json:"rollups"`
	Blocks          int                     `json:"blocks"`
	HeadRecords     int                     `json:"head_records"`
	Decoded         int                     `json:"decoded"`
	MaxScan         int                     `json:"max_scan"`
	BudgetExhausted bool                    `json:"budget_exhausted"`
	Token           string                  `json:"token"`
}

// RetentionResult is the receipt of a confirmed retention advancement.
type RetentionResult struct {
	Maintenance
	Removed        int  `json:"removed"`
	AffectedSeries int  `json:"affected_series"`
	NoOp           bool `json:"no_op"`
	StalePreview   bool `json:"stale_preview"`
}

// RetentionConfirm carries the preview token an operator wants to commit.
type RetentionConfirm struct {
	Token   string `json:"token"`
	OnStale string `json:"on_stale"`
}

// RetentionToken is the signed, stateless snapshot embedded in a preview.
type RetentionToken struct {
	Namespace       string `json:"namespace"`
	Now             int64  `json:"now"`
	CurrentFloor    int64  `json:"current_floor"`
	ProposedFloor   int64  `json:"proposed_floor"`
	SourceSequence  uint64 `json:"source_sequence"`
	RollupSignature string `json:"rollup_signature"`
	Digest          string `json:"digest"`
}
