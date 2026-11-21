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
	// Partial opts a read into per-series result reporting. It is off by
	// default so a single unreadable physical block still fails the request.
	Partial bool `json:"partial,omitempty"`
}

// Read statuses reported per selected series when a partial read is allowed.
const (
	ReadComplete    = "complete"
	ReadFailed      = "failed"
	ReadNotExecuted = "not_executed"
)

type SeriesPoints struct {
	Series Series   `json:"series"`
	Points []Sample `json:"points"`
	// Status is only set by partial reads. A non-complete status always has
	// an empty Points slice: a partial read never emits part of a series.
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

// SeriesStatus reports one selected series' disposition without repeating its
// points. Aggregate uses it because cells are grouped, not series shaped.
type SeriesStatus struct {
	Series Series `json:"series"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// GroupIssue identifies a grouping key whose aggregate is withheld because at
// least one contributing series could not be read completely.
type GroupIssue struct {
	Labels   Labels   `json:"labels"`
	Status   string   `json:"status"`
	SeriesID []string `json:"series_ids"`
}

type ScanStats struct {
	Series      int `json:"series"`
	Blocks      int `json:"blocks"`
	Head        int `json:"head"`
	Decoded     int `json:"decoded"`
	Visible     int `json:"visible"`
	Tombstones  int `json:"tombstones"`
	Failed      int `json:"failed,omitempty"`
	NotExecuted int `json:"not_executed,omitempty"`
}

type RangeResult struct {
	Sequence  uint64         `json:"sequence"`
	Floor     int64          `json:"floor"`
	Series    []SeriesPoints `json:"series"`
	Stats     ScanStats      `json:"stats"`
	Truncated bool           `json:"truncated"`
	// Partial is set only when some series were not read completely. It is
	// never set merely because the request asked for a partial read.
	Partial bool `json:"partial,omitempty"`
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
	// Partial marks a result that withheld some series or groups because their
	// sources could not be read completely. Cells never mix a withheld series.
	Partial bool `json:"partial,omitempty"`
	// Withheld lists grouping keys omitted from Cells. Each carries the status
	// of the contributing series that caused it.
	Withheld []GroupIssue `json:"withheld,omitempty"`
	// Series reports per-selected-series disposition in a partial read.
	Series []SeriesStatus `json:"series,omitempty"`
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
