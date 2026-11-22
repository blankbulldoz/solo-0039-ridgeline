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
	// TrackedFrom is the source sequence from which this namespace's logical
	// change journal is complete: every logical mutation committed after it is
	// recorded. A materialization sourced at or after TrackedFrom can be judged
	// precisely; an older one cannot and is reported as indeterminate.
	TrackedFrom    uint64 `json:"tracked_from,omitempty"`
	JournalTracked bool   `json:"journal_tracked,omitempty"`
	JournalCount   int    `json:"journal_count,omitempty"`
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

// MaterializationProvenance identifies materializations written with enough
// metadata to judge freshness precisely. Records without it predate change
// journaling and are reported as indeterminate rather than fresh.
const MaterializationProvenance = 1

type Materialization struct {
	Name           string          `json:"name"`
	Query          Query           `json:"query"`
	SourceSequence uint64          `json:"source_sequence"`
	Provenance     int             `json:"provenance,omitempty"`
	Result         AggregateResult `json:"result"`
}

// TimeRun is a half-open interval of timestamps touched by a logical change.
type TimeRun struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// SeriesTouch associates one series with the time runs a mutation touched.
type SeriesTouch struct {
	Series string    `json:"series"`
	Runs   []TimeRun `json:"runs"`
}

// Change is one logical mutation recorded for rollup freshness evaluation.
// Physical movement (seal, compaction) and policy-only transitions are not
// recorded because they cannot alter a query result.
type Change struct {
	Sequence uint64        `json:"sequence"`
	Kind     string        `json:"kind"`
	Touches  []SeriesTouch `json:"touches,omitempty"`
	// PreviousFloor and Floor are set for retention changes.
	PreviousFloor int64 `json:"previous_floor,omitempty"`
	Floor         int64 `json:"floor,omitempty"`
}

const (
	ChangeWrite     = "write"
	ChangeDelete    = "delete"
	ChangeRetention = "retention"

	FreshnessFresh         = "fresh"
	FreshnessStale         = "stale"
	FreshnessIndeterminate = "indeterminate"
)

type RollupView struct {
	Materialization
	// Stale is conservative: an indeterminate materialization also reports
	// true so a client never treats an unverifiable result as current.
	Stale bool `json:"stale"`
	// Freshness is "fresh", "stale" or "indeterminate".
	Freshness string `json:"freshness"`
	// Reason explains a stale or indeterminate verdict.
	Reason string `json:"reason,omitempty"`
}

type Maintenance struct {
	Sequence uint64 `json:"sequence"`
	Blocks   int    `json:"blocks"`
	Points   int    `json:"points"`
	Floor    int64  `json:"floor"`
}
