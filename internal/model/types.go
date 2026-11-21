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

// RollupSource names the rollup whose stored reduction state seeds a derived
// tier.
type RollupSource struct {
	Rollup string `json:"rollup"`
}

// RollupDefinition is the public request for materializing a rollup tier.
//
// Without Source it aggregates raw points at Step. With Source it reduces the
// stored reduction state of an existing rollup instead of re-reading raw
// points. A derived tier must use the same Origin and a Step that is an
// integer multiple of the source step; its Function may be any of count, sum,
// avg, min or max because all five are derived from the same count/sum/min/max
// state.
type RollupDefinition struct {
	Start    int64         `json:"start"`
	End      int64         `json:"end"`
	Matchers []Matcher     `json:"matchers"`
	Limit    int           `json:"limit"`
	Step     int64         `json:"step"`
	Origin   int64         `json:"origin"`
	GroupBy  []string      `json:"group_by"`
	Function string        `json:"function"`
	Source   *RollupSource `json:"source,omitempty"`
}

func (d RollupDefinition) Query() Query {
	return Query{
		Start: d.Start, End: d.End, Matchers: d.Matchers, Limit: d.Limit,
		Step: d.Step, Origin: d.Origin, GroupBy: d.GroupBy, Function: d.Function,
	}
}

// Reduction is the lossless per-window state needed to derive a coarser tier.
// Average is always sum/count, never an average of child averages.
type Reduction struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// CellCoverage describes one rollup cell's observation completeness.
//
// ExpectedParts is the number of source-tier windows that tile this cell (1
// for a tier built from raw points). PresentParts counts source windows that
// actually contributed samples; a window with no samples inside covered time
// means no observations were recorded, not missing data, so a cell can still
// be complete when a tiling window held nothing. PartialStart/PartialEnd mark a
// window that extends outside the interval the tier observed, either because
// the caller's range was not aligned to the step or because the retention
// floor clipped it. Exact means the stored value equaled the full-window
// aggregation at the time it was materialized.
//
// SourceExpired and Recomputable are refreshed against the current retention
// floor each time a rollup is read: SourceExpired reports that raw points
// behind this window have been purged, and Recomputable reports that the
// window can still be rebuilt exactly from currently retained raw source.
// Rule is the per-cell reading rule derived from these facts.
type CellCoverage struct {
	ExpectedParts int    `json:"expected_parts"`
	PresentParts  int    `json:"present_parts"`
	Complete      bool   `json:"complete"`
	PartialStart  bool   `json:"partial_start"`
	PartialEnd    bool   `json:"partial_end"`
	SourceExpired bool   `json:"source_expired"`
	Exact         bool   `json:"exact"`
	Recomputable  bool   `json:"recomputable"`
	Rule          string `json:"rule"`
}

// RollupCoverage is the interval a materialization fully observed. Start and
// End are window boundaries; windows outside them are partial. Floor is the
// namespace retention floor at materialization time.
type RollupCoverage struct {
	Start    int64 `json:"start"`
	End      int64 `json:"end"`
	Floor    int64 `json:"floor"`
	Complete bool  `json:"complete"`
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
	Labels    Labels        `json:"labels"`
	Start     int64         `json:"start"`
	End       int64         `json:"end"`
	Value     float64       `json:"value"`
	Count     int           `json:"count"`
	Reduction *Reduction    `json:"reduction,omitempty"`
	Coverage  *CellCoverage `json:"coverage,omitempty"`
}

type AggregateResult struct {
	Sequence uint64          `json:"sequence"`
	Cells    []Cell          `json:"cells"`
	Stats    ScanStats       `json:"stats"`
	Coverage *RollupCoverage `json:"coverage,omitempty"`
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
	Name           string            `json:"name"`
	Query          Query             `json:"query"`
	SourceSequence uint64            `json:"source_sequence"`
	Result         AggregateResult   `json:"result"`
	DerivedFrom    string            `json:"derived_from,omitempty"`
	SourceRollup   *RollupProvenance `json:"source_rollup,omitempty"`
}

// RollupProvenance records that a materialization was reduced from another
// rollup rather than from raw points.
type RollupProvenance struct {
	Name           string          `json:"name"`
	Step           int64           `json:"step"`
	Origin         int64           `json:"origin"`
	SourceSequence uint64          `json:"source_sequence"`
	Coverage       *RollupCoverage `json:"coverage,omitempty"`
}

type RollupView struct {
	Materialization
	Stale          bool              `json:"stale"`
	SourceStale    bool              `json:"source_stale"`
	ReadRule       string            `json:"read_rule"`
	SourceReadRule string            `json:"source_read_rule,omitempty"`
	Derivable      []string          `json:"derivable,omitempty"`
	Suggestion     *RollupSuggestion `json:"suggestion,omitempty"`
}

// RollupSuggestion recommends a granularity that is both aligned and currently
// retained, so a caller can rebuild this tier exactly.
type RollupSuggestion struct {
	Step   int64  `json:"step"`
	Reason string `json:"reason"`
}

type RollupSummary struct {
	Name         string          `json:"name"`
	Step         int64           `json:"step"`
	Function     string          `json:"function"`
	Origin       int64           `json:"origin"`
	GroupBy      []string        `json:"group_by"`
	DerivedFrom  string          `json:"derived_from,omitempty"`
	Coverage     *RollupCoverage `json:"coverage,omitempty"`
	Cells        int             `json:"cells"`
	Stale        bool            `json:"stale"`
	SourceStale  bool            `json:"source_stale"`
	ReadRule     string          `json:"read_rule"`
	ExactCells   int             `json:"exact_cells"`
	PartialCells int             `json:"partial_cells"`
	ExpiredCells int             `json:"expired_cells"`
	// Derivable lists functions this tier can expose from its stored state
	// without re-reading raw points.
	Derivable []string `json:"derivable,omitempty"`
}

type Maintenance struct {
	Sequence uint64 `json:"sequence"`
	Blocks   int    `json:"blocks"`
	Points   int    `json:"points"`
	Floor    int64  `json:"floor"`
}
