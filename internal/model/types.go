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

// Alternative is a proposed narrower time range and/or matcher set for the
// same namespace. Explain projects its decode cost without decoding samples,
// so an operator can compare it with the current request before retrying.
type Alternative struct {
	Label    string    `json:"label"`
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	Matchers []Matcher `json:"matchers"`
}

// ExplainRequest keeps the query contract and adds optional narrower ranges to
// project. Other routes decode Query directly, so they reject these fields.
type ExplainRequest struct {
	Query
	Alternatives []Alternative `json:"alternatives"`
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
	Evidence  *ScanEvidence  `json:"evidence,omitempty"`
}

type Cell struct {
	Labels Labels  `json:"labels"`
	Start  int64   `json:"start"`
	End    int64   `json:"end"`
	Value  float64 `json:"value"`
	Count  int     `json:"count"`
}

type AggregateResult struct {
	Sequence uint64        `json:"sequence"`
	Cells    []Cell        `json:"cells"`
	Stats    ScanStats     `json:"stats"`
	Evidence *ScanEvidence `json:"evidence,omitempty"`
}

// PlanTotals describes physical work in one read view without decoding a
// sample payload. Block points are exact decode costs. Head records are exact
// key counts inside the requested interval for candidate series. Visible
// counts are only exact where no version overlap, partial block, deletion or
// retention cutoff leaves a sample uncertain.
type PlanTotals struct {
	Series                 int `json:"series"`
	Blocks                 int `json:"blocks"`
	BlocksContained        int `json:"blocks_contained"`
	BlocksPartial          int `json:"blocks_partial"`
	BlockPoints            int `json:"block_points"`
	ContainedBlockPoints   int `json:"contained_block_points"`
	StraddlingBlockPoints  int `json:"straddling_block_points"`
	HeadRecords            int `json:"head_records"`
	HeadRecordsNamespace   int `json:"head_records_namespace"`
	HeadRecordsOverlapping int `json:"head_records_overlapping_blocks"`
	DecodePoints           int `json:"decode_points"`
	// SeriesWithSources is how many candidates actually cost anything.
	SeriesWithSources        int `json:"series_with_sources"`
	SeriesWithoutSources     int `json:"series_without_sources"`
	SeriesWithVersionOverlap int `json:"series_with_version_overlap"`
	SeriesWithDeletions      int `json:"series_with_deletions"`
	SeriesWithRetentionFloor int `json:"series_with_retention_floor"`
	VisibleUpperBound        int `json:"visible_upper_bound"`
	CertainVisiblePoints     int `json:"certain_visible_points"`
	CertainVisibleSeries     int `json:"certain_visible_series"`
}

// ScanBudget reports source work from the read view that produced a result or
// rejected it. RejectedSource names the first source that could not fit.
type ScanBudget struct {
	Limit          int             `json:"limit"`
	Used           int             `json:"used"`
	Remaining      int             `json:"remaining"`
	Within         bool            `json:"within"`
	RejectedSource *RejectedSource `json:"rejected_source,omitempty"`
}

type RejectedSource struct {
	Series    string `json:"series"`
	Kind      string `json:"kind"`
	Block     string `json:"block,omitempty"`
	Need      int    `json:"need"`
	Remaining int    `json:"remaining"`
}

// ObservedEvidence aggregates only series whose complete source set was read.
// A rejected query must not project these counters over the whole request.
type ObservedEvidence struct {
	Scope          string `json:"scope"`
	Series         int    `json:"series"`
	Blocks         int    `json:"blocks"`
	BlockPoints    int    `json:"block_points"`
	HeadRecords    int    `json:"head_records"`
	PhysicalPoints int    `json:"physical_points"`
	InRangePoints  int    `json:"in_range_points"`
	Superseded     int    `json:"superseded"`
	Hidden         int    `json:"hidden"`
	Visible        int    `json:"visible"`
}

// PartialEvidence is the partly read series that crossed the budget. Its merge
// and visibility state are incomplete and intentionally not reported.
type PartialEvidence struct {
	Series          string `json:"series"`
	Stage           string `json:"stage"`
	BlocksDecoded   int    `json:"blocks_decoded"`
	BlockPointsRead int    `json:"block_points_read"`
	HeadRecordsRead int    `json:"head_records_read"`
	PhysicalRead    int    `json:"physical_read"`
	// PlannedDecode and UnreadDecode expose how much of this series the plan
	// expected and how much was never read. Neither is visible-row evidence.
	PlannedDecode int `json:"planned_decode"`
	UnreadDecode  int `json:"unread_decode"`
}

// UnscannedEvidence is plan-only metadata for series not reached yet.
type UnscannedEvidence struct {
	Series              int  `json:"series"`
	PlannedDecodePoints int  `json:"planned_decode_points"`
	VisibleKnown        bool `json:"visible_known"`
}

// Reconciliation compares the plan-phase projection with what the read view
// actually measured. A mismatch is itself diagnostic: it means the view
// changed, or the read stopped before covering the plan.
type Reconciliation struct {
	PlannedDecode  int    `json:"planned_decode"`
	ObservedSource int    `json:"observed_source"`
	Matches        bool   `json:"matches"`
	Verdict        string `json:"verdict"`
}

// ScanEvidence accompanies a completed read and every capacity rejection.
// Outcome "complete" means every candidate source was read and the observed
// counters describe the whole request. Outcome "rejected" means only
// Observed (fully read series) and Partial (the interrupted series) are
// measured; Unscanned and Planned cost describe the remainder.
type ScanEvidence struct {
	Sequence  uint64             `json:"sequence"`
	Floor     int64              `json:"floor"`
	Outcome   string             `json:"outcome"`
	Complete  bool               `json:"complete"`
	Truncated bool               `json:"truncated"`
	Budget    ScanBudget         `json:"budget"`
	Planned   PlanTotals         `json:"planned"`
	Observed  *ObservedEvidence  `json:"observed"`
	Partial   *PartialEvidence   `json:"partial,omitempty"`
	Unscanned *UnscannedEvidence `json:"unscanned,omitempty"`
	Reconcile Reconciliation     `json:"reconcile"`
	Caveats   []string           `json:"caveats,omitempty"`
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
