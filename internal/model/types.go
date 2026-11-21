package model

type Labels map[string]string

type Namespace struct {
	Name           string `json:"name"`
	ReadOnly       bool   `json:"read_only"`
	RetentionMS    int64  `json:"retention_ms"`
	LatenessMS     int64  `json:"lateness_ms"`
	FutureMS       int64  `json:"future_ms"`
	MaxSeries      int    `json:"max_series"`
	MaxWrite       int    `json:"max_write"`
	MaxScan        int    `json:"max_scan"`
	Floor          int64  `json:"floor"`
	Watermark      int64  `json:"watermark"`
	HasWatermark   bool   `json:"has_watermark"`
	LastSequence   uint64 `json:"last_sequence"`
	Maintenance    string `json:"maintenance,omitempty"`
	MaintenanceJob string `json:"maintenance_job,omitempty"`
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
	ID           string   `json:"id"`
	Series       []string `json:"series"`
	Start        int64    `json:"start"`
	End          int64    `json:"end"`
	Sequence     uint64   `json:"sequence"`
	Resolved     bool     `json:"resolved"`
	Job          string   `json:"job,omitempty"`
	OriginSeries []string `json:"origin_series,omitempty"`
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

// RelabelRename names one label key that should be renamed inside a namespace.
type RelabelRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// RelabelRequest is the preview/execute contract for a scoped label rename.
// Empty policies select the conservative defaults, which reject the job.
type RelabelRequest struct {
	Rename         RelabelRename `json:"rename"`
	Job            string        `json:"job"`
	ValueConflict  string        `json:"value_conflict"`
	LabelConflict  string        `json:"label_conflict"`
	SourceSequence uint64        `json:"source_sequence"`
}

type RelabelSummary struct {
	SeriesAnalyzed   int `json:"series_analyzed"`
	SeriesRenamed    int `json:"series_renamed"`
	SeriesUnchanged  int `json:"series_unchanged"`
	SeriesMissing    int `json:"series_missing_label"`
	SeriesEmpty      int `json:"series_empty_label"`
	SeriesMerged     int `json:"series_merged"`
	MergeGroups      int `json:"merge_groups"`
	SourcePoints     int `json:"source_points"`
	ResultPoints     int `json:"result_points"`
	Collisions       int `json:"collisions"`
	LabelConflicts   int `json:"label_conflicts"`
	Deletions        int `json:"deletions"`
	DeletionsRewrote int `json:"deletions_rewritten"`
	Rollups          int `json:"rollups"`
	RollupsRewrote   int `json:"rollups_rewritten"`
}

type SeriesIdentityChange struct {
	SeriesID    string `json:"series_id"`
	NewSeriesID string `json:"new_series_id"`
	Labels      Labels `json:"labels"`
	NewLabels   Labels `json:"new_labels"`
	Points      int    `json:"points"`
}

type MergeGroup struct {
	NewSeriesID string   `json:"new_series_id"`
	NewLabels   Labels   `json:"new_labels"`
	SeriesIDs   []string `json:"series_ids"`
	Points      int      `json:"points"`
	Collisions  int      `json:"collisions"`
}

type CollisionSample struct {
	SeriesID string  `json:"series_id"`
	Value    float64 `json:"value"`
	Sequence uint64  `json:"sequence"`
}

type RelabelCollision struct {
	NewSeriesID string            `json:"new_series_id"`
	Time        int64             `json:"time"`
	Candidates  []CollisionSample `json:"candidates"`
	Chosen      string            `json:"chosen"`
	Conflicting bool              `json:"conflicting"`
}

type LabelConflictDetail struct {
	SeriesID  string `json:"series_id"`
	FromValue string `json:"from_value"`
	ToValue   string `json:"to_value"`
	Resolved  string `json:"resolved"`
}

type RollupImpact struct {
	Name                string `json:"name"`
	MatchersRewritten   int    `json:"matchers_rewritten"`
	GroupByRewritten    int    `json:"group_by_rewritten"`
	CellLabelsRewritten int    `json:"cell_labels_rewritten"`
	CellLabelConflicts  int    `json:"cell_label_conflicts"`
}

type RelabelPlan struct {
	Namespace      string                 `json:"namespace"`
	SourceSequence uint64                 `json:"source_sequence"`
	Rename         RelabelRename          `json:"rename"`
	ValueConflict  string                 `json:"value_conflict"`
	LabelConflict  string                 `json:"label_conflict"`
	WorkNeeded     bool                   `json:"work_needed"`
	Blocked        bool                   `json:"blocked"`
	Reasons        []string               `json:"reasons"`
	Summary        RelabelSummary         `json:"summary"`
	Changes        []SeriesIdentityChange `json:"changes"`
	Merges         []MergeGroup           `json:"merges"`
	Collisions     []RelabelCollision     `json:"collisions"`
	LabelConflicts []LabelConflictDetail  `json:"label_conflicts"`
	Rollups        []RollupImpact         `json:"rollups"`
}

type RelabelJob struct {
	ID              string         `json:"id"`
	State           string         `json:"state"`
	Completed       bool           `json:"completed"`
	Rename          RelabelRename  `json:"rename"`
	ValueConflict   string         `json:"value_conflict"`
	LabelConflict   string         `json:"label_conflict"`
	StartedSequence uint64         `json:"started_sequence"`
	SourceSequence  uint64         `json:"source_sequence"`
	ResultSequence  uint64         `json:"result_sequence"`
	Summary         RelabelSummary `json:"summary"`
	Detail          string         `json:"detail,omitempty"`
}

type RelabelJobs struct {
	Jobs []RelabelJob `json:"jobs"`
}
