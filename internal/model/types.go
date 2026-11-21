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

// Coverage is the half-open range from the first to the last visible point of
// a previewed series. It is narrower than, or equal to, the requested interval.
type Coverage struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type PreviewSeries struct {
	Series   Series    `json:"series"`
	Visible  int       `json:"visible"`
	Coverage *Coverage `json:"coverage,omitempty"`
}

// PreviewPoint is the persisted visibility reference for one sample. Sequence
// distinguishes a same-timestamp correction from the point the operator saw.
type PreviewPoint struct {
	Time     int64  `json:"time"`
	Sequence uint64 `json:"sequence"`
}

type DriftSeries struct {
	Series        Series  `json:"series"`
	Added         int     `json:"added"`
	Removed       int     `json:"removed"`
	Replaced      int     `json:"replaced"`
	AddedTimes    []int64 `json:"added_times,omitempty"`
	RemovedTimes  []int64 `json:"removed_times,omitempty"`
	ReplacedTimes []int64 `json:"replaced_times,omitempty"`
}

type NewDeletion struct {
	ID             string `json:"id"`
	Sequence       uint64 `json:"sequence"`
	AffectsPreview bool   `json:"affects_preview"`
}

type PreviewDrift struct {
	Invalidated       bool            `json:"invalidated"`
	Reasons           []string        `json:"reasons"`
	Notices           []string        `json:"notices"`
	Series            []DriftSeries   `json:"series"`
	NewSeries         []PreviewSeries `json:"new_series"`
	NewDeletions      []NewDeletion   `json:"new_deletions"`
	NamespaceSequence uint64          `json:"namespace_sequence"`
	Floor             int64           `json:"floor"`
}

type PreviewSnapshot struct {
	ID          string                    `json:"id"`
	Start       int64                     `json:"start"`
	End         int64                     `json:"end"`
	Matchers    []Matcher                 `json:"matchers"`
	Fingerprint string                    `json:"fingerprint"`
	Sequence    uint64                    `json:"sequence"`
	Floor       int64                     `json:"floor"`
	Series      []PreviewSeries           `json:"series"`
	Points      map[string][]PreviewPoint `json:"points"`
	Tombstones  []string                  `json:"tombstones"`
	Cancelled   bool                      `json:"cancelled"`
	TombstoneID string                    `json:"tombstone_id,omitempty"`
	ConfirmSeq  uint64                    `json:"confirm_sequence,omitempty"`
}

type PreviewView struct {
	ID                string          `json:"id"`
	Start             int64           `json:"start"`
	End               int64           `json:"end"`
	Matchers          []Matcher       `json:"matchers"`
	Fingerprint       string          `json:"fingerprint"`
	NamespaceSequence uint64          `json:"namespace_sequence"`
	Floor             int64           `json:"floor"`
	Series            []PreviewSeries `json:"series"`
	Visible           int             `json:"visible"`
	State             string          `json:"state"`
	TombstoneID       string          `json:"tombstone_id,omitempty"`
	ConfirmSequence   uint64          `json:"confirm_sequence,omitempty"`
	Drift             *PreviewDrift   `json:"drift,omitempty"`
}

type DeleteConfirmation struct {
	Preview     string    `json:"preview"`
	Fingerprint string    `json:"fingerprint"`
	Tombstone   Tombstone `json:"tombstone"`
	Hidden      int       `json:"hidden"`
	Notices     []string  `json:"notices,omitempty"`
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
