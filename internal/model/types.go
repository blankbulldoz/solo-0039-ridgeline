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

// HistogramObservation is one cumulative histogram snapshot: strictly
// increasing bucket upper bounds, cumulative counts over those buckets and
// the cumulative sum of every observed value. Counts reset to smaller
// values only when the emitting counter restarts.
type HistogramObservation struct {
	Labels Labels    `json:"labels"`
	Time   int64     `json:"time"`
	Bounds []float64 `json:"bounds"`
	Counts []uint64  `json:"counts"`
	Sum    float64   `json:"sum"`
}

type HistogramWriteRequest struct {
	Now        int64                  `json:"now"`
	Histograms []HistogramObservation `json:"histograms"`
}

type HistogramSample struct {
	Time     int64     `json:"time"`
	Bounds   []float64 `json:"bounds"`
	Counts   []uint64  `json:"counts"`
	Sum      float64   `json:"sum"`
	Sequence uint64    `json:"sequence"`
}

type HistogramQuery struct {
	Start     int64     `json:"start"`
	End       int64     `json:"end"`
	Step      int64     `json:"step"`
	Origin    int64     `json:"origin"`
	Quantiles []float64 `json:"quantiles"`
	GroupBy   []string  `json:"group_by"`
	Matchers  []Matcher `json:"matchers"`
}

type QuantileEstimate struct {
	Quantile float64  `json:"quantile"`
	Value    *float64 `json:"value"`
	Capped   bool     `json:"capped"`
}

type SchemePartial struct {
	Fingerprint string    `json:"scheme"`
	Bounds      []float64 `json:"bounds"`
	Counts      []uint64  `json:"counts"`
	Count       uint64    `json:"count"`
	Sum         float64   `json:"sum"`
	Series      int       `json:"series"`
}

type HistogramCell struct {
	Labels      Labels             `json:"labels"`
	Start       int64              `json:"start"`
	End         int64              `json:"end"`
	Compatible  bool               `json:"compatible"`
	Fingerprint string             `json:"scheme,omitempty"`
	Bounds      []float64          `json:"bounds,omitempty"`
	Counts      []uint64           `json:"counts,omitempty"`
	Count       uint64             `json:"count"`
	Sum         float64            `json:"sum"`
	Series      int                `json:"series"`
	Resets      int                `json:"resets"`
	Quantiles   []QuantileEstimate `json:"quantiles"`
	Schemes     []SchemePartial    `json:"schemes,omitempty"`
	Limitations []string           `json:"limitations"`
}

type SeriesHistograms struct {
	Series Series            `json:"series"`
	Points []HistogramSample `json:"points"`
}

type HistogramResult struct {
	Sequence uint64          `json:"sequence"`
	Floor    int64           `json:"floor"`
	Cells    []HistogramCell `json:"cells"`
	Stats    ScanStats       `json:"stats"`
}

type HistogramRangeResult struct {
	Sequence uint64             `json:"sequence"`
	Floor    int64              `json:"floor"`
	Series   []SeriesHistograms `json:"series"`
	Stats    ScanStats          `json:"stats"`
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
	Sequence       uint64 `json:"sequence"`
	Blocks         int    `json:"blocks"`
	Points         int    `json:"points"`
	HistogramBlock int    `json:"histogram_blocks"`
	Histograms     int    `json:"histograms"`
	Floor          int64  `json:"floor"`
}
