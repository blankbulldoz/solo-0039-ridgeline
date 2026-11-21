package engine

import (
	"bytes"
	"encoding/json"
	"sort"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func readBlocks(b *bolt.Bucket) ([]model.Block, error) {
	out := make([]model.Block, 0)
	err := b.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
		var block model.Block
		if err := json.Unmarshal(value, &block); err != nil {
			return err
		}
		out = append(out, block)
		return nil
	})
	return out, err
}

func scan(b *bolt.Bucket, n model.Namespace, q model.Query, sequence uint64) (model.RangeResult, error) {
	result := model.RangeResult{Sequence: sequence, Floor: n.Floor, Series: []model.SeriesPoints{}}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return result, err
	}
	include, err := model.NewQualitySet(q.IncludeQualities)
	if err != nil {
		return result, err
	}
	// Raw reads report quality when asked explicitly or when they filter by it.
	reportQuality := q.ReportQuality || len(q.IncludeQualities) > 0
	excluded := model.QualityTally{}
	allSeries, err := catalog(b)
	if err != nil {
		return result, err
	}
	blocks, err := readBlocks(b)
	if err != nil {
		return result, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return result, err
	}
	result.Stats.Tombstones = len(tombstones)
	selected := selectseries.Pick(allSeries, selector)
	result.Stats.Series = len(selected)
	emitted := 0
	for _, s := range selected {
		merged := map[int64]model.Sample{}
		for _, block := range blocks {
			if block.SeriesID != s.ID || block.End <= q.Start ||
				block.Start >= q.End || block.End <= n.Floor {
				continue
			}
			if result.Stats.Decoded+block.Count > n.MaxScan {
				return result, model.Capacity("query decoded point budget exceeded")
			}
			points, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
			if err != nil {
				return result, err
			}
			result.Stats.Blocks++
			result.Stats.Decoded += len(points)
			for _, p := range points {
				if p.Time < q.Start || p.Time >= q.End || p.Time < n.Floor {
					continue
				}
				old, ok := merged[p.Time]
				if !ok || p.Sequence > old.Sequence {
					merged[p.Time] = p
				}
			}
		}
		cursor := b.Bucket(volume.Head).Cursor()
		for k, v := cursor.Seek(volume.PointKey(s.ID, q.Start)); k != nil && bytes.HasPrefix(k, []byte(s.ID)); k, v = cursor.Next() {
			timestamp, err := volume.PointTime(k)
			if err != nil {
				return result, err
			}
			if timestamp >= q.End {
				break
			}
			result.Stats.Head++
			result.Stats.Decoded++
			if result.Stats.Decoded > n.MaxScan {
				return result, model.Capacity("query decoded point budget exceeded")
			}
			var p model.Sample
			if err := json.Unmarshal(v, &p); err != nil {
				return result, err
			}
			if p.Time < n.Floor {
				continue
			}
			old, ok := merged[p.Time]
			if !ok || p.Sequence >= old.Sequence {
				merged[p.Time] = p
			}
		}
		points := make([]model.Sample, 0, len(merged))
		var tally model.QualityTally
		for _, p := range merged {
			if hidden(s.ID, p, tombstones) {
				continue
			}
			// Blocks written before observation quality existed read as trusted.
			if p.Quality == "" {
				p.Quality = model.QualityTrusted
			}
			// A raw read has no reduction, so it filters only when the caller
			// explicitly selects qualities. Without that selection every visible
			// observation is returned and nothing is counted as excluded.
			if len(q.IncludeQualities) > 0 && !include.Has(p.Quality) {
				excluded.Add(p.Quality)
				continue
			}
			tally.Add(p.Quality)
			points = append(points, p)
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		result.Stats.Visible += len(points)
		if q.Limit > 0 && emitted+len(points) > q.Limit {
			keep := q.Limit - emitted
			points = points[:keep]
			result.Truncated = true
		}
		emitted += len(points)
		if len(points) > 0 {
			result.Series = append(result.Series, model.SeriesPoints{Series: s, Points: points})
		}
		if reportQuality {
			if result.Quality == nil {
				result.Quality = &model.QualitySummary{}
			}
			result.Quality.Included.Merge(tally)
			// The timeline belongs to raw reads; aggregate uses the same scan
			// only to obtain points and builds its own cell-level explanation.
			if q.Function == "" {
				if timeline := mixedTimeline(s.ID, points); timeline != nil {
					result.Qualities = append(result.Qualities, *timeline)
				}
			}
		}
	}
	if reportQuality {
		if result.Quality == nil {
			result.Quality = &model.QualitySummary{}
		}
		result.Quality.Excluded = excluded
	}
	return result, nil
}

// mixedTimeline returns the ordered visible observations of a series when that
// series carries more than one distinct quality in the requested range. It
// therefore shows exactly where quality changes across the timeline, after
// corrections, sealing and restore.
func mixedTimeline(seriesID string, points []model.Sample) *model.QualityTimeline {
	distinct := map[model.Quality]bool{}
	for _, p := range points {
		distinct[p.Quality] = true
	}
	if len(distinct) < 2 {
		return nil
	}
	timeline := &model.QualityTimeline{SeriesID: seriesID, Points: make([]model.QualityPoint, 0, len(points))}
	for _, p := range points {
		timeline.Points = append(timeline.Points, model.QualityPoint{
			Time: p.Time, Value: p.Value, Quality: p.Quality, Sequence: p.Sequence,
		})
	}
	return timeline
}

func (e *Engine) Range(name string, q model.Query) (model.RangeResult, error) {
	var result model.RangeResult
	if err := model.ValidateQuery(q); err != nil {
		return result, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result, err = scan(b, n, q, volume.Current(tx))
		return err
	})
	if err != nil {
		return result, err
	}
	// A raw read without any quality request keeps the pre-existing scalar
	// response shape. Selecting qualities explicitly is itself a quality
	// request, so the per-point quality is then reported even without
	// report_quality.
	if !q.ReportQuality && len(q.IncludeQualities) == 0 {
		for i := range result.Series {
			for j := range result.Series[i].Points {
				result.Series[i].Points[j].Quality = ""
			}
		}
	}
	return result, nil
}
