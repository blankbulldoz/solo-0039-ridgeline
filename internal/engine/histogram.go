package engine

import (
	"bytes"
	"encoding/json"
	"sort"

	"ridgeline/internal/chunk"
	"ridgeline/internal/histogram"
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// WriteHistograms publishes cumulative histogram observations. It shares the
// namespace series catalog, watermark and sequence with scalar writes, so
// corrections, limits and retention behave identically.
func (e *Engine) WriteHistograms(name string, request model.HistogramWriteRequest) (model.WriteReceipt, error) {
	var receipt model.WriteReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if err := policy.AdmitHistograms(n, request); err != nil {
			return err
		}
		series, err := catalog(b)
		if err != nil {
			return err
		}
		newSeries := map[string]model.Labels{}
		for _, p := range request.Histograms {
			id := model.SeriesID(p.Labels)
			if b.Bucket(volume.Catalog).Get([]byte(id)) == nil {
				newSeries[id] = p.Labels
			}
		}
		if len(series)+len(newSeries) > n.MaxSeries {
			return model.Capacity("series cardinality policy exceeded")
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for id, labels := range newSeries {
			if err := volume.PutJSON(b.Bucket(volume.Catalog), []byte(id),
				model.Series{ID: id, Labels: labels}); err != nil {
				return err
			}
		}
		for _, p := range request.Histograms {
			sample := model.HistogramSample{
				Time: p.Time, Bounds: p.Bounds, Counts: p.Counts,
				Sum: p.Sum, Sequence: seq,
			}
			key := volume.PointKey(model.SeriesID(p.Labels), p.Time)
			if err := volume.PutJSON(b.Bucket(volume.HistHead), key, sample); err != nil {
				return err
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
		}
		n.LastSequence = seq
		receipt = model.WriteReceipt{
			Sequence: seq, Accepted: len(request.Histograms),
			NewSeries: len(newSeries), Watermark: n.Watermark,
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

func readHistBlocks(b *bolt.Bucket) ([]model.Block, error) {
	out := make([]model.Block, 0)
	err := b.Bucket(volume.HistBlocks).ForEach(func(_, value []byte) error {
		var block model.Block
		if err := json.Unmarshal(value, &block); err != nil {
			return err
		}
		out = append(out, block)
		return nil
	})
	return out, err
}

func hiddenHistogram(id string, p model.HistogramSample, tombstones []model.Tombstone) bool {
	for _, tomb := range tombstones {
		if p.Time < tomb.Start || p.Time >= tomb.End || p.Sequence > tomb.Sequence {
			continue
		}
		for _, series := range tomb.Series {
			if series == id {
				return true
			}
		}
	}
	return false
}

// scanHistograms merges histogram blocks with head snapshots, then applies
// retention and deletion exactly like the scalar scan.
func scanHistograms(b *bolt.Bucket, n model.Namespace, q model.HistogramQuery, sequence uint64) ([]model.SeriesHistograms, model.ScanStats, error) {
	stats := model.ScanStats{}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return nil, stats, err
	}
	allSeries, err := catalog(b)
	if err != nil {
		return nil, stats, err
	}
	blocks, err := readHistBlocks(b)
	if err != nil {
		return nil, stats, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return nil, stats, err
	}
	stats.Tombstones = len(tombstones)
	selected := selectseries.Pick(allSeries, selector)
	stats.Series = len(selected)
	out := make([]model.SeriesHistograms, 0)
	for _, s := range selected {
		merged := map[int64]model.HistogramSample{}
		for _, block := range blocks {
			if block.SeriesID != s.ID || block.End <= q.Start ||
				block.Start >= q.End || block.End <= n.Floor {
				continue
			}
			if stats.Decoded+block.Count > n.MaxScan {
				return nil, stats, model.Capacity("query decoded point budget exceeded")
			}
			points, err := chunk.DecodeHistograms(
				b.Bucket(volume.HistPayloads).Get([]byte(block.ID)), block)
			if err != nil {
				return nil, stats, err
			}
			stats.Blocks++
			stats.Decoded += len(points)
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
		cursor := b.Bucket(volume.HistHead).Cursor()
		for k, v := cursor.Seek(volume.PointKey(s.ID, q.Start)); k != nil &&
			bytes.HasPrefix(k, []byte(s.ID)); k, v = cursor.Next() {
			timestamp, err := volume.PointTime(k)
			if err != nil {
				return nil, stats, err
			}
			if timestamp >= q.End {
				break
			}
			stats.Head++
			stats.Decoded++
			if stats.Decoded > n.MaxScan {
				return nil, stats, model.Capacity("query decoded point budget exceeded")
			}
			var p model.HistogramSample
			if err := json.Unmarshal(v, &p); err != nil {
				return nil, stats, err
			}
			if p.Time < n.Floor {
				continue
			}
			old, ok := merged[p.Time]
			if !ok || p.Sequence >= old.Sequence {
				merged[p.Time] = p
			}
		}
		points := make([]model.HistogramSample, 0, len(merged))
		for _, p := range merged {
			if !hiddenHistogram(s.ID, p, tombstones) {
				points = append(points, p)
			}
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		stats.Visible += len(points)
		if len(points) > 0 {
			out = append(out, model.SeriesHistograms{Series: s, Points: points})
		}
	}
	return out, stats, nil
}

// HistogramRange returns corrected, deletion- and retention-aware histogram
// snapshots without window reduction.
func (e *Engine) HistogramRange(name string, q model.HistogramQuery) (model.HistogramRangeResult, error) {
	if err := model.ValidateHistogramRange(q); err != nil {
		return model.HistogramRangeResult{}, err
	}
	var out model.HistogramRangeResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		series, stats, err := scanHistograms(b, n, q, volume.Current(tx))
		if err != nil {
			return err
		}
		out = model.HistogramRangeResult{
			Sequence: volume.Current(tx), Floor: n.Floor,
			Series: series, Stats: stats,
		}
		return nil
	})
	return out, err
}

// Histograms runs reset-aware window reduction with quantile estimation.
func (e *Engine) Histograms(name string, q model.HistogramQuery) (model.HistogramResult, error) {
	var out model.HistogramResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		sequence := volume.Current(tx)
		series, stats, err := scanHistograms(b, n, q, sequence)
		if err != nil {
			return err
		}
		cells, err := histogram.Aggregate(series, q)
		if err != nil {
			return err
		}
		out = model.HistogramResult{Sequence: sequence, Floor: n.Floor, Cells: cells, Stats: stats}
		return nil
	})
	return out, err
}

// HistogramBlocks lists compressed histogram block inventory.
func (e *Engine) HistogramBlocks(name string) ([]model.Block, error) {
	var blocks []model.Block
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		blocks, err = readHistBlocks(b)
		return err
	})
	return blocks, err
}
