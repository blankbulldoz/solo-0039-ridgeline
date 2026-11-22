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
	extend := int64(0)
	if q.Function == "rate" && q.RateEdgeContext {
		extend = q.RateMaxGapMS
	}
	return scanRange(b, n, q, sequence, extend)
}

// scanRange reads the visible samples in [q.Start, q.End). When extend is
// positive it additionally reads samples in a bounded margin on each side;
// the reducer treats those samples as neighbor-only evidence rather than as
// window members, so outside increments can never enter a window wholesale.
// The same decoded-point scan budget covers the extra work.
func scanRange(b *bolt.Bucket, n model.Namespace, q model.Query, sequence uint64, extend int64) (model.RangeResult, error) {
	result := model.RangeResult{Sequence: sequence, Floor: n.Floor, Series: []model.SeriesPoints{}}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return result, err
	}
	readStart, readEnd := q.Start, q.End
	if extend > 0 {
		if q.Start-model.MinTime <= extend {
			readStart = model.MinTime
		} else {
			readStart = q.Start - extend
		}
		if model.MaxTime+1-q.End <= extend {
			readEnd = model.MaxTime + 1
		} else {
			readEnd = q.End + extend
		}
	}
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
			if block.SeriesID != s.ID || block.End <= readStart ||
				block.Start >= readEnd || block.End <= n.Floor {
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
				if p.Time < readStart || p.Time >= readEnd || p.Time < n.Floor {
					continue
				}
				old, ok := merged[p.Time]
				if !ok || p.Sequence > old.Sequence {
					merged[p.Time] = p
				}
			}
		}
		cursor := b.Bucket(volume.Head).Cursor()
		for k, v := cursor.Seek(volume.PointKey(s.ID, readStart)); k != nil && bytes.HasPrefix(k, []byte(s.ID)); k, v = cursor.Next() {
			timestamp, err := volume.PointTime(k)
			if err != nil {
				return result, err
			}
			if timestamp >= readEnd {
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
		for _, p := range merged {
			if !hidden(s.ID, p, tombstones) {
				points = append(points, p)
			}
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		if extend > 0 {
			for _, p := range points {
				if p.Time < q.Start || p.Time >= q.End {
					result.Stats.Edges++
				}
			}
		}
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
	}
	return result, nil
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
	return result, err
}
