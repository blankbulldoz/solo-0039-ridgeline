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
	needPredecessor := q.Function == "twavg" || q.Function == "integral"
	for _, s := range selected {
		merged := map[int64]model.Sample{}
		if needPredecessor {
			// Time-weighted reducers need the value that was already in force
			// when the window opened; it never contributes on its own.
			if predecessor, ok, err := scanPredecessor(b, n, s.ID, q.Start, tombstones, &result.Stats); err != nil {
				return result, err
			} else if ok {
				merged[predecessor.Time] = predecessor
			}
		}
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
		for _, p := range merged {
			if !hidden(s.ID, p, tombstones) {
				points = append(points, p)
			}
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		// The predecessor only seeds a time-weighted hold and is not a row of
		// the queried interval, so it stays out of the visible-point budget.
		visible := len(points)
		if needPredecessor && len(points) > 0 && points[0].Time < q.Start {
			visible--
		}
		result.Stats.Visible += visible
		if q.Limit > 0 && emitted+visible > q.Limit {
			keep := q.Limit - emitted
			if keep < 0 {
				keep = 0
			}
			if keep > len(points) {
				keep = len(points)
			}
			points = points[:keep]
			result.Truncated = true
		}
		emitted += visible
		if len(points) > 0 {
			result.Series = append(result.Series, model.SeriesPoints{Series: s, Points: points})
		}
	}
	return result, nil
}

// scanPredecessor returns the newest visible point of a series strictly before
// start, resolving head, blocks, corrections, deletions and retention exactly
// as an in-window point would. It exists to seed time-weighted holds across a
// window whose first bucket has no sample of its own.
func scanPredecessor(b *bolt.Bucket, n model.Namespace, seriesID string, start int64,
	tombstones []model.Tombstone, stats *model.ScanStats) (model.Sample, bool, error) {
	var best model.Sample
	found := false
	consider := func(p model.Sample) {
		if p.Time >= start || p.Time < n.Floor || hidden(seriesID, p, tombstones) {
			return
		}
		if !found || p.Time > best.Time || (p.Time == best.Time && p.Sequence > best.Sequence) {
			best, found = p, true
		}
	}
	blocks, err := readBlocks(b)
	if err != nil {
		return best, false, err
	}
	for _, block := range blocks {
		if block.SeriesID != seriesID || block.Start >= start || block.End <= n.Floor {
			continue
		}
		if stats.Decoded+block.Count > n.MaxScan {
			return best, false, model.Capacity("query decoded point budget exceeded")
		}
		points, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
		if err != nil {
			return best, false, err
		}
		stats.Blocks++
		stats.Decoded += len(points)
		for _, p := range points {
			consider(p)
		}
	}
	// Walk the head backwards from the first key at or after start; the first
	// key belonging to this series is its newest head point before start.
	cursor := b.Bucket(volume.Head).Cursor()
	prefix := []byte(seriesID)
	k, v := cursor.Seek(volume.PointKey(seriesID, start))
	if k == nil {
		// No key at or after start anywhere in the head: the bucket's last
		// key is the only possible predecessor (it belongs to this series only
		// when another series does not sort after it).
		k, v = cursor.Last()
	} else {
		k, v = cursor.Prev()
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		timestamp, err := volume.PointTime(k)
		if err != nil {
			return best, false, err
		}
		if timestamp < start {
			stats.Head++
			stats.Decoded++
			if stats.Decoded > n.MaxScan {
				return best, false, model.Capacity("query decoded point budget exceeded")
			}
			var p model.Sample
			if err := json.Unmarshal(v, &p); err != nil {
				return best, false, err
			}
			consider(p)
		}
	}
	return best, found, nil
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
