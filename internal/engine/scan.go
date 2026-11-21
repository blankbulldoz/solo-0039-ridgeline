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

// seriesPoints merges every physical representation of one series into the
// newest committed value per timestamp. Tombstones and the retention floor are
// applied by the caller so this stays usable for both queries and correction
// checks. Points outside [start, end) and stale block payloads are ignored.
// A shared stats/cache pair keeps a multi-point correction check within one
// scan budget and decodes each covering block only once.
func seriesPoints(b *bolt.Bucket, n model.Namespace, id string, start, end int64,
	blocks []model.Block, stats *model.ScanStats, cache map[string][]model.Sample) (map[int64]model.Sample, error) {
	merged := map[int64]model.Sample{}
	for _, block := range blocks {
		if block.SeriesID != id || block.End <= start ||
			block.Start >= end || block.End <= n.Floor {
			continue
		}
		points, cached := cache[block.ID]
		if !cached {
			if stats.Decoded+block.Count > n.MaxScan {
				return nil, model.Capacity("query decoded point budget exceeded")
			}
			decoded, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
			if err != nil {
				return nil, err
			}
			points = decoded
			stats.Blocks++
			stats.Decoded += len(points)
			if cache != nil {
				cache[block.ID] = points
			}
		}
		for _, p := range points {
			if p.Time < start || p.Time >= end || p.Time < n.Floor {
				continue
			}
			old, ok := merged[p.Time]
			if !ok || p.Sequence > old.Sequence {
				merged[p.Time] = p
			}
		}
	}
	cursor := b.Bucket(volume.Head).Cursor()
	for k, v := cursor.Seek(volume.PointKey(id, start)); k != nil && bytes.HasPrefix(k, []byte(id)); k, v = cursor.Next() {
		timestamp, err := volume.PointTime(k)
		if err != nil {
			return nil, err
		}
		if timestamp >= end {
			break
		}
		stats.Head++
		stats.Decoded++
		if stats.Decoded > n.MaxScan {
			return nil, model.Capacity("query decoded point budget exceeded")
		}
		var p model.Sample
		if err := json.Unmarshal(v, &p); err != nil {
			return nil, err
		}
		if p.Time < n.Floor {
			continue
		}
		old, ok := merged[p.Time]
		if !ok || p.Sequence >= old.Sequence {
			merged[p.Time] = p
		}
	}
	return merged, nil
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
	for _, s := range selected {
		merged, err := seriesPoints(b, n, s.ID, q.Start, q.End, blocks, &result.Stats, nil)
		if err != nil {
			return result, err
		}
		points := make([]model.Sample, 0, len(merged))
		for _, p := range merged {
			if !hidden(s.ID, p, tombstones) {
				points = append(points, p)
			}
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
	}
	return result, nil
}

// observe resolves the current state of one series and timestamp. A point that
// is below the retention floor is expired; one that a deletion hides is
// deleted; otherwise the newest committed value is present. The distinctions
// let a correction author re-evaluate instead of treating all three as absent.
func observe(b *bolt.Bucket, n model.Namespace, id string, timestamp int64,
	blocks []model.Block, stats *model.ScanStats, cache map[string][]model.Sample) (model.ObservationState, error) {
	if timestamp < n.Floor {
		return model.ObservationState{State: model.StateExpired}, nil
	}
	merged, err := seriesPoints(b, n, id, timestamp, timestamp+1, blocks, stats, cache)
	if err != nil {
		return model.ObservationState{}, err
	}
	sample, ok := merged[timestamp]
	if !ok {
		return model.ObservationState{State: model.StateAbsent}, nil
	}
	state := model.ObservationState{State: model.StatePresent, Sequence: sample.Sequence, Source: "block"}
	if raw := b.Bucket(volume.Head).Get(volume.PointKey(id, timestamp)); raw != nil {
		var head model.Sample
		if err := json.Unmarshal(raw, &head); err != nil {
			return model.ObservationState{}, err
		}
		if head.Sequence == sample.Sequence {
			state.Source = "head"
		}
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return model.ObservationState{}, err
	}
	for _, tomb := range tombstones {
		if sample.Time < tomb.Start || sample.Time >= tomb.End || sample.Sequence > tomb.Sequence {
			continue
		}
		for _, series := range tomb.Series {
			if series == id {
				state.State, state.Tombstone = model.StateDeleted, tomb.ID
				break
			}
		}
		if state.State == model.StateDeleted {
			break
		}
	}
	value := sample.Value
	state.Value = &value
	return state, nil
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
