package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// errBudget reports that a series cannot be read without exceeding the
// namespace scan budget. Partial reads record that series as not executed and
// continue with the rest; strict reads surface it as a capacity failure.
var errBudget = errors.New("query decoded point budget exceeded")

// errLimit reports that a raw result limit would cut a series short. Partial
// reads mark the remainder not executed; strict reads truncate as before.
var errLimit = errors.New("raw point limit reached before the series was emitted")

// readSeries merges blocks and head records for one series and resolves
// corrections, retention and tombstones. On any unreadable source it fails
// without returning a prefix of the series: callers either discard the series
// (partial reads) or fail the whole request (strict reads). Decoding is
// charged to stats as it happens, so the scan budget bounds real work even
// when a series is later discarded.
func readSeries(b *bolt.Bucket, n model.Namespace, q model.Query, s model.Series,
	blocks []model.Block, tombstones []model.Tombstone, stats *model.ScanStats) ([]model.Sample, error) {
	merged := map[int64]model.Sample{}
	for _, block := range blocks {
		if block.SeriesID != s.ID || block.End <= q.Start ||
			block.Start >= q.End || block.End <= n.Floor {
			continue
		}
		if stats.Decoded+block.Count > n.MaxScan {
			return nil, errBudget
		}
		points, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
		if err != nil {
			return nil, model.Corrupt(fmt.Sprintf("block %s for series %s is unreadable: %v",
				block.ID, s.ID, err))
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
	cursor := b.Bucket(volume.Head).Cursor()
	for k, v := cursor.Seek(volume.PointKey(s.ID, q.Start)); k != nil && bytes.HasPrefix(k, []byte(s.ID)); k, v = cursor.Next() {
		timestamp, err := volume.PointTime(k)
		if err != nil {
			return nil, err
		}
		if timestamp >= q.End {
			break
		}
		if stats.Decoded+1 > n.MaxScan {
			return nil, errBudget
		}
		stats.Head++
		stats.Decoded++
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
	points := make([]model.Sample, 0, len(merged))
	for _, p := range merged {
		if !hidden(s.ID, p, tombstones) {
			points = append(points, p)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
	return points, nil
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
	incomplete := false
	for _, s := range selected {
		points, err := readSeries(b, n, q, s, blocks, tombstones, &result.Stats)
		if err != nil {
			if !q.Partial {
				if errors.Is(err, errBudget) {
					return result, model.Capacity(errBudget.Error())
				}
				return result, err
			}
			incomplete = true
			if errors.Is(err, errBudget) {
				// This series cannot fit in the remaining scan budget. Skip it
				// and keep reading the rest so one oversized series does not
				// hide every other device.
				result.Stats.NotExecuted++
				result.Series = append(result.Series, model.SeriesPoints{
					Series: s, Points: []model.Sample{}, Status: model.ReadNotExecuted,
					Error: "series alone exceeds the remaining scan budget",
				})
				continue
			}
			result.Stats.Failed++
			result.Series = append(result.Series, model.SeriesPoints{
				Series: s, Points: []model.Sample{}, Status: model.ReadFailed,
				Error: err.Error(),
			})
			continue
		}
		result.Stats.Visible += len(points)
		status := ""
		if q.Partial {
			status = model.ReadComplete
		}
		if q.Limit > 0 && emitted+len(points) > q.Limit {
			result.Truncated = true
			if q.Partial {
				// A partial read never emits a prefix of a series: a consumer
				// must not mistake a cut point list for the whole series.
				incomplete = true
				result.Stats.NotExecuted++
				result.Series = append(result.Series, model.SeriesPoints{
					Series: s, Points: []model.Sample{}, Status: model.ReadNotExecuted,
					Error: errLimit.Error(),
				})
				continue
			}
			keep := q.Limit - emitted
			points = points[:keep]
		}
		emitted += len(points)
		// Strict reads keep the historical shape: empty series are omitted.
		// Partial reads list every selected series so a consumer can account
		// for all of them, including complete-but-empty ones.
		if len(points) > 0 || q.Partial {
			result.Series = append(result.Series, model.SeriesPoints{
				Series: s, Points: points, Status: status,
			})
		}
	}
	if q.Partial {
		result.Partial = incomplete
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
