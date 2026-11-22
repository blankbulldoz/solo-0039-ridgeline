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

// scanBudget enforces a namespace's decoded point budget over one complete
// read operation. A caller that streams series one at a time still shares the
// same budget counter, so partially processed work counts toward the limit.
type scanBudget struct {
	max   int
	stats model.ScanStats
}

func (budget *scanBudget) reserveBlock(block model.Block) error {
	if budget.stats.Decoded+block.Count > budget.max {
		return model.Capacity("query decoded point budget exceeded")
	}
	return nil
}

func (budget *scanBudget) recordBlock(points int) {
	budget.stats.Blocks++
	budget.stats.Decoded += points
}

func (budget *scanBudget) recordHead() error {
	budget.stats.Head++
	budget.stats.Decoded++
	if budget.stats.Decoded > budget.max {
		return model.Capacity("query decoded point budget exceeded")
	}
	return nil
}

// scanSource is the shared physical read path used by range, aggregation and
// maintenance. It loads catalog and block metadata once, then resolves one
// series at a time so callers never have to retain every expanded visible
// point. The scan budget spans the entire source, not an individual series.
type scanSource struct {
	bucket     *bolt.Bucket
	namespace  model.Namespace
	query      model.Query
	series     []model.Series
	blocks     []model.Block
	bySeries   map[string][]model.Block
	tombstones []model.Tombstone
	budget     scanBudget
}

func openScan(b *bolt.Bucket, n model.Namespace, q model.Query) (*scanSource, error) {
	src := &scanSource{
		bucket: b, namespace: n, query: q,
		budget: scanBudget{max: n.MaxScan},
	}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return nil, err
	}
	allSeries, err := catalog(b)
	if err != nil {
		return nil, err
	}
	blocks, err := readBlocks(b)
	if err != nil {
		return nil, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return nil, err
	}
	src.blocks = blocks
	src.tombstones = tombstones
	src.budget.stats.Tombstones = len(tombstones)
	src.series = selectseries.Pick(allSeries, selector)
	src.budget.stats.Series = len(src.series)
	src.bySeries = make(map[string][]model.Block, len(blocks))
	for _, block := range blocks {
		src.bySeries[block.SeriesID] = append(src.bySeries[block.SeriesID], block)
	}
	return src, nil
}

func (src *scanSource) stats() model.ScanStats {
	return src.budget.stats
}

// forEach resolves physical duplicates and visibility for one selected series
// at a time, in catalog order, and hands the expanded points to visit. The
// points slice is only valid for the duration of the callback.
func (src *scanSource) forEach(visit func(model.Series, []model.Sample) error) error {
	for _, s := range src.series {
		points, err := src.visible(s)
		if err != nil {
			return err
		}
		if err := visit(s, points); err != nil {
			return err
		}
	}
	return nil
}

func (src *scanSource) visible(s model.Series) ([]model.Sample, error) {
	n, q := src.namespace, src.query
	merged := map[int64]model.Sample{}
	payloads := src.bucket.Bucket(volume.Payloads)
	for _, block := range src.bySeries[s.ID] {
		if block.End <= q.Start || block.Start >= q.End || block.End <= n.Floor {
			continue
		}
		if err := src.budget.reserveBlock(block); err != nil {
			return nil, err
		}
		points, err := chunk.Decode(payloads.Get([]byte(block.ID)), block)
		if err != nil {
			return nil, err
		}
		src.budget.recordBlock(len(points))
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
	cursor := src.bucket.Bucket(volume.Head).Cursor()
	prefix := []byte(s.ID)
	for k, v := cursor.Seek(volume.PointKey(s.ID, q.Start)); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
		timestamp, err := volume.PointTime(k)
		if err != nil {
			return nil, err
		}
		if timestamp >= q.End {
			break
		}
		if err := src.budget.recordHead(); err != nil {
			return nil, err
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
	points := make([]model.Sample, 0, len(merged))
	for _, p := range merged {
		if !hidden(s.ID, p, src.tombstones) {
			points = append(points, p)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
	src.budget.stats.Visible += len(points)
	return points, nil
}

func scan(b *bolt.Bucket, n model.Namespace, q model.Query, sequence uint64) (model.RangeResult, error) {
	result := model.RangeResult{Sequence: sequence, Floor: n.Floor, Series: []model.SeriesPoints{}}
	src, err := openScan(b, n, q)
	if err != nil {
		return result, err
	}
	emitted := 0
	err = src.forEach(func(s model.Series, points []model.Sample) error {
		// The raw limit truncates returned rows only. Scanning and budget
		// accounting continue for every selected series.
		if q.Limit > 0 && emitted+len(points) > q.Limit {
			keep := q.Limit - emitted
			points = points[:keep]
			result.Truncated = true
		}
		emitted += len(points)
		if len(points) > 0 {
			result.Series = append(result.Series, model.SeriesPoints{Series: s, Points: points})
		}
		return nil
	})
	result.Stats = src.stats()
	return result, err
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
