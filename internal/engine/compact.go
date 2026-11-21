package engine

import (
	"bytes"
	"encoding/json"
	"sort"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// rewrite is the physical work computed for one selected series. Blocks that
// merely straddle the requested interval are read in full, so points outside
// the interval survive into the rewritten block instead of being dropped.
type rewrite struct {
	series  model.Series
	blocks  []model.Block
	keys    [][]byte
	points  []model.Sample
	start   int64
	end     int64
	head    int
	hidden  int
	expired int
	dupes   int
	kept    int
}

func (w rewrite) touched() bool { return len(w.blocks) > 0 || w.head > 0 }

// prunableTombstones returns the deletions whose full series set lies inside
// the selection and whose interval is inside the compacted range. Every series
// they could hide is rewritten there, so their effect is already materialized
// into the surviving block or head and the record itself can be dropped.
// Anything reaching outside keeps hiding points that were not rewritten.
func prunableTombstones(tombstones []model.Tombstone, start, end int64,
	selected []model.Series) []model.Tombstone {
	inSelection := map[string]bool{}
	for _, s := range selected {
		inSelection[s.ID] = true
	}
	var out []model.Tombstone
	for _, tomb := range tombstones {
		if tomb.Start < start || tomb.End > end || len(tomb.Series) == 0 {
			continue
		}
		covered := true
		for _, id := range tomb.Series {
			if !inSelection[id] {
				covered = false
				break
			}
		}
		if covered {
			out = append(out, tomb)
		}
	}
	return out
}

func countRecords(b *bolt.Bucket) (int, error) {
	total := 0
	err := b.ForEach(func(_, _ []byte) error {
		total++
		return nil
	})
	return total, err
}

// readHeadRange returns the head records of one series whose timestamps fall
// in [start, end), along with their keys so a rewrite can consume exactly the
// records it folded into a block.
func readHeadRange(b *bolt.Bucket, id string, start, end int64) ([]model.Sample, [][]byte, error) {
	var points []model.Sample
	var keys [][]byte
	cursor := b.Bucket(volume.Head).Cursor()
	for k, v := cursor.Seek(volume.PointKey(id, start)); k != nil && bytes.HasPrefix(k, []byte(id)); k, v = cursor.Next() {
		timestamp, err := volume.PointTime(k)
		if err != nil {
			return nil, nil, err
		}
		if timestamp >= end {
			break
		}
		var p model.Sample
		if err := json.Unmarshal(v, &p); err != nil {
			return nil, nil, err
		}
		points = append(points, p)
		keys = append(keys, append([]byte(nil), k...))
	}
	return points, keys, nil
}

// planRewrite resolves the physical sources that overlap [start, end) for one
// series. Head corrections inside the interval take precedence on equal
// sequence, matching the read path exactly. Points outside the interval that
// share a straddling block are preserved, and deletions or the retention floor
// are resolved for in-range points before anything is rewritten.
func planRewrite(b *bolt.Bucket, n model.Namespace, s model.Series, blocks []model.Block,
	start, end int64, tombstones []model.Tombstone, decoded *int) (rewrite, error) {
	work := rewrite{series: s}
	for _, block := range blocks {
		if block.SeriesID == s.ID && block.End > start && block.Start < end {
			work.blocks = append(work.blocks, block)
		}
	}
	headPoints, headKeys, err := readHeadRange(b, s.ID, start, end)
	if err != nil {
		return work, err
	}
	work.head = len(headPoints)
	if !work.touched() {
		return rewrite{}, nil
	}
	merged := map[int64]model.Sample{}
	low, high := int64(0), int64(0)
	seen := false
	note := func(t int64) {
		if !seen || t < low {
			low = t
		}
		if !seen || t >= high {
			high = t + 1
		}
		seen = true
	}
	for _, block := range work.blocks {
		if *decoded+block.Count > n.MaxScan {
			return work, model.Capacity("compaction decoded point budget exceeded")
		}
		points, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
		if err != nil {
			return work, err
		}
		*decoded += len(points)
		for _, p := range points {
			if old, ok := merged[p.Time]; ok {
				work.dupes++
				if p.Sequence <= old.Sequence {
					continue
				}
			} else {
				note(p.Time)
			}
			merged[p.Time] = p
		}
	}
	for _, p := range headPoints {
		*decoded++
		if old, ok := merged[p.Time]; ok {
			work.dupes++
			if p.Sequence < old.Sequence {
				continue
			}
		} else {
			note(p.Time)
		}
		merged[p.Time] = p
	}
	for _, p := range merged {
		// Partial compaction only resolves deletion and retention for the
		// interval it was asked to rewrite. Points carried in from a straddling
		// block that lie outside the interval are preserved verbatim, even if a
		// reader would currently hide them; the read path still resolves that.
		if p.Time < start || p.Time >= end {
			work.points = append(work.points, p)
			work.kept++
			continue
		}
		switch {
		case p.Time < n.Floor:
			work.expired++
		case hidden(s.ID, p, tombstones):
			work.hidden++
		default:
			work.points = append(work.points, p)
		}
	}
	sort.Slice(work.points, func(i, j int) bool { return work.points[i].Time < work.points[j].Time })
	work.start, work.end = low, high
	// A single already-minimal block with nothing to fold in, hide, expire or
	// resolve has no redundancy to remove. Rewriting it would publish a new
	// sequence without shrinking anything, so repeated compaction converges
	// instead of churning. The read path keeps resolving whatever the block
	// still contains, including points a deletion or floor hides.
	if len(work.blocks) == 1 && work.head == 0 && work.hidden == 0 &&
		work.expired == 0 && work.dupes == 0 {
		return rewrite{}, nil
	}
	work.keys = headKeys
	return work, nil
}

func applyRewrite(b *bolt.Bucket, seq uint64, work rewrite) (model.ProcessedSeries, error) {
	entry := model.ProcessedSeries{
		Series: work.series, Start: work.start, End: work.end,
		BlocksRemoved: len(work.blocks), Points: len(work.points),
		PreservedPoints: work.kept, HeadPoints: work.head,
		HiddenPoints: work.hidden, ExpiredPoints: work.expired,
		DuplicatePoints: work.dupes,
	}
	for _, block := range work.blocks {
		if err := b.Bucket(volume.Blocks).Delete([]byte(block.ID)); err != nil {
			return entry, err
		}
		if err := b.Bucket(volume.Payloads).Delete([]byte(block.ID)); err != nil {
			return entry, err
		}
	}
	for _, key := range work.keys {
		if err := b.Bucket(volume.Head).Delete(key); err != nil {
			return entry, err
		}
	}
	if len(work.points) > 0 {
		if err := putBlock(b, work.series.ID, seq, work.points); err != nil {
			return entry, err
		}
		entry.BlocksWritten = 1
	}
	return entry, nil
}

// compactScoped rewrites only the blocks and head corrections that overlap the
// half-open interval for the selected series. Physical sources outside the
// selection or the interval remain untouched, and a collection that fails the
// decoded-point budget is rejected before any mutation is applied.
func compactScoped(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace,
	req model.CompactRequest) (model.Compaction, error) {
	start, end, err := model.CompactBounds(req)
	if err != nil {
		return model.Compaction{}, err
	}
	selector, err := selectseries.Compile(req.Matchers)
	if err != nil {
		return model.Compaction{}, err
	}
	all, err := catalog(b)
	if err != nil {
		return model.Compaction{}, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return model.Compaction{}, err
	}
	blocks, err := readBlocks(b)
	if err != nil {
		return model.Compaction{}, err
	}
	selected := selectseries.Pick(all, selector)
	receipt := model.Compaction{
		Maintenance: model.Maintenance{Floor: n.Floor},
		Requested:   model.Interval{Start: start, End: end},
		Processed:   []model.ProcessedSeries{},
	}
	receipt.SelectedSeries = len(selected)
	decoded := 0
	works := make([]rewrite, 0, len(selected))
	for _, s := range selected {
		work, err := planRewrite(b, n, s, blocks, start, end, tombstones, &decoded)
		if err != nil {
			return receipt, err
		}
		if work.touched() {
			works = append(works, work)
		}
	}
	seq := volume.Current(tx)
	pruned := prunableTombstones(tombstones, start, end, selected)
	if len(works) > 0 || len(pruned) > 0 {
		if seq, err = volume.Next(tx); err != nil {
			return receipt, err
		}
	}
	for _, work := range works {
		entry, err := applyRewrite(b, seq, work)
		if err != nil {
			return receipt, err
		}
		receipt.Processed = append(receipt.Processed, entry)
		receipt.BlocksRemoved += entry.BlocksRemoved
		receipt.Blocks += entry.BlocksWritten
		receipt.Points += entry.Points
		receipt.PreservedPoints += entry.PreservedPoints
		receipt.HeadPoints += entry.HeadPoints
		receipt.HiddenPoints += entry.HiddenPoints
		receipt.ExpiredPoints += entry.ExpiredPoints
		receipt.DuplicatePoints += entry.DuplicatePoints
	}
	receipt.Sequence = seq
	if len(works) > 0 || len(pruned) > 0 {
		n.LastSequence = seq
		if err := volume.PutJSON(b, volume.Config, n); err != nil {
			return receipt, err
		}
	}
	for _, tomb := range pruned {
		if err := b.Bucket(volume.Deletes).Delete([]byte(tomb.ID)); err != nil {
			return receipt, err
		}
	}
	receipt.TombstonesPruned = len(pruned)
	blocksAfter, err := readBlocks(b)
	if err != nil {
		return receipt, err
	}
	// bbolt bucket statistics can lag deletes performed in the same write
	// transaction, so inventory counts are taken by iteration.
	blockCount := 0
	err = b.Bucket(volume.Blocks).ForEach(func(_, _ []byte) error {
		blockCount++
		return nil
	})
	if err != nil {
		return receipt, err
	}
	headAfter, err := countRecords(b.Bucket(volume.Head))
	if err != nil {
		return receipt, err
	}
	chosen := map[string]bool{}
	for _, s := range selected {
		chosen[s.ID] = true
	}
	receipt.Remaining = model.Remaining{
		Series:               len(all),
		Blocks:               blockCount,
		HeadRecords:          headAfter,
		Tombstones:           len(tombstones) - receipt.TombstonesPruned,
		UntouchedBlocks:      blockCount - receipt.Blocks,
		UntouchedHeadRecords: headAfter,
		Surviving:            []model.Block{},
	}
	for _, block := range blocksAfter {
		if chosen[block.SeriesID] {
			receipt.Remaining.Surviving = append(receipt.Remaining.Surviving, block)
		}
	}
	sort.Slice(receipt.Remaining.Surviving, func(i, j int) bool {
		return receipt.Remaining.Surviving[i].ID < receipt.Remaining.Surviving[j].ID
	})
	return receipt, nil
}

func (e *Engine) Compact(name string, req model.CompactRequest) (model.Compaction, error) {
	var receipt model.Compaction
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		receipt, err = compactScoped(tx, b, n, req)
		return err
	})
	return receipt, err
}

func (e *Engine) Retain(name string, now int64) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		n.Floor, err = policy.RetentionFloor(n, now)
		if err != nil {
			return err
		}
		advance, err := compactScoped(tx, b, n, model.CompactRequest{})
		if err != nil {
			return err
		}
		receipt = advance.Maintenance
		// Advancing the floor is itself a mutation even when every block is
		// already minimal, so it must publish its own sequence and floor.
		if len(advance.Processed) == 0 && advance.TombstonesPruned == 0 {
			seq, err := volume.Next(tx)
			if err != nil {
				return err
			}
			n.LastSequence, receipt.Sequence = seq, seq
			return volume.PutJSON(b, volume.Config, n)
		}
		return nil
	})
	return receipt, err
}
