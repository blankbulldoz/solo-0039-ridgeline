package engine

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"math"
	"sort"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// WindowBatch is one fully resolved aggregation window. Every group that has
// cells in [Start, End) is present in Cells, and no later source can change
// them.
type WindowBatch struct {
	Sequence uint64          `json:"sequence"`
	Start    int64           `json:"start"`
	End      int64           `json:"end"`
	Cells    []model.Cell    `json:"cells"`
	Stats    model.ScanStats `json:"stats"`
}

// StreamSummary reports the read view and the work performed by
// AggregateStream. Stats are cumulative and remain meaningful when a stream
// ends with an error after some windows were already delivered.
type StreamSummary struct {
	Sequence uint64
	Stats    model.ScanStats
}

// streamBlock is a block selected for the query plus the earliest timestamp it
// could contribute. Blocks are consumed in that order, which lets windows
// below the next block's earliest timestamp be sealed.
type streamBlock struct {
	block    model.Block
	earliest int64
}

// headCursor advances one selected series through the mutable head in
// timestamp order. Cursors live inside the single read transaction.
type headCursor struct {
	id   string
	cur  *bolt.Cursor
	key  []byte
	val  []byte
	next int64
}

type headQueue []*headCursor

func (q headQueue) Len() int { return len(q) }
func (q headQueue) Less(i, j int) bool {
	if q[i].next != q[j].next {
		return q[i].next < q[j].next
	}
	return q[i].id < q[j].id
}
func (q headQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *headQueue) Push(x any)   { *q = append(*q, x.(*headCursor)) }
func (q *headQueue) Pop() any {
	old := *q
	last := old[len(old)-1]
	*q = old[:len(old)-1]
	return last
}

// streamWindow accumulates the merged, not yet published points of one
// alignment bucket. Contributions may arrive from the head and from several
// overlapping blocks and several series.
type streamWindow struct {
	start  int64
	series map[string]map[int64]model.Sample
}

type windowQueue []*streamWindow

func (q windowQueue) Len() int           { return len(q) }
func (q windowQueue) Less(i, j int) bool { return q[i].start < q[j].start }
func (q windowQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *windowQueue) Push(x any)        { *q = append(*q, x.(*streamWindow)) }
func (q *windowQueue) Pop() any {
	old := *q
	last := old[len(old)-1]
	*q = old[:len(old)-1]
	return last
}

// AggregateStream runs an aggregate query in one bbolt read transaction and
// invokes emit for each window as soon as no remaining source can still
// contribute to it. Windows are emitted in ascending time order and every
// group of a window is emitted together. A window is only emitted once all
// blocks that can cover it and all head records before its end have been
// decoded, so an emit result never needs to be retracted.
//
// emit runs while the read transaction is held; returning an error (for
// example because the client disconnected) stops the query and is propagated.
// Errors discovered after earlier windows were emitted leave those windows
// valid but make the stream incomplete.
func (e *Engine) AggregateStream(ctx context.Context, name string, q model.Query,
	emit func(WindowBatch) error) (StreamSummary, error) {
	var summary StreamSummary
	if err := window.Validate(q); err != nil {
		return summary, err
	}
	// A raw result limit must not bias reducers.
	q.Limit = 0
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return summary, err
	}
	err = e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		summary.Sequence = volume.Current(tx)
		allSeries, err := catalog(b)
		if err != nil {
			return err
		}
		selected := selectseries.Pick(allSeries, selector)
		summary.Stats.Series = len(selected)

		tombstones, err := readTombstones(b)
		if err != nil {
			return err
		}
		summary.Stats.Tombstones = len(tombstones)

		// Build the source plan. Sorting by the earliest possible point is
		// what makes a window final before later blocks are decoded.
		blocks, err := readBlocks(b)
		if err != nil {
			return err
		}
		selectedIDs := make(map[string]bool, len(selected))
		for _, s := range selected {
			selectedIDs[s.ID] = true
		}
		plan := make([]streamBlock, 0)
		for _, block := range blocks {
			if !selectedIDs[block.SeriesID] || block.End <= q.Start ||
				block.Start >= q.End || block.End <= n.Floor {
				continue
			}
			earliest := block.Start
			if earliest < q.Start {
				earliest = q.Start
			}
			if earliest < n.Floor {
				earliest = n.Floor
			}
			plan = append(plan, streamBlock{block: block, earliest: earliest})
		}
		sort.Slice(plan, func(i, j int) bool {
			if plan[i].earliest != plan[j].earliest {
				return plan[i].earliest < plan[j].earliest
			}
			return plan[i].block.ID < plan[j].block.ID
		})

		head := b.Bucket(volume.Head)
		heads := &headQueue{}
		for _, s := range selected {
			id := []byte(s.ID)
			cursor := head.Cursor()
			key, value := cursor.Seek(volume.PointKey(s.ID, q.Start))
			if key == nil || !bytes.HasPrefix(key, id) {
				continue
			}
			first, err := volume.PointTime(key)
			if err != nil {
				return err
			}
			if first >= q.End {
				continue
			}
			heap.Push(heads, &headCursor{id: s.ID, cur: cursor, key: key, val: value, next: first})
		}

		// pending deduplicates windows while several sources contribute.
		pending := map[int64]*streamWindow{}
		windows := &windowQueue{}

		windowFor := func(start int64) *streamWindow {
			w := pending[start]
			if w == nil {
				w = &streamWindow{start: start, series: map[string]map[int64]model.Sample{}}
				pending[start] = w
				heap.Push(windows, w)
			}
			return w
		}

		// Head samples win ties with sealed samples, matching scan's
		// "newest committed value" merge.
		add := func(seriesID string, sample model.Sample, fromHead bool) {
			w := windowFor(window.Bucket(sample.Time, q.Origin, q.Step))
			points := w.series[seriesID]
			if points == nil {
				points = map[int64]model.Sample{}
				w.series[seriesID] = points
			}
			old, ok := points[sample.Time]
			if !ok || sample.Sequence > old.Sequence ||
				(fromHead && sample.Sequence == old.Sequence) {
				points[sample.Time] = sample
			}
		}

		// An empty selection is a complete query with no windows; emit still
		// learns the read view through summary.Sequence.

		// Reductions are bucket-local, so one window's points can be handed
		// to the existing reducer without changing arithmetic.
		visible := 0
		finalize := func(w *streamWindow) (WindowBatch, bool, error) {
			seriesPoints := make([]model.SeriesPoints, 0, len(w.series))
			for _, s := range selected {
				points := w.series[s.ID]
				if len(points) == 0 {
					continue
				}
				samples := make([]model.Sample, 0, len(points))
				for _, sample := range points {
					if hidden(s.ID, sample, tombstones) {
						continue
					}
					samples = append(samples, sample)
				}
				if len(samples) == 0 {
					continue
				}
				sort.Slice(samples, func(i, j int) bool { return samples[i].Time < samples[j].Time })
				visible += len(samples)
				seriesPoints = append(seriesPoints, model.SeriesPoints{Series: s, Points: samples})
			}
			if len(seriesPoints) == 0 {
				return WindowBatch{}, false, nil
			}
			cells, err := window.Aggregate(seriesPoints, q)
			if err != nil {
				return WindowBatch{}, false, err
			}
			summary.Stats.Visible += visible
			return WindowBatch{
				Sequence: summary.Sequence,
				Start:    w.start,
				End:      w.start + q.Step,
				Cells:    cells,
				Stats:    summary.Stats,
			}, true, nil
		}

		blockIndex := 0
		payloads := b.Bucket(volume.Payloads)
		frontier := func() int64 {
			next := int64(math.MaxInt64)
			if blockIndex < len(plan) && plan[blockIndex].earliest < next {
				next = plan[blockIndex].earliest
			}
			if heads.Len() > 0 && (*heads)[0].next < next {
				next = (*heads)[0].next
			}
			return next
		}

		advanceHead := func(cursor *headCursor) error {
			key, value := cursor.key, cursor.val
			if _, err := volume.PointTime(key); err != nil {
				return err
			}
			summary.Stats.Head++
			summary.Stats.Decoded++
			if summary.Stats.Decoded > n.MaxScan {
				return model.Capacity("query decoded point budget exceeded")
			}
			var sample model.Sample
			if err := json.Unmarshal(value, &sample); err != nil {
				return err
			}
			if sample.Time >= n.Floor {
				add(cursor.id, sample, true)
			}
			next, nextValue := cursor.cur.Next()
			if next == nil || !bytes.HasPrefix(next, []byte(cursor.id)) {
				return nil
			}
			at, err := volume.PointTime(next)
			if err != nil {
				return err
			}
			if at < q.End {
				cursor.key, cursor.val, cursor.next = next, nextValue, at
				heap.Push(heads, cursor)
			}
			return nil
		}

		advanceBlock := func() error {
			ref := plan[blockIndex]
			blockIndex++
			// Same budget accounting as the non-incremental scan.
			if summary.Stats.Decoded+ref.block.Count > n.MaxScan {
				return model.Capacity("query decoded point budget exceeded")
			}
			points, err := chunk.Decode(payloads.Get([]byte(ref.block.ID)), ref.block)
			if err != nil {
				return err
			}
			summary.Stats.Blocks++
			summary.Stats.Decoded += len(points)
			for _, sample := range points {
				if sample.Time < q.Start || sample.Time >= q.End || sample.Time < n.Floor {
					continue
				}
				add(ref.block.SeriesID, sample, false)
			}
			return nil
		}

		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			next := frontier()
			for windows.Len() > 0 && (*windows)[0].start+q.Step <= next {
				w := heap.Pop(windows).(*streamWindow)
				delete(pending, w.start)
				batch, ready, err := finalize(w)
				if err != nil {
					return err
				}
				if !ready {
					continue
				}
				if err := emit(batch); err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if blockIndex >= len(plan) && heads.Len() == 0 {
				break
			}
			// Advance whichever source decides the current frontier. This
			// keeps early windows flowing even when a late block is large,
			// corrupt, or over budget.
			if blockIndex < len(plan) &&
				(heads.Len() == 0 || plan[blockIndex].earliest <= (*heads)[0].next) {
				if err := advanceBlock(); err != nil {
					return err
				}
				continue
			}
			cursor := heap.Pop(heads).(*headCursor)
			if err := advanceHead(cursor); err != nil {
				return err
			}
		}
		// Every source has been consumed; any still-buffered window is final.
		for windows.Len() > 0 {
			w := heap.Pop(windows).(*streamWindow)
			delete(pending, w.start)
			batch, ready, err := finalize(w)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			if err := emit(batch); err != nil {
				return err
			}
		}
		return nil
	})
	return summary, err
}
