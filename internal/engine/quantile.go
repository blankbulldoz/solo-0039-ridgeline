package engine

import (
	"fmt"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/quantile"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// summaryKey identifies one label group inside one aligned window.
type summaryKey struct {
	group string
	start int64
}

type summaryAccumulator struct {
	labels  model.Labels
	start   int64
	end     int64
	summary *quantile.Summary
}

// buildQuantiles streams every visible point into per-window/group GK summaries
// without retaining the value set. blockCount is the number of compressed
// blocks the scan will touch; when known, each summary is built at tightened
// internal precision so it can later be merged across blocks while honoring the
// requested epsilon.
func buildQuantiles(b *bolt.Bucket, n model.Namespace, q model.QuantileQuery,
	sequence uint64) (map[summaryKey]*summaryAccumulator, model.ScanStats, error) {
	if err := model.ValidateQuantileQuery(q); err != nil {
		return nil, model.ScanStats{}, err
	}
	blockCount := countBlocks(b, n, q)
	accum := map[summaryKey]*summaryAccumulator{}
	stats, err := scanStream(b, n, model.Query{Start: q.Start, End: q.End, Matchers: q.Matchers},
		sequence, func(series model.Series, p model.Sample) {
			groupKey, labels := window.Group(series.Labels, q.GroupBy)
			start := window.Bucket(p.Time, q.Origin, q.Step)
			key := summaryKey{group: groupKey, start: start}
			acc := accum[key]
			if acc == nil {
				s, err := quantile.NewBlockOperand(q.Epsilon, blockCount)
				if err != nil {
					return
				}
				acc = &summaryAccumulator{
					labels: labels, start: start, end: start + q.Step, summary: s,
				}
				accum[key] = acc
			}
			acc.summary.Observe(p.Value)
		})
	if err != nil {
		return nil, stats, err
	}
	return accum, stats, nil
}

// countBlocks reports how many compressed blocks overlap the query interval
// for the selected series, i.e. the fan-in of block summaries. It mirrors the
// block filtering in scanStream without decoding payloads.
func countBlocks(b *bolt.Bucket, n model.Namespace, q model.QuantileQuery) int {
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return 1
	}
	all, err := catalog(b)
	if err != nil {
		return 1
	}
	blocks, err := readBlocks(b)
	if err != nil {
		return 1
	}
	ids := map[string]bool{}
	for _, s := range selectseries.Pick(all, selector) {
		ids[s.ID] = true
	}
	count := 0
	for _, block := range blocks {
		if ids[block.SeriesID] && block.End > q.Start &&
			block.Start < q.End && block.End > n.Floor {
			count++
		}
	}
	if count < 1 {
		return 1
	}
	return count
}

// finalizeQuantiles queries each window/group summary for the requested phis
// and produces the stable-ordered cell result plus serialized summaries.
func finalizeQuantiles(accum map[summaryKey]*summaryAccumulator,
	q model.QuantileQuery, sequence uint64, stats model.ScanStats) (model.QuantileResult, []model.StoredSummary) {
	cells := make([]model.QuantileCell, 0, len(accum))
	stored := make([]model.StoredSummary, 0, len(accum))
	for key, acc := range accum {
		cell := model.QuantileCell{
			Labels: acc.labels, Start: acc.start, End: acc.end,
			Count: acc.summary.Len(), Epsilon: q.Epsilon,
			Estimates: make([]model.QuantileEstimate, 0, len(q.Phis)),
		}
		for _, phi := range q.Phis {
			est, err := acc.summary.Quantile(phi)
			if err != nil {
				continue
			}
			cell.Estimates = append(cell.Estimates, model.QuantileEstimate{
				Phi: phi, Value: est.Value, Lower: est.Lower,
				Upper: est.Upper, RankError: est.RankError,
			})
		}
		cells = append(cells, cell)
		stored = append(stored, model.StoredSummary{
			Key:    fmt.Sprintf("%s/%d", key.group, key.start),
			Labels: acc.labels, Start: acc.start, End: acc.end,
			Count:    acc.summary.Len(),
			Snapshot: acc.summary.Marshal(),
		})
	}
	sortQuantileCells(cells)
	sort.Slice(stored, func(i, j int) bool { return stored[i].Key < stored[j].Key })
	return model.QuantileResult{Sequence: sequence, Cells: cells, Stats: stats}, stored
}

// sortQuantileCells orders cells by canonical label JSON then window start,
// matching the exact aggregate result ordering.
func sortQuantileCells(cells []model.QuantileCell) {
	base := make([]model.Cell, len(cells))
	for i, c := range cells {
		base[i] = model.Cell{Labels: c.Labels, Start: c.Start, End: c.End, Count: c.Count}
	}
	window.SortCells(base)
	sorted := make([]model.QuantileCell, len(cells))
	for i, ordered := range base {
		for _, original := range cells {
			if original.Start == ordered.Start && sameLabels(original.Labels, ordered.Labels) {
				sorted[i] = original
				break
			}
		}
	}
	copy(cells, sorted)
}

func sameLabels(a, b model.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// quantileQuery runs a streaming quantile build inside an open transaction.
func quantileQuery(b *bolt.Bucket, n model.Namespace, q model.QuantileQuery,
	sequence uint64) (model.QuantileResult, []model.StoredSummary, error) {
	accum, stats, err := buildQuantiles(b, n, q, sequence)
	if err != nil {
		return model.QuantileResult{}, nil, err
	}
	result, stored := finalizeQuantiles(accum, q, sequence, stats)
	return result, stored, nil
}

// Quantile executes an ad-hoc approximate quantile query. It never stores a
// summary and never sorts the full source value set: points flow through GK
// summaries bounded by epsilon.
func (e *Engine) Quantile(name string, q model.QuantileQuery) (model.QuantileResult, error) {
	var out model.QuantileResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result, _, err := quantileQuery(b, n, q, volume.Current(tx))
		if err != nil {
			return err
		}
		out = result
		return nil
	})
	return out, err
}
