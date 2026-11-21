package engine

import (
	"bytes"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Discover(name string, matchers []model.Matcher) ([]model.Series, error) {
	selector, err := selectseries.Compile(matchers)
	if err != nil {
		return nil, err
	}
	var out []model.Series
	err = e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		series, err := catalog(b)
		if err != nil {
			return err
		}
		out = selectseries.Pick(series, selector)
		return nil
	})
	return out, err
}

func aggregate(b *bolt.Bucket, n model.Namespace, q model.Query, sequence uint64) (model.AggregateResult, error) {
	if err := window.Validate(q); err != nil {
		return model.AggregateResult{}, err
	}
	// A raw result limit must not bias reducers.
	q.Limit = 0
	source := q
	plan := window.PlanSliding(q)
	if q.Width > 0 {
		// Only the aligned output span and the pre-roll the first window may
		// read are relevant; a single scan feeds every output time. No output
		// reads a point at or after the last output time.
		source.Start, source.End = plan.ReadStart, plan.Last
		if plan.Outputs == 0 {
			return model.AggregateResult{
				Sequence: sequence, Cells: []model.Cell{}, Stats: model.ScanStats{},
				Sliding: &model.SlidingStats{Width: q.Width, Step: q.Step},
			}, nil
		}
	}
	raw, err := scan(b, n, source, sequence)
	if err != nil {
		return model.AggregateResult{}, err
	}
	cells, sliding, err := window.Aggregate(raw.Series, q)
	return model.AggregateResult{Sequence: sequence, Cells: cells, Stats: raw.Stats, Sliding: sliding}, err
}

func (e *Engine) Aggregate(name string, q model.Query) (model.AggregateResult, error) {
	var out model.AggregateResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		out, err = aggregate(b, n, q, volume.Current(tx))
		return err
	})
	return out, err
}

type Plan struct {
	Sequence        uint64               `json:"sequence"`
	Floor           int64                `json:"floor"`
	Candidates      []model.Series       `json:"candidates"`
	Blocks          []model.Block        `json:"blocks"`
	MaxDecoded      int                  `json:"max_decoded"`
	SourcePointHint int                  `json:"source_point_hint"`
	HeadRecords     int                  `json:"head_records"`
	Sliding         *model.SlidingBounds `json:"sliding,omitempty"`
}

// headPointsIn counts matching head records inside the effective source span,
// so a sliding explain reports the same work a scan would perform. The count
// stops at the namespace scan budget because the scan would be rejected there.
func headPointsIn(b *bolt.Bucket, ids map[string]bool, start, end int64, limit int64) int64 {
	var total int64
	for id := range ids {
		cursor := b.Bucket(volume.Head).Cursor()
		for k, _ := cursor.Seek(volume.PointKey(id, start)); k != nil && bytes.HasPrefix(k, []byte(id)); k, _ = cursor.Next() {
			timestamp, err := volume.PointTime(k)
			if err != nil || timestamp >= end || total >= limit {
				// Only this series' scan is finished; other series still count.
				break
			}
			total++
		}
		if total >= limit {
			break
		}
	}
	return total
}

func (e *Engine) Explain(name string, q model.Query) (Plan, error) {
	var plan Plan
	// Step and width only participate when the query asks for sliding output;
	// a plain explain is validated like any other raw range request.
	if q.Width > 0 {
		if err := window.Validate(q); err != nil {
			return plan, err
		}
	} else if err := model.ValidateQuery(q); err != nil {
		return plan, err
	}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return plan, err
	}
	start, end := q.Start, q.End
	if q.Width > 0 {
		sliding := window.PlanSliding(q)
		plan.Sliding = &model.SlidingBounds{
			Width: q.Width, Step: q.Step, FirstOutput: sliding.First,
			LastOutput: sliding.Last, Outputs: sliding.Outputs, ReadStart: sliding.ReadStart,
		}
		if sliding.Outputs > 0 {
			// The scan excludes the last output time itself; no window reads it.
			start, end = sliding.ReadStart, sliding.Last
		}
		plan.Sliding.ReadEnd = end
	}
	err = e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		all, err := catalog(b)
		if err != nil {
			return err
		}
		plan.Candidates = selectseries.Pick(all, selector)
		plan.Sequence, plan.Floor, plan.MaxDecoded = volume.Current(tx), n.Floor, n.MaxScan
		plan.HeadRecords = b.Bucket(volume.Head).Stats().KeyN
		plan.Blocks = []model.Block{}
		blocks, err := readBlocks(b)
		if err != nil {
			return err
		}
		ids := map[string]bool{}
		for _, s := range plan.Candidates {
			ids[s.ID] = true
		}
		for _, block := range blocks {
			if ids[block.SeriesID] && block.End > start &&
				block.Start < end && block.End > n.Floor {
				plan.Blocks = append(plan.Blocks, block)
				plan.SourcePointHint += block.Count
			}
		}
		if plan.Sliding != nil {
			plan.Sliding.SourcePoints = int64(plan.SourcePointHint) +
				headPointsIn(b, ids, start, end, int64(n.MaxScan-plan.SourcePointHint))
		}
		return nil
	})
	return plan, err
}
