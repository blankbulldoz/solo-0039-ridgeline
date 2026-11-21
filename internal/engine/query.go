package engine

import (
	"encoding/json"
	"sort"

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
	raw, err := scan(b, n, q, sequence)
	if err != nil {
		return model.AggregateResult{}, err
	}
	complete, withheld := partition(raw, q)
	cells, err := window.Aggregate(complete, q)
	if err != nil {
		return model.AggregateResult{}, err
	}
	result := model.AggregateResult{Sequence: sequence, Stats: raw.Stats}
	result.Partial = raw.Partial
	if q.Partial {
		result.Series = make([]model.SeriesStatus, 0, len(raw.Series))
		for _, sp := range raw.Series {
			status := sp.Status
			if status == "" {
				status = model.ReadComplete
			}
			result.Series = append(result.Series, model.SeriesStatus{
				Series: sp.Series, Status: status, Error: sp.Error,
			})
		}
	}
	if len(withheld) > 0 {
		// A grouping key that lost a source series is withheld entirely:
		// presenting its remaining series would understate the group.
		result.Partial = true
		result.Withheld = make([]model.GroupIssue, 0, len(withheld))
		for _, issue := range withheld {
			result.Withheld = append(result.Withheld, issue)
		}
		sort.Slice(result.Withheld, func(i, j int) bool {
			return groupKey(result.Withheld[i].Labels) < groupKey(result.Withheld[j].Labels)
		})
		cells = dropGroups(cells, withheld, q.GroupBy)
	}
	result.Cells = cells
	return result, nil
}

func groupKey(labels model.Labels) string {
	data, _ := json.Marshal(labels)
	return string(data)
}

// partition splits a partial scan into series safe to reduce and the grouping
// keys that must be withheld. In strict mode scan has already failed on any
// unreadable source, so every series is complete.
func partition(raw model.RangeResult, q model.Query) ([]model.SeriesPoints, map[string]model.GroupIssue) {
	complete := make([]model.SeriesPoints, 0, len(raw.Series))
	withheld := map[string]model.GroupIssue{}
	for _, sp := range raw.Series {
		if sp.Status == "" || sp.Status == model.ReadComplete {
			complete = append(complete, sp)
			continue
		}
		key, labels := window.Group(sp.Series.Labels, q.GroupBy)
		issue, seen := withheld[key]
		if !seen {
			issue = model.GroupIssue{Labels: labels, Status: sp.Status, SeriesID: []string{}}
		}
		if issue.Status == model.ReadNotExecuted && sp.Status == model.ReadFailed {
			issue.Status = sp.Status
		}
		issue.SeriesID = append(issue.SeriesID, sp.Series.ID)
		withheld[key] = issue
	}
	return complete, withheld
}

func dropGroups(cells []model.Cell, withheld map[string]model.GroupIssue, groupBy []string) []model.Cell {
	kept := make([]model.Cell, 0, len(cells))
	for _, cell := range cells {
		key, _ := window.Group(cell.Labels, groupBy)
		if _, blocked := withheld[key]; blocked {
			continue
		}
		kept = append(kept, cell)
	}
	return kept
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
	Sequence        uint64         `json:"sequence"`
	Floor           int64          `json:"floor"`
	Candidates      []model.Series `json:"candidates"`
	Blocks          []model.Block  `json:"blocks"`
	MaxDecoded      int            `json:"max_decoded"`
	SourcePointHint int            `json:"source_point_hint"`
	HeadRecords     int            `json:"head_records"`
}

func (e *Engine) Explain(name string, q model.Query) (Plan, error) {
	var plan Plan
	if err := model.ValidateStrictQuery(q); err != nil {
		return plan, err
	}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return plan, err
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
			if ids[block.SeriesID] && block.End > q.Start &&
				block.Start < q.End && block.End > n.Floor {
				plan.Blocks = append(plan.Blocks, block)
				plan.SourcePointHint += block.Count
			}
		}
		return nil
	})
	return plan, err
}
