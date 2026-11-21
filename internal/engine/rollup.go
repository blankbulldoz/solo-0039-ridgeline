package engine

import (
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

func loadRollup(b *bolt.Bucket, name string) (model.Materialization, bool, error) {
	var m model.Materialization
	if b.Bucket(volume.Rollups).Get([]byte(name)) == nil {
		return m, false, nil
	}
	if err := volume.GetJSON(b.Bucket(volume.Rollups), []byte(name), &m); err != nil {
		return m, false, err
	}
	return m, true, nil
}

// Materialize stores a rollup tier. A definition without a source aggregates
// raw points. A definition with a source reduces that rollup's stored reduction
// state instead, so a coarser tier is exact for count, sum, avg, min and max
// without re-reading raw points and without averaging child averages.
func (e *Engine) Materialize(name, rollup string, def model.RollupDefinition) (model.Materialization, error) {
	var out model.Materialization
	if !model.ValidName(rollup) {
		return out, model.Bad("invalid rollup name")
	}
	q := def.Query()
	if err := window.Validate(q); err != nil {
		return out, err
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		sequence := volume.Current(tx)
		// Rollup metadata advances the global sequence but never the namespace
		// sequence, so freshness is always measured against the latter.
		if _, err := volume.Next(tx); err != nil {
			return err
		}
		var result model.AggregateResult
		var provenance *model.RollupProvenance
		sourceSequence := n.LastSequence
		if def.Source != nil {
			result, provenance, err = deriveRollup(b, n, def, sequence)
			if err == nil {
				// A derived tier reflects the snapshot its source rollup was
				// built from, not data that arrived afterwards. Recording that
				// sequence keeps freshness honest even when the source was
				// already stale at reduction time.
				sourceSequence = provenance.SourceSequence
			}
		} else {
			result, err = aggregate(b, n, q, sequence)
		}
		if err != nil {
			return err
		}
		out = model.Materialization{
			Name: rollup, Query: q, SourceSequence: sourceSequence, Result: result,
			SourceRollup: provenance,
		}
		if provenance != nil {
			out.DerivedFrom = provenance.Name
		}
		return volume.PutJSON(b.Bucket(volume.Rollups), []byte(rollup), out)
	})
	return out, err
}

// deriveRollup reduces an existing rollup's stored cells into a coarser tier.
// It refuses configurations that cannot stay exact: a different origin, a
// step that is not a multiple of the source, a changed grouping, or a source
// that never stored reduction state.
func deriveRollup(b *bolt.Bucket, n model.Namespace, def model.RollupDefinition, sequence uint64) (model.AggregateResult, *model.RollupProvenance, error) {
	source, ok, err := loadRollup(b, def.Source.Rollup)
	if err != nil {
		return model.AggregateResult{}, nil, err
	}
	if !ok {
		return model.AggregateResult{}, nil, model.Missing("source rollup does not exist")
	}
	target := def.Query()
	if !window.Aligned(source.Query.Step, target.Step, source.Query.Origin, target.Origin) {
		return model.AggregateResult{}, nil, model.Bad(
			"target step must be a multiple of the source step and share its origin")
	}
	if !sameGroupBy(source.Query.GroupBy, target.GroupBy) {
		return model.AggregateResult{}, nil, model.Bad("derived rollup must keep the source grouping")
	}
	edge := window.Edge{Start: target.Start, End: target.End}
	if source.Result.Coverage != nil {
		edge = window.Edge{Start: source.Result.Coverage.Start, End: source.Result.Coverage.End}
	}
	// Intersect the source's observed span with the whole target windows the
	// caller asked for. A parent inside the intersection is fully observed; one
	// that reaches past it is partial even though every child it touched was
	// itself complete.
	request := window.Trim(target.Start, target.End, target.Origin, target.Step, model.MinTime)
	if request.Start > edge.Start {
		edge.Start = request.Start
	}
	if request.End < edge.End {
		edge.End = request.End
	}
	kept := make([]model.Cell, 0, len(source.Result.Cells))
	for _, cell := range source.Result.Cells {
		if cell.End > target.Start && cell.Start < target.End {
			kept = append(kept, cell)
		}
	}
	sourceExact := source.SourceSequence == n.LastSequence
	cells, err := window.ReduceCells(kept, source.Query.Step, target.Step,
		source.Query.Origin, target.Origin, edge, n.Floor, target.Function, sourceExact)
	if err != nil {
		return model.AggregateResult{}, nil, err
	}
	provenance := &model.RollupProvenance{
		Name: source.Name, Step: source.Query.Step, Origin: source.Query.Origin,
		SourceSequence: source.SourceSequence, Coverage: source.Result.Coverage,
	}
	return model.AggregateResult{
		Sequence: sequence, Cells: cells, Stats: source.Result.Stats,
		Coverage: coverComplete(cells, n.Floor),
	}, provenance, nil
}

// coverComplete reports the span of complete cells. Start and End stay at zero
// when the source never fully covered any target window.
func coverComplete(cells []model.Cell, floor int64) *model.RollupCoverage {
	coverage := &model.RollupCoverage{Floor: floor}
	for _, cell := range cells {
		if cell.Coverage == nil || !cell.Coverage.Complete {
			continue
		}
		if !coverage.Complete || cell.Start < coverage.Start {
			coverage.Start = cell.Start
		}
		if !coverage.Complete || cell.End > coverage.End {
			coverage.End = cell.End
		}
		coverage.Complete = true
	}
	return coverage
}

func sameGroupBy(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]string(nil), a...)
	right := append([]string(nil), b...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// summarize refreshes retention-dependent facts on a stored materialization and
// derives its freshness and reading rule. The namespace sequence is the only
// source of staleness: advancing the global sequence by materializing another
// rollup does not invalidate this one.
func summarize(b *bolt.Bucket, n model.Namespace, m *model.Materialization) (model.RollupSummary, error) {
	window.RefreshCoverage(m.Result.Cells, n.Floor)
	summary := model.RollupSummary{
		Name: m.Name, Step: m.Query.Step, Function: m.Query.Function,
		Origin: m.Query.Origin, GroupBy: m.Query.GroupBy,
		DerivedFrom: m.DerivedFrom, Coverage: m.Result.Coverage,
		Cells: len(m.Result.Cells), Stale: m.SourceSequence != n.LastSequence,
		Derivable: reducibleFunctions(),
	}
	if m.DerivedFrom != "" {
		source, ok, err := loadRollup(b, m.DerivedFrom)
		if err != nil {
			return summary, err
		}
		summary.SourceStale = !ok || m.SourceRollup == nil ||
			source.SourceSequence != m.SourceRollup.SourceSequence
	}
	for _, cell := range m.Result.Cells {
		if cell.Coverage == nil {
			continue
		}
		if cell.Coverage.Exact {
			summary.ExactCells++
		}
		if !cell.Coverage.Complete {
			summary.PartialCells++
		}
		if cell.Coverage.SourceExpired {
			summary.ExpiredCells++
		}
	}
	expired, partial := coverageSummary(m.Result.Cells)
	summary.ReadRule = window.SummaryRule(expired, partial,
		summary.Stale || summary.SourceStale, m.DerivedFrom != "")
	return summary, nil
}

// Rollup returns a materialization with freshness, coverage and the reading
// rule a client should apply. Retention-dependent facts are recomputed here,
// so a stored rollup never claims to be recomputable after its source expired
// and never presents a stale result as current.
func (e *Engine) Rollup(name, rollup string) (model.RollupView, error) {
	var out model.RollupView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		m, ok, err := loadRollup(b, rollup)
		if err != nil {
			return err
		}
		if !ok {
			return model.Missing("rollup does not exist")
		}
		summary, err := summarize(b, n, &m)
		if err != nil {
			return err
		}
		out.Materialization = m
		out.Result.Cells = m.Result.Cells
		out.Stale = summary.Stale
		out.SourceStale = summary.SourceStale
		out.ReadRule = summary.ReadRule
		out.Derivable = summary.Derivable
		if m.DerivedFrom != "" {
			source, ok, err := loadRollup(b, m.DerivedFrom)
			if err != nil {
				return err
			}
			if ok {
				sourceSummary, err := summarize(b, n, &source)
				if err != nil {
					return err
				}
				out.SourceReadRule = sourceSummary.ReadRule
			} else {
				out.SourceReadRule = window.RuleSourceExpired
			}
			out.Suggestion = suggestTier(m)
		}
		return nil
	})
	return out, err
}

// coverageSummary reports whether no cell can still be recomputed and whether
// any cell clipped its window.
func coverageSummary(cells []model.Cell) (expired, partial bool) {
	recomputable := false
	for _, cell := range cells {
		if cell.Coverage == nil {
			return false, false
		}
		if cell.Coverage.Recomputable {
			recomputable = true
		}
		if !cell.Coverage.Complete {
			partial = true
		}
	}
	return len(cells) > 0 && !recomputable, partial
}

func reducibleFunctions() []string {
	return []string{"count", "sum", "avg", "min", "max"}
}

// suggestTier explains how to make an inexact tier exact. A stale or
// retention-expired tier cannot be repaired by re-aligning bounds; it must be
// refreshed from a source that still covers the interval.
func suggestTier(m model.Materialization) *model.RollupSuggestion {
	if m.Result.Coverage == nil {
		return nil
	}
	partial := false
	for _, cell := range m.Result.Cells {
		if cell.Coverage == nil {
			return nil
		}
		if cell.Coverage.SourceExpired {
			return &model.RollupSuggestion{
				Step: m.Query.Step,
				Reason: "raw source behind this tier has been purged by retention; " +
					"rebuild it from a coarser tier that still covers the interval",
			}
		}
		if !cell.Coverage.Complete {
			partial = true
		}
	}
	if partial {
		return &model.RollupSuggestion{
			Step: m.Query.Step,
			Reason: "a window is clipped by the requested range; align start and end " +
				"to window boundaries to obtain an exact tier",
		}
	}
	return nil
}

// Rollups lists every materialization for a namespace with its granularity,
// coverage and reading rule, so tiers can be compared side by side.
func (e *Engine) Rollups(name string) ([]model.RollupSummary, error) {
	out := []model.RollupSummary{}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		err = b.Bucket(volume.Rollups).ForEach(func(k, _ []byte) error {
			var m model.Materialization
			if err := volume.GetJSON(b.Bucket(volume.Rollups), k, &m); err != nil {
				return err
			}
			summary, err := summarize(b, n, &m)
			if err != nil {
				return err
			}
			out = append(out, summary)
			return nil
		})
		return err
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Step == out[j].Step {
			return out[i].Name < out[j].Name
		}
		return out[i].Step < out[j].Step
	})
	return out, err
}
