package window

import (
	"math"

	"ridgeline/internal/model"
)

// Reducible reports whether a function's stored state can produce a coarser
// tier exactly. First, last and rate depend on raw observation order or
// adjacency, so they are not tier-reducible.
func Reducible(function string) bool {
	switch function {
	case "count", "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
}

// Aligned reports whether a coarser tier can be built from a stored tier.
// The source step must divide the target step and both tiers must share an
// origin; otherwise reduced windows would silently mix different spans of time.
func Aligned(sourceStep, targetStep, sourceOrigin, targetOrigin int64) bool {
	return sourceStep > 0 && targetStep >= sourceStep &&
		targetStep%sourceStep == 0 && sourceOrigin == targetOrigin
}

// Edge is the half-open interval a tier fully observed. Windows outside it were
// clipped by the requested range or by the retention floor, so they hold the
// observations that were in range but not a complete window.
type Edge struct {
	Start int64
	End   int64
}

// Trim returns the whole windows a query fully observes. A window counts as
// fully observed only when it lies entirely inside the requested range and at
// or above the retention floor. Windows at the edges are frequently clipped, so
// they are reported as partial rather than silently treated as full windows.
func Trim(start, end, origin, step, floor int64) Edge {
	low := start
	if floor > low {
		low = floor
	}
	edgeStart := Bucket(low, origin, step)
	if edgeStart < low {
		edgeStart += step
	}
	edgeEnd := Bucket(end-1, origin, step) + step
	if edgeEnd > end {
		edgeEnd -= step
	}
	return Edge{Start: edgeStart, End: edgeEnd}
}

// MarkCoverage records, for cells computed directly from raw points, whether
// each cell's window was fully observed and whether raw source behind it is
// still retained. Expiry is evaluated against floor; exactness does not depend
// on it because the value was computed from the points that existed.
func MarkCoverage(cells []model.Cell, q model.Query, floor int64) {
	edge := Trim(q.Start, q.End, q.Origin, q.Step, floor)
	for i := range cells {
		c := &cells[i]
		c.Coverage = &model.CellCoverage{
			ExpectedParts: 1, PresentParts: 1,
			PartialStart: c.Start < edge.Start,
			PartialEnd:   c.End > edge.End,
		}
		c.Coverage.Complete = !c.Coverage.PartialStart && !c.Coverage.PartialEnd
		c.Coverage.Exact = c.Coverage.Complete
		c.Coverage.SourceExpired = floor > c.Start
		c.Coverage.Recomputable = c.Coverage.Complete && !c.Coverage.SourceExpired
		c.Coverage.Rule = CellRule(c.Coverage)
	}
}

type parentKey struct {
	group string
	start int64
}

type parentState struct {
	cell    *model.Cell
	present int
}

// ReduceCells derives a coarser tier from stored cell reductions.
//
// Children are merged by count and sum, so a parent average is weighted by raw
// observation count rather than by child-window count. sourceStep/sourceOrigin
// describe the tier the cells came from; targetStep must be a positive multiple
// of it and the target must share the source origin. edge is the interval the
// source tier fully observed: a parent inside edge is complete even when some
// of its child windows held no samples, because an absent window means "no
// observations", not "missing data". A parent reaching outside edge is partial.
//
// floor is the current retention floor. A parent whose window starts below it
// has lost raw source to retention, so source_expired is set; the stored
// reduction is still arithmetically exact and the parent can still be reduced
// again from the source rollup. sourceExact reports whether the source tier
// still matched the raw data when reduced; when it did not, the parent is not
// exact.
func ReduceCells(cells []model.Cell, sourceStep, targetStep, sourceOrigin, targetOrigin int64,
	edge Edge, floor int64, function string, sourceExact bool) ([]model.Cell, error) {
	if !Aligned(sourceStep, targetStep, sourceOrigin, targetOrigin) {
		return nil, model.Bad("target step and origin must align to the source rollup")
	}
	if !Reducible(function) {
		return nil, model.Bad("rollup tiers support count, sum, avg, min and max")
	}
	groups := map[parentKey]*parentState{}
	order := make([]parentKey, 0)
	for _, cell := range cells {
		if cell.Reduction == nil {
			return nil, model.Bad("source rollup does not contain reducible state")
		}
		k := parentKey{group: GroupKey(cell.Labels), start: Bucket(cell.Start, targetOrigin, targetStep)}
		state := groups[k]
		if state == nil {
			state = &parentState{cell: &model.Cell{
				Labels: cell.Labels, Start: k.start, End: k.start + targetStep,
				Reduction: &model.Reduction{Min: math.Inf(1), Max: math.Inf(-1)},
			}}
			groups[k] = state
			order = append(order, k)
		}
		state.cell.Reduction.Count += cell.Reduction.Count
		state.cell.Reduction.Sum += cell.Reduction.Sum
		state.cell.Reduction.Min = math.Min(state.cell.Reduction.Min, cell.Reduction.Min)
		state.cell.Reduction.Max = math.Max(state.cell.Reduction.Max, cell.Reduction.Max)
		state.cell.Count += cell.Count
		state.present++
	}
	out := make([]model.Cell, 0, len(groups))
	for _, k := range order {
		state := groups[k]
		cell := state.cell
		cell.Value = reduceValue(cell.Reduction, function)
		cell.Coverage = &model.CellCoverage{
			ExpectedParts: int(targetStep / sourceStep),
			PresentParts:  state.present,
			PartialStart:  cell.Start < edge.Start,
			PartialEnd:    cell.End > edge.End,
		}
		cell.Coverage.Complete = !cell.Coverage.PartialStart && !cell.Coverage.PartialEnd
		cell.Coverage.Exact = cell.Coverage.Complete && sourceExact
		cell.Coverage.SourceExpired = floor > cell.Start
		// Recomputable means the window can be rebuilt exactly from currently
		// retained raw points. A stale source or an advanced floor both remove
		// that ability even though the stored reduction remains self-consistent.
		cell.Coverage.Recomputable = cell.Coverage.Exact && !cell.Coverage.SourceExpired
		cell.Coverage.Rule = CellRule(cell.Coverage)
		if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
			return nil, model.Capacity("rollup reduction overflow")
		}
		out = append(out, *cell)
	}
	SortCells(out)
	return out, nil
}

// Read rules are the stable vocabulary reported to clients. Per-cell rules are
// ordered by severity: purged source dominates a partial window, which
// dominates an exact one.
const (
	RuleExact          = "exact"
	RulePartial        = "partial"
	RuleSourceExpired  = "source_expired"
	RuleStale          = "stale"
	RuleDerivedExact   = "derived_exact"
	RuleDerivedStale   = "derived_stale"
	RuleDerivedExpired = "derived_source_expired"
)

// CellRule labels one cell's reading rule from its coverage facts.
func CellRule(c *model.CellCoverage) string {
	switch {
	case c.SourceExpired:
		return RuleSourceExpired
	case !c.Complete:
		return RulePartial
	case !c.Exact:
		return RuleStale
	default:
		return RuleExact
	}
}

// RefreshCoverage re-evaluates retention-dependent facts against the current
// floor. Exactness is fixed when the tier is built; expiry and recomputability
// change as retention advances, so stored cells must be refreshed on read
// rather than trusted from the write path.
func RefreshCoverage(cells []model.Cell, floor int64) {
	for i := range cells {
		c := cells[i].Coverage
		if c == nil {
			continue
		}
		c.SourceExpired = floor > cells[i].Start
		c.Recomputable = c.Complete && !c.SourceExpired
		c.Rule = CellRule(c)
	}
}

// SummaryRule selects the single reading rule reported for a materialization.
// expired means no cell can still be recomputed from retained raw source;
// partial means at least one cell clips its window; stale means the stored
// result no longer matches current source; derived marks a tier reduced from
// another rollup. Severity runs expired, stale, partial, otherwise exact.
func SummaryRule(expired, partial, stale, derived bool) string {
	switch {
	case derived && expired:
		return RuleDerivedExpired
	case derived && stale:
		return RuleDerivedStale
	case expired:
		return RuleSourceExpired
	case stale:
		return RuleStale
	case partial:
		return RulePartial
	case derived:
		return RuleDerivedExact
	default:
		return RuleExact
	}
}

// reduceValue derives a display value from reduction state so one stored tier
// can expose any reducible function without re-reading raw points.
func reduceValue(r *model.Reduction, function string) float64 {
	switch function {
	case "count":
		return float64(r.Count)
	case "sum":
		return r.Sum
	case "avg":
		return r.Sum / float64(r.Count)
	case "min":
		return r.Min
	case "max":
		return r.Max
	}
	return math.NaN()
}
