package engine

import (
	"errors"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// spaceFailure maps a per-namespace read failure to a stable status code so
// one unreadable space never hides the remaining spaces in a joint response.
func spaceFailure(err error) (string, string) {
	var domain *model.Error
	if errors.As(err, &domain) {
		return domain.Code, domain.Detail
	}
	return "error", "namespace read failed"
}

func validateFederated(spaces []string) error {
	if len(spaces) == 0 || len(spaces) > 32 {
		return model.Bad("federated query requires 1..32 namespaces")
	}
	seen := map[string]bool{}
	for _, name := range spaces {
		if !model.ValidName(name) {
			return model.Bad("invalid namespace name in federated query")
		}
		if seen[name] {
			return model.Bad("duplicate namespace in federated query")
		}
		seen[name] = true
	}
	return nil
}

// FederatedRange reads one half-open interval across namespaces inside a
// single bbolt read transaction: every space in the response reflects the
// same committed database sequence. Each space applies its own retention
// floor, scan budget and result limit; a missing or over-budget space is
// reported with an explicit status instead of failing the joint read.
func (e *Engine) FederatedRange(q model.FederatedQuery) (model.FederatedRangeResult, error) {
	out := model.FederatedRangeResult{Spaces: []model.SpaceRangeResult{}}
	if err := validateFederated(q.Spaces); err != nil {
		return out, err
	}
	if err := model.ValidateQuery(q.Query); err != nil {
		return out, err
	}
	if _, err := selectseries.Compile(q.Matchers); err != nil {
		return out, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		out.Sequence = volume.Current(tx)
		for _, name := range q.Spaces {
			space := model.SpaceRangeResult{
				SpaceStatus: model.SpaceStatus{Space: name, Status: "ok"},
				Series:      []model.SeriesPoints{},
			}
			b, n, err := loadNamespace(tx, name)
			if err != nil {
				space.Status, space.Detail = spaceFailure(err)
				out.Spaces = append(out.Spaces, space)
				continue
			}
			space.ReadOnly, space.Floor, space.MaxScan = n.ReadOnly, n.Floor, n.MaxScan
			result, err := scan(b, n, q.Query, out.Sequence)
			space.Stats = result.Stats
			if err != nil {
				// Budget and read failures publish no partial series.
				space.Status, space.Detail = spaceFailure(err)
				out.Spaces = append(out.Spaces, space)
				continue
			}
			space.Series = result.Series
			space.Truncated = result.Truncated
			out.Spaces = append(out.Spaces, space)
		}
		return nil
	})
	return out, err
}

// FederatedAggregate groups each namespace independently inside one read
// transaction, then tags every cell with its source space. Identical group
// labels in different spaces therefore produce adjacent but distinct cells;
// groups never blend measurements across namespace boundaries.
func (e *Engine) FederatedAggregate(q model.FederatedQuery) (model.FederatedAggregateResult, error) {
	out := model.FederatedAggregateResult{Cells: []model.Cell{}, Spaces: []model.SpaceStatus{}}
	if err := validateFederated(q.Spaces); err != nil {
		return out, err
	}
	if err := window.Validate(q.Query); err != nil {
		return out, err
	}
	if _, err := selectseries.Compile(q.Matchers); err != nil {
		return out, err
	}
	// A raw result limit must not bias reducers.
	q.Limit = 0
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		out.Sequence = volume.Current(tx)
		for _, name := range q.Spaces {
			space := model.SpaceStatus{Space: name, Status: "ok"}
			b, n, err := loadNamespace(tx, name)
			if err != nil {
				space.Status, space.Detail = spaceFailure(err)
				out.Spaces = append(out.Spaces, space)
				continue
			}
			space.ReadOnly, space.Floor, space.MaxScan = n.ReadOnly, n.Floor, n.MaxScan
			raw, err := scan(b, n, q.Query, out.Sequence)
			space.Stats = raw.Stats
			if err != nil {
				space.Status, space.Detail = spaceFailure(err)
				out.Spaces = append(out.Spaces, space)
				continue
			}
			cells, err := window.Aggregate(raw.Series, q.Query)
			if err != nil {
				space.Status, space.Detail = spaceFailure(err)
				out.Spaces = append(out.Spaces, space)
				continue
			}
			for _, cell := range cells {
				cell.Space = name
				out.Cells = append(out.Cells, cell)
			}
			out.Spaces = append(out.Spaces, space)
		}
		window.SortFederatedCells(out.Cells)
		return nil
	})
	return out, err
}
