package engine

import (
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

func aggregate(b *bolt.Bucket, view readView, q model.Query) (model.AggregateResult, model.ScanEvidence, error) {
	if err := window.Validate(q); err != nil {
		return model.AggregateResult{}, model.ScanEvidence{}, err
	}
	// A raw result limit must not bias reducers.
	q.Limit = 0
	raw, evidence, err := scan(b, view, q)
	if err != nil {
		return model.AggregateResult{}, evidence, err
	}
	cells, err := window.Aggregate(raw.Series, q)
	return model.AggregateResult{Sequence: view.sequence, Cells: cells,
		Stats: raw.Stats, Evidence: &evidence}, evidence, err
}

func (e *Engine) Aggregate(name string, q model.Query) (model.AggregateResult, error) {
	var out model.AggregateResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		view, err := loadView(b, n, tx)
		if err != nil {
			return err
		}
		out, _, err = aggregate(b, view, q)
		return err
	})
	return out, err
}

// Plan is the explain response. Candidates and Blocks stay for compatibility;
// the plan-phase evidence below adds the decision material: exact decode cost,
// budget projection, narrowing verdicts and per-series certainty.
type Plan struct {
	Sequence        uint64            `json:"sequence"`
	Floor           int64             `json:"floor"`
	ResultLimit     int               `json:"result_limit"`
	Candidates      []model.Series    `json:"candidates"`
	Blocks          []model.Block     `json:"blocks"`
	MaxDecoded      int               `json:"max_decoded"`
	SourcePointHint int               `json:"source_point_hint"`
	HeadRecords     int               `json:"head_records"`
	Totals          model.PlanTotals  `json:"totals"`
	Budget          BudgetPlan        `json:"budget"`
	Narrowing       PlanNarrowing     `json:"narrowing"`
	Storage         StorageAdvice     `json:"storage"`
	Series          []SeriesEvidence  `json:"series"`
	Alternatives    []AlternativePlan `json:"alternatives"`
	Caveats         []string          `json:"caveats"`
}

func (e *Engine) Explain(name string, req model.ExplainRequest) (Plan, error) {
	q := req.Query
	var plan Plan
	if err := model.ValidateQuery(q); err != nil {
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
		view, err := loadView(b, n, tx)
		if err != nil {
			return err
		}
		plan.Sequence, plan.Floor, plan.MaxDecoded = view.sequence, n.Floor, n.MaxScan
		plan.ResultLimit = q.Limit
		plan.Candidates = selectseries.Pick(view.catalog, selector)
		plan.Blocks = []model.Block{}
		// HeadRecords is the namespace-wide key count, kept as a rough size
		// signal. Totals.head_records is the exact in-interval count for
		// candidate series and is the number to budget against.
		plan.HeadRecords = b.Bucket(volume.Head).Stats().KeyN
		selected := make(map[string]bool, len(plan.Candidates))
		for _, s := range plan.Candidates {
			selected[s.ID] = true
		}
		for _, block := range view.blocks {
			if selected[block.SeriesID] &&
				BlockIntersects(block, q.Start, q.End, n.Floor) {
				plan.Blocks = append(plan.Blocks, block)
				plan.SourcePointHint += block.Count
			}
		}
		evidence, err := buildPlan(b, n, view.sequence, view.catalog, selector, q,
			view.blocks, view.tombstones, req.Alternatives)
		if err != nil {
			return err
		}
		plan.Totals = evidence.Totals
		plan.Budget = evidence.Budget
		plan.Narrowing = evidence.Narrowing
		plan.Storage = evidence.Storage
		plan.Series = evidence.Series
		plan.Alternatives = evidence.Alternatives
		plan.Caveats = evidence.Caveats
		return nil
	})
	return plan, err
}
