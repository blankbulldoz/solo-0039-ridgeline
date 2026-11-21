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
	cells, err := window.Aggregate(raw.Series, q)
	return model.AggregateResult{Sequence: sequence, Cells: cells, Stats: raw.Stats}, err
}

// calibratedAggregate projects the same raw source points through a named
// calibration view before reducing. Windows therefore see calibrated values
// while the stored samples stay untouched.
func calibratedAggregate(b *bolt.Bucket, n model.Namespace, q model.Query, sequence uint64) (model.AggregateResult, error) {
	if err := window.Validate(q); err != nil {
		return model.AggregateResult{}, err
	}
	if q.Calibration == "" {
		return model.AggregateResult{}, model.Bad("calibrated aggregate requires a calibration view name")
	}
	view, err := loadCalibration(b, q.Calibration)
	if err != nil {
		return model.AggregateResult{}, err
	}
	resolver, err := compileCalibration(b, view)
	if err != nil {
		return model.AggregateResult{}, err
	}
	q.Limit = 0
	raw, err := scan(b, n, q, sequence)
	if err != nil {
		return model.AggregateResult{}, err
	}
	source := make([]model.SeriesPoints, 0, len(raw.Series))
	for _, sp := range raw.Series {
		projected := model.SeriesPoints{Series: sp.Series, Points: make([]model.Sample, 0, len(sp.Points))}
		for _, p := range sp.Points {
			cp, err := resolver.apply(sp.Series.ID, p)
			if err != nil {
				return model.AggregateResult{}, err
			}
			projected.Points = append(projected.Points, model.Sample{
				Time: cp.Time, Value: cp.Value, Sequence: cp.Sequence,
			})
		}
		source = append(source, projected)
	}
	cells, err := window.Aggregate(source, q)
	if err != nil {
		return model.AggregateResult{}, err
	}
	return model.AggregateResult{Sequence: sequence, Cells: cells, Stats: raw.Stats}, nil
}

func (e *Engine) Aggregate(name string, q model.Query) (model.AggregateResult, error) {
	var out model.AggregateResult
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if q.Calibration != "" {
			out, err = calibratedAggregate(b, n, q, volume.Current(tx))
		} else {
			out, err = aggregate(b, n, q, volume.Current(tx))
		}
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
