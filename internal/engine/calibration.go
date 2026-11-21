package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// PutCalibration defines or replaces a named calibration view. The raw
// samples are never touched: the view is applied only when a read, aggregate
// or rollup explicitly selects it. Replacing a view bumps the database
// sequence so materializations built against the old coefficients report
// stale instead of silently presenting corrected numbers.
func (e *Engine) PutCalibration(namespace string, view model.Calibration) (model.Calibration, error) {
	if err := model.ValidateCalibration(view); err != nil {
		return view, err
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, namespace)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		if err := rejectRuleOverlap(b, view); err != nil {
			return err
		}
		if loaded, err := loadCalibration(b, view.Name); err == nil {
			view.Revision = loaded.Revision + 1
		} else {
			view.Revision = 1
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		view.Sequence = seq
		// A view definition belongs to the namespace's mutation history:
		// advancing its sequence marks rollups materialized against the old
		// coefficients stale without affecting other namespaces.
		n.LastSequence = seq
		bucket, err := volume.NamespaceBucket(b, volume.Calibrations)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(bucket, []byte(view.Name), view); err != nil {
			return err
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return view, err
}

// rejectRuleOverlap resolves every rule selector against the current catalog
// and rejects the view if two rules could calibrate the same series. A
// segment boundary inside one rule is a clean handoff; ambiguity across rules
// would make the effective coefficient depend on rule ordering, which the
// API refuses instead of resolving implicitly. A series admitted later that
// happens to match several stored rules is still deterministic at read time:
// resolve visits rules in stored order and uses the first match, and re-putting
// the view surfaces the new overlap as an explicit 400 for correction.
func rejectRuleOverlap(b *bolt.Bucket, view model.Calibration) error {
	all, err := catalog(b)
	if err != nil {
		return err
	}
	owners := map[string]int{}
	for ri, rule := range view.Rules {
		selector, err := selectseries.Compile(rule.Matchers)
		if err != nil {
			return err
		}
		for _, s := range selectseries.Pick(all, selector) {
			if prev, ok := owners[s.ID]; ok {
				return model.Bad(fmt.Sprintf(
					"calibration rules %d and %d both match series %s; rule selectors must not overlap",
					prev, ri, s.ID))
			}
			owners[s.ID] = ri
		}
	}
	return nil
}

// GetCalibration returns a stored view definition with its revision.
func (e *Engine) GetCalibration(namespace, view string) (model.Calibration, error) {
	var out model.Calibration
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, namespace)
		if err != nil {
			return err
		}
		loaded, err := loadCalibration(b, view)
		if err != nil {
			return err
		}
		out = loaded
		return nil
	})
	return out, err
}

// ListCalibrations returns every view definition ordered by name.
func (e *Engine) ListCalibrations(namespace string) ([]model.Calibration, error) {
	out := make([]model.Calibration, 0)
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, namespace)
		if err != nil {
			return err
		}
		bucket, err := volume.NamespaceBucket(b, volume.Calibrations)
		if err != nil {
			return err
		}
		return bucket.ForEach(func(_, value []byte) error {
			var view model.Calibration
			if err := json.Unmarshal(value, &view); err != nil {
				return err
			}
			out = append(out, view)
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

func loadCalibration(b *bolt.Bucket, view string) (model.Calibration, error) {
	var out model.Calibration
	bucket, err := volume.NamespaceBucket(b, volume.Calibrations)
	if err != nil {
		return out, err
	}
	data := bucket.Get([]byte(view))
	if data == nil {
		return out, model.Missing("calibration view does not exist")
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

// compiledRule resolves a stored selector once per query against the current
// catalog and keeps its coefficient intervals ordered for lookup.
type compiledRule struct {
	series   map[string]bool
	segments []model.CalibrationSegment
}

type calibrationResolver struct {
	view  string
	rules []compiledRule
}

// compileCalibration resolves view selectors to concrete series inside the
// read transaction. Selector semantics are identical to series discovery.
func compileCalibration(b *bolt.Bucket, view model.Calibration) (*calibrationResolver, error) {
	all, err := catalog(b)
	if err != nil {
		return nil, err
	}
	resolver := &calibrationResolver{view: view.Name}
	for _, rule := range view.Rules {
		selector, err := selectseries.Compile(rule.Matchers)
		if err != nil {
			return nil, err
		}
		compiled := compiledRule{series: map[string]bool{}, segments: rule.Segments}
		for _, s := range selectseries.Pick(all, selector) {
			compiled.series[s.ID] = true
		}
		resolver.rules = append(resolver.rules, compiled)
	}
	return resolver, nil
}

// resolve returns the effective segment for one series at the observation
// time. Lookup is purely by observation timestamp, independent of ingest or
// arrival order, so late data is calibrated exactly as on-time data.
// Half-open [start, end) intervals make adjacent segments join seamlessly:
// a boundary timestamp belongs to the later segment. When no segment covers
// the time, ok is false and the caller passes the raw value through.
func (r *calibrationResolver) resolve(seriesID string, t int64) (model.CalibrationSegment, bool) {
	for _, rule := range r.rules {
		if !rule.series[seriesID] {
			continue
		}
		// Segments are validated non-overlapping with ascending starts.
		// Find the last segment whose start is not after t.
		idx := sort.Search(len(rule.segments), func(i int) bool {
			return rule.segments[i].Start > t
		}) - 1
		if idx >= 0 {
			seg := rule.segments[idx]
			if t >= seg.Start && t < seg.End {
				return seg, true
			}
		}
	}
	return model.CalibrationSegment{}, false
}

// apply transforms one raw sample into a traceable calibrated point. A
// non-finite result is reported instead of silently degrading to the raw
// value, matching the overflow policy of window aggregation.
func (r *calibrationResolver) apply(seriesID string, p model.Sample) (model.CalibratedPoint, error) {
	out := model.CalibratedPoint{
		Time: p.Time, Value: p.Value, RawValue: p.Value,
		Factor: 1, Offset: 0, Calibrated: false, Sequence: p.Sequence,
	}
	if seg, ok := r.resolve(seriesID, p.Time); ok {
		calibrated := seg.Factor*p.Value + seg.Offset
		if math.IsInf(calibrated, 0) || math.IsNaN(calibrated) {
			return out, model.Capacity("calibration produced a non-finite value")
		}
		out.Value, out.Factor, out.Offset, out.Calibrated =
			calibrated, seg.Factor, seg.Offset, true
	}
	return out, nil
}

// CalibratedRange runs the identical raw scan as Range, then projects every
// visible point through the named view. Raw point storage and ordinary reads
// are unaffected.
func (e *Engine) CalibratedRange(namespace string, q model.Query) (model.CalibratedRangeResult, error) {
	var out model.CalibratedRangeResult
	if q.Calibration == "" {
		return out, model.Bad("calibrated range requires a calibration view name")
	}
	if err := model.ValidateQuery(q); err != nil {
		return out, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, namespace)
		if err != nil {
			return err
		}
		view, err := loadCalibration(b, q.Calibration)
		if err != nil {
			return err
		}
		resolver, err := compileCalibration(b, view)
		if err != nil {
			return err
		}
		raw, err := scan(b, n, q, volume.Current(tx))
		if err != nil {
			return err
		}
		out = model.CalibratedRangeResult{
			Sequence: raw.Sequence, Floor: raw.Floor, Calibration: view.Name,
			Series: []model.CalibratedSeriesPoints{}, Stats: raw.Stats,
			Truncated: raw.Truncated,
		}
		for _, sp := range raw.Series {
			cp := model.CalibratedSeriesPoints{Series: sp.Series, Points: make([]model.CalibratedPoint, 0, len(sp.Points))}
			for _, p := range sp.Points {
				projected, err := resolver.apply(sp.Series.ID, p)
				if err != nil {
					return err
				}
				cp.Points = append(cp.Points, projected)
			}
			if len(cp.Points) > 0 {
				out.Series = append(out.Series, cp)
			}
		}
		return nil
	})
	return out, err
}
