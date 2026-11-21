package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// Continuous aggregates keep a defined window query current incrementally.
// Mutations record one invalidation entry per affected window in the same
// transaction that commits them, so a restart cannot lose pending repairs.
// Refresh recomputes invalidated windows oldest first, bounded per call.

func continuousDefs(b *bolt.Bucket) ([]model.Continuous, error) {
	out := make([]model.Continuous, 0)
	bucket := b.Bucket(volume.Continuous)
	if bucket == nil {
		return out, nil
	}
	cursor := bucket.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var def model.Continuous
		if err := json.Unmarshal(v, &def); err != nil {
			return nil, err
		}
		out = append(out, def)
	}
	return out, nil
}

func ensureContinuousBuckets(b *bolt.Bucket) (defs, wins, invs *bolt.Bucket, err error) {
	if defs, err = b.CreateBucketIfNotExists(volume.Continuous); err != nil {
		return nil, nil, nil, err
	}
	if wins, err = b.CreateBucketIfNotExists(volume.CWindows); err != nil {
		return nil, nil, nil, err
	}
	if invs, err = b.CreateBucketIfNotExists(volume.CInvalid); err != nil {
		return nil, nil, nil, err
	}
	return defs, wins, invs, nil
}

// overlaps reports whether window [start, start+step) intersects the
// half-open definition range of q.
func overlaps(q model.Query, start, step int64) bool {
	return start < q.End && start+step > q.Start
}

func putInvalidation(invs *bolt.Bucket, def model.Continuous, start int64, seq uint64) error {
	bucket, err := invs.CreateBucketIfNotExists([]byte(def.Name))
	if err != nil {
		return err
	}
	entry := model.Invalidation{Start: start, Sequence: seq}
	return volume.PutJSON(bucket, volume.WindowKey(start), entry)
}

// loadInvalidationTargets returns the definitions and buckets needed to
// record invalidations, or nil definitions when nothing is defined.
func invalidationTargets(b *bolt.Bucket) ([]model.Continuous, *bolt.Bucket, *bolt.Bucket, error) {
	if b.Bucket(volume.Continuous) == nil {
		return nil, nil, nil, nil
	}
	defs, err := continuousDefs(b)
	if err != nil || len(defs) == 0 {
		return nil, nil, nil, err
	}
	_, wins, invs, err := ensureContinuousBuckets(b)
	if err != nil {
		return nil, nil, nil, err
	}
	return defs, wins, invs, nil
}

// invalidatePoints records one entry per written point per defined
// aggregate. Corrections at an already-committed timestamp take the same
// path: the window is later recomputed from current visible state.
func invalidatePoints(b *bolt.Bucket, points []model.Observation, seq uint64) error {
	defs, _, invs, err := invalidationTargets(b)
	if err != nil {
		return err
	}
	for _, def := range defs {
		step := def.Query.Step
		for _, p := range points {
			start := window.Bucket(p.Time, def.Query.Origin, step)
			if !overlaps(def.Query, start, step) {
				continue
			}
			if err := putInvalidation(invs, def, start, seq); err != nil {
				return err
			}
		}
	}
	return nil
}

// invalidateDelete flags materialized windows overlapping [start, end).
// Windows that have source points but no record yet already carry a pending
// entry from the write that produced them, so deletes and retention only
// need to revisit existing records.
func invalidateDelete(b *bolt.Bucket, start, end int64, seq uint64) error {
	defs, wins, invs, err := invalidationTargets(b)
	if err != nil {
		return err
	}
	for _, def := range defs {
		records := wins.Bucket([]byte(def.Name))
		if records == nil {
			continue
		}
		step := def.Query.Step
		first := window.Bucket(start, def.Query.Origin, step)
		cursor := records.Cursor()
		for k, _ := cursor.Seek(volume.WindowKey(first)); k != nil; k, _ = cursor.Next() {
			wstart, err := volume.WindowStart(k)
			if err != nil {
				return err
			}
			if wstart >= end {
				break
			}
			if err := putInvalidation(invs, def, wstart, seq); err != nil {
				return err
			}
		}
	}
	return nil
}

// invalidateBelowFloor flags materialized windows that start below the new
// retention floor. Recomputation truncates straddling windows and drops
// windows the floor has fully passed.
func invalidateBelowFloor(b *bolt.Bucket, floor int64, seq uint64) error {
	defs, wins, invs, err := invalidationTargets(b)
	if err != nil {
		return err
	}
	for _, def := range defs {
		records := wins.Bucket([]byte(def.Name))
		if records == nil {
			continue
		}
		cursor := records.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			wstart, err := volume.WindowStart(k)
			if err != nil {
				return err
			}
			if wstart >= floor {
				break
			}
			if err := putInvalidation(invs, def, wstart, seq); err != nil {
				return err
			}
		}
	}
	return nil
}

// DefineContinuous stores the query and materializes its current output,
// one record per populated window. Later mutations repair it through the
// invalidation log instead of full recomputation.
func (e *Engine) DefineContinuous(name, agg string, q model.Query) (model.ContinuousView, error) {
	var view model.ContinuousView
	if !model.ValidName(agg) {
		return view, model.Bad("invalid continuous aggregate name")
	}
	if err := window.Validate(q); err != nil {
		return view, err
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		defs, wins, _, err := ensureContinuousBuckets(b)
		if err != nil {
			return err
		}
		if defs.Get([]byte(agg)) != nil {
			return model.Conflict("continuous aggregate already exists")
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		result, err := aggregate(b, n, q, seq)
		if err != nil {
			return err
		}
		records, err := wins.CreateBucket([]byte(agg))
		if err != nil {
			return err
		}
		byWindow := map[int64][]model.Cell{}
		for _, cell := range result.Cells {
			byWindow[cell.Start] = append(byWindow[cell.Start], cell)
		}
		for start, cells := range byWindow {
			record := model.WindowRecord{
				Start: start, End: start + q.Step, Sequence: seq, Cells: cells,
			}
			if err := volume.PutJSON(records, volume.WindowKey(start), record); err != nil {
				return err
			}
		}
		def := model.Continuous{Name: agg, Query: q, SourceSequence: n.LastSequence}
		if err := volume.PutJSON(defs, []byte(agg), def); err != nil {
			return err
		}
		view, err = continuousView(b, def, volume.Current(tx))
		return err
	})
	return view, err
}

// continuousView separates usable windows from those awaiting refresh.
// Pending lists invalidated window starts in chronological order, including
// windows that have no materialized record yet.
func continuousView(b *bolt.Bucket, def model.Continuous, sequence uint64) (model.ContinuousView, error) {
	view := model.ContinuousView{
		Continuous: def, Sequence: sequence,
		Windows: []model.WindowView{}, Pending: []int64{},
	}
	pending := map[int64]bool{}
	if invs := b.Bucket(volume.CInvalid); invs != nil {
		if bucket := invs.Bucket([]byte(def.Name)); bucket != nil {
			cursor := bucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				start, err := volume.WindowStart(k)
				if err != nil {
					return view, err
				}
				pending[start] = true
				view.Pending = append(view.Pending, start)
			}
		}
	}
	view.PendingCount = len(view.Pending)
	if wins := b.Bucket(volume.CWindows); wins != nil {
		if records := wins.Bucket([]byte(def.Name)); records != nil {
			cursor := records.Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				var record model.WindowRecord
				if err := json.Unmarshal(v, &record); err != nil {
					return view, err
				}
				view.Windows = append(view.Windows, model.WindowView{
					WindowRecord: record, Pending: pending[record.Start],
				})
			}
		}
	}
	return view, nil
}

func (e *Engine) Continuous(name, agg string) (model.ContinuousView, error) {
	var view model.ContinuousView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		defs := b.Bucket(volume.Continuous)
		if defs == nil || defs.Get([]byte(agg)) == nil {
			return model.Missing("continuous aggregate does not exist")
		}
		var def model.Continuous
		if err := volume.GetJSON(defs, []byte(agg), &def); err != nil {
			return err
		}
		view, err = continuousView(b, def, volume.Current(tx))
		return err
	})
	return view, err
}

// RefreshContinuous repairs invalidated windows oldest first. A positive
// maxWindows bounds the work of one call; zero processes the whole backlog.
// Each recomputed window is clipped to the definition range, so stored
// cells match a full aggregate over the same query. Windows whose source
// points are no longer visible, for example below the retention floor, are
// dropped from the materialization.
func (e *Engine) RefreshContinuous(name, agg string, maxWindows int) (model.RefreshResult, error) {
	out := model.RefreshResult{Name: agg, Refreshed: []model.WindowRecord{}, Dropped: []int64{}}
	if maxWindows < 0 {
		return out, model.Bad("max_windows must be nonnegative")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		defs := b.Bucket(volume.Continuous)
		if defs == nil || defs.Get([]byte(agg)) == nil {
			return model.Missing("continuous aggregate does not exist")
		}
		var def model.Continuous
		if err := volume.GetJSON(defs, []byte(agg), &def); err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		out.Sequence = seq
		_, wins, invs, err := ensureContinuousBuckets(b)
		if err != nil {
			return err
		}
		entries := invs.Bucket([]byte(agg))
		if entries == nil {
			return nil
		}
		records, err := wins.CreateBucketIfNotExists([]byte(agg))
		if err != nil {
			return err
		}
		step := def.Query.Step
		processed := 0
		cursor := entries.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if maxWindows > 0 && processed >= maxWindows {
				out.Remaining++
				continue
			}
			start, err := volume.WindowStart(k)
			if err != nil {
				return err
			}
			wq := def.Query
			wq.Start = max(start, def.Query.Start)
			wq.End = min(start+step, def.Query.End)
			result, err := aggregate(b, n, wq, seq)
			if err != nil {
				return err
			}
			if len(result.Cells) == 0 {
				if err := records.Delete(volume.WindowKey(start)); err != nil {
					return err
				}
				out.Dropped = append(out.Dropped, start)
			} else {
				record := model.WindowRecord{
					Start: start, End: start + step, Sequence: seq, Cells: result.Cells,
				}
				if err := volume.PutJSON(records, volume.WindowKey(start), record); err != nil {
					return err
				}
				out.Refreshed = append(out.Refreshed, record)
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
			processed++
		}
		return nil
	})
	return out, err
}
