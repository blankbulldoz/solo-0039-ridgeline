package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// Materialize refreshes a rollup in two phases. The expensive scan runs in a
// read snapshot so concurrent readers keep serving the previous result and
// ingestion is not blocked; a short write transaction then records the
// attempt. The last successful materialization is replaced only by a
// successful scan, never by a failed or differently defined attempt.
func (e *Engine) Materialize(name, rollup string, q model.Query) (model.RollupView, error) {
	var view model.RollupView
	if !model.ValidName(rollup) {
		return view, model.Bad("invalid rollup name")
	}
	var (
		result  model.AggregateResult
		source  uint64
		scanErr error
	)
	readErr := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		source = n.LastSequence
		result, scanErr = aggregate(b, n, q, volume.Current(tx))
		return nil
	})
	if readErr != nil {
		return view, readErr
	}
	// The attempt is published even when the scan failed, so the definition,
	// source sequence and failure reason remain queryable afterwards.
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		record := model.RollupRecord{Name: rollup}
		bucket := b.Bucket(volume.Rollups)
		if data := bucket.Get([]byte(rollup)); data != nil {
			if err := json.Unmarshal(data, &record); err != nil {
				return err
			}
		}
		record.Attempt = model.RollupAttempt{
			Query:          q,
			SourceSequence: source,
			Overtaken:      n.LastSequence != source,
		}
		if scanErr != nil {
			record.Attempt.Status = model.AttemptFailed
			record.Attempt.Failure = model.Describe(scanErr)
		} else {
			record.Attempt.Status = model.AttemptOK
			record.Current = &model.Materialization{
				Name: rollup, Query: q, SourceSequence: source, Result: result,
			}
		}
		// Rollup metadata does not change the source namespace sequence.
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		record.Attempt.Sequence = seq
		if err := volume.PutJSON(bucket, []byte(rollup), record); err != nil {
			return err
		}
		view = rollupView(record, n)
		return nil
	})
	if err != nil {
		return view, err
	}
	return view, scanErr
}

func (e *Engine) Rollup(name, rollup string) (model.RollupView, error) {
	var out model.RollupView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		data := b.Bucket(volume.Rollups).Get([]byte(rollup))
		if data == nil {
			return model.Missing("rollup does not exist")
		}
		var record model.RollupRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		out = rollupView(record, n)
		return nil
	})
	return out, err
}

// rollupView reports whether the last success trails the namespace; a
// missing or failed attempt never makes the stored result look current.
func rollupView(record model.RollupRecord, n model.Namespace) model.RollupView {
	stale := record.Current != nil && record.Current.SourceSequence != n.LastSequence
	return model.RollupView{RollupRecord: record, Stale: stale}
}
