package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Materialize(name, rollup string, q model.Query) (model.Materialization, error) {
	var out model.Materialization
	if !model.ValidName(rollup) {
		return out, model.Bad("invalid rollup name")
	}
	err := e.mutate(name, logicalMutation, func(c *change) error {
		// Provenance is fixed before any sequence is allocated: the
		// materialization remembers the source version it actually read.
		source := c.Source()
		result, err := aggregate(c.bucket, c.namespace, q, c.Current())
		if err != nil {
			return err
		}
		out = model.Materialization{
			Name: rollup, Query: q, SourceSequence: source, Result: result,
		}
		// Publication gets its own receipt sequence, but the namespace source
		// version is deliberately not published: saving rollup metadata must
		// not move LastSequence forward, and finish leaves the config untouched.
		if _, err := c.Next(); err != nil {
			return err
		}
		return volume.PutJSON(c.bucket.Bucket(volume.Rollups), []byte(rollup), out)
	})
	return out, err
}

func (e *Engine) Rollup(name, rollup string) (model.RollupView, error) {
	var out model.RollupView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if b.Bucket(volume.Rollups).Get([]byte(rollup)) == nil {
			return model.Missing("rollup does not exist")
		}
		if err := volume.GetJSON(b.Bucket(volume.Rollups), []byte(rollup), &out.Materialization); err != nil {
			return err
		}
		out.Stale = out.SourceSequence != n.LastSequence
		return nil
	})
	return out, err
}
