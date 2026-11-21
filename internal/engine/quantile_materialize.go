package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// MaterializeQuantile builds and stores a named mergeable quantile summary for
// the given definition. The stored record carries the source sequence and the
// serialized summaries, so any later correction, deletion, overwrite, seal or
// compaction that advances the namespace sequence makes it stale rather than
// silently serving a result computed over superseded data.
func (e *Engine) MaterializeQuantile(name, view string, q model.QuantileQuery) (model.QuantileMaterialization, error) {
	var out model.QuantileMaterialization
	if !model.ValidName(view) {
		return out, model.Bad("invalid quantile view name")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		result, stored, err := quantileQuery(b, n, q, volume.Current(tx))
		if err != nil {
			return err
		}
		out = model.QuantileMaterialization{
			Name: view, Query: q,
			SourceSequence: n.LastSequence, Result: result, Summaries: stored,
		}
		// View metadata does not change the source namespace sequence.
		if _, err := volume.Next(tx); err != nil {
			return err
		}
		return volume.PutJSON(b.Bucket(volume.Quantiles), []byte(view), out)
	})
	return out, err
}

// QuantileView reads a stored quantile materialization and reports whether the
// namespace has changed since it was built. A stale view must be rebuilt before
// its estimates can be treated as current; the stored summaries are never
// combined with newer data implicitly.
func (e *Engine) QuantileView(name, view string) (model.QuantileView, error) {
	var out model.QuantileView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		bucket := b.Bucket(volume.Quantiles)
		if bucket.Get([]byte(view)) == nil {
			return model.Missing("quantile view does not exist")
		}
		if err := volume.GetJSON(bucket, []byte(view), &out.QuantileMaterialization); err != nil {
			return err
		}
		out.Stale = out.SourceSequence != n.LastSequence
		return nil
	})
	return out, err
}

// RefreshQuantile rebuilds a stored view from current data and returns the
// fresh materialization. This is the required recovery path once a write,
// late overwrite, deletion or seal has advanced the namespace sequence and the
// GET response reports the view as stale.
func (e *Engine) RefreshQuantile(name, view string) (model.QuantileView, error) {
	var out model.QuantileView
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		bucket := b.Bucket(volume.Quantiles)
		if bucket.Get([]byte(view)) == nil {
			return model.Missing("quantile view does not exist")
		}
		var def model.QuantileMaterialization
		if err := volume.GetJSON(bucket, []byte(view), &def); err != nil {
			return err
		}
		fresh, err := refreshQuantileView(tx, b, n, def)
		if err != nil {
			return err
		}
		if _, err := volume.Next(tx); err != nil {
			return err
		}
		out.QuantileMaterialization = fresh
		out.Stale = false
		return nil
	})
	return out, err
}

// refreshQuantileView rebuilds a stored view inside an existing transaction
// and returns the fresh materialization. It is the single recompute path used
// after any structural change invalidates a summary.
func refreshQuantileView(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace,
	def model.QuantileMaterialization) (model.QuantileMaterialization, error) {
	result, stored, err := quantileQuery(b, n, def.Query, volume.Current(tx))
	if err != nil {
		return def, err
	}
	fresh := model.QuantileMaterialization{
		Name: def.Name, Query: def.Query,
		SourceSequence: n.LastSequence, Result: result, Summaries: stored,
	}
	return fresh, volume.PutJSON(b.Bucket(volume.Quantiles), []byte(def.Name), fresh)
}
