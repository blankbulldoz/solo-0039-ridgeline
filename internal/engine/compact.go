package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func compact(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace) (model.Maintenance, error) {
	// Maintenance is bounded by the same scan policy as user queries.
	raw, err := scan(b, n, model.Query{Start: model.MinTime, End: model.MaxTime + 1}, volume.Current(tx))
	if err != nil {
		return model.Maintenance{}, err
	}
	for _, bucket := range [][]byte{volume.Head, volume.Blocks, volume.Payloads, volume.Deletes} {
		if err := clearBucket(b, bucket); err != nil {
			return model.Maintenance{}, err
		}
	}
	seq, err := volume.Next(tx)
	if err != nil {
		return model.Maintenance{}, err
	}
	receipt := model.Maintenance{Sequence: seq, Floor: n.Floor}
	for _, s := range raw.Series {
		if err := putBlock(b, s.Series.ID, seq, s.Points); err != nil {
			return receipt, err
		}
		receipt.Blocks++
		receipt.Points += len(s.Points)
	}
	n.LastSequence = seq
	if err := volume.PutJSON(b, volume.Config, n); err != nil {
		return receipt, err
	}
	// Compaction rewrites every block, so any materialized quantile summary
	// built over the old physical layout is invalid. Recompute each view over
	// the freshly compacted logical data within this same transaction, so a
	// stored summary never outlives the blocks it summarized.
	if err := refreshAllQuantileViews(tx, b, n); err != nil {
		return receipt, err
	}
	return receipt, nil
}

// refreshAllQuantileViews rebuilds every stored quantile view against the
// current namespace state. Views whose query no longer satisfies validation or
// exceeds the scan budget after a structural change are dropped rather than
// left pointing at superseded tuples.
func refreshAllQuantileViews(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace) error {
	bucket := b.Bucket(volume.Quantiles)
	var views [][]byte
	if err := bucket.ForEach(func(k, _ []byte) error {
		views = append(views, append([]byte(nil), k...))
		return nil
	}); err != nil {
		return err
	}
	for _, key := range views {
		var def model.QuantileMaterialization
		if err := volume.GetJSON(bucket, key, &def); err != nil {
			return err
		}
		// A view that cannot be rebuilt (e.g. it now exceeds the scan budget)
		// fails the whole maintenance transaction, which rolls back the
		// compaction as well rather than leaving a view pointing at superseded
		// tuples. The caller can drop or redefine the view and retry.
		if _, err := refreshQuantileView(tx, b, n, def); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Compact(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		receipt, err = compact(tx, b, n)
		return err
	})
	return receipt, err
}

func (e *Engine) Retain(name string, now int64) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		n.Floor, err = policy.RetentionFloor(n, now)
		if err != nil {
			return err
		}
		receipt, err = compact(tx, b, n)
		return err
	})
	return receipt, err
}
