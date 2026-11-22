package engine

import (
	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// compact rewrites the visible sample set of one namespace. The rewrite is
// driven by the shared series-at-a-time scan path: replacement blocks are
// published as each series resolves, so only one series' expanded visible
// points are held at a time. Replaced source records are removed only after
// every selected series has been rewritten. Because the whole operation shares
// one write transaction, a decode failure or budget rejection late in the scan
// rolls back every published replacement, leaving the original points,
// deletion records, retention floor and source sequence untouched.
func compact(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace) (model.Maintenance, error) {
	// Maintenance is bounded by the same whole-operation scan policy used by
	// user queries. It is never truncated by a raw result limit.
	raw, err := openScan(b, n, model.Query{Start: model.MinTime, End: model.MaxTime + 1})
	if err != nil {
		return model.Maintenance{}, err
	}
	seq, err := volume.Next(tx)
	if err != nil {
		return model.Maintenance{}, err
	}
	receipt := model.Maintenance{Sequence: seq, Floor: n.Floor}
	// Replacement block identifiers embed the maintenance sequence, so they
	// never collide with the source blocks removed below.
	err = raw.forEach(func(s model.Series, points []model.Sample) error {
		if len(points) == 0 {
			// Empty series keep their catalog identity but produce no block.
			return nil
		}
		block, payload, err := chunk.Build(s.ID, seq, points)
		if err != nil {
			return err
		}
		if err := publishBlock(b, block, payload); err != nil {
			return err
		}
		receipt.Blocks++
		receipt.Points += block.Count
		return nil
	})
	if err != nil {
		return model.Maintenance{Floor: n.Floor}, err
	}
	blocks, payloads := b.Bucket(volume.Blocks), b.Bucket(volume.Payloads)
	for _, old := range raw.blocks {
		key := []byte(old.ID)
		if err := blocks.Delete(key); err != nil {
			return receipt, err
		}
		if err := payloads.Delete(key); err != nil {
			return receipt, err
		}
	}
	// Compaction drains the head and resolves deletions into the replacement
	// blocks, so both record sets can now be reclaimed.
	for _, bucket := range [][]byte{volume.Head, volume.Deletes} {
		if err := clearBucket(b, bucket); err != nil {
			return receipt, err
		}
	}
	n.LastSequence = seq
	return receipt, volume.PutJSON(b, volume.Config, n)
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
