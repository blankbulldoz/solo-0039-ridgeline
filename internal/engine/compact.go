package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// publishCompact rewrites a namespace from one logical snapshot: head, block,
// payload and deletion buckets are replaced by a single block per non-empty
// series. Catalog identity (including empty series) is preserved.
func publishCompact(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace, raw model.RangeResult) (model.Maintenance, error) {
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
	return receipt, volume.PutJSON(b, volume.Config, n)
}

func compact(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace) (model.Maintenance, error) {
	// Maintenance is bounded by the same scan policy as user queries.
	raw, err := scan(b, n, model.Query{Start: model.MinTime, End: model.MaxTime + 1}, volume.Current(tx))
	if err != nil {
		return model.Maintenance{}, err
	}
	return publishCompact(tx, b, n, raw)
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
