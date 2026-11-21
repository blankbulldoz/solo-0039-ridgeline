package engine

import (
	"encoding/json"

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
	tombstones, err := readTombstones(b)
	if err != nil {
		return model.Maintenance{}, err
	}
	anchors := map[string]map[int64]model.Sample{}
	for _, sp := range raw.Series {
		pts := make(map[int64]model.Sample, len(sp.Points))
		for _, p := range sp.Points {
			pts[p.Time] = p
		}
		anchors[sp.Series.ID] = pts
	}
	// Physical compaction reclaims exemplars whose association can never be
	// valid again: below the irreversible floor, hidden by a tombstone, or
	// detached from every surviving anchor. Survivors keep their identity and
	// ordering because the original key is preserved verbatim.
	type keptExemplar struct {
		key    []byte
		stored model.StoredExemplar
	}
	kept := make([]keptExemplar, 0)
	receipt := model.Maintenance{Floor: n.Floor}
	err = b.Bucket(volume.Exemplars).ForEach(func(k, v []byte) error {
		var stored model.StoredExemplar
		if err := json.Unmarshal(v, &stored); err != nil {
			return err
		}
		_, hasAnchor := anchors[stored.SeriesID][stored.Time]
		drop := stored.Time < n.Floor ||
			hidden(stored.SeriesID, model.Sample{Time: stored.Time, Sequence: stored.Sequence}, tombstones) ||
			!hasAnchor
		if drop {
			receipt.ExemplarsDropped++
			return nil
		}
		kept = append(kept, keptExemplar{key: append([]byte(nil), k...), stored: stored})
		return nil
	})
	if err != nil {
		return model.Maintenance{}, err
	}
	for _, bucket := range [][]byte{volume.Head, volume.Blocks, volume.Payloads, volume.Deletes, volume.Exemplars} {
		if err := clearBucket(b, bucket); err != nil {
			return model.Maintenance{}, err
		}
	}
	seq, err := volume.Next(tx)
	if err != nil {
		return model.Maintenance{}, err
	}
	receipt.Sequence = seq
	for _, ke := range kept {
		if err := volume.PutJSON(b.Bucket(volume.Exemplars), ke.key, ke.stored); err != nil {
			return model.Maintenance{}, err
		}
		receipt.ExemplarsKept++
	}
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
