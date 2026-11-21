package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Write(name string, request model.WriteRequest) (model.WriteReceipt, error) {
	var receipt model.WriteReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if err := policy.Admit(n, request); err != nil {
			return err
		}
		series, err := catalog(b)
		if err != nil {
			return err
		}
		newSeries := map[string]model.Labels{}
		for _, p := range request.Points {
			id := model.SeriesID(p.Labels)
			if b.Bucket(volume.Catalog).Get([]byte(id)) == nil {
				newSeries[id] = p.Labels
			}
		}
		if len(series)+len(newSeries) > n.MaxSeries {
			return model.Capacity("series cardinality policy exceeded")
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		// While a seal job runs, keys absent from the head are work added
		// after the job snapshot; mark them so progress keeps initial backlog
		// and in-flight additions separate. Corrections reuse existing keys.
		var markers *bolt.Bucket
		active, err := loadSealJob(b)
		if err != nil {
			return err
		}
		if active != nil && active.State == sealStateRunning {
			markers, err = addedKeyBucket(b, active)
			if err != nil {
				return err
			}
		}
		for id, labels := range newSeries {
			if err := volume.PutJSON(b.Bucket(volume.Catalog), []byte(id),
				model.Series{ID: id, Labels: labels}); err != nil {
				return err
			}
		}
		for _, p := range request.Points {
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			key := volume.PointKey(model.SeriesID(p.Labels), p.Time)
			head := b.Bucket(volume.Head)
			isNewKey := head.Get(key) == nil
			if err := volume.PutJSON(head, key, s); err != nil {
				return err
			}
			if isNewKey {
				if err := volume.AdjustHeadCount(b, 1); err != nil {
					return err
				}
			}
			// Only a key that was never part of the job's initial snapshot is
			// "added work". A key missing because an earlier batch already
			// sealed it is a correction to initial backlog and stays unmarked.
			if markers != nil && isNewKey && !isSealedKey(b, key) {
				if err := markers.Put(key, []byte{1}); err != nil {
					return err
				}
				active.AddedPending++
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
		}
		if markers != nil {
			if err := saveSealJob(b, active); err != nil {
				return err
			}
		}
		n.LastSequence = seq
		receipt = model.WriteReceipt{
			Sequence: seq, Accepted: len(request.Points),
			NewSeries: len(newSeries), Watermark: n.Watermark,
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

func readTombstones(b *bolt.Bucket) ([]model.Tombstone, error) {
	out := make([]model.Tombstone, 0)
	err := b.Bucket(volume.Deletes).ForEach(func(_, value []byte) error {
		var tomb model.Tombstone
		if err := json.Unmarshal(value, &tomb); err != nil {
			return err
		}
		out = append(out, tomb)
		return nil
	})
	return out, err
}

func hidden(id string, p model.Sample, tombstones []model.Tombstone) bool {
	for _, tomb := range tombstones {
		if p.Time < tomb.Start || p.Time >= tomb.End || p.Sequence > tomb.Sequence {
			continue
		}
		for _, series := range tomb.Series {
			if series == id {
				return true
			}
		}
	}
	return false
}
