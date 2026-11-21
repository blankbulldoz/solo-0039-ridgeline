package engine

import (
	"encoding/json"
	"fmt"

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
		for id, labels := range newSeries {
			if err := volume.PutJSON(b.Bucket(volume.Catalog), []byte(id),
				model.Series{ID: id, Labels: labels}); err != nil {
				return err
			}
		}
		exemplars := 0
		for i, p := range request.Points {
			seriesID := model.SeriesID(p.Labels)
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			key := volume.PointKey(seriesID, p.Time)
			if err := volume.PutJSON(b.Bucket(volume.Head), key, s); err != nil {
				return err
			}
			if p.Exemplar != nil {
				labels := model.Labels{}
				for k, v := range p.Exemplar.Labels {
					labels[k] = v
				}
				stored := model.StoredExemplar{
					ID:        fmt.Sprintf("%020d-%06d", seq, i),
					SeriesID:  seriesID,
					Time:      p.Time,
					TraceID:   p.Exemplar.TraceID,
					Labels:    labels,
					Sequence:  seq,
					Offset:    i,
					AnchorVal: p.Value,
				}
				exKey := volume.ExemplarKey(seriesID, p.Time, seq, i)
				if err := volume.PutJSON(b.Bucket(volume.Exemplars), exKey, stored); err != nil {
					return err
				}
				exemplars++
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
		}
		n.LastSequence = seq
		receipt = model.WriteReceipt{
			Sequence: seq, Accepted: len(request.Points),
			NewSeries: len(newSeries), Exemplars: exemplars, Watermark: n.Watermark,
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
