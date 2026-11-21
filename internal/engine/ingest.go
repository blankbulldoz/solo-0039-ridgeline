package engine

import (
	"encoding/json"
	"fmt"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// corrections re-evaluates every expected observation against the committed
// namespace state inside the write transaction. All mismatches are reported at
// once so a caller can re-observe the whole set before retrying; nothing is
// published for a request that fails this check.
func corrections(b *bolt.Bucket, n model.Namespace, request model.WriteRequest) error {
	blocks, err := readBlocks(b)
	if err != nil {
		return err
	}
	conflicts := make([]model.ConflictEvidence, 0)
	stats, cache := model.ScanStats{}, map[string][]model.Sample{}
	for index, p := range request.Points {
		if p.Expect == nil {
			continue
		}
		state, err := observe(b, n, model.SeriesID(p.Labels), p.Time, blocks, &stats, cache)
		if err != nil {
			return err
		}
		if policy.Matches(p.Expect, state) {
			continue
		}
		conflicts = append(conflicts, model.ConflictEvidence{
			Index: index, Labels: p.Labels, Time: p.Time,
			Expected: *p.Expect, Actual: state,
		})
	}
	if len(conflicts) == 0 {
		return nil
	}
	return model.CorrectionConflict(fmt.Sprintf(
		"%d of %d observations no longer match the expected version",
		len(conflicts), len(request.Points)), conflicts)
}

func (e *Engine) Write(name string, request model.WriteRequest) (model.WriteReceipt, error) {
	var receipt model.WriteReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if err := policy.Validate(n, request); err != nil {
			return err
		}
		// Expectations are resolved before event-time admission so an expired
		// observation is reported as expired rather than as an ordinary
		// policy rejection or as absent.
		if err := corrections(b, n, request); err != nil {
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
		for _, p := range request.Points {
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			key := volume.PointKey(model.SeriesID(p.Labels), p.Time)
			if err := volume.PutJSON(b.Bucket(volume.Head), key, s); err != nil {
				return err
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
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
