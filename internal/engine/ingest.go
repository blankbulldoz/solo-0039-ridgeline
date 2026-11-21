package engine

import (
	"encoding/json"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Write(name string, request model.WriteRequest) (model.WriteReceipt, error) {
	receipt := model.WriteReceipt{Watermarks: []model.SeriesWatermark{}}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if err := volume.EnsureWatermarks(b); err != nil {
			return err
		}
		series, err := catalog(b)
		if err != nil {
			return err
		}
		known := make(map[string]bool, len(series))
		for _, s := range series {
			known[s.ID] = true
		}
		watermarks, err := readWatermarks(b)
		if err != nil {
			return err
		}
		if err := policy.Admit(n, request, policy.Admission{Known: known, Series: watermarks}); err != nil {
			return err
		}
		newSeries := map[string]model.Labels{}
		for _, p := range request.Points {
			id := model.SeriesID(p.Labels)
			if !known[id] {
				if _, pending := newSeries[id]; !pending {
					newSeries[id] = p.Labels
				}
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
		changed := map[string]bool{}
		for _, p := range request.Points {
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			id := model.SeriesID(p.Labels)
			key := volume.PointKey(id, p.Time)
			if err := volume.PutJSON(b.Bucket(volume.Head), key, s); err != nil {
				return err
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
			// A series watermark only ever moves forward and only records
			// timestamps committed with the sequence being published.
			if old, ok := watermarks[id]; !ok || p.Time > old.Time {
				watermarks[id] = model.SeriesWatermark{SeriesID: id, Time: p.Time, Sequence: seq}
				changed[id] = true
			}
		}
		for id := range changed {
			w := watermarks[id]
			if err := volume.PutJSON(b.Bucket(volume.Watermarks), []byte(id), w); err != nil {
				return err
			}
		}
		if policy.EffectiveLateMode(n) == model.LateModeSeries {
			receipt.Watermarks = sortedWatermarks(request.Points, watermarks)
		}
		n.LastSequence = seq
		receipt.Sequence, receipt.Accepted = seq, len(request.Points)
		receipt.NewSeries, receipt.Watermark = len(newSeries), n.Watermark
		receipt.LateMode = policy.EffectiveLateMode(n)
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

type WatermarkView struct {
	LateMode     string                `json:"late_mode"`
	Watermark    int64                 `json:"watermark"`
	HasWatermark bool                  `json:"has_watermark"`
	Series       []SeriesWatermarkView `json:"series"`
}

// A known series without a stored record is shown explicitly with
// HasRecord=false so the caller can see that series mode falls back to the
// namespace watermark for that series rather than treating it as new.
type SeriesWatermarkView struct {
	model.SeriesWatermark
	HasRecord bool `json:"has_record"`
}

func (e *Engine) Watermarks(name string) (WatermarkView, error) {
	var out WatermarkView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		stored, err := readWatermarks(b)
		if err != nil {
			return err
		}
		series, err := catalog(b)
		if err != nil {
			return err
		}
		out.LateMode = policy.EffectiveLateMode(n)
		out.Watermark, out.HasWatermark = n.Watermark, n.HasWatermark
		out.Series = make([]SeriesWatermarkView, 0, len(series))
		for _, s := range series {
			if w, ok := stored[s.ID]; ok {
				out.Series = append(out.Series, SeriesWatermarkView{SeriesWatermark: w, HasRecord: true})
			} else {
				out.Series = append(out.Series, SeriesWatermarkView{
					SeriesWatermark: model.SeriesWatermark{SeriesID: s.ID},
				})
			}
		}
		sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].SeriesID < out.Series[j].SeriesID })
		return nil
	})
	return out, err
}

func readWatermarks(b *bolt.Bucket) (map[string]model.SeriesWatermark, error) {
	out := map[string]model.SeriesWatermark{}
	bucket := b.Bucket(volume.Watermarks)
	if bucket == nil {
		return out, nil
	}
	err := bucket.ForEach(func(_, value []byte) error {
		var w model.SeriesWatermark
		if err := json.Unmarshal(value, &w); err != nil {
			return err
		}
		out[w.SeriesID] = w
		return nil
	})
	return out, err
}

func sortedWatermarks(points []model.Observation, stored map[string]model.SeriesWatermark) []model.SeriesWatermark {
	seen := map[string]bool{}
	out := make([]model.SeriesWatermark, 0)
	for _, p := range points {
		id := model.SeriesID(p.Labels)
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, stored[id])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SeriesID < out[j].SeriesID })
	return out
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
