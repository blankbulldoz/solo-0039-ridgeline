package engine

import (
	"encoding/json"
	"strconv"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// pointPlan is the per-input disposition derived before publication.
type pointPlan struct {
	model.PointResult
	point    model.Observation
	seriesID string
}

func (e *Engine) Write(name string, request model.WriteRequest) (model.WriteReceipt, error) {
	var receipt model.WriteReceipt
	if !model.ValidWriteMode(request.Mode) {
		return receipt, model.Bad("invalid write mode")
	}
	if request.Mode == model.WriteBestEffort {
		return e.writeBestEffort(name, request)
	}
	return e.writeAtomic(name, request)
}

// writeAtomic keeps the historical all-or-nothing semantics: one invalid point
// rejects the entire request and nothing is published.
func (e *Engine) writeAtomic(name string, request model.WriteRequest) (model.WriteReceipt, error) {
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
		head := b.Bucket(volume.Head)
		// Last write in the request wins for repeated series/timestamp pairs;
		// earlier rows are overwritten rather than counted as new points.
		winner := map[string]int{}
		for i, p := range request.Points {
			winner[model.SeriesID(p.Labels)+timeKey(p.Time)] = i
		}
		accepted, overwritten := 0, 0
		for i, p := range request.Points {
			id := model.SeriesID(p.Labels)
			key := volume.PointKey(id, p.Time)
			if winner[id+timeKey(p.Time)] != i {
				overwritten++
				continue
			}
			replaced := head.Get(key) != nil
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			if err := volume.PutJSON(head, key, s); err != nil {
				return err
			}
			if replaced {
				overwritten++
			} else {
				accepted++
			}
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
		}
		n.LastSequence = seq
		receipt = model.WriteReceipt{
			Sequence: seq, Mode: model.WriteAtomic, Requested: len(request.Points),
			Accepted: accepted, Overwritten: overwritten,
			NewSeries: len(newSeries), Watermark: n.Watermark, HasWatermark: n.HasWatermark,
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

// writeBestEffort evaluates every point independently. Rejected points are
// reported instead of failing the request, and only actually stored points
// consume series quota or move the watermark.
func (e *Engine) writeBestEffort(name string, request model.WriteRequest) (model.WriteReceipt, error) {
	var receipt model.WriteReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		// Read-only, bad observation time and batch size are request-level.
		if err := policy.RequestError(n, request); err != nil {
			return err
		}
		existingSeries, err := catalog(b)
		if err != nil {
			return err
		}
		known := make(map[string]bool, len(existingSeries))
		for _, s := range existingSeries {
			known[s.ID] = true
		}
		head := b.Bucket(volume.Head)

		plans := make([]pointPlan, len(request.Points))
		// First pass: the last input row wins for each series/timestamp, so
		// earlier duplicate rows are reported as overwritten before any
		// admission decision and never consume quota themselves.
		winner := map[string]int{}
		for i, p := range request.Points {
			winner[model.SeriesID(p.Labels)+timeKey(p.Time)] = i
		}
		// Series identities created by admitted winners during this request.
		created := map[string]model.Labels{}
		// Winning admitted rows that will be published.
		admitted := 0
		for i, p := range request.Points {
			plan := pointPlan{
				PointResult: model.PointResult{Index: i, Time: p.Time},
				point:       p,
			}
			plan.seriesID = model.SeriesID(p.Labels)
			plan.Series = plan.seriesID
			dupKey := plan.seriesID + timeKey(p.Time)
			if winner[dupKey] != i {
				plan.Status = model.PointOverwritten
				plan.Reason = "overwritten"
				plan.Detail = "superseded by later point at the same series and timestamp (index " +
					strconv.Itoa(winner[dupKey]) + ")"
				plans[i] = plan
				continue
			}

			if perr := policy.Evaluate(n, request.Now, p); perr != nil {
				plan.Status, plan.Reason, plan.Detail =
					model.PointRejected, perr.Reason, perr.Detail
				plans[i] = plan
				continue
			}
			if !known[plan.seriesID] {
				if len(known)+len(created) >= n.MaxSeries {
					plan.Status = model.PointRejected
					plan.Reason = policy.ReasonSeriesQuota
					plan.Detail = "series cardinality policy exceeded"
					plans[i] = plan
					continue
				}
				created[plan.seriesID] = p.Labels
			}
			plan.Status = model.PointAccepted
			plans[i] = plan
			admitted++
		}

		// Nothing admitted: leave the database exactly where it was, including
		// its sequence, while still returning the per-point rejection report.
		if admitted == 0 {
			receipt = summarize(request.Mode, plans, n.LastSequence, 0, n)
			return nil
		}

		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for id, labels := range created {
			if err := volume.PutJSON(b.Bucket(volume.Catalog), []byte(id),
				model.Series{ID: id, Labels: labels}); err != nil {
				return err
			}
		}
		accepted, overwritten := 0, 0
		for i := range plans {
			plan := &plans[i]
			if plan.Status == model.PointOverwritten {
				overwritten++
				continue
			}
			if plan.Status != model.PointAccepted {
				continue
			}
			p := plan.point
			key := volume.PointKey(plan.seriesID, p.Time)
			s := model.Sample{Time: p.Time, Value: p.Value, Sequence: seq}
			if head.Get(key) != nil {
				// An accepted correction replaces an already committed sample:
				// it takes effect but adds no new stored point.
				plan.Status = model.PointOverwritten
				plan.Reason = "overwritten"
				plan.Detail = "replaced previously committed value at the same timestamp"
				overwritten++
			} else {
				accepted++
			}
			if err := volume.PutJSON(head, key, s); err != nil {
				return err
			}
			// Only actually published data moves the watermark.
			if !n.HasWatermark || p.Time > n.Watermark {
				n.Watermark, n.HasWatermark = p.Time, true
			}
		}
		n.LastSequence = seq
		if err := volume.PutJSON(b, volume.Config, n); err != nil {
			return err
		}
		receipt = summarize(request.Mode, plans, seq, len(created), n)
		return nil
	})
	return receipt, err
}

func summarize(mode string, plans []pointPlan, seq uint64, newSeries int,
	n model.Namespace) model.WriteReceipt {
	results := make([]model.PointResult, len(plans))
	accepted, overwritten, rejected := 0, 0, 0
	for i, plan := range plans {
		results[i] = plan.PointResult
		switch plan.Status {
		case model.PointAccepted:
			accepted++
		case model.PointOverwritten:
			overwritten++
		default:
			rejected++
		}
	}
	return model.WriteReceipt{
		Sequence: seq, Mode: mode, Requested: len(plans), Accepted: accepted,
		Overwritten: overwritten, Rejected: rejected, NewSeries: newSeries,
		Watermark: n.Watermark, HasWatermark: n.HasWatermark, Results: results,
	}
}

// timeKey renders the signed timestamp exactly as it appears inside a storage
// point key, so duplicate detection matches stored key identity.
func timeKey(t int64) string {
	return string(volume.PointKey("", t)[:8])
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
