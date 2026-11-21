package engine

import (
	"bytes"
	"encoding/json"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// representative picks evenly spaced exemplars from an already deterministic,
// ordered slice so dense spikes are bounded without concentrating the result
// on the earliest moments. The first and last positions are always included.
func representative(in []model.Exemplar, limit int) []model.Exemplar {
	if len(in) <= limit {
		return in
	}
	if limit == 1 {
		return []model.Exemplar{in[0]}
	}
	out := make([]model.Exemplar, 0, limit)
	last := len(in) - 1
	for i := 0; i < limit; i++ {
		idx := (i*last + (limit-1)/2) / (limit - 1)
		out = append(out, in[idx])
	}
	return out
}

func exemplars(b *bolt.Bucket, n model.Namespace, q model.ExemplarQuery, sequence uint64) (model.ExemplarResult, error) {
	limit := q.Limit
	if limit == 0 {
		limit = model.DefaultExemplarLimit
	}
	result := model.ExemplarResult{
		Sequence: sequence, Floor: n.Floor, Limit: limit,
		Exemplars: []model.Exemplar{},
	}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return result, err
	}
	allSeries, err := catalog(b)
	if err != nil {
		return result, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return result, err
	}
	selected := selectseries.Pick(allSeries, selector)

	// Anchors reuse the ordinary sample visibility path (head, blocks,
	// corrections, tombstones, floor) so exemplar validity cannot drift
	// from what a range query reports.
	raw, err := scan(b, n, model.Query{
		Start: q.Start, End: q.End, Matchers: q.Matchers,
	}, sequence)
	if err != nil {
		return result, err
	}
	anchors := map[string]map[int64]model.Sample{}
	for _, sp := range raw.Series {
		pts := make(map[int64]model.Sample, len(sp.Points))
		for _, p := range sp.Points {
			pts[p.Time] = p
		}
		anchors[sp.Series.ID] = pts
	}

	cursor := b.Bucket(volume.Exemplars).Cursor()
	matched := make([]model.Exemplar, 0)
	for _, s := range selected {
		prefix := []byte(s.ID)
		for k, v := cursor.Seek(volume.ExemplarKey(s.ID, q.Start, 0, 0)); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			timestamp, err := volume.ExemplarKeyTime(k)
			if err != nil {
				return result, model.Bad("invalid stored exemplar key")
			}
			if timestamp >= q.End {
				break
			}
			result.Stats.Scanned++
			if result.Stats.Scanned > n.MaxScan {
				return result, model.Capacity("query decoded point budget exceeded")
			}
			var stored model.StoredExemplar
			if err := json.Unmarshal(v, &stored); err != nil {
				return result, err
			}
			result.Stats.Matched++
			ex := model.Exemplar{
				ID:       stored.ID,
				Series:   s,
				Time:     stored.Time,
				Value:    stored.AnchorVal,
				TraceID:  stored.TraceID,
				Labels:   stored.Labels,
				Sequence: stored.Sequence,
			}
			// Categories are mutually exclusive. Retention (the irreversible
			// floor) outranks tombstones, which outrank ordinary absence.
			switch {
			case stored.Time < n.Floor:
				result.Stats.BelowFloor++
			case hidden(s.ID, model.Sample{Time: stored.Time, Sequence: stored.Sequence}, tombstones):
				result.Stats.HiddenTombstone++
			default:
				if anchor, ok := anchors[s.ID][stored.Time]; ok {
					ex.AnchorVisible = true
					seq := anchor.Sequence
					ex.AnchorSequence = &seq
					ex.Value = anchor.Value
					result.Stats.Visible++
				} else {
					result.Stats.Detached++
				}
			}
			matched = append(matched, ex)
		}
	}
	result.Stats.Omitted = len(matched) - limit
	if result.Stats.Omitted < 0 {
		result.Stats.Omitted = 0
	}
	result.Truncated = len(matched) > limit
	result.Exemplars = representative(matched, limit)
	return result, nil
}

func (e *Engine) Exemplars(name string, q model.ExemplarQuery) (model.ExemplarResult, error) {
	var result model.ExemplarResult
	if err := model.ValidateExemplarQuery(q); err != nil {
		return result, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result, err = exemplars(b, n, q, volume.Current(tx))
		return err
	})
	return result, err
}
