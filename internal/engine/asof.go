package engine

import (
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// rightBounds narrows the neighbor scan so the shared scan budget is spent on
// points that can actually be selected. Both endpoints are half-open, so an
// exact-time neighbor is included while the bound itself stays excluded.
func rightBounds(q model.AsOfQuery) (int64, int64) {
	start, end := q.Start, q.End
	if q.MaxDistanceMS > 0 {
		if low := q.Start - q.MaxDistanceMS; low > model.MinTime {
			start = low
		} else {
			start = model.MinTime
		}
		if high := q.End + q.MaxDistanceMS; high < model.MaxTime+1 {
			end = high
		} else {
			end = model.MaxTime + 1
		}
	}
	return start, end
}

// candidate keeps one selected series together with its merged visible points.
type candidate struct {
	series model.Series
	points []model.Sample
}

// lastAtOrBefore returns the index of the last point with time <= t, or -1.
func lastAtOrBefore(points []model.Sample, t int64) int {
	i := sort.Search(len(points), func(i int) bool { return points[i].Time > t }) - 1
	return i
}

// firstAtOrAfter returns the index of the first point with time >= t, or -1.
func firstAtOrAfter(points []model.Sample, t int64) int {
	i := sort.Search(len(points), func(i int) bool { return points[i].Time >= t })
	if i >= len(points) {
		return -1
	}
	return i
}

type neighbor struct {
	seriesID string
	point    model.Sample
	distance int64
}

func within(q model.AsOfQuery, distance int64) bool {
	return q.MaxDistanceMS == 0 || distance <= q.MaxDistanceMS
}

// pickNeighbor resolves one stable neighbor across every right series sharing
// the pair key. Equal distances are broken by series identity and then time so
// duplicate-distance and multi-series inputs stay deterministic.
func pickNeighbor(q model.AsOfQuery, rights []candidate, leftTime int64) (neighbor, bool) {
	best, found := neighbor{}, false
	consider := func(c candidate, index int) {
		if index < 0 {
			return
		}
		p := c.points[index]
		d := p.Time - leftTime
		if d < 0 {
			d = -d
		}
		if !within(q, d) {
			return
		}
		cand := neighbor{seriesID: c.series.ID, point: p, distance: d}
		if !found || cand.distance < best.distance ||
			(cand.distance == best.distance &&
				(cand.seriesID < best.seriesID ||
					(cand.seriesID == best.seriesID && cand.point.Time < best.point.Time))) {
			best, found = cand, true
		}
	}
	for _, c := range rights {
		switch q.Direction {
		case model.AsOfBackward:
			consider(c, lastAtOrBefore(c.points, leftTime))
		case model.AsOfForward:
			consider(c, firstAtOrAfter(c.points, leftTime))
		case model.AsOfNearest:
			consider(c, lastAtOrBefore(c.points, leftTime))
			consider(c, firstAtOrAfter(c.points, leftTime))
		}
	}
	return best, found
}

func toPoint(s model.Series, p model.Sample) model.AsOfPoint {
	return model.AsOfPoint{Time: p.Time, Value: p.Value, SeriesID: s.ID, Sequence: p.Sequence}
}

func asOf(b *bolt.Bucket, n model.Namespace, q model.AsOfQuery, sequence uint64) (model.AsOfResult, error) {
	result := model.AsOfResult{
		Sequence:  sequence,
		Floor:     n.Floor,
		PairLabel: q.PairLabel,
		Direction: q.Direction,
		Pairs:     []model.AsOfPair{},
	}
	// A raw limit must never truncate the scans that feed the join; the public
	// as-of limit is applied to emitted pairs instead.
	left, err := scan(b, n, model.Query{
		Start: q.Start, End: q.End, Matchers: q.LeftMatchers,
	}, sequence)
	if err != nil {
		return result, err
	}
	rightStart, rightEnd := rightBounds(q)
	right, err := scan(b, n, model.Query{
		Start: rightStart, End: rightEnd, Matchers: q.RightMatchers,
	}, sequence)
	if err != nil {
		return result, err
	}
	result.Stats.Left, result.Stats.Right = left.Stats, right.Stats

	rightByKey := map[string][]candidate{}
	for _, sp := range right.Series {
		key := sp.Series.Labels[q.PairLabel]
		rightByKey[key] = append(rightByKey[key], candidate{series: sp.Series, points: sp.Points})
	}
	for key := range rightByKey {
		sort.Slice(rightByKey[key], func(i, j int) bool {
			return rightByKey[key][i].series.ID < rightByKey[key][j].series.ID
		})
	}

	type row struct {
		key    string
		left   candidate
		sample model.Sample
	}
	rows := make([]row, 0)
	for _, sp := range left.Series {
		key := sp.Series.Labels[q.PairLabel]
		c := candidate{series: sp.Series, points: sp.Points}
		for _, p := range sp.Points {
			rows = append(rows, row{key: key, left: c, sample: p})
		}
	}
	// Canonical output order: pairing key, then left series identity and time.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].key != rows[j].key {
			return rows[i].key < rows[j].key
		}
		if rows[i].left.series.ID != rows[j].left.series.ID {
			return rows[i].left.series.ID < rows[j].left.series.ID
		}
		return rows[i].sample.Time < rows[j].sample.Time
	})

	rightSeriesByID := map[string]model.Series{}
	for _, sp := range right.Series {
		rightSeriesByID[sp.Series.ID] = sp.Series
	}

	for _, r := range rows {
		if q.Limit > 0 && len(result.Pairs) >= q.Limit {
			result.Truncated = true
			break
		}
		pair := model.AsOfPair{
			PairKey:    r.key,
			LeftSeries: r.left.series,
			Left:       toPoint(r.left.series, r.sample),
		}
		if nb, ok := pickNeighbor(q, rightByKey[r.key], r.sample.Time); ok {
			rs := rightSeriesByID[nb.seriesID]
			rp := toPoint(rs, nb.point)
			pair.RightSeries = &rs
			pair.Right = &rp
			pair.DistanceMS = nb.distance
			pair.Matched = true
		}
		result.Pairs = append(result.Pairs, pair)
	}
	return result, nil
}

func (e *Engine) AsOf(name string, q model.AsOfQuery) (model.AsOfResult, error) {
	var result model.AsOfResult
	if err := model.ValidateAsOf(q); err != nil {
		return result, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result, err = asOf(b, n, q, volume.Current(tx))
		return err
	})
	return result, err
}
