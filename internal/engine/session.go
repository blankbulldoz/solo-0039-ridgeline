package engine

import (
	"encoding/json"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/session"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// scanRange widens the report window by one gap on both sides so segment
// edges are classified from the nearest visible neighbors, whether they
// live in the mutable head or in sealed blocks.
func scanRange(q model.SessionQuery) (int64, int64) {
	lo := q.Start - q.Gap
	if lo < model.MinTime {
		lo = model.MinTime
	}
	hi := q.End + q.Gap
	if hi > model.MaxTime+1 {
		hi = model.MaxTime + 1
	}
	return lo, hi
}

func sessions(b *bolt.Bucket, n model.Namespace, q model.SessionQuery, sequence uint64) (model.SessionResult, error) {
	result := model.SessionResult{
		Sequence: sequence, Floor: n.Floor, Start: q.Start, End: q.End,
		Gap: q.Gap, AsOf: q.AsOf,
		Segments: []model.Segment{}, Groups: []model.SessionGroup{},
	}
	lo, hi := scanRange(q)
	raw, err := scan(b, n, model.Query{Start: lo, End: hi, Matchers: q.Matchers}, sequence)
	if err != nil {
		return result, err
	}
	result.Stats = raw.Stats
	bounds := session.Bounds{Start: q.Start, End: q.End, Gap: q.Gap, AsOf: q.AsOf, Floor: n.Floor}
	groups := map[string]*model.SessionGroup{}
	devices := map[string]map[string]bool{}
	for _, sp := range raw.Series {
		var before, inside, after []int64
		for _, p := range sp.Points {
			switch {
			case p.Time < q.Start:
				before = append(before, p.Time)
			case p.Time >= q.End:
				after = append(after, p.Time)
			default:
				inside = append(inside, p.Time)
			}
		}
		runs := session.Split(bounds, inside, before, after)
		if len(runs) == 0 {
			continue
		}
		key, labels := window.Group(sp.Series.Labels, q.GroupBy)
		group := groups[key]
		if group == nil {
			group = &model.SessionGroup{Labels: labels}
			groups[key] = group
			devices[key] = map[string]bool{}
		}
		devices[key][sp.Series.ID] = true
		for _, run := range runs {
			result.Segments = append(result.Segments, model.Segment{
				Series: sp.Series, First: run.First, Last: run.Last,
				Points: run.Points, Left: run.Left, Right: run.Right,
			})
			group.Segments++
			group.Points += run.Points
			switch run.Left {
			case session.Started:
				group.Started++
			case session.CarriedIn:
				group.CarriedIn++
			case session.UnknownLeft:
				group.UnknownLeft++
			}
			switch run.Right {
			case session.Ended:
				group.Ended++
			case session.CarriedOut:
				group.CarriedOut++
			case session.UnknownRight:
				group.UnknownRight++
			}
		}
	}
	for key, group := range groups {
		group.Devices = len(devices[key])
		result.Groups = append(result.Groups, *group)
	}
	sort.Slice(result.Groups, func(i, j int) bool {
		a, _ := json.Marshal(result.Groups[i].Labels)
		b, _ := json.Marshal(result.Groups[j].Labels)
		return string(a) < string(b)
	})
	return result, nil
}

func (e *Engine) Sessions(name string, q model.SessionQuery) (model.SessionResult, error) {
	var result model.SessionResult
	if err := model.ValidateSessionQuery(q); err != nil {
		return result, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result, err = sessions(b, n, q, volume.Current(tx))
		return err
	})
	return result, err
}
