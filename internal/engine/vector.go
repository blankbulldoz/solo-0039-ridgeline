package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// joinSeries is one selected source series reduced to its join key and its
// visible samples inside the vector query interval.
type joinSeries struct {
	id     string
	points []model.Sample
}

// joinKey is the value-canonical identity used for association. Every label
// named in "on" appears in labels; an absent label joins as the empty string,
// exactly as matcher evaluation treats it. Series identity still preserves the
// absent/present distinction, so an absent label and a present-empty label are
// two distinct series with the same join value; they collide as a duplicate
// join key rather than being paired arbitrarily.
type joinKey struct {
	labels model.Labels
	canon  string
}

func makeJoinKey(labels model.Labels, on []string) joinKey {
	effective := model.Labels{}
	for _, name := range on {
		effective[name] = labels[name]
	}
	canon, _ := json.Marshal(effective)
	return joinKey{labels: effective, canon: string(canon)}
}

type joinSide struct {
	// byCanon groups selected series by their value-canonical join key. A key
	// with more than one series is a duplicate association and is rejected by
	// the declared cardinality instead of being paired by position or hash.
	byCanon map[string][]*joinSeries
	canon   map[string]joinKey
	series  int
	decoded int
}

func collectSide(b *bolt.Bucket, n model.Namespace, matchers []model.Matcher,
	q model.VectorQuery) (joinSide, error) {
	side := joinSide{byCanon: map[string][]*joinSeries{}, canon: map[string]joinKey{}}
	raw, err := scan(b, n, model.Query{Start: q.Start, End: q.End, Matchers: matchers}, 0)
	if err != nil {
		return side, err
	}
	side.series = len(raw.Series)
	side.decoded = raw.Stats.Decoded
	for _, sp := range raw.Series {
		key := makeJoinKey(sp.Series.Labels, q.On)
		if _, ok := side.canon[key.canon]; !ok {
			side.canon[key.canon] = key
		}
		s := &joinSeries{id: sp.Series.ID, points: sp.Points}
		side.byCanon[key.canon] = append(side.byCanon[key.canon], s)
	}
	for _, list := range side.byCanon {
		sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id })
	}
	return side, nil
}

func (e *Engine) Vector(name string, q model.VectorQuery) (model.VectorResult, error) {
	result := model.VectorResult{Op: q.Op, On: append([]string{}, q.On...), Points: []model.VectorPoint{}}
	if err := model.ValidateVector(q); err != nil {
		return result, err
	}
	relationship := q.Relationship
	if relationship == "" {
		relationship = "one_to_one"
	}
	result.Relationship = relationship

	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		result.Sequence = volume.Current(tx)
		// Both sides execute inside this single bbolt read transaction. The
		// returned sequence therefore describes one snapshot containing the
		// numerator and the denominator; no value can come from two views.
		left, err := collectSide(b, n, q.Left.Matchers, q)
		if err != nil {
			return err
		}
		rightN := n
		rightN.MaxScan = n.MaxScan - left.decoded
		right, err := collectSide(b, rightN, q.Right.Matchers, q)
		if err != nil {
			return err
		}
		result.Stats.LeftSeries = left.series
		result.Stats.RightSeries = right.series
		result.Stats.Decoded = left.decoded + right.decoded
		// A many-to-one key re-uses one right series for every left instance,
		// so emitted rows can exceed decoded points. Bound rows by the same
		// namespace budget and reject rather than truncate or grow without
		// limit. Truncating here would hide pairings that a caller asked for.
		if err := join(left, right, relationship, q, n.MaxScan, &result); err != nil {
			return err
		}
		return nil
	})
	return result, err
}

// join validates the declared cardinality and emits one row per timestamp in
// the union of each pair's samples. A key that violates the declaration is
// rejected instead of silently multiplying rows or picking one series
// arbitrarily. Keys that exist only on the right produce no rows because they
// describe no left-hand workload; they are counted in stats.right_only_keys so
// the omission is visible rather than silent.
func join(left, right joinSide, relationship string, q model.VectorQuery, budget int,
	result *model.VectorResult) error {
	for canon := range right.byCanon {
		if _, ok := left.byCanon[canon]; !ok {
			result.Stats.RightOnlyKeys++
		}
	}
	ordered := make([]string, 0, len(left.byCanon))
	for canon := range left.byCanon {
		ordered = append(ordered, canon)
	}
	sort.Strings(ordered)

	for _, canon := range ordered {
		ls, rs := left.byCanon[canon], right.byCanon[canon]
		key := left.canon[canon]
		switch relationship {
		case "one_to_one":
			if len(ls) > 1 || len(rs) > 1 {
				return model.Conflict(fmt.Sprintf(
					"join key %s has %d left and %d right series; one_to_one requires at most one on each side",
					describeKey(key), len(ls), len(rs)))
			}
		case "many_to_one":
			if len(rs) > 1 {
				return model.Conflict(fmt.Sprintf(
					"join key %s has %d right series; many_to_one requires at most one right series",
					describeKey(key), len(rs)))
			}
		case "one_to_many":
			if len(ls) > 1 {
				return model.Conflict(fmt.Sprintf(
					"join key %s has %d left series; one_to_many requires at most one left series",
					describeKey(key), len(ls)))
			}
		}
		if len(rs) == 0 {
			result.Stats.LeftOnlyKeys++
		} else {
			result.Stats.JoinedKeys++
		}
		pairs := make([][2]*joinSeries, 0)
		switch relationship {
		case "many_to_one":
			var r *joinSeries
			if len(rs) > 0 {
				r = rs[0]
			}
			for _, l := range ls {
				pairs = append(pairs, [2]*joinSeries{l, r})
			}
		case "one_to_many":
			if len(rs) == 0 {
				// A left key with no right match still surfaces as an explicit
				// missing_right row rather than disappearing.
				pairs = append(pairs, [2]*joinSeries{ls[0], nil})
			}
			for _, r := range rs {
				pairs = append(pairs, [2]*joinSeries{ls[0], r})
			}
		default:
			var r *joinSeries
			if len(rs) > 0 {
				r = rs[0]
			}
			pairs = append(pairs, [2]*joinSeries{ls[0], r})
		}
		var rows []model.VectorPoint
		for _, pair := range pairs {
			emitted, err := appendPair(key, pair[0], pair[1], q.Op, result)
			if err != nil {
				return err
			}
			rows = append(rows, emitted...)
		}
		if result.Stats.Rows > budget {
			return model.Capacity("vector query row budget exceeded")
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].Time != rows[j].Time {
				return rows[i].Time < rows[j].Time
			}
			if rows[i].LeftSeries != rows[j].LeftSeries {
				return rows[i].LeftSeries < rows[j].LeftSeries
			}
			return rows[i].RightSeries < rows[j].RightSeries
		})
		misaligned := false
		for _, row := range rows {
			if row.Status == model.VectorMissingLeft || row.Status == model.VectorMissingRight {
				misaligned = true
			}
		}
		if misaligned {
			result.Stats.MisalignedKeys++
		}
		result.Points = append(result.Points, rows...)
	}
	return nil
}

func describeKey(key joinKey) string {
	data, _ := json.Marshal(key.labels)
	return string(data)
}

// appendPair aligns two series on identical timestamps. It never interpolates
// or carries a nearby value forward: only a sample committed at exactly that
// timestamp counts as present. A side without such a sample leaves the value
// null and reports an explicit status instead of a numeric zero.
func appendPair(key joinKey, l, r *joinSeries, op string,
	result *model.VectorResult) ([]model.VectorPoint, error) {
	leftByTime, rightByTime := map[int64]float64{}, map[int64]float64{}
	var times []int64
	seen := map[int64]bool{}
	add := func(t int64) {
		if !seen[t] {
			seen[t] = true
			times = append(times, t)
		}
	}
	leftID, rightID := "", ""
	if l != nil {
		leftID = l.id
	}
	if r != nil {
		rightID = r.id
	}
	if l != nil {
		for _, p := range l.points {
			leftByTime[p.Time] = p.Value
			add(p.Time)
		}
	}
	if r != nil {
		for _, p := range r.points {
			rightByTime[p.Time] = p.Value
			add(p.Time)
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })

	rows := make([]model.VectorPoint, 0, len(times))
	for _, t := range times {
		lv, lok := leftByTime[t]
		rv, rok := rightByTime[t]
		row := model.VectorPoint{Key: key.labels, Time: t, LeftSeries: leftID, RightSeries: rightID}
		if lok {
			value := lv
			row.Left = &value
		}
		if rok {
			value := rv
			row.Right = &value
		}
		switch {
		case !lok && !rok:
			continue
		case !lok:
			row.Status = model.VectorMissingLeft
		case !rok:
			row.Status = model.VectorMissingRight
		case op == "/" && rv == 0:
			// Both positive and negative zero denominators are undefined.
			row.Status = model.VectorZeroDenominator
		default:
			value := lv
			switch op {
			case "/":
				value = lv / rv
			case "*":
				value = lv * rv
			case "+":
				value = lv + rv
			case "-":
				value = lv - rv
			}
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return nil, model.Capacity("vector arithmetic produced a non-finite result")
			}
			row.Value = &value
			row.Status = model.VectorOK
		}
		rows = append(rows, row)
		result.Stats.Rows++
		switch row.Status {
		case model.VectorOK:
			result.Stats.OK++
		case model.VectorMissingLeft:
			result.Stats.MissingLeft++
		case model.VectorMissingRight:
			result.Stats.MissingRight++
		case model.VectorZeroDenominator:
			result.Stats.ZeroDenominator++
		}
	}
	return rows, nil
}
