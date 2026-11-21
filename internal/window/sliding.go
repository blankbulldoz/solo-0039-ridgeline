package window

import (
	"math"
	"sort"

	"ridgeline/internal/model"
)

// Bounds keep a sliding request from producing an unbounded response. They
// reject excessive work instead of silently dropping outputs.
const (
	MaxOutputs = 1000000
	MaxCells   = 2000000
	MaxWork    = 20000000
)

// SlidingPlan describes the aligned output times for a sliding query and the
// source interval that must be read to answer them.
type SlidingPlan struct {
	Width     int64
	Step      int64
	First     int64 // first aligned output time
	Last      int64 // last aligned output time
	Outputs   int64
	ReadStart int64 // earliest source timestamp any window can read
}

// alignCeil returns the smallest origin-aligned time greater than or equal to t.
func alignCeil(t, origin, step int64) int64 {
	delta := t - origin
	index := delta / step
	if delta > 0 && delta%step != 0 {
		index++
	}
	return origin + index*step
}

// PlanSliding computes output and read boundaries. Output times are aligned to
// origin and restricted to the half-open query interval [start, end). Each
// output t reports [t-width, t), so the first output may read source samples
// from before start.
func PlanSliding(q model.Query) SlidingPlan {
	plan := SlidingPlan{Width: q.Width, Step: q.Step}
	plan.First = alignCeil(q.Start, q.Origin, q.Step)
	stop := alignCeil(q.End, q.Origin, q.Step)
	if stop <= plan.First {
		return plan
	}
	plan.Outputs = (stop - plan.First) / q.Step
	plan.Last = plan.First + (plan.Outputs-1)*q.Step
	plan.ReadStart = plan.First - q.Width
	if plan.ReadStart < model.MinTime {
		plan.ReadStart = model.MinTime
	}
	return plan
}

// MaxReuse is the largest number of output windows a single observation can
// belong to. It is a property of width and step, not of the data volume.
func (p SlidingPlan) MaxReuse() int64 {
	reuse := (p.Width + p.Step - 1) / p.Step
	if reuse > p.Outputs {
		reuse = p.Outputs
	}
	return reuse
}

// summary is an associative reduction of observations in queue order. has
// distinguishes an empty range from a populated one.
type summary struct {
	has   bool
	count int64
	sum   float64
	min   float64
	max   float64
	// first and last track observation order, which is (time, series ID).
	// They make the reduction associative without a merge-order tie-break.
	first slidePoint
	last  slidePoint
}

type slidePoint struct {
	time   int64
	series string
	value  float64
}

func combine(a, b summary) summary {
	if !a.has {
		return b
	}
	if !b.has {
		return a
	}
	return summary{
		has: a.has && b.has, count: a.count + b.count, sum: a.sum + b.sum,
		min: math.Min(a.min, b.min), max: math.Max(a.max, b.max),
		first: a.first, last: b.last,
	}
}

type aggEntry struct {
	point slidePoint
	agg   summary
}

// slideQueue is a two-stack sliding-window accumulator (SWAG). Pushing and
// popping an observation costs O(1) amortized and reading the aggregate costs
// O(1), so total reduction work is proportional to the number of observations,
// never to the number of window memberships those observations create.
type slideQueue struct {
	in  []aggEntry
	out []aggEntry
}

func (q *slideQueue) push(p slidePoint) {
	s := summary{
		has: true, count: 1, sum: p.value, min: p.value, max: p.value,
		first: p, last: p,
	}
	if n := len(q.in); n > 0 {
		s = combine(q.in[n-1].agg, s)
	}
	q.in = append(q.in, aggEntry{point: p, agg: s})
}

func (q *slideQueue) transfer() {
	next := make([]aggEntry, 0, len(q.in))
	for i := len(q.in) - 1; i >= 0; i-- {
		s := summary{
			has: true, count: 1, sum: q.in[i].point.value,
			min: q.in[i].point.value, max: q.in[i].point.value,
			first: q.in[i].point, last: q.in[i].point,
		}
		if n := len(next); n > 0 {
			s = combine(s, next[n-1].agg)
		}
		next = append(next, aggEntry{point: q.in[i].point, agg: s})
	}
	q.in = q.in[:0]
	q.out = next
}

func (q *slideQueue) pop() {
	if len(q.out) == 0 {
		q.transfer()
	}
	q.out = q.out[:len(q.out)-1]
}

func (q *slideQueue) front() slidePoint {
	if len(q.out) > 0 {
		return q.out[len(q.out)-1].point
	}
	return q.in[0].point
}

func (q *slideQueue) total() summary {
	var out, in summary
	if len(q.out) > 0 {
		out = q.out[len(q.out)-1].agg
	}
	if len(q.in) > 0 {
		in = q.in[len(q.in)-1].agg
	}
	return combine(out, in)
}

func reduce(s summary, function string) float64 {
	switch function {
	case "count":
		return float64(s.count)
	case "sum":
		return s.sum
	case "avg":
		return s.sum / float64(s.count)
	case "min":
		return s.min
	case "max":
		return s.max
	case "first":
		return s.first.value
	default:
		return s.last.value
	}
}

func saturatingAdd(total, delta int64) int64 {
	if delta > 0 && total > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return total + delta
}

// AggregateSliding reduces sliding windows without materializing each
// observation once per window. Streams are ordered by (time, series ID), which
// preserves the tumbling first/last tie-break rules.
func AggregateSliding(series []model.SeriesPoints, q model.Query, plan SlidingPlan) ([]model.Cell, model.SlidingStats, error) {
	stats := model.SlidingStats{
		Width: q.Width, Step: q.Step, Outputs: int(plan.Outputs), MaxReuse: plan.MaxReuse(),
	}
	streams := map[string]*groupStream{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		stream := streams[key]
		if stream == nil {
			stream = &groupStream{labels: labels, points: make([]slidePoint, 0, len(s.Points))}
			streams[key] = stream
		}
		for _, p := range s.Points {
			stream.points = append(stream.points, slidePoint{time: p.Time, series: s.Series.ID, value: p.Value})
		}
	}
	keys := make([]string, 0, len(streams))
	kFirst := (plan.First - q.Origin) / q.Step
	kLast := kFirst + plan.Outputs - 1
	// active[key] is the first output index a group can populate. Outputs are
	// then visited through the end of the span even after the group goes
	// silent, so a stopped reporter yields counted empty windows. A group that
	// never observed anything has no active index and stays omitted.
	active := make(map[string]int64, len(streams))
	for key, stream := range streams {
		sort.Slice(stream.points, func(i, j int) bool {
			a, b := stream.points[i], stream.points[j]
			if a.time != b.time {
				return a.time < b.time
			}
			return a.series < b.series
		})
		for _, p := range stream.points {
			stats.Observations++
			// A point belongs to outputs t with p < t <= p+width, i.e. output
			// indexes floor((p-origin)/step)+1 .. floor((p+width-origin)/step).
			low := floorIndex(p.time, q.Origin, q.Step) + 1
			high := floorIndex(p.time+q.Width, q.Origin, q.Step)
			if low < kFirst {
				low = kFirst
			}
			if high > kLast {
				high = kLast
			}
			if high >= low {
				stats.Memberships = saturatingAdd(stats.Memberships, high-low+1)
			}
		}
		lo := floorIndex(stream.points[0].time, q.Origin, q.Step) + 1
		if lo < kFirst {
			lo = kFirst
		}
		active[key] = lo - kFirst
		keys = append(keys, key)
	}
	stats.Reductions = stats.Observations
	stats.Groups = len(streams)
	sort.Strings(keys)

	if int64(len(streams)) > 0 && plan.Outputs > MaxWork/int64(len(streams)) {
		return nil, stats, model.Capacity("sliding output and group count exceed the supported limit")
	}
	// Groups join the live set at their first reachable output and then never
	// leave: a silent group must still be recognized as an empty window. The
	// pending schedule keeps outputs from touching groups that cannot report.
	pending := make([]string, 0, len(streams))
	for _, key := range keys {
		pending = append(pending, key)
	}
	sort.Slice(pending, func(i, j int) bool { return active[pending[i]] < active[pending[j]] })
	nextPending := 0
	live := make([]string, 0, len(streams))
	queues := make(map[string]*slideQueue, len(streams))
	positions := make(map[string]int, len(streams))
	seen := make(map[string]bool, len(streams))
	cells := make([]model.Cell, 0)
	// lastIndex bounds the visit loop. No observation can reach an output later
	// than its own index plus the reuse factor, so outputs past the largest
	// such index are empty for every group. The loop counts those
	// arithmetically instead of touching each group once per output.
	lastIndex := int64(-1)
	// silentFrom[key] is the first output index at which the group has gone
	// quiet for good; later empty windows for it are counted in one step.
	silentFrom := make(map[string]int64, len(streams))
	for key := range streams {
		last := streams[key].points[len(streams[key].points)-1].time
		hi := floorIndex(last+q.Width, q.Origin, q.Step) - kFirst
		if hi > lastIndex {
			lastIndex = hi
		}
		silentFrom[key] = hi + 1
	}
	if lastIndex >= plan.Outputs {
		lastIndex = plan.Outputs - 1
	}
	for index := int64(0); index < plan.Outputs; index++ {
		if index > lastIndex {
			// No group can reach this output or any later one. Each remaining
			// output is empty and each still-live group is silent from here on.
			remaining := int(plan.Outputs - index)
			stats.EmptyOutputs += remaining
			stats.GapCells += remaining * len(live)
			break
		}
		for nextPending < len(pending) && active[pending[nextPending]] <= index {
			live = append(live, pending[nextPending])
			nextPending++
		}
		for position := 0; position < len(live); position++ {
			key := live[position]
			if silentFrom[key] <= index {
				// The group cannot report again; accrue its empty windows once.
				if seen[key] {
					stats.GapCells += int(plan.Outputs - index)
				}
				live[position] = live[len(live)-1]
				live = live[:len(live)-1]
				position--
			}
		}
		t := plan.First + index*q.Step
		low := t - plan.Width
		populated := false
		for _, key := range live {
			stream := streams[key]
			queue := queues[key]
			if queue == nil {
				queue = &slideQueue{}
				queues[key] = queue
			}
			position := positions[key]
			for position < len(stream.points) && stream.points[position].time < t {
				queue.push(stream.points[position])
				position++
			}
			positions[key] = position
			for len(queue.in)+len(queue.out) > 0 && queue.front().time < low {
				queue.pop()
			}
			reduced := queue.total()
			if !reduced.has {
				// A silence after the group first reported is an interior gap;
				// only populated windows become cells.
				if seen[key] {
					stats.GapCells++
				}
				continue
			}
			seen[key] = true
			if len(cells) >= MaxCells {
				return nil, stats, model.Capacity("sliding cell count exceeds the supported limit")
			}
			value := reduce(reduced, q.Function)
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return nil, stats, model.Capacity("aggregation overflow")
			}
			populated = true
			cells = append(cells, model.Cell{
				Labels: stream.labels, Start: t - plan.Width, End: t,
				Value: value, Count: int(reduced.count),
			})
		}
		if !populated {
			stats.EmptyOutputs++
		}
	}
	stats.Cells = len(cells)
	SortCells(cells)
	return cells, stats, nil
}

type groupStream struct {
	labels model.Labels
	points []slidePoint
}

func floorIndex(t, origin, step int64) int64 {
	delta := t - origin
	index := delta / step
	if delta < 0 && delta%step != 0 {
		index--
	}
	return index
}
