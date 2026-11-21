// Package settle turns adjacent counter samples and boot identifier
// evidence into per-interval settlement decisions. A counter interval is
// settled only when observations of one boot identifier cover the whole
// interval; wrap candidates are bounded by a maximum growth rate so finance
// sees either one exact increment or an explicit minimum/maximum range.
package settle

import (
	"math"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/window"
)

// Interval statuses.
const (
	Settled   = "settled"   // continuous boot, exactly one plausible increment
	Ambiguous = "ambiguous" // continuous boot, several wrap counts plausible
	Rebooted  = "rebooted"  // covering boot observations disagree
	Unknown   = "unknown"   // boot evidence does not cover the interval
	Exceeded  = "exceeded"  // continuous boot, minimum increment above max rate
)

// maxLimit keeps candidate arithmetic exact: plausible increments stay well
// inside the exact integer range of both float64 samples and int64 math.
const maxLimit = 1 << 52

type device struct {
	labels model.Labels
	count  *model.SeriesPoints
	boot   *model.SeriesPoints
}

// Groups joins count and boot series on the request's device labels and
// resolves every adjacent count interval. Each device must isolate at most
// one count and one boot series; ambiguous joins are rejected, never merged.
func Groups(counts, boots []model.SeriesPoints, req model.SettleRequest) ([]model.SettleGroup, error) {
	devices := map[string]*device{}
	for i := range counts {
		key, labels := window.Group(counts[i].Series.Labels, req.Join)
		d := devices[key]
		if d == nil {
			d = &device{labels: labels}
			devices[key] = d
		}
		if d.count != nil {
			return nil, model.Bad("device join labels do not isolate one count series")
		}
		d.count = &counts[i]
	}
	for i := range boots {
		key, labels := window.Group(boots[i].Series.Labels, req.Join)
		d := devices[key]
		if d == nil {
			d = &device{labels: labels}
			devices[key] = d
		}
		if d.boot != nil {
			return nil, model.Bad("device join labels do not isolate one boot series")
		}
		d.boot = &boots[i]
	}
	keys := make([]string, 0, len(devices))
	for key, d := range devices {
		if d.count != nil {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]model.SettleGroup, 0, len(keys))
	for _, key := range keys {
		d := devices[key]
		if d.boot != nil && d.boot.Series.ID == d.count.Series.ID {
			return nil, model.Bad("count and boot selectors resolve to the same series")
		}
		var bootPoints []model.Sample
		bootID := ""
		if d.boot != nil {
			bootPoints = d.boot.Points
			bootID = d.boot.Series.ID
		}
		intervals, total, err := Resolve(d.count.Points, bootPoints, req)
		if err != nil {
			return nil, err
		}
		group := model.SettleGroup{
			Labels: d.labels, CountSeries: d.count.Series.ID, BootSeries: bootID,
			Intervals: intervals, SettledTotal: total, Statuses: map[string]int{},
		}
		for _, interval := range intervals {
			group.Statuses[interval.Status]++
		}
		out = append(out, group)
	}
	return out, nil
}

// Resolve classifies each adjacent pair of count samples. counts and boots
// must be resolved views sorted by time for one device.
func Resolve(counts, boots []model.Sample, req model.SettleRequest) ([]model.SettleInterval, int64, error) {
	values := make([]int64, len(counts))
	for i, p := range counts {
		if p.Value != math.Trunc(p.Value) || p.Value < 0 || p.Value >= float64(req.Modulus) {
			return nil, 0, model.Bad("count sample is not an integer inside [0, modulus)")
		}
		values[i] = int64(p.Value)
	}
	intervals := []model.SettleInterval{}
	var total int64
	for i := 1; i < len(counts); i++ {
		start, end := counts[i-1].Time, counts[i].Time
		interval := model.SettleInterval{
			Start: start, End: end, From: values[i-1], To: values[i],
		}
		before, after, conflict := evidence(boots, start, end)
		interval.BootBefore, interval.BootAfter = before, after
		switch {
		case before == nil || after == nil:
			interval.Status = Unknown
		case *before != *after || conflict:
			interval.Status = Rebooted
		default:
			if err := continuous(&interval, end-start, req); err != nil {
				return nil, 0, err
			}
			if interval.Status == Settled {
				if total > math.MaxInt64-*interval.Increment {
					return nil, 0, model.Capacity("settled total exceeds exact integer range")
				}
				total += *interval.Increment
			}
		}
		intervals = append(intervals, interval)
	}
	return intervals, total, nil
}

// continuous settles an interval known to run under one boot identifier.
// Plausible increments are raw + k*modulus bounded by max_rate per second.
func continuous(interval *model.SettleInterval, elapsed int64, req model.SettleRequest) error {
	min := interval.To - interval.From
	if min < 0 {
		min += req.Modulus
	}
	limit := req.MaxRate * float64(elapsed) / 1000
	if limit > maxLimit {
		return model.Capacity("settlement plausibility window exceeds exact integer range")
	}
	if float64(min) > limit {
		interval.Status = Exceeded
		return nil
	}
	k := int64(math.Floor((limit - float64(min)) / float64(req.Modulus)))
	// Correct float rounding at the candidate boundary.
	for k > 0 && float64(min+k*req.Modulus) > limit {
		k--
	}
	for float64(min+(k+1)*req.Modulus) <= limit {
		k++
	}
	if k == 0 {
		interval.Status = Settled
		interval.Increment = &min
		if req.Reconcile != nil {
			match := *req.Reconcile == min
			interval.ReconcileMatch = &match
		}
		return nil
	}
	max := min + k*req.Modulus
	interval.Status = Ambiguous
	interval.Min, interval.Max = &min, &max
	if req.Reconcile != nil {
		r := *req.Reconcile
		match := r >= min && r <= max && (r-min)%req.Modulus == 0
		interval.ReconcileMatch = &match
	}
	return nil
}

// evidence finds the boot observations covering [start, end]: the newest at
// or before start, the oldest at or after end, and whether any observation
// strictly inside the interval disagrees with the first. Only observations
// of one boot identifier across the whole interval prove continuous running.
func evidence(boots []model.Sample, start, end int64) (before, after *float64, conflict bool) {
	i := sort.Search(len(boots), func(k int) bool { return boots[k].Time > start }) - 1
	if i >= 0 {
		v := boots[i].Value
		before = &v
	}
	j := sort.Search(len(boots), func(k int) bool { return boots[k].Time >= end })
	if j < len(boots) {
		v := boots[j].Value
		after = &v
	}
	if before != nil {
		for k := i + 1; k < j; k++ {
			if boots[k].Value != *before {
				conflict = true
			}
		}
	}
	return before, after, conflict
}
