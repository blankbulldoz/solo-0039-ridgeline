// Package histogram owns cumulative histogram semantics: bucket-layout
// fingerprints, reset-aware window increases, cross-instance merges and
// bucket-interpolated quantile estimates.
package histogram

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/window"
)

// Machine-readable limitation codes attached to every result cell.
const (
	// LimitationResolution: quantiles are interpolated inside finite-width
	// buckets, so they are estimates rather than exact order statistics.
	LimitationResolution = "bucket_resolution_estimate"
	// LimitationBaseline: each window's first snapshot per series is a
	// baseline; its pre-window contribution is not extrapolated.
	LimitationBaseline = "window_baseline_excluded"
	// LimitationReset: at least one cumulative counter restart was observed.
	LimitationReset = "counter_reset_observed"
	// LimitationIncompatible: snapshots with different bucket upper bounds
	// met in the same cell; their counts were never added together.
	LimitationIncompatible = "incompatible_bucket_layouts"
	// LimitationCapped: one or more estimates fell into an open-ended edge
	// bucket and were pinned to the nearest finite bucket bound.
	LimitationCapped = "quantile_capped_at_finite_bound"
)

// Fingerprint identifies a bucket layout. Snapshots may only be merged when
// their fingerprints are identical; different layouts have different units
// per bucket and must never be summed.
func Fingerprint(bounds []float64) string {
	data, _ := json.Marshal(bounds)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:16])
}

// Segment is the reset-aware increase of one series inside one window,
// restricted to one bucket layout. Counts holds increases per bucket, not
// cumulative totals.
type Segment struct {
	Fingerprint string
	Bounds      []float64
	Counts      []uint64
	Sum         float64
	Resets      int
}

func addUint64(a, b uint64) (uint64, error) {
	if a > math.MaxUint64-b {
		return 0, model.Capacity("histogram cumulative count overflow")
	}
	return a + b, nil
}

// ReduceSeries turns an ordered series of cumulative snapshots into layout
// segments. A decrease in any bucket count or in the cumulative sum means the
// emitting counter restarted: that snapshot is treated as the post-reset
// baseline. A different bucket layout also closes the current segment, but is
// not itself a counter reset.
func ReduceSeries(points []model.HistogramSample) ([]Segment, error) {
	if len(points) == 0 {
		return nil, nil
	}
	ordered := make([]model.HistogramSample, len(points))
	copy(ordered, points)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Time < ordered[j].Time })

	segments := []Segment{}
	var previous *model.HistogramSample
	for i := range ordered {
		current := &ordered[i]
		fp := Fingerprint(current.Bounds)
		if previous == nil || Fingerprint(previous.Bounds) != fp {
			// First snapshot, or a layout change: start a fresh segment whose
			// first snapshot is only the baseline.
			segments = append(segments, Segment{
				Fingerprint: fp, Bounds: append([]float64(nil), current.Bounds...),
				Counts: make([]uint64, len(current.Counts)),
			})
			previous = current
			continue
		}
		segment := &segments[len(segments)-1]
		reset := current.Sum < previous.Sum
		for i := range current.Counts {
			if current.Counts[i] < previous.Counts[i] {
				reset = true
				break
			}
		}
		if reset {
			segment.Resets++
			for i := range current.Counts {
				total, err := addUint64(segment.Counts[i], current.Counts[i])
				if err != nil {
					return nil, err
				}
				segment.Counts[i] = total
			}
			segment.Sum += current.Sum
		} else {
			for i := range current.Counts {
				total, err := addUint64(segment.Counts[i], current.Counts[i]-previous.Counts[i])
				if err != nil {
					return nil, err
				}
				segment.Counts[i] = total
			}
			segment.Sum += current.Sum - previous.Sum
		}
		if math.IsInf(segment.Sum, 0) || math.IsNaN(segment.Sum) {
			return nil, model.Capacity("histogram sum overflow")
		}
		previous = current
	}
	return segments, nil
}

type schemeAggregate struct {
	fingerprint string
	bounds      []float64
	counts      []uint64
	sum         float64
	series      int
}

type cellAccumulator struct {
	labels  model.Labels
	start   int64
	end     int64
	schemes map[string]*schemeAggregate
	resets  int
}

// Estimate returns linear-interpolation quantile estimates over a merged
// cumulative histogram. The first bucket is treated as open-ended on the low
// side; estimates landing there are pinned to its upper bound and flagged.
func Estimate(bounds []float64, counts []uint64, quantiles []float64) ([]model.QuantileEstimate, bool, error) {
	if len(bounds) != len(counts) {
		return nil, false, model.Bad("histogram bounds and counts must have equal length")
	}
	out := make([]model.QuantileEstimate, 0, len(quantiles))
	total := uint64(0)
	if len(counts) > 0 {
		total = counts[len(counts)-1]
	}
	cappedAny := false
	for _, q := range quantiles {
		estimate := model.QuantileEstimate{Quantile: q}
		if total == 0 {
			out = append(out, estimate)
			continue
		}
		rank := q * float64(total)
		index := 0
		for index < len(counts) && float64(counts[index]) < rank {
			index++
		}
		if index == len(counts) {
			// Float rounding of very large cumulative totals can push the rank
			// past the last stored boundary; pin it to the final bucket.
			index = len(counts) - 1
		}
		upper := bounds[index]
		prevCum := uint64(0)
		if index > 0 {
			prevCum = counts[index-1]
		}
		if index == 0 {
			// Open low end: no finite lower bound exists.
			value := upper
			estimate.Value = &value
			estimate.Capped = true
			cappedAny = true
			out = append(out, estimate)
			continue
		}
		lower := bounds[index-1]
		span := float64(counts[index] - prevCum)
		fraction := 0.0
		if span > 0 {
			fraction = (rank - float64(prevCum)) / span
		}
		if fraction < 0 {
			fraction = 0
		} else if fraction > 1 {
			fraction = 1
		}
		value := lower + fraction*(upper-lower)
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return nil, false, model.Capacity("quantile estimation overflow")
		}
		estimate.Value = &value
		out = append(out, estimate)
	}
	return out, cappedAny, nil
}

func groupKey(labels model.Labels, names []string) (string, model.Labels) {
	out := model.Labels{}
	for _, name := range names {
		if value, present := labels[name]; present {
			out[name] = value
		}
	}
	data, _ := json.Marshal(out)
	return string(data), out
}

// Aggregate reduces histogram series into origin-aligned window cells. Series
// with identical bucket layouts are summed; cells that meet different layouts
// report per-layout partial results and an explicit incompatibility limitation
// instead of adding incompatible buckets.
func Aggregate(series []model.SeriesHistograms, q model.HistogramQuery) ([]model.HistogramCell, error) {
	if err := model.ValidateHistogramQuery(q); err != nil {
		return nil, err
	}
	cells := map[string]*cellAccumulator{}
	for _, s := range series {
		key, labels := groupKey(s.Series.Labels, q.GroupBy)
		windows := map[int64][]model.HistogramSample{}
		for _, point := range s.Points {
			start := window.Bucket(point.Time, q.Origin, q.Step)
			windows[start] = append(windows[start], point)
		}
		for start, points := range windows {
			segments, err := ReduceSeries(points)
			if err != nil {
				return nil, err
			}
			cellKey := fmt.Sprintf("%s/%d", key, start)
			cell := cells[cellKey]
			if cell == nil {
				cell = &cellAccumulator{
					labels: labels, start: start, end: start + q.Step,
					schemes: map[string]*schemeAggregate{},
				}
				cells[cellKey] = cell
			}
			contributed := map[string]bool{}
			for _, segment := range segments {
				agg := cell.schemes[segment.Fingerprint]
				if agg == nil {
					agg = &schemeAggregate{
						fingerprint: segment.Fingerprint,
						bounds:      append([]float64(nil), segment.Bounds...),
						counts:      make([]uint64, len(segment.Counts)),
					}
					cell.schemes[segment.Fingerprint] = agg
				}
				if len(agg.counts) != len(segment.Counts) {
					return nil, model.Bad("histogram fingerprint collision across bucket layouts")
				}
				for i := range segment.Counts {
					total, err := addUint64(agg.counts[i], segment.Counts[i])
					if err != nil {
						return nil, err
					}
					agg.counts[i] = total
				}
				agg.sum += segment.Sum
				cell.resets += segment.Resets
				if !contributed[segment.Fingerprint] {
					agg.series++
					contributed[segment.Fingerprint] = true
				}
			}
		}
	}
	out := make([]model.HistogramCell, 0, len(cells))
	for _, cell := range cells {
		built, err := buildCell(cell, q.Quantiles)
		if err != nil {
			return nil, err
		}
		out = append(out, built)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := json.Marshal(out[i].Labels)
		b, _ := json.Marshal(out[j].Labels)
		if string(a) == string(b) {
			return out[i].Start < out[j].Start
		}
		return string(a) < string(b)
	})
	return out, nil
}

func buildCell(cell *cellAccumulator, quantiles []float64) (model.HistogramCell, error) {
	result := model.HistogramCell{
		Labels: cell.labels, Start: cell.start, End: cell.end,
		Quantiles:   []model.QuantileEstimate{},
		Limitations: []string{LimitationResolution, LimitationBaseline},
	}
	if cell.resets > 0 {
		result.Limitations = append(result.Limitations, LimitationReset)
	}
	fingerprints := make([]string, 0, len(cell.schemes))
	for fp := range cell.schemes {
		fingerprints = append(fingerprints, fp)
	}
	sort.Strings(fingerprints)
	if len(fingerprints) == 1 {
		agg := cell.schemes[fingerprints[0]]
		result.Compatible = true
		result.Fingerprint = agg.fingerprint
		result.Bounds = append([]float64(nil), agg.bounds...)
		result.Counts = append([]uint64(nil), agg.counts...)
		if len(agg.counts) > 0 {
			result.Count = agg.counts[len(agg.counts)-1]
		}
		result.Sum, result.Series, result.Resets = agg.sum, agg.series, cell.resets
		estimates, capped, err := Estimate(agg.bounds, agg.counts, quantiles)
		if err != nil {
			return result, err
		}
		result.Quantiles = estimates
		if capped {
			result.Limitations = append(result.Limitations, LimitationCapped)
		}
		return result, nil
	}
	// Incompatible layouts: never sum buckets from different schemes. Surface
	// each layout's partial increase so clients can pick or reconcile.
	result.Compatible = false
	result.Limitations = append(result.Limitations, LimitationIncompatible)
	for _, fp := range fingerprints {
		agg := cell.schemes[fp]
		partial := model.SchemePartial{
			Fingerprint: agg.fingerprint,
			Bounds:      append([]float64(nil), agg.bounds...),
			Counts:      append([]uint64(nil), agg.counts...),
			Sum:         agg.sum, Series: agg.series,
		}
		if len(agg.counts) > 0 {
			partial.Count = agg.counts[len(agg.counts)-1]
		}
		result.Schemes = append(result.Schemes, partial)
	}
	result.Resets = cell.resets
	return result, nil
}
