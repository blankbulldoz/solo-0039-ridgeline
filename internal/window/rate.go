package window

import (
	"fmt"
	"math"
	"sort"

	"ridgeline/internal/model"
)

// counterIncrease applies the reset-aware counter rule to one adjacent pair:
// a decrease is treated as a reset to zero, so the new value is the increase.
func counterIncrease(prev, next float64) float64 {
	delta := next - prev
	if delta < 0 {
		return next
	}
	return delta
}

// Rates computes reset-aware per-second counter rates.
//
// Default contract (RateEdgeContext false): only samples inside a window
// participate, the window needs at least two samples, and the denominator is
// the elapsed time between its first and last observed sample. No
// extrapolation is performed.
//
// Edge context (opt in): a window holding exactly one sample may borrow the
// nearest visible neighbor just outside each boundary, provided the neighbor
// lies no farther than RateMaxGapMS from that boundary. Borrowed increments
// are never counted in full: each supporting segment contributes only the
// time-weighted share that overlaps the window. Every produced cell carries
// rate evidence describing the real supporting timestamps, the covered
// versus extrapolated duration and any reset detected across an edge.
func Rates(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	cells := map[string]*model.Cell{}
	// Unsupported single-sample series are recorded per group so that a cell
	// produced by another series still explains the missing support.
	unsupported := map[string]map[string]model.SeriesRateEvidence{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		points := s.Points
		for _, point := range points {
			if point.Value < 0 {
				return nil, model.Bad("counter rate requires nonnegative values")
			}
		}
		windows := map[int64][]model.Sample{}
		for _, point := range points {
			// Edge-context scans may include bounded outside samples. Those
			// samples only support neighbor lookups; they must never create
			// a window of their own or leak increments beyond the requested
			// range. Default scans already enforce the same range.
			if q.RateEdgeContext && (point.Time < q.Start || point.Time >= q.End) {
				continue
			}
			start := Bucket(point.Time, q.Origin, q.Step)
			windows[start] = append(windows[start], point)
		}
		for start, inside := range windows {
			windowEnd := start + q.Step
			k := fmt.Sprintf("%s/%d", key, start)
			// Lazily create cells so skipped single-sample windows leave no
			// empty cell behind, preserving the original rate contract.
			ensureCell := func() *model.Cell {
				cell := cells[k]
				if cell == nil {
					cell = &model.Cell{Labels: labels, Start: start, End: windowEnd}
					cells[k] = cell
				}
				return cell
			}
			if len(inside) >= 2 {
				value, increase, err := observedRate(inside)
				if err != nil {
					return nil, err
				}
				cell := ensureCell()
				cell.Value += value
				cell.Count += len(inside)
				if q.RateEdgeContext {
					ev := ensureEvidence(cell, q.RateMaxGapMS)
					ev.CoveredMS += inside[len(inside)-1].Time - inside[0].Time
					ev.Series = append(ev.Series, model.SeriesRateEvidence{
						SeriesID:        s.Series.ID,
						Mode:            "observed",
						SupportStart:    inside[0].Time,
						SupportEnd:      inside[len(inside)-1].Time,
						InWindowSamples: len(inside),
						CoveredMS:       inside[len(inside)-1].Time - inside[0].Time,
						Increase:        increase,
					})
				}
				continue
			}
			if !q.RateEdgeContext {
				// Preserve the original contract: single-sample windows
				// produce no value and no cell.
				continue
			}
			contrib, ok := edgeContribution(points, inside[0], start, windowEnd, q, s.Series.ID)
			if !ok {
				group := unsupported[k]
				if group == nil {
					group = map[string]model.SeriesRateEvidence{}
					unsupported[k] = group
				}
				group[s.Series.ID] = model.SeriesRateEvidence{
					SeriesID:        s.Series.ID,
					Mode:            "unsupported",
					SupportStart:    inside[0].Time,
					SupportEnd:      inside[0].Time,
					InWindowSamples: 1,
					ExtrapolatedMS:  q.Step,
				}
				continue
			}
			cell := ensureCell()
			cell.Value += contrib.rate
			cell.Count++
			ev := ensureEvidence(cell, q.RateMaxGapMS)
			ev.Series = append(ev.Series, contrib.evidence)
		}
	}
	out := make([]model.Cell, 0, len(cells))
	for k, cell := range cells {
		if q.RateEdgeContext {
			ev := cell.Evidence
			observed, edged := 0, 0
			for _, se := range ev.Series {
				switch se.Mode {
				case "observed":
					observed++
				case "edge_context":
					edged++
				}
			}
			if missed := unsupported[k]; len(missed) > 0 {
				ids := make([]string, 0, len(missed))
				for id := range missed {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					ev.Series = append(ev.Series, missed[id])
				}
				ev.UnsupportedSeries = len(ids)
			}
			switch {
			case observed > 0 && edged > 0:
				ev.Mode = "mixed"
			case observed > 0:
				ev.Mode = "observed"
			default:
				ev.Mode = "edge_context"
			}
			sort.Slice(ev.Series, func(i, j int) bool { return ev.Series[i].SeriesID < ev.Series[j].SeriesID })
			supportSet := false
			for _, se := range ev.Series {
				if se.Mode == "unsupported" {
					continue
				}
				if !supportSet || se.SupportStart < ev.SupportStart {
					ev.SupportStart = se.SupportStart
					supportSet = true
				}
				if se.SupportEnd > ev.SupportEnd {
					ev.SupportEnd = se.SupportEnd
				}
				ev.CoveredMS += se.CoveredMS
				ev.ExtrapolatedMS += se.ExtrapolatedMS
				ev.EdgeSamples += se.EdgeSamples
				if se.EdgeReset {
					ev.EdgeReset = true
				}
			}
		}
		if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
			return nil, model.Capacity("counter calculation overflow")
		}
		out = append(out, *cell)
	}
	SortCells(out)
	return out, nil
}

func ensureEvidence(cell *model.Cell, maxGap int64) *model.RateEvidence {
	if cell.Evidence == nil {
		cell.Evidence = &model.RateEvidence{MaxGapMS: maxGap, Series: []model.SeriesRateEvidence{}}
	}
	return cell.Evidence
}

// observedRate keeps the original rate arithmetic for multi-sample windows.
func observedRate(points []model.Sample) (float64, float64, error) {
	increase := 0.0
	for i := 1; i < len(points); i++ {
		increase += counterIncrease(points[i-1].Value, points[i].Value)
	}
	elapsed := float64(points[len(points)-1].Time-points[0].Time) / 1000
	if elapsed <= 0 {
		return 0, 0, model.Bad("counter rate requires distinct sample times")
	}
	value := increase / elapsed
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, 0, model.Capacity("counter calculation overflow")
	}
	return value, increase, nil
}

type edgeResult struct {
	rate     float64
	evidence model.SeriesRateEvidence
}

// edgeContribution evaluates one series' single in-window sample against the
// nearest visible neighbors on each side, bounded by the configured gap.
func edgeContribution(points []model.Sample, sole model.Sample, start, end int64, q model.Query, seriesID string) (edgeResult, bool) {
	left := nearestBefore(points, sole.Time)
	right := nearestAfter(points, sole.Time)
	if left != nil && start-left.Time > q.RateMaxGapMS {
		left = nil
	}
	if right != nil && right.Time-end > q.RateMaxGapMS {
		right = nil
	}
	if left == nil && right == nil {
		return edgeResult{}, false
	}
	increase := 0.0
	edgeReset := false
	edgeSamples := 0
	supportStart, supportEnd := sole.Time, sole.Time
	covered := int64(0)
	if left != nil {
		seg := counterIncrease(left.Value, sole.Value)
		if sole.Value-left.Value < 0 {
			edgeReset = true
		}
		increase += seg * float64(sole.Time-start) / float64(sole.Time-left.Time)
		covered += sole.Time - start
		edgeSamples++
		supportStart = left.Time
	}
	if right != nil {
		seg := counterIncrease(sole.Value, right.Value)
		if right.Value-sole.Value < 0 {
			edgeReset = true
		}
		increase += seg * float64(end-sole.Time) / float64(right.Time-sole.Time)
		covered += end - sole.Time
		edgeSamples++
		supportEnd = right.Time
	}
	if supportEnd <= supportStart {
		return edgeResult{}, false
	}
	// The value is the increase attributed to this window per second of
	// window time. The actual supporting span is exposed in evidence so
	// callers can see how much of that window rests on extrapolation.
	rate := increase / (float64(q.Step) / 1000)
	if math.IsInf(rate, 0) || math.IsNaN(rate) {
		return edgeResult{}, false
	}
	return edgeResult{
		rate: rate,
		evidence: model.SeriesRateEvidence{
			SeriesID:        seriesID,
			Mode:            "edge_context",
			SupportStart:    supportStart,
			SupportEnd:      supportEnd,
			InWindowSamples: 1,
			EdgeSamples:     edgeSamples,
			CoveredMS:       covered,
			ExtrapolatedMS:  q.Step - covered,
			Increase:        increase,
			EdgeReset:       edgeReset,
		},
	}, true
}

func nearestBefore(points []model.Sample, t int64) *model.Sample {
	// points are sorted by time within a series.
	i := sort.Search(len(points), func(i int) bool { return points[i].Time >= t })
	if i == 0 {
		return nil
	}
	return &points[i-1]
}

func nearestAfter(points []model.Sample, t int64) *model.Sample {
	i := sort.Search(len(points), func(i int) bool { return points[i].Time > t })
	if i == len(points) {
		return nil
	}
	return &points[i]
}
