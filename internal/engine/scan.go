package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func readBlocks(b *bolt.Bucket) ([]model.Block, error) {
	out := make([]model.Block, 0)
	err := b.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
		var block model.Block
		if err := json.Unmarshal(value, &block); err != nil {
			return err
		}
		out = append(out, block)
		return nil
	})
	return out, err
}

// readView is the immutable slice of storage a query plans and executes
// against. Planning and execution share it so a plan and its read agree.
type readView struct {
	n          model.Namespace
	sequence   uint64
	catalog    []model.Series
	blocks     []model.Block
	tombstones []model.Tombstone
}

func loadView(b *bolt.Bucket, n model.Namespace, tx *bolt.Tx) (readView, error) {
	view := readView{n: n, sequence: volume.Current(tx)}
	var err error
	if view.catalog, err = catalog(b); err != nil {
		return view, err
	}
	if view.blocks, err = readBlocks(b); err != nil {
		return view, err
	}
	if view.tombstones, err = readTombstones(b); err != nil {
		return view, err
	}
	return view, nil
}

// observation accumulates evidence for one series. A series is only folded
// into Observed once its entire source set has been read, so a rejection can
// never project partial counters over a whole series or a whole query.
type observation struct {
	blocks      int
	blockPoints int
	headRecords int
	inRange     int
	superseded  int
	hidden      int
	visible     int
}

func (o *observation) physical() int { return o.blockPoints + o.headRecords }

func rejectedError(detail string, evidence model.ScanEvidence) error {
	return &model.Error{Code: "capacity", Detail: detail, Evidence: &evidence}
}

// scan reads one view. It always returns the evidence gathered so far: on
// success it describes every candidate; on a budget rejection it describes
// only fully read series plus the interrupted one. The replay of budget
// checks matches buildPlan's projected rejection point.
func scan(b *bolt.Bucket, view readView, q model.Query) (model.RangeResult, model.ScanEvidence, error) {
	result := model.RangeResult{Sequence: view.sequence, Floor: view.n.Floor,
		Series: []model.SeriesPoints{}}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return result, model.ScanEvidence{Sequence: view.sequence, Floor: view.n.Floor}, err
	}
	selected := selectseries.Pick(view.catalog, selector)
	result.Stats.Series = len(selected)

	plan, err := buildPlan(b, view.n, view.sequence, view.catalog, selector, q,
		view.blocks, view.tombstones, nil)
	if err != nil {
		return result, model.ScanEvidence{Sequence: view.sequence, Floor: view.n.Floor}, err
	}
	evidence := model.ScanEvidence{
		Sequence: view.sequence, Floor: view.n.Floor, Outcome: "complete",
		Budget:  model.ScanBudget{Limit: view.n.MaxScan, Within: true},
		Planned: plan.Totals,
		Observed: &model.ObservedEvidence{
			Scope: "all candidate series",
		},
		Caveats: append([]string{}, plan.Caveats...),
	}
	emitted := 0
	totals := observation{}

	reject := func(index int, s model.Series, item *observation, stage string) error {
		evidence.Outcome = "rejected"
		evidence.Complete = false
		evidence.Budget = model.ScanBudget{Limit: view.n.MaxScan,
			Used: result.Stats.Decoded, Remaining: view.n.MaxScan - result.Stats.Decoded}
		// Only series whose whole source set was read are folded into
		// Observed. The partial series is reported separately and its
		// counters are not projected over anything.
		evidence.Observed = &model.ObservedEvidence{
			Scope: "fully read series only", Series: index,
			Blocks: totals.blocks, BlockPoints: totals.blockPoints,
			HeadRecords: totals.headRecords, PhysicalPoints: totals.physical(),
			InRangePoints: totals.inRange, Superseded: totals.superseded,
			Hidden: totals.hidden, Visible: totals.visible,
		}
		plannedHere := plan.Series[index].DecodePoints
		evidence.Partial = &model.PartialEvidence{
			Series: s.ID, Stage: stage, BlocksDecoded: item.blocks,
			BlockPointsRead: item.blockPoints, HeadRecordsRead: item.headRecords,
			PhysicalRead:  item.physical(),
			PlannedDecode: plannedHere,
			UnreadDecode:  plannedHere - item.physical(),
		}
		remaining := 0
		for _, planned := range plan.Series[index+1:] {
			remaining += planned.DecodePoints
		}
		evidence.Unscanned = &model.UnscannedEvidence{
			Series: len(selected) - index - 1, PlannedDecodePoints: remaining,
		}
		// The rejection must name the same source explain projected, so an
		// operator can reconcile the pre-flight plan with the failure.
		if plan.Budget.Rejected != nil {
			evidence.Budget.RejectedSource = plan.Budget.Rejected
		}
		evidence.Reconcile = reconcile(plan.Totals, result.Stats.Decoded, false)
		evidence.Caveats = append(evidence.Caveats,
			"read stopped at the scan budget: observed counters cover only fully read series; the partial series and all remaining sources are not evidence of visible rows")
		return rejectedError("query decoded point budget exceeded", evidence)
	}

	for index, s := range selected {
		merged := map[int64]model.Sample{}
		item := observation{}

		for _, block := range view.blocks {
			if block.SeriesID != s.ID || !BlockIntersects(block, q.Start, q.End, view.n.Floor) {
				continue
			}
			if result.Stats.Decoded+block.Count > view.n.MaxScan {
				return result, evidence, reject(index, s, &item, "blocks")
			}
			payload := b.Bucket(volume.Payloads).Get([]byte(block.ID))
			if payload == nil {
				evidence.Outcome = "error"
				evidence.Complete = false
				return result, evidence, model.Capacity("query source payload is missing")
			}
			points, err := chunk.Decode(payload, block)
			if err != nil {
				evidence.Outcome = "error"
				evidence.Complete = false
				return result, evidence, err
			}
			result.Stats.Blocks++
			result.Stats.Decoded += len(points)
			item.blocks++
			item.blockPoints += len(points)
			for _, p := range points {
				if p.Time < q.Start || p.Time >= q.End || p.Time < view.n.Floor {
					continue
				}
				item.inRange++
				old, ok := merged[p.Time]
				if ok {
					item.superseded++
				}
				if !ok || p.Sequence > old.Sequence {
					merged[p.Time] = p
				}
			}
		}

		cursor := b.Bucket(volume.Head).Cursor()
		for k, v := cursor.Seek(volume.PointKey(s.ID, q.Start)); k != nil &&
			bytes.HasPrefix(k, []byte(s.ID)); k, v = cursor.Next() {
			timestamp, err := volume.PointTime(k)
			if err != nil {
				evidence.Outcome = "error"
				evidence.Complete = false
				return result, evidence, err
			}
			if timestamp >= q.End {
				break
			}
			result.Stats.Head++
			result.Stats.Decoded++
			item.headRecords++
			if result.Stats.Decoded > view.n.MaxScan {
				result.Stats.Decoded--
				result.Stats.Head--
				item.headRecords--
				return result, evidence, reject(index, s, &item, "head")
			}
			var p model.Sample
			if err := json.Unmarshal(v, &p); err != nil {
				evidence.Outcome = "error"
				evidence.Complete = false
				return result, evidence, err
			}
			if p.Time < view.n.Floor {
				continue
			}
			item.inRange++
			old, ok := merged[p.Time]
			if ok {
				item.superseded++
			}
			if !ok || p.Sequence >= old.Sequence {
				merged[p.Time] = p
			}
		}

		points := make([]model.Sample, 0, len(merged))
		for _, p := range merged {
			if hidden(s.ID, p, view.tombstones) {
				item.hidden++
				continue
			}
			points = append(points, p)
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		result.Stats.Visible += len(points)
		item.visible = len(points)
		if q.Limit > 0 && emitted+len(points) > q.Limit {
			keep := q.Limit - emitted
			points = points[:keep]
			result.Truncated = true
		}
		emitted += len(points)
		if len(points) > 0 {
			result.Series = append(result.Series, model.SeriesPoints{Series: s, Points: points})
		}

		totals.blocks += item.blocks
		totals.blockPoints += item.blockPoints
		totals.headRecords += item.headRecords
		totals.inRange += item.inRange
		totals.superseded += item.superseded
		totals.hidden += item.hidden
		totals.visible += item.visible
	}

	result.Stats.Tombstones = len(view.tombstones)
	evidence.Outcome = "complete"
	evidence.Complete = true
	evidence.Truncated = result.Truncated
	evidence.Budget = model.ScanBudget{Limit: view.n.MaxScan,
		Used: result.Stats.Decoded, Remaining: view.n.MaxScan - result.Stats.Decoded, Within: true}
	evidence.Observed = &model.ObservedEvidence{
		Scope: "all candidate series", Series: len(selected),
		Blocks: totals.blocks, BlockPoints: totals.blockPoints,
		HeadRecords: totals.headRecords, PhysicalPoints: totals.physical(),
		InRangePoints: totals.inRange, Superseded: totals.superseded,
		Hidden: totals.hidden, Visible: totals.visible,
	}
	evidence.Reconcile = reconcile(plan.Totals, result.Stats.Decoded, true)
	return result, evidence, nil
}

// reconcile compares the projected decode cost with the measured source
// work. Only a complete read can match exactly; a rejection reports how much
// of the plan was covered so the unread remainder is visible, not inferred.
func reconcile(planned model.PlanTotals, decoded int, complete bool) model.Reconciliation {
	out := model.Reconciliation{PlannedDecode: planned.DecodePoints,
		ObservedSource: decoded, Matches: complete && decoded == planned.DecodePoints}
	switch {
	case out.Matches:
		out.Verdict = "measured decode equals the plan"
	case complete:
		out.Verdict = fmt.Sprintf(
			"measured decode %d differs from a planned %d for the same read view",
			decoded, planned.DecodePoints)
	default:
		out.Verdict = fmt.Sprintf(
			"rejected after %d of a planned %d decode points; the rest was never read",
			decoded, planned.DecodePoints)
	}
	return out
}

func (e *Engine) Range(name string, q model.Query) (model.RangeResult, error) {
	var result model.RangeResult
	if err := model.ValidateQuery(q); err != nil {
		return result, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		view, err := loadView(b, n, tx)
		if err != nil {
			return err
		}
		var evidence model.ScanEvidence
		result, evidence, err = scan(b, view, q)
		result.Evidence = &evidence
		if err != nil {
			// A rejected read must not leak the rows it decoded before
			// stopping; the evidence in the error is the diagnostic.
			result = model.RangeResult{Evidence: &evidence}
		}
		return err
	})
	return result, err
}
