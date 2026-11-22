package engine

import (
	"bytes"
	"fmt"
	"sort"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// A Source is one physical contributor to a series in the requested
// interval. Block payloads are counted from block metadata; head records are
// counted from key positions. Neither requires decoding a sample.
type Source struct {
	Block    *model.Block
	HeadFrom int64
	HeadTo   int64
	Count    int
}

// SeriesEvidence is the plan's view of one candidate series. Certainty is
// deliberately conservative: VisibleUpperBound becomes a certain visible
// count only when no version overlap, deletion, retention cutoff or partial
// block can change how many logical points appear.
type SeriesEvidence struct {
	ID              string `json:"id"`
	Blocks          int    `json:"blocks"`
	BlocksContained int    `json:"blocks_contained"`
	BlocksPartial   int    `json:"blocks_partial"`
	BlockPoints     int    `json:"block_points"`
	HeadRecords     int    `json:"head_records"`
	DecodePoints    int    `json:"decode_points"`
	// Overlap means two in-range sources may hold the same series-time.
	Overlap           bool   `json:"overlap"`
	Deletions         bool   `json:"deletions"`
	RetentionCutoff   bool   `json:"retention_cutoff"`
	Certainty         string `json:"certainty"`
	VisibleUpperBound int    `json:"visible_upper_bound"`
	CertainVisible    int    `json:"certain_visible"`
}

// StraddlingBlock is a block that protrudes past the requested interval. Its
// whole Count is decoded while any part overlaps, so a narrower interval
// saves these points only by excluding the block entirely.
type StraddlingBlock struct {
	Series       string `json:"series"`
	Block        string `json:"block"`
	Start        int64  `json:"start"`
	End          int64  `json:"end"`
	Points       int    `json:"points"`
	OverlapsHead bool   `json:"overlaps_head"`
}

type SeriesCost struct {
	Series       string `json:"series"`
	DecodePoints int    `json:"decode_points"`
}

// TimeNarrowing answers one operational question: would a smaller interval
// really decode less physical data? All inputs are metadata, so the answer is
// confident for decode cost; it says nothing about visible rows.
type TimeNarrowing struct {
	// ContainedBlockPoints belong to blocks wholly inside the interval. No
	// narrower interval within this one can avoid them.
	ContainedBlockPoints int `json:"contained_block_points"`
	// StraddlingBlockPoints are decoded in full while any part overlaps.
	StraddlingBlockPoints int               `json:"straddling_block_points"`
	StraddlingBlocks      []StraddlingBlock `json:"straddling_blocks"`
	// HeadRecords are exact and scale with the interval.
	HeadRecords int    `json:"head_records"`
	Confident   bool   `json:"confident"`
	Verdict     string `json:"verdict"`
}

// SeriesNarrowing answers whether a more selective matcher would save work.
type SeriesNarrowing struct {
	Candidates     int          `json:"candidates"`
	WithSources    int          `json:"with_sources"`
	WithoutSources int          `json:"without_sources"`
	Top            []SeriesCost `json:"top"`
	Confident      bool         `json:"confident"`
	Verdict        string       `json:"verdict"`
}

type PlanNarrowing struct {
	Time   TimeNarrowing   `json:"time"`
	Series SeriesNarrowing `json:"series"`
}

// StorageAdvice says when the remaining cost is structural: no query-side
// narrowing helps because the block layout itself forces the decode. It
// names the maintenance action and its side effect honestly.
type StorageAdvice struct {
	Drive           string `json:"drive"`
	CompactResolves bool   `json:"compact_resolves"`
	Verdict         string `json:"verdict"`
}

// BudgetPlan projects the scan budget against the same ordered sources the
// read uses, without decoding anything.
type BudgetPlan struct {
	Limit     int                   `json:"limit"`
	Planned   int                   `json:"planned"`
	Within    bool                  `json:"within"`
	Rejected  *model.RejectedSource `json:"rejected_source,omitempty"`
	Confident bool                  `json:"confident"`
	Verdict   string                `json:"verdict"`
}

// AlternativePlan projects a proposed narrower request against the same read
// view. It exists so an operator can see whether a smaller range or matcher
// set would actually decode less before spending the budget again.
type AlternativePlan struct {
	Label        string           `json:"label"`
	Start        int64            `json:"start"`
	End          int64            `json:"end"`
	MatcherCount int              `json:"matcher_count"`
	Totals       model.PlanTotals `json:"totals"`
	Budget       BudgetPlan       `json:"budget"`
	Saves        int              `json:"saves"`
	Confident    bool             `json:"confident"`
	Verdict      string           `json:"verdict"`
}

// PlanEvidence is the physical plan for one namespace, query and read view.
type PlanEvidence struct {
	Sequence     uint64            `json:"sequence"`
	Floor        int64             `json:"floor"`
	ResultLimit  int               `json:"result_limit"`
	Totals       model.PlanTotals  `json:"totals"`
	Budget       BudgetPlan        `json:"budget"`
	Narrowing    PlanNarrowing     `json:"narrowing"`
	Storage      StorageAdvice     `json:"storage"`
	Series       []SeriesEvidence  `json:"series"`
	Alternatives []AlternativePlan `json:"alternatives"`
	Caveats      []string          `json:"caveats"`
}

const (
	CertaintyExact     = "exact"
	CertaintyUpperOnly = "upper_only"
)

// BlockIntersects reports whether a block can contribute samples to the
// [start, end) interval after the retention floor. Block bounds are inclusive
// start and exclusive end.
func BlockIntersects(block model.Block, start, end, floor int64) bool {
	return block.End > start && block.Start < end && block.End > floor
}

// BlockContained reports whether every stored block point lies inside the
// interval and at or after the retention floor.
func BlockContained(block model.Block, start, end, floor int64) bool {
	return block.Start >= start && block.End <= end && block.Start >= floor
}

type headFacts struct {
	inRange  int   // keys for this series inside [start, end)
	from, to int64 // first timestamp and exclusive last timestamp in range
	has      bool
}

// headKeys seeks to each candidate series' in-range head keys. Head keys sort
// by series ID then signed timestamp, so this counts exact per-series head
// records without reading a sample value or walking unrelated series.
func headKeys(head *bolt.Bucket, candidates []model.Series, start, end int64) (map[string]headFacts, error) {
	facts := map[string]headFacts{}
	if head == nil {
		return facts, nil
	}
	cursor := head.Cursor()
	for _, s := range candidates {
		fact := headFacts{}
		prefix := []byte(s.ID)
		for k, _ := cursor.Seek(volume.PointKey(s.ID, start)); k != nil &&
			bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			t, err := volume.PointTime(k)
			if err != nil {
				return nil, err
			}
			if t >= end {
				break
			}
			fact.inRange++
			if !fact.has {
				fact.from, fact.has = t, true
			}
			if t+1 > fact.to {
				fact.to = t + 1
			}
		}
		if fact.has {
			facts[s.ID] = fact
		}
	}
	return facts, nil
}

// buildPlan computes plan evidence from the catalog, block metadata and head
// key positions. It never reads payloads or sample values.
func buildPlan(b *bolt.Bucket, n model.Namespace, seq uint64,
	all []model.Series, selector selectseries.Selector, query model.Query,
	blocks []model.Block, tombstones []model.Tombstone,
	alternatives []model.Alternative) (PlanEvidence, error) {

	candidates := selectseries.Pick(all, selector)
	plan := PlanEvidence{Sequence: seq, Floor: n.Floor, ResultLimit: query.Limit,
		Series:       make([]SeriesEvidence, 0, len(candidates)),
		Alternatives: []AlternativePlan{},
		Caveats:      []string{}}
	bySeries := map[string][]model.Block{}
	for _, block := range blocks {
		bySeries[block.SeriesID] = append(bySeries[block.SeriesID], block)
	}
	deleted := map[string]bool{}
	for _, tomb := range tombstones {
		if tomb.End <= query.Start || tomb.Start >= query.End {
			continue
		}
		for _, id := range tomb.Series {
			deleted[id] = true
		}
	}
	heads, err := headKeys(b.Bucket(volume.Head), candidates, query.Start, query.End)
	if err != nil {
		return plan, err
	}
	plan.Totals.HeadRecordsNamespace = b.Bucket(volume.Head).Stats().KeyN
	anyOverlap, anyDeleted, anyPartial, anyCutoff := false, false, false, false
	seriesCosts := make([]SeriesCost, 0, len(candidates))

	for _, s := range candidates {
		item := SeriesEvidence{ID: s.ID, Certainty: CertaintyExact}
		inRange := make([]model.Block, 0, len(bySeries[s.ID]))
		for _, block := range bySeries[s.ID] {
			if !BlockIntersects(block, query.Start, query.End, n.Floor) {
				continue
			}
			inRange = append(inRange, block)
			item.Blocks++
			item.BlockPoints += block.Count
			if BlockContained(block, query.Start, query.End, n.Floor) {
				item.BlocksContained++
				plan.Totals.ContainedBlockPoints += block.Count
			} else {
				item.BlocksPartial++
				plan.Totals.StraddlingBlockPoints += block.Count
			}
		}
		head := heads[s.ID]
		item.HeadRecords = head.inRange
		item.DecodePoints = item.BlockPoints + item.HeadRecords

		// Two in-range blocks that share an instant, or a block covering a
		// head timestamp, may hold one series-time more than once. The plan
		// cannot see per-timestamp contents, so this makes the visible count
		// an upper bound, not a count.
		sort.Slice(inRange, func(i, j int) bool {
			if inRange[i].Start == inRange[j].Start {
				return inRange[i].End < inRange[j].End
			}
			return inRange[i].Start < inRange[j].Start
		})
		for i := 0; i < len(inRange); i++ {
			for j := i + 1; j < len(inRange); j++ {
				if inRange[j].Start < inRange[i].End {
					item.Overlap = true
				}
			}
		}
		if head.has {
			for _, block := range inRange {
				if block.End > head.from && block.Start < head.to {
					item.Overlap = true
					plan.Totals.HeadRecordsOverlapping += head.inRange
					break
				}
			}
		}

		item.Deletions = deleted[s.ID]
		// The cutoff only matters for this series if it actually holds data
		// below the floor. Checking that avoids claiming uncertainty that the
		// plan can rule out from key positions and block bounds.
		item.RetentionCutoff = (head.has && head.from < n.Floor)
		for _, block := range inRange {
			if block.Start < n.Floor {
				item.RetentionCutoff = true
			}
		}
		item.VisibleUpperBound = item.DecodePoints
		if item.Overlap || item.Deletions || item.RetentionCutoff || item.BlocksPartial > 0 {
			item.Certainty = CertaintyUpperOnly
		} else {
			item.CertainVisible = item.DecodePoints
		}

		anyOverlap = anyOverlap || item.Overlap
		anyDeleted = anyDeleted || item.Deletions
		anyPartial = anyPartial || item.BlocksPartial > 0
		anyCutoff = anyCutoff || item.RetentionCutoff

		plan.Totals.Series++
		plan.Totals.Blocks += item.Blocks
		plan.Totals.BlocksContained += item.BlocksContained
		plan.Totals.BlocksPartial += item.BlocksPartial
		plan.Totals.BlockPoints += item.BlockPoints
		plan.Totals.HeadRecords += item.HeadRecords
		plan.Totals.DecodePoints += item.DecodePoints
		plan.Totals.VisibleUpperBound += item.VisibleUpperBound
		plan.Totals.CertainVisiblePoints += item.CertainVisible
		if item.Certainty == CertaintyExact {
			plan.Totals.CertainVisibleSeries++
		}
		if item.DecodePoints > 0 {
			plan.Totals.SeriesWithSources++
		} else {
			plan.Totals.SeriesWithoutSources++
		}
		if item.Overlap {
			plan.Totals.SeriesWithVersionOverlap++
		}
		if item.Deletions {
			plan.Totals.SeriesWithDeletions++
		}
		if item.RetentionCutoff {
			plan.Totals.SeriesWithRetentionFloor++
		}
		if item.DecodePoints > 0 {
			seriesCosts = append(seriesCosts, SeriesCost{
				Series: s.ID, DecodePoints: item.DecodePoints})
		}
		plan.Series = append(plan.Series, item)

		if item.BlocksPartial > 0 {
			for _, block := range inRange {
				if BlockContained(block, query.Start, query.End, n.Floor) {
					continue
				}
				plan.Narrowing.Time.StraddlingBlocks = append(
					plan.Narrowing.Time.StraddlingBlocks, StraddlingBlock{
						Series: s.ID, Block: block.ID, Start: block.Start,
						End: block.End, Points: block.Count,
						OverlapsHead: head.has && block.End > head.from && block.Start < head.to,
					})
			}
		}
	}
	plan.Budget = projectBudget(plan.Totals.DecodePoints, n.MaxScan,
		candidates, blocks, heads, query, n.Floor)
	plan.Narrowing.Time.ContainedBlockPoints = plan.Totals.ContainedBlockPoints
	plan.Narrowing.Time.StraddlingBlockPoints = plan.Totals.StraddlingBlockPoints
	plan.Narrowing.Time.HeadRecords = plan.Totals.HeadRecords
	plan.Narrowing.Time.Confident = true
	switch {
	case plan.Totals.DecodePoints == 0:
		plan.Narrowing.Time.Verdict = "this interval decodes no physical data; narrowing time cannot reduce it further"
	case plan.Totals.BlockPoints == 0:
		plan.Narrowing.Time.Verdict = "cost is head records only; a proportionally narrower interval reduces it"
	case plan.Totals.StraddlingBlockPoints == 0 && plan.Totals.HeadRecords == 0:
		plan.Narrowing.Time.Verdict = "every decoded block is wholly inside the interval; narrowing time within it saves nothing"
	case plan.Totals.StraddlingBlockPoints == 0:
		plan.Narrowing.Time.Verdict = "contained blocks are fixed cost; only head records shrink with a narrower interval"
	default:
		plan.Narrowing.Time.Verdict = "straddling blocks cost their whole point count even for a thin overlap; narrowing saves them only by excluding the block entirely"
	}

	sort.Slice(seriesCosts, func(i, j int) bool {
		if seriesCosts[i].DecodePoints == seriesCosts[j].DecodePoints {
			return seriesCosts[i].Series < seriesCosts[j].Series
		}
		return seriesCosts[i].DecodePoints > seriesCosts[j].DecodePoints
	})
	if len(seriesCosts) > 10 {
		seriesCosts = seriesCosts[:10]
	}
	plan.Narrowing.Series = SeriesNarrowing{
		Candidates: plan.Totals.Series, WithSources: plan.Totals.SeriesWithSources,
		WithoutSources: plan.Totals.SeriesWithoutSources, Top: seriesCosts, Confident: true,
	}
	switch {
	case plan.Totals.Series == 0:
		plan.Narrowing.Series.Verdict = "no series matches; no device narrowing is possible"
	case plan.Totals.SeriesWithSources == 0:
		plan.Narrowing.Series.Verdict = "candidates match but hold no in-range data; narrowing devices saves nothing here"
	default:
		plan.Narrowing.Series.Verdict = "only series with a positive decode cost matter; check the top list before tightening matchers"
	}

	for _, alt := range alternatives {
		if err := model.ValidateQuery(model.Query{
			Start: alt.Start, End: alt.End, Matchers: alt.Matchers}); err != nil {
			continue
		}
		altSelector, err := selectseries.Compile(alt.Matchers)
		if err != nil {
			continue
		}
		altQuery := model.Query{Start: alt.Start, End: alt.End}
		projected, err := buildPlan(b, n, seq, all, altSelector, altQuery, blocks, tombstones, nil)
		if err != nil {
			return plan, err
		}
		entry := AlternativePlan{
			Label: alt.Label, Start: alt.Start, End: alt.End,
			MatcherCount: len(alt.Matchers), Totals: projected.Totals,
			Budget:    projected.Budget,
			Saves:     plan.Totals.DecodePoints - projected.Totals.DecodePoints,
			Confident: true,
		}
		switch {
		case entry.Saves <= 0:
			entry.Verdict = "this narrowing does not reduce decoded physical points; it would not avoid the budget"
		default:
			entry.Verdict = fmt.Sprintf(
				"this narrowing decodes %d fewer points (%d vs %d); compare the budget verdict",
				entry.Saves, projected.Totals.DecodePoints, plan.Totals.DecodePoints)
		}
		plan.Alternatives = append(plan.Alternatives, entry)
	}

	plan.Storage = storageAdvice(plan.Totals)

	if query.Limit > 0 {
		plan.Caveats = append(plan.Caveats,
			fmt.Sprintf("result limit %d truncates returned rows after decoding; it does not lower decoded points or avoid a scan-budget rejection", query.Limit))
	}
	if anyOverlap {
		plan.Caveats = append(plan.Caveats,
			"version overlap: block points or head records may share timestamps, so visible points are an upper bound")
	}
	if anyDeleted {
		plan.Caveats = append(plan.Caveats,
			"deletions touch the interval: hide/show depends on each sample commit sequence, which the plan does not decode")
	}
	if anyCutoff {
		plan.Caveats = append(plan.Caveats,
			"retention floor cuts candidate data inside the interval: decoded points include samples that will be filtered")
	}
	if anyPartial && !anyOverlap {
		plan.Caveats = append(plan.Caveats,
			"partial blocks: the interval cuts block edges, so stored counts include points outside the interval")
	}
	return plan, nil
}

// storageAdvice distinguishes cost that a narrower query can remove from
// cost that only a physical rewrite can remove. Compact merges every visible
// point of a series into one block, so it removes duplicate versions but
// also widens blocks; that trade-off is part of the verdict.
func storageAdvice(totals model.PlanTotals) StorageAdvice {
	switch {
	case totals.DecodePoints == 0:
		return StorageAdvice{Drive: "none",
			Verdict: "no physical data is decoded, so no maintenance is indicated"}
	case totals.SeriesWithVersionOverlap > 0:
		return StorageAdvice{Drive: "version_overlap", CompactResolves: true,
			Verdict: "duplicate versions inflate decode cost. Compaction resolves them into one point per series-time, but it also rewrites one wide block per series, so a later narrow read may decode the whole span unless the retention floor removes it"}
	case totals.HeadRecords > 0:
		return StorageAdvice{Drive: "head_records",
			Verdict: "cost is mutable head records. Sealing moves them into blocks without changing logical points; sealing alone does not reduce the samples a read must decode"}
	case totals.StraddlingBlockPoints > 0:
		return StorageAdvice{Drive: "block_granularity",
			Verdict: "the interval cuts block edges, so whole blocks are decoded. Narrowing time helps only once the interval excludes a block entirely; compaction widens blocks and makes that harder, not easier"}
	case totals.ContainedBlockPoints > 0:
		return StorageAdvice{Drive: "block_granularity",
			Verdict: "every decoded point sits in a block wholly inside the interval, so no smaller interval can avoid it. Only physical maintenance that rewrites block boundaries can reduce this cost"}
	default:
		return StorageAdvice{Drive: "none",
			Verdict: "no structural cost detected"}
	}
}

// projectBudget replays the scan's own ordered budget checks over plan
// sources. When the request cannot fit, Rejected names the exact source the
// read would stop at, so explain and the eventual rejection agree.
func projectBudget(planned, limit int, candidates []model.Series,
	blocks []model.Block, heads map[string]headFacts, query model.Query,
	floor int64) BudgetPlan {

	out := BudgetPlan{Limit: limit, Planned: planned,
		Within: planned <= limit, Confident: true}
	if planned <= limit {
		out.Verdict = "planned decode fits the scan budget exactly"
		return out
	}
	used := 0
	for _, s := range candidates {
		head := heads[s.ID]
		for _, source := range sourceOrder(blocks, s.ID, query, floor, head) {
			if source.Block != nil && used+source.Count > limit {
				out.Rejected = &model.RejectedSource{
					Series: s.ID, Kind: "block", Block: source.Block.ID,
					Need: source.Count, Remaining: limit - used,
				}
				out.Verdict = "planned decode exceeds the scan budget; the read would stop at this block and return no rows"
				return out
			}
			if source.Block == nil && used+source.Count > limit {
				out.Rejected = &model.RejectedSource{
					Series: s.ID, Kind: "head",
					Need: source.Count, Remaining: limit - used,
				}
				out.Verdict = "planned decode exceeds the scan budget; the read would stop partway through head records and return no rows"
				return out
			}
			used += source.Count
		}
	}
	out.Verdict = "planned decode exceeds the scan budget"
	return out
}

// sourceOrder lists one series' in-range sources in scan order: blocks in
// metadata order, then head records. Both the planner and the read path use
// this ordering so the projected and executed costs agree.
func sourceOrder(blocks []model.Block, seriesID string, query model.Query,
	floor int64, head headFacts) []Source {
	out := make([]Source, 0, len(blocks)+1)
	for i := range blocks {
		block := blocks[i]
		if block.SeriesID != seriesID ||
			!BlockIntersects(block, query.Start, query.End, floor) {
			continue
		}
		out = append(out, Source{Block: &block, Count: block.Count})
	}
	if head.inRange > 0 {
		out = append(out, Source{HeadFrom: head.from, HeadTo: head.to, Count: head.inRange})
	}
	return out
}
