// Command partition-bench measures the work cost of Ridgeline's current
// series-prefix layout under two contrasting workloads, and derives
// counterfactual work counts for a time-partitioned alternative. It never
// changes storage code or creates a new on-disk layout; the time-partition
// numbers are computed analytically from the real block inventory.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bbolt "go.etcd.io/bbolt"
)

type workloadSpec struct {
	Name       string `json:"name"`
	Series     int    `json:"series"`
	Span       int64  `json:"span_ms"`
	Interval   int64  `json:"interval_ms"`
	SealEvery  int    `json:"points_per_seal_cycle"`
	Retention  int64  `json:"retention_ms"`
	DeleteFrom int64  `json:"delete_from_ms"`
	DeleteTo   int64  `json:"delete_to_ms"`
}

func main() {
	outDir := flag.String("out", "/tmp/ridgeline-bench", "directory for benchmark databases and results")
	mode := flag.String("mode", "full", "full: two fixed workloads; sweep: cardinality/history scaling curve")
	flag.Parse()
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		fatal(err)
	}

	if *mode == "sweep" {
		report := runSweep(*outDir)
		writeReport(*outDir, "sweep.json", report)
		fmt.Println(string(mustBytes(json.MarshalIndent(report, "", "  "))))
		return
	}

	specs := []workloadSpec{
		{
			Name: "long_few", Series: 20, Span: 2_000_000, Interval: 10_000,
			SealEvery: 1, Retention: 1_200_000,
			DeleteFrom: 900_000, DeleteTo: 910_000,
		},
		{
			Name: "short_many", Series: 10_000, Span: 200_000, Interval: 20,
			SealEvery: 0, Retention: 110_000,
			DeleteFrom: 90_000, DeleteTo: 90_050,
		},
	}

	report := map[string]any{}
	for _, spec := range specs {
		fmt.Fprintf(os.Stderr, "== workload %s ==\n", spec.Name)
		report[spec.Name] = runSpec(*outDir, spec)
	}
	writeReport(*outDir, "results.json", report)
	fmt.Println(string(mustBytes(json.MarshalIndent(report, "", "  "))))
}

func writeReport(dir, file string, report any) {
	data := mustBytes(json.MarshalIndent(report, "", "  "))
	if err := os.WriteFile(filepath.Join(dir, file), data, 0o600); err != nil {
		fatal(err)
	}
}

func mustBytes(data []byte, err error) []byte {
	if err != nil {
		fatal(err)
	}
	return data
}

// sweepPoint is one point of the scaling curve: the same narrow all-series
// query against sealed databases of increasing size.
type sweepPoint struct {
	Series            int     `json:"series"`
	SealCycles        int     `json:"seal_cycles"`
	BlockRecords      int     `json:"block_records"`
	CatalogDepth      int     `json:"catalog_depth"`
	BlocksDepth       int     `json:"blocks_depth"`
	BlockChecks       int     `json:"block_checks_narrow_query"`
	NarrowElapsedS    float64 `json:"narrow_query_s"`
	NarrowVisible     int     `json:"narrow_visible"`
	RetentionElapsedS float64 `json:"retention_s"`
}

func runSweep(root string) map[string]any {
	out := map[string]any{}

	// Short workload: one block per series; grow series cardinality.
	shortSizes := []int{1000, 2500, 5000, 10000, 20000}
	shortPoints := make([]sweepPoint, 0, len(shortSizes))
	for _, n := range shortSizes {
		fmt.Fprintf(os.Stderr, "== sweep short_many series=%d ==\n", n)
		spec := workloadSpec{
			Name: "sweep_short", Series: n, Span: int64(n) * 20, Interval: 20,
			SealEvery: 0, Retention: int64(n) * 11,
		}
		shortPoints = append(shortPoints, sweepOne(root, spec, 3))
	}
	out["short_many_cardinality"] = shortPoints

	// Long workload: fixed 20 series; grow the number of seal cycles
	// (and therefore blocks per series).
	cycleCounts := []int{25, 50, 100, 200, 400}
	longPoints := make([]sweepPoint, 0, len(cycleCounts))
	for _, cycles := range cycleCounts {
		fmt.Fprintf(os.Stderr, "== sweep long_few cycles=%d ==\n", cycles)
		spec := workloadSpec{
			Name: "sweep_long", Series: 20, Span: int64(cycles) * 10_000,
			Interval: 10_000, SealEvery: 1, Retention: int64(cycles) * 6_000,
		}
		longPoints = append(longPoints, sweepOne(root, spec, 5))
	}
	out["long_few_history"] = longPoints
	return out
}

func sweepOne(root string, spec workloadSpec, iters int) sweepPoint {
	dir := filepath.Join(root, "sweep", spec.Name+fmt.Sprintf("-%d-%d", spec.Series, spec.Span))
	_ = os.RemoveAll(dir)
	must(os.MkdirAll(dir, 0o700))
	ns := model.Namespace{
		Name: "bench", RetentionMS: spec.Retention, LatenessMS: math.MaxInt32,
		FutureMS: math.MaxInt32, MaxSeries: 100000, MaxWrite: 50000, MaxScan: 2_000_000,
	}
	eng := openEngine(filepath.Join(dir, "db"))
	_, err := eng.Create(ns)
	must(err)
	ingest(eng, spec, true)

	blocks := must2(eng.Blocks("bench"))
	narrowStart, narrowEnd := narrowWindow(spec)
	q := model.Query{
		Start: narrowStart, End: narrowEnd,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: metricName(spec)}},
	}
	var best time.Duration
	var res model.RangeResult
	for i := 0; i < iters; i++ {
		start := time.Now()
		r, err := eng.Range("bench", q)
		elapsed := time.Since(start)
		must(err)
		res = r
		if i >= 1 && (best == 0 || elapsed < best) {
			best = elapsed
		}
	}
	pages := measureBoltPages(eng, "bench", res.Stats.Series)

	// Retention on a fresh copy.
	copyPath := filepath.Join(dir, "ret.db")
	must(copyFile(filepath.Join(dir, "db"), copyPath))
	retEng := openEngine(copyPath)
	now := spec.Span - spec.Retention/2
	start := time.Now()
	_, err = retEng.Retain("bench", now)
	retElapsed := time.Since(start)
	must(err)
	_ = retEng.Close()
	_ = eng.Close()

	return sweepPoint{
		Series: spec.Series, SealCycles: int(spec.Span / spec.Interval),
		BlockRecords: len(blocks), CatalogDepth: pages.Catalog.Depth,
		BlocksDepth:    pages.Blocks.Depth,
		BlockChecks:    pages.AllSeriesBlockChecks,
		NarrowElapsedS: best.Seconds(), NarrowVisible: res.Stats.Visible,
		RetentionElapsedS: retElapsed.Seconds(),
	}
}

func runSpec(root string, spec workloadSpec) map[string]any {
	dir := filepath.Join(root, spec.Name)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fatal(err)
	}

	ns := model.Namespace{
		Name: "bench", RetentionMS: spec.Retention, LatenessMS: math.MaxInt32,
		FutureMS: math.MaxInt32, MaxSeries: 100000, MaxWrite: 50000, MaxScan: 2_000_000,
	}

	// ---- Phase 1: head-state database (no seals) -----------------------
	headPath := filepath.Join(dir, "head.db")
	headEng := openEngine(headPath)
	_, err := headEng.Create(ns)
	must(err)
	ingest(headEng, spec, false)
	headMeas := measurePhysical(headEng, "bench")

	// ---- Phase 2: sealed database --------------------------------------
	sealPath := filepath.Join(dir, "sealed.db")
	sealEng := openEngine(sealPath)
	_, err = sealEng.Create(ns)
	must(err)
	loadStart := time.Now()
	ingest(sealEng, spec, true)
	loadElapsed := time.Since(loadStart)
	sealMeas := measurePhysical(sealEng, "bench")

	out := map[string]any{
		"spec":            spec,
		"load_and_seal_s": loadElapsed.Seconds(),
		"head_state":      headMeas,
		"sealed_state":    sealMeas,
	}

	// ---- Narrow time queries -------------------------------------------
	narrowStart, narrowEnd := narrowWindow(spec)
	allLabels := model.Matcher{Label: "__name__", Op: "=", Value: metricName(spec)}

	headNarrow := timedRange(headEng, "bench", model.Query{
		Start: narrowStart, End: narrowEnd, Matchers: []model.Matcher{allLabels},
	})
	sealNarrowOne := timedRange(sealEng, "bench", model.Query{
		Start: narrowStart, End: narrowEnd,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: metricName(spec)},
			{Label: "sid", Op: "=", Value: "0"}},
	})
	// A point-selector query that actually intersects the narrow window:
	// long series sid 0 has a point every cycle; for short series the window
	// contains exactly one series (sid = span/2 / interval = series/2).
	hitSid := 0
	if spec.SealEvery == 0 {
		hitSid = spec.Series / 2
	}
	sealNarrowHit := timedRange(sealEng, "bench", model.Query{
		Start: narrowStart, End: narrowEnd,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: metricName(spec)},
			{Label: "sid", Op: "=", Value: fmt.Sprintf("%d", hitSid)}},
	})
	sealNarrowAll := timedRange(sealEng, "bench", model.Query{
		Start: narrowStart, End: narrowEnd, Matchers: []model.Matcher{allLabels},
	})
	// Wide query for contrast: same selector, full span.
	sealWideAll := timedRange(sealEng, "bench", model.Query{
		Start: -1, End: spec.Span + 1, Matchers: []model.Matcher{allLabels},
	})
	out["query_head_narrow_all_series"] = headNarrow
	out["query_sealed_narrow_one_series_miss"] = sealNarrowOne
	out["query_sealed_narrow_one_series_hit"] = sealNarrowHit
	out["query_sealed_narrow_all_series"] = sealNarrowAll
	out["query_sealed_wide_all_series"] = sealWideAll

	// ---- Catalog access work -------------------------------------------
	out["catalog_discover_all"] = timedDiscover(sealEng, "bench",
		[]model.Matcher{allLabels})
	out["catalog_discover_one"] = timedDiscover(sealEng, "bench",
		[]model.Matcher{{Label: "sid", Op: "=", Value: "0"}})
	out["blocks_inventory"] = timedBlocks(sealEng, "bench")
	out["bbolt_page_stats"] = measureBoltPages(sealEng, "bench", sealMeas.DistinctSeriesWithBlock)

	// ---- Counterfactual time-partition analysis ------------------------
	out["time_partition_counterfactual"] = counterfactual(sealEng, "bench", spec,
		narrowStart, narrowEnd)

	// ---- Deletion: fresh sealed copy so later tests stay independent ---
	delPath := filepath.Join(dir, "delete.db")
	must(copyFile(sealPath, delPath))
	delEng := openEngine(delPath)
	delStart := time.Now()
	tomb, err := delEng.Delete("bench", model.Query{
		Start: spec.DeleteFrom, End: spec.DeleteTo,
		Matchers: []model.Matcher{allLabels},
	})
	delElapsed := time.Since(delStart)
	if err != nil {
		fatal(err)
	}
	rangeAfterDelete := timedRange(delEng, "bench", model.Query{
		Start: spec.DeleteFrom - spec.Interval, End: spec.DeleteTo + spec.Interval,
		Matchers: []model.Matcher{allLabels},
	})
	// Wide scan before/after the tombstone: isolates the hidden() linear
	// tombstone-series scan paid by every surviving point.
	wideQ := model.Query{Start: -1, End: spec.Span + 1, Matchers: []model.Matcher{allLabels}}
	// The fresh copy already carries the tombstone; compare against the
	// untouched sealed engine instead of mutating this copy.
	wideBeforeDelete := timedRange(sealEng, "bench", wideQ)
	wideAfterDelete := timedRange(delEng, "bench", wideQ)
	out["delete_narrow"] = map[string]any{
		"elapsed_s":           delElapsed.Seconds(),
		"tombstone_series":    len(tomb.Series),
		"range_after_delete":  rangeAfterDelete,
		"tombstones_after":    len(must2(delEng.Deletions("bench"))),
		"wide_query_before_s": wideBeforeDelete.ElapsedS,
		"wide_query_after_s":  wideAfterDelete.ElapsedS,
		"wide_tombstone_tax_factor": func() float64 {
			if wideBeforeDelete.ElapsedS == 0 {
				return 0
			}
			return wideAfterDelete.ElapsedS / wideBeforeDelete.ElapsedS
		}(),
	}

	// Broad deletion (half the span) on another sealed copy: probes the
	// worst case of the per-point hidden() scan over a large tombstone list.
	broadPath := filepath.Join(dir, "broad_delete.db")
	must(copyFile(sealPath, broadPath))
	broadEng := openEngine(broadPath)
	broadFrom := spec.Span / 4
	broadTo := spec.Span * 3 / 4
	broadTomb, err := broadEng.Delete("bench", model.Query{
		Start: broadFrom, End: broadTo, Matchers: []model.Matcher{allLabels},
	})
	must(err)
	wideQ2 := model.Query{Start: -1, End: spec.Span + 1, Matchers: []model.Matcher{allLabels}}
	wideBeforeBroad := timedRange(sealEng, "bench", wideQ2)
	wideAfterBroad := timedRange(broadEng, "bench", wideQ2)
	out["delete_broad"] = map[string]any{
		"window_fraction":      0.5,
		"tombstone_series":     len(broadTomb.Series),
		"wide_query_before_s":  wideBeforeBroad.ElapsedS,
		"wide_query_after_s":   wideAfterBroad.ElapsedS,
		"tombstone_tax_factor": wideAfterBroad.ElapsedS / wideBeforeBroad.ElapsedS,
		"visible_after":        wideAfterBroad.VisiblePoints,
	}
	_ = broadEng.Close()

	// ---- Retention advance: another fresh sealed copy ------------------
	retPath := filepath.Join(dir, "retention.db")
	must(copyFile(sealPath, retPath))
	retEng := openEngine(retPath)
	now := spec.Span - spec.Retention/2
	retStart := time.Now()
	retReceipt, err := retEng.Retain("bench", now)
	retElapsed := time.Since(retStart)
	if err != nil {
		fatal(err)
	}
	out["retention_advance"] = map[string]any{
		"elapsed_s":          retElapsed.Seconds(),
		"new_floor":          retReceipt.Floor,
		"blocks_rewritten":   retReceipt.Blocks,
		"points_rewritten":   retReceipt.Points,
		"remaining_blocks":   len(must2(retEng.Blocks("bench"))),
		"tombstones_cleared": len(must2(retEng.Deletions("bench"))),
	}

	// Retention ladder: step the floor across the span on one engine copy to
	// show how rewrite work behaves at shallow vs deep floors. Each step is a
	// full compact, so costs are measured against the surviving block set.
	ladderPath := filepath.Join(dir, "ladder.db")
	must(copyFile(sealPath, ladderPath))
	ladderEng := openEngine(ladderPath)
	fractions := []float64{0.25, 0.5, 0.75, 0.95}
	ladder := make([]map[string]any, 0, len(fractions))
	for _, f := range fractions {
		obs := int64(f * float64(spec.Span))
		start := time.Now()
		rec, err := ladderEng.Retain("bench", obs)
		elapsed := time.Since(start)
		if err != nil {
			fatal(err)
		}
		ladder = append(ladder, map[string]any{
			"floor_fraction": f, "floor": rec.Floor, "elapsed_s": elapsed.Seconds(),
			"blocks_rewritten": rec.Blocks, "points_rewritten": rec.Points,
		})
	}
	_ = ladderEng.Close()
	out["retention_ladder"] = ladder
	out["db_file_bytes_sealed"] = fileSize(sealPath)
	out["db_file_bytes_retention"] = fileSize(retPath)

	for _, e := range []*engine.Engine{headEng, sealEng, delEng, retEng} {
		_ = e.Close()
	}
	return out
}

// ---------------- workload generation ----------------

func metricName(spec workloadSpec) string {
	if spec.SealEvery > 0 {
		return "long_gauge"
	}
	return "short_gauge"
}

func labelsFor(i int) model.Labels {
	return model.Labels{"__name__": "", "sid": fmt.Sprintf("%d", i)}
}

func ingest(e *engine.Engine, spec workloadSpec, withSeals bool) {
	if spec.SealEvery > 0 {
		// Long workload: every series emits one point per cycle; seal each cycle.
		cycles := int(spec.Span / spec.Interval)
		for c := 0; c < cycles; c++ {
			pts := make([]model.Observation, 0, spec.Series)
			ts := int64(c) * spec.Interval
			for s := 0; s < spec.Series; s++ {
				l := labelsFor(s)
				l["__name__"] = metricName(spec)
				pts = append(pts, model.Observation{Labels: l, Time: ts, Value: float64(c + s)})
			}
			rec := must2(e.Write("bench", model.WriteRequest{Now: ts, Points: pts}))
			if rec.Accepted != len(pts) {
				fatal(fmt.Errorf("short write: %d", rec.Accepted))
			}
			if withSeals {
				if _, err := e.Seal("bench"); err != nil {
					fatal(err)
				}
			}
		}
		return
	}
	// Short workload: each series has one point, ingested in batches.
	batch := 5000
	for start := 0; start < spec.Series; start += batch {
		end := start + batch
		if end > spec.Series {
			end = spec.Series
		}
		pts := make([]model.Observation, 0, end-start)
		for i := start; i < end; i++ {
			l := labelsFor(i)
			l["__name__"] = metricName(spec)
			pts = append(pts, model.Observation{
				Labels: l, Time: int64(i) * spec.Interval, Value: float64(i),
			})
		}
		now := int64(end) * spec.Interval
		rec := must2(e.Write("bench", model.WriteRequest{Now: now, Points: pts}))
		if rec.Accepted != len(pts) {
			fatal(fmt.Errorf("short write: %d", rec.Accepted))
		}
	}
	if withSeals {
		if _, err := e.Seal("bench"); err != nil {
			fatal(err)
		}
	}
}

func narrowWindow(spec workloadSpec) (int64, int64) {
	// A window covering one seal cycle / one point spacing, near the middle.
	center := spec.Span / 2
	return center, center + spec.Interval
}

// ---------------- measurements ----------------

type physical struct {
	CatalogEntries          int         `json:"catalog_entries"`
	HeadRecords             int         `json:"head_records"`
	BlockRecords            int         `json:"block_records"`
	PayloadBytes            int64       `json:"payload_bytes"`
	BlockMetaBytes          int64       `json:"block_meta_bytes"`
	BlocksPerSeriesMin      int         `json:"blocks_per_series_min"`
	BlocksPerSeriesMax      int         `json:"blocks_per_series_max"`
	DistinctSeriesWithBlock int         `json:"distinct_series_with_blocks"`
	TotalBlockPoints        int         `json:"total_block_points"`
	BlockPointCountHist     map[int]int `json:"block_point_count_histogram"`
}

func measurePhysical(e *engine.Engine, name string) physical {
	var p physical
	err := e.DB.Raw.View(func(tx *bbolt.Tx) error {
		b := volume.Namespace(tx, name)
		p.CatalogEntries = b.Bucket(volume.Catalog).Stats().KeyN
		p.HeadRecords = b.Bucket(volume.Head).Stats().KeyN
		blocksB := b.Bucket(volume.Blocks)
		payloadsB := b.Bucket(volume.Payloads)
		p.BlockRecords = blocksB.Stats().KeyN
		p.BlockPointCountHist = map[int]int{}
		return blocksB.ForEach(func(k, v []byte) error {
			p.BlockMetaBytes += int64(len(k) + len(v))
			var blk model.Block
			if err := json.Unmarshal(v, &blk); err != nil {
				return err
			}
			p.TotalBlockPoints += blk.Count
			p.BlockPointCountHist[blk.Count]++
			payload := payloadsB.Get([]byte(blk.ID))
			p.PayloadBytes += int64(len(payload))
			return nil
		})
	})
	if err != nil {
		fatal(err)
	}
	blocks := must2(e.Blocks(name))
	perSeries := map[string]int{}
	minN, maxN := math.MaxInt, 0
	for _, blk := range blocks {
		perSeries[blk.SeriesID]++
	}
	p.DistinctSeriesWithBlock = len(perSeries)
	for _, n := range perSeries {
		if n < minN {
			minN = n
		}
		if n > maxN {
			maxN = n
		}
	}
	if len(perSeries) == 0 {
		minN = 0
	}
	p.BlocksPerSeriesMin, p.BlocksPerSeriesMax = minN, maxN
	return p
}

type timed struct {
	ElapsedS      float64         `json:"elapsed_s"`
	Stats         model.ScanStats `json:"stats"`
	ResultSeries  int             `json:"result_series"`
	VisiblePoints int             `json:"visible_points"`
	Truncated     bool            `json:"truncated"`
}

func timedRange(e *engine.Engine, name string, q model.Query) timed {
	// Two warm iterations, then take the minimum of five to reduce noise.
	var best time.Duration
	var res model.RangeResult
	for i := 0; i < 7; i++ {
		start := time.Now()
		r, err := e.Range(name, q)
		elapsed := time.Since(start)
		if err != nil {
			fatal(err)
		}
		res = r
		if i >= 2 && (best == 0 || elapsed < best) {
			best = elapsed
		}
	}
	return timed{
		ElapsedS: best.Seconds(), Stats: res.Stats, ResultSeries: len(res.Series),
		VisiblePoints: res.Stats.Visible, Truncated: res.Truncated,
	}
}

func timedDiscover(e *engine.Engine, name string, matchers []model.Matcher) map[string]any {
	var best time.Duration
	var n int
	for i := 0; i < 7; i++ {
		start := time.Now()
		got, err := e.Discover(name, matchers)
		elapsed := time.Since(start)
		if err != nil {
			fatal(err)
		}
		n = len(got)
		if i >= 2 && (best == 0 || elapsed < best) {
			best = elapsed
		}
	}
	return map[string]any{"elapsed_s": best.Seconds(), "matched_series": n}
}

func timedBlocks(e *engine.Engine, name string) map[string]any {
	var best time.Duration
	var n int
	var metaBytes int64
	for i := 0; i < 7; i++ {
		start := time.Now()
		blocks, err := e.Blocks(name)
		elapsed := time.Since(start)
		if err != nil {
			fatal(err)
		}
		n, metaBytes = len(blocks), 0
		for _, b := range blocks {
			raw, _ := json.Marshal(b)
			metaBytes += int64(len(raw))
		}
		if i >= 2 && (best == 0 || elapsed < best) {
			best = elapsed
		}
	}
	return map[string]any{"elapsed_s": best.Seconds(), "blocks": n, "block_meta_json_bytes": metaBytes}
}

// ---------------- bbolt directory-tree access ----------------

type bucketTree struct {
	KeyN        int     `json:"key_n"`
	Depth       int     `json:"depth"`
	LeafPages   int     `json:"leaf_pages"`
	BranchPages int     `json:"branch_pages"`
	LeafInuse   int     `json:"leaf_inuse_bytes"`
	ForEachS    float64 `json:"for_each_s"`
}

type boltPages struct {
	Catalog           bucketTree `json:"catalog"`
	Blocks            bucketTree `json:"blocks"`
	Head              bucketTree `json:"head"`
	HeadSeekOneSeries struct {
		ElapsedS   float64 `json:"elapsed_s"`
		KeysWalked int     `json:"keys_walked"`
	} `json:"head_seek_one_series"`
	// The exact number of block records scan() touches for a one-series
	// narrow query: it iterates the full block inventory once per selected
	// series, even when only one block overlaps.
	SingleSeriesBlockChecks int `json:"single_series_block_checks"`
	AllSeriesBlockChecks    int `json:"all_series_block_checks"`
}

func treeStats(b *bbolt.Bucket, bucketName []byte) bucketTree {
	st := b.Bucket(bucketName).Stats()
	start := time.Now()
	_ = b.Bucket(bucketName).ForEach(func(_, _ []byte) error { return nil })
	return bucketTree{
		KeyN: st.KeyN, Depth: st.Depth, LeafPages: st.LeafPageN,
		BranchPages: st.BranchPageN, LeafInuse: st.LeafInuse,
		ForEachS: time.Since(start).Seconds(),
	}
}

// measureBoltPages reports the on-disk B+tree shape of each directory and the
// exact iteration work of the engine's access patterns. Read transactions use
// mmap, so "work" is expressed as tree depth/leaf pages and records walked,
// not page allocations.
func measureBoltPages(e *engine.Engine, name string, selectedSeries int) boltPages {
	var out boltPages
	err := e.DB.Raw.View(func(tx *bbolt.Tx) error {
		b := volume.Namespace(tx, name)
		out.Catalog = treeStats(b, volume.Catalog)
		out.Blocks = treeStats(b, volume.Blocks)
		out.Head = treeStats(b, volume.Head)

		var firstID string
		if err := b.Bucket(volume.Catalog).ForEach(func(k, _ []byte) error {
			firstID = string(k)
			return fmt.Errorf("stop")
		}); err != nil && err.Error() != "stop" {
			return err
		}
		start := time.Now()
		walked := 0
		cur := b.Bucket(volume.Head).Cursor()
		for k, _ := cur.Seek(volume.PointKey(firstID, model.MinTime)); k != nil; k, _ = cur.Next() {
			if !bytes.HasPrefix(k, []byte(firstID)) {
				break
			}
			walked++
		}
		out.HeadSeekOneSeries.ElapsedS = time.Since(start).Seconds()
		out.HeadSeekOneSeries.KeysWalked = walked

		blockN := b.Bucket(volume.Blocks).Stats().KeyN
		out.SingleSeriesBlockChecks = blockN
		out.AllSeriesBlockChecks = blockN * selectedSeries
		return nil
	})
	if err != nil {
		fatal(err)
	}
	return out
}

// ---------------- counterfactual analysis ----------------

type cf struct {
	PartitionWidthMS            int64 `json:"partition_width_ms"`
	PartitionsTouchedNarrow     int   `json:"partitions_touched_narrow"`
	TotalPartitions             int   `json:"total_partitions"`
	CurrentBlocksScanned        int   `json:"current_blocks_scanned"`
	CurrentBlocksOverlapping    int   `json:"current_blocks_overlapping"`
	CurrentBlockChecksNarrow    int   `json:"current_block_checks_narrow"`
	TimeLayoutBlocksNarrow      int   `json:"time_layout_blocks_narrow"`
	TimeLayoutBlockChecks       int   `json:"time_layout_block_checks"`
	CurrentBlocksDecodedNarrow  int   `json:"current_blocks_decoded_narrow"`
	RetentionFloor              int64 `json:"retention_floor"`
	CurrentBlocksDroppedByFloor int   `json:"current_blocks_dropped_by_floor"`
	TimeLayoutPartitionsDropped int   `json:"time_layout_partitions_dropped"`
	TimeLayoutBlocksDropped     int   `json:"time_layout_blocks_dropped"`
	PointsInDroppedBlocks       int   `json:"points_in_dropped_blocks"`
	PointsSurviving             int   `json:"points_surviving"`
	BlocksFullyBeforeFloor      int   `json:"blocks_fully_before_floor"`
	BlocksStraddlingFloor       int   `json:"blocks_straddling_floor"`
}

func counterfactual(e *engine.Engine, name string, spec workloadSpec, narrowStart, narrowEnd int64) cf {
	blocks := must2(e.Blocks(name))
	// Partition width: one seal cycle for the long workload, and comparable
	// slices (50 point-spacings) for the short workload.
	width := spec.Interval
	if spec.SealEvery == 0 {
		width = spec.Interval * 50
	}
	out := cf{PartitionWidthMS: width}
	partitions := map[int64]bool{}
	blockPartitions := map[int64]int{}
	for _, b := range blocks {
		p0 := b.Start / width
		p1 := (b.End - 1) / width
		for p := p0; p <= p1; p++ {
			partitions[p] = true
			blockPartitions[p]++
		}
	}
	out.TotalPartitions = len(partitions)

	narrowParts := map[int64]bool{}
	p0 := narrowStart / width
	p1 := (narrowEnd - 1) / width
	for p := p0; p <= p1; p++ {
		narrowParts[p] = true
	}
	out.PartitionsTouchedNarrow = len(narrowParts)
	for _, b := range blocks {
		out.CurrentBlockChecksNarrow++
		if b.End > narrowStart && b.Start < narrowEnd {
			out.CurrentBlocksOverlapping++
			out.CurrentBlocksDecodedNarrow++
		}
	}
	out.CurrentBlocksScanned = len(blocks)
	for p := range narrowParts {
		out.TimeLayoutBlockChecks += blockPartitions[p]
	}
	out.TimeLayoutBlocksNarrow = out.TimeLayoutBlockChecks

	now := spec.Span - spec.Retention/2
	floor := now - spec.Retention
	if floor < model.MinTime {
		floor = model.MinTime
	}
	out.RetentionFloor = floor
	droppedParts := map[int64]bool{}
	for _, b := range blocks {
		if b.End <= floor {
			out.BlocksFullyBeforeFloor++
			out.PointsInDroppedBlocks += b.Count
			out.TimeLayoutBlocksDropped++
			pp0 := b.Start / width
			pp1 := (b.End - 1) / width
			for p := pp0; p <= pp1; p++ {
				droppedParts[p] = true
			}
		} else {
			out.PointsSurviving += b.Count
			if b.Start < floor {
				out.BlocksStraddlingFloor++
			}
		}
	}
	out.CurrentBlocksDroppedByFloor = out.BlocksFullyBeforeFloor
	out.TimeLayoutPartitionsDropped = len(droppedParts)
	return out
}

// ---------------- helpers ----------------

func openEngine(path string) *engine.Engine {
	e, err := engine.Open(path)
	if err != nil {
		fatal(err)
	}
	return e
}

func must(err error) {
	if err != nil {
		fatal(err)
	}
}

func must2[T any](v T, err error) T {
	if err != nil {
		fatal(err)
	}
	return v
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bench error:", err)
	os.Exit(1)
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		fatal(err)
	}
	return info.Size()
}
