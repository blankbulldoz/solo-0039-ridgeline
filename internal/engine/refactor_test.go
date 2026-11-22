package engine

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "ridge.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})
	return e
}

func testNamespace(name string, maxWrite, maxScan int) model.Namespace {
	return model.Namespace{
		Name: name, RetentionMS: 100000, LatenessMS: model.MaxTime,
		FutureMS: model.MaxTime, MaxSeries: 100, MaxWrite: maxWrite, MaxScan: maxScan,
	}
}

func labels(name, node string) model.Labels {
	return model.Labels{"__name__": name, "node": node}
}

func writePoints(t *testing.T, e *Engine, space string, now int64, points ...model.Observation) {
	t.Helper()
	if _, err := e.Write(space, model.WriteRequest{Now: now, Points: points}); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func fullRange(t *testing.T, e *Engine, space string) []model.SeriesPoints {
	t.Helper()
	out, err := e.Range(space, model.Query{Start: model.MinTime, End: model.MaxTime + 1})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	return out.Series
}

// rawNamespace captures every durable record so a failed maintenance operation
// can be compared byte for byte against the state that preceded it.
func rawNamespace(t *testing.T, e *Engine, space string) map[string]map[string][]byte {
	t.Helper()
	out := map[string]map[string][]byte{}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, space)
		if b == nil {
			return fmt.Errorf("missing namespace")
		}
		out["__sequence"] = map[string][]byte{"": append([]byte(nil), tx.Bucket(volume.Root).Get(volume.Sequence)...)}
		out["config"] = map[string][]byte{"": append([]byte(nil), b.Get(volume.Config)...)}
		for _, name := range [][]byte{volume.Head, volume.Blocks, volume.Payloads, volume.Deletes} {
			bucket := b.Bucket(name)
			entries := map[string][]byte{}
			if err := bucket.ForEach(func(k, v []byte) error {
				entries[string(k)] = append([]byte(nil), v...)
				return nil
			}); err != nil {
				return err
			}
			out[string(name)] = entries
		}
		return nil
	})
	if err != nil {
		t.Fatalf("raw namespace: %v", err)
	}
	return out
}

func assertUnchanged(t *testing.T, before, after map[string]map[string][]byte) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		for name := range before {
			if !reflect.DeepEqual(before[name], after[name]) {
				t.Fatalf("bucket %q changed after failed maintenance (before %d keys, after %d keys)",
					name, len(before[name]), len(after[name]))
			}
		}
		t.Fatalf("namespace changed after failed maintenance")
	}
}

func blockSamples(t *testing.T, e *Engine, space, series string) []model.Sample {
	t.Helper()
	var samples []model.Sample
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, space)
		return b.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
			var block model.Block
			if err := json.Unmarshal(value, &block); err != nil {
				return err
			}
			if block.SeriesID != series {
				return nil
			}
			points, err := chunk.Decode(b.Bucket(volume.Payloads).Get([]byte(block.ID)), block)
			if err != nil {
				return err
			}
			samples = append(samples, points...)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("decode blocks: %v", err)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Time < samples[j].Time })
	return samples
}

// TestCompactMergesOverlappingBlocksAndHead checks the success path: a sealed
// block, a newer head correction at the same timestamp and a deletion all
// resolve into one block per non-empty series without changing logical reads.
func TestCompactMergesOverlappingBlocksAndHead(t *testing.T) {
	e := testEngine(t)
	const space = "overlap"
	if _, err := e.Create(testNamespace(space, 100, 1000)); err != nil {
		t.Fatalf("create: %v", err)
	}
	a, b := labels("metric_a", "a"), labels("metric_b", "b")
	aID, bID := model.SeriesID(a), model.SeriesID(b)

	writePoints(t, e, space, 5000,
		model.Observation{Labels: a, Time: 100, Value: 1},
		model.Observation{Labels: a, Time: 200, Value: 2},
		model.Observation{Labels: b, Time: 100, Value: 10})
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal: %v", err)
	}
	// Corrections live in the head and overlap block timestamps.
	writePoints(t, e, space, 5000,
		model.Observation{Labels: a, Time: 100, Value: 5},
		model.Observation{Labels: a, Time: 300, Value: 3},
		model.Observation{Labels: b, Time: 200, Value: 20})
	// The deletion hides the sealed A sample at 200 and must not hide the
	// later correction at 100.
	if _, err := e.Delete(space, model.Query{Start: 200, End: 201,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "metric_a"}}}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	before := fullRange(t, e, space)
	receipt, err := e.Compact(space)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	after := fullRange(t, e, space)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("compaction changed logical samples:\nbefore %+v\nafter  %+v", before, after)
	}
	if receipt.Blocks != 2 || receipt.Points != 4 {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}

	// Corrections keep their commits: the head value (later sequence) wins and
	// the deleted sample is physically gone.
	gotA := blockSamples(t, e, space, aID)
	if len(gotA) != 2 || gotA[0].Time != 100 || gotA[0].Value != 5 || gotA[1].Time != 300 {
		t.Fatalf("series A compaction result: %+v", gotA)
	}
	gotB := blockSamples(t, e, space, bID)
	if len(gotB) != 2 || gotB[0].Time != 100 || gotB[1].Time != 200 || gotB[1].Value != 20 {
		t.Fatalf("series B compaction result: %+v", gotB)
	}
	// The sample commit sequence is preserved; only the block identity adopts
	// the maintenance sequence.
	blocks, err := e.Blocks(space)
	if err != nil {
		t.Fatalf("blocks: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected one block per non-empty series, got %+v", blocks)
	}
	for _, block := range blocks {
		want := fmt.Sprintf("%020d-%s", receipt.Sequence, block.SeriesID)
		if block.ID != want {
			t.Fatalf("replacement block %q should carry the maintenance sequence (%q)", block.ID, want)
		}
	}
	if gotA[0].Sequence != 4 || gotB[1].Sequence != 4 || gotB[0].Sequence != 2 {
		t.Fatalf("compaction must preserve per-sample commit sequences: %+v %+v", gotA, gotB)
	}
	if tombstones, err := e.Deletions(space); err != nil || len(tombstones) != 0 {
		t.Fatalf("resolved deletion should be reclaimed: %v %+v", err, tombstones)
	}
}

// TestCompactLateBudgetFailureKeepsState drives the scan past its first series
// and then over budget. No replacement block may be published, and points,
// deletions, floor and sequence must be exactly what they were.
func TestCompactLateBudgetFailureKeepsState(t *testing.T) {
	e := testEngine(t)
	const space = "latebudget"
	// Validation requires max_scan >= max_write; a 10 point budget is
	// exceeded only once the second series is decoded.
	if _, err := e.Create(testNamespace(space, 10, 10)); err != nil {
		t.Fatalf("create: %v", err)
	}
	a, b := labels("metric_a", "a"), labels("metric_b", "b")

	points := make([]model.Observation, 0, 10)
	for i := 0; i < 10; i++ {
		points = append(points, model.Observation{Labels: a, Time: int64(i), Value: float64(i)})
	}
	writePoints(t, e, space, 5000, points...)
	writePoints(t, e, space, 5000, model.Observation{Labels: b, Time: 0, Value: 1})
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := e.Delete(space, model.Query{Start: 0, End: 1000,
		Matchers: []model.Matcher{{Label: "__name__", Op: "=", Value: "metric_a"}}}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	before := rawNamespace(t, e, space)
	if _, err := e.Compact(space); err == nil {
		t.Fatal("expected compaction to exceed the scan budget")
	} else if domain, ok := err.(*model.Error); !ok || domain.Code != "capacity" {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
	assertUnchanged(t, before, rawNamespace(t, e, space))

	ns, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if ns.Floor != model.MinTime {
		t.Fatalf("failed compaction advanced floor to %d", ns.Floor)
	}
}

// TestCompactLateDecodeFailureKeepsState corrupts the payload of a later
// series so the first series builds a replacement but the second cannot decode.
// The failed operation must not leave a half replaced block set.
func TestCompactLateDecodeFailureKeepsState(t *testing.T) {
	e := testEngine(t)
	const space = "latedecode"
	if _, err := e.Create(testNamespace(space, 100, 1000)); err != nil {
		t.Fatalf("create: %v", err)
	}
	a, b := labels("metric_a", "a"), labels("metric_b", "b")
	aID, bID := model.SeriesID(a), model.SeriesID(b)
	writePoints(t, e, space, 5000,
		model.Observation{Labels: a, Time: 1, Value: 1},
		model.Observation{Labels: b, Time: 2, Value: 2})
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal: %v", err)
	}

	// The catalog is scanned in series ID order; corrupt whichever series is
	// decoded second.
	last := aID
	if bID > aID {
		last = bID
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		bucket := volume.Namespace(tx, space)
		var target model.Block
		if err := bucket.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
			var block model.Block
			if err := json.Unmarshal(value, &block); err != nil {
				return err
			}
			if block.SeriesID == last {
				target = block
			}
			return nil
		}); err != nil {
			return err
		}
		return bucket.Bucket(volume.Payloads).Put([]byte(target.ID), []byte("not a gzip payload"))
	})
	if err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}

	before := rawNamespace(t, e, space)
	if _, err := e.Compact(space); err == nil {
		t.Fatal("expected a decode failure")
	}
	assertUnchanged(t, before, rawNamespace(t, e, space))
}

// TestRetainLateFailureKeepsFloorAndState advances the retention floor but
// forces the physical rewrite over budget. The floor and every stored record
// must roll back together.
func TestRetainLateFailureKeepsFloorAndState(t *testing.T) {
	e := testEngine(t)
	const space = "latefloor"
	n := testNamespace(space, 10, 10)
	n.RetentionMS = 100
	if _, err := e.Create(n); err != nil {
		t.Fatalf("create: %v", err)
	}
	a, b := labels("metric_a", "a"), labels("metric_b", "b")
	base := int64(1_000_000)
	points := make([]model.Observation, 0, 10)
	for i := 0; i < 10; i++ {
		points = append(points, model.Observation{Labels: a, Time: base + int64(i), Value: float64(i)})
	}
	writePoints(t, e, space, 2_000_000, points...)
	writePoints(t, e, space, 2_000_000, model.Observation{Labels: b, Time: base, Value: 1})
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal: %v", err)
	}

	before := rawNamespace(t, e, space)
	// now-retention = 1_000_000, so the floor moves but every point remains
	// within the scan and the second series pushes the decode budget over.
	if _, err := e.Retain(space, base+100); err == nil {
		t.Fatal("expected retention to exceed the scan budget")
	} else if domain, ok := err.(*model.Error); !ok || domain.Code != "capacity" {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
	assertUnchanged(t, before, rawNamespace(t, e, space))

	ns, err := e.Namespace(space)
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if ns.Floor != model.MinTime {
		t.Fatalf("failed retention advanced floor to %d", ns.Floor)
	}
}

// TestRangeLimitDoesNotBoundScanWork confirms the raw limit only truncates
// returned rows: later series still decode and the budget still applies to the
// whole operation. Aggregate ignores the raw limit entirely.
func TestRangeLimitDoesNotBoundScanWork(t *testing.T) {
	e := testEngine(t)
	const space = "limits"
	// max_scan must be at least max_write; 12 decoded points exceed it, the
	// limit of 1 would not.
	if _, err := e.Create(testNamespace(space, 9, 9)); err != nil {
		t.Fatalf("create: %v", err)
	}
	a, b, c := labels("metric_a", "a"), labels("metric_b", "b"), labels("metric_c", "c")
	for i := 0; i < 4; i++ {
		writePoints(t, e, space, 5000,
			model.Observation{Labels: a, Time: int64(i), Value: float64(i)},
			model.Observation{Labels: b, Time: int64(i), Value: float64(i)},
			model.Observation{Labels: c, Time: int64(i), Value: float64(i)})
	}
	if _, err := e.Seal(space); err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 12 decoded points exceed max_scan=9 even though the limit is 1.
	if _, err := e.Range(space, model.Query{Start: model.MinTime, End: model.MaxTime + 1, Limit: 1}); err == nil {
		t.Fatal("limit must not exempt later series from the scan budget")
	} else if domain, ok := err.(*model.Error); !ok || domain.Code != "capacity" {
		t.Fatalf("expected capacity rejection, got %v", err)
	}

	if _, err := e.Create(testNamespace("roomy", 100, 100)); err != nil {
		t.Fatalf("create roomy: %v", err)
	}
	for i := 0; i < 4; i++ {
		writePoints(t, e, "roomy", 5000,
			model.Observation{Labels: a, Time: int64(i), Value: float64(i)},
			model.Observation{Labels: b, Time: int64(i), Value: float64(i)})
	}
	out, err := e.Range("roomy", model.Query{Start: model.MinTime, End: model.MaxTime + 1, Limit: 1})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(out.Series) != 1 || len(out.Series[0].Points) != 1 || !out.Truncated {
		t.Fatalf("limit should truncate returned rows: %+v", out)
	}
	if out.Stats.Decoded != 8 || out.Stats.Series != 2 {
		t.Fatalf("truncated range must still account for all source work: %+v", out.Stats)
	}
	agg, err := e.Aggregate("roomy", model.Query{
		Start: model.MinTime, End: model.MaxTime + 1, Limit: 1,
		Step: 100, Origin: 0, Function: "count",
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	total := 0
	for _, cell := range agg.Cells {
		total += cell.Count
	}
	if total != 8 {
		t.Fatalf("aggregate must ignore the raw limit, counted %d", total)
	}
}
