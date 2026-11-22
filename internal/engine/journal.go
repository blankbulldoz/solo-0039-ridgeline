package engine

import (
	"encoding/binary"
	"encoding/json"
	"sort"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// maxJournalEntries bounds the logical change journal of one namespace. Older
// entries are pruned, which advances TrackedFrom; materializations sourced
// before the remaining boundary become impossible to judge precisely and are
// reported as indeterminate instead of being silently called fresh.
var maxJournalEntries = 4096

func journalKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func journalSequence(key []byte) (uint64, bool) {
	if len(key) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
}

// beginTracking marks sequence as the source sequence from which a namespace's
// logical change journal is complete. Namespaces created by this version begin
// at creation. A namespace restored from an older volume begins at its first
// logical mutation or materialization after the upgrade, so materializations
// written earlier stay indeterminate rather than being falsely called fresh.
func beginTracking(n *model.Namespace, sequence uint64) {
	if n.JournalTracked {
		return
	}
	n.JournalTracked = true
	n.TrackedFrom = sequence
}

// mergeTimestamps converts exact written timestamps into half-open runs,
// coalescing adjacent integers so the journal stays compact while remaining
// exact about which observations were touched.
func mergeTimestamps(times []int64) []model.TimeRun {
	if len(times) == 0 {
		return nil
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	runs := make([]model.TimeRun, 0, len(times))
	for _, t := range times {
		last := len(runs) - 1
		if last >= 0 && t <= runs[last].End {
			if t+1 > runs[last].End {
				runs[last].End = t + 1
			}
			continue
		}
		runs = append(runs, model.TimeRun{Start: t, End: t + 1})
	}
	return runs
}

// recordChange appends one logical mutation and prunes the oldest entries when
// the journal exceeds its bound. Callers persist the namespace config, which
// carries TrackedFrom and JournalCount, in the same transaction.
func recordChange(b *bolt.Bucket, n *model.Namespace, change model.Change) error {
	if change.Sequence == 0 {
		return nil
	}
	beginTracking(n, change.Sequence)
	// A namespace restored from an older volume has no journal bucket yet.
	journal := b.Bucket(volume.Changes)
	if journal == nil {
		var err error
		if journal, err = b.CreateBucket(volume.Changes); err != nil {
			return err
		}
	}
	if err := volume.PutJSON(journal, journalKey(change.Sequence), change); err != nil {
		return err
	}
	n.JournalCount++
	if n.JournalCount <= maxJournalEntries {
		return nil
	}
	cursor := journal.Cursor()
	var pruned uint64
	for n.JournalCount > maxJournalEntries/2 {
		key, _ := cursor.First()
		if key == nil {
			break
		}
		smallest, ok := journalSequence(key)
		if !ok {
			break
		}
		if err := cursor.Delete(); err != nil {
			return err
		}
		n.JournalCount--
		pruned = smallest
	}
	// Every change after the highest pruned sequence is still retained, so a
	// materialization sourced at or after it still has a complete history.
	if pruned > n.TrackedFrom {
		n.TrackedFrom = pruned
	}
	return nil
}

func readChangesAfter(b *bolt.Bucket, sequence uint64) ([]model.Change, error) {
	changes := make([]model.Change, 0)
	journal := b.Bucket(volume.Changes)
	if journal == nil {
		return changes, nil
	}
	cursor := journal.Cursor()
	key, value := cursor.Seek(journalKey(sequence + 1))
	for ; key != nil; key, value = cursor.Next() {
		var change model.Change
		if err := json.Unmarshal(value, &change); err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func runsIntersect(runs []model.TimeRun, start, end int64) bool {
	for _, run := range runs {
		if run.Start < end && run.End > start {
			return true
		}
	}
	return false
}

// freshnessVerdict is the internal result of comparing a stored
// materialization with the journaled logical changes that followed it.
type freshnessVerdict struct {
	state  string
	reason string
}

func (v freshnessVerdict) stale() bool {
	return v.state != model.FreshnessFresh
}

// assessFreshness decides whether a stored rollup still describes the
// namespace. A change invalidates the materialization only when it could alter
// the same query: it must touch a series selected by the stored matchers and
// an interval intersecting the stored time range. Seal, compaction and
// read-only transitions are never journaled because they move or gate physical
// data without changing a query result.
func assessFreshness(b *bolt.Bucket, n model.Namespace, m model.Materialization) (freshnessVerdict, error) {
	if m.SourceSequence == n.LastSequence {
		return freshnessVerdict{model.FreshnessFresh,
			"namespace sequence is unchanged since materialization"}, nil
	}
	// A record written before provenance tracking, or one whose source
	// predates the retained journal, has an unaccounted history. Report that
	// honestly rather than claiming the result is current.
	if m.Provenance < model.MaterializationProvenance || !n.JournalTracked ||
		m.SourceSequence < n.TrackedFrom {
		return freshnessVerdict{model.FreshnessIndeterminate,
			"materialization predates complete change tracking; refresh to regain precise freshness"}, nil
	}
	selector, err := selectseries.Compile(m.Query.Matchers)
	if err != nil {
		return freshnessVerdict{}, err
	}
	all, err := catalog(b)
	if err != nil {
		return freshnessVerdict{}, err
	}
	labels := make(map[string]model.Labels, len(all))
	for _, series := range all {
		labels[series.ID] = series.Labels
	}
	// An unknown series cannot be proven irrelevant, so it is treated as
	// selected rather than risking a false "fresh" claim.
	selected := func(id string) bool {
		seriesLabels, ok := labels[id]
		if !ok {
			return true
		}
		return selector.Matches(seriesLabels)
	}
	changes, err := readChangesAfter(b, m.SourceSequence)
	if err != nil {
		return freshnessVerdict{}, err
	}
	for _, change := range changes {
		switch change.Kind {
		case model.ChangeWrite, model.ChangeDelete:
			for _, touch := range change.Touches {
				if selected(touch.Series) && runsIntersect(touch.Runs, m.Query.Start, m.Query.End) {
					action := "write"
					if change.Kind == model.ChangeDelete {
						action = "deletion"
					}
					return freshnessVerdict{model.FreshnessStale,
						"a later " + action + " touched a selected series inside the query interval"}, nil
				}
			}
		case model.ChangeRetention:
			if change.Floor > change.PreviousFloor &&
				runsIntersect([]model.TimeRun{{Start: change.PreviousFloor, End: change.Floor}},
					m.Query.Start, m.Query.End) {
				return freshnessVerdict{model.FreshnessStale,
					"a later retention boundary removed samples inside the query interval"}, nil
			}
		}
	}
	return freshnessVerdict{model.FreshnessFresh,
		"later changes did not affect the selected series or query interval"}, nil
}
