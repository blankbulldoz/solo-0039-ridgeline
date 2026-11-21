package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// Preview states are derived from the stored snapshot and current visibility.
const (
	PreviewPending     = "pending"
	PreviewInvalidated = "invalidated"
	PreviewCancelled   = "cancelled"
	PreviewConfirmed   = "confirmed"
)

func previewFingerprint(snap model.PreviewSnapshot) string {
	matchers := append([]model.Matcher(nil), snap.Matchers...)
	sort.Slice(matchers, func(i, j int) bool {
		a, b := matchers[i], matchers[j]
		if a.Label != b.Label {
			return a.Label < b.Label
		}
		if a.Op != b.Op {
			return a.Op < b.Op
		}
		return a.Value < b.Value
	})
	payload := struct {
		Start    int64                           `json:"start"`
		End      int64                           `json:"end"`
		Matchers []model.Matcher                 `json:"matchers"`
		Points   map[string][]model.PreviewPoint `json:"points"`
	}{snap.Start, snap.End, matchers, snap.Points}
	data, _ := json.Marshal(payload)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func previewQuery(snap model.PreviewSnapshot) model.Query {
	return model.Query{Start: snap.Start, End: snap.End, Matchers: snap.Matchers}
}

func snapshotOf(id string, n model.Namespace, q model.Query,
	rows []visibleSeries, tombstones []string) model.PreviewSnapshot {
	snap := model.PreviewSnapshot{
		ID: id, Start: q.Start, End: q.End,
		Matchers: append([]model.Matcher(nil), q.Matchers...),
		Sequence: n.LastSequence, Floor: n.Floor,
		Series:     make([]model.PreviewSeries, 0, len(rows)),
		Points:     make(map[string][]model.PreviewPoint, len(rows)),
		Tombstones: tombstones,
	}
	for _, row := range rows {
		entry := model.PreviewSeries{Series: row.Series, Visible: len(row.Points)}
		refs := make([]model.PreviewPoint, 0, len(row.Points))
		for _, p := range row.Points {
			refs = append(refs, model.PreviewPoint{Time: p.Time, Sequence: p.Sequence})
		}
		if len(row.Points) > 0 {
			entry.Coverage = &model.Coverage{
				Start: row.Points[0].Time,
				End:   row.Points[len(row.Points)-1].Time + 1,
			}
		}
		snap.Series = append(snap.Series, entry)
		snap.Points[row.Series.ID] = refs
	}
	return snap
}

func previewView(snap model.PreviewSnapshot, drift *model.PreviewDrift) model.PreviewView {
	view := model.PreviewView{
		ID: snap.ID, Start: snap.Start, End: snap.End,
		Matchers: snap.Matchers, Fingerprint: snap.Fingerprint,
		NamespaceSequence: snap.Sequence, Floor: snap.Floor,
		Series: snap.Series, TombstoneID: snap.TombstoneID,
		ConfirmSequence: snap.ConfirmSeq, Drift: drift,
	}
	for _, s := range snap.Series {
		view.Visible += s.Visible
	}
	switch {
	case snap.Cancelled:
		view.State = PreviewCancelled
	case snap.TombstoneID != "":
		view.State = PreviewConfirmed
	case drift != nil && drift.Invalidated:
		view.State = PreviewInvalidated
	default:
		view.State = PreviewPending
	}
	return view
}

func loadPreview(b *bolt.Bucket, id string) (model.PreviewSnapshot, error) {
	var snap model.PreviewSnapshot
	bucket := b.Bucket(volume.Previews)
	if bucket == nil || bucket.Get([]byte(id)) == nil {
		return snap, model.Missing("deletion preview does not exist")
	}
	err := volume.GetJSON(bucket, []byte(id), &snap)
	return snap, err
}

func putPreview(b *bolt.Bucket, snap model.PreviewSnapshot) error {
	return volume.PutJSON(b.Bucket(volume.Previews), []byte(snap.ID), snap)
}

func tombstoneIDs(tombstones []model.Tombstone) []string {
	ids := make([]string, 0, len(tombstones))
	for _, t := range tombstones {
		ids = append(ids, t.ID)
	}
	return ids
}

func sameStringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

// previewDrift compares the frozen preview selection with current visibility.
// Changes that alter what confirmation would hide invalidate the preview;
// changes outside the previewed selection are reported as notices only.
func previewDrift(b *bolt.Bucket, n model.Namespace, snap model.PreviewSnapshot) (model.PreviewDrift, error) {
	drift := model.PreviewDrift{
		Reasons: []string{}, Notices: []string{},
		Series: []model.DriftSeries{}, NewSeries: []model.PreviewSeries{},
		NewDeletions:      []model.NewDeletion{},
		NamespaceSequence: n.LastSequence,
		Floor:             n.Floor,
	}
	invalidate := func(reason string) {
		drift.Invalidated = true
		for _, existing := range drift.Reasons {
			if existing == reason {
				return
			}
		}
		drift.Reasons = append(drift.Reasons, reason)
	}
	if n.Floor > snap.Floor {
		// The floor itself is not a visibility change; points it removes from
		// the preview are reported below as removed points.
		drift.Notices = append(drift.Notices, "retention floor advanced since the preview")
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return drift, err
	}
	known := sameStringSet(snap.Tombstones)
	seriesInPreview := sameStringSet(func() []string {
		ids := make([]string, 0, len(snap.Series))
		for _, s := range snap.Series {
			ids = append(ids, s.Series.ID)
		}
		return ids
	}())
	for _, tomb := range tombstones {
		if known[tomb.ID] {
			continue
		}
		affects := tomb.End > snap.Start && tomb.Start < snap.End
		if affects {
			intersects := false
			for _, id := range tomb.Series {
				if seriesInPreview[id] {
					intersects = true
					break
				}
			}
			affects = intersects
		}
		drift.NewDeletions = append(drift.NewDeletions, model.NewDeletion{
			ID: tomb.ID, Sequence: tomb.Sequence, AffectsPreview: affects,
		})
		if affects {
			invalidate("overlapping_deletion")
		} else {
			drift.Notices = append(drift.Notices,
				fmt.Sprintf("deletion %s does not affect the previewed selection", tomb.ID))
		}
	}
	rows, _, err := visiblePoints(b, n, previewQuery(snap))
	if err != nil {
		return drift, err
	}
	current := make(map[string][]model.Sample, len(rows))
	for _, row := range rows {
		current[row.Series.ID] = row.Points
		if _, ok := snap.Points[row.Series.ID]; ok {
			continue
		}
		entry := model.PreviewSeries{Series: row.Series, Visible: len(row.Points)}
		if len(row.Points) > 0 {
			entry.Coverage = &model.Coverage{
				Start: row.Points[0].Time,
				End:   row.Points[len(row.Points)-1].Time + 1,
			}
		}
		drift.NewSeries = append(drift.NewSeries, entry)
		drift.Notices = append(drift.Notices,
			fmt.Sprintf("series %s matches now but was not in the preview", row.Series.ID))
	}
	for _, entry := range snap.Series {
		id := entry.Series.ID
		before := make(map[int64]uint64, len(snap.Points[id]))
		for _, p := range snap.Points[id] {
			before[p.Time] = p.Sequence
		}
		after := make(map[int64]uint64, len(current[id]))
		for _, p := range current[id] {
			after[p.Time] = p.Sequence
		}
		change := model.DriftSeries{Series: entry.Series}
		for t, seq := range after {
			old, ok := before[t]
			switch {
			case !ok:
				change.Added++
				change.AddedTimes = append(change.AddedTimes, t)
			case old != seq:
				change.Replaced++
				change.ReplacedTimes = append(change.ReplacedTimes, t)
			}
		}
		for t := range before {
			if _, ok := after[t]; !ok {
				change.Removed++
				change.RemovedTimes = append(change.RemovedTimes, t)
			}
		}
		if change.Added > 0 {
			invalidate("visible_points_added")
		}
		if change.Removed > 0 {
			invalidate("visible_points_removed")
		}
		if change.Replaced > 0 {
			invalidate("visible_points_replaced")
		}
		if change.Added+change.Removed+change.Replaced > 0 {
			sort.Slice(change.AddedTimes, func(i, j int) bool { return change.AddedTimes[i] < change.AddedTimes[j] })
			sort.Slice(change.RemovedTimes, func(i, j int) bool { return change.RemovedTimes[i] < change.RemovedTimes[j] })
			sort.Slice(change.ReplacedTimes, func(i, j int) bool { return change.ReplacedTimes[i] < change.ReplacedTimes[j] })
			drift.Series = append(drift.Series, change)
		}
	}
	sort.Slice(drift.Series, func(i, j int) bool { return drift.Series[i].Series.ID < drift.Series[j].Series.ID })
	sort.Slice(drift.NewSeries, func(i, j int) bool { return drift.NewSeries[i].Series.ID < drift.NewSeries[j].Series.ID })
	return drift, nil
}

// PreviewDelete freezes the visible samples a deletion would hide. It never
// changes namespace data or the namespace sequence; only a private preview
// record is published with its own global sequence.
func (e *Engine) PreviewDelete(name string, q model.Query) (model.PreviewView, error) {
	var view model.PreviewView
	if err := model.ValidateQuery(q); err != nil {
		return view, err
	}
	q.Limit = 0
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if b.Bucket(volume.Previews) == nil {
			if _, err := b.CreateBucket(volume.Previews); err != nil {
				return err
			}
		}
		rows, _, err := visiblePoints(b, n, q)
		if err != nil {
			return err
		}
		tombstones, err := readTombstones(b)
		if err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		snap := snapshotOf(fmt.Sprintf("%020d", seq), n, q, rows, tombstoneIDs(tombstones))
		snap.Fingerprint = previewFingerprint(snap)
		if err := putPreview(b, snap); err != nil {
			return err
		}
		view = previewView(snap, nil)
		return nil
	})
	return view, err
}

func (e *Engine) Preview(name, id string) (model.PreviewView, error) {
	var view model.PreviewView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		snap, err := loadPreview(b, id)
		if err != nil {
			return err
		}
		if snap.Cancelled || snap.TombstoneID != "" {
			view = previewView(snap, nil)
			return nil
		}
		drift, err := previewDrift(b, n, snap)
		if err != nil {
			return err
		}
		view = previewView(snap, &drift)
		return nil
	})
	return view, err
}

func (e *Engine) CancelPreview(name, id, fingerprint string) (model.PreviewView, error) {
	var view model.PreviewView
	if fingerprint == "" {
		return view, model.Bad("preview fingerprint is required")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		snap, err := loadPreview(b, id)
		if err != nil {
			return err
		}
		if snap.Fingerprint != fingerprint {
			return model.Conflict("confirmation does not match preview fingerprint")
		}
		if snap.TombstoneID != "" {
			return model.Conflict("preview already confirmed")
		}
		if snap.Cancelled {
			view = previewView(snap, nil)
			return nil
		}
		snap.Cancelled = true
		if err := putPreview(b, snap); err != nil {
			return err
		}
		view = previewView(snap, nil)
		return nil
	})
	return view, err
}

// ConfirmDelete creates the deletion bound to a previewed selection. Any drift
// that changes which samples would be hidden rejects the confirmation, leaving
// the preview readable so the operator can inspect and cancel it.
func (e *Engine) ConfirmDelete(name, id, fingerprint string) (model.DeleteConfirmation, error) {
	var out model.DeleteConfirmation
	if fingerprint == "" {
		return out, model.Bad("preview fingerprint is required")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		snap, err := loadPreview(b, id)
		if err != nil {
			return err
		}
		if snap.Cancelled {
			return model.Conflict("preview was cancelled")
		}
		if snap.TombstoneID != "" {
			return model.Conflict("preview already confirmed")
		}
		if snap.Fingerprint != fingerprint {
			return model.Conflict("confirmation does not match preview fingerprint")
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		drift, err := previewDrift(b, n, snap)
		if err != nil {
			return err
		}
		if drift.Invalidated {
			return model.Conflict(fmt.Sprintf(
				"preview %s is no longer valid: %v", snap.ID, drift.Reasons))
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		tomb := model.Tombstone{
			ID: fmt.Sprintf("%020d", seq), Start: snap.Start, End: snap.End,
			Sequence: seq, Series: make([]string, 0, len(snap.Series)),
		}
		hidden := 0
		for _, s := range snap.Series {
			tomb.Series = append(tomb.Series, s.Series.ID)
			hidden += s.Visible
		}
		if err := volume.PutJSON(b.Bucket(volume.Deletes), []byte(tomb.ID), tomb); err != nil {
			return err
		}
		snap.TombstoneID, snap.ConfirmSeq = tomb.ID, seq
		if err := putPreview(b, snap); err != nil {
			return err
		}
		n.LastSequence = seq
		if err := volume.PutJSON(b, volume.Config, n); err != nil {
			return err
		}
		out = model.DeleteConfirmation{
			Preview: snap.ID, Fingerprint: snap.Fingerprint,
			Tombstone: tomb, Hidden: hidden, Notices: drift.Notices,
		}
		return nil
	})
	return out, err
}
