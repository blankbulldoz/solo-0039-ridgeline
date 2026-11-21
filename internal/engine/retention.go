package engine

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

const retentionOnStaleReject = "reject"
const retentionOnStaleContinue = "continue"

// fullRange scans every point the namespace can currently expose.
func fullRangeQuery() model.Query {
	return model.Query{Start: model.MinTime, End: model.MaxTime + 1}
}

// signToken binds the token fields so a client cannot forge or mutate a
// preview snapshot before confirming it.
func signToken(t model.RetentionToken) (model.RetentionToken, error) {
	unsigned := t
	unsigned.Digest = ""
	canonical, err := json.Marshal(unsigned)
	if err != nil {
		return t, err
	}
	digest := sha256.Sum256(canonical)
	t.Digest = hex.EncodeToString(digest[:])
	return t, nil
}

func encodeToken(t model.RetentionToken) (string, error) {
	raw, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeToken(encoded string) (model.RetentionToken, error) {
	var t model.RetentionToken
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return t, model.Bad("invalid retention preview token")
	}
	if err := json.Unmarshal(raw, &t); err != nil {
		return t, model.Bad("invalid retention preview token")
	}
	provided := t.Digest
	t.Digest = ""
	canonical, err := json.Marshal(t)
	if err != nil {
		return t, model.Bad("invalid retention preview token")
	}
	digest := sha256.Sum256(canonical)
	if provided == "" || provided != hex.EncodeToString(digest[:]) {
		return t, model.Bad("retention preview token signature does not match")
	}
	t.Digest = provided
	return t, nil
}

// rollupSignature fingerprints the materialization set and the stored query
// intervals. Rollup writes do not advance the namespace source sequence, so
// they need their own change signal.
func rollupSignature(b *bolt.Bucket) (string, error) {
	names := []string{}
	queries := map[string]model.Query{}
	err := b.Bucket(volume.Rollups).ForEach(func(k, v []byte) error {
		var mat model.Materialization
		if err := json.Unmarshal(v, &mat); err != nil {
			return err
		}
		name := string(k)
		names = append(names, name)
		queries[name] = mat.Query
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		q := queries[name]
		parts = append(parts, fmt.Sprintf("%s:%d:%d", name, q.Start, q.End))
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%v", parts)))
	return hex.EncodeToString(digest[:]), nil
}

func readRollups(b *bolt.Bucket) ([]model.Materialization, error) {
	out := make([]model.Materialization, 0)
	err := b.Bucket(volume.Rollups).ForEach(func(_, v []byte) error {
		var mat model.Materialization
		if err := json.Unmarshal(v, &mat); err != nil {
			return err
		}
		out = append(out, mat)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// buildPreview evaluates the effect of a retention advance without mutating
// anything. It runs entirely inside a read transaction.
func buildPreview(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace, now int64) (model.RetentionPreview, error) {
	preview := model.RetentionPreview{
		Namespace:    n.Name,
		Now:          now,
		CurrentFloor: n.Floor,
		Series:       []model.RetentionSeriesImpact{},
		Rollups:      []model.RetentionRollupImpact{},
		HeadRecords:  b.Bucket(volume.Head).Stats().KeyN,
		MaxScan:      n.MaxScan,
	}
	floor, err := policy.RetentionFloor(n, now)
	if err != nil {
		return preview, err
	}
	preview.ProposedFloor = floor
	preview.Advances = floor > n.Floor
	preview.SourceSequence = n.LastSequence

	// Scan against the current floor so the preview counts points that are
	// visible today and would be removed. Budget rejection is a capacity
	// error and leaves the database untouched.
	raw, err := scan(b, n, fullRangeQuery(), volume.Current(tx))
	if err != nil {
		return preview, err
	}
	preview.Blocks = raw.Stats.Blocks
	preview.Decoded = raw.Stats.Decoded
	preview.BudgetExhausted = preview.Decoded >= n.MaxScan
	for _, sp := range raw.Series {
		removed, kept := 0, 0
		for _, p := range sp.Points {
			if p.Time < floor {
				removed++
			} else {
				kept++
			}
		}
		preview.VisiblePoints += len(sp.Points)
		preview.AffectedPoints += removed
		if removed > 0 {
			preview.Series = append(preview.Series, model.RetentionSeriesImpact{
				Series: sp.Series, Removed: removed, Kept: kept,
			})
		}
	}

	rollups, err := readRollups(b)
	if err != nil {
		return preview, err
	}
	for _, mat := range rollups {
		impact := model.RetentionRollupImpact{
			Name:  mat.Name,
			Start: mat.Query.Start,
			End:   mat.Query.End,
			Stale: mat.SourceSequence != n.LastSequence,
		}
		// A rollup is affected when its definition reaches below the new
		// floor; its stored output would not be reproducible afterwards.
		if mat.Query.Start < floor {
			impact.Overlaps = true
			for _, sp := range raw.Series {
				for _, p := range sp.Points {
					if p.Time >= floor && p.Time >= mat.Query.Start && p.Time < mat.Query.End {
						impact.Remaining++
					}
				}
			}
		}
		preview.Rollups = append(preview.Rollups, impact)
	}

	signature, err := rollupSignature(b)
	if err != nil {
		return preview, err
	}
	token, err := signToken(model.RetentionToken{
		Namespace:       n.Name,
		Now:             now,
		CurrentFloor:    n.Floor,
		ProposedFloor:   floor,
		SourceSequence:  n.LastSequence,
		RollupSignature: signature,
	})
	if err != nil {
		return preview, err
	}
	preview.Token, err = encodeToken(token)
	return preview, err
}

// RetentionPreview returns the planned floor, affected visible points, series
// and materializations, plus a token required to confirm the advance.
func (e *Engine) RetentionPreview(name string, now int64) (model.RetentionPreview, error) {
	var preview model.RetentionPreview
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		preview, err = buildPreview(tx, b, n, now)
		return err
	})
	return preview, err
}

// RetentionConfirm commits a preview. The token is rejected unless the
// namespace source sequence, floor and rollup set still match the preview,
// letting the operator choose to recompute the preview or proceed anyway.
func (e *Engine) RetentionConfirm(name string, encoded string, onStale string) (model.RetentionResult, error) {
	if onStale == "" {
		onStale = retentionOnStaleReject
	}
	if onStale != retentionOnStaleReject && onStale != retentionOnStaleContinue {
		return model.RetentionResult{}, model.Bad("on_stale must be reject or continue")
	}
	if encoded == "" {
		return model.RetentionResult{}, model.Bad("retention preview token is required")
	}
	token, err := decodeToken(encoded)
	if err != nil {
		return model.RetentionResult{}, err
	}
	var result model.RetentionResult
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		if token.Namespace != n.Name {
			return model.Conflict("retention preview token belongs to another namespace")
		}
		// Recompute the floor from the same observation time. A regressing
		// `now` can never lower the monotonic floor.
		floor, err := policy.RetentionFloor(n, token.Now)
		if err != nil {
			return err
		}

		reasons := []string{}
		if n.Floor != token.CurrentFloor {
			reasons = append(reasons, "floor changed since preview")
		}
		if n.LastSequence != token.SourceSequence {
			reasons = append(reasons, "namespace source data changed since preview")
		}
		signature, err := rollupSignature(b)
		if err != nil {
			return err
		}
		if signature != token.RollupSignature {
			reasons = append(reasons, "rollup set changed since preview")
		}
		if floor != token.ProposedFloor {
			reasons = append(reasons, "recomputed floor no longer matches preview")
		}
		stale := len(reasons) > 0
		if stale && onStale == retentionOnStaleReject {
			return model.Conflict("retention preview is no longer current (" +
				joinReasons(reasons) + "); recompute a preview or confirm with on_stale=continue")
		}

		result.StalePreview = stale
		if floor <= n.Floor {
			// No advancement: retention is irreversible, so nothing is
			// written and no sequence is consumed.
			result.NoOp = true
			result.Maintenance = model.Maintenance{Floor: n.Floor}
			return nil
		}

		// Scan against the current floor so points about to be removed are
		// still observable for the receipt, then filter them out before the
		// physical rewrite. A current scan rather than the preview snapshot
		// decides what survives, so points admitted during the preview
		// window are handled correctly.
		raw, err := scan(b, n, fullRangeQuery(), volume.Current(tx))
		if err != nil {
			return err
		}
		surviving := model.RangeResult{Sequence: raw.Sequence, Floor: floor, Series: []model.SeriesPoints{}}
		removed := 0
		affected := map[string]bool{}
		for _, sp := range raw.Series {
			kept := make([]model.Sample, 0, len(sp.Points))
			for _, p := range sp.Points {
				if p.Time < floor {
					removed++
					affected[sp.Series.ID] = true
					continue
				}
				kept = append(kept, p)
			}
			if len(kept) > 0 {
				surviving.Series = append(surviving.Series,
					model.SeriesPoints{Series: sp.Series, Points: kept})
			}
		}
		n.Floor = floor
		receipt, err := publishCompact(tx, b, n, surviving)
		if err != nil {
			return err
		}
		result.Maintenance = receipt
		result.Removed = removed
		result.AffectedSeries = len(affected)
		return nil
	})
	return result, err
}

func joinReasons(reasons []string) string {
	out := ""
	for i, reason := range reasons {
		if i > 0 {
			out += "; "
		}
		out += reason
	}
	return out
}

// Retain keeps the immediate retention operation available for callers that do
// not need the two-phase preview flow; it advances and compacts atomically.
func (e *Engine) Retain(name string, now int64) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		floor, err := policy.RetentionFloor(n, now)
		if err != nil {
			return err
		}
		if floor <= n.Floor {
			// Retention is irreversible; a regressing now writes nothing.
			receipt = model.Maintenance{Floor: n.Floor}
			return nil
		}
		raw, err := scan(b, n, fullRangeQuery(), volume.Current(tx))
		if err != nil {
			return err
		}
		surviving := model.RangeResult{Sequence: raw.Sequence, Floor: floor, Series: []model.SeriesPoints{}}
		for _, sp := range raw.Series {
			kept := make([]model.Sample, 0, len(sp.Points))
			for _, p := range sp.Points {
				if p.Time >= floor {
					kept = append(kept, p)
				}
			}
			if len(kept) > 0 {
				surviving.Series = append(surviving.Series,
					model.SeriesPoints{Series: sp.Series, Points: kept})
			}
		}
		n.Floor = floor
		receipt, err = publishCompact(tx, b, n, surviving)
		return err
	})
	return receipt, err
}
