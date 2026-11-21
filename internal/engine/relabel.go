package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// A relabel job moves one label key inside a namespace without changing the
// logical observations. Three facts make a naive rename wrong:
//
//   - a series without the source key has no identity to change, so it must be
//     left untouched rather than acquire an empty target label;
//   - a source value that is explicitly empty is a present value and must move
//     as empty; it must not be confused with an absent label;
//   - two source identities can collapse onto one target identity, so distinct
//     committed samples can land on the same timestamp.
//
// Timestamp collisions are resolved by an explicit policy. The default refuses
// the job, so no caller can accidentally discard a value. deduplicate_identical
// accepts repeated equal values, keep_first/keep_last choose by commit sequence.
//
// A successful execution rewrites the catalog, head, blocks and both persisted
// references to old identities: deletion intervals and materialized rollups.
// Deletion intervals no longer point at removed identities; samples they hid are
// dropped during the rewrite, so the interval is settled rather than redirected
// at a merged series where it would hide unrelated samples. Materialized queries
// are repointed at the new key and left explicitly stale so old values are never
// presented as current.
//
// The whole rewrite and its terminal job record publish in one bbolt write
// transaction. The namespace is marked as paused for the duration of that
// transaction. A reopened database can therefore tell "not applied" (no job
// record), "refused with no effect" (failed record) and "completed" (completed
// record whose result sequence equals the post-migration sequence).

const (
	CollisionReject    = "reject"
	CollisionKeepLast  = "keep_last"
	CollisionDedupSame = "deduplicate_identical"
	CollisionKeepFirst = "keep_first"

	LabelConflictReject       = "reject"
	LabelConflictKeepTarget   = "keep_target"
	LabelConflictPreferSource = "prefer_source"

	jobRunning   = "running"
	jobCompleted = "completed"
	jobFailed    = "failed"
)

var valuePolicies = map[string]bool{
	CollisionReject: true, CollisionKeepLast: true,
	CollisionDedupSame: true, CollisionKeepFirst: true,
}

var labelPolicies = map[string]bool{
	LabelConflictReject: true, LabelConflictKeepTarget: true,
	LabelConflictPreferSource: true,
}

type sourceInfo struct {
	old     model.Series
	labels  model.Labels
	newID   string
	visible []model.Sample
	decoded int
	changed bool // identity changes or its samples fold into a target
}

type candidate struct {
	seriesID string
	sample   model.Sample
}

type targetPlan struct {
	newID        string
	labels       model.Labels
	contributors []string
	points       map[int64]model.Sample
	collisions   []model.RelabelCollision
	colliding    int
}

type relabelAnalysis struct {
	sources   []sourceInfo
	byOld     map[string]int
	targets   map[string]*targetPlan
	absorbed  map[string]bool
	plan      model.RelabelPlan
	workCount int
}

func normalizeRequest(req model.RelabelRequest) (model.RelabelRequest, error) {
	from, to := req.Rename.From, req.Rename.To
	if !model.ValidName(from) || !model.ValidName(to) {
		return req, model.Bad("rename requires valid from and to label names")
	}
	if from == to {
		return req, model.Bad("rename from and to must differ")
	}
	if from == "__name__" {
		return req, model.Bad("the __name__ label identifies every series and cannot be renamed")
	}
	if req.ValueConflict == "" {
		req.ValueConflict = CollisionReject
	}
	if !valuePolicies[req.ValueConflict] {
		return req, model.Bad("unknown value conflict policy")
	}
	if req.LabelConflict == "" {
		req.LabelConflict = LabelConflictReject
	}
	if !labelPolicies[req.LabelConflict] {
		return req, model.Bad("unknown label conflict policy")
	}
	if req.Job != "" && !model.ValidName(req.Job) {
		return req, model.Bad("invalid job identifier")
	}
	return req, nil
}

// defaultJobID names a job after the namespace state it migrates. Repeating the
// same attempt is idempotent, while a later migration with new host-labelled
// data gets a distinct identifier and is not silently skipped.
func defaultJobID(req model.RelabelRequest, sourceSequence uint64) string {
	if req.Job != "" {
		return req.Job
	}
	return fmt.Sprintf("relabel-%s-%s-%d", req.Rename.From, req.Rename.To, sourceSequence)
}

// seriesReader answers visible-sample questions for one read transaction. It
// loads block metadata and deletion intervals once instead of per series, so a
// namespace-wide migration does not repeatedly scan the same bucket.
type seriesReader struct {
	blocks     []model.Block
	tombstones []model.Tombstone
	payloads   *bolt.Bucket
	head       *bolt.Bucket
	floor      int64
}

func newSeriesReader(b *bolt.Bucket, n model.Namespace) (*seriesReader, error) {
	blocks, err := readBlocks(b)
	if err != nil {
		return nil, err
	}
	tombstones, err := readTombstones(b)
	if err != nil {
		return nil, err
	}
	return &seriesReader{
		blocks: blocks, tombstones: tombstones,
		payloads: b.Bucket(volume.Payloads), head: b.Bucket(volume.Head),
		floor: n.Floor,
	}, nil
}

// visible reads the visible samples of one series, applying the retention floor
// and deletion intervals so the migration never moves stale or hidden values
// into a new identity. It also reports the decoded sample count so the caller can
// enforce the namespace scan budget.
func (r *seriesReader) visible(s model.Series) ([]model.Sample, int, error) {
	decoded := 0
	merged := map[int64]model.Sample{}
	for _, block := range r.blocks {
		if block.SeriesID != s.ID || block.End <= r.floor {
			continue
		}
		points, err := chunk.Decode(r.payloads.Get([]byte(block.ID)), block)
		if err != nil {
			return nil, decoded, err
		}
		decoded += len(points)
		for _, p := range points {
			if p.Time < r.floor {
				continue
			}
			if old, ok := merged[p.Time]; !ok || p.Sequence > old.Sequence {
				merged[p.Time] = p
			}
		}
	}
	prefix := []byte(s.ID)
	cursor := r.head.Cursor()
	for k, v := cursor.Seek(volume.PointKey(s.ID, model.MinTime)); k != nil && strings.HasPrefix(string(k), string(prefix)); k, v = cursor.Next() {
		if _, err := volume.PointTime(k); err != nil {
			return nil, decoded, err
		}
		var p model.Sample
		if err := json.Unmarshal(v, &p); err != nil {
			return nil, decoded, err
		}
		decoded++
		if p.Time < r.floor {
			continue
		}
		if old, ok := merged[p.Time]; !ok || p.Sequence >= old.Sequence {
			merged[p.Time] = p
		}
	}
	out := make([]model.Sample, 0, len(merged))
	for _, p := range merged {
		if !hidden(s.ID, p, r.tombstones) {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time < out[j].Time })
	return out, decoded, nil
}

func classify(old model.Series, req model.RelabelRequest, plan *model.RelabelPlan) sourceInfo {
	src := sourceInfo{old: old, labels: old.Labels, newID: old.ID}
	result := model.RenameLabel(old.Labels, req.Rename.From, req.Rename.To)
	if !result.Present {
		plan.Summary.SeriesMissing++
		return src
	}
	if result.FromValue == "" {
		plan.Summary.SeriesEmpty++
	}
	if result.ToExists && result.ToValue != result.FromValue {
		plan.Summary.LabelConflicts++
		plan.LabelConflicts = append(plan.LabelConflicts, model.LabelConflictDetail{
			SeriesID: old.ID, FromValue: result.FromValue,
			ToValue: result.ToValue, Resolved: req.LabelConflict,
		})
		switch req.LabelConflict {
		case LabelConflictReject:
			plan.Blocked = true
			src.labels = result.Labels
		case LabelConflictKeepTarget:
			src.labels = model.DropLabel(old.Labels, req.Rename.From)
		case LabelConflictPreferSource:
			src.labels = model.SetLabel(model.DropLabel(old.Labels, req.Rename.From),
				req.Rename.To, result.FromValue)
		}
	} else {
		src.labels = result.Labels
	}
	src.newID = model.SeriesID(src.labels)
	src.changed = true
	return src
}

func analyze(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace, req model.RelabelRequest) (*relabelAnalysis, error) {
	a := &relabelAnalysis{
		byOld:    map[string]int{},
		targets:  map[string]*targetPlan{},
		absorbed: map[string]bool{},
	}
	a.plan = model.RelabelPlan{
		Namespace: n.Name, Rename: req.Rename,
		ValueConflict: req.ValueConflict, LabelConflict: req.LabelConflict,
		Reasons: []string{}, Changes: []model.SeriesIdentityChange{},
		Merges: []model.MergeGroup{}, Collisions: []model.RelabelCollision{},
		LabelConflicts: []model.LabelConflictDetail{}, Rollups: []model.RollupImpact{},
	}
	plan := &a.plan
	all, err := catalog(b)
	if err != nil {
		return nil, err
	}
	// Provenance is the namespace's own sequence, not the global one: a write to
	// another namespace must not invalidate this plan or change its identity.
	plan.SourceSequence = n.LastSequence
	plan.Summary.SeriesAnalyzed = len(all)

	for _, old := range all {
		src := classify(old, req, plan)
		a.byOld[old.ID] = len(a.sources)
		a.sources = append(a.sources, src)
	}
	// Group changing sources by target identity.
	for i := range a.sources {
		src := &a.sources[i]
		if !src.changed {
			continue
		}
		if err := model.ValidateLabels(src.labels); err != nil {
			plan.Blocked = true
			plan.Reasons = append(plan.Reasons, fmt.Sprintf(
				"series %s would have invalid target labels: %v", shortID(src.old.ID), err))
		}
		if src.newID == src.old.ID {
			// Label conflict policy left this identity in place.
			src.changed = false
			continue
		}
		target := a.targets[src.newID]
		if target == nil {
			target = &targetPlan{newID: src.newID, labels: src.labels, points: map[int64]model.Sample{}}
			a.targets[src.newID] = target
		}
		target.contributors = append(target.contributors, src.old.ID)
		a.absorbed[src.old.ID] = true
	}
	// A pre-existing identity that a rename lands on joins that target too.
	for newID := range a.targets {
		index, ok := a.byOld[newID]
		if !ok {
			continue
		}
		existing := &a.sources[index]
		if existing.changed && existing.newID != existing.old.ID {
			// This identity is itself moving away; it cannot also be a target.
			continue
		}
		if a.absorbed[existing.old.ID] {
			continue
		}
		a.targets[newID].contributors = append(a.targets[newID].contributors, existing.old.ID)
		a.absorbed[existing.old.ID] = true
	}
	reader, err := newSeriesReader(b, n)
	if err != nil {
		return nil, err
	}
	// Load points and merge every target.
	for newID, target := range a.targets {
		sort.Strings(target.contributors)
		candidates := map[int64][]candidate{}
		for _, oldID := range target.contributors {
			src := &a.sources[a.byOld[oldID]]
			visible, decoded, err := reader.visible(src.old)
			if err != nil {
				return nil, err
			}
			src.visible, src.decoded = visible, decoded
			a.workCount += decoded
			plan.Summary.SourcePoints += len(visible)
			for _, p := range visible {
				candidates[p.Time] = append(candidates[p.Time],
					candidate{seriesID: src.old.ID, sample: p})
			}
		}
		if a.workCount > n.MaxScan {
			return nil, model.Capacity("relabel source point budget exceeded")
		}
		times := make([]int64, 0, len(candidates))
		for t := range candidates {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		for _, t := range times {
			group := candidates[t]
			sort.Slice(group, func(i, j int) bool {
				if group[i].sample.Sequence != group[j].sample.Sequence {
					return group[i].sample.Sequence < group[j].sample.Sequence
				}
				return group[i].seriesID < group[j].seriesID
			})
			chosen, err := resolveCandidates(req.ValueConflict, group)
			if err != nil {
				// The collision is still reported in full so the caller can see
				// exactly what blocks the job, but no output is chosen.
				plan.Blocked = true
				plan.Reasons = append(plan.Reasons, fmt.Sprintf(
					"target series %s has %d samples at time %d: %v",
					shortID(newID), len(group), t, err))
			} else {
				target.points[t] = chosen.sample
			}
			if len(group) > 1 {
				plan.Summary.Collisions++
				target.colliding++
				chosenID := ""
				if err == nil {
					chosenID = chosen.seriesID
				}
				plan.Collisions = append(plan.Collisions, model.RelabelCollision{
					NewSeriesID: newID, Time: t, Candidates: toSamples(group),
					Chosen: chosenID, Conflicting: hasDifferingValues(group),
				})
			}
		}
	}
	// Result points count the samples written to rewritten target identities.
	// When the plan is refused no output is produced.
	if !plan.Blocked {
		for _, target := range a.targets {
			plan.Summary.ResultPoints += len(target.points)
		}
	}
	// Report identity changes. A changed identity always folds into a target.
	absorbedUnchanged := 0
	for i := range a.sources {
		src := &a.sources[i]
		if !src.changed {
			if a.absorbed[src.old.ID] {
				absorbedUnchanged++
			}
			continue
		}
		plan.Summary.SeriesRenamed++
		plan.Changes = append(plan.Changes, model.SeriesIdentityChange{
			SeriesID: src.old.ID, NewSeriesID: src.newID,
			Labels: src.old.Labels, NewLabels: src.labels, Points: len(src.visible),
		})
	}
	for newID, target := range a.targets {
		if len(target.contributors) < 2 {
			continue
		}
		plan.Summary.MergeGroups++
		plan.Summary.SeriesMerged += len(target.contributors)
		input := 0
		for _, oldID := range target.contributors {
			input += len(a.sources[a.byOld[oldID]].visible)
		}
		plan.Merges = append(plan.Merges, model.MergeGroup{
			NewSeriesID: newID, NewLabels: target.labels,
			SeriesIDs: append([]string{}, target.contributors...),
			Points:    input, Collisions: target.colliding,
		})
	}
	plan.Summary.SeriesUnchanged = plan.Summary.SeriesAnalyzed -
		plan.Summary.SeriesRenamed - absorbedUnchanged
	if plan.Summary.SeriesUnchanged < 0 {
		plan.Summary.SeriesUnchanged = 0
	}
	sort.Slice(plan.Changes, func(i, j int) bool { return plan.Changes[i].SeriesID < plan.Changes[j].SeriesID })
	sort.Slice(plan.Merges, func(i, j int) bool { return plan.Merges[i].NewSeriesID < plan.Merges[j].NewSeriesID })
	sort.Slice(plan.Collisions, func(i, j int) bool {
		if plan.Collisions[i].NewSeriesID != plan.Collisions[j].NewSeriesID {
			return plan.Collisions[i].NewSeriesID < plan.Collisions[j].NewSeriesID
		}
		return plan.Collisions[i].Time < plan.Collisions[j].Time
	})
	sort.Slice(plan.LabelConflicts, func(i, j int) bool {
		return plan.LabelConflicts[i].SeriesID < plan.LabelConflicts[j].SeriesID
	})
	if plan.Blocked {
		// A refused plan publishes nothing; do not advertise an output shape.
		plan.Summary.ResultPoints = 0
	}
	plan.WorkNeeded = plan.Summary.SeriesRenamed > 0
	if plan.WorkNeeded {
		plan.Summary.Deletions = countTombstones(b)
		plan.Summary.Rollups = countRollups(b)
		plan.Rollups, err = planRollups(b, req)
		if err != nil {
			return nil, err
		}
		for _, impact := range plan.Rollups {
			if impact.MatchersRewritten > 0 || impact.GroupByRewritten > 0 ||
				impact.CellLabelsRewritten > 0 {
				plan.Summary.RollupsRewrote++
			}
		}
	}
	return a, nil
}

// resolveCandidates selects the sample that survives at one timestamp. Groups
// arrive sorted by commit sequence then series ID. keep_first and keep_last
// therefore mean oldest and newest committed value; samples committed in the
// same request share a sequence, and that tie is broken by source series ID so
// the outcome is deterministic and reported in the plan.
func resolveCandidates(policy string, group []candidate) (candidate, error) {
	if len(group) == 1 {
		return group[0], nil
	}
	switch policy {
	case CollisionKeepFirst:
		return group[0], nil
	case CollisionKeepLast:
		return group[len(group)-1], nil
	case CollisionDedupSame:
		for _, c := range group[1:] {
			if c.sample.Value != group[0].sample.Value {
				return candidate{}, fmt.Errorf("differing values cannot be deduplicated")
			}
		}
		return group[len(group)-1], nil
	default:
		return candidate{}, fmt.Errorf("choose a value_conflict policy")
	}
}

func toSamples(group []candidate) []model.CollisionSample {
	out := make([]model.CollisionSample, 0, len(group))
	for _, c := range group {
		out = append(out, model.CollisionSample{
			SeriesID: c.seriesID, Value: c.sample.Value, Sequence: c.sample.Sequence,
		})
	}
	return out
}

func hasDifferingValues(group []candidate) bool {
	for _, c := range group[1:] {
		if c.sample.Value != group[0].sample.Value {
			return true
		}
	}
	return false
}

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func countTombstones(b *bolt.Bucket) int {
	return b.Bucket(volume.Deletes).Stats().KeyN
}

func countRollups(b *bolt.Bucket) int {
	return b.Bucket(volume.Rollups).Stats().KeyN
}

// planRollups reports which materialized definitions reference the renamed key.
func planRollups(b *bolt.Bucket, req model.RelabelRequest) ([]model.RollupImpact, error) {
	out := []model.RollupImpact{}
	err := b.Bucket(volume.Rollups).ForEach(func(k, v []byte) error {
		var m model.Materialization
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		impact := model.RollupImpact{Name: string(k)}
		for _, matcher := range m.Query.Matchers {
			if matcher.Label == req.Rename.From {
				impact.MatchersRewritten++
			}
		}
		for _, label := range m.Query.GroupBy {
			if label == req.Rename.From {
				impact.GroupByRewritten++
			}
		}
		for _, cell := range m.Result.Cells {
			if _, ok := cell.Labels[req.Rename.From]; ok {
				impact.CellLabelsRewritten++
			}
		}
		out = append(out, impact)
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

// PreviewRelabel computes the migration plan without mutating the database.
func (e *Engine) PreviewRelabel(name string, req model.RelabelRequest) (model.RelabelPlan, error) {
	normalized, err := normalizeRequest(req)
	if err != nil {
		return model.RelabelPlan{}, err
	}
	var plan model.RelabelPlan
	err = e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		a, err := analyze(tx, b, n, normalized)
		if err != nil {
			return err
		}
		plan = a.plan
		return nil
	})
	return plan, err
}

func (e *Engine) RelabelJobs(name string) (model.RelabelJobs, error) {
	out := model.RelabelJobs{Jobs: []model.RelabelJob{}}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		return b.Bucket(volume.Jobs).ForEach(func(_, v []byte) error {
			var job model.RelabelJob
			if err := json.Unmarshal(v, &job); err != nil {
				return err
			}
			out.Jobs = append(out.Jobs, job)
			return nil
		})
	})
	if err != nil {
		return out, err
	}
	sort.Slice(out.Jobs, func(i, j int) bool {
		if out.Jobs[i].StartedSequence != out.Jobs[j].StartedSequence {
			return out.Jobs[i].StartedSequence < out.Jobs[j].StartedSequence
		}
		return out.Jobs[i].ID < out.Jobs[j].ID
	})
	return out, nil
}

func (e *Engine) RelabelJob(name, id string) (model.RelabelJob, error) {
	var job model.RelabelJob
	if !model.ValidName(id) {
		return job, model.Bad("invalid job identifier")
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if b.Bucket(volume.Jobs).Get([]byte(id)) == nil {
			return model.Missing("relabel job does not exist")
		}
		return volume.GetJSON(b.Bucket(volume.Jobs), []byte(id), &job)
	})
	return job, err
}

// ExecuteRelabel runs in two committed phases. The first records a running job
// and a namespace pause marker; from that commit on, ingestion is rejected. The
// second rewrites the namespace and replaces the running record with the
// completed one in the same transaction. Because the data rewrite and its
// terminal record commit together, a process that dies mid-job leaves either a
// running record with no data change (never committed) or a completed record
// with the change; startup recovery turns the former into a failed record and
// clears the pause. A refusal writes a terminal failed record and no data.
// Replaying a completed job with the same settings returns the recorded job.
func (e *Engine) ExecuteRelabel(name string, req model.RelabelRequest) (model.RelabelJob, error) {
	normalized, err := normalizeRequest(req)
	if err != nil {
		return model.RelabelJob{}, err
	}
	var job model.RelabelJob
	var refusal error
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		if n.Maintenance != "" {
			return model.Conflict("another maintenance job is active")
		}
		a, err := analyze(tx, b, n, normalized)
		if err != nil {
			return err
		}
		plan := a.plan
		normalized.Job = defaultJobID(normalized, plan.SourceSequence)
		if prior := b.Bucket(volume.Jobs).Get([]byte(normalized.Job)); prior != nil {
			var existing model.RelabelJob
			if err := json.Unmarshal(prior, &existing); err != nil {
				return err
			}
			if existing.State == jobCompleted {
				if existing.Rename == normalized.Rename &&
					existing.ValueConflict == normalized.ValueConflict &&
					existing.LabelConflict == normalized.LabelConflict {
					job = existing
					return nil
				}
				return model.Conflict("job identifier already completed with different settings")
			}
		}
		if req.SourceSequence != 0 && req.SourceSequence != plan.SourceSequence {
			return model.Conflict("namespace changed since the preview")
		}
		job = model.RelabelJob{
			ID: normalized.Job, State: jobFailed, Completed: false,
			Rename: normalized.Rename, ValueConflict: normalized.ValueConflict,
			LabelConflict:   normalized.LabelConflict,
			StartedSequence: volume.Current(tx), SourceSequence: plan.SourceSequence,
			Summary: plan.Summary,
		}
		if plan.Blocked {
			job.Detail = strings.Join(plan.Reasons, "; ")
			refusal = model.Conflict(job.Detail)
			return volume.PutJSON(b.Bucket(volume.Jobs), []byte(job.ID), job)
		}
		if !plan.WorkNeeded {
			job.State = jobCompleted
			job.Completed = true
			job.Detail = "no labels required renaming"
			job.ResultSequence = plan.SourceSequence
			return volume.PutJSON(b.Bucket(volume.Jobs), []byte(job.ID), job)
		}
		job.State = jobRunning
		job.Detail = "namespace writes paused for label maintenance"
		n.Maintenance, n.MaintenanceJob = "relabel", job.ID
		if err := volume.PutJSON(b, volume.Config, n); err != nil {
			return err
		}
		return volume.PutJSON(b.Bucket(volume.Jobs), []byte(job.ID), job)
	})
	if err != nil || refusal != nil {
		if refusal != nil {
			err = refusal
		}
		return job, err
	}
	if job.State != jobRunning {
		return job, nil
	}
	// Phase two. Writes are paused, so the namespace is unchanged since phase
	// one and the same plan applies.
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.MaintenanceJob != job.ID {
			return model.Conflict("relabel job is no longer active")
		}
		a, err := analyze(tx, b, n, normalized)
		if err != nil {
			return err
		}
		if a.plan.SourceSequence != job.SourceSequence || n.LastSequence != job.SourceSequence {
			return model.Conflict("namespace changed during relabel")
		}
		return applyRelabel(tx, b, n, normalized, a, &job)
	})
	if err != nil {
		// The rewrite transaction rolled back in full. Record the failure and
		// resume writes so callers can retry without reopening the database.
		e.markRelabelFailed(name, job.ID, err)
		return job, err
	}
	return job, nil
}

// markRelabelFailed clears a pause marker and records a terminal failed job when
// the rewrite transaction could not publish. It runs in its own transaction and
// never touches namespace data.
func (e *Engine) markRelabelFailed(name, id string, cause error) {
	_ = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.MaintenanceJob == id {
			n.Maintenance, n.MaintenanceJob = "", ""
			if err := volume.PutJSON(b, volume.Config, n); err != nil {
				return err
			}
		}
		var job model.RelabelJob
		if err := volume.GetJSON(b.Bucket(volume.Jobs), []byte(id), &job); err != nil {
			return err
		}
		job.State = jobFailed
		job.Completed = false
		job.Detail = "rewrite failed and rolled back: " + cause.Error()
		return volume.PutJSON(b.Bucket(volume.Jobs), []byte(id), job)
	})
}

// applyRelabel is the only mutating half of the job. It runs in the same
// transaction as the terminal job record. Samples keep their original commit
// sequences, so deletion and correction semantics survive the move.
func applyRelabel(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace, req model.RelabelRequest, a *relabelAnalysis, job *model.RelabelJob) error {
	seq, err := volume.Next(tx)
	if err != nil {
		return err
	}
	job.ResultSequence = seq
	// Remove only the identities that fold into a target. Untouched series keep
	// their exact physical head and blocks, so an unrelated series cannot be
	// rewritten or lose a hidden-but-not-deleted sample.
	blocks, err := readBlocks(b)
	if err != nil {
		return err
	}
	catalogBucket := b.Bucket(volume.Catalog)
	for oldID := range a.absorbed {
		if err := catalogBucket.Delete([]byte(oldID)); err != nil {
			return err
		}
		if err := deleteHeadPrefix(b.Bucket(volume.Head), oldID); err != nil {
			return err
		}
		if err := deleteSeriesBlocks(b, blocks, oldID); err != nil {
			return err
		}
	}
	for newID, target := range a.targets {
		if err := volume.PutJSON(catalogBucket, []byte(newID),
			model.Series{ID: newID, Labels: target.labels}); err != nil {
			return err
		}
		if len(target.points) == 0 {
			continue
		}
		points := make([]model.Sample, 0, len(target.points))
		for _, p := range target.points {
			points = append(points, p)
		}
		sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
		block, payload, err := chunk.Build(newID, seq, points)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(b.Bucket(volume.Blocks), []byte(block.ID), block); err != nil {
			return err
		}
		if err := b.Bucket(volume.Payloads).Put([]byte(block.ID), payload); err != nil {
			return err
		}
	}
	if err := resolveTombstones(b, a, job); err != nil {
		return err
	}
	if err := rewriteRollups(b, req, job); err != nil {
		return err
	}
	n.LastSequence = seq
	n.Maintenance, n.MaintenanceJob = "", ""
	job.State = jobCompleted
	job.Completed = true
	job.Detail = "namespace labels migrated"
	if err := volume.PutJSON(b, volume.Config, n); err != nil {
		return err
	}
	// The terminal record shares this transaction: either both the rewrite and
	// the completed marker commit, or neither does.
	return volume.PutJSON(b.Bucket(volume.Jobs), []byte(job.ID), job)
}

// resolveTombstones detaches deletion intervals from identities that no longer
// exist. Their hidden samples were dropped by the rewrite, so redirecting the
// interval at a merged identity would hide samples that were never deleted.
func resolveTombstones(b *bolt.Bucket, a *relabelAnalysis, job *model.RelabelJob) error {
	tombstones, err := readTombstones(b)
	if err != nil {
		return err
	}
	job.Summary.Deletions = len(tombstones)
	job.Summary.DeletionsRewrote = 0
	for _, tomb := range tombstones {
		remaining := make([]string, 0, len(tomb.Series))
		changed := false
		for _, id := range tomb.Series {
			if a.absorbed[id] {
				changed = true
				continue
			}
			remaining = append(remaining, id)
		}
		if !changed {
			continue
		}
		// Keep the first audit trail across successive migrations.
		if len(tomb.OriginSeries) == 0 {
			tomb.OriginSeries = append([]string{}, tomb.Series...)
		}
		tomb.Series = remaining
		tomb.Resolved = true
		tomb.Job = job.ID
		if err := volume.PutJSON(b.Bucket(volume.Deletes), []byte(tomb.ID), tomb); err != nil {
			return err
		}
		job.Summary.DeletionsRewrote++
	}
	return nil
}

// rewriteRollups repoints materialized definitions and stored cell labels at the
// new key. Execution already advances the namespace sequence, so every stored
// result is reported stale and is never presented as current after the rename.
func rewriteRollups(b *bolt.Bucket, req model.RelabelRequest, job *model.RelabelJob) error {
	names := []string{}
	err := b.Bucket(volume.Rollups).ForEach(func(k, _ []byte) error {
		names = append(names, string(k))
		return nil
	})
	if err != nil {
		return err
	}
	job.Summary.Rollups = len(names)
	job.Summary.RollupsRewrote = 0
	for _, name := range names {
		var m model.Materialization
		if err := volume.GetJSON(b.Bucket(volume.Rollups), []byte(name), &m); err != nil {
			return err
		}
		changed := false
		for i := range m.Query.Matchers {
			if m.Query.Matchers[i].Label == req.Rename.From {
				m.Query.Matchers[i].Label = req.Rename.To
				changed = true
			}
		}
		for i := range m.Query.GroupBy {
			if m.Query.GroupBy[i] == req.Rename.From {
				m.Query.GroupBy[i] = req.Rename.To
				changed = true
			}
		}
		for i := range m.Result.Cells {
			value, ok := m.Result.Cells[i].Labels[req.Rename.From]
			if !ok {
				continue
			}
			if existing, exists := m.Result.Cells[i].Labels[req.Rename.To]; exists && existing != value {
				// Two source keys would collapse on one cell label. Leave the
				// stored value untouched; staleness flags it for rematerialize.
				continue
			}
			delete(m.Result.Cells[i].Labels, req.Rename.From)
			m.Result.Cells[i].Labels[req.Rename.To] = value
			changed = true
		}
		if !changed {
			continue
		}
		if err := volume.PutJSON(b.Bucket(volume.Rollups), []byte(name), m); err != nil {
			return err
		}
		job.Summary.RollupsRewrote++
	}
	return nil
}

// deleteHeadPrefix removes every head point of one series identity.
func deleteHeadPrefix(head *bolt.Bucket, seriesID string) error {
	prefix := []byte(seriesID)
	cursor := head.Cursor()
	keys := [][]byte{}
	for k, _ := cursor.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = cursor.Next() {
		keys = append(keys, append([]byte{}, k...))
	}
	for _, k := range keys {
		if err := head.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// deleteSeriesBlocks removes block metadata and payloads belonging to one
// series identity. The caller supplies the block inventory so repeated calls
// during one rewrite do not rescan the bucket.
func deleteSeriesBlocks(b *bolt.Bucket, blocks []model.Block, seriesID string) error {
	for _, block := range blocks {
		if block.SeriesID != seriesID {
			continue
		}
		if err := b.Bucket(volume.Blocks).Delete([]byte(block.ID)); err != nil {
			return err
		}
		if err := b.Bucket(volume.Payloads).Delete([]byte(block.ID)); err != nil {
			return err
		}
	}
	return nil
}
