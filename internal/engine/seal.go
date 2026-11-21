package engine

import (
	"encoding/json"
	"errors"
	"fmt"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// MaxSealBatch bounds the work a single seal batch may move, mirroring the
// namespace write/scan capacity boundaries.
const MaxSealBatch = 100000

const (
	sealStateRunning  = "running"
	sealStateComplete = "complete"
)

// sealMarkers names the nested bucket (sibling of the active-job record) that
// marks head keys created after the job started.
var sealMarkers = []byte("markers")

var errBatchLimitReached = errors.New("seal batch point limit reached")

func putBlock(b *bolt.Bucket, id string, seq uint64, points []model.Sample) error {
	block, payload, err := chunk.Build(id, seq, points)
	if err != nil {
		return err
	}
	if err := volume.PutJSON(b.Bucket(volume.Blocks), []byte(block.ID), block); err != nil {
		return err
	}
	return b.Bucket(volume.Payloads).Put([]byte(block.ID), payload)
}

func clearBucket(parent *bolt.Bucket, name []byte) error {
	if err := parent.DeleteBucket(name); err != nil {
		return err
	}
	_, err := parent.CreateBucket(name)
	return err
}

// loadSealJob returns the active job record, or nil when no job is stored.
func loadSealJob(b *bolt.Bucket) (*model.SealJob, error) {
	raw := b.Bucket(volume.SealJobs).Get(volume.ActiveSealJob)
	if raw == nil {
		return nil, nil
	}
	var job model.SealJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func saveSealJob(b *bolt.Bucket, job *model.SealJob) error {
	return volume.PutJSON(b.Bucket(volume.SealJobs), volume.ActiveSealJob, job)
}

func clearSealJob(b *bolt.Bucket) error {
	jobs := b.Bucket(volume.SealJobs)
	if err := jobs.DeleteBucket(sealMarkers); err != nil && err != bolt.ErrBucketNotFound {
		return err
	}
	return jobs.Delete(volume.ActiveSealJob)
}

// addedKeyBucket holds markers for head keys that did not exist when the job
// started. A correction to an initial key has no marker, so initial work and
// work added while running stay distinguishable even after value rewrites.
func addedKeyBucket(b *bolt.Bucket, job *model.SealJob) (*bolt.Bucket, error) {
	return b.Bucket(volume.SealJobs).CreateBucketIfNotExists(sealMarkers)
}

// isSealedKey reports whether a head key has already been moved into a block.
// It distinguishes a correction to a sealed initial point from genuinely new
// work arriving while the job runs.
func isSealedKey(b *bolt.Bucket, key []byte) bool {
	series := key[:64]
	t, err := volume.PointTime(key)
	if err != nil {
		return false
	}
	var block model.Block
	err = b.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
		if err := json.Unmarshal(value, &block); err != nil {
			return err
		}
		if block.SeriesID == string(series) && t >= block.Start && t < block.End {
			return errBatchLimitReached // sentinel: match found, stop scanning
		}
		return nil
	})
	return err == errBatchLimitReached
}

func refreshProgress(b *bolt.Bucket, job *model.SealJob) {
	headCount := volume.ReadHeadCount(b)
	initialPending := headCount - job.AddedPending
	if initialPending < 0 {
		initialPending = 0
	}
	job.Progress = model.SealProgress{
		Total: job.InitialTotal + job.AddedSealed + job.AddedPending +
			job.CorrectionsSealed,
		Sealed:            job.InitialSealed + job.AddedSealed + job.CorrectionsSealed,
		Remaining:         headCount,
		InitialTotal:      job.InitialTotal,
		InitialSealed:     job.InitialSealed,
		InitialPending:    initialPending,
		AddedSealed:       job.AddedSealed,
		AddedPending:      job.AddedPending,
		CorrectionsSealed: job.CorrectionsSealed,
	}
}

func sealReceipt(n model.Namespace, seq uint64, blocks, points int, job *model.SealJob, b *bolt.Bucket) model.BatchReceipt {
	view := model.SealJobView{}
	if job != nil {
		refreshProgress(b, job)
		copy := *job
		view = model.SealJobView{Active: job.State == sealStateRunning, Job: &copy}
	}
	return model.BatchReceipt{
		Sequence: seq, Blocks: blocks, Points: points, Floor: n.Floor, Job: view,
	}
}

// runSealBatch moves at most job.BatchPoints head keys into compressed blocks
// inside the caller's transaction. Every written block preserves the original
// sample sequence, so the read merge keeps newer corrections over older sealed
// values. Deleting the sealed head keys in the same transaction makes a crash
// or repeated call harmless: either the whole batch committed or none of it.
func runSealBatch(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace, job *model.SealJob) (model.BatchReceipt, error) {
	grouped := map[string][]model.Sample{}
	keys := make([][]byte, 0, job.BatchPoints)
	err := b.Bucket(volume.Head).ForEach(func(k, v []byte) error {
		if len(keys) >= job.BatchPoints {
			return errBatchLimitReached
		}
		if len(k) != 72 {
			return model.Bad("invalid stored point key")
		}
		var point model.Sample
		if err := json.Unmarshal(v, &point); err != nil {
			return err
		}
		keys = append(keys, append([]byte(nil), k...))
		grouped[string(k[:64])] = append(grouped[string(k[:64])], point)
		return nil
	})
	if err != nil && err != errBatchLimitReached {
		return model.BatchReceipt{}, err
	}

	markers, err := addedKeyBucket(b, job)
	if err != nil {
		return model.BatchReceipt{}, err
	}
	initialCount, addedCount, correctionCount := 0, 0, 0
	for _, key := range keys {
		switch {
		case markers.Get(key) != nil:
			addedCount++
			if err := markers.Delete(key); err != nil {
				return model.BatchReceipt{}, err
			}
		case isSealedKey(b, key):
			// The key was sealed in an earlier batch and has returned with a
			// newer value; it is correction work, not new backlog.
			correctionCount++
		default:
			initialCount++
		}
	}

	receiptBlocks, receiptPoints, seq := 0, len(keys), volume.Current(tx)
	if len(keys) > 0 {
		seq, err = volume.Next(tx)
		if err != nil {
			return model.BatchReceipt{}, err
		}
		for id, points := range grouped {
			if err := putBlock(b, id, seq, points); err != nil {
				return model.BatchReceipt{}, err
			}
			receiptBlocks++
		}
		for _, key := range keys {
			if err := b.Bucket(volume.Head).Delete(key); err != nil {
				return model.BatchReceipt{}, err
			}
		}
		if err := volume.AdjustHeadCount(b, -len(keys)); err != nil {
			return model.BatchReceipt{}, err
		}
	}

	job.Batches++
	job.Blocks += receiptBlocks
	job.PointsSealed += receiptPoints
	job.InitialSealed += initialCount
	job.AddedSealed += addedCount
	job.CorrectionsSealed += correctionCount
	job.AddedPending -= addedCount
	n.LastSequence = seq

	if volume.ReadHeadCount(b) == 0 {
		job.State = sealStateComplete
		job.FinishedSequence = seq
		if err := clearSealJob(b); err != nil {
			return model.BatchReceipt{}, err
		}
	} else {
		if err := saveSealJob(b, job); err != nil {
			return model.BatchReceipt{}, err
		}
	}
	if err := volume.PutJSON(b, volume.Config, n); err != nil {
		return model.BatchReceipt{}, err
	}
	return sealReceipt(n, seq, receiptBlocks, receiptPoints, job, b), nil
}

func validateBatchPoints(points int) error {
	if points < 1 || points > MaxSealBatch {
		return model.Bad(fmt.Sprintf("batch_points must be between 1 and %d", MaxSealBatch))
	}
	return nil
}

// ensureNoSealJob guards maintenance operations that rebuild physical storage.
func ensureNoSealJob(b *bolt.Bucket) error {
	job, err := loadSealJob(b)
	if err != nil {
		return err
	}
	if job != nil && job.State == sealStateRunning {
		return model.Conflict("seal job is in progress; finish it before compaction or retention")
	}
	return nil
}

// StartSealJob opens a persistent seal job, snapshotting the current head as
// the initial backlog, and immediately runs its first batch.
func (e *Engine) StartSealJob(name string, batchPoints int) (model.BatchReceipt, error) {
	if err := validateBatchPoints(batchPoints); err != nil {
		return model.BatchReceipt{}, err
	}
	var receipt model.BatchReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if active, err := loadSealJob(b); err != nil {
			return err
		} else if active != nil && active.State == sealStateRunning {
			return model.Conflict("seal job already in progress")
		}
		if err := clearSealJob(b); err != nil {
			return err
		}
		headCount := volume.ReadHeadCount(b)
		job := &model.SealJob{
			ID:              fmt.Sprintf("seal-%020d", volume.Current(tx)+1),
			State:           sealStateRunning,
			BatchPoints:     batchPoints,
			CreatedSequence: volume.Current(tx),
			InitialTotal:    headCount,
		}
		if err := saveSealJob(b, job); err != nil {
			return err
		}
		if _, err := addedKeyBucket(b, job); err != nil {
			return err
		}
		receipt, err = runSealBatch(tx, b, n, job)
		return err
	})
	return receipt, err
}

// SealBatch advances a running seal job by one batch. A zero batchPoints value
// keeps the cap chosen when the job started.
func (e *Engine) SealBatch(name string, batchPoints int) (model.BatchReceipt, error) {
	if batchPoints != 0 {
		if err := validateBatchPoints(batchPoints); err != nil {
			return model.BatchReceipt{}, err
		}
	}
	var receipt model.BatchReceipt
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		job, err := loadSealJob(b)
		if err != nil {
			return err
		}
		if job == nil {
			return model.Missing("no seal job is active; start one first")
		}
		if job.State != sealStateRunning {
			return model.Conflict("seal job is not running")
		}
		if batchPoints > 0 {
			job.BatchPoints = batchPoints
		}
		receipt, err = runSealBatch(tx, b, n, job)
		return err
	})
	return receipt, err
}

// SealJobStatus reports persistent seal-job progress, including after restart.
func (e *Engine) SealJobStatus(name string) (model.SealJobView, error) {
	var view model.SealJobView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		job, err := loadSealJob(b)
		if err != nil {
			return err
		}
		if job == nil {
			view = model.SealJobView{Active: false, Job: nil}
			return nil
		}
		refreshProgress(b, job)
		copy := *job
		view = model.SealJobView{Active: job.State == sealStateRunning, Job: &copy}
		return nil
	})
	return view, err
}

// Seal preserves the original one-shot, whole-head transaction for callers that
// do not request a batch cap. It refuses to run while a batched job owns the
// head so the two maintenance paths cannot interleave.
func (e *Engine) Seal(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if active, err := loadSealJob(b); err != nil {
			return err
		} else if active != nil && active.State == sealStateRunning {
			return model.Conflict("batched seal job is in progress")
		}
		grouped := map[string][]model.Sample{}
		err = b.Bucket(volume.Head).ForEach(func(k, v []byte) error {
			if len(k) != 72 {
				return model.Bad("invalid stored point key")
			}
			var point model.Sample
			if err := json.Unmarshal(v, &point); err != nil {
				return err
			}
			grouped[string(k[:64])] = append(grouped[string(k[:64])], point)
			return nil
		})
		if err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for id, points := range grouped {
			if err := putBlock(b, id, seq, points); err != nil {
				return err
			}
			receipt.Blocks++
			receipt.Points += len(points)
		}
		if err := clearBucket(b, volume.Head); err != nil {
			return err
		}
		if err := volume.AdjustHeadCount(b, -volume.ReadHeadCount(b)); err != nil {
			return err
		}
		n.LastSequence = seq
		receipt.Sequence, receipt.Floor = seq, n.Floor
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

func (e *Engine) Blocks(name string) ([]model.Block, error) {
	var blocks []model.Block
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		blocks, err = readBlocks(b)
		return err
	})
	return blocks, err
}
