package engine

import (
	"fmt"
	"math"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// Migrate rewrites every block older than the configured write format to
// that format in one storage transaction. An interrupted migration commits
// nothing, so no confirmed observation can be lost or half-rewritten. Blocks
// already at the target format are skipped; when nothing needs rewriting the
// database is left untouched and no sequence is consumed. Migration is
// forward-only: blocks newer than the target, or in an unknown format, fail
// the operation without changing the database.
func (e *Engine) Migrate(name string) (model.Migration, error) {
	receipt := model.Migration{Format: e.WriteFormat}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		blocks, err := readBlocks(b)
		if err != nil {
			return err
		}
		var outdated []model.Block
		points := 0
		for _, block := range blocks {
			switch {
			case block.Format == e.WriteFormat:
				receipt.Current++
			case block.Format == chunk.FormatV1 && e.WriteFormat == chunk.FormatV2:
				outdated = append(outdated, block)
				points += block.Count
			case block.Format > e.WriteFormat && chunk.Supported(block.Format):
				return model.Conflict(fmt.Sprintf(
					"block %s uses format %d, newer than migration target %d; downgrade is not supported",
					block.ID, block.Format, e.WriteFormat))
			default:
				return model.Conflict(fmt.Sprintf(
					"block %s has unknown format %d", block.ID, block.Format))
			}
		}
		if len(outdated) == 0 {
			receipt.Sequence = volume.Current(tx)
			return nil
		}
		// Maintenance is bounded by the same scan policy as user queries.
		if points > n.MaxScan {
			return model.Capacity("migration decoded point budget exceeded")
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for _, block := range outdated {
			payload := b.Bucket(volume.Payloads).Get([]byte(block.ID))
			if payload == nil {
				return fmt.Errorf("block %s payload missing", block.ID)
			}
			samples, err := chunk.Decode(payload, block)
			if err != nil {
				return err
			}
			upgraded, checksum, err := chunk.Encode(samples, e.WriteFormat)
			if err != nil {
				return err
			}
			// Verify the rewritten payload against the source samples before
			// publishing it; time, value bits and sequence must be identical.
			probe := block
			probe.Format, probe.Checksum, probe.Bytes = e.WriteFormat, checksum, len(upgraded)
			check, err := chunk.Decode(upgraded, probe)
			if err != nil {
				return fmt.Errorf("block %s migration verification: %w", block.ID, err)
			}
			if err := sameSamples(samples, check); err != nil {
				return fmt.Errorf("block %s migration mismatch: %w", block.ID, err)
			}
			if err := volume.PutJSON(b.Bucket(volume.Blocks), []byte(block.ID), probe); err != nil {
				return err
			}
			if err := b.Bucket(volume.Payloads).Put([]byte(block.ID), upgraded); err != nil {
				return err
			}
			receipt.Rewritten++
			receipt.Points += len(samples)
		}
		n.LastSequence = seq
		receipt.Sequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

func sameSamples(a, b []model.Sample) error {
	if len(a) != len(b) {
		return fmt.Errorf("sample count %d became %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Time != b[i].Time || a[i].Sequence != b[i].Sequence ||
			math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return fmt.Errorf("sample %d changed during re-encode", i)
		}
	}
	return nil
}
