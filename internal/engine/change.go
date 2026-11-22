package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// change is the single boundary between an engine maintenance or mutation
// entry point and durable publication. A step never opens its own write
// transaction, calls volume.Next, assigns Namespace.LastSequence or writes the
// namespace config itself. Instead it asks this boundary for a receipt
// sequence, a source publication or the final commit, so the same invariants
// hold for every current and future composed maintenance step.
//
// Contracts enforced here:
//
//   - Logical mutations reject a read-only namespace before any work runs;
//     physical maintenance is started with physical=true and remains allowed.
//   - Next allocates the database-wide receipt sequence. Receipts therefore
//     stay globally monotonic even when a step never becomes a namespace
//     source version.
//   - PublishSource allocates a receipt sequence and makes it the namespace
//     source version. Materialization must not use it: a materialized rollup
//     remembers the source it actually read, and publishing that rollup never
//     pushes the source version forward.
//   - finish persists the config exactly once, and only when it changed.
//   - Everything runs in one bbolt write transaction, so a failing composite
//     step (for example retention staging a floor and then compacting) rolls
//     back every boundary, version and payload change together.
type change struct {
	tx            *bolt.Tx
	bucket        *bolt.Bucket
	namespace     model.Namespace
	initial       model.Namespace
	configWritten bool
}

// Admission mode for a change boundary.
const (
	// logicalMutation is refused while the namespace is read-only: writes,
	// deletions, retention and rollup publication.
	logicalMutation = false
	// physicalMaintenance is admitted while read-only: sealing and compaction
	// only restructure physical storage.
	physicalMaintenance = true
)

// mutate opens the boundary and runs step in the same write transaction. The
// physical flag is the read-only admission decision: physical steps (seal,
// compact, and the policy-only read-only mode toggle) are the only writes a
// read-only namespace admits. Any error from admission or from the step rolls
// the whole transaction back; nothing staged before the failure becomes
// visible.
func (e *Engine) mutate(name string, physical bool, step func(*change) error) error {
	c := &change{}
	return e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly && !physical {
			return model.Conflict("namespace is read-only")
		}
		c.tx, c.bucket, c.namespace, c.initial = tx, b, n, n
		if err := step(c); err != nil {
			return err
		}
		return c.finish()
	})
}

// Next allocates a new database sequence for a mutation receipt without
// changing the namespace source version.
func (c *change) Next() (uint64, error) {
	return volume.Next(c.tx)
}

// Current reports the database sequence the step is reading at.
func (c *change) Current() uint64 {
	return volume.Current(c.tx)
}

// PublishSource allocates a receipt sequence and makes it the namespace source
// version. The config is saved once by finish when the transaction commits.
func (c *change) PublishSource() (uint64, error) {
	seq, err := volume.Next(c.tx)
	if err != nil {
		return 0, err
	}
	c.namespace.LastSequence = seq
	return seq, nil
}

// Source is the namespace source version captured when the boundary opened;
// materializations use it as provenance instead of the receipt sequence.
func (c *change) Source() uint64 {
	return c.initial.LastSequence
}

// finish stores the namespace config when the step changed it. Steps that only
// allocate a receipt sequence (rollup publication) leave the source config
// untouched on disk.
func (c *change) finish() error {
	if c.configWritten || c.namespace == c.initial {
		return nil
	}
	if err := volume.PutJSON(c.bucket, volume.Config, c.namespace); err != nil {
		return err
	}
	c.configWritten = true
	return nil
}
