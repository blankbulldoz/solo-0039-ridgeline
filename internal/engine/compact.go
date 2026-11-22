package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"
)

// compactStep rebuilds one block per visible series inside an existing change
// boundary. It is a reusable internal maintenance step: it neither decides
// read-only admission nor allocates sequences or saves the namespace config
// itself, so it can be composed by retention without duplicating publication
// rules.
func compactStep(c *change) (model.Maintenance, error) {
	b, n := c.bucket, c.namespace
	// Maintenance is bounded by the same scan policy as user queries.
	raw, err := scan(b, n, model.Query{Start: model.MinTime, End: model.MaxTime + 1}, c.Current())
	if err != nil {
		return model.Maintenance{}, err
	}
	for _, bucket := range [][]byte{volume.Head, volume.Blocks, volume.Payloads, volume.Deletes} {
		if err := clearBucket(b, bucket); err != nil {
			return model.Maintenance{}, err
		}
	}
	seq, err := c.PublishSource()
	if err != nil {
		return model.Maintenance{}, err
	}
	receipt := model.Maintenance{Sequence: seq, Floor: n.Floor}
	for _, s := range raw.Series {
		if err := putBlock(b, s.Series.ID, seq, s.Points); err != nil {
			return receipt, err
		}
		receipt.Blocks++
		receipt.Points += len(s.Points)
	}
	return receipt, nil
}

func (e *Engine) Compact(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	// Compaction is physical maintenance: read-only namespaces still admit it.
	err := e.mutate(name, physicalMaintenance, func(c *change) error {
		var err error
		receipt, err = compactStep(c)
		return err
	})
	return receipt, err
}

func (e *Engine) Retain(name string, now int64) (model.Maintenance, error) {
	var receipt model.Maintenance
	// Retention changes a logical boundary, so it is rejected while read-only.
	// The floor decision and the physical rebuild run inside one boundary: if
	// the rebuild fails, the transaction rolls back and neither the new floor
	// nor a new source version survives.
	err := e.mutate(name, logicalMutation, func(c *change) error {
		floor, err := policy.RetentionFloor(c.namespace, now)
		if err != nil {
			return err
		}
		c.namespace.Floor = floor
		receipt, err = compactStep(c)
		return err
	})
	return receipt, err
}
