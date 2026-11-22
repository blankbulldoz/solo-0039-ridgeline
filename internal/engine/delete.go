package engine

import (
	"fmt"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Delete(name string, q model.Query) (model.Tombstone, error) {
	var tomb model.Tombstone
	if err := model.ValidateQuery(q); err != nil {
		return tomb, err
	}
	selector, err := selectseries.Compile(q.Matchers)
	if err != nil {
		return tomb, err
	}
	err = e.mutate(name, logicalMutation, func(c *change) error {
		b := c.bucket
		all, err := catalog(b)
		if err != nil {
			return err
		}
		seq, err := c.PublishSource()
		if err != nil {
			return err
		}
		tomb = model.Tombstone{
			ID: fmt.Sprintf("%020d", seq), Start: q.Start, End: q.End,
			Sequence: seq, Series: []string{},
		}
		for _, s := range selectseries.Pick(all, selector) {
			tomb.Series = append(tomb.Series, s.ID)
		}
		return volume.PutJSON(b.Bucket(volume.Deletes), []byte(tomb.ID), tomb)
	})
	return tomb, err
}

func (e *Engine) Deletions(name string) ([]model.Tombstone, error) {
	var tombstones []model.Tombstone
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		tombstones, err = readTombstones(b)
		return err
	})
	return tombstones, err
}
