package engine

import (
	"encoding/json"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func putBlock(b *bolt.Bucket, hexID string, seq uint64, points []model.Sample) error {
	block, payload, err := chunk.Build(hexID, seq, points)
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

func (e *Engine) Seal(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		type groupedPoints struct {
			hexID  string
			points []model.Sample
		}
		grouped := map[string]*groupedPoints{}
		err = b.Bucket(volume.Head).ForEach(func(k, v []byte) error {
			if len(k) != volume.PointKeySize {
				return model.Bad("invalid stored point key")
			}
			// Fixed-width split: the first 32 bytes are the raw series key
			// and the trailing 8 bytes are the timestamp suffix.
			seriesKey := volume.PointSeries(k)
			var point model.Sample
			if err := json.Unmarshal(v, &point); err != nil {
				return err
			}
			hexID := model.DecodeSeriesKey(seriesKey)
			group, ok := grouped[hexID]
			if !ok {
				group = &groupedPoints{hexID: hexID}
				grouped[hexID] = group
			}
			group.points = append(group.points, point)
			return nil
		})
		if err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for _, group := range grouped {
			if _, err := model.EncodeSeriesKey(group.hexID); err != nil {
				return err
			}
			if err := putBlock(b, group.hexID, seq, group.points); err != nil {
				return err
			}
			receipt.Blocks++
			receipt.Points += len(group.points)
		}
		if err := clearBucket(b, volume.Head); err != nil {
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
