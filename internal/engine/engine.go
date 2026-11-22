package engine

import (
	"encoding/json"
	"sync"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

type Engine struct {
	DB *volume.DB

	gateMu sync.RWMutex
	gates  map[string]*spaceGate
}

func Open(path string) (*Engine, error) {
	db, err := volume.Open(path)
	if err != nil {
		return nil, err
	}
	e := &Engine{DB: db, gates: map[string]*spaceGate{}}
	if err := e.recover(); err != nil {
		db.Close()
		return nil, err
	}
	return e, nil
}

// recover loads every namespace into the process-local lifecycle table.
//
// A request cannot survive a process restart, so a namespace reopened in the
// draining state has no in-flight logical work left to wait for. Recovery does
// not silently complete the drain, however: completing it is a logical state
// change with its own commit sequence. It records that this process did not
// start the drain, which is what makes the observable status report the
// restart truthfully; an operator can then complete or cancel deliberately.
func (e *Engine) recover() error {
	return e.DB.Raw.View(func(tx *bolt.Tx) error {
		spaces := tx.Bucket(volume.Root).Bucket(volume.Spaces)
		return spaces.ForEach(func(name, _ []byte) error {
			b := spaces.Bucket(name)
			var n model.Namespace
			if err := volume.GetJSON(b, volume.Config, &n); err != nil {
				return err
			}
			n.Normalize()
			e.registerGate(string(name), n.EffectiveState(), false)
			return nil
		})
	})
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

func loadNamespace(tx *bolt.Tx, name string) (*bolt.Bucket, model.Namespace, error) {
	b := volume.Namespace(tx, name)
	var n model.Namespace
	if b == nil {
		return nil, n, model.Missing("namespace does not exist")
	}
	if err := volume.GetJSON(b, volume.Config, &n); err != nil {
		return nil, n, err
	}
	return b, n, nil
}

func catalog(b *bolt.Bucket) ([]model.Series, error) {
	out := make([]model.Series, 0)
	err := b.Bucket(volume.Catalog).ForEach(func(k, v []byte) error {
		var s model.Series
		if err := json.Unmarshal(v, &s); err != nil {
			return err
		}
		out = append(out, s)
		return nil
	})
	return out, err
}

func (e *Engine) Create(n model.Namespace) (model.Namespace, error) {
	if err := model.ValidateNamespace(n); err != nil {
		return n, err
	}
	n.Floor, n.Watermark, n.HasWatermark = model.MinTime, 0, false
	n.State = model.StateWritable
	n.ReadOnly = false
	n.DrainSequence, n.DrainCompletedSequence, n.DrainCanceledSequence = 0, 0, 0
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		if volume.Namespace(tx, n.Name) != nil {
			return model.Conflict("namespace already exists")
		}
		b, err := volume.CreateNamespace(tx, n.Name)
		if err != nil {
			return err
		}
		n.LastSequence, err = volume.Next(tx)
		if err != nil {
			return err
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	if err != nil {
		return n, err
	}
	e.registerGate(n.Name, n.EffectiveState(), true)
	return n, nil
}

// ReadOnly is retained for callers of the original boolean mode API.
func (e *Engine) ReadOnly(name string, readOnly bool) (model.NamespaceView, error) {
	return e.SetReadOnly(name, readOnly)
}
