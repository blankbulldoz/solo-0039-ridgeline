package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

type Engine struct {
	DB *volume.DB
}

func Open(path string) (*Engine, error) {
	db, err := volume.Open(path)
	if err != nil {
		return nil, err
	}
	return &Engine{DB: db}, nil
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
	// Configs written before per-series lateness existed have no late_mode
	// field. They keep the original namespace-wide behavior.
	if n.LateMode == "" {
		n.LateMode = model.LateModeNamespace
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
	if n.LateMode == "" {
		n.LateMode = model.LateModeNamespace
	}
	if err := model.ValidateNamespace(n); err != nil {
		return n, err
	}
	n.Floor, n.Watermark, n.HasWatermark = model.MinTime, 0, false
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
	return n, err
}

func (e *Engine) Namespace(name string) (model.Namespace, error) {
	var n model.Namespace
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		_, loaded, err := loadNamespace(tx, name)
		n = loaded
		if err == nil {
			n.LateMode = policy.EffectiveLateMode(n)
		}
		return err
	})
	return n, err
}

// SetLateMode switches how lateness is evaluated without changing the
// namespace watermark, the retention floor or any stored sample. Switching to
// series mode first seeds a watermark for every existing series that lacks
// one, using the namespace watermark and the switching sequence. Without that
// seed a pre-existing series would look brand new and could accept history
// that the namespace watermark had already aged out. Per-series watermarks
// keep accumulating in namespace mode, so repeated switches stay deterministic.
func (e *Engine) SetLateMode(name, mode string) (model.Namespace, error) {
	if err := model.ValidateLateMode(mode); err != nil {
		return model.Namespace{}, err
	}
	var n model.Namespace
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if err := volume.EnsureWatermarks(b); err != nil {
			return err
		}
		n = loaded
		n.LastSequence, err = volume.Next(tx)
		if err != nil {
			return err
		}
		if mode == model.LateModeSeries && n.HasWatermark {
			stored, err := readWatermarks(b)
			if err != nil {
				return err
			}
			series, err := catalog(b)
			if err != nil {
				return err
			}
			for _, s := range series {
				if _, ok := stored[s.ID]; ok {
					continue
				}
				seed := model.SeriesWatermark{SeriesID: s.ID, Time: n.Watermark, Sequence: n.LastSequence}
				if err := volume.PutJSON(b.Bucket(volume.Watermarks), []byte(s.ID), seed); err != nil {
					return err
				}
			}
		}
		n.LateMode = mode
		return volume.PutJSON(b, volume.Config, n)
	})
	return n, err
}

func (e *Engine) ReadOnly(name string, readOnly bool) (model.Namespace, error) {
	var n model.Namespace
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		n = loaded
		n.ReadOnly = readOnly
		n.LastSequence, err = volume.Next(tx)
		if err != nil {
			return err
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return n, err
}
