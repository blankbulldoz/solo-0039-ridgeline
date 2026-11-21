package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
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
	e := &Engine{DB: db}
	if err := recoverMaintenance(e); err != nil {
		db.Close()
		return nil, err
	}
	return e, nil
}

// recoverMaintenance resolves a pause marker left by a process that died while a
// relabel job was in flight. The data rewrite and its completed record publish in
// one transaction, so a committed pause marker without a completed record proves
// the rewrite never applied. The attempt is recorded as failed and writes resume.
func recoverMaintenance(e *Engine) error {
	return e.DB.Raw.Update(func(tx *bolt.Tx) error {
		spaces := tx.Bucket(volume.Root).Bucket(volume.Spaces)
		return spaces.ForEach(func(name, _ []byte) error {
			ns := spaces.Bucket(name)
			if ns == nil {
				return nil
			}
			var n model.Namespace
			if err := volume.GetJSON(ns, volume.Config, &n); err != nil {
				return err
			}
			if n.Maintenance == "" {
				return nil
			}
			jobID := n.MaintenanceJob
			n.Maintenance, n.MaintenanceJob = "", ""
			if err := volume.PutJSON(ns, volume.Config, n); err != nil {
				return err
			}
			if jobID == "" {
				return nil
			}
			jobs := ns.Bucket(volume.Jobs)
			if jobs == nil || jobs.Get([]byte(jobID)) == nil {
				return nil
			}
			var job model.RelabelJob
			if err := volume.GetJSON(jobs, []byte(jobID), &job); err != nil {
				return err
			}
			if job.State == jobCompleted {
				return nil
			}
			job.State = jobFailed
			job.Completed = false
			job.Detail = "interrupted before publication; no data was rewritten"
			return volume.PutJSON(jobs, []byte(jobID), job)
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
		return err
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
		if loaded.Maintenance != "" {
			return model.Conflict("namespace is paused for label maintenance")
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
