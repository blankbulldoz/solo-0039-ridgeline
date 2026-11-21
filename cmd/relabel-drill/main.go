// Command relabel-drill reproduces and inspects the durable state around a
// namespace label-migration job. It exists so an operator can rehearse pause and
// recovery behaviour on a copy of a real database. It is a drill tool, not part
// of the server, and it must only run while no server holds the database file.
//
//	relabel-drill set-pause <db> <namespace> <job>
//	relabel-drill clear-pause <db> <namespace>
//	relabel-drill inspect <db> <namespace>
//	relabel-drill check-admission <db> <namespace>
//
// set-pause writes exactly what a job leaves behind if it dies after committing
// its intent but before its rewrite: a namespace pause marker and a running job
// record. No sample, catalog, block or deletion data is touched. Starting the
// server afterwards exercises startup recovery.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("usage: %s <set-pause|clear-pause|inspect> <db> <namespace> [job]", os.Args[0])
	}
	command, path, space := os.Args[1], os.Args[2], os.Args[3]
	var err error
	switch command {
	case "set-pause":
		if len(os.Args) != 5 {
			log.Fatal("set-pause requires a job identifier")
		}
		err = setPause(path, space, os.Args[4])
	case "clear-pause":
		err = clearPause(path, space)
	case "inspect":
		err = inspect(path, space)
	case "check-admission":
		err = checkAdmission(path, space)
	default:
		log.Fatalf("unknown command %q", command)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func namespaceTx(path, space string, action func(*bolt.Bucket) error) error {
	db, err := volume.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Raw.Update(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, space)
		if b == nil {
			return fmt.Errorf("namespace %q does not exist", space)
		}
		return action(b)
	})
}

func readConfig(b *bolt.Bucket) (model.Namespace, error) {
	var n model.Namespace
	return n, volume.GetJSON(b, volume.Config, &n)
}

func setPause(path, space, jobID string) error {
	return namespaceTx(path, space, func(b *bolt.Bucket) error {
		n, err := readConfig(b)
		if err != nil {
			return err
		}
		if n.Maintenance != "" {
			return fmt.Errorf("namespace is already paused for %q", n.MaintenanceJob)
		}
		job := model.RelabelJob{
			ID: jobID, State: "running", Completed: false,
			Rename:        model.RelabelRename{From: "host", To: "node"},
			ValueConflict: "reject", LabelConflict: "reject",
			StartedSequence: n.LastSequence, SourceSequence: n.LastSequence,
			Detail: "interrupted after intent was committed",
		}
		if err := volume.PutJSON(b.Bucket(volume.Jobs), []byte(jobID), job); err != nil {
			return err
		}
		n.Maintenance, n.MaintenanceJob = "relabel", jobID
		return volume.PutJSON(b, volume.Config, n)
	})
}

func clearPause(path, space string) error {
	return namespaceTx(path, space, func(b *bolt.Bucket) error {
		n, err := readConfig(b)
		if err != nil {
			return err
		}
		n.Maintenance, n.MaintenanceJob = "", ""
		return volume.PutJSON(b, volume.Config, n)
	})
}

func inspect(path, space string) error {
	db, err := volume.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	var report map[string]any
	err = db.Raw.View(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, space)
		if b == nil {
			return fmt.Errorf("namespace %q does not exist", space)
		}
		n, err := readConfig(b)
		if err != nil {
			return err
		}
		jobs := []model.RelabelJob{}
		if bucket := b.Bucket(volume.Jobs); bucket != nil {
			if err := bucket.ForEach(func(_, v []byte) error {
				var job model.RelabelJob
				if err := json.Unmarshal(v, &job); err != nil {
					return err
				}
				jobs = append(jobs, job)
				return nil
			}); err != nil {
				return err
			}
		}
		report = map[string]any{
			"namespace": n.Name, "maintenance": n.Maintenance,
			"maintenance_job": n.MaintenanceJob,
			"last_sequence":   n.LastSequence, "jobs": jobs,
		}
		return nil
	})
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// checkAdmission evaluates the production admission policy against the stored
// namespace, without writing anything. It shows whether ingestion would be
// refused while a maintenance pause is in effect.
func checkAdmission(path, space string) error {
	db, err := volume.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	var report map[string]any
	err = db.Raw.View(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, space)
		if b == nil {
			return fmt.Errorf("namespace %q does not exist", space)
		}
		n, err := readConfig(b)
		if err != nil {
			return err
		}
		err = policy.Admit(n, model.WriteRequest{Now: 100000, Points: []model.Observation{{
			Labels: model.Labels{"__name__": "cpu", "host": "host-1"},
			Time:   1000, Value: 1,
		}}})
		report = map[string]any{
			"maintenance": n.Maintenance, "maintenance_job": n.MaintenanceJob,
			"admitted": err == nil, "refusal": fmt.Sprint(err),
		}
		return nil
	})
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
