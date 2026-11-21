package volume

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// Open must upgrade a database created before exemplars existed by adding the
// exemplar bucket to every namespace, without disturbing stored data.
func TestOpenAddsExemplarBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("bolt open: %v", err)
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket(Root)
		if err != nil {
			return err
		}
		spaces, err := root.CreateBucket(Spaces)
		if err != nil {
			return err
		}
		ns, err := spaces.CreateBucket([]byte("legacy"))
		if err != nil {
			return err
		}
		for _, bucket := range [][]byte{Catalog, Head, Blocks, Payloads, Deletes, Rollups} {
			if _, err := ns.CreateBucket(bucket); err != nil {
				return err
			}
		}
		return ns.Put(Config, []byte(`{"name":"legacy"}`))
	})
	if err != nil {
		t.Fatalf("seed legacy db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer db.Close()
	err = db.Raw.View(func(tx *bolt.Tx) error {
		ns := Namespace(tx, "legacy")
		if ns == nil {
			t.Fatal("namespace missing after open")
		}
		if ns.Bucket(Exemplars) == nil {
			t.Fatal("exemplar bucket was not created for legacy namespace")
		}
		if ns.Bucket(Catalog) == nil {
			t.Fatal("existing bucket was disturbed")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}

	// Opening again must stay idempotent.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Open(path); err != nil {
		t.Fatalf("second open failed: %v", err)
	}
}

func TestExemplarKeyRoundTrip(t *testing.T) {
	series := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, ts := range []int64{-(1 << 52), -1, 0, 1, 1 << 52} {
		key := ExemplarKey(series, ts, 42, 7)
		if len(key) != 84 {
			t.Fatalf("unexpected key length %d", len(key))
		}
		gotTS, err := ExemplarKeyTime(key)
		if err != nil || gotTS != ts {
			t.Fatalf("time round trip: got %d want %d err %v", gotTS, ts, err)
		}
		gotSeries, err := ExemplarKeySeries(key)
		if err != nil || gotSeries != series {
			t.Fatalf("series round trip failed: %v", err)
		}
	}
	// Ordering: time then sequence then offset, including negative time.
	if ExemplarKey(series, 1, 1, 0) != nil &&
		string(ExemplarKey(series, -2, 1, 0)) >= string(ExemplarKey(series, -1, 1, 0)) {
		t.Fatal("negative timestamps must sort before positive ones")
	}
	if string(ExemplarKey(series, 5, 1, 0)) >= string(ExemplarKey(series, 5, 2, 0)) {
		t.Fatal("sequence must order exemplars at the same time")
	}
	if string(ExemplarKey(series, 5, 2, 0)) >= string(ExemplarKey(series, 5, 2, 1)) {
		t.Fatal("offset must order exemplars at the same time and sequence")
	}
}
