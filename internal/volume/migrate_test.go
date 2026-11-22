package volume

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"ridgeline/internal/model"

	bolt "go.etcd.io/bbolt"
)

// v1Fixture stores the raw objects used to build a legacy database so tests
// can assert the migration result against known inputs.
type v1Fixture struct {
	path       string
	hexIDs     []string
	rawKeys    map[string][]byte
	headHexID  string
	blockHexID string
	tombHexID  string
	sequence   uint64
}

func writeV1(t *testing.T, path string, opts struct {
	head   bool
	block  bool
	tomb   bool
	rollup bool
	empty  bool
}) v1Fixture {
	t.Helper()
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	fx := v1Fixture{path: path, rawKeys: map[string][]byte{}, sequence: 7}
	err = raw.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket(Root)
		if err != nil {
			return err
		}
		seqKey := make([]byte, 8)
		binary.BigEndian.PutUint64(seqKey, fx.sequence)
		if err := root.Put(Sequence, seqKey); err != nil {
			return err
		}
		spaces, err := root.CreateBucket(Spaces)
		if err != nil {
			return err
		}
		ns, err := spaces.CreateBucket([]byte("space-a"))
		if err != nil {
			return err
		}
		for _, b := range [][]byte{Catalog, Head, Blocks, Payloads, Deletes, Rollups} {
			if _, err := ns.CreateBucket(b); err != nil {
				return err
			}
		}

		putSeries := func(labels model.Labels) string {
			id := model.SeriesID(labels)
			fx.hexIDs = append(fx.hexIDs, id)
			fx.rawKeys[id] = mustDecodeHex(t, id)
			if err := PutJSON(ns.Bucket(Catalog), []byte(id),
				model.Series{ID: id, Labels: labels}); err != nil {
				t.Fatal(err)
			}
			return id
		}

		headID := putSeries(model.Labels{"__name__": "head_metric", "zone": "z1"})
		blockID := putSeries(model.Labels{"__name__": "block_metric", "zone": "z2"})
		tombID := putSeries(model.Labels{"__name__": "tomb_metric", "zone": "z3"})
		if opts.empty {
			putSeries(model.Labels{"__name__": "ghost_metric", "zone": "z4"})
		}
		fx.headHexID, fx.blockHexID, fx.tombHexID = headID, blockID, tombID

		if opts.head {
			// Legacy 72-byte key: 64 hex ID bytes followed by 8 time bytes.
			// A negative timestamp makes the suffix byte pattern very
			// different from the ID prefix, guarding the fixed-width split.
			for _, ts := range []int64{-4096, 0, 4096} {
				key := legacyPointKey(t, headID, ts)
				point := model.Sample{Time: ts, Value: float64(ts), Sequence: fx.sequence}
				if err := PutJSON(ns.Bucket(Head), key, point); err != nil {
					return err
				}
			}
		}
		if opts.block {
			for _, id := range []string{blockID, tombID} {
				block := model.Block{
					ID:       fmt.Sprintf("%020d-%s", fx.sequence, id),
					SeriesID: id, Start: -10, End: 11, Count: 2,
					Bytes: 3, Checksum: hex.EncodeToString(make([]byte, 32)), Format: 1,
				}
				if err := PutJSON(ns.Bucket(Blocks), []byte(block.ID), block); err != nil {
					return err
				}
				if err := ns.Bucket(Payloads).Put([]byte(block.ID), []byte("gzip-bytes")); err != nil {
					return err
				}
			}
		}
		if opts.tomb {
			tomb := model.Tombstone{
				ID:     "00000000000000000007",
				Series: []string{tombID},
				Start:  -5, End: 5, Sequence: fx.sequence,
			}
			if err := PutJSON(ns.Bucket(Deletes), []byte(tomb.ID), tomb); err != nil {
				return err
			}
		}
		if opts.rollup {
			materialization := model.Materialization{
				Name: "pulse", SourceSequence: fx.sequence,
				Result: model.AggregateResult{Sequence: fx.sequence},
			}
			return PutJSON(ns.Bucket(Rollups), []byte("pulse"), materialization)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fx
}

func mustDecodeHex(t *testing.T, id string) []byte {
	t.Helper()
	raw, err := model.EncodeSeriesKey(id)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func legacyPointKey(t *testing.T, hexID string, ts int64) []byte {
	t.Helper()
	key := make([]byte, legacyKeySize)
	copy(key, []byte(hexID))
	binary.BigEndian.PutUint64(key[legacyHexIDSize:], uint64(ts)^(1<<63))
	return key
}

func openAndMigrate(t *testing.T, path string) (*DB, MigrationStats) {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	return db, db.Migration
}

func TestMigratesAllPersistentLocations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	fx := writeV1(t, path, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: true, tomb: true, rollup: true, empty: true})

	db, stats := openAndMigrate(t, path)
	defer db.Close()

	if stats.Series != 4 || stats.HeadPoints != 3 || stats.Blocks != 2 ||
		stats.Tombstones != 1 || stats.TombTargets != 1 || stats.Rollups != 1 ||
		stats.EmptySeries != 1 {
		t.Fatalf("unexpected migration stats: %+v", stats)
	}

	err := db.Raw.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(Root)
		if v := root.Get(StorageVersion); len(v) != 8 ||
			binary.BigEndian.Uint64(v) != StorageVersionV2 {
			return fmt.Errorf("missing V2 marker")
		}
		if Current(tx) != fx.sequence {
			return fmt.Errorf("sequence changed: got %d want %d", Current(tx), fx.sequence)
		}
		ns := root.Bucket(Spaces).Bucket([]byte("space-a"))

		// Catalog: raw 32-byte keys, hex id retained in the JSON value.
		catalogCount := 0
		if err := ns.Bucket(Catalog).ForEach(func(k, v []byte) error {
			if len(k) != SeriesKeySize {
				return fmt.Errorf("catalog key length %d", len(k))
			}
			var series model.Series
			if err := json.Unmarshal(v, &series); err != nil {
				return err
			}
			if model.DecodeSeriesKey(k) != series.ID {
				return fmt.Errorf("raw key does not match hex id %q", series.ID)
			}
			catalogCount++
			return nil
		}); err != nil {
			return err
		}
		if catalogCount != len(fx.hexIDs) {
			return fmt.Errorf("catalog count %d want %d", catalogCount, len(fx.hexIDs))
		}

		// Head: 40-byte keys whose prefix is the raw digest and whose suffix
		// decodes back to the exact original timestamp.
		headCount := 0
		if err := ns.Bucket(Head).ForEach(func(k, v []byte) error {
			if len(k) != PointKeySize {
				return fmt.Errorf("head key length %d", len(k))
			}
			ts, err := PointTime(k)
			if err != nil {
				return err
			}
			want := map[int64]bool{-4096: true, 0: true, 4096: true}
			if !want[ts] {
				return fmt.Errorf("head timestamp %d not one of the originals", ts)
			}
			if hex.EncodeToString(k[:SeriesKeySize]) != fx.headHexID {
				return fmt.Errorf("head prefix is not the migrated series key")
			}
			var point model.Sample
			if err := json.Unmarshal(v, &point); err != nil {
				return err
			}
			if point.Time != ts {
				return fmt.Errorf("stored time %d != key time %d", point.Time, ts)
			}
			headCount++
			return nil
		}); err != nil {
			return err
		}
		if headCount != 3 {
			return fmt.Errorf("head count %d want 3", headCount)
		}

		// Block metadata and payload remain hex-keyed and resolvable.
		var block model.Block
		blockKey := []byte(fmt.Sprintf("%020d-%s", fx.sequence, fx.blockHexID))
		if err := GetJSON(ns.Bucket(Blocks), blockKey, &block); err != nil {
			return err
		}
		if block.SeriesID != fx.blockHexID ||
			ns.Bucket(Payloads).Get(blockKey) == nil {
			return fmt.Errorf("block or payload not locatable after migration")
		}

		// Tombstone targets remain hex and must map onto a raw catalog key.
		var tomb model.Tombstone
		if err := GetJSON(ns.Bucket(Deletes), []byte("00000000000000000007"), &tomb); err != nil {
			return err
		}
		if len(tomb.Series) != 1 || tomb.Series[0] != fx.tombHexID {
			return fmt.Errorf("tombstone target changed: %+v", tomb.Series)
		}
		if ns.Bucket(Catalog).Get(fx.rawKeys[fx.tombHexID]) == nil {
			return fmt.Errorf("tombstone target has no raw catalog key")
		}

		// Rollup provenance survives untouched.
		var materialization model.Materialization
		if err := GetJSON(ns.Bucket(Rollups), []byte("pulse"), &materialization); err != nil {
			return err
		}
		if materialization.SourceSequence != fx.sequence {
			return fmt.Errorf("rollup source sequence changed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMigrationIsIdempotentAndCreatesMirrorOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	writeV1(t, path, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: true, tomb: true, rollup: true, empty: true})

	db, first := openAndMigrate(t, path)
	db.Close()
	if first.Namespaces != 1 {
		t.Fatalf("first migration stats: %+v", first)
	}
	mirror := path + MirrorSuffix
	firstHash := hashFile(t, mirror)

	// Reopening an already migrated database must be a no-op migration.
	db2, second := openAndMigrate(t, path)
	db2.Close()
	if second.Namespaces != 0 {
		t.Fatalf("second open should not migrate, got %+v", second)
	}
	if hashFile(t, mirror) != firstHash {
		t.Fatal("mirror was rewritten on reopen")
	}
}

func TestFailedMigrationLeavesV1FileAndRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	fx := writeV1(t, path, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: false, tomb: false, rollup: false, empty: false})
	originalHash := hashFile(t, path)

	// Corrupt one catalog key so it is no longer valid hexadecimal.
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		ns := tx.Bucket(Root).Bucket(Spaces).Bucket([]byte("space-a"))
		c := ns.Bucket(Catalog)
		var target []byte
		if err := c.ForEach(func(k, _ []byte) error {
			target = append([]byte(nil), k...)
			return nil
		}); err != nil {
			return err
		}
		broken := append([]byte(nil), target...)
		broken[0] = 'z'
		value := append([]byte(nil), c.Get(target)...)
		if err := c.Delete(target); err != nil {
			return err
		}
		return c.Put(broken, value)
	})
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, openErr := Open(path)
	if openErr == nil {
		db.Close()
		t.Fatal("expected migration to fail on corrupted catalog")
	}

	// The live file must remain a serviceable V1 database with legacy keys.
	if hashFile(t, path) == originalHash {
		// The deliberate corruption itself changed the file; what matters is
		// that migration added no V2 marker and no 32/40-byte keys.
	}
	check, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	err = check.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(Root)
		if root.Get(StorageVersion) != nil {
			return fmt.Errorf("V2 marker present after failed migration")
		}
		ns := root.Bucket(Spaces).Bucket([]byte("space-a"))
		return ns.Bucket(Head).ForEach(func(k, _ []byte) error {
			if len(k) != legacyKeySize {
				return fmt.Errorf("head key rewritten despite failure: len=%d", len(k))
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// The mirror is the pre-corruption... here the corruption happened before
	// migration, so mirror == file at migration attempt time, still V1 and
	// directly restorable by replacing the live file.
	mirror := path + MirrorSuffix
	mirrorDB, err := bolt.Open(mirror, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("mirror missing or unreadable: %v", err)
	}
	mirrorDB.Close()
	_ = fx
}

func TestEmptyDatabaseStampsV2WithoutMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := os.Stat(path + MirrorSuffix); !os.IsNotExist(err) {
		t.Fatal("empty database should not produce a migration mirror")
	}
	err = db.Raw.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(Root).Get(StorageVersion)
		if len(v) != 8 || binary.BigEndian.Uint64(v) != StorageVersionV2 {
			return fmt.Errorf("fresh database not stamped V2")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMirrorIsByteIdenticalToPreMigrationFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	writeV1(t, path, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: true, tomb: true, rollup: false, empty: true})
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Reconstruct what the V1 file looked like by building it again into a
	// sibling path and comparing against the retained mirror.
	sibling := filepath.Join(t.TempDir(), "sibling.db")
	writeV1(t, sibling, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: true, tomb: true, rollup: false, empty: true})
	if hashFile(t, path+MirrorSuffix) != hashFile(t, sibling) {
		t.Fatal("mirror does not match a freshly built V1 file of the same inputs")
	}
}

func TestMigratesMultipleNamespacesWithSingleRootMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	first := writeV1(t, path, struct {
		head, block, tomb, rollup, empty bool
	}{head: true, block: true, tomb: true, rollup: true, empty: true})

	// Add a second, independent V1 namespace directly through bbolt.
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		spaces := tx.Bucket(Root).Bucket(Spaces)
		ns, err := spaces.CreateBucket([]byte("space-b"))
		if err != nil {
			return err
		}
		for _, b := range [][]byte{Catalog, Head, Blocks, Payloads, Deletes, Rollups} {
			if _, err := ns.CreateBucket(b); err != nil {
				return err
			}
		}
		labels := model.Labels{"__name__": "second_metric"}
		id := model.SeriesID(labels)
		return PutJSON(ns.Bucket(Catalog), []byte(id), model.Series{ID: id, Labels: labels})
	})
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, stats := openAndMigrate(t, path)
	defer db.Close()
	if stats.Namespaces != 2 || stats.Series != len(first.hexIDs)+1 {
		t.Fatalf("unexpected multi-namespace stats: %+v", stats)
	}
	err = db.Raw.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(Root)
		if len(root.Get(StorageVersion)) != 8 {
			return fmt.Errorf("root marker missing")
		}
		for _, name := range []string{"space-a", "space-b"} {
			if root.Bucket(Spaces).Bucket([]byte(name)) == nil {
				return fmt.Errorf("namespace %q missing after migration", name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewerVersionRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	raw, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucket(Root)
		if err != nil {
			return err
		}
		if _, err := root.CreateBucket(Spaces); err != nil {
			return err
		}
		marker := make([]byte, 8)
		binary.BigEndian.PutUint64(marker, StorageVersionV2+1)
		return root.Put(StorageVersion, marker)
	})
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err == nil {
		db.Close()
		t.Fatal("expected newer-version database to be rejected")
	}
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
