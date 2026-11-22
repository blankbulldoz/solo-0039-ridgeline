package volume

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"ridgeline/internal/model"

	bolt "go.etcd.io/bbolt"
)

// Storage layout versions.
//
// V1 persisted the series identifier as a 64-character lowercase hexadecimal
// string: catalog keys were 64 bytes and head point keys 72 bytes (64 bytes
// of hex ID followed by an 8-byte timestamp). V2 persists the raw 32-byte
// digest: catalog keys are 32 bytes and head point keys 40 bytes. HTTP
// payloads keep using the 64-character hexadecimal form in every version.
const (
	StorageVersionV1 uint64 = 1
	StorageVersionV2 uint64 = 2
)

// StorageVersion is the root-level version marker key.
var StorageVersion = []byte("storage_version")

// MirrorSuffix names the untouched pre-migration copy kept beside a database.
const MirrorSuffix = ".pre-v2-mirror"

// MigrationStats reports what a V1 -> V2 migration touched.
type MigrationStats struct {
	Namespaces   int `json:"namespaces"`
	Series       int `json:"series"`
	EmptySeries  int `json:"empty_series"`
	HeadPoints   int `json:"head_points"`
	Blocks       int `json:"blocks"`
	Tombstones   int `json:"tombstones"`
	TombTargets  int `json:"tombstone_targets"`
	Rollups      int `json:"rollups"`
	OrphanBlocks int `json:"orphan_blocks"`
}

func readStorageVersion(tx *bolt.Tx) (uint64, bool) {
	v := tx.Bucket(Root).Get(StorageVersion)
	if len(v) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(v), true
}

// MigrateIfNeeded upgrades a V1 database in a single write transaction and
// stamps the V2 marker. A V2 database is returned untouched. The whole
// migration is atomic: bbolt copy-on-write means the original file content
// stays valid until the transaction commits.
func MigrateIfNeeded(raw *bolt.DB, path string) (MigrationStats, error) {
	var stats MigrationStats
	var version uint64
	empty := false
	if err := raw.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(Root)
		if root == nil {
			version = StorageVersionV1 // empty file; Open stamps V2 after migration
			empty = true
			return nil
		}
		if v, ok := readStorageVersion(tx); ok {
			version = v
		} else {
			version = StorageVersionV1
		}
		return nil
	}); err != nil {
		return stats, err
	}
	if version == StorageVersionV2 {
		return stats, nil
	}
	if version > StorageVersionV2 {
		return stats, fmt.Errorf("database storage version %d is newer than this build (max %d)", version, StorageVersionV2)
	}

	// File-level safety net before any write transaction begins. An empty
	// file holds no user data, so no mirror is required.
	if !empty {
		if _, err := createPreMigrationMirror(path); err != nil {
			return stats, fmt.Errorf("pre-migration mirror failed: %w", err)
		}
	}

	err := raw.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(Root)
		if root == nil {
			// Empty database: nothing to migrate; Open creates the buckets
			// and stamps V2. Do not create a mirror for an empty file.
			return nil
		}
		spaces := root.Bucket(Spaces)
		if spaces == nil {
			return nil
		}
		if err := spaces.ForEach(func(name, nsValue []byte) error {
			if nsValue != nil {
				return nil // namespace entries are buckets
			}
			ns := spaces.Bucket(name)
			migrated, err := migrateNamespace(ns)
			if err != nil {
				return fmt.Errorf("namespace %q: %w", name, err)
			}
			stats.Namespaces++
			stats.Series += migrated.Series
			stats.EmptySeries += migrated.EmptySeries
			stats.HeadPoints += migrated.HeadPoints
			stats.Blocks += migrated.Blocks
			stats.Tombstones += migrated.Tombstones
			stats.TombTargets += migrated.TombTargets
			stats.Rollups += migrated.Rollups
			stats.OrphanBlocks += migrated.OrphanBlocks
			return nil
		}); err != nil {
			return err
		}
		// One root-level marker for the whole database, written only after
		// every namespace has converted successfully.
		marker := make([]byte, 8)
		binary.BigEndian.PutUint64(marker, StorageVersionV2)
		return root.Put(StorageVersion, marker)
	})
	if err != nil {
		// The transaction rolled back; the live file is still V1 and the
		// pre-migration mirror (path + MirrorSuffix) is an additional
		// byte-identical fallback.
		return stats, err
	}
	return stats, nil
}

func migrateNamespace(ns *bolt.Bucket) (MigrationStats, error) {
	var stats MigrationStats

	// 1. Catalog: 64-byte hex keys -> 32-byte raw digest keys. Values keep
	//    their hex "id" field because that is the external HTTP identifier.
	catalogBucket := ns.Bucket(Catalog)
	hexToRaw := make(map[string][]byte)
	if err := catalogBucket.ForEach(func(k, v []byte) error {
		if len(k) != 64 {
			return fmt.Errorf("catalog key has unexpected length %d", len(k))
		}
		hexID := string(k)
		raw, err := model.EncodeSeriesKey(hexID)
		if err != nil {
			return fmt.Errorf("catalog key %q: %w", hexID, err)
		}
		var series model.Series
		if err := json.Unmarshal(v, &series); err != nil {
			return fmt.Errorf("catalog record %q: %w", hexID, err)
		}
		if series.ID != hexID {
			return fmt.Errorf("catalog record %q carries mismatched id %q", hexID, series.ID)
		}
		if existing := catalogBucket.Get(raw); existing != nil {
			return fmt.Errorf("raw catalog key collision for %q", hexID)
		}
		hexToRaw[hexID] = raw
		return nil
	}); err != nil {
		return stats, err
	}
	for hexID, raw := range hexToRaw {
		value := catalogBucket.Get([]byte(hexID))
		if err := catalogBucket.Put(raw, value); err != nil {
			return stats, err
		}
		if err := catalogBucket.Delete([]byte(hexID)); err != nil {
			return stats, err
		}
		stats.Series++
	}

	// 2. Head: 72-byte keys (64 hex ID bytes + 8 time bytes) -> 40-byte keys
	//    (32 raw ID bytes + 8 time bytes). The split is fixed-width: only the
	//    trailing 8 bytes are decoded as a timestamp, so time bytes can never
	//    be absorbed into the identifier.
	headBucket := ns.Bucket(Head)
	type headMove struct {
		oldKey []byte
		newKey []byte
		value  []byte
	}
	// busy tracks catalog IDs that own at least one head point or block; a
	// catalog entry absent from this set is an empty series whose identity the
	// migration must still preserve.
	busy := make(map[string]bool)
	moves := make([]headMove, 0)
	if err := headBucket.ForEach(func(k, v []byte) error {
		if len(k) != legacyKeySize {
			return fmt.Errorf("head key has unexpected length %d", len(k))
		}
		hexID := string(k[:legacyHexIDSize])
		raw, ok := hexToRaw[hexID]
		if !ok {
			return fmt.Errorf("head point references unknown series %q", hexID)
		}
		timestamp, err := LegacyPointTime(k)
		if err != nil {
			return err
		}
		if !model.ValidTime(timestamp) {
			return fmt.Errorf("head point outside supported time range: %d", timestamp)
		}
		var point model.Sample
		if err := json.Unmarshal(v, &point); err != nil {
			return fmt.Errorf("head point for %q: %w", hexID, err)
		}
		moves = append(moves, headMove{
			oldKey: append([]byte(nil), k...),
			newKey: PointKey(raw, timestamp),
			value:  append([]byte(nil), v...),
		})
		busy[hexID] = true
		return nil
	}); err != nil {
		return stats, err
	}
	for _, move := range moves {
		if err := headBucket.Put(move.newKey, move.value); err != nil {
			return stats, err
		}
		if err := headBucket.Delete(move.oldKey); err != nil {
			return stats, err
		}
		stats.HeadPoints++
	}

	// 3. Block metadata and payloads. Block IDs and the JSON series_id stay
	//    hexadecimal because they are part of the external block inventory and
	//    payload keys; migration verifies every block resolves to a catalog
	//    series so scans can locate it through the raw 32-byte catalog key.
	if err := ns.Bucket(Blocks).ForEach(func(_, v []byte) error {
		var block model.Block
		if err := json.Unmarshal(v, &block); err != nil {
			return err
		}
		if _, ok := hexToRaw[block.SeriesID]; !ok {
			stats.OrphanBlocks++
			return fmt.Errorf("block %q references unknown series %q", block.ID, block.SeriesID)
		}
		if ns.Bucket(Payloads).Get([]byte(block.ID)) == nil {
			return fmt.Errorf("block %q is missing its payload", block.ID)
		}
		stats.Blocks++
		busy[block.SeriesID] = true
		return nil
	}); err != nil {
		return stats, err
	}

	// 4. Deletion targets. Tombstone JSON and its key remain hexadecimal wire
	//    data; each referenced series must resolve to a catalog raw key.
	if err := ns.Bucket(Deletes).ForEach(func(_, v []byte) error {
		var tomb model.Tombstone
		if err := json.Unmarshal(v, &tomb); err != nil {
			return err
		}
		for _, hexID := range tomb.Series {
			if _, ok := hexToRaw[hexID]; !ok {
				return fmt.Errorf("tombstone %q references unknown series %q", tomb.ID, hexID)
			}
			stats.TombTargets++
		}
		stats.Tombstones++
		return nil
	}); err != nil {
		return stats, err
	}

	// 5. Rollups. Stored materializations reference series through label
	//    matchers and source-sequence provenance, never through stored series
	//    keys, so they need no rewrite; validate that each record still parses.
	if err := ns.Bucket(Rollups).ForEach(func(_, v []byte) error {
		var materialization model.Materialization
		if err := json.Unmarshal(v, &materialization); err != nil {
			return err
		}
		stats.Rollups++
		return nil
	}); err != nil {
		return stats, err
	}

	// Empty series carry catalog identity but no head points or blocks; the
	// catalog rewrite already preserved them, and counting them lets operators
	// confirm identity retention after migration.
	for hexID := range hexToRaw {
		if !busy[hexID] {
			stats.EmptySeries++
		}
	}
	return stats, nil
}

// createPreMigrationMirror copies the closed-consistent bbolt file beside the
// database. An existing mirror is never overwritten: the first pre-migration
// image is the authoritative rollback source.
func createPreMigrationMirror(path string) (string, error) {
	mirror := path + MirrorSuffix
	if _, err := os.Stat(mirror); err == nil {
		return mirror, nil
	}
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()
	tmp := mirror + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, mirror); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return mirror, nil
}
