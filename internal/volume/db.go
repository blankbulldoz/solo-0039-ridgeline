package volume

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var Root = []byte("ridge-v1")
var Spaces = []byte("spaces")
var Sequence = []byte("sequence")
var Catalog = []byte("catalog")
var Head = []byte("head")
var Blocks = []byte("blocks")
var Payloads = []byte("payloads")
var Deletes = []byte("deletes")
var Rollups = []byte("rollups")
var Config = []byte("config")

type DB struct {
	Raw        *bolt.DB
	Migration  MigrationStats
	Migrated   bool
	MirrorPath string
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	raw, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	// Migration must precede every other write transaction so the
	// pre-migration mirror is byte-identical to the file as last committed by
	// a V1 process (bbolt rewrites meta pages on any writable transaction).
	stats, err := MigrateIfNeeded(raw, path)
	if err != nil {
		raw.Close()
		return nil, err
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		root, e := tx.CreateBucketIfNotExists(Root)
		if e != nil {
			return e
		}
		if _, e = root.CreateBucketIfNotExists(Spaces); e != nil {
			return e
		}
		if root.Get(StorageVersion) == nil {
			marker := make([]byte, 8)
			binary.BigEndian.PutUint64(marker, StorageVersionV2)
			return root.Put(StorageVersion, marker)
		}
		return nil
	})
	if err != nil {
		raw.Close()
		return nil, err
	}
	db := &DB{Raw: raw, Migration: stats, MirrorPath: path + MirrorSuffix}
	db.Migrated = stats.Namespaces > 0
	return db, nil
}

func (d *DB) Close() error {
	return d.Raw.Close()
}

func Namespace(tx *bolt.Tx, name string) *bolt.Bucket {
	return tx.Bucket(Root).Bucket(Spaces).Bucket([]byte(name))
}

func CreateNamespace(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	ns, err := tx.Bucket(Root).Bucket(Spaces).CreateBucket([]byte(name))
	if err != nil {
		return nil, err
	}
	for _, bucket := range [][]byte{Catalog, Head, Blocks, Payloads, Deletes, Rollups} {
		if _, err = ns.CreateBucket(bucket); err != nil {
			return nil, err
		}
	}
	return ns, nil
}

func Next(tx *bolt.Tx) (uint64, error) {
	root := tx.Bucket(Root)
	value := Current(tx) + 1
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, value)
	return value, root.Put(Sequence, key)
}

func Current(tx *bolt.Tx) uint64 {
	v := tx.Bucket(Root).Get(Sequence)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func PutJSON(b *bolt.Bucket, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

func GetJSON(b *bolt.Bucket, key []byte, value any) error {
	data := b.Get(key)
	if data == nil {
		return fmt.Errorf("missing stored record %q", key)
	}
	return json.Unmarshal(data, value)
}
