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
var SealJobs = []byte("sealjobs")
var Config = []byte("config")
var HeadCount = []byte("head_count")
var ActiveSealJob = []byte("active")

type DB struct {
	Raw *bolt.DB
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	raw, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	err = raw.Update(func(tx *bolt.Tx) error {
		root, e := tx.CreateBucketIfNotExists(Root)
		if e != nil {
			return e
		}
		_, e = root.CreateBucketIfNotExists(Spaces)
		return e
	})
	if err != nil {
		raw.Close()
		return nil, err
	}
	return &DB{Raw: raw}, nil
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
	for _, bucket := range [][]byte{Catalog, Head, Blocks, Payloads, Deletes, Rollups, SealJobs} {
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

// ReadHeadCount returns the durable head key counter. Unlike Bucket.Stats(),
// which snapshots root-page counts and ignores uncommitted writes inside a
// transaction, this counter tracks puts and deletes synchronously.
func ReadHeadCount(b *bolt.Bucket) int {
	raw := b.Get(HeadCount)
	if len(raw) != 8 {
		return 0
	}
	return int(binary.BigEndian.Uint64(raw))
}

func putHeadCount(b *bolt.Bucket, count int) error {
	if count < 0 {
		return fmt.Errorf("head count cannot be negative")
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(count))
	return b.Put(HeadCount, key)
}

// AdjustHeadCount applies a signed delta to the durable head key counter.
func AdjustHeadCount(b *bolt.Bucket, delta int) error {
	return putHeadCount(b, ReadHeadCount(b)+delta)
}
