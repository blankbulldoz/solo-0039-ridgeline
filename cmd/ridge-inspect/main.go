// Command ridge-inspect reports the on-disk storage shape of a Ridgeline
// database without opening it through the service. It is read-only and works
// against both V1 (64-byte hex catalog keys, 72-byte head keys) and V2
// (32-byte raw catalog keys, 40-byte head keys) files.
//
// The tamper subcommand deliberately corrupts one catalog entry to let the
// migration failure path be exercised; it only runs against an explicit
// scratch copy supplied by the caller.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	bolt "go.etcd.io/bbolt"
)

var (
	root    = []byte("ridge-v1")
	spaces  = []byte("spaces")
	version = []byte("storage_version")
	catalog = []byte("catalog")
	head    = []byte("head")
	blocks  = []byte("blocks")
	deletes = []byte("deletes")
	rollups = []byte("rollups")
)

type report struct {
	Path            string           `json:"path"`
	StorageVersion  uint64           `json:"storage_version"`
	Namespaces      []namespaceShape `json:"namespaces"`
	CatalogKeySizes map[int]int      `json:"catalog_key_sizes"`
	HeadKeySizes    map[int]int      `json:"head_key_sizes"`
}

type namespaceShape struct {
	Name        string `json:"name"`
	Catalog     int    `json:"catalog_series"`
	Head        int    `json:"head_points"`
	Blocks      int    `json:"blocks"`
	Deletes     int    `json:"tombstones"`
	Rollups     int    `json:"rollups"`
	EmptySeries int    `json:"empty_series"`
}

func inspect(path string) (report, error) {
	out := report{Path: path, CatalogKeySizes: map[int]int{}, HeadKeySizes: map[int]int{}}
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer db.Close()
	err = db.View(func(tx *bolt.Tx) error {
		r := tx.Bucket(root)
		if r == nil {
			return fmt.Errorf("not a ridgeline database: %s", path)
		}
		if v := r.Get(version); len(v) == 8 {
			out.StorageVersion = binary.BigEndian.Uint64(v)
		} else {
			out.StorageVersion = 1
		}
		return r.Bucket(spaces).ForEach(func(name, nv []byte) error {
			if nv != nil {
				return nil
			}
			ns := r.Bucket(spaces).Bucket(name)
			shape := namespaceShape{Name: string(name)}
			busyHexIDs := map[string]bool{}
			if err := ns.Bucket(catalog).ForEach(func(k, _ []byte) error {
				out.CatalogKeySizes[len(k)]++
				shape.Catalog++
				return nil
			}); err != nil {
				return err
			}
			if err := ns.Bucket(head).ForEach(func(k, _ []byte) error {
				out.HeadKeySizes[len(k)]++
				shape.Head++
				return nil
			}); err != nil {
				return err
			}
			if err := ns.Bucket(blocks).ForEach(func(_, v []byte) error {
				var block struct {
					SeriesID string `json:"series_id"`
				}
				if err := json.Unmarshal(v, &block); err != nil {
					return err
				}
				shape.Blocks++
				busyHexIDs[block.SeriesID] = true
				return nil
			}); err != nil {
				return err
			}
			if err := ns.Bucket(deletes).ForEach(func(_, _ []byte) error {
				shape.Deletes++
				return nil
			}); err != nil {
				return err
			}
			if err := ns.Bucket(rollups).ForEach(func(_, _ []byte) error {
				shape.Rollups++
				return nil
			}); err != nil {
				return err
			}
			// Empty series: a catalog entry whose hex ID has neither head
			// points nor a block. Catalog values carry the external hex ID in
			// every storage version, so this check is version agnostic.
			headHexIDs := map[string]bool{}
			if err := ns.Bucket(head).ForEach(func(k, _ []byte) error {
				switch len(k) {
				case 40:
					headHexIDs[fmt.Sprintf("%x", k[:32])] = true
				case 72:
					headHexIDs[string(k[:64])] = true
				default:
					return fmt.Errorf("unexpected head key length %d", len(k))
				}
				return nil
			}); err != nil {
				return err
			}
			if err := ns.Bucket(catalog).ForEach(func(_, v []byte) error {
				var series struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(v, &series); err != nil {
					return err
				}
				if !busyHexIDs[series.ID] && !headHexIDs[series.ID] {
					shape.EmptySeries++
				}
				return nil
			}); err != nil {
				return err
			}
			out.Namespaces = append(out.Namespaces, shape)
			return nil
		})
	})
	return out, err
}

func tamper(path, namespace string, index int) error {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		ns := tx.Bucket(root).Bucket(spaces).Bucket([]byte(namespace))
		if ns == nil {
			return fmt.Errorf("namespace %q not found", namespace)
		}
		keys := make([][]byte, 0)
		if err := ns.Bucket(catalog).ForEach(func(k, _ []byte) error {
			keys = append(keys, append([]byte(nil), k...))
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
		if index < 0 || index >= len(keys) {
			return fmt.Errorf("catalog index %d out of range (have %d)", index, len(keys))
		}
		target := keys[index]
		broken := append([]byte(nil), target...)
		broken[0] = 'z' // no longer valid hexadecimal; the V1 migration must reject it
		value := append([]byte(nil), ns.Bucket(catalog).Get(target)...)
		if err := ns.Bucket(catalog).Delete(target); err != nil {
			return err
		}
		return ns.Bucket(catalog).Put(broken, value)
	})
}

func main() {
	flag.Parse()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ridge-inspect inspect <db> | ridge-inspect tamper <db> <namespace> <catalog-index>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "inspect":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: ridge-inspect inspect <db>")
			os.Exit(2)
		}
		rep, err := inspect(os.Args[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, "inspect failed:", err)
			os.Exit(1)
		}
		encoded, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(string(encoded))
	case "tamper":
		if len(os.Args) != 5 {
			fmt.Fprintln(os.Stderr, "usage: ridge-inspect tamper <db> <namespace> <catalog-index>")
			os.Exit(2)
		}
		var idx int
		if _, err := fmt.Sscanf(os.Args[4], "%d", &idx); err != nil {
			fmt.Fprintln(os.Stderr, "invalid catalog index:", err)
			os.Exit(2)
		}
		if err := tamper(os.Args[2], os.Args[3], idx); err != nil {
			fmt.Fprintln(os.Stderr, "tamper failed:", err)
			os.Exit(1)
		}
		fmt.Println("tampered")
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand:", os.Args[1])
		os.Exit(2)
	}
}
