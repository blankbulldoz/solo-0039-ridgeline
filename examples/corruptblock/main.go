// Command corruptblock flips one byte of a stored block payload so the
// walkthrough can demonstrate how readers behave with unreadable bytes.
// It is demonstration tooling, not part of the server; it opens the database
// file the same way the service does and never contacts a running process.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func main() {
	database := flag.String("db", "", "path to the bbolt database file")
	space := flag.String("space", "", "namespace name")
	series := flag.String("series", "", "series ID whose block payload should be damaged")
	flag.Parse()
	if *database == "" || *space == "" || *series == "" {
		fmt.Fprintln(os.Stderr, "usage: corruptblock -db FILE -space NAME -series ID")
		os.Exit(2)
	}
	db, err := volume.Open(*database)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	err = db.Raw.Update(func(tx *bolt.Tx) error {
		b := volume.Namespace(tx, *space)
		if b == nil {
			return fmt.Errorf("namespace %q does not exist", *space)
		}
		var target *model.Block
		err := b.Bucket(volume.Blocks).ForEach(func(_, value []byte) error {
			var block model.Block
			if err := json.Unmarshal(value, &block); err != nil {
				return err
			}
			if block.SeriesID == *series {
				block := block
				target = &block
				return nil
			}
			return nil
		})
		if err != nil {
			return err
		}
		if target == nil {
			return fmt.Errorf("series %q has no stored block", *series)
		}
		payload := b.Bucket(volume.Payloads).Get([]byte(target.ID))
		if len(payload) == 0 {
			return fmt.Errorf("block %q has no payload", target.ID)
		}
		damaged := append([]byte(nil), payload...)
		damaged[len(damaged)/2] ^= 0xff
		return b.Bucket(volume.Payloads).Put([]byte(target.ID), damaged)
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("damaged one block payload for series %s in namespace %s\n", *series, *space)
}
