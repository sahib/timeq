package bucket

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/sahib/lemurq/item"
	"github.com/tidwall/btree"
)

type Buckets struct {
	dir  string
	tree btree.Map[item.Key, *Bucket]
}

func LoadAll(dir string) (*Buckets, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	tree := btree.Map[item.Key, *Bucket]{}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}

		bucket, err := Open(ent.Name())
		if err != nil {
			return nil, err
		}

		tree.Set(bucket.key, bucket)
	}

	return &Buckets{
		dir:  dir,
		tree: tree,
	}, nil
}

func (bs Buckets) ByKey(key item.Key) (*Bucket, error) {
	if b, ok := bs.tree.Get(key); ok {
		return b, nil
	}

	buck, err := Open(filepath.Join(bs.dir, key.String()))
	if err != nil {
		return nil, err
	}

	bs.tree.Set(key, buck)
	return buck, nil
}

// IterStop can be returned in Iter's func when you want to stop
// It does not count as error.
var IterStop = errors.New("iteration stopped")

func (bs Buckets) Iter(fn func(b *Bucket) error) error {
	var err error
	bs.tree.Scan(func(key item.Key, buck *Bucket) bool {
		if err = fn(buck); err != nil {
			if err == IterStop {
				err = nil
			}

			return false
		}
		return true
	})
	return err
}
