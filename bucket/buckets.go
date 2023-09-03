package bucket

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

type Buckets struct {
	mu   sync.Mutex
	dir  string
	tree btree.Map[item.Key, *Bucket]
	opts Options
}

func LoadAll(dir string, opts Options) (*Buckets, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	tree := btree.Map[item.Key, *Bucket]{}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}

		bucket, err := Open(ent.Name(), opts)
		if err != nil {
			return nil, err
		}

		tree.Set(bucket.key, bucket)
	}

	return &Buckets{
		dir:  dir,
		tree: tree,
		opts: opts,
	}, nil
}

func (bs *Buckets) buckPath(key item.Key) string {
	return filepath.Join(bs.dir, key.String())
}

func (bs *Buckets) ByKey(key item.Key) (*Bucket, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if b, ok := bs.tree.Get(key); ok {
		return b, nil
	}

	buck, err := Open(bs.buckPath(key), bs.opts)
	if err != nil {
		return nil, err
	}

	bs.tree.Set(key, buck)
	return buck, nil
}

func (bs *Buckets) Delete(key item.Key) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.tree.Delete(key)
	return os.RemoveAll(bs.buckPath(key))
}

// IterStop can be returned in Iter's func when you want to stop
// It does not count as error.
var IterStop = errors.New("iteration stopped")

func (bs *Buckets) Iter(fn func(b *Bucket) error) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

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
