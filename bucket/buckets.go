package bucket

import (
	"errors"
	"fmt"
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

func loadAndDeleteBucketIfEmpty(buckPath string, opts Options) (*Bucket, bool, error) {
	// NOTE: If you have a lot of buckets this would take a bit of time.
	// However, it's simple, stupid and works. If one needs to do better
	// we could write some marker to the bucket that quickly tells us if
	// the bucket is empty.
	buck, err := Open(buckPath, opts)
	if err != nil {
		return nil, false, err
	}

	if !buck.Empty() {
		return buck, false, nil
	}

	if err := buck.Close(); err != nil {
		return nil, false, err
	}

	if err := os.RemoveAll(buckPath); err != nil {
		return nil, false, err
	}

	return nil, true, nil
}

func LoadAll(dir string, opts Options) (*Buckets, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read-dir: %w", err)
	}

	var dirsHandled int
	tree := btree.Map[item.Key, *Bucket]{}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}

		buckPath := filepath.Join(dir, ent.Name())
		key, err := item.KeyFromString(filepath.Base(buckPath))
		if err != nil {
			opts.Logger.Printf("failed to parse %s as bucket path\n", buckPath)
			continue
		}

		// NOTE: This is intentionally kept simple. There is no size marker or
		// similar to hint deleting a bucket without loading it first. This
		// means that we have to load all buckets on startup, occupying some
		// memory. If you think about optimizing this, you have to consider:
		//
		// - Len() needs some way to also report unloaded bucket size
		// - ByKey() and Iter() need to load not yet loaded buckets.
		buck, wasDeleted, err := loadAndDeleteBucketIfEmpty(buckPath, opts)
		dirsHandled++
		if err != nil {
			return nil, fmt.Errorf("load-or-delete: %w", err)
		} else if wasDeleted {
			continue
		}

		// nil entries indicate buckets that were not loaded yet:
		tree.Set(key, buck)
	}

	if dirsHandled == 0 && len(ents) > 0 {
		return nil, fmt.Errorf("%s is not empty; refusing to create db", dir)

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

// ForKey returns a bucket for the specified key and creates if not there yet.
// `key` must be the lowest key that is stored in this bucket. You cannot just
// use a key that is somewhere in the bucket.
func (bs *Buckets) ForKey(key item.Key) (*Bucket, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	buck, _ := bs.tree.Get(key)
	if buck != nil {
		// fast path:
		return buck, nil
	}

	var err error
	buck, err = Open(bs.buckPath(key), bs.opts)
	if err != nil {
		return nil, err
	}

	bs.tree.Set(key, buck)
	return buck, nil
}

func (bs *Buckets) Delete(key item.Key) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.delete(key)
}

func (bs *Buckets) delete(key item.Key) error {
	buck, ok := bs.tree.Get(key)
	if !ok {
		return fmt.Errorf("no bucket with key %v", key)
	}

	var err error
	if buck != nil {
		// make sure to close the bucket, otherwise we ill accumulate mmaps, which
		// will sooner or later lead to memory allocation issues/errors.
		err = buck.Close()
	}

	bs.tree.Delete(key)
	return errors.Join(
		err,
		os.RemoveAll(bs.buckPath(key)),
	)
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

func (bs *Buckets) Sync() error {
	// TOOD: sync syncs all buckets currently and opens some newly
	var err error
	_ = bs.Iter(func(b *Bucket) error {
		// try to sync as much as possible:
		err = errors.Join(err, b.Sync(true))
		return nil
	})

	return err
}

func (bs *Buckets) Clear() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	keys := bs.tree.Keys()
	for _, key := range keys {
		if err := bs.delete(key); err != nil {
			return err
		}
	}

	return nil
}

func (bs *Buckets) Close() error {
	return bs.Iter(func(b *Bucket) error {
		return b.Close()
	})
}

func (bs *Buckets) Len() int {
	var len int
	_ = bs.Iter(func(b *Bucket) error {
		len += b.Len()
		return nil
	})

	return len
}
