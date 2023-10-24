package bucket

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

type Buckets struct {
	mu       sync.Mutex
	dir      string
	tree     btree.Map[item.Key, *Bucket]
	trailers map[item.Key]index.Trailer
	opts     Options
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
	trailers := make(map[item.Key]index.Trailer, len(ents))
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}

		buckPath := filepath.Join(dir, ent.Name())
		key, err := item.KeyFromString(filepath.Base(buckPath))
		if err != nil {
			if opts.ErrorMode == ErrorModeAbort {
				return nil, err
			}

			opts.Logger.Printf("failed to parse %s as bucket path\n", buckPath)
			continue
		}

		dirsHandled++

		trailer, err := index.ReadTrailer(filepath.Join(buckPath, "idx.log"))
		if err != nil {
			if opts.ErrorMode == ErrorModeAbort {
				return nil, err
			}

			opts.Logger.Printf("failed to read trailer on bucket %s: %v", buckPath, err)
			continue
		}

		if trailer.TotalEntries == 0 {
			// It's an empty bucket. Delete it.
			if err := os.RemoveAll(buckPath); err != nil {
				if opts.ErrorMode == ErrorModeAbort {
					return nil, err
				}

				opts.Logger.Printf("failed to remove old bucket: %v", err)
			}

			continue
		}

		// nil entries indicate buckets that were not loaded yet:
		trailers[key] = trailer
		tree.Set(key, nil)
	}

	if dirsHandled == 0 && len(ents) > 0 {
		return nil, fmt.Errorf("%s is not empty; refusing to create db", dir)
	}

	return &Buckets{
		dir:      dir,
		tree:     tree,
		opts:     opts,
		trailers: trailers,
	}, nil
}

// ValidateBucketKeys checks if the keys in the buckets correspond to the result
// of the key func. Failure here indicates that the key function changed. No error
// does not guarantee that the key func did not change though (e.g. the identity func
// would produce no error in this check)
func (bs *Buckets) ValidateBucketKeys(bucketFn func(item.Key) item.Key) error {
	for iter := bs.tree.Iter(); iter.Next(); {
		ik := iter.Key()
		bk := bucketFn(ik)

		if ik != bk {
			return fmt.Errorf(
				"bucket with key %s does not match key func (%d) - did it change",
				ik,
				bk,
			)
		}
	}

	return nil
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

	return bs.forKey(key)
}

func (bs *Buckets) forKey(key item.Key) (*Bucket, error) {
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

type IterMode int

const (
	// IncludeNil goes over all buckets, including those that are nil (not loaded.)
	IncludeNil = IterMode(iota)

	// LoadedOnly iterates over all buckets that were loaded already.
	LoadedOnly

	// Load loads buckets that were not loaded yet.
	Load
)

// IterStop can be returned in Iter's func when you want to stop
// It does not count as error.
var IterStop = errors.New("iteration stopped")

// Iter iterats over all buckets, starting with the lowest. The buckets include
// unloaded depending on `mode`. The error you return in `fn` will be returned
// by Iter() and iteration immediately stops. If you return IterStop then
// Iter() will return nil and will also stop the iteration.
func (bs *Buckets) Iter(mode IterMode, fn func(key item.Key, b *Bucket) error) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var err error
	bs.tree.Scan(func(key item.Key, buck *Bucket) bool {
		if buck == nil {
			if mode == LoadedOnly {
				return true
			}

			if mode == Load {
				// load the bucket fresh from disk:
				buck, err = bs.forKey(key)
				if err != nil {
					return false
				}
			}
		}

		if err = fn(key, buck); err != nil {
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
	var err error
	_ = bs.Iter(LoadedOnly, func(_ item.Key, b *Bucket) error {
		// try to sync as much as possible:
		err = errors.Join(err, b.Sync(true))
		return nil
	})

	return err
}

func (bs *Buckets) Clear() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.clear()
}

func (bs *Buckets) clear() error {
	keys := bs.tree.Keys()
	for _, key := range keys {
		if err := bs.delete(key); err != nil {
			return err
		}
	}

	return nil
}

func (bs *Buckets) Close() error {
	return bs.Iter(LoadedOnly, func(_ item.Key, b *Bucket) error {
		return b.Close()
	})
}

func (bs *Buckets) Len() int {
	var len int
	_ = bs.Iter(IncludeNil, func(key item.Key, b *Bucket) error {
		if b == nil {
			trailer, ok := bs.trailers[key]
			if !ok {
				bs.opts.Logger.Printf("bug: no trailer for %v", key)
				return nil
			}

			len += int(trailer.TotalEntries)
			return nil
		}

		len += b.Len()
		return nil
	})

	return len
}

func (bs *Buckets) Shovel(dstBs *Buckets) (int, error) {
	dstBs.mu.Lock()
	defer dstBs.mu.Unlock()

	buf := make(item.Items, 0, 4000)

	var ntotalcopied int
	err := bs.Iter(IncludeNil, func(key item.Key, srcBuck *Bucket) error {
		if _, ok := dstBs.tree.Get(key); !ok {
			// fast path: We can just move the bucket directory.
			dstPath := dstBs.buckPath(key)
			srcPath := bs.buckPath(key)
			dstBs.tree.Set(key, nil)

			trailer, err := index.ReadTrailer(filepath.Join(srcPath, "idx.log"))
			if err != nil {
				return err
			}

			ntotalcopied += int(trailer.TotalEntries)
			dstBs.trailers[key] = trailer

			return moveFileOrDir(srcPath, dstPath)
		}

		// In this case we have to copy the items more intelligently,
		// since we have to append it to the destination bucket.

		if srcBuck == nil {
			var err error
			srcBuck, err = Open(bs.buckPath(key), bs.opts)
			if err != nil {
				return err
			}
		}

		// NOTE: This assumes that the destination has the same bucket func.
		dstBuck, err := dstBs.forKey(key)
		if err != nil {
			return err
		}

		_, ncopied, err := srcBuck.Move(math.MaxInt, buf, dstBuck)
		ntotalcopied += ncopied
		return err
	})

	if err != nil {
		return ntotalcopied, err
	}

	if err := bs.clear(); err != nil {
		return ntotalcopied, err
	}

	return ntotalcopied, err
}
