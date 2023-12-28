package bucket

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

// trailerKey is the key to access a index.Trailer for a certain bucket.
// Buckets that are not loaded have some info that is easily accessible without
// loading them fully (i.e. the len). Since the Len can be different for each
// fork we need to keep it for each one separately.
type trailerKey struct {
	Key      item.Key
	Consumer string
}

type Buckets struct {
	mu                     sync.Mutex
	dir                    string
	tree                   btree.Map[item.Key, *Bucket]
	trailers               map[trailerKey]index.Trailer
	opts                   Options
	maxParallelOpenBuckets int
}

func LoadAll(dir string, maxParallelOpenBuckets int, opts Options) (*Buckets, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read-dir: %w", err)
	}

	var dirsHandled int
	tree := btree.Map[item.Key, *Bucket]{}
	trailers := make(map[trailerKey]index.Trailer, len(ents))
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

		if err := index.ReadTrailers(buckPath, func(consumer string, trailer index.Trailer) {
			// nil entries indicate buckets that were not loaded yet:
			trailers[trailerKey{
				Key:      key,
				Consumer: consumer,
			}] = trailer
		}); err != nil {
			// reading trailers is not too fatal, but applications may break in unexpected
			// ways when Len() returns wrong results.
			return nil, err
		}

		tree.Set(key, nil)
	}

	if dirsHandled == 0 && len(ents) > 0 {
		return nil, fmt.Errorf("%s is not empty; refusing to create db", dir)
	}

	return &Buckets{
		dir:                    dir,
		tree:                   tree,
		opts:                   opts,
		trailers:               trailers,
		maxParallelOpenBuckets: maxParallelOpenBuckets,
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

// forKey returns a bucket for the specified key and creates if not there yet.
// `key` must be the lowest key that is stored in this bucket. You cannot just
// use a key that is somewhere in the bucket.
func (bs *Buckets) forKey(key item.Key) (*Bucket, error) {
	buck, _ := bs.tree.Get(key)
	if buck != nil {
		// fast path:
		return buck, nil
	}

	// make room for one so we don't jump over the maximum:
	if err := bs.closeUnused(bs.maxParallelOpenBuckets - 1); err != nil {
		return nil, err
	}

	var err error
	buck, err = Open(bs.buckPath(key), bs.opts)
	if err != nil {
		return nil, err
	}

	bs.tree.Set(key, buck)
	return buck, nil
}

func (bs *Buckets) delete(key item.Key) error {
	buck, ok := bs.tree.Get(key)
	if !ok {
		return fmt.Errorf("no bucket with key %v", key)
	}

	for tk := range bs.trailers {
		if tk.Key == key {
			delete(bs.trailers, tk)
		}
	}

	var err error
	var dir string
	if buck != nil {
		// make sure to close the bucket, otherwise we will accumulate mmaps, which
		// will sooner or later lead to memory allocation issues/errors.
		err = buck.Close()
		dir = buck.dir // save on allocation of buckPath()
	} else {
		dir = bs.buckPath(key)
	}

	bs.tree.Delete(key)
	return errors.Join(
		err,
		removeBucketDir(dir),
	)
}

type IterMode int

const (
	// IncludeNil goes over all buckets, including those that are nil (not loaded.)
	IncludeNil = IterMode(iota)

	// LoadedOnly iterates over all buckets that were loaded already.
	LoadedOnly

	// Load loads all buckets, including those that were not loaded yet.
	Load
)

// IterStop can be returned in Iter's func when you want to stop
// It does not count as error.
var IterStop = errors.New("iteration stopped")

// Iter iterates over all buckets, starting with the lowest. The buckets include
// unloaded depending on `mode`. The error you return in `fn` will be returned
// by Iter() and iteration immediately stops. If you return IterStop then
// Iter() will return nil and will also stop the iteration. Note that Iter() honors the
// MaxParallelOpenBuckets option, i.e. when the mode is `Load` it will immediately close
// old buckets again before proceeding.
func (bs *Buckets) iter(mode IterMode, fn func(key item.Key, b *Bucket) error) error {
	// NOTE: We cannot directly iterate over the tree here, we need to make a copy
	// of they keys, as the btree library does not like if the tree is modified during iteration.
	// Modifications can happen in forKey() (which might close unused buckets) or in the user-supplied
	// function (notably Pop(), which deletes exhausted buckets). By copying it we make sure to
	// iterate over one consistent snapshot. This might need to change if we'd create new buckets
	// in fn() - let's hope that this does not happen.
	keys := bs.tree.Keys()
	for _, key := range keys {
		// Fetch from non-copied tree as this is the one that is modified.
		buck, ok := bs.tree.Get(key)
		if !ok {
			// it was deleted already? Skip it.
			continue
		}

		if buck == nil {
			if mode == LoadedOnly {
				continue
			}

			if mode == Load {
				// load the bucket fresh from disk.
				// NOTE: This might unload other buckets!
				var err error
				buck, err = bs.forKey(key)
				if err != nil {
					return err
				}
			}
		}

		if err := fn(key, buck); err != nil {
			if err == IterStop {
				err = nil
			}

			return err
		}
	}

	return nil
}

func (bs *Buckets) Sync() error {
	var err error
	bs.mu.Lock()
	defer bs.mu.Unlock()

	_ = bs.iter(LoadedOnly, func(_ item.Key, b *Bucket) error {
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
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.iter(LoadedOnly, func(_ item.Key, b *Bucket) error {
		// TODO: We need to store trailer info for all known consumers here.
		//       On fork we also need to copy it.
		// bs.trailers[b.Key()] = b.idx.Trailer()
		return b.Close()
	})
}

func (bs *Buckets) Len(consumer string) (int, error) {
	var len int
	bs.mu.Lock()
	defer bs.mu.Unlock()

	_ = bs.iter(IncludeNil, func(key item.Key, b *Bucket) error {
		if b == nil {
			trailer, ok := bs.trailers[trailerKey{
				Key:      key,
				Consumer: consumer,
			}]

			if !ok {
				bs.opts.Logger.Printf("bug: no trailer for %v", key)
				return nil
			}

			len += int(trailer.TotalEntries)
			return nil
		}

		blen, err := b.Len(consumer)
		if err != nil {
			return err
		}

		len += blen
		return nil
	})

	return len, nil
}

// TODO: Figure out how shovel and multi-consumer mode works together...
func (bs *Buckets) Shovel(dstBs *Buckets) (int, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	dstBs.mu.Lock()
	defer dstBs.mu.Unlock()

	buf := make(item.Items, 0, 4000)

	var ntotalcopied int
	err := bs.iter(IncludeNil, func(key item.Key, _ *Bucket) error {
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
			// TODO: figure this out. This needs
			// dstBs.trailers[key] = trailer

			return moveFileOrDir(srcPath, dstPath)
		}

		// In this case we have to copy the items more intelligently,
		// since we have to append it to the destination bucket.

		srcBuck, err := bs.forKey(key)
		if err != nil {
			return err
		}

		// NOTE: This assumes that the destination has the same bucket func.
		dstBuck, err := dstBs.forKey(key)
		if err != nil {
			return err
		}

		// TODO: Specifying a single consumer here does not work well. Do we have to copy all consumers then?
		// Or should we skip the directory move?
		_, ncopied, err := srcBuck.Move(math.MaxInt, buf[:0], dstBuck, "")
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

func (bs *Buckets) nloaded() int {
	var nloaded int
	bs.tree.Scan(func(_ item.Key, buck *Bucket) bool {
		if buck != nil {
			nloaded++
		}
		return true
	})

	return nloaded
}

// closeUnused closes as many buckets as needed to reach `maxBucks` total loaded buckets.
// The closed buckets are marked as nil and can be loaded again afterwards.
// If `maxBucks` is negative, this is a no-op.
func (bs *Buckets) closeUnused(maxBucks int) error {
	if maxBucks < 0 {
		// This disables this feature. You likely do not want that.
		return nil
	}

	// Fetch the number of loaded buckets.
	// We could optimize that by having another count for that,
	// but it should be cheap enough and this way we have only one
	// source of truth.
	nloaded := bs.nloaded()
	if nloaded <= maxBucks {
		// nothing to do, this should be the normal case.
		return nil
	}

	var closeErrs error

	// This logic here produces a sequence like this:
	// pivot=4: 4+0, 4-1, 4+1, 4-2, 4+2, 4-3, 4+3, 4-4, 4+4, ...
	//
	// In other words, it alternates around the middle of the buckets and
	// closes buckets that are more in the middle of the queue. This should be
	// a reasonable heuristic for a typical queue system where you pop from end
	// and push to the other one, but seldomly access buckets in the middle
	// range If you're priorities are very random, this will be rather random
	// too though.
	nClosed, nClosable := 0, nloaded-maxBucks
	pivotIdx := bs.tree.Len() / 2
	for idx := 0; idx < bs.tree.Len() && nClosed < nClosable; idx++ {
		realIdx := pivotIdx - idx/2 - 1
		if idx%2 == 0 {
			realIdx = pivotIdx + idx/2
		}

		key, bucket, ok := bs.tree.GetAt(realIdx)
		if !ok {
			// should not happen, but better be safe.
			continue
		}

		if bucket == nil {
			// already closed.
			continue
		}

		// TODO: Store all trailers here.
		// trailer := bucket.idx.Trailer()
		if err := bucket.Close(); err != nil {
			switch bs.opts.ErrorMode {
			case ErrorModeAbort:
				closeErrs = errors.Join(closeErrs, err)
			case ErrorModeContinue:
				bs.opts.Logger.Printf("failed to reap bucket %s", key)
			}
		}

		bs.tree.Set(key, nil)
		// bs.trailers[key] = trailer
		nClosed++
	}

	return closeErrs
}

// binsplit returns the first index of `items` that would
// not go to the bucket `comp`. There are two assumptions:
//
// * "items" is not empty.
// * "comp" exists for at least one fn(item.Key)
// * The first key in `items` must be fn(key) == comp
//
// If assumptions are not fulfilled you will get bogus results.
func binsplit(items item.Items, comp item.Key, fn func(item.Key) item.Key) int {
	l := len(items)
	if l == 0 {
		return 0
	}
	if l == 1 {
		return 1
	}

	pivotIdx := l / 2
	pivotKey := fn(items[pivotIdx].Key)
	if pivotKey != comp {
		// search left:
		return binsplit(items[:pivotIdx], comp, fn)
	}

	// search right:
	return pivotIdx + binsplit(items[pivotIdx:], comp, fn)
}

// Push pushes a batch of `items` to the queue.
func (bs *Buckets) Push(items item.Items) error {
	if len(items) == 0 {
		return nil
	}

	slices.SortFunc(items, func(i, j item.Item) int {
		return int(i.Key - j.Key)
	})

	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Sort items into the respective buckets:
	for len(items) > 0 {
		keyMod := bs.opts.BucketFunc(items[0].Key)
		nextIdx := binsplit(items, keyMod, bs.opts.BucketFunc)
		buck, err := bs.forKey(keyMod)
		if err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return fmt.Errorf("bucket: for-key: %w", err)
			}

			bs.opts.Logger.Printf("failed to push: %v", err)
		} else {
			if err := buck.Push(items[:nextIdx]); err != nil {
				if bs.opts.ErrorMode == ErrorModeAbort {
					return fmt.Errorf("bucket: push: %w", err)
				}

				bs.opts.Logger.Printf("failed to push: %v", err)
			}
		}

		items = items[nextIdx:]
	}

	return nil
}

// ReadOp define the kind of operation done by the Read() function.
type ReadOp int

const (
	ReadOpPeek = 0
	ReadOpPop  = 1
	ReadOpMove = 2
)

type ReadFn func(items item.Items) error

// Read handles all kind of reading operations. It is a low-level function that is not part of the official API.
func (bs *Buckets) Read(op ReadOp, n int, dst item.Items, consumer string, fn ReadFn, dstBs *Buckets) error {
	if n < 0 {
		// use max value in this case:
		n = int(^uint(0) >> 1)
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	var count = n
	return bs.iter(Load, func(key item.Key, b *Bucket) error {
		// Choose the right func for the operation:
		var opFn func(n int, dst item.Items, consumer string) (item.Items, int, error)
		switch op {
		case ReadOpPop:
			opFn = b.Pop
		case ReadOpPeek:
			opFn = b.Peek
		case ReadOpMove:
			// Move() has a slightly different signature, so adapter is needed.
			opFn = func(count int, dst item.Items, consumer string) (item.Items, int, error) {
				dstBuck, err := dstBs.forKey(b.key)
				if err != nil {
					return dst, 0, nil
				}

				return b.Move(count, dst, dstBuck, consumer)
			}
		default:
			// programmer error
			panic("invalid op")
		}

		items, nitems, err := opFn(count, dst, consumer)
		if err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return err
			}

			// try with the next bucket in the hope that it works:
			bs.opts.Logger.Printf("failed to pop: %v", err)
			return nil
		}

		var fnErr error
		if len(items) > 0 && fn != nil {
			fnErr = fn(items)
		}

		if b.AllEmpty() {
			if err := bs.delete(key); err != nil {
				return fmt.Errorf("failed to delete bucket: %w", err)
			}
		}

		if fnErr != nil {
			return fnErr
		}

		count -= nitems
		if count <= 0 {
			return IterStop
		}

		return nil
	})
}

func (bs *Buckets) DeleteLowerThan(consumer string, key item.Key) (int, error) {
	var numDeleted int
	var deletableBucks []*Bucket

	bs.mu.Lock()
	defer bs.mu.Unlock()

	err := bs.iter(Load, func(bucketKey item.Key, buck *Bucket) error {
		if bucketKey >= key {
			// stop loading buckets if not necessary.
			return IterStop
		}

		numDeletedOfBucket, err := buck.DeleteLowerThan(consumer, key)
		if err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return err
			}

			// try with the next bucket in the hope that it works:
			bs.opts.Logger.Printf("failed to delete : %v", err)
			return nil
		}

		numDeleted += numDeletedOfBucket
		if buck.AllEmpty() {
			deletableBucks = append(deletableBucks, buck)
		}

		return nil
	})

	if err != nil {
		return numDeleted, err
	}

	for _, bucket := range deletableBucks {
		if err := bs.delete(bucket.Key()); err != nil {
			return numDeleted, fmt.Errorf("bucket delete: %w", err)
		}
	}

	return numDeleted, nil
}

func (bs *Buckets) Fork(consumer string) error {
	return bs.iter(Load, func(key item.Key, buck *Bucket) error {
	})
}

func (bs *Buckets) RemoveFork(consumer string) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.iter(IncludeNil, func(key item.Key, buck *Bucket) error {
		if buck != nil {
			return buck.RemoveFork(consumer)
		}

		// Quick path: bucket was not loaded, so we can just throw out
		// the to-be-removed index file:
		buckDir := filepath.Join(bs.dir, key.String())
		return os.Remove(idxPath(buckDir, consumer))
	})
}
