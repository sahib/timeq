package bucket

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
)

type Bucket struct {
	mu     sync.Mutex
	dir    string
	key    item.Key
	log    *vlog.Log
	idxLog *index.Writer
	idx    *index.Index
	opts   Options
}

func Open(dir string, opts Options) (buck *Bucket, outErr error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	// Setting this allows us to handle mmap() errors gracefully.
	// The typical scenario where those errors happen, are full filesystems.
	// This can happen like this:
	//
	// * ftruncate() grows a file beyond the available space without error.
	//   Since the new "space" are just zeros that do not take any physical
	//   space this makes sense.
	// * Accessing this mapped memory however will cause the filesystem to actually
	//   try to serve some more pages, which fails as it's full (would also happen on
	//   hardware failure or similar)
	// * This causes a SIGBUS to be send to our process. By default Go crashes the program
	//   and prints a stack trace. Changing this to a recoverable panic allows us to intervene
	//   and continue execution with a proper error return.
	debug.SetPanicOnFault(true)

	defer recoverMmapError(&outErr)

	log, err := vlog.Open(filepath.Join(dir, "dat.log"), opts.SyncMode&SyncData > 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil {
		// We try to re-generate the index from the value log.
		// Since we have all the keys and offsets there too,
		// we should be able to recover from that.
		opts.Logger.Printf("failed to load index %s: %v", idxPath, err)
		idx, err = index.FromVlog(log)
		if err != nil {
			// not much we can do for that case:
			return nil, fmt.Errorf("index load failed & could not regenerate: %w", err)
		}

		if err := os.Remove(idxPath); err != nil {
			return nil, fmt.Errorf("index failover: could not remove broken index: %w", err)
		}

		opts.Logger.Printf("recovered index with %d entries", idx.Len())
	}

	idxLog, err := index.NewWriter(idxPath, opts.SyncMode&SyncIndex > 0)
	if err != nil {
		return nil, fmt.Errorf("index writer: %w", err)
	}

	key, err := item.KeyFromString(filepath.Base(dir))
	if err != nil {
		return nil, err
	}

	return &Bucket{
		dir:    dir,
		key:    item.Key(key),
		log:    log,
		idx:    idx,
		idxLog: idxLog,
		opts:   opts,
	}, nil
}

func (b *Bucket) Sync(force bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return errors.Join(b.log.Sync(force), b.idxLog.Sync(force))
}

func (b *Bucket) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return errors.Join(
		b.log.Sync(true),
		b.idxLog.Sync(true),
		b.log.Close(),
		b.idxLog.Close(),
	)
}

func recoverMmapError(dstErr *error) {
	// See comment in Open()
	if recErr := recover(); recErr != nil {
		*dstErr = fmt.Errorf("panic (do you have enough space left?): %v", recErr)
	}
}

// Push expects pre-sorted items!
func (b *Bucket) Push(items []item.Item) (outErr error) {
	if len(items) == 0 {
		return nil
	}

	// defer recoverMmapError(&outErr)

	// TODO: locking would only be needed when modifying the index?
	//       all attributes of bucket itself are not modified after Open.
	b.mu.Lock()
	defer b.mu.Unlock()

	loc, err := b.log.Push(items)
	if err != nil {
		return fmt.Errorf("push: log: %w", err)
	}

	b.idx.Set(loc)
	if err := b.idxLog.Push(loc, b.idx.Trailer()); err != nil {
		return fmt.Errorf("push: index-log: %w", err)
	}

	return nil
}

// addPopIter adds a new batchIter to `batchIters` and advances the idxIter.
func (b *Bucket) addPopIter(batchIters *vlog.LogIters, idxIter *index.Iter) (bool, error) {
	loc := idxIter.Value()
	batchIter := b.log.At(loc)
	if !batchIter.Next(nil) {
		// might be empty or I/O error:
		return false, batchIter.Err()
	}

	heap.Push(batchIters, batchIter)
	return !idxIter.Next(), nil
}

func (b *Bucket) Pop(n int, dst []item.Item) (outItems []item.Item, npopped int, outErr error) {
	if n <= 0 {
		// technically that's a valid usecase.
		return dst, 0, nil
	}

	defer recoverMmapError(&outErr)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Fetch the lowest entry of the index:
	idxIter := b.idx.Iter()
	if !idxIter.Next() {
		// The index is empty. Nothing to pop.
		return dst, 0, nil
	}

	// initialize with first batch iter:
	batchItersSlice := make(vlog.LogIters, 0, 1)
	batchIters := &batchItersSlice
	indexExhausted, err := b.addPopIter(batchIters, &idxIter)
	if err != nil {
		return dst, 0, err
	}

	if len(*batchIters) == 0 {
		// this should not happen normally, but can possibly
		// in case of broken index or wal.
		return dst, 0, err
	}

	// Choose the lowest item of all iterators here and make sure the next loop
	// iteration will yield the next highest key.
	var numAppends int
	for numAppends < n && !(*batchIters)[0].Exhausted() {
		batchItem := (*batchIters)[0].Item()
		dst = append(dst, batchItem)
		numAppends++

		// advance current batch iter. We will make sure at the
		// end of the loop that the currently first one gets sorted
		// correctly if it turns out to be out-of-order.
		var nextItem item.Item
		(*batchIters)[0].Next(&nextItem)
		if err := (*batchIters)[0].Err(); err != nil {
			return dst, 0, err
		}

		// index batch entries might be overlapping. We need to check if the
		// next entry in the index needs to be taken into account for the next
		// iteration. For this we compare the next index entry to the
		// supposedly next batch value.
		if !indexExhausted {
			nextLoc := idxIter.Value()
			if (*batchIters)[0].Exhausted() || nextLoc.Key <= nextItem.Key {
				indexExhausted, err = b.addPopIter(batchIters, &idxIter)
				if err != nil {
					return dst, 0, err
				}
			}
		}

		// Repair sorting of the heap as we changed the value of the first iter.
		heap.Fix(batchIters, 0)
	}

	// NOTE: In theory we could also use fallocate(FALLOC_FL_ZERO_RANGE) on
	// ext4 to "put holes" into the log file where we read batches from to save
	// some space early. This would make sense only for very big buckets
	// though. We delete the bucket once it was completely exhausted anyways.

	// Now since we've collected all data we need to remember what we consumed.
	for _, batchIter := range *batchIters {
		if !batchIter.Exhausted() {
			currLoc := batchIter.CurrentLocation()

			// some keys were take from it, but not all (or none)
			// we need to adjust the index to keep those reachable.
			b.idx.Set(currLoc)

			if err := b.idxLog.Push(currLoc, b.idx.Trailer()); err != nil {
				return dst, 0, fmt.Errorf("idxlog: append begun: %w", err)
			}
		}

		// Make sure the previous batch index entry gets deleted:
		b.idx.Delete(batchIter.FirstKey())
		deadLoc := item.Location{
			Key: batchIter.FirstKey(),
			Len: 0,
			Off: 0,
		}
		if err := b.idxLog.Push(deadLoc, b.idx.Trailer()); err != nil {
			return dst, 0, fmt.Errorf("idxlog: append begun: %w", err)
		}
	}

	return dst, numAppends, b.idxLog.Sync(false)
}

func (b *Bucket) DeleteLowerThan(key item.Key) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.key >= key {
		// this bucket is safe from the clear.
		return 0, nil
	}

	var deleteEntries []item.Key
	var numDeleted int
	for iter := b.idx.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key >= key {
			// this index entry may live untouched.
			break
		}

		// we need to check until what point we need to delete.
		var partialFound bool
		var partialItem item.Item
		var partialLoc item.Location

		logIter := b.log.At(loc)
		for logIter.Next(&partialItem) {
			if partialItem.Key >= key {
				partialFound = true
				break
			}

			partialLoc = logIter.CurrentLocation()
		}

		if partialFound {
			// we found an enrty in the log that is >= key.
			// resize the index entry to skip the entries before.
			b.idx.Set(item.Location{
				Key: partialItem.Key,
				Off: partialLoc.Off,
				Len: partialLoc.Len,
			})
			numDeleted += int(loc.Len - partialLoc.Len)
		} else {
			// nothing found, this index entry can be dropped.
			numDeleted += int(loc.Len)
		}

		deleteEntries = append(deleteEntries, loc.Key)
	}

	for _, key := range deleteEntries {
		b.idx.Delete(key)
	}

	return numDeleted, b.idxLog.Sync(false)
}

func (b *Bucket) Empty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.idx.Len() == 0
}

func (b *Bucket) Key() item.Key {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.key
}

func (b *Bucket) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := 0
	iter := b.idx.Iter()
	for iter.Next() {
		size += int(iter.Value().Len)
	}

	return size
}
