package bucket

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
	"github.com/tidwall/btree"
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

func Open(dir string, opts Options) (*Bucket, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	log := vlog.Open(filepath.Join(dir, "dat.log"), opts.SyncMode&SyncData > 0)
	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil {
		opts.Logger.Printf("failed to load index %s: %v", idxPath, err)

		tree, genErr := log.GenerateIndex()
		if err != nil {
			// not much we can by now
			return nil, fmt.Errorf("index load: %w (could not regen: %w)", err, genErr)
		}

		// continue with rebuild index, but remove broken one:
		idx = &index.Index{Map: *tree}
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

// Push expects pre-sorted items!
func (b *Bucket) Push(items []item.Item) error {
	if len(items) == 0 {
		return nil
	}

	// TODO: locking would only be needed when modifying the index?
	//       all attributes of bucket itself are not modified after Open.
	b.mu.Lock()
	defer b.mu.Unlock()

	loc, err := b.log.Push(items)
	if err != nil {
		return fmt.Errorf("push: log: %w", err)
	}

	maxSkew := int(b.opts.MaxSkew)
	skewLoc, skews := b.idx.SetSkewed(loc, maxSkew)
	if skews == maxSkew {
		// at least warn user since this might lead to lost data:
		b.opts.Logger.Printf("push: maximum skew was reached (key=%v skew=%d)", loc.Key, b.opts.MaxSkew)
	}

	if err := b.idxLog.Push(skewLoc); err != nil {
		return fmt.Errorf("push: index-log: %w", err)
	}

	return nil
}

// addPopIter adds a new batchIter to `batchIters` and advances the idxIter.
func (b *Bucket) addPopIter(batchIters *vlog.LogIters, idxIter *btree.MapIter[item.Key, item.Location]) (bool, error) {
	loc := idxIter.Value()
	batchIter, err := b.log.At(loc)
	if err != nil {
		return false, err
	}

	if !batchIter.Next(nil) {
		// might be empty or I/O error:
		return false, batchIter.Err()
	}

	heap.Push(batchIters, batchIter)
	return !idxIter.Next(), nil
}

func (b *Bucket) Pop(n int, dst []item.Item) ([]item.Item, int, error) {
	if n <= 0 {
		// technically that's a valid usecase.
		return dst, 0, nil
	}

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
		for !indexExhausted {
			nextLoc := idxIter.Value()

			// Push might "skew" the key of the index by adding an offset to the key
			// so we can efficiently store it. This offset will show up in nextLoc.Key
			// and might lead to the odd situation that location's key is higher than the
			// item key. We have to load all iterators up to diff of MaxSkew to be sure that
			// we iterate in the correct order.
			keyDiff := nextLoc.Key - nextItem.Key
			if (*batchIters)[0].Exhausted() || keyDiff <= item.Key(b.opts.MaxSkew) {
				indexExhausted, err = b.addPopIter(batchIters, &idxIter)
				if err != nil {
					return dst, 0, err
				}

			} else {
				break
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
			b.idx.SetSkewed(currLoc, int(b.opts.MaxSkew))
			if err := b.idxLog.Push(currLoc); err != nil {
				return dst, 0, fmt.Errorf("idxlog: append begun: %w", err)
			}
		}

		b.idx.Delete(batchIter.FirstKey())

		// this batch was fullly exhausted during Pop()
		// mark it as such in the index-wal:
		deadLoc := item.Location{
			Key: batchIter.FirstKey(),
			Len: 0,
			Off: 0,
		}
		if err := b.idxLog.Push(deadLoc); err != nil {
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

		logIter, err := b.log.At(loc)
		if err != nil {
			return numDeleted, err
		}

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
			b.idx.SetSkewed(item.Location{
				Key: partialItem.Key,
				Off: partialLoc.Off,
				Len: partialLoc.Len,
			}, int(b.opts.MaxSkew))
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
