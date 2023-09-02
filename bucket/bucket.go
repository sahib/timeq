package bucket

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
	"github.com/tidwall/btree"
	"golang.org/x/exp/slog"
)

type SyncMode int

// The available option are inspired by SQLite:
// https://www.sqlite.org/pragma.html#pragma_synchronous
const (
	// SyncNone does not sync on normal operation (only on close)
	SyncNone = SyncMode(1 << iota)
	// SyncData only synchronizes the data log
	SyncData
	// SyncIndex only synchronizes the index log (does not make sense alone)
	SyncIndex
	// SyncFull syncs both the data and index log
	SyncFull = SyncData | SyncIndex
)

type Options struct {
	// MaxSkew defines how much a key may be shifted in case of duplicates.
	// This can lead to very small inaccuracies. Zero disables this.
	MaxSkew time.Duration

	// SyncMode controls how often we sync data to the disk. The more data we sync
	// the more durable is the queue at the cost of throughput.
	SyncMode SyncMode
}

func DefaultOptions() Options {
	return Options{
		// 1us is plenty time to shift a key
		MaxSkew: time.Microsecond,
		// default is to be safe:
		SyncMode: SyncFull,
	}
}

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

	log, err := vlog.Open(filepath.Join(dir, "dat.log"), opts.SyncMode&SyncData > 0)
	if err != nil {
		return nil, err
	}

	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil {
		return nil, fmt.Errorf("index load: %w", err)
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

func (b *Bucket) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return errors.Join(b.log.Sync(), b.idxLog.Sync())
}

func (b *Bucket) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return errors.Join(
		b.log.Sync(),
		b.idxLog.Sync(),
		b.log.Close(),
		b.idxLog.Close(),
	)
}

// Push expects pre-sorted items!
func (b *Bucket) Push(items []item.Item) error {
	if len(items) == 0 {
		return nil
	}

	b.mu.Lock()
	// TODO: locking would only be needed when modifying the index?
	defer b.mu.Unlock()

	loc, err := b.log.Push(items)
	if err != nil {
		return err
	}

	maxSkew := int(b.opts.MaxSkew)
	skewLoc, skews := b.idx.SetSkewed(loc, maxSkew)
	if skews == maxSkew {
		// at least warn user since this might lead to lost data:
		slog.Warn("push: maximum skew was reached", "key", loc.Key, "skew", b.opts.MaxSkew)
	}

	if err := b.idxLog.Push(skewLoc); err != nil {
		return fmt.Errorf("push: index-log: %w", err)
	}

	return nil
}

// addPopIter adds a new batchIter to `batchIters` and advances the idxIter.
func (b *Bucket) addPopIter(batchIters *vlog.LogIters, idxIter *btree.MapIter[item.Key, item.Location]) (bool, error) {
	loc := idxIter.Value()
	batchIter := b.log.At(loc)
	if !batchIter.Next(nil) {
		// might be empty or I/O error:
		return false, batchIter.Err()
	}

	heap.Push(batchIters, batchIter)
	if !idxIter.Next() {
		return true, nil
	}

	return false, nil
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
		b.idx.Delete(batchIter.Key())

		currLoc := batchIter.CurrentLocation()
		if batchIter.Exhausted() {
			// this batch was fullly exhausted during Pop()
			currLoc.Len = 0
		} else {
			// some keys were take from it, but not all (or none)
			// we need to adjust the index to keep those reachable.
			b.idx.Set(currLoc.Key, currLoc)
		}

		if err := b.idxLog.Push(currLoc); err != nil {
			return dst, 0, fmt.Errorf("idxlog: append begun: %w", err)
		}
	}

	return dst, numAppends, b.idxLog.Sync()
}

func (b *Bucket) DeleteLowerThan(key item.Key) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.key >= key {
		// this bucket is safe from the clear.
		return 0, nil
	}

	var numDeleted int
	for iter := b.idx.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key >= key {
			// this index entry may live.
			break
		}

		// we need to check until what point we need to delete.
		var partialFound bool
		var partialItem item.Item
		var partialLoc item.Location
		for logIter := b.log.At(loc); logIter.Next(&partialItem); {
			if partialItem.Key >= key {
				partialFound = true
				partialLoc = logIter.CurrentLocation()
				break
			}
		}

		if partialFound {
			// we found an enrty in the log that is >= key.
			// resize the index entry to skip the entries before.
			b.idx.Set(loc.Key, item.Location{
				Key: partialItem.Key,
				Off: partialLoc.Off,
				Len: partialLoc.Len,
			})
			numDeleted += int(loc.Len - partialLoc.Len)
		} else {
			// nothing found, this index entry can be dropped.
			// TODO: can we do that while iterating?
			b.idx.Delete(loc.Key)
			numDeleted += int(loc.Len)
		}
	}

	return numDeleted, b.idxLog.Sync()
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

func (b *Bucket) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := 0
	iter := b.idx.Iter()
	for iter.Next() {
		fmt.Println(iter.Value())
		size += int(iter.Value().Len)
	}

	return size
}
