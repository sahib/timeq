package bucket

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
	"golang.org/x/exp/slog"
)

type Options struct {
	// MaxSkew defines how much a key may be shifted in case of duplicates.
	// This can lead to very small inaccuracies. Zero disables this.
	MaxSkew int
}

func DefaultOptions() Options {
	return Options{
		// for nanosecond timestamp keys this would be just 1μs:
		MaxSkew: 1e3,
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

	log, err := vlog.Open(filepath.Join(dir, "dat.log"))
	if err != nil {
		return nil, err
	}

	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil {
		return nil, fmt.Errorf("index load: %w", err)
	}

	idxLog, err := index.NewWriter(idxPath)
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
	if err := b.log.Sync(); err != nil {
		return err
	}

	if err := b.idxLog.Sync(); err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Close() error {
	if err := b.log.Close(); err != nil {
		return err
	}

	if err := b.idxLog.Close(); err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Push(items []item.Item) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	loc, err := b.log.Push(items)
	if err != nil {
		return err
	}

	skewLoc, skews := b.idx.SetSkewed(loc, b.opts.MaxSkew)
	if skews == b.opts.MaxSkew {
		// at least warn user since this might lead to lost data:
		slog.Warn("push: maximum skew was reached", "key", loc.Key, "skew", b.opts.MaxSkew)
	}

	if err := b.idxLog.Push(skewLoc); err != nil {
		return fmt.Errorf("push: index-log: %w", err)
	}

	return nil
}

func (b *Bucket) Pop(n int, dst []item.Item) ([]item.Item, int, error) {
	if n <= 0 {
		// technically that's a valid usecase.
		return nil, 0, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Check how many index entries we have to iterate until we can fill up
	// `dst` with `n` items. Alternatively, all of them have to be used.
	iter := b.idx.Iter()
	minEntriesNeeded := 0
	for count := 0; count < n && iter.Next(); count += int(iter.Value().Len) {
		minEntriesNeeded++
	}

	// Figure out what the highest key (excluding) of the index is that
	// we don't need anymore.
	var highestKey item.Key
	if iter.Next() {
		highestKey = iter.Value().Key
	} else {
		// iterator is at the end of index,
		// we have to take all of it. Use int64 max value.
		highestKey = item.Key((^int64(0)) >> 1)
	}

	// Collect all wal iterators that we need to fetch all keys.
	batchIters := make(vlog.LogIters, 0, minEntriesNeeded)
	for iter := b.idx.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key >= highestKey {
			break
		}

		batchIter := b.log.At(loc)
		batchIter.Next(nil)
		if err := batchIter.Err(); err != nil {
			// some error on read.
			return nil, 0, err
		}

		batchIters = append(batchIters, batchIter)
	}

	// Choose the lowest item of all iterators here and make sure the next loop
	// iteration will yield the next highest key.
	numAppends := 0
	heap.Init(batchIters)
	for len(dst) < n && !batchIters[0].Exhausted() {
		dst = append(dst, batchIters[0].Item())
		numAppends++

		batchIters[0].Next(nil)
		if err := batchIters[0].Err(); err != nil {
			return nil, 0, err
		}

		heap.Fix(batchIters, 0)
	}

	// NOTE: In theory we could also use fallocate(FALLOC_FL_ZERO_RANGE) on
	// ext4 to "put holes" into the log file where we read batches from to save
	// some space early. This would make sense only for very big buckets
	// though. We delete the whole bucket once it was completely exhausted
	// anyways.

	// Now since we've collected all data we need to delete
	for _, batchIter := range batchIters {
		currLoc := batchIter.CurrentLocation()
		if batchIter.Exhausted() {
			// this batch was fullly exhausted during Pop()
			b.idx.Delete(batchIter.Key())

			currLoc.Len = 0
			if err := b.idxLog.Push(currLoc); err != nil {
				return nil, 0, fmt.Errorf("idxlog: append delete: %w", err)
			}
		} else {
			// some keys were take from it, but not all (or none)
			b.idx.Set(currLoc.Key, currLoc)
			if err := b.idxLog.Push(currLoc); err != nil {
				return nil, 0, fmt.Errorf("idxlog: append begun: %w", err)
			}
		}
	}

	return dst, numAppends, b.idxLog.Sync()
}

func (b *Bucket) DeleteLowerThan(key item.Key) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.key >= key {
		return 0, nil
	}

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
		} else {
			// nothing found, this index entry can be dropped.
			// TODO: can we do that while iterating?
			b.idx.Delete(loc.Key)
		}
	}

	return 0, nil
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
		size += int(iter.Value().Len)
	}

	return size
}
