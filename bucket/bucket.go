package bucket

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/renameio"
	"github.com/sahib/lemurq/index"
	"github.com/sahib/lemurq/item"
	"github.com/sahib/lemurq/vlog"
)

// TODO: add locking and evauluate if we need locking in index/vlog

const (
	maxBatchSize = 50
)

func indexNameFromID(id int) string {
	return fmt.Sprintf("%08X.idx", id)
}

type Bucket struct {
	mu         sync.Mutex
	dir        string
	key        item.Key
	wal        *vlog.Log
	indexCount int
	fullIndex  *index.Index
	batchIndex *index.Index
}

func Open(dir string) (*Bucket, error) {
	log, err := vlog.Open(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, err
	}

	indexPaths, err := filepath.Glob(filepath.Join(dir, "*.idx"))
	if err != nil {
		return nil, err
	}

	key, err := item.KeyFromString(filepath.Base(dir))
	if err != nil {
		return nil, err
	}

	// TODO: Load full index here.

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	return &Bucket{
		dir:        dir,
		key:        item.Key(key),
		wal:        log,
		indexCount: len(indexPaths),
		batchIndex: index.Empty(),
	}, nil
}

func (b *Bucket) Push(items []item.Item) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	loc, err := b.wal.Push(items)
	if err != nil {
		return err
	}

	// TODO:
	// Append to index-wal (+ sync)
	// Update to in-memory index (do duplicate check here?)

	b.batchIndex.Set(loc.Key, loc)
	b.fullIndex.Set(loc.Key, loc)

	// TODO: Wouldn't it be better to just continously write to that file?
	if b.batchIndex.Len() > maxBatchSize {
		if err := b.flushBatchIndex(); err != nil {
			return err
		}
	}

	return b.flushWAL()
}

func (b *Bucket) flushBatchIndex() error {
	flags := os.O_TRUNC | os.O_CREATE | os.O_WRONLY
	fullPath := filepath.Join(b.dir, indexNameFromID(b.indexCount+1))
	fd, err := os.OpenFile(fullPath, flags, 0600)
	if err != nil {
		return err
	}

	defer fd.Close()

	// TODO: Use renameio here too?
	if err := b.batchIndex.Marshal(fd); err != nil {
		return err
	}

	b.indexCount++
	b.batchIndex = index.Empty()
	return fd.Sync()
}

func (b *Bucket) loadFullIndex() error {
	if b.fullIndex != nil {
		// nothing to do.
		return nil
	}

	// Make sure the batch index will be part of the full index:
	if err := b.flushBatchIndex(); err != nil {
		return err
	}

	fullIndex, err := index.Load(b.dir)
	if err != nil {
		return err
	}

	b.fullIndex = fullIndex
	return nil
}

func (b *Bucket) Pop(n int, dst []item.Item) ([]item.Item, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if n <= 0 {
		// technically that's a valid usecase.
		return nil, nil
	}

	// Pop() requires a full picture of the index data in the bucket.
	// Push() may be dumb and just marshal a small amount of the data.
	// TODO: Now that the index became quite small, can't we just
	// have a stored index + index-wal? Would be easier than a full merge algo.
	if err := b.loadFullIndex(); err != nil {
		return nil, err
	}

	// Check how many index entries we have to iterate until we can fill up
	// `dst` with `n` items. Alternatively, all of them have to be used.
	iter := b.fullIndex.Iter()
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
	for iter := b.fullIndex.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key >= highestKey {
			break
		}

		batchIter := b.wal.At(loc)
		batchIter.Next()
		if err := batchIter.Err(); err != nil {
			// some error on read.
			return nil, err
		}

		batchIters = append(batchIters, batchIter)
	}

	// Choose the lowest item of all iterators here and make sure the next loop
	// iteration will yield the next highest key.
	heap.Init(batchIters)
	for len(dst) < n && !batchIters[0].Exhausted() {
		dst = append(dst, batchIters[0].Item())
		batchIters[0].Next()
		if err := batchIters[0].Err(); err != nil {
			return nil, err
		}

		heap.Fix(batchIters, 0)
	}

	// Now since we've collected all data we need to delete
	for _, batchIter := range batchIters {
		if batchIter.Exhausted() {
			// this batch was fullly exhausted during Pop()
			b.fullIndex.Delete(batchIter.Key())
			b.batchIndex.Delete(batchIter.Key())
		} else {
			currLoc := batchIter.CurrentLocation()
			b.fullIndex.Set(currLoc.Key, currLoc)
			b.batchIndex.Set(currLoc.Key, currLoc)
		}
	}

	if err := b.flushAndMergeFullIndex(); err != nil {
		return dst, err
	}

	return dst, nil
}

func (b *Bucket) flushWAL() error {
	return b.wal.Flush()
}

// flushAndMergeFullIndex realizes any writes made to the index (as done in Pop/Push)
func (b *Bucket) flushAndMergeFullIndex() error {
	// TODO: Only flush if changes were made (i.e. dirty flag)?
	oldIndexes, err := filepath.Glob(filepath.Join(b.dir, "*.idx"))
	if err != nil {
		return fmt.Errorf("bucket: flush: glob: %w", err)
	}

	newIndexName := fmt.Sprintf("%08X.idx", len(oldIndexes)+1)
	fd, err := renameio.TempFile(b.dir, newIndexName)
	if err != nil {
		return fmt.Errorf("bucket: flush: temp: %w", err)
	}
	defer fd.Cleanup()

	if err := fd.CloseAtomicallyReplace(); err != nil {
		return fmt.Errorf("bucket: flush: %w", err)
	}

	for _, oldIndex := range oldIndexes {
		// no error check, because removing old index is just an optimization.
		os.Remove(oldIndex)
	}

	return nil
}
