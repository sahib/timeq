package bucket

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
)

const (
	DataLogName = "dat.log"
)

type Bucket struct {
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
	//	// * ftruncate() grows a file beyond the available space without error.
	//   Since the new "space" are just zeros that do not take any physical
	//   space this makes sense.
	// * Accessing this mapped memory however will cause the filesystem to actually
	//   try to serve some more pages, which fails as it's full (would also happen on
	//   hardware failure or similar)
	// * This causes a SIGBUS to be send to our process. By default Go crashes the program
	//   and prints a stack trace. Changing this to a recoverable panic allows us to intervene
	//   and continue execution with a proper error return.
	//
	// Other errors like suddenly deleted database files might cause this too.
	// The drawback of this approach is that this might cause issues if the calling processes
	// also sets this option (but with False!). If this turns out to be a problem we have to
	// introduce an option to disable this error handling.
	debug.SetPanicOnFault(true)

	defer recoverMmapError(&outErr)

	logPath := filepath.Join(dir, DataLogName)
	log, err := vlog.Open(logPath, opts.SyncMode&SyncData > 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil || (idx.NEntries() == 0 && !log.IsEmpty()) {
		// We try to re-generate the index from the value log if
		// the index is damaged or missing (in case the value log has some entries).
		//
		// Since we have all the keys and offsets there too,
		// we should be able to recover from that.

		var idxErr error
		idx, idxErr = index.FromVlog(log)
		if idxErr != nil {
			// not much we can do for that case:
			return nil, fmt.Errorf("index load failed & could not regenerate: %w", idxErr)
		}

		if idx.Len() > 0 {
			if err == nil {
				opts.Logger.Printf("index is empty, but log is not (%s)", idxPath)
			} else {
				opts.Logger.Printf("failed to load index %s: %v", idxPath, err)
			}
		}

		if err := os.Remove(idxPath); err != nil {
			return nil, fmt.Errorf("index failover: could not remove broken index: %w", err)
		}

		// We should write the repaired index after repair, so we don't have to do it
		// again if this gets interrupted. Also this allows us to just push to the index
		// instead of having logic later that writes the not yet written part.
		if err := index.WriteIndex(idx, idxPath); err != nil {
			return nil, fmt.Errorf("index: write during recover did not work")
		}

		if idx.Len() > 0 {
			opts.Logger.Printf("recovered index with %d entries", idx.Len())
		}
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
	return errors.Join(b.log.Sync(force), b.idxLog.Sync(force))
}

func (b *Bucket) Close() error {
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
		*dstErr = fmt.Errorf("panic (check: enough space left / file issues): %v - trace:\n%s", recErr, string(debug.Stack()))
	}
}

// Push expects pre-sorted items!
func (b *Bucket) Push(items item.Items) (outErr error) {
	if len(items) == 0 {
		return nil
	}

	defer recoverMmapError(&outErr)

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

func (b *Bucket) logAt(loc item.Location) vlog.Iter {
	continueOnErr := b.opts.ErrorMode != ErrorModeAbort
	return b.log.At(loc, continueOnErr)
}

// addIter adds a new batchIter to `batchIters` and advances the idxIter.
func (b *Bucket) addIter(batchIters *vlog.Iters, idxIter *index.Iter) (bool, error) {
	loc := idxIter.Value()
	batchIter := b.logAt(loc)
	if !batchIter.Next(nil) {
		// might be empty or I/O error:
		return false, batchIter.Err()
	}

	heap.Push(batchIters, batchIter)
	return !idxIter.Next(), nil
}

func (b *Bucket) Pop(n int, dst item.Items) (item.Items, int, error) {
	if n <= 0 {
		// technically that's a valid use case.
		return dst, 0, nil
	}

	iters, items, npopped, err := b.peek(n, dst)
	if err != nil {
		return items, npopped, err
	}

	if iters != nil {
		if err := b.popSync(iters); err != nil {
			return items, npopped, err
		}
	}

	return items, npopped, nil
}

func (b *Bucket) Peek(n int, dst item.Items) (item.Items, int, error) {
	if n <= 0 {
		return dst, 0, nil
	}

	_, items, npopped, err := b.peek(n, dst)
	return items, npopped, err
}

// Move moves data between two buckets in a safer way. In case of
// crashes the data might be present in the destination queue, but is
// not yet deleted from the source queue. Callers should be ready to
// handle duplicates.
func (b *Bucket) Move(n int, dst item.Items, dstBuck *Bucket) (item.Items, int, error) {
	if n <= 0 {
		// technically that's a valid usecase.
		return dst, 0, nil
	}

	iters, items, npopped, err := b.peek(n, dst)
	if err != nil {
		return items, npopped, err
	}

	if dstBuck != nil {
		if err := dstBuck.Push(items[len(dst):]); err != nil {
			return items, npopped, err
		}
	}

	return items, npopped, b.popSync(iters)
}

// peek reads from the bucket, but does not mark the elements as deleted yet.
func (b *Bucket) peek(n int, dst item.Items) (batchIters *vlog.Iters, outItems item.Items, npopped int, outErr error) {
	defer recoverMmapError(&outErr)

	// Fetch the lowest entry of the index:
	idxIter := b.idx.Iter()
	if !idxIter.Next() {
		// The index is empty. Nothing to pop.
		return nil, dst, 0, nil
	}

	// initialize with first batch iter:
	batchItersSlice := make(vlog.Iters, 0, 1)
	batchIters = &batchItersSlice
	indexExhausted, err := b.addIter(batchIters, &idxIter)
	if err != nil {
		return nil, dst, 0, err
	}

	if len(*batchIters) == 0 {
		// this should not happen normally, but can possibly
		// in case of broken index or wal.
		return nil, dst, 0, err
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
			return nil, dst, 0, err
		}

		// Check the exhausted state as heap.Fix might change the sorting.
		// NOTE: we could do heap.Pop() here to pop the exhausted iters away,
		// but we need the exhausted iters too give them to popSync() later.
		currIsExhausted := (*batchIters)[0].Exhausted()

		// Repair sorting of the heap as we changed the value of the first iter.
		heap.Fix(batchIters, 0)

		// index batch entries might be overlapping. We need to check if the
		// next entry in the index needs to be taken into account for the next
		// iteration. For this we compare the next index entry to the
		// supposedly next batch value.
		if !indexExhausted {
			nextLoc := idxIter.Value()
			if currIsExhausted || nextLoc.Key <= nextItem.Key {
				indexExhausted, err = b.addIter(batchIters, &idxIter)
				if err != nil {
					return nil, dst, 0, err
				}
			}
		}
	}

	return batchIters, dst, numAppends, nil
}

func (b *Bucket) popSync(batchIters *vlog.Iters) error {
	if batchIters == nil || len(*batchIters) == 0 {
		return nil
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
				return fmt.Errorf("idxlog: append begun: %w", err)
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
			return fmt.Errorf("idxlog: append begun: %w", err)
		}
	}

	return b.idxLog.Sync(false)
}

func (b *Bucket) DeleteLowerThan(key item.Key) (ndeleted int, outErr error) {
	defer recoverMmapError(&outErr)

	if b.key >= key {
		// this bucket is safe from the clear.
		return 0, nil
	}

	lenBefore := b.idx.Len()

	var pushErr error
	var deleteEntries []item.Key
	var partialSetEntries []item.Location
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

		logIter := b.logAt(loc)
		for logIter.Next(&partialItem) {
			partialLoc = logIter.CurrentLocation()
			if partialItem.Key >= key {
				partialFound = true
				break
			}
		}

		if partialFound {
			// we found an entry in the log that is >= key.
			// resize the index entry to skip the entries before.
			// we should be careful not to mutate the map during iteration:
			// https://github.com/tidwall/btree/issues/39
			partialSetEntries = append(partialSetEntries, partialLoc)
			ndeleted += int(loc.Len - partialLoc.Len)
			pushErr = errors.Join(
				pushErr,
				b.idxLog.Push(partialLoc, index.Trailer{
					TotalEntries: lenBefore - item.Off(ndeleted),
				}),
			)
		} else {
			// nothing found, this index entry can be dropped.
			ndeleted += int(loc.Len)
		}

		deleteEntries = append(deleteEntries, loc.Key)
	}

	for _, key := range deleteEntries {
		b.idx.Delete(key)
		pushErr = errors.Join(pushErr, b.idxLog.Push(item.Location{
			Key: key,
			Len: 0,
			Off: 0,
		}, index.Trailer{
			TotalEntries: lenBefore - item.Off(ndeleted),
		}))
	}

	for _, loc := range partialSetEntries {
		b.idx.Set(loc)
	}

	return ndeleted, errors.Join(pushErr, b.idxLog.Sync(false))
}

func (b *Bucket) Empty() bool {
	return b.idx.Len() == 0
}

func (b *Bucket) Key() item.Key {
	return b.key
}

func (b *Bucket) Len() int {
	return int(b.idx.Len())
}
