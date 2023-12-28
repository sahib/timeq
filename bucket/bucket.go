package bucket

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
)

const (
	DataLogName = "dat.log"
)

type Index struct {
	Log *index.Writer
	Mem *index.Index
}

type Bucket struct {
	dir  string
	key  item.Key
	log  *vlog.Log
	opts Options

	// TODO: Support lazy loading indexes later on?
	indexes map[string]Index
}

var (
	ErrNoSuchConsumer = errors.New("no consumer with that name")
)

func (b *Bucket) idxForConsumer(consumer string) (Index, error) {
	idx, ok := b.indexes[consumer]
	if !ok {
		return idx, ErrNoSuchConsumer
	}

	return idx, nil
}

func recoverIndexFromLog(opts *Options, log *vlog.Log, idxPath string) (*index.Index, error) {
	// We try to re-generate the index from the value log if
	// the index is damaged or missing (in case the value log has some entries).
	//
	// Since we have all the keys and offsets there too,
	// we should be able to recover from that. This will not consider already deleted
	// entries of course, as those are marked in the index file, but not in the value log.
	// It's better to replay old values twice then to loose values.

	var memErr error
	mem, memErr := index.FromVlog(log)
	if memErr != nil {
		// not much we can do for that case:
		return nil, fmt.Errorf("index load failed & could not regenerate: %w", memErr)
	}

	// TODO: what is this doing?
	// if mem.Len() > 0 {
	// 	if err == nil {
	// 		opts.Logger.Printf("index is empty, but log is not (%s)", idxPath)
	// 	} else {
	// 		opts.Logger.Printf("failed to load index %s: %v", idxPath, err)
	// 	}
	// }

	if err := os.Remove(idxPath); err != nil {
		return nil, fmt.Errorf("index failover: could not remove broken index: %w", err)
	}

	// We should write the repaired index after repair, so we don't have to do it
	// again if this gets interrupted. Also this allows us to just push to the index
	// instead of having logic later that writes the not yet written part.
	if err := index.WriteIndex(mem, idxPath); err != nil {
		return nil, fmt.Errorf("index: write during recover did not work")
	}

	if ln := mem.Len(); ln > 0 {
		opts.Logger.Printf("recovered index with %d entries", ln)
	}

	return mem, nil
}

func idxPath(dir, consumer string) string {
	// TODO: Do we use this logic somewhere else?
	var idxName string
	if consumer == "" {
		idxName = "idx.log"
	} else {
		idxName = consumer + ".idx.log"
	}

	return filepath.Join(dir, idxName)
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

	idxEnts, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	indexes := make(map[string]Index, len(idxEnts)-1)
	for _, idxEnt := range idxEnts {
		idxName := idxEnt.Name()
		if !strings.HasSuffix(idxName, "idx.log") {
			// not a index file.
			continue
		}

		idxPath := filepath.Join(dir, idxName)
		idx, err := index.Load(idxPath)
		if err != nil || (idx.NEntries() == 0 && !log.IsEmpty()) {
			idx, err = recoverIndexFromLog(&opts, log, idxPath)
			if err != nil {
				return nil, err
			}
		}

		idxLog, err := index.NewWriter(idxPath, opts.SyncMode&SyncIndex > 0)
		if err != nil {
			return nil, fmt.Errorf("index writer: %w", err)
		}

		indexes[index.ConsumerNameFromBasename(idxName)] = Index{
			Log: idxLog,
			Mem: idx,
		}
	}

	key, err := item.KeyFromString(filepath.Base(dir))
	if err != nil {
		return nil, err
	}

	return &Bucket{
		dir:     dir,
		key:     item.Key(key),
		log:     log,
		indexes: indexes,
		opts:    opts,
	}, nil
}

func (b *Bucket) Sync(force bool) error {
	err := b.log.Sync(force)
	for _, idx := range b.indexes {
		err = errors.Join(err, idx.Log.Sync(force))
	}

	return err
}

func (b *Bucket) Close() error {
	err := b.log.Close()
	for _, idx := range b.indexes {
		err = errors.Join(err, idx.Log.Close())
	}

	return err
}

func recoverMmapError(dstErr *error) {
	// See comment in Open().
	// NOTE: calling recover() is surprisingly quite expensive.
	// Do not call in this loops.
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

	for name, idx := range b.indexes {
		idx.Mem.Set(loc)
		if err := idx.Log.Push(loc, idx.Mem.Trailer()); err != nil {
			return fmt.Errorf("push: index-log: %s: %w", name, err)
		}
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
	if !batchIter.Next() {
		// might be empty or I/O error:
		return false, batchIter.Err()
	}

	batchIters.Push(batchIter)
	return !idxIter.Next(), nil
}

func (b *Bucket) Pop(n int, dst item.Items, consumer string) (item.Items, int, error) {
	if n <= 0 {
		// technically that's a valid use case.
		return dst, 0, nil
	}

	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return dst, 0, err
	}

	iters, items, npopped, err := b.peek(n, dst, idx.Mem)
	if err != nil {
		return items, npopped, err
	}

	if iters != nil {
		if err := b.popSync(idx, iters); err != nil {
			return items, npopped, err
		}
	}

	return items, npopped, nil
}

func (b *Bucket) Peek(n int, dst item.Items, consumer string) (item.Items, int, error) {
	if n <= 0 {
		return dst, 0, nil
	}

	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return dst, 0, err
	}

	_, items, npopped, err := b.peek(n, dst, idx.Mem)
	return items, npopped, err
}

// Move moves data between two buckets in a safer way. In case of
// crashes the data might be present in the destination queue, but is
// not yet deleted from the source queue. Callers should be ready to
// handle duplicates.
func (b *Bucket) Move(n int, dst item.Items, dstBuck *Bucket, consumer string) (item.Items, int, error) {
	if n <= 0 {
		// technically that's a valid use case.
		return dst, 0, nil
	}

	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return nil, 0, err
	}

	iters, items, npopped, err := b.peek(n, dst, idx.Mem)
	if err != nil {
		return items, npopped, err
	}

	if dstBuck != nil {
		if err := dstBuck.Push(items[len(dst):]); err != nil {
			return items, npopped, err
		}
	}

	return items, npopped, b.popSync(idx, iters)
}

// peek reads from the bucket, but does not mark the elements as deleted yet.
func (b *Bucket) peek(n int, dst item.Items, idx *index.Index) (batchIters *vlog.Iters, outItems item.Items, npopped int, outErr error) {
	defer recoverMmapError(&outErr)

	// Fetch the lowest entry of the index:
	idxIter := idx.Iter()
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
		// in case of broken index or WAL.
		return nil, dst, 0, err
	}

	// Choose the lowest item of all iterators here and make sure the next loop
	// iteration will yield the next highest key.
	var numAppends int
	for numAppends < n && !(*batchIters)[0].Exhausted() {
		var currIter *vlog.Iter = &(*batchIters)[0]
		dst = append(dst, currIter.Item())
		numAppends++

		// advance current batch iter. We will make sure at the
		// end of the loop that the currently first one gets sorted
		// correctly if it turns out to be out-of-order.
		currIter.Next()
		currKey := currIter.Item().Key
		if err := currIter.Err(); err != nil {
			return nil, dst, 0, err
		}

		// Check the exhausted state as heap.Fix might change the sorting.
		// NOTE: we could do heap.Pop() here to pop the exhausted iters away,
		// but we need the exhausted iters too give them to popSync() later.
		currIsExhausted := currIter.Exhausted()

		// Repair sorting of the heap as we changed the value of the first iter.
		batchIters.Fix(0)

		// index batch entries might be overlapping. We need to check if the
		// next entry in the index needs to be taken into account for the next
		// iteration. For this we compare the next index entry to the
		// supposedly next batch value.
		if !indexExhausted {
			nextLoc := idxIter.Value()
			if currIsExhausted || nextLoc.Key <= currKey {
				indexExhausted, err = b.addIter(batchIters, &idxIter)
				if err != nil {
					return nil, dst, 0, err
				}
			}
		}
	}

	return batchIters, dst, numAppends, nil
}

func (b *Bucket) popSync(idx Index, batchIters *vlog.Iters) error {
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
			idx.Mem.Set(currLoc)

			if err := idx.Log.Push(currLoc, idx.Mem.Trailer()); err != nil {
				return fmt.Errorf("idxlog: append begun: %w", err)
			}
		}

		// Make sure the previous batch index entry gets deleted:
		idx.Mem.Delete(batchIter.FirstKey())
		deadLoc := item.Location{
			Key: batchIter.FirstKey(),
			Len: 0,
			Off: 0,
		}
		if err := idx.Log.Push(deadLoc, idx.Mem.Trailer()); err != nil {
			return fmt.Errorf("idxlog: append begun: %w", err)
		}
	}

	return idx.Log.Sync(false)
}

func (b *Bucket) DeleteLowerThan(consumer string, key item.Key) (ndeleted int, outErr error) {
	defer recoverMmapError(&outErr)

	if b.key >= key {
		// this bucket is safe from the clear.
		return 0, nil
	}

	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return 0, err
	}

	lenBefore := idx.Mem.Len()

	var pushErr error
	var deleteEntries []item.Key
	var partialSetEntries []item.Location
	for iter := idx.Mem.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key >= key {
			// this index entry may live untouched.
			break
		}

		// we need to check until what point we need to delete.
		var partialFound bool
		var partialLoc item.Location

		logIter := b.logAt(loc)
		for logIter.Next() {
			partialItem := logIter.Item()
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
				idx.Log.Push(partialLoc, index.Trailer{
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
		idx.Mem.Delete(key)
		pushErr = errors.Join(pushErr, idx.Log.Push(item.Location{
			Key: key,
			Len: 0,
			Off: 0,
		}, index.Trailer{
			TotalEntries: lenBefore - item.Off(ndeleted),
		}))
	}

	for _, loc := range partialSetEntries {
		idx.Mem.Set(loc)
	}

	return ndeleted, errors.Join(pushErr, idx.Log.Sync(false))
}

func (b *Bucket) AllEmpty() bool {
	for _, idx := range b.indexes {
		if idx.Mem.Len() > 0 {
			return false
		}
	}

	return true
}

func (b *Bucket) Empty(consumer string) (bool, error) {
	// TODO: Split in AllEmpty() und Empty(name)
	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return false, err
	}

	return idx.Mem.Len() == 0, nil
}

func (b *Bucket) Key() item.Key {
	return b.key
}

func (b *Bucket) Trailer(consumer string) index.Trailer {
	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return index.Trailer{}
	}

	return idx.Mem.Trailer()
}

func (b *Bucket) Len(consumer string) (int, error) {
	idx, err := b.idxForConsumer(consumer)
	if err != nil {
		return 0, err
	}

	return int(idx.Mem.Len()), nil
}

func (b *Bucket) Fork(src, dst string) error {
	// TODO:
	// 1. Copy some index (which one?!) to name.idx
	// 2. Either return a Bucket API that uses that or make every op take a name.
	//    (latter one would be less efficient?)
	srcIdx, err := b.idxForConsumer(src)
	if err != nil {
		return err
	}

	// If we fork to `dst`, then dst should better not exist.
	if _, err := b.idxForConsumer(dst); err == nil {
		return errors.New("dst already exists")
	}

	dstPath := idxPath(b.dir, dst)
	if err := index.WriteIndex(srcIdx.Mem, dstPath); err != nil {
		return err
	}

	dstIdxLog, err := index.NewWriter(dstPath, b.opts.SyncMode&SyncIndex > 0)
	if err != nil {
		return err
	}

	b.indexes[dst] = Index{
		Log: dstIdxLog,
		Mem: srcIdx.Mem.Copy(),
	}
	return nil
}

func (b *Bucket) RemoveFork(name string) error {
	idx, err := b.idxForConsumer(name)
	if err != nil {
		return err
	}

	dstPath := idxPath(b.dir, name)
	delete(b.indexes, name)
	return errors.Join(
		idx.Log.Close(),
		os.Remove(dstPath),
	)
}

func (b *Bucket) Consumers() []string {
	consumers := make([]string, 0, len(b.indexes))
	for consumer := range b.indexes {
		consumers = append(consumers, consumer)
	}

	return consumers
}

func filterIsNotExist(err error) error {
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func removeBucketDir(dir string) error {
	// We do this here because os.RemoveAll() is a bit more expensive,
	// as it does some extra syscalls and some portability checks that
	// we do not really need. Just delete them explicitly.
	//
	// We also don't care if the files actually existed, as long as they
	// are gone after this function call.
	return errors.Join(
		filterIsNotExist(os.Remove(filepath.Join(dir, "idx.log"))),
		filterIsNotExist(os.Remove(filepath.Join(dir, "dat.log"))),
		filterIsNotExist(os.Remove(dir)),
	)
}
