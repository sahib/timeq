package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
	"golang.org/x/sys/unix"
)

// TODO: Think about making the disk format more failproof. Ideas:
// - Add a block with magic number every N items so iter can seek to it
//   in case of errors.
// - Iters should automatically skip bad items.
// - Checksums?
// - Solomon Reed?

const itemHeaderSize = 12

type Log struct {
	path        string
	fd          *os.File
	mmap        []byte
	size        int64
	syncOnWrite bool
}

func nextSize(size int64) int64 {
	pageSize := int64(os.Getpagesize())
	currPages := size / pageSize

	// decide on how much to increase:
	var shift int
	var mb int64 = 1024 * 1024
	switch {
	default:
		// 8 pages per block
		shift = 3
	case size > 200*1024:
		// 16 pages per block
		shift = 4
	case size > (1 * mb):
		// 32 pages per block
		shift = 5
	case size > (10 * mb):
		// 64 pages per block
		shift = 6
	case size > (100 * mb):
		// 128 pages per block:
		shift = 7
	}

	// use shift to round to next page alignment:
	nextSize := (((currPages >> shift) + 1) << shift) * pageSize
	return nextSize
}

func Open(path string, syncOnWrite bool) *Log {
	return &Log{
		// this will lazy-initialze on first access:
		path:        path,
		syncOnWrite: syncOnWrite,
	}
}

func (l *Log) init() error {
	if len(l.mmap) > 0 {
		return nil
	}

	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR
	fd, err := os.OpenFile(l.path, flags, 0600)
	if err != nil {
		return fmt.Errorf("log: open: %w", err)
	}

	info, err := fd.Stat()
	if err != nil {
		fd.Close()
		return fmt.Errorf("log: stat: %w", err)
	}

	mmapSize := info.Size()
	if mmapSize == 0 {
		mmapSize = nextSize(0)
		if err := fd.Truncate(mmapSize); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
	}

	mmap, err := unix.Mmap(
		int(fd.Fd()),
		0,
		int(mmapSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED_VALIDATE,
	)

	if err != nil {
		fd.Close()
		return fmt.Errorf("log: mmap: %w", err)
	}

	l.size = info.Size()
	l.fd = fd
	l.mmap = mmap

	// read the initial size. We can't use the file size as we
	// pre-allocated the file to a certain length and we don't
	// know much of it was used. If we would use info.Size() here
	// we would waste some space since new pushes are written beyond
	// the truncated area.

	size, err := l.readActualSize()
	if err != nil {
		return fmt.Errorf("log: initial size: %w", err)
	}

	l.size = size
	return nil
}

func (l *Log) readActualSize() (int64, error) {
	iter := LogIter{
		// we're cheating a little here by trusting the iterator
		// to go not over the end, even if the Len is bogus.
		currOff: 0,
		currLen: ^item.Off(0),
		log:     l,
	}

	var size int64
	var it item.Item
	for iter.Next(&it) && len(it.Blob) > 0 {
		size += int64(len(it.Blob)) + itemHeaderSize
	}

	if err := iter.Err(); err != nil {
		return 0, fmt.Errorf("size: %w", err)
	}

	return size, nil

}

func (l *Log) writeItem(item item.Item) {
	binary.BigEndian.PutUint32(l.mmap[l.size+0:], uint32(len(item.Blob)))
	binary.BigEndian.PutUint64(l.mmap[l.size+4:], uint64(item.Key))
	copy(l.mmap[l.size+itemHeaderSize:], item.Blob)
}

func (l *Log) Push(items []item.Item) (item.Location, error) {
	if err := l.init(); err != nil {
		return item.Location{}, err
	}

	addSize := len(items) * itemHeaderSize
	for i := 0; i < len(items); i++ {
		addSize += len(items[i].Blob)
	}

	loc := item.Location{
		Key: items[0].Key,
		Off: item.Off(l.size),
		Len: item.Off(len(items)),
	}

	nextMmapSize := nextSize(l.size + int64(addSize))
	if nextMmapSize > int64(len(l.mmap)) {
		// currently mmapped region does not suffice,
		// allocate more space for it.
		if err := l.fd.Truncate(nextMmapSize); err != nil {
			return item.Location{}, fmt.Errorf("truncate: %w", err)
		}

		// If we're unlucky we gonna have to move it:
		m, err := unix.Mremap(l.mmap, int(nextMmapSize), unix.MREMAP_MAYMOVE)
		if err != nil {
			return item.Location{}, fmt.Errorf("remap: %w", err)
		}

		l.mmap = m
	}

	// copy the items to the file map:
	for i := 0; i < len(items); i++ {
		l.writeItem(items[i])
		l.size += int64(itemHeaderSize + len(items[i].Blob))
	}

	if err := l.Sync(false); err != nil {
		return item.Location{}, fmt.Errorf("sync: %w", err)
	}

	return loc, nil
}

func (l *Log) At(loc item.Location) (LogIter, error) {
	if err := l.init(); err != nil {
		return LogIter{}, err
	}

	return LogIter{
		key:     loc.Key,
		currOff: loc.Off,
		currLen: loc.Len,
		log:     l,
	}, nil
}

func (l *Log) readItemAt(off item.Off, it *item.Item) error {
	if int64(off)+itemHeaderSize >= l.size {
		return fmt.Errorf("log: bad offset: off=%d size=%d (header too big)", off, l.size)
	}

	// parse header:
	len := binary.BigEndian.Uint32(l.mmap[off+0:])
	key := binary.BigEndian.Uint64(l.mmap[off+4:])

	if len > 4*1024*1024 {
		return fmt.Errorf("log: allocation too big for one value: %d", len)
	}

	if int64(off)+itemHeaderSize+int64(len) > l.size {
		return fmt.Errorf(
			"log: bad offset: %d+%d >= %d (payload too big)",
			off,
			len,
			l.size,
		)
	}

	// NOTE: We directly slice the memory map here. This means that the caller
	// has to copy the slice if he wants to save it somewhere as we might
	// overwrite, unmap or resize the underlying memory at a later point.
	// Caller can use item.Copy() or items.Copy() to obtain a copy.
	blobOff := off + itemHeaderSize
	*it = item.Item{
		Key:  item.Key(key),
		Blob: l.mmap[blobOff : blobOff+item.Off(len)],
	}

	return nil
}

// GenerateIndex produces an index from the data in the value log.
// It's main use is to re-generate the index in case the index file
// is damaged or broken in some way. The resulting index is likely
// not the same as before, but probably a bit cleaner.
func (l *Log) GenerateIndex() (*btree.Map[item.Key, item.Location], error) {
	if err := l.init(); err != nil {
		return nil, err
	}

	iter := LogIter{
		// we're cheating a little here by trusting the iterator
		// to go not over the end, even if the Len is bogus.
		currOff: 0,
		currLen: ^item.Off(0),
		log:     l,
	}

	var tree btree.Map[item.Key, item.Location]
	var it item.Item
	var prevLoc item.Location
	var lastLoc item.Location
	var isInitialItem bool = true

	// Go over the data and try to find runs of data that are sorted in
	// ascending order. Each deviant item is the start of a new run.
	for iter.Next(&it) {
		if prevLoc.Key > it.Key {
			tree.Set(lastLoc.Key, lastLoc)
			lastLoc.Off = prevLoc.Off
			lastLoc.Key = it.Key
			lastLoc.Len = 0
		}

		lastLoc.Len++
		if isInitialItem {
			lastLoc.Key = it.Key
			isInitialItem = false
		}

		prevLoc.Off += itemHeaderSize + item.Off(len(it.Blob))
		prevLoc.Key = it.Key
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	// also pick up last run in the data:
	if lastLoc.Len > 0 {
		tree.Set(lastLoc.Key, lastLoc)
	}

	return &tree, nil

}

func (l *Log) Sync(force bool) error {
	if !l.syncOnWrite && !force {
		return nil
	}

	if len(l.mmap) == 0 {
		return nil
	}

	return unix.Msync(l.mmap, unix.MS_SYNC)
}

func (l *Log) Close() error {
	if len(l.mmap) == 0 {
		// not yet loaded.
		return nil
	}

	syncErr := unix.Msync(l.mmap, unix.MS_SYNC)
	unmapErr := unix.Munmap(l.mmap)
	closeErr := l.fd.Close()
	return errors.Join(syncErr, unmapErr, closeErr)
}
