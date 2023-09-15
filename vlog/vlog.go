package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/sahib/timeq/item"
	"golang.org/x/sys/unix"
)

// TODO: Think about making the disk format more failproof. Ideas:
// - Add a block with magic number every N items so iter can seek to it
//   in case of errors.
// - Iters should automatically skip bad items.
// - Checksums?
// - Solomon Reed?

const ItemHeaderSize = 12

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

func Open(path string, syncOnWrite bool) (*Log, error) {
	l := &Log{
		// this will lazy-initialze on first access:
		path:        path,
		syncOnWrite: syncOnWrite,
	}
	return l, l.init()
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

	// give OS a hint that we will likely need that memory soon:
	_ = unix.Madvise(mmap, unix.MADV_WILLNEED)

	l.size = info.Size()
	l.fd = fd
	l.mmap = mmap

	// read the initial size. We can't use the file size as we
	// pre-allocated the file to a certain length and we don't
	// know much of it was used. If we would use info.Size() here
	// we would waste some space since new pushes are written beyond
	// the truncated area. Just shrink to the last written data.
	l.size = l.shrink()
	return nil
}

func (l *Log) shrink() int64 {
	// shrink assumes that 1 byte is written after the
	idx := l.size - 1
	for ; idx >= 0 && l.mmap[idx] == 0; idx-- {
	}

	return idx + 1
}

func (l *Log) writeItem(item item.Item) {
	off := l.size
	binary.BigEndian.PutUint32(l.mmap[off:], uint32(len(item.Blob)))
	off += 4
	binary.BigEndian.PutUint64(l.mmap[off:], uint64(item.Key))
	off += 8
	off += int64(copy(l.mmap[off:], item.Blob))
	l.mmap[off] = 0xFF // end of each item
}

func (l *Log) Push(items []item.Item) (loc item.Location, err error) {
	addSize := len(items) * ItemHeaderSize
	for i := 0; i < len(items); i++ {
		addSize += len(items[i].Blob)
	}

	loc = item.Location{
		Key: items[0].Key,
		Off: item.Off(l.size),
		Len: item.Off(len(items)),
	}

	nextMmapSize := nextSize(l.size + int64(addSize))
	if nextMmapSize > int64(len(l.mmap)) {
		if nextMmapSize > int64(^item.Off(0)) {
			// the mmap size is bigger than what our offsets can handle.
			// we have to error out.
			err = fmt.Errorf("wal-file bigger than 4GB")
			return
		}

		// currently mmapped region does not suffice,
		// allocate more space for it.
		if err = l.fd.Truncate(nextMmapSize); err != nil {
			err = fmt.Errorf("truncate: %w", err)
			return
		}

		// If we're unlucky we gonna have to move it:
		l.mmap, err = unix.Mremap(l.mmap, int(nextMmapSize), unix.MREMAP_MAYMOVE)
		if err != nil {
			err = fmt.Errorf("remap: %w", err)
			return
		}
	}

	// copy the items to the file map:
	for i := 0; i < len(items); i++ {
		l.writeItem(items[i])
		l.size += int64(ItemHeaderSize+len(items[i].Blob)) + 1
	}

	if err = l.Sync(false); err != nil {
		err = fmt.Errorf("sync: %w", err)
		return
	}

	return loc, nil
}

func (l *Log) At(loc item.Location) LogIter {
	return LogIter{
		key:     loc.Key,
		currOff: loc.Off,
		currLen: loc.Len,
		log:     l,
	}
}

func (l *Log) readItemAt(off item.Off, it *item.Item) (err error) {
	if int64(off)+ItemHeaderSize >= l.size {
		// NOTE: This migh happen in valid cases: i.e. when the initial size is determined we iterate
		// until the end of the WAL. If the last item happens to be cut off we would throw an error
		// here. In this case we just want to stop iterating.
		return nil
	}

	// parse header:
	len := binary.BigEndian.Uint32(l.mmap[off+0:])
	key := binary.BigEndian.Uint64(l.mmap[off+4:])

	if len > 64*1024*1024 {
		// fail-safe if the size field is corrupt:
		return fmt.Errorf("log: allocation too big for one value: %d", len)
	}

	if int64(off)+ItemHeaderSize+int64(len) > l.size {
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
	blobOff := off + ItemHeaderSize
	*it = item.Item{
		Key:  item.Key(key),
		Blob: l.mmap[blobOff : blobOff+item.Off(len)],
	}

	return nil
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
