package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/sahib/timeq/item"
	"golang.org/x/sys/unix"
)

type Log struct {
	path        string
	fd          *os.File
	mmap        []byte
	size        int64
	syncOnWrite bool
}

var PageSize int64 = 4096

func init() {
	PageSize = int64(os.Getpagesize())
}

func nextSize(size int64) int64 {
	if size < 0 {
		return 0
	}

	currPages := size / PageSize

	// decide on how much to increase:
	var shift int
	const mb int64 = 1024 * 1024
	switch {
	case size >= (100 * mb):
		// 128 pages per block:
		shift = 7
	case size >= (10 * mb):
		// 64 pages per block
		shift = 6
	case size >= (1 * mb):
		// 32 pages per block
		shift = 5
	case size >= 200*1024:
		// 16 pages per block
		shift = 4
	default:
		// 8 pages per block
		shift = 3
	}

	// use shift to round to next page alignment:
	nextSize := (((currPages >> shift) + 1) << shift) * PageSize
	return nextSize
}

func Open(path string, syncOnWrite bool) (*Log, error) {
	l := &Log{
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
	// we take advantage of the end marker appended to each
	// log entry. Since ftruncate() will always pad with zeroes
	// it's easy for us to find the beginning of the file.
	idx := l.size - 1
	for ; idx >= 0 && l.mmap[idx] == 0; idx-- {
	}

	return idx + 1
}

func (l *Log) writeItem(it item.Item) {
	off := l.size
	binary.BigEndian.PutUint32(l.mmap[off:], uint32(len(it.Blob)))
	off += 4
	binary.BigEndian.PutUint64(l.mmap[off:], uint64(it.Key))
	off += 8
	off += int64(copy(l.mmap[off:], it.Blob))

	// add trailer mark:
	l.mmap[off+0] = 0xFF
	l.mmap[off+1] = 0xFF
	l.size = off + item.TrailerSize
}

func (l *Log) Push(items item.Items) (loc item.Location, err error) {
	addSize := items.StorageSize()

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
	}

	if err = l.Sync(false); err != nil {
		err = fmt.Errorf("sync: %w", err)
		return
	}

	return loc, nil
}

func (l *Log) At(loc item.Location, continueOnErr bool) LogIter {
	return LogIter{
		firstKey:      loc.Key,
		currOff:       loc.Off,
		currLen:       loc.Len,
		log:           l,
		continueOnErr: continueOnErr,
	}
}

func (l *Log) findNextItem(off item.Off) item.Off {
	offSize := item.Off(l.size)

	for idx := off + 1; idx < offSize-1; idx++ {
		if l.mmap[idx] == 0xFF && l.mmap[idx+1] == 0xFF {
			// we found a marker.
			nextItemIdx := idx + 2
			if nextItemIdx >= offSize {
				return offSize
			}

			return nextItemIdx
		}
	}

	return offSize
}

func (l *Log) readItemAt(off item.Off, it *item.Item) (err error) {
	if int64(off)+item.HeaderSize >= l.size {
		return nil
	}

	// parse header:
	len := binary.BigEndian.Uint32(l.mmap[off+0:])
	key := binary.BigEndian.Uint64(l.mmap[off+4:])

	if len > 64*1024*1024 {
		// fail-safe if the size field is corrupt:
		return fmt.Errorf("log: allocation too big for one value: %d", len)
	}

	if int64(off)+item.HeaderSize+int64(len)+item.TrailerSize > l.size {
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
	blobOff := off + item.HeaderSize
	trailerOff := blobOff + item.Off(len)

	// check that the trailer was correctly written.
	// (not a checksum, but could be made to one in future versions)
	if l.mmap[trailerOff] != 0xFF && l.mmap[trailerOff+1] != 0xFF {
		return fmt.Errorf("log: missing trailer: %d", off)
	}

	*it = item.Item{
		Key:  item.Key(key),
		Blob: l.mmap[blobOff:trailerOff],
	}

	return nil
}

func (l *Log) Sync(force bool) error {
	if !l.syncOnWrite && !force {
		return nil
	}

	return unix.Msync(l.mmap, unix.MS_SYNC)
}

func (l *Log) Close() error {
	syncErr := unix.Msync(l.mmap, unix.MS_SYNC)
	unmapErr := unix.Munmap(l.mmap)
	closeErr := l.fd.Close()
	return errors.Join(syncErr, unmapErr, closeErr)
}
