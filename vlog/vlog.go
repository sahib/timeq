package vlog

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/sahib/timeq/item"
	"golang.org/x/sys/unix"
)

const itemHeaderSize = 12

type Log struct {
	fd   *os.File
	mmap mmap.MMap
	size int64
}

func Open(path string) (*Log, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR
	fd, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("log: open: %w", err)
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("log: stat: %w", err)
	}

	m, err := mmap.Map(fd, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}

	return &Log{
		fd:   fd,
		mmap: m,
		size: info.Size(),
	}, nil
}

func (l *Log) writeItem(item item.Item) {
	binary.BigEndian.PutUint32(l.mmap[l.size+0:], uint32(len(item.Blob)))
	binary.BigEndian.PutUint64(l.mmap[l.size+4:], uint64(item.Key))
	copy(l.mmap[l.size+12:], item.Blob)
}

func (l *Log) Push(items []item.Item) (item.Location, error) {
	addSize := len(items) * itemHeaderSize
	for i := 0; i < len(items); i++ {
		addSize += len(items[i].Blob)
	}

	loc := item.Location{
		Key: items[0].Key,
		Off: item.Off(l.size),
		Len: item.Off(addSize),
	}

	// extend the wal file to fit the new items:
	newSize := l.size + int64(addSize)
	if err := l.fd.Truncate(newSize); err != nil {
		return item.Location{}, err
	}

	m, err := unix.Mremap(l.mmap, int(newSize), unix.MREMAP_MAYMOVE)
	if err != nil {
		return item.Location{}, err
	}

	l.size = newSize
	l.mmap = m

	// copy the items to the file map:
	for i := 0; i < len(items); i++ {
		l.writeItem(items[i])
	}

	return loc, l.Sync()
}

func (l *Log) At(loc item.Location) LogIter {
	return LogIter{
		key:     loc.Key,
		currOff: loc.Off,
		currLen: loc.Len,
		log:     l,
	}
}

func (l *Log) readItemAt(off item.Off, it *item.Item) error {
	if int64(off)+itemHeaderSize >= l.size {
		return fmt.Errorf("log: bad offset: %d %d (header too big)", off, l.size)
	}

	// parse header:
	len := binary.BigEndian.Uint32(l.mmap[off+0:])
	key := binary.BigEndian.Uint64(l.mmap[off+4:])

	if len > 4*1024*1024 {
		return fmt.Errorf("log: allocation too big for one value: %d", len)
	}

	if int64(off)+itemHeaderSize+int64(len) >= l.size {
		return fmt.Errorf("log: bad offset: %d+%d %d (payload too big)", off, len, l.size)
	}

	// NOTE: We directly slice the memory map here. This means that the caller
	// has to copy the slice if he wants to save it somewhere as we might overwrite,
	// unmap or resize the underlying memory at a later point. Caller can use item.Copy()
	// to obtain a copy.
	*it = item.Item{
		Key:  item.Key(key),
		Blob: l.mmap[off+itemHeaderSize:],
	}

	return nil
}

func (l *Log) Sync() error {
	return unix.Msync(l.mmap, unix.MS_SYNC)
}

func (l *Log) Close() error {
	l.Sync()
	l.mmap.Unmap()
	return l.fd.Close()
}
