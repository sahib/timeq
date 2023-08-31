package vlog

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/sahib/lemurq/item"
	"golang.org/x/sys/unix"
)

const itemHeaderSize = 12

type Log struct {
	dirty bool
	fd    *os.File
	size  int64
	mmap  mmap.MMap
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

	return &Log{
		fd:   fd,
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
		Off: item.Off(l.size),
		Len: item.Off(addSize),
	}

	// extend the wal file to fit the new items:
	if err := l.fd.Truncate(l.size + int64(addSize)); err != nil {
		return item.Location{}, err
	}

	// copy the items to the file map:
	for i := 0; i < len(items); i++ {
		l.writeItem(items[i])
	}

	l.dirty = true
	return loc, nil
}

func (l *Log) At(loc item.Location) LogIter {
	return LogIter{
		log: l,
		loc: loc,
	}
}

func (l *Log) at(off item.Off) (item.Item, error) {
	if len(l.mmap) == 0 {
		m, err := mmap.Map(l.fd, mmap.RDWR, 0)
		if err != nil {
			return item.Item{}, err
		}

		l.mmap = m
		l.dirty = false
	}

	if l.dirty {
		m, err := unix.Mremap(l.mmap, int(l.size), unix.MREMAP_MAYMOVE)
		if err != nil {
			return item.Item{}, err
		}

		l.mmap = m
		l.dirty = false
	}

	if int64(off)+itemHeaderSize >= l.size {
		return item.Item{}, fmt.Errorf("log: bad offset: %d %d (header too big)", off, l.size)
	}

	// parse header:
	len := binary.BigEndian.Uint32(l.mmap[off+0:])
	key := binary.BigEndian.Uint64(l.mmap[off+4:])

	if len > 4*1024*1024 {
		return item.Item{}, fmt.Errorf("log: allocation too big for one value: %d", len)
	}

	if int64(off)+itemHeaderSize+int64(len) >= l.size {
		return item.Item{}, fmt.Errorf("log: bad offset: %d+%d %d (payload too big)", off, len, l.size)
	}

	// TODO: We could think about not copying here, but returning the slice.
	//       Then the caller would need to copy it, but it could segfault anytime
	//       when the bucket is closed.
	blob := make([]byte, len)
	copy(blob, l.mmap[off+itemHeaderSize:])
	return item.Item{
		Key:  item.Key(key),
		Blob: blob,
	}, nil
}

func (l *Log) Flush() error {
	return unix.Msync(l.mmap, unix.MS_SYNC)
}

func (l *Log) Close() error {
	l.Flush()
	l.mmap.Unmap()
	return l.fd.Close()
}
