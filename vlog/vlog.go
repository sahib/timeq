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
	fd   *os.File
	mmap []byte
	size int64
	sync bool
}

func Open(path string, sync bool) (*Log, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_RDWR
	fd, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("log: open: %w", err)
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("log: stat: %w", err)
	}

	mmapSize := int(info.Size())
	if info.Size() == 0 {
		// mmap() does not like to map zero size files.
		// fake it by truncating to a small file size
		// in that case.
		if err := fd.Truncate(itemHeaderSize); err != nil {
			return nil, fmt.Errorf("log: init-trunc: %w", err)
		}

		mmapSize = itemHeaderSize
	}

	m, err := unix.Mmap(
		int(fd.Fd()),
		0,
		mmapSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)

	if err != nil {
		return nil, fmt.Errorf("log: mmap: %w", err)
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
	copy(l.mmap[l.size+itemHeaderSize:], item.Blob)
}

func (l *Log) Push(items []item.Item) (item.Location, error) {
	addSize := len(items) * itemHeaderSize
	for i := 0; i < len(items); i++ {
		addSize += len(items[i].Blob)
	}

	loc := item.Location{
		Key: items[0].Key,
		Off: item.Off(l.size),
		Len: item.Off(len(items)),
	}

	// extend the wal file to fit the new items:
	newSize := l.size + int64(addSize)
	if err := l.fd.Truncate(newSize); err != nil {
		return item.Location{}, fmt.Errorf("truncate: %w", err)
	}

	m, err := unix.Mremap(l.mmap, int(newSize), unix.MREMAP_MAYMOVE)
	if err != nil {
		return item.Location{}, fmt.Errorf("remap: %w", err)
	}

	l.mmap = m

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
	// has to copy the slice if he wants to save it somewhere as we might overwrite,
	// unmap or resize the underlying memory at a later point. Caller can use item.Copy()
	// to obtain a copy.
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
	if !l.sync && !force {
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
