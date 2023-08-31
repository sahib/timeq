package index

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/sahib/lemurq/item"
)

// LocationSize is the physical storage of a single item
// (8 for the key, 4 for the wal offset, 4 for the len)
const LocationSize = 16

// Reader gives access to a single index on disk
type Reader struct {
	id     int
	r      io.Reader
	err    error
	entBuf []byte
}

func Reader(r io.Reader) *Reader {
	return &Reader{
		r:      bufio.NewReaderSize(r, 16*1024),
		entBuf: make([]byte, LocationSize),
		id:     id,
	}
}

func (fi *Reader) Next(loc *item.Location) bool {
	if _, err := io.ReadFull(fi.r, fi.entBuf); err != nil {
		if err != io.EOF {
			fi.err = err
		}

		return false
	}

	loc.Key = item.Key(binary.BigEndian.Uint64(fi.entBuf[:8]))
	loc.Off = item.Off(binary.BigEndian.Uint32(fi.entBuf[8:]))
	loc.Len = item.Off(binary.BigEndian.Uint32(fi.entBuf[12:]))
	return true
}

func (fi *Reader) Err() error {
	return fi.err
}

func (fi *Reader) ID() int {
	return fi.id
}
