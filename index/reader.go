package index

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/sahib/timeq/item"
)

// LocationSize is the physical storage of a single item
// (8 for the key, 4 for the wal offset, 4 for the len)
const LocationSize = 16

// Reader gives access to a single index on disk
type Reader struct {
	r      io.Reader
	err    error
	locBuf []byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:      bufio.NewReaderSize(r, 16*1024),
		locBuf: make([]byte, LocationSize),
	}
}

func (fi *Reader) Next(loc *item.Location) bool {
	if _, err := io.ReadFull(fi.r, fi.locBuf); err != nil {
		if err != io.EOF {
			fi.err = err
		}

		return false
	}

	loc.Key = item.Key(binary.BigEndian.Uint64(fi.locBuf[:8]))
	loc.Off = item.Off(binary.BigEndian.Uint32(fi.locBuf[8:]))
	loc.Len = item.Off(binary.BigEndian.Uint32(fi.locBuf[12:]))
	return true
}

func (fi *Reader) Err() error {
	return fi.err
}
