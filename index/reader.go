package index

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/sahib/timeq/item"
)

const TrailerSize = 4

// LocationSize is the physical storage of a single item
// (8 for the key, 4 for the wal offset, 4 for the len)
const LocationSize = 8 + 4 + 4 + TrailerSize

type Trailer struct {
	TotalEntries item.Off
}

// Reader gives access to a single index on disk
type Reader struct {
	r      io.Reader
	err    error
	locBuf [LocationSize]byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: bufio.NewReaderSize(r, 16*1024),
	}
}

func (fi *Reader) Next(loc *item.Location) bool {
	if _, err := io.ReadFull(fi.r, fi.locBuf[:]); err != nil {
		if err != io.EOF {
			fi.err = err
		}

		return false
	}

	loc.Key = item.Key(binary.BigEndian.Uint64(fi.locBuf[:8]))
	loc.Off = item.Off(binary.BigEndian.Uint32(fi.locBuf[8:]))
	loc.Len = item.Off(binary.BigEndian.Uint32(fi.locBuf[12:]))
	// NOTE: trailer with size / len is ignored here. See ReadTrailer()
	return true
}

func (fi *Reader) Err() error {
	return fi.err
}

// ReadTrailer reads the trailer of the index log.
// It contains the number of entries in the index.
func ReadTrailer(path string) (Trailer, error) {
	fd, err := os.Open(path)
	if err != nil {
		return Trailer{}, err
	}

	info, err := fd.Stat()
	if err != nil {
		return Trailer{}, err
	}

	if info.Size() < LocationSize {
		return Trailer{TotalEntries: 0}, nil
	}

	defer fd.Close()
	if _, err := fd.Seek(-TrailerSize, io.SeekEnd); err != nil {
		return Trailer{}, err
	}

	buf := make([]byte, TrailerSize)
	if _, err := io.ReadFull(fd, buf); err != nil {
		return Trailer{}, nil
	}

	totalEntries := item.Off(binary.BigEndian.Uint32(buf))
	return Trailer{TotalEntries: totalEntries}, nil
}
