package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/sahib/timeq/item"
)

type Writer struct {
	fd     *os.File
	locBuf [LocationSize]byte
	sync   bool
}

func NewWriter(path string, sync bool) (*Writer, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	fd, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}

	return &Writer{
		fd:   fd,
		sync: sync,
	}, nil
}

func (w *Writer) Push(loc item.Location, trailer Trailer) error {
	binary.BigEndian.PutUint64(w.locBuf[0:], uint64(loc.Key))
	binary.BigEndian.PutUint64(w.locBuf[8:], uint64(loc.Off))
	binary.BigEndian.PutUint32(w.locBuf[16:], uint32(loc.Len))
	binary.BigEndian.PutUint32(w.locBuf[20:], uint32(trailer.TotalEntries))
	_, err := w.fd.Write(w.locBuf[:])
	return err
}

func (w *Writer) Close() error {
	syncErr := w.fd.Sync()
	closeErr := w.fd.Close()
	return errors.Join(syncErr, closeErr)
}

func (w *Writer) Sync(force bool) error {
	if !w.sync && !force {
		return nil
	}

	return w.fd.Sync()
}

// WriteIndex is a convenience function to write the contents
// of `idx` to `path`.
func WriteIndex(idx *Index, path string) error {
	iter := idx.Iter()
	writer, err := NewWriter(path, true)
	if err != nil {
		return err
	}

	var totalEntries item.Off
	for iter.Next() {
		loc := iter.Value()
		if err := writer.Push(loc, Trailer{TotalEntries: totalEntries}); err != nil {
			return fmt.Errorf("push: %w", err)
		}

		totalEntries++
	}

	return nil
}
