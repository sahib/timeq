package index

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/sahib/timeq/item"
)

type Writer struct {
	fd     *os.File
	locBuf []byte
	sync   bool
}

func NewWriter(path string, sync bool) (*Writer, error) {
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	fd, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}

	return &Writer{
		fd:     fd,
		locBuf: make([]byte, LocationSize),
	}, nil
}

func (w *Writer) Push(loc item.Location) error {
	binary.BigEndian.PutUint64(w.locBuf[:8], uint64(loc.Key))
	binary.BigEndian.PutUint32(w.locBuf[8:], uint32(loc.Off))
	binary.BigEndian.PutUint32(w.locBuf[12:], uint32(loc.Len))
	_, err := w.fd.Write(w.locBuf)
	return err
}

func (w *Writer) Close() error {
	syncErr := w.fd.Sync()
	closeErr := w.fd.Close()
	return errors.Join(syncErr, closeErr)
}

func (w *Writer) Sync() error {
	if !w.sync {
		return nil
	}

	return w.fd.Sync()
}
