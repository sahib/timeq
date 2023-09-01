package index

import (
	"encoding/binary"
	"os"

	"github.com/sahib/timeq/item"
)

type Writer struct {
	fd     *os.File
	locBuf []byte
}

func NewWriter(path string) (*Writer, error) {
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

func (w *Writer) Append(loc item.Location) error {
	binary.BigEndian.PutUint64(w.locBuf[:8], uint64(loc.Key))
	binary.BigEndian.PutUint32(w.locBuf[8:], uint32(loc.Off))
	binary.BigEndian.PutUint32(w.locBuf[12:], uint32(loc.Len))
	_, err := w.fd.Write(w.locBuf)
	return err
}

func (w *Writer) Close() error {
	w.fd.Sync()
	return w.fd.Close()
}

func (w *Writer) Sync() error {
	return w.fd.Sync()
}
