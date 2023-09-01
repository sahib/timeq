package item

import (
	"fmt"
	"path/filepath"
	"strconv"
)

// Key is a priority key in the queue. It has to be unique
// to avoid overwriting other entries. This was written with
// unix nanosecond epoch stamps in mind.
type Key int64

// KeyFromString is the reverse of String()
func KeyFromString(s string) (Key, error) {
	key, err := strconv.ParseInt(filepath.Base(s), 16, 64)
	if err != nil {
		return 0, err
	}

	return Key(key), nil
}

func (k Key) String() string {
	return fmt.Sprintf("%08X", int64(k))
}

type Off uint32

type Item struct {
	Key  Key
	Blob []byte
}

func (i Item) String() string {
	return fmt.Sprintf("%d:%s", i.Key, i.Blob)
}

func (i *Item) Copy() Item {
	blob := make([]byte, len(i.Blob))
	copy(blob, i.Blob)
	return Item{
		Key:  i.Key,
		Blob: blob,
	}
}

// Location references the location of a batch in a
type Location struct {
	// Key is the priority key of the first item in the batch
	Key Key

	// Off is the offset in bytes to the start of the batch in the vlog.
	Off Off

	// Len is the number of items in this batch.
	// A zero len has a special meaning: this batch was deleted.
	Len Off
}

func (l Location) String() string {
	return fmt.Sprintf("[key=%d, off=%d, len=%d]", l.Key, l.Off, l.Len)
}
