package item

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	HeaderSize  = 12
	TrailerSize = 2
)

// Key is a priority key in the queue. It has to be unique
// to avoid overwriting other entries. This was written with
// unix nanosecond epoch stamps in mind.
type Key int64

// KeyFromString is the reverse of String()
func KeyFromString(s string) (Key, error) {
	s = strings.TrimPrefix(s, "K")
	key, err := strconv.ParseInt(filepath.Base(s), 10, 64)
	if err != nil {
		return 0, err
	}

	return Key(key), nil
}

func (k Key) String() string {
	// keys are int64, so we need to pad with log10(2**63) at least
	// to be sure that buckets are sorted on filesystem.
	return fmt.Sprintf("K%020d", int64(k))
}

type Off uint64

type Item struct {
	Key  Key
	Blob []byte
}

func (i Item) String() string {
	return fmt.Sprintf("%s:%s", i.Key, i.Blob)
}

func (i Item) StorageSize() Off {
	return HeaderSize + Off(len(i.Blob)) + TrailerSize
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
	return fmt.Sprintf("[key=%s, off=%d, len=%d]", l.Key, l.Off, l.Len)
}

type Items []Item

func (items Items) Copy() Items {
	// This Copy() is allocation optimized, i.e. it first goes through the data
	// and decides how much memory is required. Then that memory is allocated once
	// instead of many times. It's about 50% faster than the straightforward way.

	var bufSize int
	for idx := 0; idx < len(items); idx++ {
		bufSize += len(items[idx].Blob)
	}

	itemsCopy := make(Items, len(items))
	copyBuf := make([]byte, bufSize)
	for idx := 0; idx < len(items); idx++ {
		blobCopy := copyBuf[:len(items[idx].Blob)]
		copy(blobCopy, items[idx].Blob)
		itemsCopy[idx] = Item{
			Key:  items[idx].Key,
			Blob: blobCopy,
		}

		copyBuf = copyBuf[len(blobCopy):]
	}

	return itemsCopy
}

func (items Items) StorageSize() Off {
	sum := Off(len(items)) * (HeaderSize + TrailerSize)
	for idx := 0; idx < len(items); idx++ {
		sum += Off(len(items[idx].Blob))
	}
	return sum
}
