package vlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestIter(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Push a few items before:
	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	_, err = log.Push(testutils.GenItems(0, 10, 1))
	require.NoError(t, err)

	loc, err := log.Push(testutils.GenItems(10, 20, 1))
	require.NoError(t, err)

	firstBatchOff := (item.HeaderSize+item.TrailerSize)*10 + 10
	require.Equal(t, loc, item.Location{
		Key: 10,
		Off: item.Off(firstBatchOff),
		Len: 10,
	})

	var count int
	iter := log.At(loc, true)
	for iter.Next() {
		it := iter.Item()
		require.Equal(t, item.Item{
			Key:  item.Key(count + 10),
			Blob: []byte(fmt.Sprintf("%d", count+10)),
		}, it)

		// current location is sitting on the next entry already.
		currLoc := iter.CurrentLocation()
		require.Equal(t, item.Location{
			Key: item.Key(count + 10),
			Off: item.Off(firstBatchOff + count*(item.HeaderSize+2+item.TrailerSize)),
			Len: item.Off(10 - count),
		}, currLoc)
		count++
	}

	require.Equal(t, 10, count)
	require.NoError(t, iter.Err())
	require.NoError(t, log.Close())
}

func TestIterEmpty(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	iter := log.At(item.Location{}, true)

	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, log.Close())
}

func TestIterInvalidLocation(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	iter := log.At(item.Location{
		Off: 0x2A,
		Len: 1000,
	}, true)

	require.False(t, iter.Next())
	require.True(t, iter.Exhausted())
	require.NoError(t, iter.Err())
	require.NoError(t, log.Close())
}

func TestIterBrokenStream(t *testing.T) {
	t.Parallel()

	for _, continueOnErr := range []bool{false, true} {
		for idx := 0; idx < 4; idx++ {
			t.Run(fmt.Sprintf("%d-continue-%v", idx, continueOnErr), func(t *testing.T) {
				// depending on which index of the size field
				// is overwritten we test for different errors.
				testIterBrokenStream(t, idx, continueOnErr)
			})
		}
	}
}

func testIterBrokenStream(t *testing.T, overwriteIndex int, continueOnErr bool) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)

	item1 := item.Item{Key: 23, Blob: []byte("blob1")}
	item2 := item.Item{Key: 42, Blob: []byte("blob2")}

	loc, err := log.Push(item.Items{item1, item2})
	require.NoError(t, err)

	// Modify the size field to make bigger than log.size
	log.mmap[overwriteIndex] = 0xFF

	// The iterator should be able to figure out the next
	// value at least:
	iter := log.At(loc, continueOnErr)
	if continueOnErr {
		require.True(t, iter.Next())
		it := iter.Item()
		require.Equal(t, item.Key(42), it.Key)
		require.Equal(t, item2.Blob, it.Blob)
	}
	require.False(t, iter.Next())
}

func TestIterHeap(t *testing.T) {
	iters := Iters{}
	itersHeap := &iters
	require.Equal(t, 0, itersHeap.Len())

	itersHeap.Push(Iter{
		exhausted: true,
		item:      item.Item{Key: 100},
	})
	itersHeap.Push(Iter{
		exhausted: false,
		item:      item.Item{Key: 50},
	})
	itersHeap.Push(Iter{
		exhausted: false,
		item:      item.Item{Key: 0},
	})

	it1 := iters[0] // min must be at front.
	it2 := iters[2] // heap condition says it should be second.
	it3 := iters[1] // third one.

	require.False(t, it1.Exhausted())
	require.False(t, it2.Exhausted())
	require.True(t, it3.Exhausted())

	require.Equal(t, item.Key(0), it1.CurrentLocation().Key)
	require.Equal(t, item.Key(50), it2.CurrentLocation().Key)
	require.Equal(t, item.Key(100), it3.CurrentLocation().Key)
}
