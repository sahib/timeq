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
	var it item.Item
	iter := log.At(loc)
	for iter.Next(&it) {
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
	iter := log.At(item.Location{})

	var it item.Item
	require.False(t, iter.Next(&it))
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
	})

	var it item.Item
	require.False(t, iter.Next(&it))
	require.True(t, iter.Exhausted())
	require.NoError(t, iter.Err())
	require.NoError(t, log.Close())
}
