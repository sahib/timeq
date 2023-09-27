package vlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestLogOpenUnaligned(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fakeBlob := []byte{
		0x0, 0x0, 0x0, 0x1, // size=1
		0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0xF, // key=15
		0xFF,
	}

	logPath := filepath.Join(tmpDir, "log")
	require.NoError(t, os.WriteFile(logPath, fakeBlob, 0600))

	log, err := Open(logPath, true)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}

func TestLogOpenEmpty(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}

func TestLogOpenPushRead(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	loc, err := log.Push(testutils.GenItems(1, 2, 1))
	require.NoError(t, err)
	require.Equal(t, loc, item.Location{
		Key: 1,
		Off: 0,
		Len: 1,
	})

	var it item.Item
	require.NoError(t, log.readItemAt(loc.Off, &it))
	require.Equal(t, item.Item{
		Key:  1,
		Blob: []byte("1"),
	}, it)

	require.NoError(t, log.Close())
}

func TestLogShrink(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	firstLoc, err := log.Push(testutils.GenItems(1, 2, 1))
	require.NoError(t, err)
	require.NoError(t, log.Close())

	// re-open:
	log, err = Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)

	sndLoc, err := log.Push(testutils.GenItems(2, 3, 1))
	require.NoError(t, err)

	var it item.Item
	iter := log.At(firstLoc, true)
	require.True(t, iter.Next(&it))
	require.Equal(t, item.Item{
		Key:  1,
		Blob: []byte("1"),
	}, it)
	require.False(t, iter.Next(&it))

	iter = log.At(sndLoc, true)
	require.True(t, iter.Next(&it))
	require.Equal(t, item.Item{
		Key:  2,
		Blob: []byte("2"),
	}, it)
	require.False(t, iter.Next(&it))

	require.NoError(t, iter.Err())
	require.NoError(t, log.Close())
}

func TestLogOpenNonExisting(t *testing.T) {
	_, err := Open("/nope", true)
	require.Error(t, err)
}

func TestLogNextSize(t *testing.T) {
	var kb int64 = 1024
	var mb int64 = 1024 * kb
	require.Equal(t, int64(0), nextSize(-1))
	require.Equal(t, 8*PageSize, nextSize(0))
	require.Equal(t, 8*PageSize, nextSize(1))

	require.Equal(t, 14*PageSize, nextSize(200*kb)-200*kb)
	require.Equal(t, 32*PageSize, nextSize(1*mb)-1*mb)
	require.Equal(t, 64*PageSize, nextSize(10*mb)-10*mb)
	require.Equal(t, 128*PageSize, nextSize(100*mb)-100*mb)
}

func TestLogRemap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)

	// that's enough to trigger the grow quite a few times:
	for idx := 0; idx < 100; idx++ {
		log.Push(testutils.GenItems(0, 200, 1))
	}

	require.NoError(t, log.Close())
}

func TestLogFindNextItem(t *testing.T) {
	l := &Log{
		mmap: make([]byte, 200),
	}

	item1 := item.Item{Key: 23, Blob: []byte("blob1")}
	item2 := item.Item{Key: 42, Blob: []byte("blob2")}

	l.writeItem(item1)
	l.writeItem(item2)

	expOffset1 := item1.StorageSize()
	expOffset2 := expOffset1 + item2.StorageSize()
	nextItemOff := l.findNextItem(0)
	require.Equal(t, expOffset1, nextItemOff)
	require.Equal(t, uint8(0xFF), l.mmap[nextItemOff-1])
	require.Equal(t, uint8(0xFF), l.mmap[nextItemOff-2])

	nextNextItemOff := l.findNextItem(nextItemOff)
	require.Equal(t, expOffset2, nextNextItemOff)
	require.Equal(t, uint8(0xFF), l.mmap[nextNextItemOff-1])
	require.Equal(t, uint8(0xFF), l.mmap[nextNextItemOff-2])

	// should not progress further (i.e.) beyond size:
	require.Equal(t, item.Off(l.size), nextNextItemOff)
	require.Equal(t, nextNextItemOff, l.findNextItem(nextNextItemOff))
}
