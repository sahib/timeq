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
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}

func TestLogOpenPushRead(t *testing.T) {
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
	iter := log.At(firstLoc)
	require.True(t, iter.Next(&it))
	require.Equal(t, item.Item{
		Key:  1,
		Blob: []byte("1"),
	}, it)
	require.False(t, iter.Next(&it))

	iter = log.At(sndLoc)
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
