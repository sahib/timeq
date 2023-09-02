package vlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestOpenUnaligned(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fakeBlob := []byte{
		0x0, 0x0, 0x0, 0x1, // size=1
		0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0xF, // key=15
	}

	logPath := filepath.Join(tmpDir, "log")
	require.NoError(t, os.WriteFile(logPath, fakeBlob, 0600))

	log, err := Open(logPath, true)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}

func TestOpenEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	require.NoError(t, log.Close())
}

func TestOpenPushRead(t *testing.T) {
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
