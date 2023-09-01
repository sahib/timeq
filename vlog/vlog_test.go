package vlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/stretchr/testify/require"
)

func genItems(start, stop, step int) []item.Item {
	var its []item.Item
	for idx := start; idx < stop; idx += step {
		its = append(its, item.Item{
			Key:  item.Key(idx),
			Blob: []byte(fmt.Sprintf("%d", idx)),
		})
	}

	return its
}

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

	loc, err := log.Push(genItems(1, 2, 1))
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
