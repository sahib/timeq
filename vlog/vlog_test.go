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
	}

	logPath := filepath.Join(tmpDir, "log")
	require.NoError(t, os.WriteFile(logPath, fakeBlob, 0600))

	log := Open(logPath, true)
	require.NoError(t, log.Close())
}

func TestLogOpenEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, log.Close())
}

func TestLogOpenPushRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := Open(filepath.Join(tmpDir, "log"), true)
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

func testLogGenerateIndex(t *testing.T, pushes [][]item.Item, expLocs []item.Location) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log := Open(filepath.Join(tmpDir, "log"), true)
	for _, push := range pushes {
		_, err = log.Push(push)
		require.NoError(t, err)

	}

	tree, err := log.GenerateIndex()
	require.NoError(t, err)

	gotLocs := tree.Values()
	gotKeys := tree.Keys()

	expKeys := []item.Key{}
	for _, expLoc := range expLocs {
		expKeys = append(expKeys, expLoc.Key)
	}

	require.Equal(t, expLocs, gotLocs)
	require.Equal(t, expKeys, gotKeys)
}

func TestLogGenerateIndex(t *testing.T) {
	tcs := []struct {
		Name    string
		Pushes  [][]item.Item
		ExpLocs []item.Location
	}{
		{
			Name: "consecutive",
			Pushes: [][]item.Item{
				testutils.GenItems(15, 20, 1),
				testutils.GenItems(0, 10, 1),
			},
			ExpLocs: []item.Location{
				{
					Key: 0,
					Off: 5*itemHeaderSize + 5*2,
					Len: 10,
				}, {
					Key: 15,
					Off: 0,
					Len: 5,
				},
			},
		}, {
			Name: "strided",
			Pushes: [][]item.Item{
				testutils.GenItems(0, 10, 2),
				testutils.GenItems(1, 10, 2),
			},
			ExpLocs: []item.Location{
				{
					Key: 0,
					Off: 0,
					Len: 5,
				}, {
					Key: 1,
					Off: itemHeaderSize*5 + 5,
					Len: 5,
				},
			},
		}, {
			Name: "gap",
			Pushes: [][]item.Item{
				testutils.GenItems(300, 400, 2),
				testutils.GenItems(100, 200, 1),
			},
			ExpLocs: []item.Location{
				{
					Key: 100,
					Off: 750,
					Len: 100,
				}, {
					Key: 300,
					Off: 0,
					Len: 50,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			testLogGenerateIndex(t, tc.Pushes, tc.ExpLocs)
		})
	}
}
