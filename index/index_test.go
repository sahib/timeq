package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/item/testutils"
	"github.com/sahib/timeq/vlog"
	"github.com/stretchr/testify/require"
)

func TestIndexLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-indextest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	indexPath := filepath.Join(tmpDir, "index")
	w, err := NewWriter(indexPath, true)
	require.NoError(t, err)

	var lenCount item.Off
	for idx := 0; idx < 10; idx++ {
		require.NoError(t, w.Push(item.Location{
			Key: item.Key(idx),
			Off: item.Off(idx),
			Len: item.Off(idx),
		}, Trailer{}))
		lenCount += item.Off(idx)
	}

	require.NoError(t, w.Close())

	index, err := Load(indexPath)
	require.NoError(t, err)

	// if length=0 then Load() considers the entry
	// as "delete previous items with this key".
	var count int = 1
	for iter := index.Iter(); iter.Next(); {
		require.Equal(t, item.Location{
			Key: item.Key(count),
			Off: item.Off(count),
			Len: item.Off(count),
		}, iter.Value())
		count++
	}

	require.Equal(t, lenCount, index.Len())
}

func TestIndexSet(t *testing.T) {
	index := Index{}

	oldLoc := item.Location{Key: 23}
	newLoc, skew := index.Set(oldLoc)
	require.Equal(t, oldLoc, newLoc)
	require.Equal(t, 0, skew)
}

func testIndexFromVlog(t *testing.T, pushes [][]item.Item, expLocs [][]item.Location) {
	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	log, err := vlog.Open(filepath.Join(tmpDir, "log"), true)
	require.NoError(t, err)
	for _, push := range pushes {
		_, err = log.Push(push)
		require.NoError(t, err)
	}

	index, err := FromVlog(log)
	require.NoError(t, err)

	gotLocs := index.m.Values()
	gotKeys := index.m.Keys()

	expKeys := []item.Key{}
	for _, expSlice := range expLocs {
		expKeys = append(expKeys, expSlice[0].Key)
	}

	require.Equal(t, expLocs, gotLocs)
	require.Equal(t, expKeys, gotKeys)
}

func TestIndexFromVlog(t *testing.T) {
	tcs := []struct {
		Name    string
		Pushes  [][]item.Item
		ExpLocs [][]item.Location
	}{
		{
			Name: "consecutive",
			Pushes: [][]item.Item{
				testutils.GenItems(15, 20, 1),
				testutils.GenItems(0, 10, 1),
			},
			ExpLocs: [][]item.Location{{{
				Key: 0,
				Off: 5*vlog.ItemHeaderSize + 5*2,
				Len: 10,
			}}, {{
				Key: 15,
				Off: 0,
				Len: 5,
			},
			}},
		}, {
			Name: "strided",
			Pushes: [][]item.Item{
				testutils.GenItems(0, 10, 2),
				testutils.GenItems(1, 10, 2),
			},
			ExpLocs: [][]item.Location{{{
				Key: 0,
				Off: 0,
				Len: 5,
			}}, {{
				Key: 1,
				Off: vlog.ItemHeaderSize*5 + 5,
				Len: 5,
			},
			}},
		}, {
			Name: "gap",
			Pushes: [][]item.Item{
				testutils.GenItems(300, 400, 2),
				testutils.GenItems(100, 200, 1),
			},
			ExpLocs: [][]item.Location{{{
				Key: 100,
				Off: 750,
				Len: 100,
			}}, {{
				Key: 300,
				Off: 0,
				Len: 50,
			},
			}},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			testIndexFromVlog(t, tc.Pushes, tc.ExpLocs)
		})
	}
}

func TestIndexDuplicateSet(t *testing.T) {
	index := &Index{}
	loc1 := item.Location{
		Key: 10,
		Off: 23,
		Len: 5,
	}
	loc2 := item.Location{
		Key: 10,
		Off: 42,
		Len: 19,
	}
	index.Set(loc1)
	index.Set(loc2)
	require.Equal(t, item.Off(24), index.Len())

	var count int
	for iter := index.Iter(); iter.Next(); count++ {
		loc := iter.Value()
		if count == 0 {
			require.Equal(t, loc1, loc)
		}

		if count == 1 {
			require.Equal(t, loc2, loc)
		}
	}
	require.Equal(t, 2, count)

	// check if deletion of one item
	// let's the other one survive:
	index.Delete(10)
	iter := index.Iter()
	require.True(t, iter.Next())
	require.Equal(t, loc2, iter.Value())
	require.False(t, iter.Next())
	require.Equal(t, item.Off(19), index.Len())

	// check if deleting all of them works nicely:
	index.Delete(10)
	iter = index.Iter()
	require.False(t, iter.Next())
	require.Equal(t, item.Off(0), index.Len())
}

func TestIndexNoCrashOnBadAPIUsage(t *testing.T) {
	index := &Index{}
	iter := index.Iter()
	iter.Value() // this should not crash
}
