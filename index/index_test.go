package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/stretchr/testify/require"
)

func TestIndexLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "timeq-indextest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	indexPath := filepath.Join(tmpDir, "index")
	w, err := NewWriter(indexPath)
	require.NoError(t, err)

	for idx := 0; idx < 10; idx++ {
		require.NoError(t, w.Push(item.Location{
			Key: item.Key(idx),
			Off: item.Off(idx),
			Len: item.Off(idx),
		}))
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

	require.Equal(t, 9, index.Len())
}

func TestIndexSetNoSkew(t *testing.T) {
	index := Index{}

	oldLoc := item.Location{Key: 23}
	newLoc, skew := index.SetSkewed(oldLoc, 10)
	require.Equal(t, oldLoc, newLoc)
	require.Equal(t, 0, skew)
}

func TestIndexSetWithSkew(t *testing.T) {
	index := Index{}

	oldLoc := item.Location{Key: 23}
	index.SetSkewed(oldLoc, 10)
	newLoc, skew := index.SetSkewed(oldLoc, 10)
	require.Equal(t, 1, skew)
	require.Equal(t, item.Key(24), newLoc.Key)
}

func TestIndexSetWithMaxSkew(t *testing.T) {
	index := Index{}

	oldLoc := item.Location{Key: 23}
	for idx := 0; idx < 100; idx++ {
		index.SetSkewed(oldLoc, 10)
	}

	newLoc, skew := index.SetSkewed(oldLoc, 10)
	require.Equal(t, 10, skew)
	require.Equal(t, item.Key(23), newLoc.Key)
}
