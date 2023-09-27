package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/stretchr/testify/require"
)

func TestIndexReadTrailer(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	idxPath := filepath.Join(tmpDir, "idx.log")
	idxWriter, err := NewWriter(idxPath, true)
	require.NoError(t, err)

	for idx := item.Off(0); idx <= 123; idx++ {
		require.NoError(t, idxWriter.Push(item.Location{
			Key: item.Key(idx),
			Off: idx,
			Len: idx,
		}, Trailer{
			TotalEntries: idx,
		}))
	}

	require.NoError(t, idxWriter.Close())

	trailer, err := ReadTrailer(idxPath)
	require.NoError(t, err)
	require.Equal(t, item.Off(123), trailer.TotalEntries)
}
