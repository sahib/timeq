package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexWriterEmpty(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "timeq-vlogtest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	idxPath := filepath.Join(tmpDir, "idx.log")
	idxWriter, err := NewWriter(idxPath, false)
	require.NoError(t, err)
	require.NoError(t, idxWriter.Sync(false))
	require.NoError(t, idxWriter.Close())
}
