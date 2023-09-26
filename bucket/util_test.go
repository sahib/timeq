package bucket

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMoveFile(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-copyfile")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	expData := []byte("Hello")

	aPath := filepath.Join(dir, "a")
	bPath := filepath.Join(dir, "b")
	require.NoError(t, os.WriteFile(aPath, expData, 0600))
	require.NoError(t, moveFileOrDir(aPath, bPath))

	gotData, err := os.ReadFile(bPath)
	require.NoError(t, err)
	require.Equal(t, expData, gotData)

	_, err = os.Stat(aPath)
	require.True(t, os.IsNotExist(err))
}
