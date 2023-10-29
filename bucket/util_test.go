package bucket

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestMoveFileIfRenamable(t *testing.T) {
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

func TestMoveIfNotRenamable(t *testing.T) {
	t.Parallel()

	testutils.WithTempMount(t, func(ext4Dir string) {
		tmpDir, err := os.MkdirTemp("", "timeq-copyfile")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		aData := []byte("Hello")
		bData := []byte("World")
		aPath := filepath.Join(tmpDir, "a")
		bPath := filepath.Join(tmpDir, "b")
		require.NoError(t, os.WriteFile(aPath, aData, 0600))
		require.NoError(t, os.WriteFile(bPath, bData, 0600))

		dstDir := filepath.Join(ext4Dir, "dst")
		require.NoError(t, moveFileOrDir(tmpDir, dstDir))

		entries, err := os.ReadDir(dstDir)
		require.NoError(t, err)
		require.Len(t, entries, 2)

		expAData, err := os.ReadFile(filepath.Join(dstDir, "a"))
		require.NoError(t, err)

		expBData, err := os.ReadFile(filepath.Join(dstDir, "b"))
		require.NoError(t, err)

		require.Equal(t, aData, expAData)
		require.Equal(t, bData, expBData)

		_, err = os.Stat(aPath)
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(bPath)
		require.True(t, os.IsNotExist(err))
	})
}

func TestMoveErrorIfRenamable(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "timeq-copyfile")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	expData := []byte("Hello")
	aPath := filepath.Join(dir, "a")
	require.NoError(t, os.WriteFile(aPath, expData, 0600))

	badDir := filepath.Join(dir, "bad")
	require.NoError(t, os.MkdirAll(badDir, 0400))
	require.Error(t, moveFileOrDir(aPath, badDir))

	// Nothing should have been deleted:
	_, err = os.Stat(aPath)
	require.NoError(t, err)
}

func TestMoveErrorIfNotRenamable(t *testing.T) {
	t.Parallel()

	testutils.WithTempMount(t, func(ext4Dir string) {
		tmpDir, err := os.MkdirTemp("", "timeq-copyfile")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		aData := []byte("Hello")
		bData := []byte("World")
		aPath := filepath.Join(tmpDir, "a")
		bPath := filepath.Join(tmpDir, "b")
		require.NoError(t, os.WriteFile(aPath, aData, 0600))
		require.NoError(t, os.WriteFile(bPath, bData, 0600))

		// Create dst dir as only-readable, not even cd-able dir:
		dstDir := filepath.Join(ext4Dir, "dst")
		require.NoError(t, os.MkdirAll(dstDir, 0400))

		require.Error(t, moveFileOrDir(tmpDir, dstDir))

		_, err = os.Stat(aPath)
		require.NoError(t, err)
		_, err = os.Stat(bPath)
		require.NoError(t, err)
	})
}
