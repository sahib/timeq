package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/sahib/timeq/item"
	"github.com/stretchr/testify/require"
)

func ItemFromIndex(idx int) item.Item {
	return item.Item{
		Key:  item.Key(idx),
		Blob: []byte(fmt.Sprintf("%d", idx)),
	}
}

func GenItems(start, stop, step int) item.Items {
	if step == 0 {
		return nil
	}

	var its item.Items
	for idx := start; ; idx += step {
		if step > 0 && idx >= stop {
			break
		}

		if step < 0 && idx <= stop {
			break
		}

		its = append(its, ItemFromIndex(idx))
	}

	return its
}

// WithTempMount calls `fn` with the path to directory that contains an empty
// ext4 filesystem that will be unmounted once the test finished.
func WithTempMount(t *testing.T, fn func(mountDir string)) {
	if runtime.GOOS != "linux" {
		t.Skipf("this test uses ext4 and other linux specific tools")
	}

	if os.Geteuid() != 0 {
		t.Skipf("this test needs to be run with root permissions to work")
	}

	dir, err := os.MkdirTemp("", "mount-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create a file big enough to hold a small filesystem:
	loopPath := filepath.Join(dir, "loop")
	fd, err := os.Create(loopPath)
	require.NoError(t, err)
	require.NoError(t, fd.Truncate(1*1024*1024))
	require.NoError(t, fd.Close())

	// Create a filesystem in the loop file:
	ext4Out, err := exec.Command("mkfs.ext4", loopPath).Output()
	require.NoError(t, err, string(ext4Out))

	// Mount the ext4 fs to a newly created directory:
	mountDir := filepath.Join(dir, "mount")
	require.NoError(t, os.MkdirAll(mountDir, 0600))
	mountOut, err := exec.Command("mount", loopPath, mountDir).Output()
	require.NoError(t, err, string(mountOut))

	defer func() {
		require.NoError(t, syscall.Unmount(mountDir, 0))
	}()

	fn(mountDir)
}
