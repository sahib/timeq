package bucket

import (
	"errors"
	"os"
	"syscall"

	"github.com/otiai10/copy"
)

func moveFileOrDir(src, dst string) error {
	if err := os.Rename(src, dst); !errors.Is(err, syscall.EXDEV) {
		// NOTE: this includes err==nil
		return err
	}

	// copying directories has many edge cases, so rely on a library
	// for that (and avoid writing too much tests for that)
	opts := copy.Options{Sync: true}
	if err := copy.Copy(src, dst, opts); err != nil {
		return err
	}

	return os.RemoveAll(src)
}
