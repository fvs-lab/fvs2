//go:build linux

package repo

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func publishCheckout(source, destination, _ string) error {
	err := unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_EXCHANGE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.ENOENT) {
		return os.Rename(source, destination)
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
		return fmt.Errorf("atomic checkout replacement is unsupported: %w", err)
	}
	return err
}
