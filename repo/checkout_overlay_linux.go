//go:build linux

package repo

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

func setOverlayWhiteout(path string) error {
	if err := unix.Setxattr(path, "user.overlay.whiteout", []byte("y"), 0); err != nil {
		return overlayWhiteoutError(err)
	}
	return nil
}

func setOverlayOpaque(path, value string) error {
	if current, err := overlayXattr(path, "user.overlay.opaque"); err == nil && string(current) == "y" {
		return nil
	}
	if err := unix.Setxattr(path, "user.overlay.opaque", []byte(value), 0); err != nil {
		return overlayWhiteoutError(err)
	}
	return nil
}

func verifyOverlayWhiteout(path string) error {
	value, err := overlayXattr(path, "user.overlay.whiteout")
	if err != nil {
		return err
	}
	if string(value) != "y" {
		return errors.New("overlay whiteout marker changed")
	}
	return nil
}

func verifyOverlayOpaque(path, expected string) error {
	value, err := overlayXattr(path, "user.overlay.opaque")
	if err != nil {
		return err
	}
	if string(value) != expected {
		return errors.New("overlay opaque marker changed")
	}
	return nil
}

func overlayXattr(path, name string) ([]byte, error) {
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	value := make([]byte, size)
	_, err = unix.Getxattr(path, name, value)
	return value, err
}

func overlayWhiteoutError(err error) error {
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EPERM) {
		return fmt.Errorf("%w: %v", ErrOverlayWhiteoutsUnsupported, err)
	}
	return err
}
