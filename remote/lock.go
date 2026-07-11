package remote

import (
	"errors"
	"os"
	"syscall"
)

// errRefConflict signals a lost compare-and-swap inside a locked section.
var errRefConflict = errors.New("ref conflict")

// acquireFileLock takes an exclusive flock on path and returns the unlock
// function, for critical sections that span too much code for a closure.
func acquireFileLock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// withFileLock runs fn while holding an exclusive flock on path, so critical
// sections coordinate across every server process sharing the same root, not
// just goroutines of one process.
func withFileLock(path string, fn func() error) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}
