package meta

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// RepoLock is an exclusive advisory lock over a repo's metadata, serializing
// mutating operations (commit, drop, gc) across processes. Readers are
// lock-free: metadata writes are atomic renames.
type RepoLock struct {
	f *os.File
}

func lockPath(root string) string { return filepath.Join(metaDir(root), "lock") }

// LockRepo acquires the repo lock, retrying until timeout.
func LockRepo(root string, timeout time.Duration) (*RepoLock, error) {
	if _, err := LoadConfig(root); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath(root), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &RepoLock{f: f}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = f.Close()
			return nil, err
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, fmt.Errorf("repo is locked by another process (lock: %s): %w", lockPath(root), ErrLockTimeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Unlock releases the lock. Safe to call once.
func (l *RepoLock) Unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	cerr := l.f.Close()
	l.f = nil
	if err != nil {
		return err
	}
	return cerr
}
