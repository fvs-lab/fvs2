package repo

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fvs-lab/fvs2/internal/meta"
)

func TestTypedErrorClassification(t *testing.T) {
	root := t.TempDir()

	if _, err := Commit(root, "c", false, nil); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("uninitialized repo: %v", err)
	}
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(root, "feedface", RestoreOptions{To: t.TempDir()}); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("unknown state: %v", err)
	}
	if _, err := meta.LoadCommit(root, "feedface"); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("missing commit doc: %v", err)
	}

	lock, err := meta.LockRepo(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	if _, err := meta.LockRepo(root, 100*time.Millisecond); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("held lock: %v", err)
	}

	cfgPath := filepath.Join(root, ".fvs2", "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"format": 99, "block_size": 4096}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := meta.LoadConfig(root); !errors.Is(err, ErrFormatUnsupported) {
		t.Fatalf("newer format: %v", err)
	}
}
