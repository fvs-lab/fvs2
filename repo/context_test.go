package repo

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"fvs2/internal/meta"
)

func TestCommitContextCancellation(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64<<10)
	rand.New(rand.NewSource(3)).Read(data)
	if err := os.WriteFile(filepath.Join(root, "f.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CommitContext(ctx, root, "c", false, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit: %v", err)
	}

	// A live context commits normally.
	if res, err := CommitContext(context.Background(), root, "c", false, nil); err != nil || !res.Created {
		t.Fatalf("commit after cancel: %+v, %v", res, err)
	}
}

func TestRestoreContextCancellation(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dest := t.TempDir()
	if _, err := RestoreContext(ctx, root, id, RestoreOptions{To: dest}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled restore: %v", err)
	}
	if _, err := RestoreContext(context.Background(), root, id, RestoreOptions{To: dest}); err != nil {
		t.Fatalf("restore after cancel: %v", err)
	}
}
