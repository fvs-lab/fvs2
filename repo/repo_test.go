package repo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommitCreatesRevisionAndSkipsNoop(t *testing.T) {
	root := t.TempDir()
	repository, err := Init(root, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Path != root || repository.BlockSize != 4096 {
		t.Fatalf("repository = %+v", repository)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("first"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := Commit(root, "first", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.StateID == "" || first.FileCount != 1 {
		t.Fatalf("first commit = %+v", first)
	}
	second, err := Commit(root, "second", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if second.Created || second.StateID != first.StateID {
		t.Fatalf("no-op commit = %+v, want state %s", second, first.StateID)
	}
}

func TestInitIsIdempotent(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 8192); err != nil {
		t.Fatal(err)
	}
	if repository, err := Init(root, 8192); err != nil || repository.BlockSize != 8192 {
		t.Fatalf("retry = %+v, %v", repository, err)
	}
	if _, err := Init(root, 4096); err == nil {
		t.Fatal("expected conflicting block size to fail")
	}
}
