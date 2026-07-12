package repo

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"fvs2/internal/meta"
)

func TestResolveCommitPrecedence(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := Commit(root, "c1", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("two!"), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := Commit(root, "c2", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.CreateBranch(root, "pinned"); err != nil {
		t.Fatal(err)
	}
	if err := meta.WriteBranchHead(root, "pinned", first.StateID); err != nil {
		t.Fatal(err)
	}

	// Explicit prefix wins over branch and HEAD.
	if id, err := ResolveCommit(root, first.StateID[:12], "pinned"); err != nil || id != first.StateID {
		t.Fatalf("prefix resolution = %q, %v", id, err)
	}
	// Branch head.
	if id, err := ResolveCommit(root, "", "pinned"); err != nil || id != first.StateID {
		t.Fatalf("branch resolution = %q, %v", id, err)
	}
	// HEAD.
	if id, err := ResolveCommit(root, "", ""); err != nil || id != second.StateID {
		t.Fatalf("HEAD resolution = %q, %v", id, err)
	}
	// Unknowns classify as not-found.
	if _, err := ResolveCommit(root, "feedface", ""); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("unknown prefix: %v", err)
	}
	if _, err := ResolveCommit(root, "", "nope"); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("unknown branch: %v", err)
	}
}

func TestDescribeState(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b"), []byte("bb"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := Commit(root, "described", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := DescribeState(root, res.StateID[:12])
	if err != nil {
		t.Fatal(err)
	}
	if detail.ID != res.StateID || detail.Message != "described" {
		t.Fatalf("detail = %+v", detail)
	}
	if detail.FileCount != 2 || detail.TotalSize != 6 {
		t.Fatalf("counts = %d files, %d bytes", detail.FileCount, detail.TotalSize)
	}
	if detail.Format != meta.CurrentFormat || detail.BlockSize <= 0 {
		t.Fatalf("format/block size = %d/%d", detail.Format, detail.BlockSize)
	}
}
