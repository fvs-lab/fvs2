package repo

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"fvs2/internal/meta"
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

// TestCommitHonorsFvsignore checks the .fvsignore rules end to end: an
// excluded file, an excluded directory (whose contents must never even be
// hashed) and a re-included path via negation.
func TestCommitHonorsFvsignore(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, ".fvsignore", "*.log\nbuild/\n!keep.log\n")
	writeTestFile(t, root, "main.go", "package main")
	writeTestFile(t, root, "debug.log", "noise")
	writeTestFile(t, root, "keep.log", "kept")
	writeTestFile(t, root, "build/output.bin", "binary junk")

	res, err := Commit(root, "with ignore rules", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(root, res.StateID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	for _, want := range []string{".fvsignore", "main.go", "keep.log"} {
		if !got[want] {
			t.Errorf("expected %s to be committed, files = %v", want, got)
		}
	}
	for _, excluded := range []string{"debug.log", "build/output.bin"} {
		if got[excluded] {
			t.Errorf("expected %s to be excluded by .fvsignore, files = %v", excluded, got)
		}
	}
}

func writeTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
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

// TestCommitDetectsSameSecondRewrite rewrites a file with same size, mode
// and second-granularity mtime but a different nanosecond timestamp: the
// nanosecond mtimes recorded in state metadata must defeat the reuse
// shortcut and hash the new content.
func TestCommitDetectsSameSecondRewrite(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "f.bin")
	if err := os.WriteFile(p, []byte("aaaaaaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Truncate(time.Second).Add(500 * time.Nanosecond)
	if err := os.Chtimes(p, base, base); err != nil {
		t.Fatal(err)
	}
	first, err := Commit(root, "c1", false, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Same size, same second, different content and nanoseconds.
	if err := os.WriteFile(p, []byte("bbbbbbbb"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, base.Add(time.Microsecond), base.Add(time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	second, err := Commit(root, "c2", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Created || second.StateID == first.StateID {
		t.Fatalf("same-second rewrite missed: %+v vs %+v", second, first)
	}
}

// TestModTimeNSRoundTripsThroughTrees checks the nanosecond mtime survives
// the tree encode/decode and drives the restore skip.
func TestModTimeNSRoundTripsThroughTrees(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "f.txt")
	if err := os.WriteFile(p, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c1", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(root, id)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ModTimeNS != info.ModTime().UnixNano() {
		t.Fatalf("mod_time_ns lost in trees: %+v vs %d", files, info.ModTime().UnixNano())
	}
}
