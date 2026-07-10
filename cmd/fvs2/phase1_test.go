package main

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fvs2/internal/meta"
)

func newRepo(t *testing.T) (string, *CLI) {
	t.Helper()
	dir := t.TempDir()
	cli := &CLI{Path: dir}
	if err := (&InitCmd{BlockSize: 4096, Root: cli}).Run(); err != nil {
		t.Fatalf("init: %v", err)
	}
	return dir, cli
}

func commitAll(t *testing.T, cli *CLI, msg string) {
	t.Helper()
	if err := (&CommitCmd{Message: msg, Root: cli}).Run(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func write(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func countBlocks(t *testing.T, root string) int {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(root, ".fvs2", "blocks"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() && e.Name()[0] != '.' {
			n++
		}
	}
	return n
}

func headID(t *testing.T, root string) string {
	t.Helper()
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCommitRestoreRoundTripV2(t *testing.T) {
	root, cli := newRepo(t)

	big := make([]byte, 300<<10)
	rand.New(rand.NewSource(11)).Read(big)
	write(t, root, "big.bin", big)
	write(t, root, "sub/small.txt", []byte("hello"))
	write(t, root, "empty", nil)
	if err := os.Symlink("big.bin", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	commitAll(t, cli, "first")

	// The commit must be format 2 with per-block sizes on multi-chunk files.
	c, err := meta.LoadCommit(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if c.Format != meta.CurrentFormat {
		t.Fatalf("commit format = %d, want %d", c.Format, meta.CurrentFormat)
	}
	for _, f := range c.Files {
		if f.Path == "big.bin" {
			if len(f.Blocks) < 2 {
				t.Fatalf("big.bin should span multiple chunks, got %d", len(f.Blocks))
			}
			if len(f.BlockSizes) != len(f.Blocks) {
				t.Fatalf("block_sizes/blocks length mismatch: %d vs %d", len(f.BlockSizes), len(f.Blocks))
			}
			var sum int64
			for _, s := range f.BlockSizes {
				sum += s
			}
			if sum != f.Size {
				t.Fatalf("block sizes sum %d != file size %d", sum, f.Size)
			}
		}
	}

	dest := t.TempDir()
	if err := (&RestoreCmd{State: c.ID[:12], To: dest, Root: cli}).Run(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big) {
		t.Fatal("restored big.bin differs from original")
	}
	small, _ := os.ReadFile(filepath.Join(dest, "sub/small.txt"))
	if string(small) != "hello" {
		t.Fatal("restored small.txt differs")
	}
	if fi, err := os.Stat(filepath.Join(dest, "empty")); err != nil || fi.Size() != 0 {
		t.Fatalf("empty file not restored: %v", err)
	}
	if target, err := os.Readlink(filepath.Join(dest, "link")); err != nil || target != "big.bin" {
		t.Fatalf("symlink not restored: %q %v", target, err)
	}
}

func TestCDCInsertionStoresFewNewBlocks(t *testing.T) {
	root, cli := newRepo(t)

	data := make([]byte, 1<<20)
	rand.New(rand.NewSource(23)).Read(data)
	write(t, root, "data.bin", data)
	commitAll(t, cli, "base")
	before := countBlocks(t, root)

	// Insert 100 bytes near the start; only a handful of new chunks may land
	// in the store.
	mutated := append(append(append([]byte(nil), data[:1000]...), make([]byte, 100)...), data[1000:]...)
	write(t, root, "data.bin", mutated)
	commitAll(t, cli, "insert")
	after := countBlocks(t, root)

	if grown := after - before; grown > 8 {
		t.Fatalf("insertion added %d blocks (store %d -> %d); dedup is not content-defined", grown, before, after)
	}
}

func TestDropRefusesReferencedState(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f", []byte("one"))
	commitAll(t, cli, "c1")
	head := headID(t, root)

	err := (&DropCmd{State: head[:12], Root: cli}).Run()
	if err == nil {
		t.Fatal("drop of the branch head must fail")
	}
	if _, lerr := meta.LoadCommit(root, head); lerr != nil {
		t.Fatalf("refused drop must not delete the state: %v", lerr)
	}
}

func TestDropAndGCReclaimSpace(t *testing.T) {
	root, cli := newRepo(t)

	oldData := make([]byte, 256<<10)
	rand.New(rand.NewSource(5)).Read(oldData)
	write(t, root, "f.bin", oldData)
	commitAll(t, cli, "c1")
	c1 := headID(t, root)

	newData := make([]byte, 256<<10)
	rand.New(rand.NewSource(6)).Read(newData)
	write(t, root, "f.bin", newData)
	// Same size in the same second: bump mtime so the change is rehashed.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(root, "f.bin"), future, future); err != nil {
		t.Fatal(err)
	}
	commitAll(t, cli, "c2")
	c2 := headID(t, root)
	if c1 == c2 {
		t.Fatal("expected two distinct states")
	}

	beforeGC := countBlocks(t, root)
	if err := (&DropCmd{State: c1[:12], Root: cli}).Run(); err != nil {
		t.Fatalf("drop c1: %v", err)
	}
	if err := (&GcCmd{Root: cli}).Run(); err != nil {
		t.Fatalf("gc: %v", err)
	}
	afterGC := countBlocks(t, root)
	if afterGC >= beforeGC {
		t.Fatalf("gc did not reclaim blocks: %d -> %d", beforeGC, afterGC)
	}

	// The surviving state must still restore byte-identically.
	dest := t.TempDir()
	if err := (&RestoreCmd{State: c2[:12], To: dest, Root: cli}).Run(); err != nil {
		t.Fatalf("restore after gc: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "f.bin"))
	if !bytes.Equal(got, newData) {
		t.Fatal("surviving state corrupted by gc")
	}
}

func TestGCRemovesOrphanCommitDoc(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f", []byte("x"))
	commitAll(t, cli, "c1")

	// Simulate a crash between commit-doc write and index update.
	orphan := filepath.Join(root, ".fvs2", "commits", "deadbeef.json")
	if err := os.WriteFile(orphan, []byte(`{"id":"deadbeef","files":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := (&GcCmd{Root: cli}).Run(); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("gc did not remove the orphan commit document")
	}
}

func TestRepoLockIsExclusive(t *testing.T) {
	root, _ := newRepo(t)
	l1, err := meta.LockRepo(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := meta.LockRepo(root, 200*time.Millisecond); err == nil {
		t.Fatal("second lock must fail while the first is held")
	}
	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}
	l2, err := meta.LockRepo(root, time.Second)
	if err != nil {
		t.Fatalf("lock after unlock: %v", err)
	}
	_ = l2.Unlock()
}

func TestStatusCleanAndDirtyV2(t *testing.T) {
	root, cli := newRepo(t)
	data := make([]byte, 128<<10)
	rand.New(rand.NewSource(9)).Read(data)
	write(t, root, "f.bin", data)
	commitAll(t, cli, "c1")

	dirty, changed, err := computeDirty(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Fatalf("fresh commit reported dirty (%d changed)", changed)
	}

	// Touch without content change: block ids still match, stays clean.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(root, "f.bin"), future, future); err != nil {
		t.Fatal(err)
	}
	dirty, _, err = computeDirty(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Fatal("touched-but-unchanged file reported dirty")
	}

	data[0] ^= 0xff
	write(t, root, "f.bin", data)
	dirty, changed, err = computeDirty(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if !dirty || changed != 1 {
		t.Fatalf("modified file not detected: dirty=%v changed=%d", dirty, changed)
	}
}

func TestUnsupportedFormatRejected(t *testing.T) {
	root, _ := newRepo(t)
	cfgPath := filepath.Join(root, ".fvs2", "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"format": 99, "block_size": 4096}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := meta.LoadConfig(root); err == nil {
		t.Fatal("format 99 must be rejected")
	}
}

func TestLegacyFixedRepoStillCommits(t *testing.T) {
	root, cli := newRepo(t)
	// Rewrite the config as a legacy format-1 repo (no format, no chunking).
	cfgPath := filepath.Join(root, ".fvs2", "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"block_size": 4096}`), 0o644); err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 10000)
	rand.New(rand.NewSource(3)).Read(data)
	write(t, root, "f.bin", data)
	commitAll(t, cli, "legacy")

	c, err := meta.LoadCommit(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if c.Format != 0 {
		t.Fatalf("legacy commit must not claim format 2, got %d", c.Format)
	}
	for _, f := range c.Files {
		if f.Path != "f.bin" {
			continue
		}
		// Fixed 4096-byte blocks: 2 full + 1 partial.
		if len(f.Blocks) != 3 || f.BlockSizes[0] != 4096 || f.BlockSizes[2] != 10000-2*4096 {
			t.Fatalf("legacy chunking wrong: blocks=%d sizes=%v", len(f.Blocks), f.BlockSizes)
		}
	}

	dest := t.TempDir()
	if err := (&RestoreCmd{State: c.ID[:12], To: dest, Root: cli}).Run(); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "f.bin"))
	if !bytes.Equal(got, data) {
		t.Fatal("legacy restore differs")
	}
}
