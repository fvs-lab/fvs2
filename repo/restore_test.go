package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

// plantCommit writes a hand-crafted commit document and indexes it, the way
// a hostile or corrupted state would land in a repo (e.g. through pull).
func plantCommit(t *testing.T, root string, c meta.Commit) {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta.CommitPath(root, c.ID), data, 0o644); err != nil {
		t.Fatal(err)
	}
	idx, err := meta.LoadIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	idx.Commits = append(idx.Commits, meta.CommitSummary{ID: c.ID, TimeUTC: c.TimeUTC, Message: c.Message})
	if err := meta.SaveIndex(root, idx); err != nil {
		t.Fatal(err)
	}
}

func fakeID(seed byte) string {
	return strings.Repeat(fmt.Sprintf("%02x", seed), 32)
}

func TestRestoreRejectsEscapingPaths(t *testing.T) {
	outside := t.TempDir()
	for i, evil := range []string{
		"../escape.txt",
		"/abs.txt",
		"sub/../../escape.txt",
		"sub/..",
		".",
		"",
		"a\\..\\b",
	} {
		root := t.TempDir()
		if _, err := Init(root, 0); err != nil {
			t.Fatal(err)
		}
		store, err := meta.NewBlockStore(root)
		if err != nil {
			t.Fatal(err)
		}
		bid, err := store.Put([]byte("payload"))
		if err != nil {
			t.Fatal(err)
		}
		plantCommit(t, root, meta.Commit{
			ID: fakeID(byte(i + 1)), Format: 2, BlockSize: 4096,
			Files: []meta.FileEntry{{Path: evil, Mode: 0o644, Size: 7, Blocks: []core.BlockID{bid}, BlockSizes: []int64{7}}},
		})
		dest := t.TempDir()
		if _, err := Restore(root, fakeID(byte(i+1)), RestoreOptions{To: dest}); err == nil {
			t.Fatalf("restore of path %q must fail", evil)
		}
	}
	ents, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("restore escaped into %s: %v", outside, ents)
	}
}

func TestRestoreRejectsMaliciousTreeEntryNames(t *testing.T) {
	for i, evil := range []string{"..", ".", "", "a/b", "a\\b"} {
		root := t.TempDir()
		if _, err := Init(root, 0); err != nil {
			t.Fatal(err)
		}
		store, err := meta.NewBlockStore(root)
		if err != nil {
			t.Fatal(err)
		}
		bid, err := store.Put([]byte("payload"))
		if err != nil {
			t.Fatal(err)
		}
		tree, err := json.Marshal([]meta.TreeEntry{{
			Name: evil, Kind: "f", Mode: 0o644, Size: 7,
			Blocks: []core.BlockID{bid}, Sizes: []int64{7},
		}})
		if err != nil {
			t.Fatal(err)
		}
		rootTree, err := store.Put(tree)
		if err != nil {
			t.Fatal(err)
		}
		plantCommit(t, root, meta.Commit{
			ID: fakeID(byte(i + 1)), Format: 3, BlockSize: 4096, RootTree: rootTree, FileCount: 1,
		})
		if _, err := Restore(root, fakeID(byte(i+1)), RestoreOptions{To: t.TempDir()}); err == nil {
			t.Fatalf("restore of tree entry %q must fail", evil)
		}
	}
}

func TestRestoreDoesNotFollowSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "inner.txt"), []byte("safe"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c1", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}

	// The destination has "sub" planted as a symlink pointing outside: the
	// restore must not write through it.
	outside := t.TempDir()
	dest := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dest, "sub")); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(root, id, RestoreOptions{To: dest}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(outside, "inner.txt")); !os.IsNotExist(err) {
		t.Fatal("restore wrote through a symlinked ancestor")
	}
	info, err := os.Lstat(filepath.Join(dest, "sub"))
	if err != nil || !info.Mode().IsDir() {
		t.Fatalf("symlinked ancestor not replaced with a real directory: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "sub", "inner.txt"))
	if err != nil || string(got) != "safe" {
		t.Fatalf("restored file wrong: %q %v", got, err)
	}
}

func TestRestoreKeepsDotFvs2LookalikeFiles(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".fvs2evil"), []byte("legit"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c1", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if _, err := Restore(root, id, RestoreOptions{To: dest}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dest, ".fvs2evil"))
	if err != nil || string(got) != "legit" {
		t.Fatalf(".fvs2evil must restore (it is not repo metadata): %q %v", got, err)
	}
	// Actual metadata paths inside a state are skipped, never written.
	if _, err := os.Stat(filepath.Join(dest, ".fvs2")); !os.IsNotExist(err) {
		t.Fatal("restore must not materialize .fvs2 metadata")
	}
}

// TestRestoreRepairsMetadataMatchingCorruption tampers a restored file while
// keeping size, mode and mtime identical: the default restore must verify
// content and repair it; --fast (FastSkip) must trust the metadata.
func TestRestoreRepairsMetadataMatchingCorruption(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f.bin"), []byte("genuine!"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c1", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if _, err := Restore(root, id, RestoreOptions{To: dest}); err != nil {
		t.Fatal(err)
	}

	tamper := func() {
		p := filepath.Join(dest, "f.bin")
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		mtime := info.ModTime()
		if err := os.WriteFile(p, []byte("tampered"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}

	tamper()
	if _, err := Restore(root, id, RestoreOptions{To: dest, FastSkip: true}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "f.bin"))
	if string(got) != "tampered" {
		t.Fatal("FastSkip must not read or repair metadata-matching files")
	}

	if _, err := Restore(root, id, RestoreOptions{To: dest}); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(filepath.Join(dest, "f.bin"))
	if string(got) != "genuine!" {
		t.Fatalf("default restore must repair tampered content, got %q", got)
	}
}

// TestRestoreInPlaceTakesRepoLock holds the repo lock and expects an
// in-place restore (and any --reset restore) to time out instead of racing
// a concurrent mutating operation.
func TestRestoreInPlaceTakesRepoLock(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Commit(root, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		t.Fatal(err)
	}

	lock, err := meta.LockRepo(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()

	oldTimeout := lockTimeout
	lockTimeout = 100 * time.Millisecond
	t.Cleanup(func() { lockTimeout = oldTimeout })

	if _, err := Restore(root, id, RestoreOptions{}); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("in-place restore under a held lock: %v", err)
	}
	if _, err := Restore(root, id, RestoreOptions{To: t.TempDir(), Reset: true}); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("--reset restore under a held lock: %v", err)
	}
	// A plain restore elsewhere needs no lock.
	if _, err := Restore(root, id, RestoreOptions{To: t.TempDir()}); err != nil {
		t.Fatalf("out-of-place restore: %v", err)
	}
}

// TestRestoreLeavesNoPartialFiles interrupts a restore mid-file via context
// cancellation racing... simpler: verifies no temp leftovers after a normal
// restore, and that a failing restore (missing block) leaves no partial file
// under the final name.
func TestRestoreLeavesNoPartialFiles(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		t.Fatal(err)
	}
	present, err := store.Put([]byte("present"))
	if err != nil {
		t.Fatal(err)
	}
	missing := core.BlockID(strings.Repeat("9", 64))
	plantCommit(t, root, meta.Commit{
		ID: fakeID(0xAB), Format: 2, BlockSize: 4096,
		Files: []meta.FileEntry{{
			Path: "torn.bin", Mode: 0o644, Size: 14,
			Blocks: []core.BlockID{present, missing}, BlockSizes: []int64{7, 7},
		}},
	})
	dest := t.TempDir()
	if _, err := Restore(root, fakeID(0xAB), RestoreOptions{To: dest}); err == nil {
		t.Fatal("restore with a missing block must fail")
	}
	if _, err := os.Stat(filepath.Join(dest, "torn.bin")); !os.IsNotExist(err) {
		t.Fatal("failed restore left a partial file under its final name")
	}
	ents, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".fvs2-restore-") {
			t.Fatalf("temp file leaked: %s", e.Name())
		}
	}
}
