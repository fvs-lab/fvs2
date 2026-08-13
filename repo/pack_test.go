package repo

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fvs-lab/fvs2/internal/meta"
)

// TestPackedRepoEndToEnd exercises the whole pack path: several versions of
// a text file (adaptive chunking), compaction into lineage frames, byte-exact
// restore of every version from the packed store, push from it, and the
// frame amnesty after a drop.
func TestPackedRepoEndToEnd(t *testing.T) {
	dir := t.TempDir()
	if _, err := Init(dir, 0); err != nil {
		t.Fatal(err)
	}

	// Three versions of a .po-like text file plus one binary. Varied lines,
	// so content-defined cuts happen naturally.
	var sb strings.Builder
	for i := 0; i < 800; i++ {
		fmt.Fprintf(&sb, "msgid \"string number %d\"\nmsgstr \"stringa numero %d\"\n", i, i)
	}
	base := sb.String()
	versions := []string{
		"# rev 1\n" + base,
		"# rev 2\n" + strings.Replace(base, "stringa numero 42", "stringa quarantadue", 1),
		"# rev 3\n" + strings.Replace(base, "string number 700", "renamed string", 1),
	}
	var stateIDs []string
	for i, v := range versions {
		if err := os.WriteFile(filepath.Join(dir, "it.po"), []byte(v), 0o644); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			bin := bytes.Repeat([]byte{0x00, 0x7f, 0x33}, 40000)
			if err := os.WriteFile(filepath.Join(dir, "tool.bin"), bin, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		res, err := Commit(dir, fmt.Sprintf("rev %d", i+1), false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Created {
			t.Fatalf("rev %d produced no state", i+1)
		}
		stateIDs = append(stateIDs, res.StateID)
		bumpMtimes(t, dir)
	}

	// The adaptive policy must have chunked the text file finely.
	files, err := StateFiles(dir, stateIDs[2])
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Path == "it.po" && len(f.Blocks) < 4 {
			t.Fatalf("text file chunked into %d blocks, adaptive policy not applied", len(f.Blocks))
		}
	}

	// Compact into lineage-ordered frames.
	store, err := meta.NewBlockStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := meta.OrderedLiveBlocks(dir, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(ordered); err != nil {
		t.Fatal(err)
	}
	if packed, _ := store.HasPacks(); !packed {
		t.Fatal("no packs after compaction")
	}

	// Every version restores byte-exact from the packed store.
	for i, id := range stateIDs {
		dest := t.TempDir()
		if _, err := Restore(dir, id, RestoreOptions{To: dest}); err != nil {
			t.Fatalf("restore rev %d from pack: %v", i+1, err)
		}
		got, err := os.ReadFile(filepath.Join(dest, "it.po"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != versions[i] {
			t.Fatalf("rev %d differs after pack round trip", i+1)
		}
	}

	// Push straight out of the packed store.
	_, rm := newRemote(t, "")
	push, err := Push(dir, rm, "", false)
	if err != nil {
		t.Fatalf("push from packed store: %v", err)
	}
	if push.UploadedBlocks == 0 {
		t.Fatal("push moved nothing")
	}

	// Amnesty: drop the first state and recompact; its exclusive chunks die.
	firstFiles, err := StateFiles(dir, stateIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.DeleteCommit(dir, stateIDs[0]); err != nil {
		t.Fatal(err)
	}
	ordered, err = meta.OrderedLiveBlocks(dir, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(ordered); err != nil {
		t.Fatal(err)
	}
	kept := map[string]bool{}
	for _, id := range ordered {
		kept[string(id)] = true
	}
	gone := 0
	for _, f := range firstFiles {
		for _, b := range f.Blocks {
			if !kept[string(b)] {
				if ok, _ := store.Has(b); ok {
					t.Fatalf("dead chunk %s survived the amnesty", b)
				}
				gone++
			}
		}
	}
	if gone == 0 {
		t.Fatal("expected the dropped state to free at least one chunk")
	}
	// Surviving states still restore.
	dest := t.TempDir()
	if _, err := Restore(dir, stateIDs[2], RestoreOptions{To: dest}); err != nil {
		t.Fatalf("restore after amnesty: %v", err)
	}
}

// bumpMtimes pushes every file's mtime forward so same-second commits are
// always detected.
func bumpMtimes(t *testing.T, dir string) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || strings.Contains(p, ".fvs2") {
			return nil
		}
		next := info.ModTime().Add(2e9)
		return os.Chtimes(p, next, next)
	})
}
