package meta

import (
	"testing"

	core "github.com/fvs-lab/core"
)

// countingStore is an in-memory block store that counts reads, so tests can
// prove how many times metadata objects are actually decoded.
type countingStore struct {
	blocks map[core.BlockID][]byte
	gets   int
}

func newCountingStore() *countingStore {
	return &countingStore{blocks: map[core.BlockID][]byte{}}
}

func (s *countingStore) Put(data []byte) (core.BlockID, error) {
	id := core.ContentID(data)
	s.blocks[id] = append([]byte(nil), data...)
	return id, nil
}

func (s *countingStore) Get(id core.BlockID) ([]byte, error) {
	s.gets++
	b, ok := s.blocks[id]
	if !ok {
		return nil, core.ErrBlockNotFound
	}
	return b, nil
}

func testFiles() []FileEntry {
	return []FileEntry{
		{Path: "a.txt", Mode: 0o644, Size: 1, Blocks: []core.BlockID{"01"}, BlockSizes: []int64{1}},
		{Path: "sub/b.txt", Mode: 0o644, Size: 2, Blocks: []core.BlockID{"02", "03", "04"}, BlockSizes: []int64{1, 1, 0}},
		{Path: "sub/deep/c", Mode: 0o600, Size: 1, Blocks: []core.BlockID{"05"}, BlockSizes: []int64{1}},
		{Path: "link", Mode: 0o777, Link: "a.txt"},
	}
}

// TestCommitReachSinglePass proves that one traversal yields both the file
// list and the metadata closure, matching what the two legacy walks return.
func TestCommitReachSinglePass(t *testing.T) {
	store := newCountingStore()
	root, err := WriteTree(store, testFiles())
	if err != nil {
		t.Fatal(err)
	}
	commit := Commit{ID: "x", Format: 3, RootTree: root}

	files, err := CommitFiles(store, commit)
	if err != nil {
		t.Fatal(err)
	}
	trees, err := TreeBlocks(store, root)
	if err != nil {
		t.Fatal(err)
	}

	reach, err := CommitReach(store, commit, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reach.Files) != len(files) {
		t.Fatalf("reach files = %d, want %d", len(reach.Files), len(files))
	}
	for i := range files {
		if reach.Files[i].Path != files[i].Path {
			t.Fatalf("file %d = %q, want %q", i, reach.Files[i].Path, files[i].Path)
		}
	}
	if len(reach.MetaBlocks) != len(trees) {
		t.Fatalf("reach meta = %d, want %d", len(reach.MetaBlocks), len(trees))
	}

	blocks, err := CommitBlocks(store, commit)
	if err != nil {
		t.Fatal(err)
	}
	want := map[core.BlockID]bool{}
	for _, id := range trees {
		want[id] = true
	}
	for _, f := range files {
		for _, b := range f.Blocks {
			want[b] = true
		}
	}
	if len(blocks) != len(want) {
		t.Fatalf("CommitBlocks = %d ids, want %d", len(blocks), len(want))
	}
	for _, b := range blocks {
		if !want[b] {
			t.Fatalf("unexpected block %s", b)
		}
	}
}

// TestTreeCacheDecodesOnce walks the same state twice with a shared cache:
// the second walk must not touch the store at all.
func TestTreeCacheDecodesOnce(t *testing.T) {
	store := newCountingStore()
	root, err := WriteTree(store, testFiles())
	if err != nil {
		t.Fatal(err)
	}
	commit := Commit{ID: "x", Format: 3, RootTree: root}

	cache := NewTreeCache()
	if _, err := CommitReach(store, commit, cache); err != nil {
		t.Fatal(err)
	}
	cold := store.gets
	if cold == 0 {
		t.Fatal("first walk should read metadata objects")
	}
	if _, err := CommitReach(store, commit, cache); err != nil {
		t.Fatal(err)
	}
	if store.gets != cold {
		t.Fatalf("cached walk read %d more objects", store.gets-cold)
	}
}
