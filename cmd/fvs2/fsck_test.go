package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fvs2/internal/meta"
)

func TestFsckPassesOnHealthyRepo(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "a.txt", []byte("hello"))
	write(t, root, "sub/b.bin", []byte("world"))
	commitAll(t, cli, "c1")

	if err := (&FsckCmd{Root: cli}).Run(); err != nil {
		t.Fatalf("fsck on a healthy repo: %v", err)
	}
	if err := (&FsckCmd{Full: true, Root: cli}).Run(); err != nil {
		t.Fatalf("fsck --full on a healthy repo: %v", err)
	}
}

func TestFsckDetectsMissingBlock(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f.bin", []byte("some content"))
	commitAll(t, cli, "c1")

	c, err := meta.LoadCommit(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := meta.CommitFiles(store, c)
	if err != nil {
		t.Fatal(err)
	}
	victim := files[0].Blocks[0]
	if err := os.Remove(filepath.Join(root, ".fvs2", "blocks", string(victim))); err != nil {
		t.Fatal(err)
	}
	if err := (&FsckCmd{Root: cli}).Run(); err == nil {
		t.Fatal("fsck must fail when a referenced block is missing")
	}
}

func TestFsckFullDetectsCorruptBlock(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f.bin", []byte("integrity matters here"))
	commitAll(t, cli, "c1")

	c, err := meta.LoadCommit(root, headID(t, root))
	if err != nil {
		t.Fatal(err)
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := meta.CommitFiles(store, c)
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(root, ".fvs2", "blocks", string(files[0].Blocks[0]))
	if err := os.WriteFile(victim, []byte("flipped bytes, same name"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Existence-only fsck cannot see it; --full must.
	if err := (&FsckCmd{Root: cli}).Run(); err != nil {
		t.Fatalf("existence fsck should still pass: %v", err)
	}
	if err := (&FsckCmd{Full: true, Root: cli}).Run(); err == nil {
		t.Fatal("fsck --full must fail on a corrupt block")
	}
}

func TestFsckDetectsDanglingRef(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f", []byte("x"))
	commitAll(t, cli, "c1")
	if err := meta.CreateBranch(root, "dangling"); err != nil {
		t.Fatal(err)
	}
	if err := meta.WriteBranchHead(root, "dangling", strings.Repeat("d", 64)); err != nil {
		t.Fatal(err)
	}
	if err := (&FsckCmd{Root: cli}).Run(); err == nil {
		t.Fatal("fsck must fail on a branch pointing at an unknown state")
	}
}

func TestPackVerifyDetectsCorruption(t *testing.T) {
	root, cli := newRepo(t)
	write(t, root, "f.bin", []byte(strings.Repeat("payload ", 4000)))
	commitAll(t, cli, "c1")
	if err := (&PackCmd{Root: cli}).Run(); err != nil {
		t.Fatal(err)
	}
	if err := (&PackVerifyCmd{Root: cli}).Run(); err != nil {
		t.Fatalf("pack verify on a clean pack: %v", err)
	}

	blocksDir := filepath.Join(root, ".fvs2", "blocks")
	ents, err := os.ReadDir(blocksDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "pack-") {
			continue
		}
		p := filepath.Join(blocksDir, e.Name())
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 0xff
		if err := os.WriteFile(p, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&PackVerifyCmd{Root: cli}).Run(); err == nil {
		t.Fatal("pack verify must fail on a corrupt pack")
	}
}
