package repo

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	core "github.com/fvs-lab/core"
)

func TestSnapshotUsesSharedBlockStore(t *testing.T) {
	root := t.TempDir()
	blocks := filepath.Join(root, "blocks")
	content := bytes.Repeat([]byte("shared-content\n"), 2048)
	stamp := time.Unix(1700000000, 0).UTC()

	var count int
	for _, name := range []string{"first", "second"} {
		repository, err := InitWithOptions(filepath.Join(root, name), InitOptions{BlocksPath: blocks})
		if err != nil {
			t.Fatal(err)
		}
		if repository.BlocksPath != blocks {
			t.Fatalf("blocks path = %q, want %q", repository.BlocksPath, blocks)
		}
		writer, err := BeginSnapshot(repository.Path, SnapshotOptions{Message: "layer", ComputeSHA256: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Add(Entry{Path: "app/data", Kind: EntryFile, Mode: 0o644, Size: int64(len(content)), ModTime: stamp}, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Commit(); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(blocks)
		if err != nil {
			t.Fatal(err)
		}
		if name == "first" {
			count = len(entries)
		} else if len(entries) != count {
			t.Fatalf("shared import added blocks: before %d, after %d", count, len(entries))
		}
	}
}

func TestSnapshotPreservesUnixBackslashes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("backslash is a path separator on Windows")
	}
	root := t.TempDir()
	repository, err := Init(filepath.Join(root, "repository"), 0)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	name := `system-systemd\x2dmute\x2dconsole.slice`
	if err := os.WriteFile(filepath.Join(source, name), []byte("slice"), 0o644); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository.Path, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AddTree(source); err != nil {
		t.Fatal(err)
	}
	result, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(repository.Path, result.StateID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != name {
		t.Fatalf("snapshot paths = %+v", files)
	}
}

func TestSnapshotAddsExistingBlockReference(t *testing.T) {
	root := t.TempDir()
	blocks := filepath.Join(root, "blocks")
	first, err := InitWithOptions(filepath.Join(root, "first"), InitOptions{BlocksPath: blocks})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(first.Path, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("shared payload")
	if err := writer.Add(Entry{Path: "source", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	result, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(first.Path, result.StateID)
	if err != nil {
		t.Fatal(err)
	}

	second, err := InitWithOptions(filepath.Join(root, "second"), InitOptions{BlocksPath: blocks})
	if err != nil {
		t.Fatal(err)
	}
	files[0].Path = "reused"
	writer, err = BeginSnapshot(second.Path, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AddReference(files[0]); err != nil {
		t.Fatal(err)
	}
	result, err = writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reused, err := StateFiles(second.Path, result.StateID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reused) != 1 || reused[0].ContentDigest != files[0].ContentDigest {
		t.Fatalf("reused entry = %+v", reused)
	}
}

func TestSnapshotRejectsMissingBlockReference(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	err = writer.AddReference(FileEntry{
		Path: "missing", Kind: string(EntryFile), Size: 1,
		Blocks:     []core.BlockID{"0000000000000000000000000000000000000000000000000000000000000000"},
		BlockSizes: []int64{1},
	})
	if err == nil {
		t.Fatal("expected missing block rejection")
	}
}

func TestGCSharedKeepsBlocksFromEveryRepository(t *testing.T) {
	root := t.TempDir()
	blocks := filepath.Join(root, "blocks")
	var repositories []string
	for _, name := range []string{"first", "second"} {
		repository, err := InitWithOptions(filepath.Join(root, name), InitOptions{BlocksPath: blocks})
		if err != nil {
			t.Fatal(err)
		}
		writer, err := BeginSnapshot(repository.Path, SnapshotOptions{Message: name})
		if err != nil {
			t.Fatal(err)
		}
		content := []byte("content-" + name)
		if err := writer.Add(Entry{Path: "value", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Commit(); err != nil {
			t.Fatal(err)
		}
		repositories = append(repositories, repository.Path)
	}
	store, err := core.NewDiskBlockStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := store.Put([]byte("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := GCShared(context.Background(), blocks, repositories, GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Repositories != 2 || result.RemovedBlocks != 1 {
		t.Fatalf("gc result = %+v", result)
	}
	if present, err := store.Has(orphan); err != nil || present {
		t.Fatalf("orphan present = %v, err = %v", present, err)
	}
	for _, repository := range repositories {
		states, err := States(repository)
		if err != nil || len(states) != 1 {
			t.Fatalf("states for %s = %d, %v", repository, len(states), err)
		}
		if _, err := StateFiles(repository, states[0].ID); err != nil {
			t.Fatalf("read %s after gc: %v", repository, err)
		}
	}
}

func TestGCSharedRemovesEveryBlockWithoutRepositories(t *testing.T) {
	blocks := filepath.Join(t.TempDir(), "blocks")
	store, err := core.NewDiskBlockStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := store.Put([]byte("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := GCShared(context.Background(), blocks, nil, GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Repositories != 0 || result.RemovedBlocks != 1 || result.RemovedBytes != 6 {
		t.Fatalf("gc result = %+v", result)
	}
	if present, err := store.Has(orphan); err != nil || present {
		t.Fatalf("orphan present = %v, err = %v", present, err)
	}
}

func TestSnapshotPreservesLayerEntries(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000, 123).UTC()
	content := []byte("payload")
	writer, err := BeginSnapshot(root, SnapshotOptions{Message: "layer", ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	entries := []struct {
		entry   Entry
		content []byte
	}{
		{Entry{Path: "bin", Kind: EntryDir, Mode: 0o750, ModTime: stamp}, nil},
		{Entry{Path: "bin/tool", Kind: EntryFile, Mode: 0o755, Size: int64(len(content)), ModTime: stamp}, content},
		{Entry{Path: "bin/tool-copy", Kind: EntryHardlink, Mode: 0o755, ModTime: stamp, Link: "bin/tool"}, nil},
		{Entry{Path: "bin/current", Kind: EntrySymlink, Mode: 0o777, ModTime: stamp, Link: "tool"}, nil},
		{Entry{Path: "run/pipe", Kind: EntryFIFO, Mode: 0o620, ModTime: stamp}, nil},
	}
	for _, item := range entries {
		if err := writer.Add(item.entry, bytes.NewReader(item.content)); err != nil {
			t.Fatal(err)
		}
	}
	result, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(root, result.StateID)
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]FileEntry, len(files))
	for _, file := range files {
		byPath[file.Path] = file
	}
	if got := byPath["bin"].Kind; got != string(EntryDir) {
		t.Fatalf("directory kind = %q", got)
	}
	if got := byPath["run/pipe"].Kind; got != string(EntryFIFO) {
		t.Fatalf("fifo kind = %q", got)
	}
	if byPath["bin/tool"].ContentDigest == "" {
		t.Fatal("regular file digest is empty")
	}
	if len(byPath["bin/tool"].Blocks) == 0 || byPath["bin/tool"].Blocks[0] != byPath["bin/tool-copy"].Blocks[0] {
		t.Fatal("hardlink content does not reuse blocks")
	}

	destination := filepath.Join(t.TempDir(), "restore")
	if _, err := Restore(root, result.StateID, RestoreOptions{To: destination}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(destination, "bin")); err != nil || info.Mode().Perm() != 0o750 {
		t.Fatalf("directory mode = %v, err = %v", info, err)
	}
	if target, err := os.Readlink(filepath.Join(destination, "bin/current")); err != nil || target != "tool" {
		t.Fatalf("symlink target = %q, err = %v", target, err)
	}
	if info, err := os.Lstat(filepath.Join(destination, "run/pipe")); err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("fifo mode = %v, err = %v", info, err)
	}
}

func TestSnapshotAbortLeavesNoState(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshotContext(context.Background(), root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(Entry{Path: "value", Kind: EntryFile, Mode: 0o644, Size: 5}, bytes.NewBufferString("value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
	states, err := States(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("states after abort = %d", len(states))
	}
}

func TestSnapshotAddTreePreservesUnreadableEmptyFile(t *testing.T) {
	source := t.TempDir()
	marker := filepath.Join(source, ".wh.removed")
	if err := os.WriteFile(marker, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(marker, 0); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AddTree(source); err != nil {
		t.Fatal(err)
	}
	result, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(root, result.StateID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != ".wh.removed" || files[0].Mode != 0 || files[0].Size != 0 {
		t.Fatalf("files = %+v", files)
	}
}

func TestSnapshotRejectsDigestMismatch(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(root, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	err = writer.Add(Entry{
		Path: "value", Kind: EntryFile, Mode: 0o644, Size: 5,
		ContentDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}, bytes.NewBufferString("value"))
	if err == nil {
		t.Fatal("expected content digest mismatch")
	}
}
