package repo

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	core "github.com/fvs-lab/core"
	"golang.org/x/sys/unix"
)

func TestCheckoutMaterializesReusableNativeLayer(t *testing.T) {
	root := t.TempDir()
	blocks := filepath.Join(root, "blocks")
	content := []byte("shared executable")
	paths := make([]string, 0, 2)
	var firstState string

	for _, name := range []string{"first", "second"} {
		repository := filepath.Join(root, name)
		if _, err := InitWithOptions(repository, InitOptions{BlocksPath: blocks}); err != nil {
			t.Fatal(err)
		}
		writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Add(Entry{Path: "usr/bin/demo", Kind: EntryFile, Mode: 0o755, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
			t.Fatal(err)
		}
		result, err := writer.Commit()
		if err != nil {
			t.Fatal(err)
		}
		if firstState == "" {
			firstState = result.StateID
		}
		checkout, err := CheckoutContext(context.Background(), repository, result.StateID, CheckoutOptions{
			To: filepath.Join(root, "checkouts", name), PruneLooseBlocks: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !checkout.Created {
			t.Fatal("first checkout was reported as cached")
		}
		paths = append(paths, filepath.Join(checkout.Root, "usr/bin/demo"))
	}

	first, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	firstStat := first.Sys().(*syscall.Stat_t)
	secondStat := second.Sys().(*syscall.Stat_t)
	if firstStat.Dev != secondStat.Dev || firstStat.Ino != secondStat.Ino {
		t.Fatal("identical checkout files do not share one whole-file object")
	}

	files, err := StateFiles(filepath.Join(root, "first"), firstState)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range files[0].Blocks {
		if _, err := os.Stat(filepath.Join(blocks, string(block))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("loose block %s was not pruned: %v", block, err)
		}
	}
	store, err := core.NewDiskBlockStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range files[0].Blocks {
		if present, err := store.Has(block); err != nil || !present {
			t.Fatalf("object-backed block %s is unavailable: %v", block, err)
		}
	}

	cached, err := Checkout(filepath.Join(root, "first"), firstState, CheckoutOptions{
		To: filepath.Join(root, "checkouts", "first"), PruneLooseBlocks: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cached.Created || cached.Root != filepath.Join(root, "checkouts", "first", "rootfs") {
		t.Fatalf("cached checkout = %+v", cached)
	}
}

func TestCheckoutPreservesStickyDirectory(t *testing.T) {
	repository := t.TempDir()
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(Entry{Path: "run", Kind: EntryDir, Mode: 0o1755}, nil); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "checkout")
	checkout, err := Checkout(repository, state.StateID, CheckoutOptions{To: destination})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(checkout.Root, "run"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 || info.Mode()&os.ModeSticky == 0 {
		t.Fatalf("directory mode = %v", info.Mode())
	}
	if err := VerifyCheckout(repository, state.StateID, CheckoutOptions{To: destination}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckoutTranslatesOCIWhiteoutsForRootlessOverlay(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Path: "app", Kind: EntryDir, Mode: 0o755},
		{Path: "app/.wh.removed", Kind: EntryFile, Mode: 0o644},
		{Path: "cache", Kind: EntryDir, Mode: 0o755},
		{Path: "cache/.wh..wh..opq", Kind: EntryFile, Mode: 0o644},
	}
	for _, entry := range entries {
		var content *bytes.Reader
		if entry.Kind == EntryFile {
			content = bytes.NewReader(nil)
		}
		if err := writer.Add(entry, content); err != nil {
			t.Fatal(err)
		}
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	checkout, err := Checkout(repository, state.StateID, CheckoutOptions{
		To: filepath.Join(root, "checkout"), OverlayWhiteouts: true,
	})
	if err != nil {
		if errors.Is(err, ErrOverlayWhiteoutsUnsupported) {
			t.Skip(err)
		}
		t.Fatal(err)
	}

	whiteout := filepath.Join(checkout.Root, "app", "removed")
	if info, err := os.Stat(whiteout); err != nil || info.Size() != 0 {
		t.Fatalf("whiteout = %v, %v", info, err)
	}
	if value, err := readCheckoutXattr(whiteout, "user.overlay.whiteout"); err != nil || string(value) != "y" {
		t.Fatalf("whiteout xattr = %q, %v", value, err)
	}
	if value, err := readCheckoutXattr(filepath.Join(checkout.Root, "app"), "user.overlay.opaque"); err != nil || string(value) != "x" {
		t.Fatalf("whiteout parent xattr = %q, %v", value, err)
	}
	if value, err := readCheckoutXattr(filepath.Join(checkout.Root, "cache"), "user.overlay.opaque"); err != nil || string(value) != "y" {
		t.Fatalf("opaque directory xattr = %q, %v", value, err)
	}
	for _, marker := range []string{"app/.wh.removed", "cache/.wh..wh..opq"} {
		if _, err := os.Stat(filepath.Join(checkout.Root, marker)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("OCI marker %s leaked into checkout: %v", marker, err)
		}
	}
}

func TestCheckoutPreservesWhiteoutsByDefault(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(Entry{Path: ".wh.removed", Kind: EntryFile, Mode: 0o644}, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	checkout, err := Checkout(repository, state.StateID, CheckoutOptions{To: filepath.Join(root, "checkout")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(checkout.Root, ".wh.removed")); err != nil {
		t.Fatal(err)
	}
}

func TestCheckoutExistingOnlyDoesNotMaterialize(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Add(Entry{Path: "file", Kind: EntryFile, Mode: 0o644}, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "checkout")
	_, err = Checkout(repository, state.StateID, CheckoutOptions{To: destination, ExistingOnly: true})
	if !errors.Is(err, ErrCheckoutMissing) {
		t.Fatalf("existing-only checkout error = %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing-only lookup changed the destination: %v", err)
	}
}

func TestCheckoutFailureDoesNotPublishPartialTree(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("missing")
	if err := writer.Add(Entry{Path: "missing", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	files, err := StateFiles(repository, state.StateID)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := BlockStorePath(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(blocks, string(files[0].Blocks[0]))); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "checkout")
	if _, err := Checkout(repository, state.StateID, CheckoutOptions{To: destination}); err == nil {
		t.Fatal("checkout with a missing block succeeded")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed checkout published destination: %v", err)
	}
}

func TestCheckoutReplacesDamagedTree(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("original")
	if err := writer.Add(Entry{Path: "value", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "checkout")
	if _, err := Checkout(repository, state.StateID, CheckoutOptions{To: destination}); err != nil {
		t.Fatal(err)
	}
	value := filepath.Join(destination, "rootfs", "value")
	replacement := value + ".damaged"
	if err := os.WriteFile(replacement, []byte("damaged"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, value); err != nil {
		t.Fatal(err)
	}
	if _, err := Checkout(repository, state.StateID, CheckoutOptions{To: destination, ReplaceExisting: true}); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(value)
	if err != nil || !bytes.Equal(restored, content) {
		t.Fatalf("restored content = %q, err = %v", restored, err)
	}
	if err := VerifyCheckout(repository, state.StateID, CheckoutOptions{To: destination}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckoutFailedReplacementKeepsPublishedTree(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("published")
	if err := writer.Add(Entry{Path: "value", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "checkout")
	if _, err := Checkout(repository, state.StateID, CheckoutOptions{To: destination}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CheckoutContext(ctx, repository, state.StateID, CheckoutOptions{To: destination, ReplaceExisting: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("replacement error = %v", err)
	}
	stored, err := os.ReadFile(filepath.Join(destination, "rootfs", "value"))
	if err != nil || !bytes.Equal(stored, content) {
		t.Fatalf("published content = %q, err = %v", stored, err)
	}
}

func TestVerifyCheckoutDetectsChangedContentAndUnexpectedEntries(t *testing.T) {
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	if _, err := Init(repository, 0); err != nil {
		t.Fatal(err)
	}
	writer, err := BeginSnapshot(repository, SnapshotOptions{ComputeSHA256: true})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("verified content")
	if err := writer.Add(Entry{Path: "app/value", Kind: EntryFile, Mode: 0o644, Size: int64(len(content))}, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "checkout")
	opts := CheckoutOptions{To: destination}
	checkout, err := Checkout(repository, state.StateID, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckout(repository, state.StateID, opts); err != nil {
		t.Fatal(err)
	}
	value := filepath.Join(checkout.Root, "app/value")
	replacement := filepath.Join(checkout.Root, "app/.replacement")
	if err := os.WriteFile(replacement, []byte("tampered content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, value); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckout(repository, state.StateID, opts); err == nil {
		t.Fatal("changed checkout content passed verification")
	}
	if err := os.RemoveAll(destination); err != nil {
		t.Fatal(err)
	}
	checkout, err = Checkout(repository, state.StateID, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout.Root, "unexpected"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckout(repository, state.StateID, opts); err == nil {
		t.Fatal("unexpected checkout entry passed verification")
	}
}

func readCheckoutXattr(path, name string) ([]byte, error) {
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	value := make([]byte, size)
	_, err = unix.Getxattr(path, name, value)
	return value, err
}
