package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

type expectedCheckoutEntry struct {
	entry    meta.FileEntry
	implicit bool
	whiteout bool
}

// VerifyCheckout verifies a prepared checkout against an FVS state.
func VerifyCheckout(root, state string, opts CheckoutOptions) error {
	return VerifyCheckoutContext(context.Background(), root, state, opts)
}

// VerifyCheckoutContext verifies a prepared checkout against an FVS state.
func VerifyCheckoutContext(ctx context.Context, root, state string, opts CheckoutOptions) error {
	if opts.To == "" {
		return errors.New("checkout destination is required")
	}
	root, err := absolute(root)
	if err != nil {
		return err
	}
	destination, err := absolute(opts.To)
	if err != nil {
		return err
	}
	lock, err := meta.LockRepo(root, lockTimeout)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	id, err := meta.ResolveCommitID(root, state)
	if err != nil {
		return err
	}
	marker := checkoutMarker{
		Format: checkoutFormat, StateID: id,
		ClearPrivilegedBits: opts.ClearPrivilegedBits,
		OverlayWhiteouts:    opts.OverlayWhiteouts,
	}
	if _, ready := existingCheckout(destination, marker); !ready {
		return ErrCheckoutMissing
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return err
	}
	blocksPath, err := meta.BlockStorePath(root)
	if err != nil {
		return err
	}
	blocks, _, err := core.NewObjectBackedBlockStore(blocksPath)
	if err != nil {
		return err
	}
	files, err := meta.CommitFiles(blocks, commit)
	if err != nil {
		return err
	}
	expected, opaque, err := expectedCheckout(files, opts)
	if err != nil {
		return err
	}
	checkoutRoot := filepath.Join(destination, "rootfs")
	seen := make(map[string]struct{}, len(expected))
	err = filepath.WalkDir(checkoutRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == checkoutRoot {
			return nil
		}
		relative, err := filepath.Rel(checkoutRoot, name)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		wanted, exists := expected[relative]
		if !exists {
			return fmt.Errorf("checkout contains unexpected entry %q", relative)
		}
		if err := verifyCheckoutEntry(name, wanted, blocks, opts); err != nil {
			return fmt.Errorf("checkout entry %q: %w", relative, err)
		}
		seen[relative] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	if len(seen) != len(expected) {
		for name := range expected {
			if _, exists := seen[name]; !exists {
				return fmt.Errorf("checkout entry %q is missing", name)
			}
		}
	}
	for name, value := range opaque {
		directory := checkoutRoot
		if name != "" {
			directory = filepath.Join(checkoutRoot, filepath.FromSlash(name))
		}
		if err := verifyOverlayOpaque(directory, value); err != nil {
			return fmt.Errorf("checkout directory %q: %w", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(destination, "checkout.json"))
	if err != nil {
		return err
	}
	var actual checkoutMarker
	if err := json.Unmarshal(data, &actual); err != nil || actual != marker {
		return ErrCheckoutMissing
	}
	return nil
}

func expectedCheckout(files []meta.FileEntry, opts CheckoutOptions) (map[string]expectedCheckoutEntry, map[string]string, error) {
	expected := make(map[string]expectedCheckoutEntry)
	opaque := make(map[string]string)
	addParents := func(name string) {
		for parent := path.Dir(name); parent != "." && parent != ""; parent = path.Dir(parent) {
			if _, exists := expected[parent]; !exists {
				expected[parent] = expectedCheckoutEntry{entry: meta.FileEntry{Path: parent, Kind: string(EntryDir)}, implicit: true}
			}
		}
	}
	for _, entry := range files {
		if err := meta.ValidateRelPath(entry.Path); err != nil {
			return nil, nil, fmt.Errorf("checkout entry: %w", err)
		}
		if entry.Path == ".fvs2" || strings.HasPrefix(entry.Path, ".fvs2/") {
			continue
		}
		name := entry.Path
		if opts.OverlayWhiteouts && strings.HasPrefix(path.Base(name), ".wh.") {
			directory := strings.Trim(path.Dir(name), "/")
			if directory == "." {
				directory = ""
			}
			addParents(name)
			if path.Base(name) == ".wh..wh..opq" {
				opaque[directory] = "y"
				continue
			}
			name = path.Join(directory, strings.TrimPrefix(path.Base(name), ".wh."))
			if opaque[directory] != "y" {
				opaque[directory] = "x"
			}
			expected[name] = expectedCheckoutEntry{entry: meta.FileEntry{Path: name, Kind: string(EntryFile), Mode: 0o600}, whiteout: true}
			addParents(name)
			continue
		}
		expected[name] = expectedCheckoutEntry{entry: entry}
		addParents(name)
	}
	return expected, opaque, nil
}

func verifyCheckoutEntry(name string, expected expectedCheckoutEntry, blocks *core.DiskBlockStore, opts CheckoutOptions) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	entry := expected.entry
	mode := entry.Mode
	if opts.ClearPrivilegedBits {
		mode &^= 0o6000
	}
	switch entry.Kind {
	case string(EntryDir):
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("expected directory")
		}
		if !expected.implicit && posixMode(info.Mode()) != mode {
			return fmt.Errorf("mode is %04o, expected %04o", posixMode(info.Mode()), mode)
		}
		return nil
	case string(EntrySymlink):
		if info.Mode()&os.ModeSymlink == 0 {
			return errors.New("expected symbolic link")
		}
		target, err := os.Readlink(name)
		if err != nil {
			return err
		}
		if target != entry.Link {
			return errors.New("symbolic link target changed")
		}
		return nil
	case string(EntryFIFO):
		if info.Mode()&os.ModeNamedPipe == 0 {
			return errors.New("expected fifo")
		}
		if posixMode(info.Mode()) != mode {
			return fmt.Errorf("mode is %04o, expected %04o", posixMode(info.Mode()), mode)
		}
		return nil
	case "", string(EntryFile):
		if !info.Mode().IsRegular() {
			return errors.New("expected regular file")
		}
		if posixMode(info.Mode()) != mode {
			return fmt.Errorf("mode is %04o, expected %04o", posixMode(info.Mode()), mode)
		}
		if expected.whiteout {
			if info.Size() != 0 {
				return errors.New("whiteout is not empty")
			}
			return verifyOverlayWhiteout(name)
		}
		return verifyCheckoutFile(name, entry, blocks)
	default:
		return fmt.Errorf("unsupported kind %q", entry.Kind)
	}
}

func verifyCheckoutFile(name string, entry meta.FileEntry, blocks *core.DiskBlockStore) error {
	if len(entry.Blocks) != len(entry.BlockSizes) {
		return errors.New("block metadata is inconsistent")
	}
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	var size int64
	for index, block := range entry.Blocks {
		content, err := blocks.Get(block)
		if err != nil {
			return err
		}
		if int64(len(content)) != entry.BlockSizes[index] {
			return errors.New("block size changed")
		}
		actual := make([]byte, len(content))
		if _, err := io.ReadFull(file, actual); err != nil {
			return err
		}
		if !bytes.Equal(actual, content) {
			return errors.New("content changed")
		}
		size += int64(len(content))
	}
	if size != entry.Size {
		return fmt.Errorf("size is %d, expected %d", size, entry.Size)
	}
	var extra [1]byte
	if count, err := file.Read(extra[:]); count != 0 || !errors.Is(err, io.EOF) {
		return errors.New("content is longer than expected")
	}
	return nil
}
