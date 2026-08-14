package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

const checkoutFormat = 1

var (
	ErrCheckoutMissing             = errors.New("checkout is not prepared")
	ErrOverlayWhiteoutsUnsupported = errors.New("overlay whiteouts are unsupported")
)

type CheckoutOptions struct {
	To                  string
	ClearPrivilegedBits bool
	OverlayWhiteouts    bool
	PruneLooseBlocks    bool
	ExistingOnly        bool
	ReplaceExisting     bool
}

type CheckoutResult struct {
	StateID string
	Root    string
	Created bool
	Files   int
	Written int64
	Reused  int64
}

type checkoutMarker struct {
	Format              int    `json:"format"`
	StateID             string `json:"state_id"`
	ClearPrivilegedBits bool   `json:"clear_privileged_bits"`
	OverlayWhiteouts    bool   `json:"overlay_whiteouts"`
}

func Checkout(root, state string, opts CheckoutOptions) (CheckoutResult, error) {
	return CheckoutContext(context.Background(), root, state, opts)
}

func CheckoutContext(ctx context.Context, root, state string, opts CheckoutOptions) (CheckoutResult, error) {
	if opts.To == "" {
		return CheckoutResult{}, errors.New("checkout destination is required")
	}
	if opts.ExistingOnly && opts.ReplaceExisting {
		return CheckoutResult{}, errors.New("existing-only checkout cannot be replaced")
	}
	root, err := absolute(root)
	if err != nil {
		return CheckoutResult{}, err
	}
	destination, err := absolute(opts.To)
	if err != nil {
		return CheckoutResult{}, err
	}
	if destination == root || strings.HasPrefix(destination, filepath.Join(root, ".fvs2")+string(os.PathSeparator)) {
		return CheckoutResult{}, errors.New("checkout destination overlaps repository metadata")
	}

	lock, err := meta.LockRepo(root, lockTimeout)
	if err != nil {
		return CheckoutResult{}, err
	}
	defer lock.Unlock()
	id, err := meta.ResolveCommitID(root, state)
	if err != nil {
		return CheckoutResult{}, err
	}
	marker := checkoutMarker{
		Format: checkoutFormat, StateID: id,
		ClearPrivilegedBits: opts.ClearPrivilegedBits,
		OverlayWhiteouts:    opts.OverlayWhiteouts,
	}
	if result, ready := existingCheckout(destination, marker); ready && !opts.ReplaceExisting {
		return result, nil
	}
	if opts.ExistingOnly {
		return CheckoutResult{}, ErrCheckoutMissing
	}

	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return CheckoutResult{}, err
	}
	blocksPath, err := meta.BlockStorePath(root)
	if err != nil {
		return CheckoutResult{}, err
	}
	blocks, objects, err := core.NewObjectBackedBlockStore(blocksPath)
	if err != nil {
		return CheckoutResult{}, err
	}
	files, err := meta.CommitFiles(blocks, commit)
	if err != nil {
		return CheckoutResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return CheckoutResult{}, err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".partial-")
	if err != nil {
		return CheckoutResult{}, err
	}
	defer os.RemoveAll(temporary)
	previous := temporary + ".previous"
	defer os.RemoveAll(previous)
	checkoutRoot := filepath.Join(temporary, "rootfs")
	if err := os.Mkdir(checkoutRoot, 0o755); err != nil {
		return CheckoutResult{}, err
	}

	result := CheckoutResult{StateID: id, Root: filepath.Join(destination, "rootfs"), Created: true}
	if err := materializeCheckout(ctx, checkoutRoot, blocks, objects, files, opts, &result); err != nil {
		return CheckoutResult{}, err
	}
	if err := objects.Sync(); err != nil {
		return CheckoutResult{}, err
	}
	if err := writeJSONAtomic(filepath.Join(temporary, "checkout.json"), marker); err != nil {
		return CheckoutResult{}, err
	}
	if err := publishCheckout(temporary, destination, previous); err != nil {
		if cached, ready := existingCheckout(destination, marker); ready {
			return cached, nil
		}
		return CheckoutResult{}, err
	}
	parent, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return CheckoutResult{}, err
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil {
		return CheckoutResult{}, syncErr
	}
	if closeErr != nil {
		return CheckoutResult{}, closeErr
	}
	return result, nil
}

func publishCheckoutFallback(source, destination, previous string) error {
	if err := os.Rename(destination, previous); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.Rename(source, destination)
		}
		return err
	}
	if err := os.Rename(source, destination); err != nil {
		return errors.Join(err, os.Rename(previous, destination))
	}
	return nil
}

func existingCheckout(destination string, expected checkoutMarker) (CheckoutResult, bool) {
	data, err := os.ReadFile(filepath.Join(destination, "checkout.json"))
	if err != nil {
		return CheckoutResult{}, false
	}
	var marker checkoutMarker
	if json.Unmarshal(data, &marker) != nil || marker != expected {
		return CheckoutResult{}, false
	}
	root := filepath.Join(destination, "rootfs")
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return CheckoutResult{}, false
	}
	return CheckoutResult{StateID: marker.StateID, Root: root}, true
}

func materializeCheckout(ctx context.Context, root string, blocks *core.DiskBlockStore, objects *core.ObjectStore, files []meta.FileEntry, opts CheckoutOptions, result *CheckoutResult) error {
	for _, entry := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := meta.ValidateRelPath(entry.Path); err != nil {
			return fmt.Errorf("checkout entry: %w", err)
		}
		if entry.Path == ".fvs2" || strings.HasPrefix(entry.Path, ".fvs2/") {
			continue
		}
		if opts.OverlayWhiteouts && strings.HasPrefix(path.Base(entry.Path), ".wh.") {
			if err := materializeOverlayWhiteout(root, entry.Path); err != nil {
				return err
			}
		}
	}

	directories := make([]meta.FileEntry, 0)
	for _, entry := range files {
		if entry.Kind == string(EntryDir) {
			directories = append(directories, entry)
		}
	}
	sort.Slice(directories, func(i, j int) bool {
		leftDepth := strings.Count(directories[i].Path, "/")
		rightDepth := strings.Count(directories[j].Path, "/")
		if leftDepth == rightDepth {
			return directories[i].Path < directories[j].Path
		}
		return leftDepth < rightDepth
	})
	for _, entry := range directories {
		if err := materializeCheckoutDirectory(root, entry.Path); err != nil {
			return err
		}
	}

	for _, entry := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Kind == string(EntryDir) || opts.OverlayWhiteouts && strings.HasPrefix(path.Base(entry.Path), ".wh.") {
			continue
		}
		output, err := safeOutPath(root, entry.Path)
		if err != nil {
			return err
		}
		if err := removeCheckoutConflict(output); err != nil {
			return err
		}
		mode := entry.Mode
		if opts.ClearPrivilegedBits {
			mode &^= 0o6000
		}
		switch entry.Kind {
		case string(EntrySymlink):
			if err := os.Symlink(entry.Link, output); err != nil {
				return err
			}
		case string(EntryFIFO):
			if err := syscall.Mkfifo(output, mode); err != nil {
				return err
			}
		case "", string(EntryFile):
			materialized, err := objects.MaterializeBlocks(ctx, output, entry.Blocks, entry.BlockSizes, blocks, core.MaterializeOptions{
				Mode: mode, Size: entry.Size, ContentDigest: entry.ContentDigest,
				PruneLoose: opts.PruneLooseBlocks, Deferred: true,
			})
			if err != nil {
				return err
			}
			result.Files++
			result.Written += materialized.Written
			result.Reused += materialized.Reused
		default:
			return fmt.Errorf("checkout entry %q has unsupported kind %q", entry.Path, entry.Kind)
		}
	}

	sort.Slice(directories, func(i, j int) bool {
		leftDepth := strings.Count(directories[i].Path, "/")
		rightDepth := strings.Count(directories[j].Path, "/")
		if leftDepth == rightDepth {
			return directories[i].Path > directories[j].Path
		}
		return leftDepth > rightDepth
	})
	for _, entry := range directories {
		mode := entry.Mode
		if opts.ClearPrivilegedBits {
			mode &^= 0o6000
		}
		if err := os.Chmod(filepath.Join(root, filepath.FromSlash(entry.Path)), fileModeFromPOSIX(mode)); err != nil {
			return err
		}
	}
	return nil
}

func materializeCheckoutDirectory(root, name string) error {
	output, err := safeOutPath(root, name)
	if err != nil {
		return err
	}
	info, err := os.Lstat(output)
	if err == nil && !info.IsDir() {
		if err := os.Remove(output); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(output, 0o755)
}

func removeCheckoutConflict(name string) error {
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		return os.RemoveAll(name)
	}
	return os.Remove(name)
}

func materializeOverlayWhiteout(root, name string) error {
	clean := strings.Trim(filepath.ToSlash(name), "/")
	base := path.Base(clean)
	directory := strings.Trim(path.Dir(clean), "/")
	parent := root
	if directory != "" && directory != "." {
		var err error
		parent, err = safeOutPath(root, directory)
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	if base == ".wh..wh..opq" {
		return setOverlayOpaque(parent, "y")
	}
	target := filepath.Join(parent, strings.TrimPrefix(base, ".wh."))
	if err := removeCheckoutConflict(target); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := setOverlayWhiteout(target); err != nil {
		return err
	}
	return setOverlayOpaque(parent, "x")
}
