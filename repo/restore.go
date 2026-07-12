package repo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

type State struct {
	ID        string
	CreatedAt time.Time
	Message   string
}

// States lists the saved states, newest first. The index records states in
// creation order, which stays correct even when timestamps collide within the
// same second.
func States(root string) ([]State, error) {
	root, err := absolute(root)
	if err != nil {
		return nil, err
	}
	index, err := meta.LoadIndex(root)
	if err != nil {
		return nil, err
	}
	out := make([]State, 0, len(index.Commits))
	for i := len(index.Commits) - 1; i >= 0; i-- {
		c := index.Commits[i]
		out = append(out, State{ID: c.ID, CreatedAt: time.Unix(c.TimeUTC, 0).UTC(), Message: c.Message})
	}
	return out, nil
}

type RestoreOptions struct {
	// To overrides the destination directory (default: the repo root).
	To string
	// Clean removes files in the destination that are not part of the state.
	Clean bool
	// Reset moves HEAD to the restored state.
	Reset bool
	// FastSkip trusts size, mode and mtime to skip existing files without
	// reading them. The default verifies content against the recorded chunk
	// hashes, so a file whose bytes changed under an unchanged stat never
	// survives a restore.
	FastSkip bool
	// Verbose receives per-file progress when non-nil.
	Verbose io.Writer
}

type RestoreResult struct {
	StateID string
	Dest    string
}

// Restore materializes a state into a directory.
func Restore(root, state string, opts RestoreOptions) (RestoreResult, error) {
	return RestoreContext(context.Background(), root, state, opts)
}

// RestoreContext is Restore with cancellation: the file loop checks ctx
// between files and between chunks, so a daemon can abort a long restore.
func RestoreContext(ctx context.Context, root, state string, opts RestoreOptions) (RestoreResult, error) {
	root, err := absolute(root)
	if err != nil {
		return RestoreResult{}, err
	}
	id, err := meta.ResolveCommitID(root, state)
	if err != nil {
		return RestoreResult{}, err
	}
	dest := root
	if opts.To != "" {
		dest, err = absolute(opts.To)
		if err != nil {
			return RestoreResult{}, err
		}
	}
	// Writing into the repo root or moving HEAD races with concurrent
	// mutating operations (commit, gc): take the same advisory lock they do.
	if dest == root || opts.Reset {
		lock, err := meta.LockRepo(root, lockTimeout)
		if err != nil {
			return RestoreResult{}, err
		}
		defer lock.Unlock()
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return RestoreResult{}, err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return RestoreResult{}, err
	}
	commitFiles, err := meta.CommitFiles(store, commit)
	if err != nil {
		return RestoreResult{}, err
	}

	if err := restoreCommit(ctx, dest, store, commitFiles, commit.BlockSize, opts); err != nil {
		return RestoreResult{}, err
	}
	if opts.Clean {
		if err := cleanDest(dest, commitFiles, opts.Verbose); err != nil {
			return RestoreResult{}, err
		}
	}
	if opts.Reset {
		if err := meta.AdvanceHeadAfterCommit(root, id); err != nil {
			return RestoreResult{}, err
		}
	}
	return RestoreResult{StateID: id, Dest: dest}, nil
}

// safeOutPath resolves a validated state path under dest, creating its
// parent directories while refusing to walk through symlinks: a symlinked
// ancestor (from a hostile state, or planted in the destination) would
// redirect the write outside dest. An ancestor that exists as anything but a
// real directory is replaced with one.
func safeOutPath(dest, rel string) (string, error) {
	parts := strings.Split(rel, "/")
	dir := dest
	for _, part := range parts[:len(parts)-1] {
		dir = filepath.Join(dir, part)
		info, err := os.Lstat(dir)
		switch {
		case err == nil && info.Mode().IsDir():
			continue
		case err == nil:
			if err := os.Remove(dir); err != nil {
				return "", err
			}
		case !os.IsNotExist(err):
			return "", err
		}
		if err := os.Mkdir(dir, 0o755); err != nil && !os.IsExist(err) {
			return "", err
		}
	}
	out := filepath.Join(dir, parts[len(parts)-1])
	// Belt and braces: validated components cannot escape, but prove it.
	if out != dest && !strings.HasPrefix(out, filepath.Clean(dest)+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes the destination", rel)
	}
	return out, nil
}

func restoreCommit(ctx context.Context, dest string, store core.BlockStore, files []meta.FileEntry, blockSize int, opts RestoreOptions) error {
	verbose := opts.Verbose
	for _, fe := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		// State metadata is untrusted (it may come from a remote): refuse
		// anything that could resolve outside the destination.
		if err := meta.ValidateRelPath(fe.Path); err != nil {
			return fmt.Errorf("state file: %w", err)
		}
		if fe.Path == ".fvs2" || strings.HasPrefix(fe.Path, ".fvs2/") {
			continue
		}
		outPath, err := safeOutPath(dest, fe.Path)
		if err != nil {
			return err
		}

		if verbose != nil {
			fmt.Fprintf(verbose, "restoring: %s\n", fe.Path)
		}

		if fe.Link != "" {
			if cur, err := os.Readlink(outPath); err == nil && cur == fe.Link {
				continue
			}
			_ = os.Remove(outPath)
			if err := os.Symlink(fe.Link, outPath); err != nil {
				return err
			}
			continue
		}

		if info, err := os.Lstat(outPath); err == nil {
			if info.Mode().IsRegular() && info.Size() == fe.Size && uint32(info.Mode().Perm()) == fe.Mode && sameMtime(fe, info.ModTime()) {
				// Metadata alone is not proof of content: verify against the
				// recorded chunk hashes unless the caller opted into the fast
				// metadata-only mode.
				if opts.FastSkip || fileMatchesBlocks(outPath, fe, blockSize) {
					continue
				}
			}
			if !info.Mode().IsRegular() {
				_ = os.Remove(outPath)
			}
		}

		if err := writeFileFromBlocks(ctx, outPath, store, fe); err != nil {
			return err
		}
		_ = os.Chtimes(outPath, time.Now(), fileMtime(fe))
	}
	return nil
}

// writeFileFromBlocks materializes one file atomically: the chunks stream
// into a temp file in the target directory, which is fsynced and renamed
// over the final name. An interrupted restore never leaves a partial file
// under its real name.
func writeFileFromBlocks(ctx context.Context, outPath string, store core.BlockStore, fe meta.FileEntry) error {
	tmp, err := os.CreateTemp(filepath.Dir(outPath), ".fvs2-restore-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(tmp, 65536)
	var written int64
	for _, bid := range fe.Blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := store.Get(bid)
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		written += int64(len(b))
		if written >= fe.Size {
			break
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := tmp.Chmod(os.FileMode(fe.Mode)); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Rename replaces whatever sits at outPath (stale file or symlink) in
	// one atomic step.
	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}
	ok = true
	return nil
}

// fileMatchesBlocks reports whether the file's bytes hash to the entry's
// recorded chunk list. It re-reads the file but never re-chunks it: the
// recorded per-chunk sizes (or the fixed block size for format-1 states)
// give the exact segment boundaries.
func fileMatchesBlocks(path string, fe meta.FileEntry, blockSize int) bool {
	sizes := fe.BlockSizes
	if len(sizes) != len(fe.Blocks) {
		sizes = make([]int64, len(fe.Blocks))
		if len(fe.Blocks) == 1 {
			sizes[0] = fe.Size
		} else {
			if blockSize <= 0 {
				return false
			}
			remaining := fe.Size
			for i := range sizes {
				n := int64(blockSize)
				if remaining < n {
					n = remaining
				}
				sizes[i] = n
				remaining -= n
			}
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 65536)
	var total int64
	for i, bid := range fe.Blocks {
		buf := make([]byte, sizes[i])
		if _, err := io.ReadFull(r, buf); err != nil {
			return false
		}
		if core.ContentID(buf) != bid {
			return false
		}
		total += sizes[i]
	}
	if total != fe.Size {
		return false
	}
	// The file must end exactly where the chunk list does.
	if _, err := r.ReadByte(); err != io.EOF {
		return false
	}
	return true
}

// fileMtime returns the entry's mtime at the finest recorded granularity.
func fileMtime(fe meta.FileEntry) time.Time {
	if fe.ModTimeNS != 0 {
		return time.Unix(0, fe.ModTimeNS)
	}
	return time.Unix(fe.ModTime, 0)
}

// cleanDest removes any file or symlink under dest that is not part of the
// restored commit, then prunes the directories left empty. The .fvs2 metadata
// directory is always preserved.
func cleanDest(dest string, files []meta.FileEntry, verbose io.Writer) error {
	want := make(map[string]bool, len(files))
	for _, fe := range files {
		want[filepath.Clean(filepath.Join(dest, filepath.FromSlash(fe.Path)))] = true
	}
	metaPath := filepath.Join(dest, ".fvs2")

	var toRemove []string
	err := filepath.WalkDir(dest, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		clean := filepath.Clean(p)
		if clean == metaPath || strings.HasPrefix(clean, metaPath+string(os.PathSeparator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if clean == filepath.Clean(dest) || d.IsDir() {
			return nil
		}
		if !want[clean] {
			toRemove = append(toRemove, clean)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, p := range toRemove {
		if verbose != nil {
			fmt.Fprintf(verbose, "removing: %s\n", p)
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	var dirs []string
	_ = filepath.WalkDir(dest, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		clean := filepath.Clean(p)
		if clean == metaPath {
			return filepath.SkipDir
		}
		if d.IsDir() && clean != filepath.Clean(dest) {
			dirs = append(dirs, clean)
		}
		return nil
	})
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err == nil && len(entries) == 0 {
			_ = os.Remove(dir)
		}
	}
	return nil
}
