package repo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

type EntryKind string

const (
	EntryFile     EntryKind = "file"
	EntryDir      EntryKind = "dir"
	EntrySymlink  EntryKind = "symlink"
	EntryHardlink EntryKind = "hardlink"
	EntryFIFO     EntryKind = "fifo"
)

type Entry struct {
	Path          string
	Kind          EntryKind
	Mode          uint32
	Size          int64
	ModTime       time.Time
	Link          string
	ContentDigest string
}

type SnapshotOptions struct {
	Message       string
	ComputeSHA256 bool
}

// SnapshotWriter builds one immutable state directly from streams. Content
// blocks are durable before the commit document becomes visible. An aborted
// writer may leave unreferenced blocks for gc, but never a partial state.
type SnapshotWriter struct {
	ctx     context.Context
	root    string
	opts    SnapshotOptions
	cfg     meta.Config
	store   *core.DiskBlockStore
	lock    *meta.RepoLock
	entries map[string]meta.FileEntry
	pending map[string]Entry
	closed  bool
}

func BeginSnapshot(root string, opts SnapshotOptions) (*SnapshotWriter, error) {
	return BeginSnapshotContext(context.Background(), root, opts)
}

func BeginSnapshotContext(ctx context.Context, root string, opts SnapshotOptions) (*SnapshotWriter, error) {
	root, err := absolute(root)
	if err != nil {
		return nil, err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return nil, err
	}
	if cfg.Format < 4 {
		return nil, fmt.Errorf("streaming snapshots require repository format 4")
	}
	lock, err := meta.LockRepo(root, lockTimeout)
	if err != nil {
		return nil, err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		_ = lock.Unlock()
		return nil, err
	}
	return &SnapshotWriter{
		ctx: ctx, root: root, opts: opts, cfg: cfg, store: store, lock: lock,
		entries: map[string]meta.FileEntry{}, pending: map[string]Entry{},
	}, nil
}

func (w *SnapshotWriter) Add(entry Entry, content io.Reader) error {
	if w == nil || w.closed {
		return errors.New("snapshot writer is closed")
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	entry.Path = strings.Trim(path.Clean(filepath.ToSlash(entry.Path)), "/")
	if err := meta.ValidateRelPath(entry.Path); err != nil {
		return fmt.Errorf("snapshot entry: %w", err)
	}
	if entry.Path == ".fvs2" || strings.HasPrefix(entry.Path, ".fvs2/") {
		return fmt.Errorf("snapshot entry uses reserved path %q", entry.Path)
	}
	if err := w.validateParents(entry.Path); err != nil {
		return err
	}
	if entry.ModTime.IsZero() {
		entry.ModTime = time.Unix(0, 0).UTC()
	}
	base := meta.FileEntry{
		Path: entry.Path, Kind: string(entry.Kind), Mode: entry.Mode,
		ModTime: entry.ModTime.Unix(), ModTimeNS: entry.ModTime.UnixNano(),
	}

	switch entry.Kind {
	case EntryFile:
		if err := w.validateLeaf(entry.Path); err != nil {
			return err
		}
		if content == nil {
			return fmt.Errorf("snapshot file %q has no content", entry.Path)
		}
		if entry.Size < 0 {
			return fmt.Errorf("snapshot file %q has invalid size", entry.Path)
		}
		blocks, sizes, size, digest, err := w.putFile(content, entry.Size, entry.ContentDigest)
		if err != nil {
			return fmt.Errorf("snapshot file %q: %w", entry.Path, err)
		}
		base.Size = size
		base.Blocks = blocks
		base.BlockSizes = sizes
		base.ContentDigest = digest
	case EntryDir:
		if err := w.validateDirectory(entry.Path); err != nil {
			return err
		}
	case EntrySymlink:
		if entry.Link == "" {
			return fmt.Errorf("snapshot symlink %q has an empty target", entry.Path)
		}
		if err := w.validateLeaf(entry.Path); err != nil {
			return err
		}
		base.Link = entry.Link
	case EntryHardlink:
		entry.Link = strings.Trim(path.Clean(filepath.ToSlash(entry.Link)), "/")
		if err := meta.ValidateRelPath(entry.Link); err != nil {
			return fmt.Errorf("snapshot hardlink %q: %w", entry.Path, err)
		}
		if err := w.validateLeaf(entry.Path); err != nil {
			return err
		}
		delete(w.entries, entry.Path)
		w.pending[entry.Path] = entry
		return nil
	case EntryFIFO:
		if err := w.validateLeaf(entry.Path); err != nil {
			return err
		}
	default:
		return fmt.Errorf("snapshot entry %q has unsupported kind %q", entry.Path, entry.Kind)
	}

	delete(w.pending, entry.Path)
	w.entries[entry.Path] = base
	return nil
}

// AddReference adds a regular file that is already present in the repository's
// shared block store. The referenced blocks are checked before the entry is
// accepted, so a missing block cannot produce a commit that fails at read time.
func (w *SnapshotWriter) AddReference(entry FileEntry) error {
	if w == nil || w.closed {
		return errors.New("snapshot writer is closed")
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	entry.Path = strings.Trim(path.Clean(filepath.ToSlash(entry.Path)), "/")
	if err := meta.ValidateRelPath(entry.Path); err != nil {
		return fmt.Errorf("snapshot entry: %w", err)
	}
	if entry.Path == ".fvs2" || strings.HasPrefix(entry.Path, ".fvs2/") {
		return fmt.Errorf("snapshot entry uses reserved path %q", entry.Path)
	}
	if entry.Kind != "" && entry.Kind != string(EntryFile) {
		return fmt.Errorf("snapshot reference %q has unsupported kind %q", entry.Path, entry.Kind)
	}
	if err := w.validateParents(entry.Path); err != nil {
		return err
	}
	if err := w.validateLeaf(entry.Path); err != nil {
		return err
	}
	if len(entry.Blocks) != len(entry.BlockSizes) {
		return fmt.Errorf("snapshot reference %q has inconsistent block metadata", entry.Path)
	}
	var size int64
	for i, block := range entry.Blocks {
		present, err := w.store.Has(block)
		if err != nil {
			return fmt.Errorf("snapshot reference %q block %s: %w", entry.Path, block, err)
		}
		if !present {
			return fmt.Errorf("snapshot reference %q block %s is missing", entry.Path, block)
		}
		if entry.BlockSizes[i] < 0 {
			return fmt.Errorf("snapshot reference %q has invalid block size", entry.Path)
		}
		size += entry.BlockSizes[i]
	}
	if size != entry.Size {
		return fmt.Errorf("snapshot reference %q size mismatch: expected %d, blocks contain %d", entry.Path, entry.Size, size)
	}
	if entry.Kind == "" {
		entry.Kind = string(EntryFile)
	}
	delete(w.pending, entry.Path)
	w.entries[entry.Path] = entry
	return nil
}

// AddTree imports a materialized directory into the snapshot. Repository
// metadata inside the source is skipped.
func (w *SnapshotWriter) AddTree(source string) error {
	return w.AddTreeProgress(source, nil)
}

// AddTreeProgress imports a materialized directory and reports the number of
// regular file bytes accepted by the snapshot.
func (w *SnapshotWriter) AddTreeProgress(source string, progress func(int64)) error {
	root, err := absolute(source)
	if err != nil {
		return err
	}
	var completed int64
	return filepath.WalkDir(root, func(name string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := w.ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".fvs2" || strings.HasPrefix(rel, ".fvs2"+string(os.PathSeparator)) {
			if dirEntry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := os.Lstat(name)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		entry := Entry{
			Path: filepath.ToSlash(rel), Mode: posixMode(info.Mode()),
			ModTime: info.ModTime(),
		}
		switch {
		case info.IsDir():
			entry.Kind = EntryDir
			return w.Add(entry, nil)
		case info.Mode()&os.ModeSymlink != 0:
			entry.Kind = EntrySymlink
			entry.Link, err = os.Readlink(name)
			if err != nil {
				return err
			}
			return w.Add(entry, nil)
		case info.Mode()&os.ModeNamedPipe != 0:
			entry.Kind = EntryFIFO
			return w.Add(entry, nil)
		case info.Mode().IsRegular():
			entry.Kind = EntryFile
			entry.Size = info.Size()
			if entry.Size == 0 {
				return w.Add(entry, strings.NewReader(""))
			}
			file, err := os.Open(name)
			if err != nil {
				return err
			}
			addErr := w.Add(entry, file)
			closeErr := file.Close()
			if addErr != nil {
				return addErr
			}
			if closeErr != nil {
				return closeErr
			}
			completed += entry.Size
			if progress != nil {
				progress(completed)
			}
			return nil
		default:
			return fmt.Errorf("snapshot tree contains unsupported entry %q", rel)
		}
	})
}

func posixMode(mode fs.FileMode) uint32 {
	result := uint32(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		result |= 0o4000
	}
	if mode&os.ModeSetgid != 0 {
		result |= 0o2000
	}
	if mode&os.ModeSticky != 0 {
		result |= 0o1000
	}
	return result
}

func (w *SnapshotWriter) validateParents(name string) error {
	for parent := path.Dir(name); parent != "." && parent != ""; parent = path.Dir(parent) {
		if entry, ok := w.entries[parent]; ok && entry.Kind != string(EntryDir) {
			return fmt.Errorf("snapshot entry %q has non-directory parent %q", name, parent)
		}
		if _, ok := w.pending[parent]; ok {
			return fmt.Errorf("snapshot entry %q has unresolved parent %q", name, parent)
		}
	}
	return nil
}

func (w *SnapshotWriter) validateDirectory(name string) error {
	if existing, ok := w.entries[name]; ok && existing.Kind != string(EntryDir) {
		return fmt.Errorf("snapshot directory %q replaces another entry", name)
	}
	if _, ok := w.pending[name]; ok {
		return fmt.Errorf("snapshot directory %q replaces a hardlink", name)
	}
	return nil
}

func (w *SnapshotWriter) validateLeaf(name string) error {
	prefix := name + "/"
	for existing := range w.entries {
		if strings.HasPrefix(existing, prefix) {
			return fmt.Errorf("snapshot entry %q replaces a populated directory", name)
		}
	}
	for existing := range w.pending {
		if strings.HasPrefix(existing, prefix) {
			return fmt.Errorf("snapshot entry %q replaces a populated directory", name)
		}
	}
	return nil
}

func (w *SnapshotWriter) putFile(content io.Reader, expectedSize int64, expectedDigest string) ([]core.BlockID, []int64, int64, string, error) {
	if expectedDigest != "" && !strings.HasPrefix(expectedDigest, "sha256:") {
		return nil, nil, 0, "", fmt.Errorf("unsupported content digest %q", expectedDigest)
	}
	hashContent := w.opts.ComputeSHA256 || expectedDigest != ""
	var digestHash io.Writer = io.Discard
	sha := sha256.New()
	if hashContent {
		digestHash = sha
	}
	limited := &io.LimitedReader{R: content, N: expectedSize + 1}
	reader := io.TeeReader(limited, digestHash)
	blocks, sizes, size, err := chunkReader(w.ctx, reader, w.store, w.cfg.ChunkParams(), w.cfg.ChunkingPolicy)
	if err != nil {
		return nil, nil, 0, "", err
	}
	if size != expectedSize {
		return nil, nil, 0, "", fmt.Errorf("size mismatch: expected %d, read %d", expectedSize, size)
	}
	digest := expectedDigest
	if hashContent {
		actual := "sha256:" + hex.EncodeToString(sha.Sum(nil))
		if expectedDigest != "" && !strings.EqualFold(actual, expectedDigest) {
			return nil, nil, 0, "", fmt.Errorf("content digest mismatch")
		}
		digest = actual
	}
	return blocks, sizes, size, digest, nil
}

func chunkReader(ctx context.Context, reader io.Reader, store *core.DiskBlockStore, params core.ChunkParams, policy int) ([]core.BlockID, []int64, int64, error) {
	buffered := bufio.NewReaderSize(reader, max(params.Max, 8192))
	if policy >= 1 {
		head, _ := buffered.Peek(8192)
		params = core.ParamsForContent(policy, params, head)
	}
	chunker, err := core.NewChunker(buffered, params)
	if err != nil {
		return nil, nil, 0, err
	}
	var blocks []core.BlockID
	var sizes []int64
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		chunk, err := chunker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, 0, err
		}
		id, err := store.PutDeferred(chunk)
		if err != nil {
			return nil, nil, 0, err
		}
		blocks = append(blocks, id)
		sizes = append(sizes, int64(len(chunk)))
		total += int64(len(chunk))
	}
	return blocks, sizes, total, nil
}

func (w *SnapshotWriter) resolveHardlinks() error {
	for len(w.pending) > 0 {
		resolved := 0
		for name, link := range w.pending {
			target, ok := w.entries[link.Link]
			if !ok || target.Kind != string(EntryFile) {
				continue
			}
			target.Path = name
			target.Mode = link.Mode
			target.ModTime = link.ModTime.Unix()
			target.ModTimeNS = link.ModTime.UnixNano()
			w.entries[name] = target
			delete(w.pending, name)
			resolved++
		}
		if resolved == 0 {
			for name, link := range w.pending {
				return fmt.Errorf("snapshot hardlink %q targets missing regular file %q", name, link.Link)
			}
		}
	}
	return nil
}

func (w *SnapshotWriter) Commit() (CommitResult, error) {
	if w == nil || w.closed {
		return CommitResult{}, errors.New("snapshot writer is closed")
	}
	defer w.release()
	if err := w.ctx.Err(); err != nil {
		return CommitResult{}, err
	}
	if err := w.resolveHardlinks(); err != nil {
		return CommitResult{}, err
	}
	files := make([]meta.FileEntry, 0, len(w.entries))
	for _, entry := range w.entries {
		files = append(files, entry)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })

	commitStore := commitBlockStore{w.store}
	now := time.Now().UTC()
	id := meta.NewCommitID(now, w.opts.Message, files)
	rootTree, err := meta.WriteTree(commitStore, files)
	if err != nil {
		return CommitResult{}, err
	}
	commit := meta.Commit{
		ID: id, Format: w.cfg.Format, TimeUTC: now.Unix(), Message: w.opts.Message,
		BlockSize: w.cfg.BlockSize, ChunkingPolicy: w.cfg.ChunkingPolicy,
		RootTree: rootTree, FileCount: len(files),
	}
	for _, entry := range files {
		if entry.Kind == string(EntryFile) {
			commit.TotalSize += entry.Size
		}
	}
	if err := w.store.Sync(); err != nil {
		return CommitResult{}, err
	}
	if err := writeJSONAtomic(meta.CommitPath(w.root, id), commit); err != nil {
		return CommitResult{}, err
	}
	index, err := meta.LoadIndex(w.root)
	if err != nil {
		return CommitResult{}, err
	}
	index.Commits = append(index.Commits, meta.CommitSummary{ID: id, TimeUTC: commit.TimeUTC, Message: commit.Message})
	if err := meta.SaveIndex(w.root, index); err != nil {
		return CommitResult{}, err
	}
	if err := meta.AdvanceHeadAfterCommit(w.root, id); err != nil {
		return CommitResult{}, err
	}
	return result(commit, true), nil
}

func (w *SnapshotWriter) Abort() error {
	if w == nil || w.closed {
		return nil
	}
	return w.release()
}

func (w *SnapshotWriter) release() error {
	w.closed = true
	if w.lock == nil {
		return nil
	}
	err := w.lock.Unlock()
	w.lock = nil
	return err
}
