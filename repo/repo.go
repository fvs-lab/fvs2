package repo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

type Repository struct {
	Path       string
	BlockSize  int
	BlocksPath string
}

type InitOptions struct {
	BlockSize  int
	Format     int
	BlocksPath string
}

type CommitResult struct {
	StateID   string
	Created   bool
	CreatedAt time.Time
	FileCount int
}

type commitBlockStore struct {
	*core.DiskBlockStore
}

func (s commitBlockStore) Put(data []byte) (core.BlockID, error) {
	return s.PutDeferred(data)
}

// FileEntry aliases the state file entry for external consumers.
type FileEntry = meta.FileEntry

// StateFiles returns the flattened file list of a state, whatever the repo
// format.
func StateFiles(root, id string) ([]FileEntry, error) {
	root, err := absolute(root)
	if err != nil {
		return nil, err
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return nil, err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return nil, err
	}
	return meta.CommitFiles(store, commit)
}

// InitFormat initializes a repository pinned to an explicit on-disk format.
func InitFormat(root string, blockSize, format int) (Repository, error) {
	return InitWithOptions(root, InitOptions{BlockSize: blockSize, Format: format})
}

func Init(root string, blockSize int) (Repository, error) {
	return InitWithOptions(root, InitOptions{BlockSize: blockSize, Format: meta.CurrentFormat})
}

// InitWithOptions initializes repository metadata and optionally points it at
// a block store shared with other repositories.
func InitWithOptions(root string, opts InitOptions) (Repository, error) {
	blockSize, format := opts.BlockSize, opts.Format
	if format == 0 {
		format = meta.CurrentFormat
	}
	root, err := absolute(root)
	if err != nil {
		return Repository{}, err
	}
	if cfg, err := meta.LoadConfig(root); err == nil {
		if blockSize > 0 && blockSize != cfg.BlockSize {
			return Repository{}, fmt.Errorf("repository already uses block size %d", cfg.BlockSize)
		}
		blocksPath, err := meta.BlockStorePath(root)
		if err != nil {
			return Repository{}, err
		}
		if opts.BlocksPath != "" {
			requested, err := filepath.Abs(opts.BlocksPath)
			if err != nil {
				return Repository{}, err
			}
			if filepath.Clean(requested) != blocksPath {
				return Repository{}, fmt.Errorf("repository already uses block store %s", blocksPath)
			}
		}
		return Repository{Path: root, BlockSize: cfg.BlockSize, BlocksPath: blocksPath}, nil
	} else if !errors.Is(err, meta.ErrNotInitialized) {
		return Repository{}, err
	}
	if err := meta.InitWithStorage(root, blockSize, format, opts.BlocksPath); err != nil {
		return Repository{}, err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return Repository{}, err
	}
	blocksPath, err := meta.BlockStorePath(root)
	if err != nil {
		return Repository{}, err
	}
	return Repository{Path: root, BlockSize: cfg.BlockSize, BlocksPath: blocksPath}, nil
}

// BlockStorePath returns the resolved block-store directory for root.
func BlockStorePath(root string) (string, error) {
	root, err := absolute(root)
	if err != nil {
		return "", err
	}
	return meta.BlockStorePath(root)
}

func Commit(root, message string, allowEmpty bool, verbose io.Writer) (CommitResult, error) {
	return CommitContext(context.Background(), root, message, allowEmpty, verbose)
}

// CommitContext is Commit with cancellation: the snapshot walk checks ctx
// between files and between chunks, so a daemon can abort a long snapshot.
func CommitContext(ctx context.Context, root, message string, allowEmpty bool, verbose io.Writer) (CommitResult, error) {
	root, err := absolute(root)
	if err != nil {
		return CommitResult{}, err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return CommitResult{}, err
	}
	lock, err := meta.LockRepo(root, lockTimeout)
	if err != nil {
		return CommitResult{}, err
	}
	defer lock.Unlock()
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return CommitResult{}, err
	}
	commitStore := commitBlockStore{store}

	var head *meta.Commit
	headID, err := meta.ResolveHeadCommit(root)
	if err != nil {
		return CommitResult{}, err
	}
	var headFiles map[string]meta.FileEntry
	if headID != "" {
		commit, err := meta.LoadCommit(root, headID)
		if err != nil {
			return CommitResult{}, err
		}
		head = &commit
		headList, err := meta.CommitFiles(store, commit)
		if err != nil {
			return CommitResult{}, err
		}
		headFiles = make(map[string]meta.FileEntry, len(headList))
		for _, file := range headList {
			headFiles[file.Path] = file
		}
	}

	files, err := snapshot(ctx, root, commitStore, cfg.ChunkParams(), cfg.ChunkingPolicy, cfg.Format, headFiles, verbose)
	if err != nil {
		return CommitResult{}, err
	}
	if !allowEmpty && head != nil && sameFiles(headFiles, files) {
		return result(*head, false), nil
	}

	now := time.Now().UTC()
	id := meta.NewCommitID(now, message, files)
	commit := meta.Commit{ID: id, Format: cfg.Format, TimeUTC: now.Unix(), Message: message, BlockSize: cfg.BlockSize, ChunkingPolicy: cfg.ChunkingPolicy}
	if cfg.Format >= 3 {
		rootTree, err := meta.WriteTree(commitStore, files)
		if err != nil {
			return CommitResult{}, err
		}
		commit.RootTree = rootTree
		commit.FileCount = len(files)
		for _, f := range files {
			commit.TotalSize += f.Size
		}
	} else {
		commit.Files = files
	}
	if err := store.Sync(); err != nil {
		return CommitResult{}, err
	}
	if err := writeJSONAtomic(meta.CommitPath(root, id), commit); err != nil {
		return CommitResult{}, err
	}
	index, err := meta.LoadIndex(root)
	if err != nil {
		return CommitResult{}, err
	}
	index.Commits = append(index.Commits, meta.CommitSummary{ID: id, TimeUTC: commit.TimeUTC, Message: message})
	if err := meta.SaveIndex(root, index); err != nil {
		return CommitResult{}, err
	}
	if err := meta.AdvanceHeadAfterCommit(root, id); err != nil {
		return CommitResult{}, err
	}
	return result(commit, true), nil
}

func result(commit meta.Commit, created bool) CommitResult {
	count := commit.FileCount
	if commit.RootTree == "" {
		count = len(commit.Files)
	}
	return CommitResult{
		StateID:   commit.ID,
		Created:   created,
		CreatedAt: time.Unix(commit.TimeUTC, 0).UTC(),
		FileCount: count,
	}
}

func absolute(path string) (string, error) {
	if path == "" {
		path = "."
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(path), nil
}

var errFileVanished = errors.New("file vanished")

// lockTimeout is how long mutating operations wait for the repo lock.
var lockTimeout = 5 * time.Second

// sameMtime compares a working-tree mtime against the recorded one at the
// finest granularity the state carries: nanoseconds when present, seconds
// for states written before nanosecond mtimes existed. This is what lets the
// commit shortcut catch a same-size rewrite within one second.
func sameMtime(old meta.FileEntry, t time.Time) bool {
	if old.ModTimeNS != 0 {
		return old.ModTimeNS == t.UnixNano()
	}
	return old.ModTime == t.Unix()
}

func snapshot(ctx context.Context, root string, store core.BlockStore, params core.ChunkParams, policy, format int, head map[string]meta.FileEntry, verbose io.Writer) ([]meta.FileEntry, error) {
	ignore, err := loadIgnore(root)
	if err != nil {
		return nil, err
	}
	var files []meta.FileEntry
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".fvs2" || strings.HasPrefix(rel, ".fvs2"+string(os.PathSeparator)) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel = filepath.ToSlash(rel)

		if ignore.Match(rel, entry.IsDir()) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			info, err := os.Lstat(path)
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			kind := ""
			if format >= 4 {
				kind = "symlink"
			}
			files = append(files, meta.FileEntry{Path: rel, Kind: kind, Mode: posixMode(info.Mode()), ModTime: info.ModTime().Unix(), ModTimeNS: info.ModTime().UnixNano(), Link: target})
			return nil
		}
		if entry.IsDir() {
			if format >= 4 {
				info, err := entry.Info()
				if os.IsNotExist(err) {
					return nil
				}
				if err != nil {
					return err
				}
				files = append(files, meta.FileEntry{Path: rel, Kind: "dir", Mode: posixMode(info.Mode()), ModTime: info.ModTime().Unix(), ModTimeNS: info.ModTime().UnixNano()})
			}
			return nil
		}
		info, err := entry.Info()
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			if format >= 4 && info.Mode()&os.ModeNamedPipe != 0 {
				files = append(files, meta.FileEntry{Path: rel, Kind: "fifo", Mode: posixMode(info.Mode()), ModTime: info.ModTime().Unix(), ModTimeNS: info.ModTime().UnixNano()})
			}
			return nil
		}
		if old, ok := head[rel]; ok && old.Link == "" && old.Size == info.Size() && sameMtime(old, info.ModTime()) && old.Mode == posixMode(info.Mode()) {
			files = append(files, old)
			return nil
		}
		if verbose != nil {
			fmt.Fprintf(verbose, "hashing: %s\n", rel)
		}
		blocks, sizes, size, err := putFileBlocks(ctx, path, store, params, policy)
		if errors.Is(err, errFileVanished) {
			return nil
		}
		if err != nil {
			return err
		}
		kind := ""
		if format >= 4 {
			kind = "file"
		}
		files = append(files, meta.FileEntry{Path: rel, Kind: kind, Mode: posixMode(info.Mode()), Size: size, ModTime: info.ModTime().Unix(), ModTimeNS: info.ModTime().UnixNano(), Blocks: blocks, BlockSizes: sizes})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func putFileBlocks(ctx context.Context, path string, store core.BlockStore, params core.ChunkParams, policy int) ([]core.BlockID, []int64, int64, error) {
	if policy >= 1 {
		head := make([]byte, 8192)
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			return nil, nil, 0, errFileVanished
		}
		if err != nil {
			return nil, nil, 0, err
		}
		n, _ := io.ReadFull(f, head)
		_ = f.Close()
		params = core.ParamsForContent(policy, params, head[:n])
	}
	return ChunkFile(path, params, func(chunk []byte) (core.BlockID, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return store.Put(chunk)
	})
}

// ChunkFile splits the file at path according to params and hands each chunk
// to emit, which returns the chunk's block id. It returns the ids, the
// per-chunk sizes, and the total byte count.
func ChunkFile(path string, params core.ChunkParams, emit func([]byte) (core.BlockID, error)) ([]core.BlockID, []int64, int64, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil, 0, errFileVanished
	}
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()

	chunker, err := core.NewChunker(bufio.NewReaderSize(file, params.Max), params)
	if err != nil {
		return nil, nil, 0, err
	}
	var blocks []core.BlockID
	var sizes []int64
	var total int64
	for {
		chunk, err := chunker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, 0, err
		}
		id, err := emit(chunk)
		if err != nil {
			return nil, nil, 0, err
		}
		blocks = append(blocks, id)
		sizes = append(sizes, int64(len(chunk)))
		total += int64(len(chunk))
	}
	return blocks, sizes, total, nil
}

// ErrFileVanished reports whether err means the file disappeared between the
// directory walk and the open.
func ErrFileVanished(err error) bool { return errors.Is(err, errFileVanished) }

func sameFiles(head map[string]meta.FileEntry, files []meta.FileEntry) bool {
	if len(head) != len(files) {
		return false
	}
	for _, file := range files {
		old, ok := head[file.Path]
		if !ok || old.Kind != file.Kind || old.Mode != file.Mode || old.Size != file.Size || old.Link != file.Link || old.ContentDigest != file.ContentDigest || len(old.Blocks) != len(file.Blocks) {
			return false
		}
		for i := range file.Blocks {
			if old.Blocks[i] != file.Blocks[i] {
				return false
			}
		}
	}
	return true
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	ok := false
	defer func() {
		_ = temp.Close()
		if !ok {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := temp.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	ok = true
	// fsync the directory so the rename itself is durable.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
