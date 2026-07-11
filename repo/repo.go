package repo

import (
	"bufio"
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

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

type Repository struct {
	Path      string
	BlockSize int
}

type CommitResult struct {
	StateID   string
	Created   bool
	CreatedAt time.Time
	FileCount int
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
	return initRepo(root, blockSize, format)
}

func Init(root string, blockSize int) (Repository, error) {
	return initRepo(root, blockSize, meta.CurrentFormat)
}

func initRepo(root string, blockSize, format int) (Repository, error) {
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
		return Repository{Path: root, BlockSize: cfg.BlockSize}, nil
	} else if !errors.Is(err, meta.ErrNotInitialized) {
		return Repository{}, err
	}
	if err := meta.InitWithFormat(root, blockSize, format); err != nil {
		return Repository{}, err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return Repository{}, err
	}
	return Repository{Path: root, BlockSize: cfg.BlockSize}, nil
}

func Commit(root, message string, allowEmpty bool, verbose io.Writer) (CommitResult, error) {
	root, err := absolute(root)
	if err != nil {
		return CommitResult{}, err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return CommitResult{}, err
	}
	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return CommitResult{}, err
	}
	defer lock.Unlock()
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return CommitResult{}, err
	}

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

	files, err := snapshot(root, store, cfg.ChunkParams(), cfg.ChunkingPolicy, headFiles, verbose)
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
		rootTree, err := meta.WriteTree(store, files)
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

func snapshot(root string, store core.BlockStore, params core.ChunkParams, policy int, head map[string]meta.FileEntry, verbose io.Writer) ([]meta.FileEntry, error) {
	var files []meta.FileEntry
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
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
			files = append(files, meta.FileEntry{Path: rel, Mode: uint32(info.Mode().Perm()), ModTime: info.ModTime().Unix(), Link: target})
			return nil
		}
		if entry.IsDir() {
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
			return nil
		}
		if old, ok := head[rel]; ok && old.Link == "" && old.Size == info.Size() && old.ModTime == info.ModTime().Unix() && old.Mode == uint32(info.Mode().Perm()) {
			files = append(files, old)
			return nil
		}
		if verbose != nil {
			fmt.Fprintf(verbose, "hashing: %s\n", rel)
		}
		blocks, sizes, size, err := putFileBlocks(path, store, params, policy)
		if errors.Is(err, errFileVanished) {
			return nil
		}
		if err != nil {
			return err
		}
		files = append(files, meta.FileEntry{Path: rel, Mode: uint32(info.Mode().Perm()), Size: size, ModTime: info.ModTime().Unix(), Blocks: blocks, BlockSizes: sizes})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func putFileBlocks(path string, store core.BlockStore, params core.ChunkParams, policy int) ([]core.BlockID, []int64, int64, error) {
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
	return ChunkFile(path, params, store.Put)
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
		if !ok || old.Mode != file.Mode || old.Size != file.Size || old.Link != file.Link || len(old.Blocks) != len(file.Blocks) {
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
