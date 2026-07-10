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

func Init(root string, blockSize int) (Repository, error) {
	root, err := absolute(root)
	if err != nil {
		return Repository{}, err
	}
	if err := meta.Init(root, blockSize); err != nil {
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
		headFiles = make(map[string]meta.FileEntry, len(commit.Files))
		for _, file := range commit.Files {
			headFiles[file.Path] = file
		}
	}

	files, err := snapshot(root, store, cfg.BlockSize, headFiles, verbose)
	if err != nil {
		return CommitResult{}, err
	}
	if !allowEmpty && head != nil && sameFiles(headFiles, files) {
		return result(*head, false), nil
	}

	now := time.Now().UTC()
	id := meta.NewCommitID(now, message, files)
	commit := meta.Commit{ID: id, TimeUTC: now.Unix(), Message: message, BlockSize: cfg.BlockSize, Files: files}
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
	return CommitResult{
		StateID:   commit.ID,
		Created:   created,
		CreatedAt: time.Unix(commit.TimeUTC, 0).UTC(),
		FileCount: len(commit.Files),
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

func snapshot(root string, store core.BlockStore, blockSize int, head map[string]meta.FileEntry, verbose io.Writer) ([]meta.FileEntry, error) {
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
		blocks, size, err := putFileBlocks(path, store, blockSize)
		if errors.Is(err, errFileVanished) {
			return nil
		}
		if err != nil {
			return err
		}
		files = append(files, meta.FileEntry{Path: rel, Mode: uint32(info.Mode().Perm()), Size: size, ModTime: info.ModTime().Unix(), Blocks: blocks})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func putFileBlocks(path string, store core.BlockStore, blockSize int) ([]core.BlockID, int64, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, 0, errFileVanished
	}
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, blockSize)
	buffer := make([]byte, blockSize)
	var blocks []core.BlockID
	var total int64
	for {
		n, err := io.ReadFull(reader, buffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, err
		}
		if n > 0 {
			id, err := store.Put(buffer[:n])
			if err != nil {
				return nil, 0, err
			}
			blocks = append(blocks, id)
			total += int64(n)
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
	}
	return blocks, total, nil
}

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
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}
