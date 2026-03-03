package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "fvs-v2-core"

	"github.com/zeebo/blake3"
)

type Config struct {
	BlockSize int `json:"block_size"`
}

type Index struct {
	Commits []CommitSummary `json:"commits"`
}

type CommitSummary struct {
	ID      string `json:"id"`
	TimeUTC int64  `json:"time_utc"`
	Message string `json:"message"`
}

type Commit struct {
	ID        string      `json:"id"`
	TimeUTC   int64       `json:"time_utc"`
	Message   string      `json:"message"`
	BlockSize int         `json:"block_size"`
	Files     []FileEntry `json:"files"`
}

type FileEntry struct {
	Path   string         `json:"path"`
	Mode   uint32         `json:"mode"`
	Size   int64          `json:"size"`
	Blocks []core.BlockID `json:"blocks"`
}

var ErrNotInitialized = errors.New("repo not initialized (run: fvs2 init)")

func metaDir(root string) string    { return filepath.Join(root, ".fvs2") }
func blocksDir(root string) string  { return filepath.Join(metaDir(root), "blocks") }
func commitsDir(root string) string { return filepath.Join(metaDir(root), "commits") }
func configPath(root string) string { return filepath.Join(metaDir(root), "config.json") }
func indexPath(root string) string  { return filepath.Join(metaDir(root), "index.json") }

func ensureDir(p string) error { return os.MkdirAll(p, 0o755) }

func Init(root string, blockSize int) error {
	if blockSize <= 0 {
		blockSize = 4096
	}
	if err := ensureDir(blocksDir(root)); err != nil {
		return err
	}
	if err := ensureDir(commitsDir(root)); err != nil {
		return err
	}

	cfg := Config{BlockSize: blockSize}
	if err := writeJSONAtomic(configPath(root), cfg); err != nil {
		return err
	}
	if _, err := os.Stat(indexPath(root)); errors.Is(err, os.ErrNotExist) {
		idx := Index{Commits: nil}
		if err := writeJSONAtomic(indexPath(root), idx); err != nil {
			return err
		}
	}

	if err := ensureRefs(root); err != nil {
		return err
	}
	// default branch + HEAD
	if _, err := os.Stat(branchRefPath(root, "main")); errors.Is(err, os.ErrNotExist) {
		if err := WriteBranchHead(root, "main", ""); err != nil {
			return err
		}
	}
	if _, err := os.Stat(headPath(root)); errors.Is(err, os.ErrNotExist) {
		if err := writeJSONAtomic(headPath(root), Head{Type: "branch", Name: "main"}); err != nil {
			return err
		}
	}
	return nil
}

func LoadConfig(root string) (Config, error) {
	b, err := os.ReadFile(configPath(root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, ErrNotInitialized
		}
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 4096
	}
	return cfg, nil
}

func LoadIndex(root string) (Index, error) {
	b, err := os.ReadFile(indexPath(root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Index{}, ErrNotInitialized
		}
		return Index{}, err
	}
	var idx Index
	if err := json.Unmarshal(b, &idx); err != nil {
		return Index{}, err
	}
	return idx, nil
}

func SaveIndex(root string, idx Index) error {
	return writeJSONAtomic(indexPath(root), idx)
}

func CommitPath(root, id string) string {
	return filepath.Join(commitsDir(root), id+".json")
}

func LoadCommit(root, id string) (Commit, error) {
	b, err := os.ReadFile(CommitPath(root, id))
	if err != nil {
		return Commit{}, err
	}
	var c Commit
	if err := json.Unmarshal(b, &c); err != nil {
		return Commit{}, err
	}
	return c, nil
}

func ResolveCommitID(root, prefix string) (string, error) {
	idx, err := LoadIndex(root)
	if err != nil {
		return "", err
	}
	if prefix == "" {
		return "", errors.New("empty state id")
	}
	var hits []string
	for _, c := range idx.Commits {
		if strings.HasPrefix(c.ID, prefix) {
			hits = append(hits, c.ID)
		}
	}
	sort.Strings(hits)
	if len(hits) == 0 {
		return "", fmt.Errorf("state not found: %s", prefix)
	}
	if len(hits) > 1 {
		return "", fmt.Errorf("ambiguous state prefix: %s", prefix)
	}
	return hits[0], nil
}

func NewBlockStore(root string) (*core.DiskBlockStore, error) {
	return core.NewDiskBlockStore(blocksDir(root))
}

func NewCommitID(t time.Time, message string, files []FileEntry) string {
	h := blake3.New()
	_, _ = h.Write([]byte(fmt.Sprintf("%d\n", t.UTC().UnixNano())))
	_, _ = h.Write([]byte(message))
	for _, f := range files {
		_, _ = h.Write([]byte("\n" + f.Path))
		_, _ = h.Write([]byte(fmt.Sprintf("\n%d\n%d", f.Mode, f.Size)))
		for _, b := range f.Blocks {
			_, _ = h.Write([]byte(string(b)))
		}
	}
	sum := h.Sum(nil)
	return fmt.Sprintf("%x", sum)
}

func writeJSONAtomic(path string, v any) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
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
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if _, err := tmp.Write([]byte("\n")); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}
