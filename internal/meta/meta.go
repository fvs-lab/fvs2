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

// CurrentFormat is the repo format written by Init. Format 1 (implicit when
// the field is absent) uses fixed-size blocks; format 2 uses content-defined
// chunking and records per-block sizes in commits.
const CurrentFormat = 3

type Config struct {
	// ChunkingPolicy selects how chunk parameters are chosen per file.
	// Policy 0 always uses the repo parameters; policy 1 sniffs text content
	// (a format-defined, content-only rule) and chunks it finer. The policy
	// affects chunk boundaries, so it is part of the format.
	ChunkingPolicy int `json:"chunking_policy,omitempty"`
	// Format is the on-disk repo format version. 0 means 1 (legacy).
	Format    int `json:"format,omitempty"`
	BlockSize int `json:"block_size"`
	// Chunking holds the content-defined chunking parameters for format >= 2.
	Chunking *ChunkingConfig `json:"chunking,omitempty"`
}

type ChunkingConfig struct {
	MinSize int `json:"min_size"`
	AvgSize int `json:"avg_size"`
	MaxSize int `json:"max_size"`
}

// ChunkParams resolves the effective chunking parameters for this repo:
// content-defined for format >= 2, fixed-size blocks otherwise.
func (c Config) ChunkParams() core.ChunkParams {
	if c.Format >= 2 {
		if ch := c.Chunking; ch != nil && ch.MinSize > 0 && ch.AvgSize > 0 && ch.MaxSize > 0 {
			return core.ChunkParams{Min: ch.MinSize, Avg: ch.AvgSize, Max: ch.MaxSize}
		}
		return core.DefaultChunkParams()
	}
	return core.FixedChunkParams(c.BlockSize)
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
	Format    int         `json:"format,omitempty"`
	TimeUTC   int64       `json:"time_utc"`
	Message   string      `json:"message"`
	BlockSize int         `json:"block_size"`
	Files     []FileEntry `json:"files,omitempty"`
	// RootTree points at the state's root tree object in the block store
	// (format >= 3); Files stays inline in older formats.
	RootTree  core.BlockID `json:"root_tree,omitempty"`
	FileCount int          `json:"file_count,omitempty"`
	TotalSize int64        `json:"total_size,omitempty"`
	// ChunkingPolicy records the policy the state was chunked under, so
	// readers and future migrations never have to guess boundaries.
	ChunkingPolicy int `json:"chunking_policy,omitempty"`
}

// CommitFiles returns the state's flattened file list regardless of format.
func CommitFiles(store BlockGetter, c Commit) ([]FileEntry, error) {
	reach, err := CommitReach(store, c, nil)
	if err != nil {
		return nil, err
	}
	return reach.Files, nil
}

// CommitReach walks a state once, returning its flattened file list together
// with every metadata object it references. cache (optional) lets one
// command share decoded tree objects across states.
func CommitReach(store BlockGetter, c Commit, cache *TreeCache) (Reach, error) {
	if c.RootTree == "" {
		return Reach{Files: c.Files}, nil
	}
	return walkTree(store, c.RootTree, cache)
}

// CommitBlocks returns every block a state references: tree objects first
// (format >= 3), then file content blocks, deduplicated.
func CommitBlocks(store BlockGetter, c Commit) ([]core.BlockID, error) {
	return CommitBlocksCached(store, c, nil)
}

// CommitBlocksCached is CommitBlocks with a shared decoded-tree cache, for
// callers walking many states in one command (gc, pack ordering).
func CommitBlocksCached(store BlockGetter, c Commit, cache *TreeCache) ([]core.BlockID, error) {
	reach, err := CommitReach(store, c, cache)
	if err != nil {
		return nil, err
	}
	seen := map[core.BlockID]bool{}
	out := make([]core.BlockID, 0, len(reach.MetaBlocks))
	add := func(id core.BlockID) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, t := range reach.MetaBlocks {
		add(t)
	}
	for _, f := range reach.Files {
		for _, b := range f.Blocks {
			add(b)
		}
	}
	return out, nil
}

type FileEntry struct {
	Path    string `json:"path"`
	Mode    uint32 `json:"mode"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mod_time"`
	// ModTimeNS is the full nanosecond mtime. It disambiguates same-second
	// rewrites in the commit reuse shortcut; older states carry only seconds
	// (0 here), and readers fall back accordingly.
	ModTimeNS int64          `json:"mod_time_ns,omitempty"`
	Blocks    []core.BlockID `json:"blocks"`
	// BlockSizes holds the byte length of each entry in Blocks. It is present
	// in format >= 2 commits, where blocks are content-defined and vary in
	// size; format-1 readers derive offsets from the fixed block_size instead.
	BlockSizes []int64 `json:"block_sizes,omitempty"`
	// Link is the target of a symbolic link. When non-empty the entry
	// represents a symlink (not a regular file) and Blocks is empty.
	Link string `json:"link,omitempty"`
}

var ErrNotInitialized = errors.New("repo not initialized (run: fvs2 init)")

// Sentinel errors for callers that classify failures (the mount daemon maps
// them to RPC codes). Wrapped with %w, so errors.Is works through the
// contextual messages.
var (
	ErrStateNotFound     = errors.New("state not found")
	ErrLockTimeout       = errors.New("repo lock timeout")
	ErrFormatUnsupported = errors.New("repo format not supported by this build")
)

func metaDir(root string) string    { return filepath.Join(root, ".fvs2") }
func blocksDir(root string) string  { return filepath.Join(metaDir(root), "blocks") }
func commitsDir(root string) string { return filepath.Join(metaDir(root), "commits") }
func configPath(root string) string { return filepath.Join(metaDir(root), "config.json") }
func indexPath(root string) string  { return filepath.Join(metaDir(root), "index.json") }

func ensureDir(p string) error { return os.MkdirAll(p, 0o755) }

func Init(root string, blockSize int) error {
	return InitWithFormat(root, blockSize, CurrentFormat)
}

// InitWithFormat initializes a repo at a specific format, for consumers that
// must stay readable by older tooling (legacy format 2).
// policyForFormat pins new repos to the adaptive policy; legacy formats keep
// the fixed parameters older tooling expects.
func policyForFormat(format int) int {
	if format >= 3 {
		return 1
	}
	return 0
}

func InitWithFormat(root string, blockSize, format int) error {
	if blockSize <= 0 {
		blockSize = 4096
	}
	if format < 2 || format > CurrentFormat {
		return fmt.Errorf("unsupported init format %d", format)
	}
	if err := ensureDir(blocksDir(root)); err != nil {
		return err
	}
	if err := ensureDir(commitsDir(root)); err != nil {
		return err
	}

	params := core.DefaultChunkParams()
	cfg := Config{
		ChunkingPolicy: policyForFormat(format),
		Format:         format,
		BlockSize:      blockSize,
		Chunking: &ChunkingConfig{
			MinSize: params.Min,
			AvgSize: params.Avg,
			MaxSize: params.Max,
		},
	}
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
	if cfg.Format > CurrentFormat {
		return Config{}, fmt.Errorf("repo format %d (max %d): %w", cfg.Format, CurrentFormat, ErrFormatUnsupported)
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
		if errors.Is(err, os.ErrNotExist) {
			return Commit{}, fmt.Errorf("%w: %s", ErrStateNotFound, id)
		}
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
		return "", fmt.Errorf("%w: %s", ErrStateNotFound, prefix)
	}
	if len(hits) > 1 {
		return "", fmt.Errorf("ambiguous state prefix: %s", prefix)
	}
	return hits[0], nil
}

func NewBlockStore(root string) (*core.DiskBlockStore, error) {
	return core.NewDiskBlockStore(blocksDir(root))
}

// DeleteCommit removes a state. The index entry is removed first, so a crash
// leaves an orphan commit document for gc, never a dangling index entry.
func DeleteCommit(root, id string) error {
	idx, err := LoadIndex(root)
	if err != nil {
		return err
	}
	kept := idx.Commits[:0]
	found := false
	for _, c := range idx.Commits {
		if c.ID == id {
			found = true
			continue
		}
		kept = append(kept, c)
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrStateNotFound, id)
	}
	idx.Commits = kept
	if err := SaveIndex(root, idx); err != nil {
		return err
	}
	if err := os.Remove(CommitPath(root, id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// CommitsDirIDs lists the commit ids present on disk in the commits
// directory, whether or not they are referenced by the index.
func CommitsDirIDs(root string) ([]string, error) {
	ents, err := os.ReadDir(commitsDir(root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		out = append(out, strings.TrimSuffix(name, ".json"))
	}
	sort.Strings(out)
	return out, nil
}

func NewCommitID(t time.Time, message string, files []FileEntry) string {
	h := blake3.New()
	_, _ = h.Write([]byte(fmt.Sprintf("%d\n", t.UTC().UnixNano())))
	_, _ = h.Write([]byte(message))
	for _, f := range files {
		_, _ = h.Write([]byte("\n" + f.Path))
		_, _ = h.Write([]byte(fmt.Sprintf("\n%d\n%d", f.Mode, f.Size)))
		if f.Link != "" {
			_, _ = h.Write([]byte("\nlink:" + f.Link))
		}
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
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	// fsync the directory so the rename itself is durable.
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
