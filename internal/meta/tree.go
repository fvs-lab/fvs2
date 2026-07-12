package meta

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	core "fvs-v2-core"
)

// Format 3 stores a state's file list as content-addressed tree objects in
// the block store, one object per directory: an unchanged directory hashes to
// the same object across states, so metadata deduplicates like file content
// does. The commit document shrinks to a pointer at the root tree.

// BlockGetter is the read-side dependency of tree operations: any
// content-addressed store that can fetch a block by id works.
type BlockGetter interface {
	Get(id core.BlockID) ([]byte, error)
}

// BlockPutter is the write-side dependency of tree operations.
type BlockPutter interface {
	Put(data []byte) (core.BlockID, error)
}

// TreeEntry is one child of a tree object. Kind is "f" (file), "d"
// (directory) or "l" (symlink).
type TreeEntry struct {
	Name    string         `json:"n"`
	Kind    string         `json:"k"`
	Mode    uint32         `json:"m,omitempty"`
	Size    int64          `json:"s,omitempty"`
	ModTime int64          `json:"t,omitempty"`
	Blocks  []core.BlockID `json:"b,omitempty"`
	Sizes   []int64        `json:"z,omitempty"`
	Link    string         `json:"l,omitempty"`
	Tree    core.BlockID   `json:"d,omitempty"`
	// Manifest points at the file's chunk list stored as its own
	// content-addressed object. Long chunk lists move there so an edit to
	// one file does not re-serialize its siblings' lists into the parent
	// tree object.
	Manifest core.BlockID `json:"mf,omitempty"`
}

// manifest is the indirect chunk list of one file.
type manifest struct {
	Blocks []core.BlockID `json:"b"`
	Sizes  []int64        `json:"z,omitempty"`
}

// manifestThreshold is the chunk count above which a file's list moves into
// a manifest object; short lists stay inline, where indirection would cost
// more than it saves.
const manifestThreshold = 2

// TreeCache memoizes decoded tree and manifest objects for the duration of
// one command: unchanged directories shared across states decode once, so
// gc, push and pull stop re-reading the same objects per state.
type TreeCache struct {
	trees     map[core.BlockID][]TreeEntry
	manifests map[core.BlockID]manifest
}

func NewTreeCache() *TreeCache {
	return &TreeCache{
		trees:     map[core.BlockID][]TreeEntry{},
		manifests: map[core.BlockID]manifest{},
	}
}

// tree returns the decoded, name-validated entries of one tree object.
func (c *TreeCache) tree(store BlockGetter, id core.BlockID) ([]TreeEntry, error) {
	if c != nil {
		if entries, ok := c.trees[id]; ok {
			return entries, nil
		}
	}
	blob, err := store.Get(id)
	if err != nil {
		return nil, fmt.Errorf("tree %s: %w", id, err)
	}
	var entries []TreeEntry
	if err := json.Unmarshal(blob, &entries); err != nil {
		return nil, fmt.Errorf("tree %s: %w", id, err)
	}
	for _, e := range entries {
		// Tree objects are untrusted: a hostile name ("..", "a/b") could
		// steer a restore outside its destination.
		if err := ValidateEntryName(e.Name); err != nil {
			return nil, fmt.Errorf("tree %s: %w", id, err)
		}
	}
	if c != nil {
		c.trees[id] = entries
	}
	return entries, nil
}

func (c *TreeCache) manifest(store BlockGetter, id core.BlockID) (manifest, error) {
	if c != nil {
		if m, ok := c.manifests[id]; ok {
			return m, nil
		}
	}
	blob, err := store.Get(id)
	if err != nil {
		return manifest{}, fmt.Errorf("manifest %s: %w", id, err)
	}
	var m manifest
	if err := json.Unmarshal(blob, &m); err != nil {
		return manifest{}, fmt.Errorf("manifest %s: %w", id, err)
	}
	if c != nil {
		c.manifests[id] = m
	}
	return m, nil
}

// WriteTree stores the file list as tree objects, bottom-up, and returns the
// root tree id. Unchanged subtrees dedup naturally through content
// addressing.
func WriteTree(store BlockPutter, files []FileEntry) (core.BlockID, error) {
	children := map[string][]TreeEntry{}
	dirs := map[string]bool{"": true}

	for _, f := range files {
		dir := path.Dir(f.Path)
		if dir == "." {
			dir = ""
		}
		for d := dir; ; d = parentDir(d) {
			dirs[d] = true
			if d == "" {
				break
			}
		}
		entry := TreeEntry{
			Name:    path.Base(f.Path),
			Kind:    "f",
			Mode:    f.Mode,
			Size:    f.Size,
			ModTime: f.ModTime,
			Blocks:  f.Blocks,
			Sizes:   f.BlockSizes,
		}
		if f.Link != "" {
			entry.Kind = "l"
			entry.Link = f.Link
			entry.Blocks = nil
			entry.Sizes = nil
		} else if len(entry.Blocks) > manifestThreshold {
			blob, err := json.Marshal(manifest{Blocks: entry.Blocks, Sizes: entry.Sizes})
			if err != nil {
				return "", err
			}
			id, err := store.Put(blob)
			if err != nil {
				return "", err
			}
			entry.Manifest = id
			entry.Blocks = nil
			entry.Sizes = nil
		}
		children[dir] = append(children[dir], entry)
	}

	// Deepest directories first, so parents can reference their children.
	sorted := make([]string, 0, len(dirs))
	for d := range dirs {
		sorted = append(sorted, d)
	}
	sort.Slice(sorted, func(i, j int) bool { return depth(sorted[i]) > depth(sorted[j]) })

	ids := map[string]core.BlockID{}
	for _, d := range sorted {
		entries := children[d]
		for name, sub := range subdirsOf(d, dirs) {
			entries = append(entries, TreeEntry{Name: name, Kind: "d", Tree: ids[sub]})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		blob, err := json.Marshal(entries)
		if err != nil {
			return "", err
		}
		id, err := store.Put(blob)
		if err != nil {
			return "", err
		}
		ids[d] = id
	}
	return ids[""], nil
}

// Reach is one traversal's result: the flattened file list plus every
// metadata object (tree and manifest ids) the walk visited.
type Reach struct {
	Files      []FileEntry
	MetaBlocks []core.BlockID
}

// walkTree flattens the tree rooted at id in a single pass, collecting the
// metadata object ids as it goes. cache may be nil.
func walkTree(store BlockGetter, id core.BlockID, cache *TreeCache) (Reach, error) {
	var out Reach
	var walk func(prefix string, tree core.BlockID) error
	walk = func(prefix string, tree core.BlockID) error {
		out.MetaBlocks = append(out.MetaBlocks, tree)
		entries, err := cache.tree(store, tree)
		if err != nil {
			return err
		}
		for _, e := range entries {
			full := path.Join(prefix, e.Name)
			switch e.Kind {
			case "d":
				if err := walk(full, e.Tree); err != nil {
					return err
				}
			case "l":
				out.Files = append(out.Files, FileEntry{Path: full, Mode: e.Mode, ModTime: e.ModTime, Link: e.Link})
			default:
				blocks, sizes := e.Blocks, e.Sizes
				if e.Manifest != "" {
					out.MetaBlocks = append(out.MetaBlocks, e.Manifest)
					m, err := cache.manifest(store, e.Manifest)
					if err != nil {
						return err
					}
					blocks, sizes = m.Blocks, m.Sizes
				}
				out.Files = append(out.Files, FileEntry{
					Path: full, Mode: e.Mode, Size: e.Size, ModTime: e.ModTime,
					Blocks: blocks, BlockSizes: sizes,
				})
			}
		}
		return nil
	}
	if err := walk("", id); err != nil {
		return Reach{}, err
	}
	sort.Slice(out.Files, func(i, j int) bool { return out.Files[i].Path < out.Files[j].Path })
	return out, nil
}

// ReadTree flattens the tree rooted at id back into the format-2 file list
// shape every consumer already understands.
func ReadTree(store BlockGetter, id core.BlockID) ([]FileEntry, error) {
	reach, err := walkTree(store, id, nil)
	if err != nil {
		return nil, err
	}
	return reach.Files, nil
}

// TreeBlocks collects every tree and manifest object id reachable from the
// root, for gc marking and sync transfers.
func TreeBlocks(store BlockGetter, id core.BlockID) ([]core.BlockID, error) {
	reach, err := walkTree(store, id, nil)
	if err != nil {
		return nil, err
	}
	return reach.MetaBlocks, nil
}

func parentDir(d string) string {
	p := path.Dir(d)
	if p == "." || p == d {
		return ""
	}
	return p
}

func depth(d string) int {
	if d == "" {
		return 0
	}
	return strings.Count(d, "/") + 1
}

// subdirsOf lists the immediate subdirectories of d among the known dirs.
func subdirsOf(d string, dirs map[string]bool) map[string]string {
	out := map[string]string{}
	for sub := range dirs {
		if sub == "" || sub == d {
			continue
		}
		if parentDir(sub) == d {
			out[path.Base(sub)] = sub
		}
	}
	return out
}
