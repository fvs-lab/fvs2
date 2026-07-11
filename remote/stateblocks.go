package remote

import (
	"encoding/json"
	"fmt"

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

// CollectStateBlocks returns every block a state document references: inline
// file lists for legacy formats, tree objects plus file blocks for format 3.
// Servers use it for gc marking and for expanding state documents.
func CollectStateBlocks(blocks BlockBackend, doc []byte) ([]core.BlockID, error) {
	var commit struct {
		Format   int          `json:"format"`
		RootTree core.BlockID `json:"root_tree"`
		Files    []struct {
			Blocks []core.BlockID `json:"blocks"`
		} `json:"files"`
	}
	if err := json.Unmarshal(doc, &commit); err != nil {
		return nil, err
	}
	// A state written by newer tooling may reference blocks through
	// structures this build cannot see: treating it as empty would let gc
	// destroy live data, so refuse instead.
	if commit.Format > meta.CurrentFormat {
		return nil, fmt.Errorf("state format %d is newer than this build supports (max %d)", commit.Format, meta.CurrentFormat)
	}
	seen := map[core.BlockID]bool{}
	var out []core.BlockID
	add := func(id core.BlockID) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	if commit.RootTree == "" {
		for _, f := range commit.Files {
			for _, b := range f.Blocks {
				add(b)
			}
		}
		return out, nil
	}
	files, err := expandTree(blocks, commit.RootTree, add)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		for _, b := range f.Blocks {
			add(b)
		}
	}
	return out, nil
}

// ExpandStateFiles flattens a state document's file list, walking format-3
// trees through the block backend.
func ExpandStateFiles(blocks BlockBackend, doc []byte) ([]meta.FileEntry, error) {
	var commit meta.Commit
	if err := json.Unmarshal(doc, &commit); err != nil {
		return nil, err
	}
	if commit.RootTree == "" {
		return commit.Files, nil
	}
	return expandTree(blocks, commit.RootTree, func(core.BlockID) {})
}

func expandTree(blocksBackend BlockBackend, root core.BlockID, visitTree func(core.BlockID)) ([]meta.FileEntry, error) {
	var out []meta.FileEntry
	var walk func(prefix string, tree core.BlockID) error
	walk = func(prefix string, tree core.BlockID) error {
		visitTree(tree)
		blob, err := blocksBackend.Get(tree)
		if err != nil {
			return fmt.Errorf("tree %s: %w", tree, err)
		}
		var entries []meta.TreeEntry
		if err := json.Unmarshal(blob, &entries); err != nil {
			return fmt.Errorf("tree %s: %w", tree, err)
		}
		for _, e := range entries {
			full := e.Name
			if prefix != "" {
				full = prefix + "/" + e.Name
			}
			switch e.Kind {
			case "d":
				if err := walk(full, e.Tree); err != nil {
					return err
				}
			case "l":
				out = append(out, meta.FileEntry{Path: full, Mode: e.Mode, ModTime: e.ModTime, Link: e.Link})
			default:
				blocks, sizes := e.Blocks, e.Sizes
				if e.Manifest != "" {
					visitTree(e.Manifest)
					mblob, err := blocksBackend.Get(e.Manifest)
					if err != nil {
						return fmt.Errorf("manifest %s: %w", e.Manifest, err)
					}
					var m struct {
						Blocks []core.BlockID `json:"b"`
						Sizes  []int64        `json:"z"`
					}
					if err := json.Unmarshal(mblob, &m); err != nil {
						return fmt.Errorf("manifest %s: %w", e.Manifest, err)
					}
					blocks, sizes = m.Blocks, m.Sizes
				}
				out = append(out, meta.FileEntry{
					Path: full, Mode: e.Mode, Size: e.Size, ModTime: e.ModTime,
					Blocks: blocks, BlockSizes: sizes,
				})
			}
		}
		return nil
	}
	if err := walk("", root); err != nil {
		return nil, err
	}
	return out, nil
}
