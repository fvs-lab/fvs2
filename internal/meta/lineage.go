package meta

import (
	"sort"

	core "fvs-v2-core"
)

// OrderedLiveBlocks returns every live block of the repo in lineage order:
// for each path (sorted), the chunks of its versions oldest to newest, then
// tree and manifest objects grouped at the end. Packing frames in this order
// puts consecutive versions of the same file next to each other, so the
// frame compression window captures their redundancy.
func OrderedLiveBlocks(root string, store *core.DiskBlockStore) ([]core.BlockID, error) {
	index, err := LoadIndex(root)
	if err != nil {
		return nil, err
	}

	perPath := map[string][]core.BlockID{}
	pathSeen := map[string]bool{}
	var paths []string
	var metaObjects []core.BlockID
	seen := map[core.BlockID]bool{}
	seenMeta := map[core.BlockID]bool{}

	for _, sum := range index.Commits {
		commit, err := LoadCommit(root, sum.ID)
		if err != nil {
			return nil, err
		}
		files, err := CommitFiles(store, commit)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if !pathSeen[f.Path] {
				pathSeen[f.Path] = true
				paths = append(paths, f.Path)
			}
			for _, b := range f.Blocks {
				if !seen[b] {
					seen[b] = true
					perPath[f.Path] = append(perPath[f.Path], b)
				}
			}
		}
		if commit.RootTree != "" {
			trees, err := TreeBlocks(store, commit.RootTree)
			if err != nil {
				return nil, err
			}
			for _, t := range trees {
				if !seenMeta[t] {
					seenMeta[t] = true
					metaObjects = append(metaObjects, t)
				}
			}
		}
	}

	sort.Strings(paths)
	out := make([]core.BlockID, 0, len(seen)+len(metaObjects))
	for _, p := range paths {
		out = append(out, perPath[p]...)
	}
	return append(out, metaObjects...), nil
}
