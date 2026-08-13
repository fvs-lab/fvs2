package meta

import (
	"sort"

	core "github.com/fvs-lab/core"
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

	cache := NewTreeCache()
	for _, sum := range index.Commits {
		commit, err := LoadCommit(root, sum.ID)
		if err != nil {
			return nil, err
		}
		reach, err := CommitReach(store, commit, cache)
		if err != nil {
			return nil, err
		}
		for _, f := range reach.Files {
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
		for _, t := range reach.MetaBlocks {
			if !seenMeta[t] {
				seenMeta[t] = true
				metaObjects = append(metaObjects, t)
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
