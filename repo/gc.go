package repo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

type GCOptions struct {
	DryRun bool
}

type GCResult struct {
	Repositories  int
	States        int
	LiveBlocks    int
	RemovedBlocks int
	RemovedBytes  int64
	OrphanStates  int
	Compacted     bool
}

// GCShared marks every repository before sweeping a shared block store. All
// repositories must point at blocksPath. Omitting one would make its blocks
// appear dead, so callers must provide their complete repository set. An empty
// set removes every block from the store.
func GCShared(ctx context.Context, blocksPath string, repositories []string, opts GCOptions) (GCResult, error) {
	resolvedBlocks, err := filepath.Abs(blocksPath)
	if err != nil {
		return GCResult{}, err
	}
	resolvedBlocks = filepath.Clean(resolvedBlocks)

	roots := make([]string, 0, len(repositories))
	seenRoots := map[string]bool{}
	for _, repository := range repositories {
		root, err := absolute(repository)
		if err != nil {
			return GCResult{}, err
		}
		if seenRoots[root] {
			continue
		}
		configured, err := meta.BlockStorePath(root)
		if err != nil {
			return GCResult{}, err
		}
		if filepath.Clean(configured) != resolvedBlocks {
			return GCResult{}, fmt.Errorf("repository %s uses block store %s", root, configured)
		}
		seenRoots[root] = true
		roots = append(roots, root)
	}
	sort.Strings(roots)

	locks := make([]*meta.RepoLock, 0, len(roots))
	for _, root := range roots {
		lock, err := meta.LockRepo(root, lockTimeout)
		if err != nil {
			for i := len(locks) - 1; i >= 0; i-- {
				_ = locks[i].Unlock()
			}
			return GCResult{}, err
		}
		locks = append(locks, lock)
	}
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			_ = locks[i].Unlock()
		}
	}()

	store, err := core.NewDiskBlockStore(resolvedBlocks)
	if err != nil {
		return GCResult{}, err
	}
	live := map[core.BlockID]bool{}
	result := GCResult{Repositories: len(roots)}
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return GCResult{}, err
		}
		index, err := meta.LoadIndex(root)
		if err != nil {
			return GCResult{}, err
		}
		indexed := make(map[string]bool, len(index.Commits))
		cache := meta.NewTreeCache()
		for _, summary := range index.Commits {
			indexed[summary.ID] = true
			commit, err := meta.LoadCommit(root, summary.ID)
			if err != nil {
				return GCResult{}, fmt.Errorf("load state %s: %w", summary.ID, err)
			}
			blocks, err := meta.CommitBlocksCached(store, commit, cache)
			if err != nil {
				return GCResult{}, fmt.Errorf("walk state %s: %w", summary.ID, err)
			}
			for _, id := range blocks {
				live[id] = true
			}
		}
		result.States += len(index.Commits)
		onDisk, err := meta.CommitsDirIDs(root)
		if err != nil {
			return GCResult{}, err
		}
		for _, id := range onDisk {
			if indexed[id] {
				continue
			}
			result.OrphanStates++
			if !opts.DryRun {
				if err := os.Remove(meta.CommitPath(root, id)); err != nil && !os.IsNotExist(err) {
					return GCResult{}, err
				}
			}
		}
	}
	result.LiveBlocks = len(live)

	if packed, err := store.HasPacks(); err != nil {
		return GCResult{}, err
	} else if packed && !opts.DryRun {
		ordered := make([]core.BlockID, 0, len(live))
		for id := range live {
			ordered = append(ordered, id)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
		if err := store.Compact(ordered); err != nil {
			return GCResult{}, err
		}
		result.Compacted = true
		return result, nil
	}

	err = store.ForEach(func(id core.BlockID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if live[id] {
			return nil
		}
		if size, err := store.Size(id); err == nil {
			result.RemovedBytes += size
		}
		result.RemovedBlocks++
		if opts.DryRun {
			return nil
		}
		return store.Delete(id)
	})
	return result, err
}
