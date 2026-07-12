package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

type DropCmd struct {
	State string `arg:"" required:"true" help:"state id (full or prefix)"`
	Root  *CLI   `internal:"ignore"`
}

func (c *DropCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	id, err := meta.ResolveCommitID(root, c.State)
	if err != nil {
		return err
	}

	// A state still referenced by a branch or HEAD cannot be dropped.
	branches, err := meta.ListBranches(root)
	if err != nil {
		return err
	}
	for _, b := range branches {
		head, err := meta.ReadBranchHead(root, b)
		if err != nil {
			continue
		}
		if strings.TrimSpace(head) == id {
			return fmt.Errorf("state %s is the head of branch %q; move or delete the branch first", id[:12], b)
		}
	}
	h, err := meta.GetHead(root)
	if err != nil {
		return err
	}
	if h.Type == "commit" && strings.TrimSpace(h.ID) == id {
		return fmt.Errorf("state %s is currently checked out (detached HEAD); checkout something else first", id[:12])
	}

	if err := meta.DeleteCommit(root, id); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: dropped %s (run 'fvs2 gc' to reclaim space)\n", id[:12])
	return nil
}

type GcCmd struct {
	DryRun bool `cli:"dry-run" help:"report what would be removed without deleting"`
	Root   *CLI `internal:"ignore"`
}

func (c *GcCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	idx, err := meta.LoadIndex(root)
	if err != nil {
		return err
	}
	markStore, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}

	// Mark: every block reachable from an indexed state is live, tree
	// objects included. One decoded-tree cache spans the whole mark phase, so
	// directories shared across states decode once.
	live := map[core.BlockID]bool{}
	indexed := map[string]bool{}
	cache := meta.NewTreeCache()
	for _, sum := range idx.Commits {
		indexed[sum.ID] = true
		commit, err := meta.LoadCommit(root, sum.ID)
		if err != nil {
			return fmt.Errorf("load state %s: %w", sum.ID[:12], err)
		}
		blocks, err := meta.CommitBlocksCached(markStore, commit, cache)
		if err != nil {
			return fmt.Errorf("walk state %s: %w", sum.ID[:12], err)
		}
		for _, b := range blocks {
			live[b] = true
		}
	}

	// Orphan commit documents: dropped states or leftovers from a crash
	// between writing the commit and updating the index.
	onDisk, err := meta.CommitsDirIDs(root)
	if err != nil {
		return err
	}
	orphanDocs := 0
	for _, id := range onDisk {
		if indexed[id] {
			continue
		}
		orphanDocs++
		if !c.DryRun {
			if err := os.Remove(meta.CommitPath(root, id)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	// Sweep: with packs present the amnesty path rewrites the store around
	// the live set; otherwise dead loose blocks are deleted one by one.
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}
	if packed, err := store.HasPacks(); err != nil {
		return err
	} else if packed && !c.DryRun {
		ordered, err := meta.OrderedLiveBlocks(root, markStore)
		if err != nil {
			return err
		}
		if err := store.Compact(ordered); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "ok: repacked around %d live blocks, %d orphan docs removed"+"\n", len(ordered), orphanDocs)
		return nil
	}
	removed := 0
	var freed int64
	err = store.ForEach(func(id core.BlockID) error {
		if live[id] {
			return nil
		}
		if size, err := store.Size(id); err == nil {
			freed += size
		}
		removed++
		if c.DryRun {
			return nil
		}
		return store.Delete(id)
	})
	if err != nil {
		return err
	}

	verb := "removed"
	if c.DryRun {
		verb = "would remove"
	}
	fmt.Fprintf(os.Stdout, "ok: %s %d blocks (%s) and %d orphan states; %d states kept\n",
		verb, removed, humanBytes(freed), orphanDocs, len(idx.Commits))
	return nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
