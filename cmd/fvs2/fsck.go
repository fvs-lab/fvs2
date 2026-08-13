package main

import (
	"fmt"
	"os"
	"time"

	core "github.com/fvs-lab/core"
	"github.com/fvs-lab/fvs2/internal/meta"
)

type FsckCmd struct {
	Full bool `cli:"full" help:"also read and re-hash every referenced block (slow)"`
	Root *CLI `internal:"ignore"`
}

// Run validates the repository: every indexed state must load, use a
// supported format, decode its whole tree/manifest closure with safe paths,
// and reference only blocks the store has; refs and HEAD must point at
// indexed states. --full additionally reads every referenced block, which
// re-verifies its content hash (and, for packed chunks, the frame checksum).
func (c *FsckCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	var problems []string
	report := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	idx, err := meta.LoadIndex(root)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}

	states := 0
	blocksChecked := map[core.BlockID]bool{}
	indexed := map[string]bool{}
	cache := meta.NewTreeCache()
	for _, sum := range idx.Commits {
		indexed[sum.ID] = true
		commit, err := meta.LoadCommit(root, sum.ID)
		if err != nil {
			report("state %.12s: %v", sum.ID, err)
			continue
		}
		if commit.ID != sum.ID {
			report("state %.12s: document carries id %.12s", sum.ID, commit.ID)
			continue
		}
		if commit.Format > meta.CurrentFormat {
			report("state %.12s: format %d is newer than this build supports (max %d)", sum.ID, commit.Format, meta.CurrentFormat)
			continue
		}
		reach, err := meta.CommitReach(store, commit, cache)
		if err != nil {
			report("state %.12s: %v", sum.ID, err)
			continue
		}
		states++
		for _, f := range reach.Files {
			if err := meta.ValidateRelPath(f.Path); err != nil {
				report("state %.12s: %v", sum.ID, err)
				continue
			}
			if len(f.BlockSizes) > 0 {
				if len(f.BlockSizes) != len(f.Blocks) {
					report("state %.12s: %s: %d block sizes for %d blocks", sum.ID, f.Path, len(f.BlockSizes), len(f.Blocks))
				} else {
					var total int64
					for _, s := range f.BlockSizes {
						total += s
					}
					if total != f.Size {
						report("state %.12s: %s: block sizes sum to %d, size is %d", sum.ID, f.Path, total, f.Size)
					}
				}
			}
			for _, b := range f.Blocks {
				if blocksChecked[b] {
					continue
				}
				blocksChecked[b] = true
				if c.Full {
					if _, err := store.Get(b); err != nil {
						report("state %.12s: %s: block %.12s: %v", sum.ID, f.Path, b, err)
					}
					continue
				}
				ok, err := store.Has(b)
				if err != nil {
					report("state %.12s: %s: block %.12s: %v", sum.ID, f.Path, b, err)
				} else if !ok {
					report("state %.12s: %s: block %.12s missing", sum.ID, f.Path, b)
				}
			}
		}
		// The metadata objects themselves decoded during the walk; in full
		// mode re-read them so their content hashes are verified too.
		if c.Full {
			for _, b := range reach.MetaBlocks {
				if blocksChecked[b] {
					continue
				}
				blocksChecked[b] = true
				if _, err := store.Get(b); err != nil {
					report("state %.12s: metadata object %.12s: %v", sum.ID, b, err)
				}
			}
		}
	}

	// Refs: every branch and HEAD must point at an indexed state.
	branches, err := meta.ListBranches(root)
	if err != nil {
		return err
	}
	for _, b := range branches {
		id, err := meta.ReadBranchHead(root, b)
		if err != nil {
			report("branch %s: %v", b, err)
			continue
		}
		if id != "" && !indexed[id] {
			report("branch %s points at unknown state %.12s", b, id)
		}
	}
	if headID, err := meta.ResolveHeadCommit(root); err != nil {
		report("HEAD: %v", err)
	} else if headID != "" && !indexed[headID] {
		report("HEAD points at unknown state %.12s", headID)
	}

	// Orphan commit documents are not corruption (gc reclaims them), but
	// worth surfacing.
	orphans := 0
	onDisk, err := meta.CommitsDirIDs(root)
	if err != nil {
		return err
	}
	for _, id := range onDisk {
		if !indexed[id] {
			orphans++
		}
	}

	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintf(os.Stderr, "fsck: %s\n", p)
		}
		return fmt.Errorf("fsck found %d problem(s)", len(problems))
	}
	mode := "existence"
	if c.Full {
		mode = "content"
	}
	orphanNote := ""
	if orphans > 0 {
		orphanNote = fmt.Sprintf("; %d orphan state doc(s), run 'fvs2 gc'", orphans)
	}
	fmt.Fprintf(os.Stdout, "ok: %d states, %d blocks verified (%s), %d branches%s\n",
		states, len(blocksChecked), mode, len(branches), orphanNote)
	return nil
}
