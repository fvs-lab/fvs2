package main

import (
	"fmt"
	"os"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
)

type PackCmd struct {
	Cold bool `cli:"cold" help:"cold tier: 4 MiB frames, maximum compression (best for archived history)"`

	Verify PackVerifyCmd `cmd:"verify" help:"Verify pack frame checksums and chunk hashes"`

	Root *CLI `internal:"ignore"`
}

type PackVerifyCmd struct {
	Root *CLI `internal:"ignore"`
}

// Run re-reads every pack frame, checking the stored frame checksum against
// the compressed payload and every chunk against its content address.
func (c *PackVerifyCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}
	frames, chunks, err := store.VerifyPacks()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: %d frames, %d chunks verified\n", frames, chunks)
	return nil
}

// Run compacts the store into lineage-ordered pack frames: consecutive
// versions of the same file land in the same compression window, and
// unreferenced objects disappear (the frame amnesty).
func (c *PackCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}
	before, err := storeBytes(root)
	if err != nil {
		return err
	}
	ordered, err := meta.OrderedLiveBlocks(root, store)
	if err != nil {
		return err
	}
	opts := core.PackOptions{}
	if c.Cold {
		opts = core.ColdPackOptions()
	}
	if err := store.CompactOptions(ordered, opts); err != nil {
		return err
	}
	after, err := storeBytes(root)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: packed %d blocks, %s -> %s\n",
		len(ordered), humanBytes(before), humanBytes(after))
	return nil
}

func storeBytes(root string) (int64, error) {
	dir := root + "/.fvs2/blocks"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total, nil
}
