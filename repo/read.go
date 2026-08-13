package repo

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/fvs-lab/fvs2/internal/meta"
)

func WriteFile(ctx context.Context, root, state, name string, destination io.Writer) error {
	root, err := absolute(root)
	if err != nil {
		return err
	}
	id, err := meta.ResolveCommitID(root, state)
	if err != nil {
		return err
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}
	files, err := meta.CommitFiles(store, commit)
	if err != nil {
		return err
	}
	name = strings.Trim(path.Clean(strings.ReplaceAll(name, "\\", "/")), "/")
	for _, file := range files {
		if file.Path != name {
			continue
		}
		if file.Kind != "" && file.Kind != string(EntryFile) {
			return fmt.Errorf("state entry %q is not a regular file", name)
		}
		if len(file.BlockSizes) > 0 && len(file.BlockSizes) != len(file.Blocks) {
			return fmt.Errorf("state entry %q has inconsistent block metadata", name)
		}
		var written int64
		for index, block := range file.Blocks {
			if err := ctx.Err(); err != nil {
				return err
			}
			content, err := store.Get(block)
			if err != nil {
				return err
			}
			if len(file.BlockSizes) > 0 && int64(len(content)) != file.BlockSizes[index] {
				return fmt.Errorf("state entry %q has an invalid block size", name)
			}
			count, err := destination.Write(content)
			written += int64(count)
			if err != nil {
				return err
			}
			if count != len(content) {
				return io.ErrShortWrite
			}
		}
		if written != file.Size {
			return fmt.Errorf("state entry %q size mismatch", name)
		}
		return nil
	}
	return fmt.Errorf("state entry %q not found", name)
}
