package remote

import (
	"encoding/json"
	"fmt"

	"github.com/fvs-lab/fvs2/internal/meta"
)

// ValidateState checks a state document before the server accepts it or lets
// a ref point at it. Uploaded states are untrusted: the document must parse,
// carry the id it is stored under, use a format this build understands (so a
// newer-format state can never make gc inoperable), reference only safe
// paths, and its whole tree/manifest closure must decode out of the block
// store with every referenced content block present.
func ValidateState(blocks BlockBackend, id string, doc []byte) error {
	var commit meta.Commit
	if err := json.Unmarshal(doc, &commit); err != nil {
		return fmt.Errorf("state %.12s: invalid document: %w", id, err)
	}
	if commit.ID != id {
		return fmt.Errorf("state document id %.12s does not match %.12s", commit.ID, id)
	}
	if commit.Format > meta.CurrentFormat {
		return fmt.Errorf("state format %d is newer than this server supports (max %d)", commit.Format, meta.CurrentFormat)
	}
	files, _, err := ExpandState(blocks, doc)
	if err != nil {
		return fmt.Errorf("state %.12s: %w", id, err)
	}
	for _, f := range files {
		if err := meta.ValidateRelPath(f.Path); err != nil {
			return fmt.Errorf("state %.12s: %w", id, err)
		}
		if f.Link != "" {
			if len(f.Blocks) != 0 {
				return fmt.Errorf("state %.12s: symlink %q carries blocks", id, f.Path)
			}
			continue
		}
		if len(f.BlockSizes) > 0 {
			if len(f.BlockSizes) != len(f.Blocks) {
				return fmt.Errorf("state %.12s: %q has %d block sizes for %d blocks", id, f.Path, len(f.BlockSizes), len(f.Blocks))
			}
			var sum int64
			for _, s := range f.BlockSizes {
				if s < 0 {
					return fmt.Errorf("state %.12s: %q has a negative block size", id, f.Path)
				}
				sum += s
			}
			if sum != f.Size {
				return fmt.Errorf("state %.12s: %q block sizes sum to %d, size is %d", id, f.Path, sum, f.Size)
			}
		}
		for _, b := range f.Blocks {
			if !hexID.MatchString(string(b)) {
				return fmt.Errorf("state %.12s: %q references invalid block id %q", id, f.Path, b)
			}
			ok, err := blocks.Has(b)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("state %.12s: %q references missing block %.12s", id, f.Path, b)
			}
		}
	}
	return nil
}
