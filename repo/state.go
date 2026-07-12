package repo

import (
	"errors"
	"fmt"
	"os"
	"time"

	"fvs2/internal/meta"
)

// ResolveCommit resolves which state to mount or restore: an explicit state
// id or unique prefix wins, then a branch head, then HEAD. Exported so
// embedders (the mount daemon) stop parsing repository metadata themselves.
// An empty result with a nil error means the resolved branch has no states
// yet.
func ResolveCommit(root, statePrefix, branch string) (string, error) {
	root, err := absolute(root)
	if err != nil {
		return "", err
	}
	if statePrefix != "" {
		return meta.ResolveCommitID(root, statePrefix)
	}
	if branch != "" {
		id, err := meta.ReadBranchHead(root, branch)
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("branch not found: %s: %w", branch, ErrStateNotFound)
		}
		if err != nil {
			return "", err
		}
		return id, nil
	}
	return meta.ResolveHeadCommit(root)
}

// StateDetail describes one state's document-level metadata, whatever the
// repo format.
type StateDetail struct {
	ID             string
	CreatedAt      time.Time
	Message        string
	Format         int
	BlockSize      int
	FileCount      int
	TotalSize      int64
	ChunkingPolicy int
}

// DescribeState loads one state's metadata by id or unique prefix. Together
// with StateFiles (the flattened mount tree / file list) it covers what a
// mount daemon needs without touching .fvs2 internals.
func DescribeState(root, state string) (StateDetail, error) {
	root, err := absolute(root)
	if err != nil {
		return StateDetail{}, err
	}
	id, err := meta.ResolveCommitID(root, state)
	if err != nil {
		return StateDetail{}, err
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return StateDetail{}, err
	}
	detail := StateDetail{
		ID:             commit.ID,
		CreatedAt:      time.Unix(commit.TimeUTC, 0).UTC(),
		Message:        commit.Message,
		Format:         commit.Format,
		BlockSize:      commit.BlockSize,
		FileCount:      commit.FileCount,
		TotalSize:      commit.TotalSize,
		ChunkingPolicy: commit.ChunkingPolicy,
	}
	if commit.RootTree == "" {
		detail.FileCount = len(commit.Files)
		detail.TotalSize = 0
		for _, f := range commit.Files {
			detail.TotalSize += f.Size
		}
	}
	return detail, nil
}
