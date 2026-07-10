package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
	"fvs2/remote"
)

type PushResult struct {
	Branch         string
	StateID        string
	TotalBlocks    int
	UploadedBlocks int
}

type PullResult struct {
	Branch           string
	StateID          string
	TotalBlocks      int
	DownloadedBlocks int
	UpToDate         bool
}

func stateBlocks(commit meta.Commit) []core.BlockID {
	seen := map[core.BlockID]bool{}
	var out []core.BlockID
	for _, f := range commit.Files {
		for _, b := range f.Blocks {
			if !seen[b] {
				seen[b] = true
				out = append(out, b)
			}
		}
	}
	return out
}

// Push uploads the head state of branch to the remote: only blocks the remote
// is missing are transferred, then the state document, then the remote ref is
// advanced with compare-and-swap. A remote ref pointing at a state unknown to
// this repo means the remote has moved past us; that requires force.
func Push(root string, rm meta.Remote, branch string, force bool) (PushResult, error) {
	root, err := absolute(root)
	if err != nil {
		return PushResult{}, err
	}
	if branch == "" {
		head, err := meta.GetHead(root)
		if err != nil {
			return PushResult{}, err
		}
		if head.Type != "branch" {
			return PushResult{}, errors.New("detached HEAD: pass a branch to push")
		}
		branch = head.Name
	}
	id, err := meta.ReadBranchHead(root, branch)
	if err != nil {
		return PushResult{}, err
	}
	if id == "" {
		return PushResult{}, fmt.Errorf("branch %q has no states to push", branch)
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return PushResult{}, err
	}
	doc, err := os.ReadFile(meta.CommitPath(root, id))
	if err != nil {
		return PushResult{}, err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return PushResult{}, err
	}

	client := remote.NewClientNS(rm.URL, rm.Token, rm.Namespace)

	remoteID, err := client.GetRef(branch)
	if err != nil {
		return PushResult{}, err
	}
	if remoteID == id {
		return PushResult{Branch: branch, StateID: id, TotalBlocks: len(stateBlocks(commit))}, nil
	}
	if remoteID != "" && !force {
		if _, err := meta.LoadCommit(root, remoteID); err != nil {
			return PushResult{}, fmt.Errorf(
				"remote %q points at state %.12s which is unknown here; pull first or push --force", branch, remoteID)
		}
	}

	blocks := stateBlocks(commit)
	missing, err := client.MissingBlocks(blocks)
	if err != nil {
		return PushResult{}, err
	}
	if _, err := client.PutBlocks(missing, store.Get); err != nil {
		return PushResult{}, err
	}
	if err := client.PutState(id, doc); err != nil {
		return PushResult{}, err
	}
	if err := client.PutRef(branch, id, remoteID); err != nil {
		return PushResult{}, err
	}
	return PushResult{
		Branch:         branch,
		StateID:        id,
		TotalBlocks:    len(blocks),
		UploadedBlocks: len(missing),
	}, nil
}

// Pull downloads the state a remote branch points at (blocks the local store
// is missing, plus the state document) and moves the local branch to it. The
// working tree is not touched; restore materializes the state when wanted.
func Pull(root string, rm meta.Remote, branch string) (PullResult, error) {
	root, err := absolute(root)
	if err != nil {
		return PullResult{}, err
	}
	if branch == "" {
		head, err := meta.GetHead(root)
		if err != nil {
			return PullResult{}, err
		}
		if head.Type != "branch" {
			return PullResult{}, errors.New("detached HEAD: pass a branch to pull")
		}
		branch = head.Name
	}

	client := remote.NewClientNS(rm.URL, rm.Token, rm.Namespace)
	id, err := client.GetRef(branch)
	if err != nil {
		return PullResult{}, err
	}
	if id == "" {
		return PullResult{}, fmt.Errorf("branch %q does not exist on the remote", branch)
	}

	lock, err := meta.LockRepo(root, 5*time.Second)
	if err != nil {
		return PullResult{}, err
	}
	defer lock.Unlock()

	localID, _ := meta.ReadBranchHead(root, branch)
	if localID == id {
		return PullResult{Branch: branch, StateID: id, UpToDate: true}, nil
	}

	doc, err := client.GetState(id)
	if err != nil {
		return PullResult{}, err
	}
	var commit meta.Commit
	if err := json.Unmarshal(doc, &commit); err != nil {
		return PullResult{}, fmt.Errorf("remote state %.12s is not a valid state document: %w", id, err)
	}
	if commit.ID != id {
		return PullResult{}, fmt.Errorf("remote state document id mismatch: got %.12s, want %.12s", commit.ID, id)
	}
	if commit.Format > meta.CurrentFormat {
		return PullResult{}, fmt.Errorf("remote state uses format %d, not supported by this build (max %d)", commit.Format, meta.CurrentFormat)
	}

	store, err := meta.NewBlockStore(root)
	if err != nil {
		return PullResult{}, err
	}
	blocks := stateBlocks(commit)
	var wanted []core.BlockID
	for _, b := range blocks {
		ok, err := store.Has(b)
		if err != nil {
			return PullResult{}, err
		}
		if !ok {
			wanted = append(wanted, b)
		}
	}
	downloaded := 0
	if err := client.FetchBlocks(wanted, func(_ core.BlockID, data []byte) error {
		if _, err := store.Put(data); err != nil {
			return err
		}
		downloaded++
		return nil
	}); err != nil {
		return PullResult{}, err
	}

	if err := writeJSONAtomic(meta.CommitPath(root, id), commit); err != nil {
		return PullResult{}, err
	}
	index, err := meta.LoadIndex(root)
	if err != nil {
		return PullResult{}, err
	}
	known := false
	for _, c := range index.Commits {
		if c.ID == id {
			known = true
			break
		}
	}
	if !known {
		index.Commits = append(index.Commits, meta.CommitSummary{ID: id, TimeUTC: commit.TimeUTC, Message: commit.Message})
		if err := meta.SaveIndex(root, index); err != nil {
			return PullResult{}, err
		}
	}
	if err := meta.WriteBranchHead(root, branch, id); err != nil {
		return PullResult{}, err
	}
	return PullResult{
		Branch:           branch,
		StateID:          id,
		TotalBlocks:      len(blocks),
		DownloadedBlocks: downloaded,
	}, nil
}
