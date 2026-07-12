package repo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	core "fvs-v2-core"
	"fvs2/attest"
	"fvs2/internal/meta"
	"fvs2/remote"
)

// Remote aliases the remote configuration so external embedders can drive
// Push and Pull without reaching into internal packages.
type Remote = meta.Remote

type PushResult struct {
	Branch         string
	StateID        string
	TotalBlocks    int
	UploadedBlocks int
	Attestations   int
}

type PullResult struct {
	Branch           string
	StateID          string
	TotalBlocks      int
	DownloadedBlocks int
	Attestations     int
	UpToDate         bool
}

func stateBlocks(store *core.DiskBlockStore, commit meta.Commit) ([]core.BlockID, error) {
	return meta.CommitBlocks(store, commit)
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
		all, err := stateBlocks(store, commit)
		if err != nil {
			return PushResult{}, err
		}
		return PushResult{Branch: branch, StateID: id, TotalBlocks: len(all)}, nil
	}
	if remoteID != "" && !force {
		if _, err := meta.LoadCommit(root, remoteID); err != nil {
			return PushResult{}, fmt.Errorf(
				"remote %q points at state %.12s which is unknown here; pull first or push --force", branch, remoteID)
		}
	}

	blocks, err := stateBlocks(store, commit)
	if err != nil {
		return PushResult{}, err
	}
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
	pushedAtt := pushAttestations(client, root, id)
	return PushResult{
		Branch:         branch,
		StateID:        id,
		TotalBlocks:    len(blocks),
		UploadedBlocks: len(missing),
		Attestations:   pushedAtt,
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

	doc, err := client.GetStateExpanded(id)
	if err != nil {
		return PullResult{}, err
	}
	// The expanded response carries the state fields plus the server-side
	// walk of its metadata: the flattened file list and the tree/manifest
	// object ids, so the whole closure downloads in one batch.
	var expanded struct {
		meta.Commit
		MetaBlocks []core.BlockID `json:"meta_blocks"`
	}
	if err := json.Unmarshal(doc, &expanded); err != nil {
		return PullResult{}, fmt.Errorf("remote state %.12s is not a valid state document: %w", id, err)
	}
	commit := expanded.Commit
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
	downloaded := 0
	put := func(_ core.BlockID, data []byte) error {
		if _, err := store.Put(data); err != nil {
			return err
		}
		downloaded++
		return nil
	}
	if commit.RootTree != "" && expanded.MetaBlocks == nil {
		// A server predating expansion: walk the tree level by level.
		downloaded, err = fetchTrees(store, client, commit)
		if err != nil {
			return PullResult{}, err
		}
	} else {
		wanted, err := missingBlocks(store, expanded.MetaBlocks)
		if err != nil {
			return PullResult{}, err
		}
		if err := client.FetchBlocks(wanted, put); err != nil {
			return PullResult{}, err
		}
	}

	// With the metadata objects local, one verified walk (they are
	// content-addressed) yields the authoritative block list; the server's
	// expanded file list is never trusted for reachability.
	cache := meta.NewTreeCache()
	reach, err := meta.CommitReach(store, commit, cache)
	if err != nil {
		return PullResult{}, fmt.Errorf("remote state %.12s: incomplete metadata closure: %w", id, err)
	}
	var fileBlocks []core.BlockID
	for _, f := range reach.Files {
		fileBlocks = append(fileBlocks, f.Blocks...)
	}
	wanted, err := missingBlocks(store, dedup(fileBlocks))
	if err != nil {
		return PullResult{}, err
	}
	if err := client.FetchBlocks(wanted, put); err != nil {
		return PullResult{}, err
	}
	blocks, err := meta.CommitBlocksCached(store, commit, cache)
	if err != nil {
		return PullResult{}, err
	}

	if commit.RootTree != "" {
		// The expanded response inlines the flattened list; the local
		// document keeps only the root tree pointer, like the pushed one.
		commit.Files = nil
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
	pulledAtt := pullAttestations(client, root, id)
	return PullResult{
		Branch:           branch,
		StateID:          id,
		TotalBlocks:      len(blocks),
		DownloadedBlocks: downloaded,
		Attestations:     pulledAtt,
	}, nil
}

// missingBlocks filters ids down to those the local store does not have yet.
func missingBlocks(store *core.DiskBlockStore, ids []core.BlockID) ([]core.BlockID, error) {
	var out []core.BlockID
	for _, id := range ids {
		ok, err := store.Has(id)
		if err != nil {
			return nil, err
		}
		if !ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// dedup keeps the first occurrence of each id, preserving order.
func dedup(ids []core.BlockID) []core.BlockID {
	seen := make(map[core.BlockID]bool, len(ids))
	out := ids[:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// fetchTrees walks a format-3 state's tree objects, fetching the missing ones
// level by level. It remains the fallback for servers predating state
// expansion (no meta_blocks in the response).
func fetchTrees(store *core.DiskBlockStore, client *remote.Client, commit meta.Commit) (int, error) {
	if commit.RootTree == "" {
		return 0, nil
	}
	downloaded := 0
	pending := []core.BlockID{commit.RootTree}
	for len(pending) > 0 {
		var missing []core.BlockID
		for _, id := range pending {
			ok, err := store.Has(id)
			if err != nil {
				return downloaded, err
			}
			if !ok {
				missing = append(missing, id)
			}
		}
		if err := client.FetchBlocks(missing, func(_ core.BlockID, data []byte) error {
			if _, err := store.Put(data); err != nil {
				return err
			}
			downloaded++
			return nil
		}); err != nil {
			return downloaded, err
		}
		var next []core.BlockID
		for _, id := range pending {
			blob, err := store.Get(id)
			if err != nil {
				return downloaded, err
			}
			var entries []meta.TreeEntry
			if err := json.Unmarshal(blob, &entries); err != nil {
				// A pending id may be a manifest object rather than a tree:
				// it carries no children, so nothing further to walk.
				continue
			}
			for _, e := range entries {
				if e.Kind == "d" {
					next = append(next, e.Tree)
				}
				if e.Manifest != "" {
					next = append(next, e.Manifest)
				}
			}
		}
		pending = next
	}
	return downloaded, nil
}

// pushAttestations uploads the local attestations for a state. Attestations
// are an optional protocol family, so a server that does not support them
// leaves the push unaffected.
func pushAttestations(client *remote.Client, root, state string) int {
	list, err := LoadAttestations(root, state)
	if err != nil || len(list) == 0 {
		return 0
	}
	n, err := client.PutAttestations(list)
	if err != nil {
		return 0
	}
	return n
}

// pullAttestations downloads and stores the attestations a remote holds for a
// state. Only signature-valid ones are kept.
func pullAttestations(client *remote.Client, root, state string) int {
	list, err := client.GetAttestations(state)
	if err != nil {
		return 0
	}
	stored := 0
	for _, a := range list {
		if attest.Verify(a) != nil {
			continue
		}
		if _, err := StoreAttestation(root, a); err == nil {
			stored++
		}
	}
	return stored
}
