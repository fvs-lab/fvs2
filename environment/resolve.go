package environment

import (
	"fmt"
	"path/filepath"

	"fvs2/internal/meta"
	fvsrepo "fvs2/repo"
)

func absRepo(baseDir, repo string) string {
	if filepath.IsAbs(repo) {
		return repo
	}
	return filepath.Join(baseDir, repo)
}

// Resolve pins every layer of the manifest to a concrete state and returns a
// lockfile. A layer with a Pull directive is refreshed from its remote first,
// so locking captures the latest shared layer. baseDir is the directory the
// manifest's relative repo paths resolve against.
func Resolve(m Manifest, baseDir string) (Lock, error) {
	lock := Lock{Name: m.Name, Upper: m.Upper, Mount: m.Mount}
	for _, layer := range m.Layers {
		repoPath := m.repoPath(baseDir, layer)

		if layer.Pull != nil {
			rm, err := meta.GetRemote(repoPath, layer.Pull.Remote)
			if err != nil {
				return Lock{}, fmt.Errorf("layer %q: %w", layer.Name, err)
			}
			if _, err := fvsrepo.Pull(repoPath, rm, layer.Pull.Branch); err != nil {
				return Lock{}, fmt.Errorf("layer %q: pull: %w", layer.Name, err)
			}
		}

		id, err := resolveRevision(repoPath, layer)
		if err != nil {
			return Lock{}, fmt.Errorf("layer %q: %w", layer.Name, err)
		}
		lock.Layers = append(lock.Layers, LockedLayer{
			Name:    layer.Name,
			Repo:    layer.Repo,
			StateID: id,
		})
	}
	return lock, nil
}

func resolveRevision(repoPath string, layer Layer) (string, error) {
	switch {
	case layer.State != "":
		return meta.ResolveCommitID(repoPath, layer.State)
	case layer.Branch != "":
		id, err := meta.ReadBranchHead(repoPath, layer.Branch)
		if err != nil {
			return "", err
		}
		if id == "" {
			return "", fmt.Errorf("branch %q has no states", layer.Branch)
		}
		return id, nil
	default:
		id, err := meta.ResolveHeadCommit(repoPath)
		if err != nil {
			return "", err
		}
		if id == "" {
			return "", fmt.Errorf("repo HEAD has no state")
		}
		return id, nil
	}
}

// Verify checks that every pinned state is present in its repository, so a
// lockfile can be validated before a mount is attempted. baseDir is the
// lockfile's directory.
func Verify(l Lock, baseDir string) error {
	for _, layer := range l.Layers {
		repoPath := absRepo(baseDir, layer.Repo)
		if _, err := meta.LoadCommit(repoPath, layer.StateID); err != nil {
			return fmt.Errorf("layer %q: state %.12s missing in %s: %w",
				layer.Name, layer.StateID, layer.Repo, err)
		}
	}
	return nil
}

// Sync pulls, for every layer that declares a Pull directive, the pinned
// state's blocks from the remote into the layer repository, so a machine with
// only the lockfile can materialize the environment. Layers without a Pull
// directive are assumed already present and are verified. baseDir is the
// manifest's directory.
func Sync(m Manifest, l Lock, baseDir string) error {
	pinned := map[string]LockedLayer{}
	for _, ll := range l.Layers {
		pinned[ll.Name] = ll
	}
	for _, layer := range m.Layers {
		locked, ok := pinned[layer.Name]
		if !ok {
			return fmt.Errorf("layer %q is in the manifest but not the lockfile", layer.Name)
		}
		repoPath := m.repoPath(baseDir, layer)
		if _, err := meta.LoadCommit(repoPath, locked.StateID); err == nil {
			continue // already present
		}
		if layer.Pull == nil {
			return fmt.Errorf("layer %q: state %.12s not present and no pull source", layer.Name, locked.StateID)
		}
		rm, err := meta.GetRemote(repoPath, layer.Pull.Remote)
		if err != nil {
			return fmt.Errorf("layer %q: %w", layer.Name, err)
		}
		if _, err := fvsrepo.Pull(repoPath, rm, layer.Pull.Branch); err != nil {
			return fmt.Errorf("layer %q: pull: %w", layer.Name, err)
		}
		if _, err := meta.LoadCommit(repoPath, locked.StateID); err != nil {
			return fmt.Errorf("layer %q: pinned state %.12s not on the remote branch %q",
				layer.Name, locked.StateID, layer.Pull.Branch)
		}
	}
	return nil
}
