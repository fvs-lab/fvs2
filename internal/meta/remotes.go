package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Remote struct {
	URL       string `json:"url"`
	Token     string `json:"token,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

func remotesPath(root string) string { return filepath.Join(metaDir(root), "remotes.json") }

func LoadRemotes(root string) (map[string]Remote, error) {
	if _, err := LoadConfig(root); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(remotesPath(root))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Remote{}, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]Remote
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func saveRemotes(root string, remotes map[string]Remote) error {
	b, err := json.MarshalIndent(remotes, "", "  ")
	if err != nil {
		return err
	}
	// Tokens live in this file: keep it owner-only.
	if err := writeFileAtomic(remotesPath(root), append(b, '\n'), 0o600); err != nil {
		return err
	}
	return nil
}

func AddRemote(root, name string, remote Remote) error {
	if err := validateRefName(name); err != nil {
		return fmt.Errorf("invalid remote name: %w", err)
	}
	remotes, err := LoadRemotes(root)
	if err != nil {
		return err
	}
	remotes[name] = remote
	return saveRemotes(root, remotes)
}

func RemoveRemote(root, name string) error {
	remotes, err := LoadRemotes(root)
	if err != nil {
		return err
	}
	if _, ok := remotes[name]; !ok {
		return fmt.Errorf("remote not found: %s", name)
	}
	delete(remotes, name)
	return saveRemotes(root, remotes)
}

// GetRemote resolves a remote by name; with an empty name it returns the sole
// configured remote, or fails when there are zero or several.
func GetRemote(root, name string) (Remote, error) {
	remotes, err := LoadRemotes(root)
	if err != nil {
		return Remote{}, err
	}
	if name != "" {
		r, ok := remotes[name]
		if !ok {
			return Remote{}, fmt.Errorf("remote not found: %s", name)
		}
		return r, nil
	}
	switch len(remotes) {
	case 0:
		return Remote{}, errors.New("no remotes configured (run: fvs2 remote add <name> <url>)")
	case 1:
		for _, r := range remotes {
			return r, nil
		}
	}
	names := make([]string, 0, len(remotes))
	for n := range remotes {
		names = append(names, n)
	}
	sort.Strings(names)
	return Remote{}, fmt.Errorf("several remotes configured (%v); pass one with --remote", names)
}
