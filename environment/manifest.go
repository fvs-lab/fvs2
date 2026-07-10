// Package environment composes reproducible multi-layer environments from FVS
// repositories: a manifest names the layers and how to pin them, a lockfile
// records the exact states, and a plan feeds fvs2d's mount API. This is the
// building block for sharing a versioned stack (base runtime, dependencies,
// application) across a team so every machine mounts byte-identical layers.
package environment

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	ManifestName = "env.json"
	LockName     = "env.lock.json"
)

// Pull optionally refreshes a layer's repository from a remote before pinning,
// so `env lock` captures the latest shared layer and other machines fetch the
// exact pinned state.
type Pull struct {
	Remote string `json:"remote,omitempty"`
	Branch string `json:"branch,omitempty"`
}

// Layer is one lower layer of the environment, lowest first. Revision picks
// what to pin: an explicit State (id or prefix), a Branch head, or the repo
// HEAD when both are empty.
type Layer struct {
	Name   string `json:"name"`
	Repo   string `json:"repo"`
	State  string `json:"state,omitempty"`
	Branch string `json:"branch,omitempty"`
	Pull   *Pull  `json:"pull,omitempty"`
}

// Manifest declares an environment. Repo paths are relative to the manifest's
// directory.
type Manifest struct {
	Name   string  `json:"name"`
	Layers []Layer `json:"layers"`
	Upper  string  `json:"upper,omitempty"`
	Mount  string  `json:"mount,omitempty"`
}

func LoadManifest(path string) (Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

func (m Manifest) Save(path string) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func (m Manifest) validate() error {
	if len(m.Layers) == 0 {
		return fmt.Errorf("environment %q has no layers", m.Name)
	}
	seen := map[string]bool{}
	for i, l := range m.Layers {
		if l.Name == "" {
			return fmt.Errorf("layer %d has no name", i)
		}
		if seen[l.Name] {
			return fmt.Errorf("duplicate layer name %q", l.Name)
		}
		seen[l.Name] = true
		if l.Repo == "" {
			return fmt.Errorf("layer %q has no repo", l.Name)
		}
		if l.State != "" && l.Branch != "" {
			return fmt.Errorf("layer %q sets both state and branch", l.Name)
		}
	}
	return nil
}

// repoPath resolves a layer's repo against the manifest directory.
func (m Manifest) repoPath(baseDir string, l Layer) string {
	if filepath.IsAbs(l.Repo) {
		return l.Repo
	}
	return filepath.Join(baseDir, l.Repo)
}
