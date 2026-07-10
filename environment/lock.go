package environment

import (
	"encoding/json"
	"fmt"
	"os"
)

// LockedLayer pins one layer to a concrete state.
type LockedLayer struct {
	Name    string `json:"name"`
	Repo    string `json:"repo"`
	StateID string `json:"state_id"`
}

// Lock is the resolved, reproducible form of a manifest: the same lockfile
// resolves to the same states on any machine.
type Lock struct {
	Name   string        `json:"name"`
	Layers []LockedLayer `json:"layers"`
	Upper  string        `json:"upper,omitempty"`
	Mount  string        `json:"mount,omitempty"`
}

func LoadLock(path string) (Lock, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Lock{}, err
	}
	var l Lock
	if err := json.Unmarshal(b, &l); err != nil {
		return Lock{}, fmt.Errorf("%s: %w", path, err)
	}
	return l, nil
}

func (l Lock) Save(path string) error {
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// PlanLayer is one entry of a mount plan: a repo and the exact state to mount.
type PlanLayer struct {
	Name    string
	Repo    string
	StateID string
}

// Plan turns a lockfile into an ordered mount plan with absolute repo paths,
// ready to hand to fvs2d's CreateMount. baseDir is the lockfile's directory.
func (l Lock) Plan(baseDir string) []PlanLayer {
	out := make([]PlanLayer, 0, len(l.Layers))
	for _, layer := range l.Layers {
		out = append(out, PlanLayer{
			Name:    layer.Name,
			Repo:    absRepo(baseDir, layer.Repo),
			StateID: layer.StateID,
		})
	}
	return out
}
