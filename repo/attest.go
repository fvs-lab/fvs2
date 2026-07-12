package repo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"fvs2/attest"
)

func attestDir(root string) string {
	return filepath.Join(root, ".fvs2", "attestations")
}

// StoreAttestation writes an attestation into the repo, content-addressed by
// its id so re-signing the same thing dedups. Returns the attestation id.
func StoreAttestation(root string, a attest.Attestation) (string, error) {
	dir := attestDir(root)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	id := a.ID()
	if err := os.WriteFile(filepath.Join(dir, id+".json"), a.Encode(), 0o644); err != nil {
		return "", err
	}
	return id, nil
}

// LoadAttestations reads every attestation in the repo, optionally filtered to
// one state id.
func LoadAttestations(root, state string) ([]attest.Attestation, error) {
	entries, err := os.ReadDir(attestDir(root))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []attest.Attestation
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(attestDir(root), e.Name()))
		if err != nil {
			continue
		}
		var a attest.Attestation
		if json.Unmarshal(b, &a) != nil {
			continue
		}
		if state != "" && a.State != state {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SignedAt < out[j].SignedAt })
	return out, nil
}
