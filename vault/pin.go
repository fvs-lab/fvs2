package vault

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Pin is the client's trust-on-first-use anchor for one log: the latest tree
// head it has verified. Every later head must prove consistency with it, so a
// hub that forks its history (a split view) is caught.
type Pin struct {
	Host      string `json:"host"`
	LogID     string `json:"log_id"`
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
	TreeSize  uint64 `json:"tree_size"`
	RootHash  string `json:"root_hash"`
	Sig       string `json:"sig"`
	Timestamp int64  `json:"timestamp"`
}

func pinFrom(host string, key LogKey, sth SignedTreeHead) Pin {
	return Pin{
		Host: host, LogID: key.LogID, KeyID: key.KeyID, PublicKey: key.PublicKey,
		TreeSize: sth.TreeSize, RootHash: sth.RootHash, Sig: sth.Sig, Timestamp: sth.Timestamp,
	}
}

// Reconcile validates a freshly fetched tree head against the existing pin and
// returns the pin to persist. getConsistency fetches a consistency proof from
// a prior tree size to the new head. A nil old pin is trust-on-first-use.
func Reconcile(old *Pin, host string, key LogKey, sth SignedTreeHead, getConsistency func(first uint64) ([]string, error)) (Pin, error) {
	pub, err := key.pub()
	if err != nil {
		return Pin{}, err
	}
	if sth.LogID != key.LogID {
		return Pin{}, errors.New("tree head is for a different log than the key")
	}
	if !VerifySTH(pub, sth) {
		return Pin{}, errors.New("tree head signature is invalid")
	}
	if old == nil {
		return pinFrom(host, key, sth), nil
	}
	if old.PublicKey != key.PublicKey || old.LogID != key.LogID {
		return Pin{}, errors.New("the log's identity changed since it was pinned: refusing to trust a new key")
	}
	if sth.TreeSize < old.TreeSize {
		return Pin{}, errors.New("the log regressed to a smaller size: it equivocated")
	}
	if sth.TreeSize == old.TreeSize {
		if sth.RootHash != old.RootHash {
			return Pin{}, errors.New("the log shows a different root at the pinned size: it equivocated")
		}
		return pinFrom(host, key, sth), nil
	}
	proof, err := getConsistency(old.TreeSize)
	if err != nil {
		return Pin{}, err
	}
	if !VerifyConsistency(int(old.TreeSize), int(sth.TreeSize), old.RootHash, sth.RootHash, proof) {
		return Pin{}, errors.New("the new tree head is not consistent with the pinned history: the log equivocated")
	}
	return pinFrom(host, key, sth), nil
}

func pinDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "fvs2", "vault"), nil
}

func pinPath(host string) (string, error) {
	dir, err := pinDir()
	if err != nil {
		return "", err
	}
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(host)
	return filepath.Join(dir, safe+".json"), nil
}

// LoadPin returns the stored pin for a host, or nil if none exists yet.
func LoadPin(host string) (*Pin, error) {
	p, err := pinPath(host)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var pin Pin
	if err := json.Unmarshal(b, &pin); err != nil {
		return nil, err
	}
	return &pin, nil
}

// SavePin persists a pin for a host.
func SavePin(pin Pin) error {
	dir, err := pinDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	p, err := pinPath(pin.Host)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(pin, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, b, 0o600)
}
