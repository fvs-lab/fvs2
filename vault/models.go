package vault

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// LogKey is the transparency log's published identity: callers pin it and
// verify every tree head and proof against it.
type LogKey struct {
	LogID     string `json:"log_id"`
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
}

func (k LogKey) pub() (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(k.PublicKey)
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, errors.New("log key is not a valid ed25519 public key")
	}
	return ed25519.PublicKey(b), nil
}

// Admission mirrors the hub's leaf record byte-for-byte so its canonical bytes
// re-hash to the same leaf. The field order and tags must not diverge.
type Admission struct {
	V             int    `json:"v"`
	Type          string `json:"type"`
	LogID         string `json:"log_id"`
	Repo          string `json:"repo"`
	AttestationID string `json:"attestation_id"`
	State         string `json:"state"`
	Role          string `json:"role"`
	Signer        string `json:"signer"`
	Account       string `json:"account"`
	SignedAt      int64  `json:"signed_at"`
	AdmittedAt    int64  `json:"admitted_at"`
}

func (a Admission) canonical() []byte {
	b, _ := json.Marshal(a)
	return b
}

// Anchor is the public-anchoring proof for a tree head (Bitcoin via
// OpenTimestamps); empty until the anchored tier lands.
type Anchor struct {
	Status       string `json:"status"`
	BitcoinBlock int64  `json:"bitcoin_block,omitempty"`
	ConfirmedAt  int64  `json:"confirmed_at,omitempty"`
}

// Proof is the portable evidence that an attestation is in the log.
type Proof struct {
	Admission     Admission      `json:"admission"`
	LeafIndex     uint64         `json:"leaf_index"`
	LeafHash      string         `json:"leaf_hash"`
	STH           SignedTreeHead `json:"sth"`
	InclusionPath []string       `json:"inclusion_path"`
	Anchor        *Anchor        `json:"anchor,omitempty"`
}

// Verify checks a proof end to end against a log public key: the tree head is
// signed by the log, the admission re-hashes to the claimed leaf, and the leaf
// is included under the signed root.
func (p Proof) Verify(key LogKey) error {
	pub, err := key.pub()
	if err != nil {
		return err
	}
	if p.Admission.LogID != key.LogID {
		return errors.New("proof is for a different log")
	}
	if !VerifySTH(pub, p.STH) {
		return errors.New("signed tree head signature is invalid")
	}
	if LeafHashHex(p.Admission.canonical()) != p.LeafHash {
		return errors.New("leaf hash does not match the admission record")
	}
	if !VerifyInclusion(int(p.LeafIndex), int(p.STH.TreeSize), p.LeafHash, p.STH.RootHash, p.InclusionPath) {
		return errors.New("inclusion proof does not verify against the signed root")
	}
	return nil
}
