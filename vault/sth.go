package vault

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
)

// sthDomain separates tree-head signatures from every other Ed25519 signature
// in the system.
const sthDomain = "FVS-VAULT-STH-V1\n"

// SignedTreeHead commits the hub to a tree of a given size and root at a
// point in time.
type SignedTreeHead struct {
	V         int    `json:"v"`
	LogID     string `json:"log_id"`
	KeyID     string `json:"key_id"`
	TreeSize  uint64 `json:"tree_size"`
	RootHash  string `json:"root_hash"`
	Timestamp int64  `json:"timestamp"`
	Sig       string `json:"sig"`
}

// unsigned is the canonical payload the signature covers.
func (s SignedTreeHead) unsigned() []byte {
	s.Sig = ""
	b, _ := json.Marshal(s)
	return append([]byte(sthDomain), b...)
}

// SignSTH produces a signed tree head over root at treeSize.
func SignSTH(priv ed25519.PrivateKey, logID, keyID, root string, treeSize uint64, at int64) SignedTreeHead {
	s := SignedTreeHead{
		V: 1, LogID: logID, KeyID: keyID, TreeSize: treeSize,
		RootHash: root, Timestamp: at,
	}
	sig := ed25519.Sign(priv, s.unsigned())
	s.Sig = hex.EncodeToString(sig)
	return s
}

// VerifySTH checks a tree head's signature against a public key.
func VerifySTH(pub ed25519.PublicKey, s SignedTreeHead) bool {
	sig, err := hex.DecodeString(s.Sig)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, s.unsigned(), sig)
}
