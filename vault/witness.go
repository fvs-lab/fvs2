package vault

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
)

// witnessDomain separates witness cosignatures from the log's own tree-head
// signatures and from every other signature in the system.
const witnessDomain = "FVS-VAULT-WITNESS-V1\n"

// Cosignature is an independent observer's signature over a tree head. A
// witness that has seen a size commits to its root; two witnesses committing
// to different roots at the same size is cryptographic proof the log
// equivocated.
type Cosignature struct {
	Witness   string `json:"witness"`
	TreeSize  uint64 `json:"tree_size"`
	RootHash  string `json:"root_hash"`
	Timestamp int64  `json:"timestamp"`
	Sig       string `json:"sig"`
}

// cosigInput is the canonical payload a witness signs.
func cosigInput(treeSize uint64, root string, timestamp int64) []byte {
	b, _ := json.Marshal(struct {
		TreeSize  uint64 `json:"tree_size"`
		RootHash  string `json:"root_hash"`
		Timestamp int64  `json:"timestamp"`
	}{treeSize, root, timestamp})
	return append([]byte(witnessDomain), b...)
}

// CosignSTH produces a witness cosignature over a tree head.
func CosignSTH(priv ed25519.PrivateKey, s SignedTreeHead) Cosignature {
	c := Cosignature{
		Witness:   hex.EncodeToString(priv.Public().(ed25519.PublicKey)),
		TreeSize:  s.TreeSize,
		RootHash:  s.RootHash,
		Timestamp: s.Timestamp,
	}
	c.Sig = hex.EncodeToString(ed25519.Sign(priv, cosigInput(c.TreeSize, c.RootHash, c.Timestamp)))
	return c
}

// VerifyCosignature checks a cosignature against the witness's own public key.
// It proves the witness signed this (size, root); the caller still decides
// whether that witness is trusted.
func VerifyCosignature(c Cosignature) bool {
	pub, err := hex.DecodeString(c.Witness)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	sig, err := hex.DecodeString(c.Sig)
	if err != nil {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), cosigInput(c.TreeSize, c.RootHash, c.Timestamp), sig)
}

// CheckCosignatures confirms a pinned (size, root) against registered
// witnesses' cosignatures. It returns how many distinct trusted witnesses
// confirm the root, and an error the moment a trusted witness is found to have
// cosigned a different root at that size: incontrovertible proof of a split view.
func CheckCosignatures(registered map[string]bool, size uint64, root string, cosigs []Cosignature) (int, error) {
	confirmers := map[string]bool{}
	for _, c := range cosigs {
		if c.TreeSize != size || !registered[c.Witness] || !VerifyCosignature(c) {
			continue
		}
		if c.RootHash != root {
			return 0, &Equivocation{Witness: c.Witness, Size: size, Expected: root, Got: c.RootHash}
		}
		confirmers[c.Witness] = true
	}
	return len(confirmers), nil
}

// Equivocation reports a trusted witness cosigning a root that differs from the
// one the log presented at the same size.
type Equivocation struct {
	Witness  string
	Size     uint64
	Expected string
	Got      string
}

func (e *Equivocation) Error() string {
	return "log equivocation: witness " + e.Witness[:16] + " cosigned a different root at the pinned size"
}
