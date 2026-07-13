// Package vault is the FVS transparency log: an append-only BLAKE3 Merkle
// tree (RFC 9162 shape) whose leaves are attestation admissions. It gives
// signed tree heads, inclusion proofs and consistency proofs so the hub is
// accountable for what it admitted, without ever touching state content.
package vault

import (
	"encoding/hex"

	"github.com/zeebo/blake3"
)

// Domain-separated prefixes keep leaf and interior node hashes distinct, so a
// leaf can never be reinterpreted as an internal node (RFC 9162 section 2.1).
var (
	leafPrefix = []byte{0x00}
	nodePrefix = []byte{0x01}
)

// hashLeaf hashes raw leaf bytes.
func hashLeaf(data []byte) []byte {
	h := blake3.New()
	h.Write(leafPrefix)
	h.Write(data)
	sum := h.Sum(nil)
	return sum[:32]
}

// hashNode hashes two child hashes into their parent.
func hashNode(left, right []byte) []byte {
	h := blake3.New()
	h.Write(nodePrefix)
	h.Write(left)
	h.Write(right)
	sum := h.Sum(nil)
	return sum[:32]
}

// LeafHashHex returns the leaf hash of data in hex.
func LeafHashHex(data []byte) string {
	return hex.EncodeToString(hashLeaf(data))
}

// rootHash computes the Merkle root of the first n leaf hashes.
func rootHash(leaves [][]byte) []byte {
	n := len(leaves)
	if n == 0 {
		// Empty tree: hash of nothing, per RFC 9162.
		sum := blake3.Sum256(nil)
		return sum[:32]
	}
	if n == 1 {
		return leaves[0]
	}
	k := largestPowerOfTwoBelow(n)
	left := rootHash(leaves[:k])
	right := rootHash(leaves[k:])
	return hashNode(left, right)
}

// RootHex returns the tree root over the given leaf hashes, in hex.
func RootHex(leaves [][]byte) string {
	return hex.EncodeToString(rootHash(leaves))
}

// inclusionProof returns the audit path for leaf m among the first n leaves.
func inclusionProof(m, n int, leaves [][]byte) [][]byte {
	if m >= n {
		return nil
	}
	if n == 1 {
		return nil
	}
	k := largestPowerOfTwoBelow(n)
	if m < k {
		path := inclusionProof(m, k, leaves[:k])
		return append(path, rootHash(leaves[k:]))
	}
	path := inclusionProof(m-k, n-k, leaves[k:])
	return append(path, rootHash(leaves[:k]))
}

// verifyInclusion checks that leafHash at index m in a tree of size n has the
// given root, using the audit path. It recomputes the root the same way the
// proof was generated: the path is consumed outermost-split last.
func verifyInclusion(m, n int, leafHash, root []byte, path [][]byte) bool {
	if m < 0 || m >= n {
		return false
	}
	got, rest, ok := rebuildRoot(m, n, leafHash, path)
	return ok && len(rest) == 0 && equalHash(got, root)
}

// rebuildRoot folds the audit path back into a root, mirroring inclusionProof:
// it consumes the outermost sibling first and returns the still-unconsumed
// prefix so the caller can assert the whole path was used.
func rebuildRoot(m, n int, node []byte, path [][]byte) (root []byte, rest [][]byte, ok bool) {
	if n == 1 {
		return node, path, true
	}
	if len(path) == 0 {
		return nil, nil, false
	}
	sibling := path[len(path)-1]
	remaining := path[:len(path)-1]
	k := largestPowerOfTwoBelow(n)
	if m < k {
		left, rem, ok := rebuildRoot(m, k, node, remaining)
		if !ok {
			return nil, nil, false
		}
		return hashNode(left, sibling), rem, true
	}
	right, rem, ok := rebuildRoot(m-k, n-k, node, remaining)
	if !ok {
		return nil, nil, false
	}
	return hashNode(sibling, right), rem, true
}

// consistencyProof proves a tree of size m is a prefix of a tree of size n.
func consistencyProof(m, n int, leaves [][]byte) [][]byte {
	if m <= 0 || m > n {
		return nil
	}
	if m == n {
		return nil
	}
	return subproof(m, leaves[:n], true)
}

func subproof(m int, leaves [][]byte, b bool) [][]byte {
	n := len(leaves)
	if m == n {
		if b {
			return nil
		}
		return [][]byte{rootHash(leaves)}
	}
	k := largestPowerOfTwoBelow(n)
	if m <= k {
		path := subproof(m, leaves[:k], b)
		return append(path, rootHash(leaves[k:]))
	}
	path := subproof(m-k, leaves[k:], false)
	return append(path, rootHash(leaves[:k]))
}

// verifyConsistency checks that oldRoot (size m) is consistent with newRoot
// (size n) via the proof.
func verifyConsistency(m, n int, oldRoot, newRoot []byte, proof [][]byte) bool {
	if m <= 0 || m > n {
		return false
	}
	if m == n {
		return len(proof) == 0 && equalHash(oldRoot, newRoot)
	}
	p := proof
	if isPowerOfTwo(m) {
		p = append([][]byte{oldRoot}, p...)
	}
	if len(p) == 0 {
		return false
	}
	fn, sn := m-1, n-1
	for fn%2 == 1 {
		fn /= 2
		sn /= 2
	}
	fr, sr := p[0], p[0]
	for _, c := range p[1:] {
		if sn == 0 {
			return false
		}
		if fn%2 == 1 || fn == sn {
			fr = hashNode(c, fr)
			sr = hashNode(c, sr)
			for fn%2 == 0 && fn != 0 {
				fn /= 2
				sn /= 2
			}
		} else {
			sr = hashNode(sr, c)
		}
		fn /= 2
		sn /= 2
	}
	return equalHash(fr, oldRoot) && equalHash(sr, newRoot) && sn == 0
}

func largestPowerOfTwoBelow(n int) int {
	k := 1
	for k<<1 < n {
		k <<= 1
	}
	return k
}

func isPowerOfTwo(n int) bool { return n > 0 && n&(n-1) == 0 }

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func decodeHashes(hexes []string) ([][]byte, bool) {
	out := make([][]byte, len(hexes))
	for i, h := range hexes {
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 32 {
			return nil, false
		}
		out[i] = b
	}
	return out, true
}

func encodeHashes(hs [][]byte) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = hex.EncodeToString(h)
	}
	return out
}
