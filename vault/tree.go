package vault

// The exported tree API works in hex, matching the rest of the hub's on-wire
// and on-disk conventions. Callers keep the ordered list of leaf hashes; the
// functions here derive roots and proofs from it.

// Root returns the tree root over the first size leaf hashes (hex).
func Root(leafHashes []string) (string, bool) {
	hs, ok := decodeHashes(leafHashes)
	if !ok {
		return "", false
	}
	return RootHex(hs), true
}

// InclusionProof returns the audit path (hex) for the leaf at index m in a
// tree of the given leaf hashes.
func InclusionProof(m int, leafHashes []string) ([]string, bool) {
	hs, ok := decodeHashes(leafHashes)
	if !ok || m < 0 || m >= len(hs) {
		return nil, false
	}
	return encodeHashes(inclusionProof(m, len(hs), hs)), true
}

// VerifyInclusion checks a leaf against a root using an audit path, all hex.
func VerifyInclusion(m, treeSize int, leafHash, root string, path []string) bool {
	lh, err1 := decodeHash(leafHash)
	rt, err2 := decodeHash(root)
	p, ok := decodeHashes(path)
	if err1 != nil || err2 != nil || !ok {
		return false
	}
	return verifyInclusion(m, treeSize, lh, rt, p)
}

// ConsistencyProof proves the first m leaves are a prefix of the full list.
func ConsistencyProof(m int, leafHashes []string) ([]string, bool) {
	hs, ok := decodeHashes(leafHashes)
	if !ok || m < 1 || m > len(hs) {
		return nil, false
	}
	return encodeHashes(consistencyProof(m, len(hs), hs)), true
}

// VerifyConsistency checks that oldRoot (size m) is a prefix of newRoot
// (size n) via the proof, all hex.
func VerifyConsistency(m, n int, oldRoot, newRoot string, proof []string) bool {
	or, err1 := decodeHash(oldRoot)
	nr, err2 := decodeHash(newRoot)
	p, ok := decodeHashes(proof)
	if err1 != nil || err2 != nil || !ok {
		return false
	}
	return verifyConsistency(m, n, or, nr, p)
}

func decodeHash(h string) ([]byte, error) {
	hs, ok := decodeHashes([]string{h})
	if !ok {
		return nil, errBadHash
	}
	return hs[0], nil
}

var errBadHash = errHash("hash must be 32 bytes hex")

type errHash string

func (e errHash) Error() string { return string(e) }
