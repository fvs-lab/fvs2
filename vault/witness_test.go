package vault

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"
)

func TestCosignatureRoundTrip(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	sth := SignedTreeHead{TreeSize: 7, RootHash: "abcd", Timestamp: 100}
	c := CosignSTH(priv, sth)
	if !VerifyCosignature(c) {
		t.Fatal("valid cosignature did not verify")
	}
	c.RootHash = "ffff"
	if VerifyCosignature(c) {
		t.Fatal("tampered cosignature verified")
	}
}

func TestCheckCosignaturesDetectsEquivocation(t *testing.T) {
	pubA, privA, _ := ed25519.GenerateKey(nil)
	pubB, privB, _ := ed25519.GenerateKey(nil)
	wa, wb := hex.EncodeToString(pubA), hex.EncodeToString(pubB)
	registered := map[string]bool{wa: true, wb: true}

	honest := SignedTreeHead{TreeSize: 5, RootHash: "1111", Timestamp: 1}
	ca := CosignSTH(privA, honest)
	cb := CosignSTH(privB, honest)

	n, err := CheckCosignatures(registered, 5, "1111", []Cosignature{ca, cb})
	if err != nil || n != 2 {
		t.Fatalf("honest cosignatures: n=%d err=%v", n, err)
	}

	// Witness B is caught cosigning a different root at the same size.
	forked := SignedTreeHead{TreeSize: 5, RootHash: "2222", Timestamp: 1}
	cbFork := CosignSTH(privB, forked)
	if _, err := CheckCosignatures(registered, 5, "1111", []Cosignature{ca, cbFork}); err == nil {
		t.Fatal("equivocation was not detected")
	}

	// An unregistered witness is ignored, not trusted.
	_, privC, _ := ed25519.GenerateKey(nil)
	cc := CosignSTH(privC, honest)
	n, err = CheckCosignatures(registered, 5, "1111", []Cosignature{cc})
	if err != nil || n != 0 {
		t.Fatalf("unregistered witness counted: n=%d err=%v", n, err)
	}
}
