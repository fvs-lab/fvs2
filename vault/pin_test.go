package vault

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"testing"
)

func leaves(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = LeafHashHex([]byte(fmt.Sprintf("leaf-%d", i)))
	}
	return out
}

func headAt(t *testing.T, priv ed25519.PrivateKey, logID string, ls []string, size int) SignedTreeHead {
	t.Helper()
	root, ok := Root(ls[:size])
	if !ok {
		t.Fatalf("root at %d", size)
	}
	return SignSTH(priv, logID, "2026", root, uint64(size), int64(size))
}

func TestReconcileAdvancesOnConsistentGrowth(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	key := LogKey{LogID: "log-1", KeyID: "2026", PublicKey: hex.EncodeToString(pub)}
	ls := leaves(16)

	// Trust on first use at size 4.
	first := headAt(t, priv, key.LogID, ls, 4)
	pin, err := Reconcile(nil, "hub:1", key, first, nil)
	if err != nil {
		t.Fatalf("tofu: %v", err)
	}
	if pin.TreeSize != 4 {
		t.Fatalf("pin size = %d", pin.TreeSize)
	}

	// Advance to size 11 with a real consistency proof.
	next := headAt(t, priv, key.LogID, ls, 11)
	pin2, err := Reconcile(&pin, "hub:1", key, next, func(f uint64) ([]string, error) {
		p, _ := ConsistencyProof(int(f), ls[:11])
		return p, nil
	})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if pin2.TreeSize != 11 {
		t.Fatalf("advanced size = %d", pin2.TreeSize)
	}
}

func TestReconcileRejectsEquivocation(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	key := LogKey{LogID: "log-1", KeyID: "2026", PublicKey: hex.EncodeToString(pub)}

	honest := leaves(8)
	pin, err := Reconcile(nil, "hub:1", key, headAt(t, priv, key.LogID, honest, 4), nil)
	if err != nil {
		t.Fatal(err)
	}

	// A forked history: same size 4 pinned, but the hub now signs a tree whose
	// first 4 leaves differ. A consistency proof cannot bridge them.
	forked := leaves(8)
	forked[2] = LeafHashHex([]byte("tampered"))
	forkedHead := headAt(t, priv, key.LogID, forked, 8)
	_, err = Reconcile(&pin, "hub:1", key, forkedHead, func(f uint64) ([]string, error) {
		p, _ := ConsistencyProof(int(f), forked[:8])
		return p, nil
	})
	if err == nil {
		t.Fatal("forked history was accepted as consistent")
	}
}

func TestReconcileRejectsRegressionAndKeySwap(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	key := LogKey{LogID: "log-1", KeyID: "2026", PublicKey: hex.EncodeToString(pub)}
	ls := leaves(8)
	pin, _ := Reconcile(nil, "hub:1", key, headAt(t, priv, key.LogID, ls, 6), nil)

	// A smaller tree is a rollback.
	if _, err := Reconcile(&pin, "hub:1", key, headAt(t, priv, key.LogID, ls, 3), nil); err == nil {
		t.Fatal("regression accepted")
	}

	// A different key for the same log is refused.
	pub2, priv2, _ := ed25519.GenerateKey(nil)
	key2 := LogKey{LogID: "log-1", KeyID: "2026", PublicKey: hex.EncodeToString(pub2)}
	if _, err := Reconcile(&pin, "hub:1", key2, headAt(t, priv2, key2.LogID, ls, 8), nil); err == nil {
		t.Fatal("key swap accepted")
	}
}
