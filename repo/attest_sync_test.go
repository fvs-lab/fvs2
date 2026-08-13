package repo

import (
	"testing"

	"github.com/fvs-lab/fvs2/attest"
)

func TestAttestationsRoundTrip(t *testing.T) {
	_, rm := newRemote(t, "")

	src := newSyncRepo(t)
	writeSync(t, src, "a.txt", []byte("hello"))
	c, err := Commit(src, "first", false, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Sign the state as author and approve, from two identities.
	author, _ := attest.GenerateKey()
	reviewer, _ := attest.GenerateKey()
	a1, _ := author.Sign(attest.Payload{State: c.StateID, Role: attest.RoleAuthor})
	a2, _ := reviewer.Sign(attest.Payload{State: c.StateID, Role: attest.RoleApprove})
	if _, err := StoreAttestation(src, a1); err != nil {
		t.Fatal(err)
	}
	if _, err := StoreAttestation(src, a2); err != nil {
		t.Fatal(err)
	}

	push, err := Push(src, rm, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if push.Attestations != 2 {
		t.Fatalf("push should carry 2 attestations, got %d", push.Attestations)
	}

	// A fresh clone pulls the state and its attestations.
	dst := newSyncRepo(t)
	pull, err := Pull(dst, rm, "main")
	if err != nil {
		t.Fatal(err)
	}
	if pull.Attestations != 2 {
		t.Fatalf("pull should recover 2 attestations, got %d", pull.Attestations)
	}
	got, err := LoadAttestations(dst, c.StateID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("local store should hold 2 attestations, got %d", len(got))
	}
	roles := map[attest.Role]bool{}
	for _, a := range got {
		if err := attest.Verify(a); err != nil {
			t.Fatalf("pulled attestation invalid: %v", err)
		}
		roles[a.Role] = true
	}
	if !roles[attest.RoleAuthor] || !roles[attest.RoleApprove] {
		t.Fatalf("expected author and approve roles, got %v", roles)
	}
}

func TestServerRejectsForgedAttestation(t *testing.T) {
	_, rm := newRemote(t, "")
	src := newSyncRepo(t)
	writeSync(t, src, "a.txt", []byte("x"))
	c, _ := Commit(src, "first", false, nil)

	k, _ := attest.GenerateKey()
	a, _ := k.Sign(attest.Payload{State: c.StateID, Role: attest.RoleAuthor})
	other, _ := attest.GenerateKey()
	a.Signer = other.Public() // break the binding: signature no longer verifies
	if _, err := StoreAttestation(src, a); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(src, rm, "", false); err != nil {
		t.Fatal(err)
	}
	// The forged attestation must not have been stored server-side, so a pull
	// recovers none.
	dst := newSyncRepo(t)
	pull, err := Pull(dst, rm, "main")
	if err != nil {
		t.Fatal(err)
	}
	if pull.Attestations != 0 {
		t.Fatalf("forged attestation should be rejected, pulled %d", pull.Attestations)
	}
}
