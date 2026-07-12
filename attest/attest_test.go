package attest

import (
	"strings"
	"testing"
)

const testState = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestSignAndVerify(t *testing.T) {
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	a, err := k.Sign(Payload{State: testState, Role: RoleAuthor})
	if err != nil {
		t.Fatal(err)
	}
	if a.Signer != k.Public() {
		t.Fatalf("signer mismatch")
	}
	if a.SignedAt == 0 || a.V != Version {
		t.Fatalf("envelope not filled: %+v", a.Payload)
	}
	if err := Verify(a); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Round-trips through JSON.
	parsed, err := ParseAttestation(a.Encode())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.ID() != a.ID() {
		t.Fatalf("id changed across encode")
	}
}

func TestTamperFails(t *testing.T) {
	k, _ := GenerateKey()
	a, _ := k.Sign(Payload{State: testState, Role: RoleApprove})

	// Flip the role: the signature must no longer verify.
	tampered := a
	tampered.Role = RoleReject
	if err := Verify(tampered); err == nil {
		t.Fatal("tampered role verified")
	}

	// Swap the signer to another key: must fail.
	other, _ := GenerateKey()
	forged := a
	forged.Signer = other.Public()
	if err := Verify(forged); err == nil {
		t.Fatal("forged signer verified")
	}
}

func TestKeyRoundTrip(t *testing.T) {
	k, _ := GenerateKey()
	seed := k.SeedHex()
	if len(seed) != 64 {
		t.Fatalf("seed hex len %d", len(seed))
	}
	k2, err := KeyFromSeedHex(seed)
	if err != nil {
		t.Fatal(err)
	}
	if k2.Public() != k.Public() {
		t.Fatal("reloaded key has different public")
	}
	// A signature from the reloaded key verifies.
	a, _ := k2.Sign(Payload{State: testState, Role: RoleAuthor})
	if err := Verify(a); err != nil {
		t.Fatalf("reloaded key sign/verify: %v", err)
	}
}

func TestMultiPartySameState(t *testing.T) {
	// Two identities sign the SAME state with different roles: both verify
	// and produce distinct attestation IDs. This is the multi-party,
	// retroactive property the whole design rests on.
	author, _ := GenerateKey()
	reviewer, _ := GenerateKey()
	a1, _ := author.Sign(Payload{State: testState, Role: RoleAuthor})
	a2, _ := reviewer.Sign(Payload{State: testState, Role: RoleApprove})
	if err := Verify(a1); err != nil {
		t.Fatal(err)
	}
	if err := Verify(a2); err != nil {
		t.Fatal(err)
	}
	if a1.ID() == a2.ID() {
		t.Fatal("distinct attestations share an id")
	}
}

func TestRejectsBadInput(t *testing.T) {
	k, _ := GenerateKey()
	if _, err := k.Sign(Payload{State: "short", Role: RoleAuthor}); err == nil {
		t.Fatal("accepted short state")
	}
	if _, err := k.Sign(Payload{State: testState}); err == nil {
		t.Fatal("accepted empty role")
	}
	if !KnownRole(RoleApprove) || KnownRole(Role("weird")) {
		t.Fatal("KnownRole wrong")
	}
	if strings.Contains(k.SeedHex(), " ") {
		t.Fatal("seed has whitespace")
	}
}
