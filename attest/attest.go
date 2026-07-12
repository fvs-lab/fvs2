// Package attest is the signing layer of FVS: detached cryptographic
// attestations over content-addressed state IDs. States never change, and
// they carry no author; instead any key may sign any state at any time, and
// many keys may sign the same state with different roles (author, approve,
// review, release). Signatures live beside states, never inside them, so the
// content hash stays sacred. This is what makes attribution portable across
// remotes, multi-party, and retroactive.
package attest

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zeebo/blake3"
)

// Role is what a signature asserts about a state. A signature proves the key
// asserted this role over the state, not that the key authored the content.
type Role string

const (
	RoleAuthor  Role = "author"  // "I am the author of this state"
	RoleApprove Role = "approve" // a signed review approval
	RoleReject  Role = "reject"  // a signed review rejection
	RoleReview  Role = "review"  // a signed review with no verdict
	RoleRelease Role = "release" // a signed release marker
)

// KnownRole reports whether r is one of the built-in roles. Custom roles are
// allowed but flagged, so a policy engine can decide how to weigh them.
func KnownRole(r Role) bool {
	switch r {
	case RoleAuthor, RoleApprove, RoleReject, RoleReview, RoleRelease:
		return true
	}
	return false
}

// Version is the attestation envelope version.
const Version = 1

// Payload is the canonical, signed content of an attestation. Field order is
// the wire order; json.Marshal of a struct is deterministic, so the bytes are
// stable and safe to sign and re-verify.
type Payload struct {
	V        int    `json:"v"`
	State    string `json:"state"` // 64-hex content-addressed state id
	Role     Role   `json:"role"`
	Signer   string `json:"signer"` // ed25519 public key, hex (64 chars)
	SignedAt int64  `json:"signed_at"`
	Repo     string `json:"repo,omitempty"` // optional namespace/name hint
	Prev     string `json:"prev,omitempty"` // optional lineage: the parent state
	Note     string `json:"note,omitempty"` // optional short human note
}

// Attestation is a payload plus its detached signature.
type Attestation struct {
	Payload
	Sig string `json:"sig"` // ed25519 signature over canonical(Payload), hex
}

var hexRx = func() func(string, int) bool {
	return func(s string, n int) bool {
		if len(s) != n {
			return false
		}
		_, err := hex.DecodeString(s)
		return err == nil
	}
}()

// domainTag prefixes the signed bytes so an FVS attestation signature can
// never be replayed as a signature over some other protocol's message. It is
// part of the signed input, not the stored payload.
const domainTag = "FVS-ATTESTATION-V1\n"

// canonical returns the deterministic payload bytes. json.Marshal of this
// fixed-field struct is stable (declaration order, no maps), which is the v1
// canonical form; verifiers re-marshal the same struct.
func (p Payload) canonical() []byte {
	b, _ := json.Marshal(p)
	return b
}

// signingInput is the domain-separated message that is actually signed.
func (p Payload) signingInput() []byte {
	return append([]byte(domainTag), p.canonical()...)
}

// ID content-addresses the full attestation (payload + signature) so
// attestations dedup and are addressable like everything else in FVS.
func (a Attestation) ID() string {
	h := blake3.New()
	h.Write(a.Payload.canonical())
	h.Write([]byte(a.Sig))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:32])
}

// Key is an Ed25519 identity. The public half is the identity fingerprint;
// the secret half stays with the user, and losing it means re-signing.
type Key struct {
	priv ed25519.PrivateKey
}

// GenerateKey creates a fresh Ed25519 identity.
func GenerateKey() (Key, error) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return Key{}, err
	}
	return Key{priv: priv}, nil
}

// KeyFromSeedHex loads a key from its 32-byte seed in hex.
func KeyFromSeedHex(seedHex string) (Key, error) {
	seed, err := hex.DecodeString(strings.TrimSpace(seedHex))
	if err != nil {
		return Key{}, fmt.Errorf("bad key hex: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return Key{}, fmt.Errorf("key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return Key{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// SeedHex returns the secret seed in hex, the durable form to store at 0600.
func (k Key) SeedHex() string {
	return hex.EncodeToString(k.priv.Seed())
}

// Public returns the public key fingerprint in hex.
func (k Key) Public() string {
	pub := k.priv.Public().(ed25519.PublicKey)
	return hex.EncodeToString(pub)
}

// Sign fills the signer and timestamp (if unset) and produces a detached
// attestation over the given payload.
func (k Key) Sign(p Payload) (Attestation, error) {
	if !hexRx(p.State, 64) {
		return Attestation{}, errors.New("state must be a 64-hex id")
	}
	if p.Role == "" {
		return Attestation{}, errors.New("role required")
	}
	p.V = Version
	p.Signer = k.Public()
	if p.SignedAt == 0 {
		p.SignedAt = time.Now().UTC().Unix()
	}
	sig := ed25519.Sign(k.priv, p.signingInput())
	return Attestation{Payload: p, Sig: hex.EncodeToString(sig)}, nil
}

// Verify checks that the attestation is well-formed and its signature is
// valid for the public key it names. It proves the signer asserted this role
// over this state; it does not prove the signer authored the content.
func Verify(a Attestation) error {
	if a.V != Version {
		return fmt.Errorf("unsupported attestation version %d", a.V)
	}
	if !hexRx(a.State, 64) {
		return errors.New("state must be a 64-hex id")
	}
	if !hexRx(a.Signer, 64) {
		return errors.New("signer must be a 64-hex public key")
	}
	if a.Role == "" {
		return errors.New("role required")
	}
	pub, err := hex.DecodeString(a.Signer)
	if err != nil {
		return fmt.Errorf("bad signer: %w", err)
	}
	sig, err := hex.DecodeString(a.Sig)
	if err != nil {
		return fmt.Errorf("bad signature: %w", err)
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), a.Payload.signingInput(), sig) {
		return errors.New("signature does not verify")
	}
	return nil
}

// ParseAttestation decodes and validates an attestation from JSON bytes.
func ParseAttestation(b []byte) (Attestation, error) {
	var a Attestation
	if err := json.Unmarshal(b, &a); err != nil {
		return Attestation{}, err
	}
	if err := Verify(a); err != nil {
		return Attestation{}, err
	}
	return a, nil
}

// Encode serializes an attestation to canonical JSON.
func (a Attestation) Encode() []byte {
	b, _ := json.Marshal(a)
	return b
}
