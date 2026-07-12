package main

import (
	"fmt"
	"os"
	"path/filepath"

	"fvs2/attest"
	"fvs2/internal/meta"
	"fvs2/repo"
)

// keyPath is where the signing identity lives by default: one identity per
// user, kept at 0600. Losing it means re-signing.
func keyPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "fvs2", "key.hex"), nil
}

func loadKey() (attest.Key, error) {
	p, err := keyPath()
	if err != nil {
		return attest.Key{}, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return attest.Key{}, fmt.Errorf("no signing identity yet: run 'fvs2 key gen'")
		}
		return attest.Key{}, err
	}
	return attest.KeyFromSeedHex(string(b))
}

// ---- key management ----

type KeyCmd struct {
	Gen  KeyGenCmd  `cmd:"gen" help:"Generate a signing identity (Ed25519)"`
	Show KeyShowCmd `cmd:"show" help:"Show your public key fingerprint"`
	Root *CLI       `internal:"ignore"`
}

type KeyGenCmd struct {
	Force bool `cli:"force" help:"overwrite an existing identity"`
	Root  *CLI `internal:"ignore"`
}

func (c *KeyGenCmd) Run() error {
	p, err := keyPath()
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); err == nil && !c.Force {
		return fmt.Errorf("an identity already exists at %s (use --force to replace, but you would have to re-sign)", p)
	}
	key, err := attest.GenerateKey()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(p, []byte(key.SeedHex()), 0o600); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "identity created: %s\npublic key: %s\nkeep the secret at %s; if you lose it you must re-sign your states\n",
		key.Public()[:16], key.Public(), p)
	return nil
}

type KeyShowCmd struct {
	Root *CLI `internal:"ignore"`
}

func (c *KeyShowCmd) Run() error {
	key, err := loadKey()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%s\n", key.Public())
	return nil
}

// ---- signing ----

type SignCmd struct {
	State string `cli:"state" help:"state id or prefix (default: current HEAD)"`
	Role  string `cli:"role" default:"author" help:"author|approve|reject|review|release (or a custom role)"`
	Note  string `cli:"note" help:"optional short note recorded in the attestation"`
	Root  *CLI   `internal:"ignore"`
}

func (c *SignCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	id, err := resolveSignTarget(root, c.State)
	if err != nil {
		return err
	}
	key, err := loadKey()
	if err != nil {
		return err
	}
	a, err := key.Sign(attest.Payload{State: id, Role: attest.Role(c.Role), Note: c.Note})
	if err != nil {
		return err
	}
	aid, err := repo.StoreAttestation(root, a)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "signed %.12s as %q by %.16s (attestation %.12s)\n", id, c.Role, key.Public(), aid)
	return nil
}

// resolveSignTarget resolves the state to sign: an explicit id/prefix, or
// HEAD when none is given.
func resolveSignTarget(root, state string) (string, error) {
	if state != "" {
		return meta.ResolveCommitID(root, state)
	}
	id, err := meta.ResolveHeadCommit(root)
	if err != nil {
		return "", fmt.Errorf("no state given and HEAD is unresolved: %w", err)
	}
	return id, nil
}

// ---- inspecting ----

type AttestCmd struct {
	Ls     AttestLsCmd     `cmd:"ls" help:"List attestations in this repo"`
	Verify AttestVerifyCmd `cmd:"verify" help:"Verify attestation signatures"`
	Root   *CLI            `internal:"ignore"`
}

type AttestLsCmd struct {
	State string `cli:"state" help:"only attestations for this state id or prefix"`
	Root  *CLI   `internal:"ignore"`
}

func (c *AttestLsCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	filter := ""
	if c.State != "" {
		if filter, err = meta.ResolveCommitID(root, c.State); err != nil {
			return err
		}
	}
	list, err := repo.LoadAttestations(root, filter)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		fmt.Fprintln(os.Stdout, "no attestations")
		return nil
	}
	for _, a := range list {
		mark := "signed"
		if attest.Verify(a) != nil {
			mark = "INVALID"
		}
		fmt.Fprintf(os.Stdout, "%.12s  %-8s  %.16s  %s  %s\n",
			a.State, a.Role, a.Signer, mark, a.Note)
	}
	return nil
}

type AttestVerifyCmd struct {
	State string `cli:"state" help:"only verify attestations for this state id or prefix"`
	Root  *CLI   `internal:"ignore"`
}

func (c *AttestVerifyCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	filter := ""
	if c.State != "" {
		if filter, err = meta.ResolveCommitID(root, c.State); err != nil {
			return err
		}
	}
	list, err := repo.LoadAttestations(root, filter)
	if err != nil {
		return err
	}
	bad := 0
	for _, a := range list {
		if err := attest.Verify(a); err != nil {
			bad++
			fmt.Fprintf(os.Stderr, "INVALID %.12s by %.16s: %v\n", a.State, a.Signer, err)
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d of %d attestations failed to verify", bad, len(list))
	}
	fmt.Fprintf(os.Stdout, "ok: %d attestations verified\n", len(list))
	return nil
}
