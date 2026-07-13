package main

import (
	"fmt"
	"os"

	"fvs2/attest"
	"fvs2/remote"
	fvsrepo "fvs2/repo"
	fvsvault "fvs2/vault"
)

// ---- key deposit ----

type KeyDepositCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *KeyDepositCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	key, err := loadKey()
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	if err := client.DepositKey(key.Public()); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "deposited public key %.16s on the remote account\n", key.Public())
	return nil
}

// ---- vault ----

type VaultCmd struct {
	Verify  VaultVerifyCmd  `cmd:"verify" help:"Verify a state's attestations against the transparency log"`
	Monitor VaultMonitorCmd `cmd:"monitor" help:"Fetch the log's tree head and check it against the pin"`
	Witness VaultWitnessCmd `cmd:"witness" help:"Register witnesses and cosign tree heads"`
	Anchor  VaultAnchorCmd  `cmd:"anchor" help:"Anchor the log's tree head to Bitcoin via OpenTimestamps"`
	Root    *CLI            `internal:"ignore"`
}

// reconcilePin fetches the log key and current tree head, checks them against
// the stored pin (trust-on-first-use, then consistency), and persists the
// advanced pin. It returns the verified log key.
func reconcilePin(client *remote.Client) (fvsvault.LogKey, error) {
	key, err := client.VaultLogKey()
	if err != nil {
		return fvsvault.LogKey{}, err
	}
	sth, err := client.VaultSTH()
	if err != nil {
		return fvsvault.LogKey{}, err
	}
	host := client.Host()
	old, err := fvsvault.LoadPin(host)
	if err != nil {
		return fvsvault.LogKey{}, err
	}
	pin, err := fvsvault.Reconcile(old, host, key, sth, func(first uint64) ([]string, error) {
		proof, _, err := client.VaultConsistency(first)
		return proof, err
	})
	if err != nil {
		return fvsvault.LogKey{}, err
	}
	if err := fvsvault.SavePin(pin); err != nil {
		return fvsvault.LogKey{}, err
	}
	return key, nil
}

type VaultVerifyCmd struct {
	State  string `cli:"state" help:"state id or prefix (default: current HEAD)"`
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *VaultVerifyCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	state, err := resolveSignTarget(root, c.State)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	key, err := reconcilePin(client)
	if err != nil {
		return err
	}
	if pin, err := fvsvault.LoadPin(client.Host()); err == nil && pin != nil {
		if err := checkWitnesses(client, *pin); err != nil {
			return err
		}
	}
	list, err := fvsrepo.LoadAttestations(root, state)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		fmt.Fprintf(os.Stdout, "no attestations for %.12s\n", state)
		return nil
	}
	guaranteed, plain, bad := 0, 0, 0
	for _, a := range list {
		if attest.Verify(a) != nil {
			bad++
			fmt.Fprintf(os.Stderr, "INVALID signature %.12s by %.16s\n", a.State, a.Signer)
			continue
		}
		proof, ok, err := client.VaultProof(a.ID())
		if err != nil {
			return err
		}
		if !ok {
			plain++
			fmt.Fprintf(os.Stdout, "%-8s %.16s  signed (not in log)\n", a.Role, a.Signer)
			continue
		}
		if err := proof.Verify(key); err != nil {
			bad++
			fmt.Fprintf(os.Stderr, "UNPROVEN %-8s %.16s: %v\n", a.Role, a.Signer, err)
			continue
		}
		guaranteed++
		if proof.Anchor != nil && proof.Anchor.Status == "confirmed" {
			fmt.Fprintf(os.Stdout, "%-8s %.16s  anchored (leaf %d, btc block %d)\n", a.Role, a.Signer, proof.LeafIndex, proof.Anchor.BitcoinBlock)
		} else {
			fmt.Fprintf(os.Stdout, "%-8s %.16s  guaranteed (leaf %d)\n", a.Role, a.Signer, proof.LeafIndex)
		}
	}
	fmt.Fprintf(os.Stdout, "%.12s: %d guaranteed, %d signed-only, %d failed\n", state, guaranteed, plain, bad)
	if bad > 0 {
		return fmt.Errorf("%d attestations failed verification", bad)
	}
	return nil
}

type VaultMonitorCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *VaultMonitorCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	if _, err := reconcilePin(client); err != nil {
		return err
	}
	pin, err := fvsvault.LoadPin(client.Host())
	if err != nil {
		return err
	}
	if err := checkWitnesses(client, *pin); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "log %s consistent at size %d\n", pin.LogID, pin.TreeSize)
	return nil
}

// ---- witnesses ----

type VaultWitnessCmd struct {
	Register VaultWitnessRegisterCmd `cmd:"register" help:"Register a witness public key (admin)"`
	Cosign   VaultWitnessCosignCmd   `cmd:"cosign" help:"Cosign the log's current tree head as a witness"`
	Root     *CLI                    `internal:"ignore"`
}

type VaultWitnessRegisterCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Label  string `cli:"label" help:"human label for the witness"`
	Public string `arg:"" required:"true" help:"witness ed25519 public key (64 hex)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *VaultWitnessRegisterCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	if err := client.RegisterWitness(c.Public, c.Label); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "registered witness %.16s\n", c.Public)
	return nil
}

type VaultWitnessCosignCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *VaultWitnessCosignCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	key, err := loadKey()
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	sth, err := client.VaultSTH()
	if err != nil {
		return err
	}
	cs := fvsvault.CosignSTH(key.Private(), sth)
	if err := client.Cosign(cs); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "cosigned tree size %d as %.16s\n", sth.TreeSize, key.Public())
	return nil
}

// checkWitnesses fetches the registered witnesses and their cosignatures at the
// pinned size and confirms they all agree on the pinned root.
func checkWitnesses(client *remote.Client, pin fvsvault.Pin) error {
	witnesses, err := client.Witnesses()
	if err != nil {
		return err
	}
	if len(witnesses) == 0 {
		return nil
	}
	registered := map[string]bool{}
	for _, wt := range witnesses {
		registered[wt.Public] = true
	}
	cosigs, err := client.Cosignatures(pin.TreeSize)
	if err != nil {
		return err
	}
	n, err := fvsvault.CheckCosignatures(registered, pin.TreeSize, pin.RootHash, cosigs)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%d of %d witnesses confirm the log at size %d\n", n, len(witnesses), pin.TreeSize)
	return nil
}

// ---- anchoring ----

type VaultAnchorCmd struct {
	Upgrade bool   `cli:"upgrade" help:"complete pending anchors against the calendars instead of submitting a new one"`
	Remote  string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root    *CLI   `internal:"ignore"`
}

func (c *VaultAnchorCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	if c.Upgrade {
		if err := client.UpgradeAnchors(); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, "requested calendar upgrade of pending anchors")
		return nil
	}
	a, err := client.AnchorHead()
	if err != nil {
		return err
	}
	if a.Status == "confirmed" {
		fmt.Fprintf(os.Stdout, "tree size %d anchored to bitcoin block %d\n", a.Size, a.Height)
	} else {
		fmt.Fprintf(os.Stdout, "tree size %d submitted to calendars (pending bitcoin confirmation)\n", a.Size)
	}
	return nil
}
