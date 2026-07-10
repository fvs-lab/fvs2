package main

import (
	"fmt"
	"os"
	"path/filepath"

	"fvs2/environment"
)

type EnvCmd struct {
	Lock   EnvLockCmd   `cmd:"lock" help:"Resolve a manifest into a lockfile"`
	Verify EnvVerifyCmd `cmd:"verify" help:"Check that every pinned state is present"`
	Sync   EnvSyncCmd   `cmd:"sync" help:"Fetch pinned layer states from their remotes"`
	Plan   EnvPlanCmd   `cmd:"plan" help:"Print the ordered mount plan"`

	Root *CLI `internal:"ignore"`
}

// envFiles resolves the manifest and lockfile paths from a --file flag that
// may point at either a directory or the manifest itself.
func envFiles(file string) (baseDir, manifest, lock string, err error) {
	if file == "" {
		file = "."
	}
	abs, err := filepath.Abs(file)
	if err != nil {
		return "", "", "", err
	}
	info, err := os.Stat(abs)
	if err == nil && info.IsDir() {
		return abs, filepath.Join(abs, environment.ManifestName), filepath.Join(abs, environment.LockName), nil
	}
	dir := filepath.Dir(abs)
	return dir, abs, filepath.Join(dir, environment.LockName), nil
}

type EnvLockCmd struct {
	File string `cli:"file" help:"manifest path or directory (default: .)"`
	Root *CLI   `internal:"ignore"`
}

func (c *EnvLockCmd) Run() error {
	baseDir, manifestPath, lockPath, err := envFiles(c.File)
	if err != nil {
		return err
	}
	m, err := environment.LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	lock, err := environment.Resolve(m, baseDir)
	if err != nil {
		return err
	}
	if err := lock.Save(lockPath); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: locked %s (%d layers) -> %s\n", lock.Name, len(lock.Layers), filepath.Base(lockPath))
	for _, l := range lock.Layers {
		fmt.Fprintf(os.Stdout, "  %-16s %.12s  %s\n", l.Name, l.StateID, l.Repo)
	}
	return nil
}

type EnvVerifyCmd struct {
	File string `cli:"file" help:"manifest path or directory (default: .)"`
	Root *CLI   `internal:"ignore"`
}

func (c *EnvVerifyCmd) Run() error {
	_, _, lockPath, err := envFiles(c.File)
	if err != nil {
		return err
	}
	lock, err := environment.LoadLock(lockPath)
	if err != nil {
		return err
	}
	if err := environment.Verify(lock, filepath.Dir(lockPath)); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: %s is complete (%d layers present)\n", lock.Name, len(lock.Layers))
	return nil
}

type EnvSyncCmd struct {
	File string `cli:"file" help:"manifest path or directory (default: .)"`
	Root *CLI   `internal:"ignore"`
}

func (c *EnvSyncCmd) Run() error {
	baseDir, manifestPath, lockPath, err := envFiles(c.File)
	if err != nil {
		return err
	}
	m, err := environment.LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	lock, err := environment.LoadLock(lockPath)
	if err != nil {
		return err
	}
	if err := environment.Sync(m, lock, baseDir); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: %s synced (%d layers present)\n", lock.Name, len(lock.Layers))
	return nil
}

type EnvPlanCmd struct {
	File string `cli:"file" help:"manifest path or directory (default: .)"`
	Root *CLI   `internal:"ignore"`
}

func (c *EnvPlanCmd) Run() error {
	_, _, lockPath, err := envFiles(c.File)
	if err != nil {
		return err
	}
	lock, err := environment.LoadLock(lockPath)
	if err != nil {
		return err
	}
	plan := lock.Plan(filepath.Dir(lockPath))
	for i, l := range plan {
		fmt.Fprintf(os.Stdout, "%d  %-16s %.12s  %s\n", i, l.Name, l.StateID, l.Repo)
	}
	if lock.Upper != "" {
		fmt.Fprintf(os.Stdout, "upper  %s\n", lock.Upper)
	}
	if lock.Mount != "" {
		fmt.Fprintf(os.Stdout, "mount  %s\n", lock.Mount)
	}
	return nil
}
