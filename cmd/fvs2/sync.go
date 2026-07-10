package main

import (
	"fmt"
	"os"
	"time"

	"fvs2/internal/meta"
	"fvs2/remote"
	fvsrepo "fvs2/repo"
)

type RemoteCmd struct {
	Add    RemoteAddCmd    `cmd:"add" help:"Add a remote"`
	List   RemoteListCmd   `cmd:"list" help:"List remotes"`
	Remove RemoteRemoveCmd `cmd:"remove" help:"Remove a remote"`
	Gc     RemoteGcCmd     `cmd:"gc" help:"Run garbage collection on a remote (admin)"`

	Root *CLI `internal:"ignore"`
}

type RemoteGcCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Grace  int    `cli:"grace" default:"3600" help:"keep objects newer than this many seconds"`
	Root   *CLI   `internal:"ignore"`
}

func (c *RemoteGcCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	rm, err := meta.GetRemote(root, c.Remote)
	if err != nil {
		return err
	}
	client := remote.NewClient(rm.URL, rm.Token)
	res, err := client.GC(time.Duration(c.Grace) * time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: removed %d blocks (%d bytes) and %d states on the remote\n",
		res.RemovedBlocks, res.FreedBytes, res.RemovedStates)
	return nil
}

type RemoteAddCmd struct {
	Token string `cli:"token" help:"bearer token for the remote"`
	Name  string `arg:"" required:"true" help:"remote name"`
	URL   string `arg:"" required:"true" help:"remote base URL (e.g. https://host:8040)"`
	Root  *CLI   `internal:"ignore"`
}

func (c *RemoteAddCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.AddRemote(root, c.Name, meta.Remote{URL: c.URL, Token: c.Token}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: remote %s -> %s\n", c.Name, c.URL)
	return nil
}

type RemoteListCmd struct {
	Root *CLI `internal:"ignore"`
}

func (c *RemoteListCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	remotes, err := meta.LoadRemotes(root)
	if err != nil {
		return err
	}
	if len(remotes) == 0 {
		fmt.Fprintln(os.Stdout, "(no remotes)")
		return nil
	}
	for name, r := range remotes {
		fmt.Fprintf(os.Stdout, "%s  %s\n", name, r.URL)
	}
	return nil
}

type RemoteRemoveCmd struct {
	Name string `arg:"" required:"true" help:"remote name"`
	Root *CLI   `internal:"ignore"`
}

func (c *RemoteRemoveCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.RemoveRemote(root, c.Name); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: remote %s removed\n", c.Name)
	return nil
}

type PushCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Branch string `cli:"branch" help:"branch to push (default: current)"`
	Force  bool   `cli:"force" help:"push even if the remote points at a state unknown here"`
	Root   *CLI   `internal:"ignore"`
}

func (c *PushCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	rm, err := meta.GetRemote(root, c.Remote)
	if err != nil {
		return err
	}
	res, err := fvsrepo.Push(root, rm, c.Branch, c.Force)
	if err != nil {
		return err
	}
	if res.UploadedBlocks == 0 {
		fmt.Fprintf(os.Stdout, "ok: %s is up to date at %.12s (%d blocks already on the remote)\n",
			res.Branch, res.StateID, res.TotalBlocks)
		return nil
	}
	fmt.Fprintf(os.Stdout, "ok: pushed %s at %.12s (%d/%d blocks uploaded)\n",
		res.Branch, res.StateID, res.UploadedBlocks, res.TotalBlocks)
	return nil
}

type PullCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Branch string `cli:"branch" help:"branch to pull (default: current)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *PullCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	rm, err := meta.GetRemote(root, c.Remote)
	if err != nil {
		return err
	}
	res, err := fvsrepo.Pull(root, rm, c.Branch)
	if err != nil {
		return err
	}
	if res.UpToDate {
		fmt.Fprintf(os.Stdout, "ok: %s is up to date at %.12s\n", res.Branch, res.StateID)
		return nil
	}
	fmt.Fprintf(os.Stdout, "ok: pulled %s at %.12s (%d/%d blocks downloaded); restore it with: fvs2 restore -s %.12s\n",
		res.Branch, res.StateID, res.DownloadedBlocks, res.TotalBlocks, res.StateID)
	return nil
}

type ServeCmd struct {
	RootDir string `cli:"root" help:"directory backing the remote (default: --path)"`
	Addr    string `cli:"addr" default:"127.0.0.1:8040" help:"listen address"`
	Token   string `cli:"token" help:"single admin account with this bearer token"`
	Users   string `cli:"users" help:"JSON file with per-user accounts (name, token, quota_bytes, admin)"`
	Root    *CLI   `internal:"ignore"`
}

func (c *ServeCmd) Run() error {
	dir := c.RootDir
	if dir == "" {
		dir = c.Root.Path
	}
	dir, err := absClean(dir)
	if err != nil {
		return err
	}
	var server *remote.Server
	if c.Users != "" {
		users, err := remote.LoadUsers(c.Users)
		if err != nil {
			return err
		}
		server, err = remote.NewServerWithUsers(dir, users)
		if err != nil {
			return err
		}
	} else {
		server, err = remote.NewServer(dir, c.Token)
		if err != nil {
			return err
		}
	}
	return server.ListenAndServe(c.Addr)
}
