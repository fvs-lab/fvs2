package main

import (
	"fmt"
	"os"
	"strings"
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
	User   RemoteUserCmd   `cmd:"user" help:"Manage accounts on a remote (admin)"`

	Root *CLI `internal:"ignore"`
}

type RemoteUserCmd struct {
	Add    RemoteUserAddCmd    `cmd:"add" help:"Create an account on a remote"`
	List   RemoteUserListCmd   `cmd:"list" help:"List accounts on a remote"`
	Remove RemoteUserRemoveCmd `cmd:"remove" help:"Delete an account on a remote"`

	Root *CLI `internal:"ignore"`
}

func remoteClient(root, name string) (*remote.Client, error) {
	rm, err := meta.GetRemote(root, name)
	if err != nil {
		return nil, err
	}
	return remote.NewClientNS(rm.URL, rm.Token, rm.Namespace), nil
}

type RemoteUserAddCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Token  string `cli:"token" required:"true" help:"bearer token for the new account"`
	Quota  int64  `cli:"quota" help:"quota in bytes (0 = unlimited)"`
	Admin  bool   `cli:"admin" help:"grant admin rights"`
	Teams  string `cli:"teams" help:"comma-separated team namespaces to grant"`
	Name   string `arg:"" required:"true" help:"account name"`
	Root   *CLI   `internal:"ignore"`
}

func (c *RemoteUserAddCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	var teams []string
	if c.Teams != "" {
		teams = strings.Split(c.Teams, ",")
	}
	if err := client.AddUser(remote.User{Name: c.Name, Token: c.Token, QuotaBytes: c.Quota, Admin: c.Admin, Teams: teams}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: account %s created\n", c.Name)
	return nil
}

type RemoteUserListCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Root   *CLI   `internal:"ignore"`
}

func (c *RemoteUserListCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	users, err := client.ListUsers()
	if err != nil {
		return err
	}
	if len(users) == 0 {
		fmt.Fprintln(os.Stdout, "(no accounts)")
		return nil
	}
	for _, u := range users {
		flags := ""
		if u.Admin {
			flags = " admin"
		}
		if len(u.Teams) > 0 {
			flags += " teams=" + strings.Join(u.Teams, ",")
		}
		fmt.Fprintf(os.Stdout, "%-16s quota=%d%s\n", u.Name, u.QuotaBytes, flags)
	}
	return nil
}

type RemoteUserRemoveCmd struct {
	Remote string `cli:"remote" help:"remote name (default: the only one configured)"`
	Name   string `arg:"" required:"true" help:"account name"`
	Root   *CLI   `internal:"ignore"`
}

func (c *RemoteUserRemoveCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	client, err := remoteClient(root, c.Remote)
	if err != nil {
		return err
	}
	if err := client.RemoveUser(c.Name); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: account %s removed\n", c.Name)
	return nil
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
	Token     string `cli:"token" help:"bearer token for the remote"`
	Namespace string `cli:"namespace" help:"ref namespace to push under (a team you belong to)"`
	Name      string `arg:"" required:"true" help:"remote name"`
	URL       string `arg:"" required:"true" help:"remote base URL (e.g. https://host:8040)"`
	Root      *CLI   `internal:"ignore"`
}

func (c *RemoteAddCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.AddRemote(root, c.Name, meta.Remote{URL: c.URL, Token: c.Token, Namespace: c.Namespace}); err != nil {
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
	RootDir  string `cli:"root" help:"directory backing the remote (default: --path)"`
	Addr     string `cli:"addr" default:"127.0.0.1:8040" help:"listen address"`
	Token    string `cli:"token" help:"single admin account with this bearer token"`
	Accounts string `cli:"accounts" help:"JSON file of accounts; managed at runtime and persisted"`
	Audit    string `cli:"audit" help:"append-only audit log file"`
	Rate     int    `cli:"rate" help:"per-account request rate limit (req/s, 0 = unlimited)"`
	Burst    int    `cli:"burst" default:"64" help:"rate limiter burst"`
	TLSCert  string `cli:"tls-cert" help:"TLS certificate file (enables HTTPS)"`
	TLSKey   string `cli:"tls-key" help:"TLS private key file"`
	CORS     string `cli:"cors-origin" help:"allow browser clients from this origin (e.g. https://ui.example.org, or *)"`

	S3Endpoint string `cli:"s3-endpoint" help:"S3 endpoint (host:port); stores blocks in S3 instead of the local disk"`
	S3Bucket   string `cli:"s3-bucket" help:"S3 bucket"`
	S3Key      string `cli:"s3-access-key" help:"S3 access key"`
	S3Secret   string `cli:"s3-secret-key" help:"S3 secret key"`
	S3Region   string `cli:"s3-region" help:"S3 region"`
	S3Prefix   string `cli:"s3-prefix" help:"key prefix inside the bucket"`
	S3SSL      bool   `cli:"s3-ssl" help:"use TLS to reach the S3 endpoint"`

	Root *CLI `internal:"ignore"`
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

	cfg := remote.Config{
		Root:         dir,
		AccountsFile: c.Accounts,
		AuditFile:    c.Audit,
		RatePerSec:   float64(c.Rate),
		RateBurst:    c.Burst,
		CORSOrigin:   c.CORS,
	}
	if c.Accounts == "" && c.Token != "" {
		cfg.Users = []remote.User{{Name: "default", Token: c.Token, Admin: true}}
	}
	if c.S3Endpoint != "" {
		backend, err := remote.NewS3Backend(remote.S3Config{
			Endpoint:  c.S3Endpoint,
			Bucket:    c.S3Bucket,
			AccessKey: c.S3Key,
			SecretKey: c.S3Secret,
			Region:    c.S3Region,
			Prefix:    c.S3Prefix,
			UseSSL:    c.S3SSL,
		})
		if err != nil {
			return err
		}
		cfg.Blocks = backend
	}

	server, err := remote.NewServerConfig(cfg)
	if err != nil {
		return err
	}
	defer server.Close()

	if c.Accounts == "" && c.Token == "" {
		fmt.Fprintf(os.Stderr, "warning: no accounts and no --token: every caller is an unauthenticated admin. Use --token or --accounts, and bind to a loopback or trusted address.\n")
	}

	if c.TLSCert != "" || c.TLSKey != "" {
		if c.TLSCert == "" || c.TLSKey == "" {
			return fmt.Errorf("both --tls-cert and --tls-key are required for HTTPS")
		}
		return server.ListenAndServeTLS(c.Addr, c.TLSCert, c.TLSKey)
	}
	return server.ListenAndServe(c.Addr)
}
