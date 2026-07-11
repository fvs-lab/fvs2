package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"
	fvsrepo "fvs2/repo"

	clibuilder "github.com/mirkobrombin/go-cli-builder/v2/pkg/cli"
)

type CLI struct {
	Path string `cli:"path" default:"." help:"target directory (repo root)"`

	Init     InitCmd     `cmd:"init" help:"Initialize a directory for versioning"`
	Commit   CommitCmd   `cmd:"commit" help:"Create a new state (snapshot)"`
	States   StatesCmd   `cmd:"states" help:"List saved states"`
	Drop     DropCmd     `cmd:"drop" help:"Delete a state (snapshot)"`
	Gc       GcCmd       `cmd:"gc" help:"Remove unreferenced blocks and orphan states from the store"`
	Restore  RestoreCmd  `cmd:"restore" help:"Restore a state into a directory"`
	Branch   BranchCmd   `cmd:"branch" help:"Manage branches"`
	Checkout CheckoutCmd `cmd:"checkout" help:"Update HEAD to a branch or a commit (detached)"`
	Status   StatusCmd   `cmd:"status" help:"Show HEAD, active branch, and dirty state"`
	Remote   RemoteCmd   `cmd:"remote" help:"Manage remotes"`
	Push     PushCmd     `cmd:"push" help:"Upload a branch head to a remote"`
	Pull     PullCmd     `cmd:"pull" help:"Download a branch head from a remote"`
	Serve    ServeCmd    `cmd:"serve" help:"Serve a directory as an FVS remote"`
	Env      EnvCmd      `cmd:"env" help:"Compose reproducible multi-layer environments"`

	clibuilder.Base
}

func (c *CLI) Before() error {
	c.Init.Root = c
	c.Commit.Root = c
	c.States.Root = c
	c.Drop.Root = c
	c.Gc.Root = c
	c.Restore.Root = c
	c.Branch.Root = c
	c.Branch.List.Root = c
	c.Branch.Create.Root = c
	c.Branch.Delete.Root = c
	c.Checkout.Root = c
	c.Status.Root = c
	c.Remote.Root = c
	c.Remote.Add.Root = c
	c.Remote.List.Root = c
	c.Remote.Remove.Root = c
	c.Remote.Gc.Root = c
	c.Remote.User.Root = c
	c.Remote.User.Add.Root = c
	c.Remote.User.List.Root = c
	c.Remote.User.Remove.Root = c
	c.Push.Root = c
	c.Pull.Root = c
	c.Serve.Root = c
	c.Env.Root = c
	c.Env.Lock.Root = c
	c.Env.Verify.Root = c
	c.Env.Sync.Root = c
	c.Env.Plan.Root = c
	return nil
}

type InitCmd struct {
	BlockSize int  `cli:"block-size" default:"4096" help:"block size in bytes"`
	Format    int  `cli:"format" default:"3" help:"on-disk format (2 = legacy inline metadata)"`
	Root      *CLI `internal:"ignore"`
}

func (c *InitCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	repository, err := fvsrepo.InitFormat(root, c.BlockSize, c.Format)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: initialized %s\n", repository.Path)
	return nil
}

type CommitCmd struct {
	Message    string `cli:"message,m" help:"commit message"`
	Verbose    bool   `cli:"verbose,v" help:"print verbose logs"`
	AllowEmpty bool   `cli:"allow-empty" help:"create a state even if nothing changed"`
	Root       *CLI   `internal:"ignore"`
}

func (c *CommitCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	var verbose io.Writer
	if c.Verbose {
		verbose = os.Stdout
	}
	result, err := fvsrepo.Commit(root, c.Message, c.AllowEmpty, verbose)
	if err != nil {
		return err
	}
	if !result.Created {
		fmt.Fprintln(os.Stdout, "nothing to commit, working tree clean")
		return nil
	}
	fmt.Fprintf(os.Stdout, "ok: commit %s (%d files)\n", result.StateID[:12], result.FileCount)
	return nil
}

type StatesCmd struct {
	Root *CLI `internal:"ignore"`
}

func (c *StatesCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	idx, err := meta.LoadIndex(root)
	if err != nil {
		return err
	}
	if len(idx.Commits) == 0 {
		fmt.Fprintln(os.Stdout, "(no states)")
		return nil
	}
	sort.Slice(idx.Commits, func(i, j int) bool { return idx.Commits[i].TimeUTC > idx.Commits[j].TimeUTC })
	for _, c := range idx.Commits {
		ts := time.Unix(c.TimeUTC, 0).UTC().Format(time.RFC3339)
		msg := strings.TrimSpace(c.Message)
		if msg == "" {
			msg = "(no message)"
		}
		fmt.Fprintf(os.Stdout, "%s  %s  %s\n", c.ID[:12], ts, msg)
	}
	return nil
}

type RestoreCmd struct {
	State   string `cli:"state,s" required:"true" help:"state id (full or prefix)"`
	To      string `cli:"to" help:"restore destination (default: --path)"`
	Reset   bool   `cli:"reset" help:"reset HEAD to the restored commit"`
	Clean   bool   `cli:"clean" help:"remove files in the destination that are not in the state (exact checkout)"`
	Verbose bool   `cli:"verbose,v" help:"print verbose logs"`
	Root    *CLI   `internal:"ignore"`
}

func (c *RestoreCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	var verbose io.Writer
	if c.Verbose {
		verbose = os.Stdout
	}
	res, err := fvsrepo.Restore(root, c.State, fvsrepo.RestoreOptions{
		To:      c.To,
		Clean:   c.Clean,
		Reset:   c.Reset,
		Verbose: verbose,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: restored %s into %s\n", res.StateID[:12], res.Dest)
	return nil
}

type BranchCmd struct {
	List   BranchListCmd   `cmd:"list" help:"List branches"`
	Create BranchCreateCmd `cmd:"create" help:"Create a branch"`
	Delete BranchDeleteCmd `cmd:"delete" help:"Delete a branch"`

	Root *CLI `internal:"ignore"`
}

type BranchListCmd struct {
	Root *CLI `internal:"ignore"`
}

func (c *BranchListCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	bs, err := meta.ListBranches(root)
	if err != nil {
		return err
	}
	if len(bs) == 0 {
		fmt.Fprintln(os.Stdout, "(no branches)")
		return nil
	}
	for _, b := range bs {
		fmt.Fprintln(os.Stdout, b)
	}
	return nil
}

type BranchCreateCmd struct {
	Name string `arg:"" required:"true" help:"name"`
	Root *CLI   `internal:"ignore"`
}

func (c *BranchCreateCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.CreateBranch(root, c.Name); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: branch created %s\n", c.Name)
	return nil
}

type BranchDeleteCmd struct {
	Name string `arg:"" required:"true" help:"name"`
	Root *CLI   `internal:"ignore"`
}

func (c *BranchDeleteCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.DeleteBranch(root, c.Name); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: branch deleted %s\n", c.Name)
	return nil
}

type CheckoutCmd struct {
	Target string `arg:"" required:"true" help:"branch|commit"`
	Root   *CLI   `internal:"ignore"`
}

func (c *CheckoutCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	exists, err := meta.BranchExists(root, c.Target)
	if err != nil {
		return err
	}
	if exists {
		if err := meta.SetHeadBranch(root, c.Target); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "ok: HEAD -> branch %s\n", c.Target)
		return nil
	}
	id, err := meta.ResolveCommitID(root, c.Target)
	if err != nil {
		return err
	}
	if err := meta.SetHeadCommit(root, id); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: HEAD -> commit %s\n", id[:12])
	return nil
}

type StatusCmd struct {
	CheckDirty bool `cli:"check-dirty" help:"perform expensive hashing to detect changed files"`
	Root       *CLI `internal:"ignore"`
}

func (c *StatusCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	h, err := meta.GetHead(root)
	if err != nil {
		return err
	}
	headCommit, _ := meta.ResolveHeadCommit(root)

	if h.Type == "branch" {
		fmt.Fprintf(os.Stdout, "head_type=branch\n")
		fmt.Fprintf(os.Stdout, "branch=%s\n", h.Name)
	} else {
		fmt.Fprintf(os.Stdout, "head_type=commit\n")
		fmt.Fprintf(os.Stdout, "detached=true\n")
	}
	if headCommit != "" {
		fmt.Fprintf(os.Stdout, "head_commit=%s\n", headCommit)
	} else {
		fmt.Fprintf(os.Stdout, "head_commit=\n")
	}

	dirty := false
	changed := 0
	if c.CheckDirty {
		var derr error
		dirty, changed, derr = computeDirty(root, headCommit)
		if derr != nil {
			return derr
		}
	}
	fmt.Fprintf(os.Stdout, "dirty=%v\n", dirty)
	fmt.Fprintf(os.Stdout, "changed_files=%d\n", changed)
	return nil
}

func main() {
	app := &CLI{}
	a, err := clibuilder.New(app)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(1)
	}
	a.SetName("fvs2")
	a.RootNode.Description = "FVS v2 standalone CLI"

	if err := a.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(1)
	}
}

func absClean(p string) (string, error) {
	if p == "" {
		p = "."
	}
	a, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return filepath.Clean(a), nil
}

// errFileVanished signals that a file disappeared between directory walk and
// open (a benign race). It is distinct from a legitimately empty file, which
// must still be recorded.
var errFileVanished = errors.New("file vanished")

func computeDirty(root, headCommit string) (bool, int, error) {
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return false, 0, err
	}
	// No head commit => treat as dirty if there are any files.
	if headCommit == "" {
		files, err := snapshotIDs(root, cfg.ChunkParams())
		if err != nil {
			return false, 0, err
		}
		return len(files) > 0, len(files), nil
	}
	c, err := meta.LoadCommit(root, headCommit)
	if err != nil {
		return false, 0, err
	}
	// Chunk the working tree with the head commit's parameters.
	params := cfg.ChunkParams()
	if c.Format < 2 {
		params = core.FixedChunkParams(c.BlockSize)
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return false, 0, err
	}
	commitList, err := meta.CommitFiles(store, c)
	if err != nil {
		return false, 0, err
	}
	want := make(map[string]meta.FileEntry, len(commitList))
	for _, fe := range commitList {
		want[fe.Path] = fe
	}
	got, err := snapshotIDs(root, params)
	if err != nil {
		return false, 0, err
	}
	changed := 0
	seen := map[string]bool{}
	for p, g := range got {
		seen[p] = true
		w, ok := want[p]
		if !ok {
			changed++
			continue
		}
		if w.Size != g.Size || w.Mode != g.Mode || w.Link != g.Link || !equalBlocksBlockIDs(w.Blocks, g.Blocks) {
			changed++
		}
	}
	for p := range want {
		if !seen[p] {
			changed++
		}
	}
	return changed != 0, changed, nil
}

func equalBlocksBlockIDs(a []core.BlockID, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if string(a[i]) != b[i] {
			return false
		}
	}
	return true
}

type snapEntry struct {
	Path    string
	Mode    uint32
	Size    int64
	ModTime int64
	Blocks  []string
	Link    string
}

func snapshotIDs(root string, params core.ChunkParams) (map[string]snapEntry, error) {
	out := map[string]snapEntry{}
	files, err := snapshotDirectoryNoStore(root)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.Link != "" {
			out[f.Path] = snapEntry{Path: f.Path, Mode: f.Mode, ModTime: f.ModTime, Link: f.Link}
			continue
		}
		blocks, size, err := hashFileBlocks(filepath.Join(root, filepath.FromSlash(f.Path)), params)
		if err != nil {
			if errors.Is(err, errFileVanished) {
				continue
			}
			return nil, err
		}
		out[f.Path] = snapEntry{Path: f.Path, Mode: f.Mode, Size: size, ModTime: f.ModTime, Blocks: blocks}
	}
	return out, nil
}

func snapshotDirectoryNoStore(root string) ([]meta.FileEntry, error) {
	var files []meta.FileEntry
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".fvs2" || strings.HasPrefix(rel, ".fvs2"+string(os.PathSeparator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, lerr := os.Readlink(p)
			if lerr != nil {
				if os.IsNotExist(lerr) {
					return nil
				}
				return lerr
			}
			li, lerr := os.Lstat(p)
			if lerr != nil {
				if os.IsNotExist(lerr) {
					return nil
				}
				return lerr
			}
			files = append(files, meta.FileEntry{Path: filepath.ToSlash(rel), Mode: uint32(li.Mode().Perm()), ModTime: li.ModTime().Unix(), Link: target})
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		files = append(files, meta.FileEntry{Path: filepath.ToSlash(rel), Mode: uint32(info.Mode().Perm()), Size: info.Size(), ModTime: info.ModTime().Unix()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func hashFileBlocks(path string, params core.ChunkParams) ([]string, int64, error) {
	ids, _, total, err := fvsrepo.ChunkFile(path, params, func(chunk []byte) (core.BlockID, error) {
		return core.ContentID(chunk), nil
	})
	if err != nil {
		if fvsrepo.ErrFileVanished(err) {
			return nil, 0, errFileVanished
		}
		return nil, 0, err
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out, total, nil
}
