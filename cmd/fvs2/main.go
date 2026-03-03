package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	core "fvs-v2-core"
	"fvs2/internal/meta"

	clibuilder "github.com/mirkobrombin/go-cli-builder/v2/pkg/cli"
	"github.com/zeebo/blake3"
)

type CLI struct {
	Path string `cli:"path" default:"." help:"target directory (repo root)"`

	Init     InitCmd     `cmd:"init" help:"Initialize a directory for versioning"`
	Commit   CommitCmd   `cmd:"commit" help:"Create a new state (snapshot)"`
	States   StatesCmd   `cmd:"states" help:"List saved states"`
	Restore  RestoreCmd  `cmd:"restore" help:"Restore a state into a directory"`
	Branch   BranchCmd   `cmd:"branch" help:"Manage branches"`
	Checkout CheckoutCmd `cmd:"checkout" help:"Update HEAD to a branch or a commit (detached)"`
	Status   StatusCmd   `cmd:"status" help:"Show HEAD, active branch, and dirty state"`
	Mount    MountCmd    `cmd:"mount" help:"Ask fvs2d (IPC) to mount a branch"`
	Unmount  UnmountCmd  `cmd:"unmount" help:"Ask fvs2d (IPC) to unmount a mountpoint"`

	clibuilder.Base
}

func (c *CLI) Before() error {
	c.Init.Root = c
	c.Commit.Root = c
	c.States.Root = c
	c.Restore.Root = c
	c.Branch.Root = c
	c.Branch.List.Root = c
	c.Branch.Create.Root = c
	c.Branch.Delete.Root = c
	c.Checkout.Root = c
	c.Status.Root = c
	return nil
}

type InitCmd struct {
	BlockSize int  `cli:"block-size" default:"4096" help:"block size in bytes"`
	Root      *CLI `internal:"ignore"`
}

func (c *InitCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	if err := meta.Init(root, c.BlockSize); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: initialized %s\n", root)
	return nil
}

type CommitCmd struct {
	Message string `cli:"message,m" help:"commit message"`
	Root    *CLI   `internal:"ignore"`
}

func (c *CommitCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}

	files, err := snapshotDirectory(root, store, cfg.BlockSize)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	id := meta.NewCommitID(now, c.Message, files)
	commit := meta.Commit{ID: id, TimeUTC: now.Unix(), Message: c.Message, BlockSize: cfg.BlockSize, Files: files}
	if err := writeCommit(root, commit); err != nil {
		return err
	}
	if err := appendIndex(root, meta.CommitSummary{ID: id, TimeUTC: commit.TimeUTC, Message: c.Message}); err != nil {
		return err
	}
	if err := meta.AdvanceHeadAfterCommit(root, id); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: commit %s (%d files)\n", id[:12], len(files))
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
	State string `cli:"state,s" required:"true" help:"state id (full or prefix)"`
	To    string `cli:"to" help:"restore destination (default: --path)"`
	Root  *CLI   `internal:"ignore"`
}

func (c *RestoreCmd) Run() error {
	root, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	id, err := meta.ResolveCommitID(root, c.State)
	if err != nil {
		return err
	}
	commit, err := meta.LoadCommit(root, id)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}

	dest := root
	if c.To != "" {
		dest, err = absClean(c.To)
		if err != nil {
			return err
		}
	}
	if err := restoreCommit(dest, store, commit); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: restored %s into %s\n", id[:12], dest)
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
	Root *CLI `internal:"ignore"`
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

	dirty, changed, derr := computeDirty(root, headCommit)
	if derr != nil {
		return derr
	}
	fmt.Fprintf(os.Stdout, "dirty=%v\n", dirty)
	fmt.Fprintf(os.Stdout, "changed_files=%d\n", changed)
	return nil
}

type MountCmd struct {
	Socket   string `cli:"socket" help:"unix socket path"`
	Readonly bool   `cli:"readonly" help:"mount read-only"`
	Branch   string `arg:"" required:"true" help:"branch"`
	Path     string `arg:"" required:"true" help:"path"`
}

func (c *MountCmd) Before() error {
	if c.Socket == "" {
		c.Socket = defaultSocketPath()
	}
	return nil
}

func (c *MountCmd) Run() error {
	resp, err := callIPC(c.Socket, rpcReq{ID: "1", Method: "Mount", Params: map[string]any{"branch": c.Branch, "mountpoint": c.Path, "readonly": c.Readonly}})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("ipc error: %d %s", resp.Error.Code, resp.Error.Message)
	}
	fmt.Fprintln(os.Stdout, "ok")
	return nil
}

type UnmountCmd struct {
	Socket string `cli:"socket" help:"unix socket path"`
	Path   string `arg:"" required:"true" help:"path"`
}

func (c *UnmountCmd) Before() error {
	if c.Socket == "" {
		c.Socket = defaultSocketPath()
	}
	return nil
}

func (c *UnmountCmd) Run() error {
	resp, err := callIPC(c.Socket, rpcReq{ID: "1", Method: "Unmount", Params: map[string]any{"mountpoint": c.Path}})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("ipc error: %d %s", resp.Error.Code, resp.Error.Message)
	}
	fmt.Fprintln(os.Stdout, "ok")
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

func snapshotDirectory(root string, store core.BlockStore, blockSize int) ([]meta.FileEntry, error) {
	var files []meta.FileEntry

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
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
		// never snapshot internal metadata
		if rel == ".fvs2" || strings.HasPrefix(rel, ".fvs2"+string(os.PathSeparator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			// v0: ignore symlinks
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		blocks, size, err := putFileBlocks(p, store, blockSize)
		if err != nil {
			return err
		}
		files = append(files, meta.FileEntry{Path: filepath.ToSlash(rel), Mode: uint32(info.Mode().Perm()), Size: size, Blocks: blocks})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func putFileBlocks(path string, store core.BlockStore, blockSize int) ([]core.BlockID, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var out []core.BlockID
	var total int64
	br := bufio.NewReaderSize(f, blockSize)
	buf := make([]byte, blockSize)

	for {
		n, err := io.ReadFull(br, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// last partial block
				if n > 0 {
					id, perr := store.Put(buf[:n])
					if perr != nil {
						return nil, 0, perr
					}
					out = append(out, id)
					total += int64(n)
				}
				break
			}
			return nil, 0, err
		}
		id, err := store.Put(buf[:n])
		if err != nil {
			return nil, 0, err
		}
		out = append(out, id)
		total += int64(n)
	}
	return out, total, nil
}

func writeCommit(root string, c meta.Commit) error {
	return writeJSONAtomic(meta.CommitPath(root, c.ID), c)
}

func appendIndex(root string, sum meta.CommitSummary) error {
	idx, err := meta.LoadIndex(root)
	if err != nil {
		return err
	}
	idx.Commits = append(idx.Commits, sum)
	return meta.SaveIndex(root, idx)
}

func restoreCommit(dest string, store core.BlockStore, c meta.Commit) error {
	for _, fe := range c.Files {
		outPath := filepath.Join(dest, filepath.FromSlash(fe.Path))
		if strings.HasPrefix(filepath.Clean(outPath), filepath.Join(dest, ".fvs2")) {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(fe.Mode))
		if err != nil {
			return err
		}
		var written int64
		for _, bid := range fe.Blocks {
			b, err := store.Get(bid)
			if err != nil {
				_ = f.Close()
				return err
			}
			if _, err := f.Write(b); err != nil {
				_ = f.Close()
				return err
			}
			written += int64(len(b))
			if written >= fe.Size {
				break
			}
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}

type rpcReq struct {
	ID     string      `json:"id"`
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

type rpcResp struct {
	Ok     bool            `json:"ok"`
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Details json.RawMessage `json:"details"`
	} `json:"error"`
}

func defaultSocketPath() string {
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		return filepath.Join(x, "fvs2d.sock")
	}
	return "/tmp/fvs2d.sock"
}

func callIPC(sock string, req rpcReq) (rpcResp, error) {
	c, err := net.Dial("unix", sock)
	if err != nil {
		return rpcResp{}, err
	}
	defer c.Close()
	enc := json.NewEncoder(c)
	if err := enc.Encode(req); err != nil {
		return rpcResp{}, err
	}
	r := bufio.NewReader(c)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return rpcResp{}, err
	}
	var resp rpcResp
	if err := json.Unmarshal(bytesTrimSpace(line), &resp); err != nil {
		return rpcResp{}, err
	}
	if resp.Error == nil {
		resp.Error = &struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Details json.RawMessage `json:"details"`
		}{Code: 0, Message: ""}
	}
	return resp, nil
}

func bytesTrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

func computeDirty(root, headCommit string) (bool, int, error) {
	// No head commit => treat as dirty if there are any files.
	if headCommit == "" {
		files, err := snapshotIDs(root, 4096)
		if err != nil {
			return false, 0, err
		}
		return len(files) > 0, len(files), nil
	}
	c, err := meta.LoadCommit(root, headCommit)
	if err != nil {
		return false, 0, err
	}
	want := make(map[string]meta.FileEntry, len(c.Files))
	for _, fe := range c.Files {
		want[fe.Path] = fe
	}
	got, err := snapshotIDs(root, c.BlockSize)
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
		if w.Size != g.Size || w.Mode != g.Mode || !equalBlocksBlockIDs(w.Blocks, g.Blocks) {
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
	Path   string
	Mode   uint32
	Size   int64
	Blocks []string
}

func snapshotIDs(root string, blockSize int) (map[string]snapEntry, error) {
	out := map[string]snapEntry{}
	files, err := snapshotDirectoryNoStore(root)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		blocks, size, err := hashFileBlocks(filepath.Join(root, filepath.FromSlash(f.Path)), blockSize)
		if err != nil {
			return nil, err
		}
		out[f.Path] = snapEntry{Path: f.Path, Mode: f.Mode, Size: size, Blocks: blocks}
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
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		files = append(files, meta.FileEntry{Path: filepath.ToSlash(rel), Mode: uint32(info.Mode().Perm()), Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func hashFileBlocks(path string, blockSize int) ([]string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	if blockSize <= 0 {
		blockSize = 4096
	}
	br := bufio.NewReaderSize(f, blockSize)
	buf := make([]byte, blockSize)
	var total int64
	var blocks []string
	for {
		n, err := io.ReadFull(br, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if n > 0 {
					blocks = append(blocks, blake3hex(buf[:n]))
					total += int64(n)
				}
				break
			}
			return nil, 0, err
		}
		blocks = append(blocks, blake3hex(buf[:n]))
		total += int64(n)
	}
	return blocks, total, nil
}

func blake3hex(b []byte) string {
	sum := blake3.Sum256(b)
	return hex.EncodeToString(sum[:])
}
