package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	Mount    MountCmd    `cmd:"mount" help:"Spawn fvs2d to mount a branch (gRPC control)"`
	Unmount  UnmountCmd  `cmd:"unmount" help:"Ask fvs2d over gRPC to unmount and exit"`

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
	c.Mount.Root = c
	c.Unmount.Root = c
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
	cfg, err := meta.LoadConfig(root)
	if err != nil {
		return err
	}
	store, err := meta.NewBlockStore(root)
	if err != nil {
		return err
	}

	var headFiles map[string]meta.FileEntry
	headCommit, _ := meta.ResolveHeadCommit(root)
	if headCommit != "" {
		hc, err := meta.LoadCommit(root, headCommit)
		if err == nil {
			headFiles = make(map[string]meta.FileEntry, len(hc.Files))
			for _, f := range hc.Files {
				headFiles[f.Path] = f
			}
		}
	}

	files, err := snapshotDirectory(root, store, cfg.BlockSize, headFiles, c.Verbose)
	if err != nil {
		return err
	}
	if !c.AllowEmpty && headCommit != "" && sameFileSet(headFiles, files) {
		fmt.Fprintln(os.Stdout, "nothing to commit, working tree clean")
		return nil
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
	if err := restoreCommit(dest, store, commit, c.Verbose); err != nil {
		return err
	}
	if c.Clean {
		if err := cleanDest(dest, commit, c.Verbose); err != nil {
			return err
		}
	}
	if c.Reset {
		if err := meta.AdvanceHeadAfterCommit(root, id); err != nil {
			return err
		}
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

type MountCmd struct {
	Socket   string `cli:"socket" help:"control socket path (default: derived from mountpoint)"`
	Fvs2d    string `cli:"fvs2d" default:"fvs2d" help:"path to the fvs2d daemon binary"`
	Upper    string `cli:"upper" help:"writable upper layer dir (enables writes)"`
	Readonly bool   `cli:"readonly" help:"mount read-only (no upper layer)"`
	Branch   string `arg:"" required:"true" help:"branch"`
	Path     string `arg:"" required:"true" help:"mountpoint"`
	Root     *CLI   `internal:"ignore"`
}

func (c *MountCmd) Run() error {
	repo, err := absClean(c.Root.Path)
	if err != nil {
		return err
	}
	mp, err := filepath.Abs(c.Path)
	if err != nil {
		return err
	}

	sock := c.Socket
	if sock == "" {
		sock, err = socketForMount(mp)
		if err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mp, 0o755); err != nil {
		return fmt.Errorf("create mountpoint: %w", err)
	}

	args := []string{"-repo", repo, "-mount", mp, "-control", "unix:" + sock}
	if c.Branch != "" {
		args = append(args, "-branch", c.Branch)
	}
	if c.Upper != "" {
		upper, uerr := filepath.Abs(c.Upper)
		if uerr != nil {
			return uerr
		}
		args = append(args, "-upper", upper)
	} else if !c.Readonly {
		return fmt.Errorf("a writable mount requires --upper <dir>; pass --readonly for a read-only mount")
	}

	if err := spawnDaemon(c.Fvs2d, args, sock, 30*time.Second); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: mounted %s (control %s)\n", mp, sock)
	return nil
}

type UnmountCmd struct {
	Socket string `cli:"socket" help:"control socket path (default: derived from mountpoint)"`
	Lazy   bool   `cli:"lazy" help:"detach even if the mountpoint is still busy"`
	Path   string `arg:"" required:"true" help:"mountpoint"`
	Root   *CLI   `internal:"ignore"`
}

func (c *UnmountCmd) Run() error {
	mp, err := filepath.Abs(c.Path)
	if err != nil {
		return err
	}
	sock := c.Socket
	if sock == "" {
		sock, err = socketForMount(mp)
		if err != nil {
			return err
		}
	}
	if err := shutdownDaemon(sock, c.Lazy); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "ok: unmounted %s\n", mp)
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

func snapshotDirectory(root string, store core.BlockStore, blockSize int, headFiles map[string]meta.FileEntry, verbose bool) ([]meta.FileEntry, error) {
	var files []meta.FileEntry

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
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
		relSlash := filepath.ToSlash(rel)

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
			files = append(files, meta.FileEntry{Path: relSlash, Mode: uint32(li.Mode().Perm()), ModTime: li.ModTime().Unix(), Link: target})
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

		if headFiles != nil {
			if hf, ok := headFiles[relSlash]; ok {
				if hf.Link == "" && hf.Size == info.Size() && hf.ModTime == info.ModTime().Unix() && hf.Mode == uint32(info.Mode().Perm()) {
					files = append(files, hf)
					return nil
				}
			}
		}

		if verbose {
			fmt.Printf("hashing: %s\n", relSlash)
		}

		blocks, size, perr := putFileBlocks(p, store, blockSize)
		if perr != nil {
			if errors.Is(perr, errFileVanished) {
				return nil
			}
			return perr
		}
		// Empty files (size 0, no blocks) are still recorded so they survive
		// a commit/restore round-trip.
		files = append(files, meta.FileEntry{Path: relSlash, Mode: uint32(info.Mode().Perm()), Size: size, ModTime: info.ModTime().Unix(), Blocks: blocks})
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
		if os.IsNotExist(err) {
			return nil, 0, errFileVanished
		}
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

func restoreCommit(dest string, store core.BlockStore, c meta.Commit, verbose bool) error {
	for _, fe := range c.Files {
		outPath := filepath.Join(dest, filepath.FromSlash(fe.Path))
		if strings.HasPrefix(filepath.Clean(outPath), filepath.Join(dest, ".fvs2")) {
			continue
		}

		if verbose {
			fmt.Printf("restoring: %s\n", fe.Path)
		}

		if fe.Link != "" {
			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return err
			}
			if cur, err := os.Readlink(outPath); err == nil && cur == fe.Link {
				continue
			}
			_ = os.Remove(outPath)
			if err := os.Symlink(fe.Link, outPath); err != nil {
				return err
			}
			continue
		}

		if info, err := os.Lstat(outPath); err == nil {
			if info.Mode().IsRegular() && info.Size() == fe.Size && uint32(info.Mode().Perm()) == fe.Mode && info.ModTime().Unix() == fe.ModTime {
				// Skip if metadata matches
				continue
			}
			// A non-regular file (e.g. a stale symlink) occupies the path: drop it.
			if !info.Mode().IsRegular() {
				_ = os.Remove(outPath)
			}
		}

		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(fe.Mode))
		if err != nil {
			return err
		}
		bw := bufio.NewWriterSize(f, 65536) // 64KB buffer
		var written int64
		for _, bid := range fe.Blocks {
			b, err := store.Get(bid)
			if err != nil {
				_ = f.Close()
				return err
			}
			if _, err := bw.Write(b); err != nil {
				_ = f.Close()
				return err
			}
			written += int64(len(b))
			if written >= fe.Size {
				break
			}
		}
		if err := bw.Flush(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		// Set mod time to allow future fast skips
		_ = os.Chtimes(outPath, time.Now(), time.Unix(fe.ModTime, 0))
	}
	return nil
}

// cleanDest removes any file or symlink under dest that is not part of the
// restored commit, then prunes the directories left empty. The .fvs2 metadata
// directory is always preserved. This turns restore into an exact checkout.
func cleanDest(dest string, c meta.Commit, verbose bool) error {
	want := make(map[string]bool, len(c.Files))
	for _, fe := range c.Files {
		want[filepath.Clean(filepath.Join(dest, filepath.FromSlash(fe.Path)))] = true
	}
	metaPath := filepath.Join(dest, ".fvs2")

	var toRemove []string
	err := filepath.WalkDir(dest, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		clean := filepath.Clean(p)
		if clean == metaPath || strings.HasPrefix(clean, metaPath+string(os.PathSeparator)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if clean == filepath.Clean(dest) || d.IsDir() {
			return nil
		}
		if !want[clean] {
			toRemove = append(toRemove, clean)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, p := range toRemove {
		if verbose {
			fmt.Printf("removing: %s\n", p)
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// Prune now-empty directories (deepest first), never touching .fvs2.
	var dirs []string
	_ = filepath.WalkDir(dest, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		clean := filepath.Clean(p)
		if clean == metaPath {
			return filepath.SkipDir
		}
		if d.IsDir() && clean != filepath.Clean(dest) {
			dirs = append(dirs, clean)
		}
		return nil
	})
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err == nil && len(entries) == 0 {
			_ = os.Remove(dir)
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

// sameFileSet reports whether the freshly snapshotted files are identical, in
// content, to the HEAD commit's files. ModTime is intentionally ignored so a
// touched-but-unchanged file does not count as a change.
func sameFileSet(head map[string]meta.FileEntry, files []meta.FileEntry) bool {
	if len(head) != len(files) {
		return false
	}
	for _, f := range files {
		h, ok := head[f.Path]
		if !ok {
			return false
		}
		if h.Mode != f.Mode || h.Size != f.Size || h.Link != f.Link {
			return false
		}
		if len(h.Blocks) != len(f.Blocks) {
			return false
		}
		for i := range f.Blocks {
			if h.Blocks[i] != f.Blocks[i] {
				return false
			}
		}
	}
	return true
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

func snapshotIDs(root string, blockSize int) (map[string]snapEntry, error) {
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
		blocks, size, err := hashFileBlocks(filepath.Join(root, filepath.FromSlash(f.Path)), blockSize)
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

func hashFileBlocks(path string, blockSize int) ([]string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, errFileVanished
		}
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
