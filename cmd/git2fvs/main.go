// git2fvs converts a git repository into an FVS repository, replaying the
// branch's first-parent history oldest to newest: every git commit becomes an
// FVS state (and, with -remote, a push that lands in the remote's reflog).
//
// Git author identities cannot be mapped onto remote accounts: every pushed
// state is attributed to the token's account. With -authors the original
// author name is appended to the state message; otherwise the message is the
// git subject line alone.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	fvsrepo "github.com/fvs-lab/fvs2/repo"
)

type options struct {
	git     string
	branch  string
	limit   int
	work    string
	remote  string
	token   string
	keep    bool
	authors bool
}

func main() {
	var o options
	flag.StringVar(&o.git, "git", "", "git repository URL or local path")
	flag.StringVar(&o.branch, "branch", "", "branch to convert (default: the clone's default branch)")
	flag.IntVar(&o.limit, "limit", 0, "convert only the last N commits (0 = all)")
	flag.StringVar(&o.work, "work", "", "working directory (default: a temp dir)")
	flag.StringVar(&o.remote, "remote", "", "FVS remote URL to push each state to")
	flag.StringVar(&o.token, "token", "", "remote auth token")
	flag.BoolVar(&o.keep, "keep", false, "keep the working directory")
	flag.BoolVar(&o.authors, "authors", false, "append the git author name to each state message")
	flag.Parse()

	if o.git == "" {
		log.Fatal("git2fvs: -git is required")
	}
	if _, err := convert(o, os.Stderr); err != nil {
		log.Fatalf("git2fvs: %v", err)
	}
}

// convert runs the full conversion and returns the number of FVS states
// created.
func convert(o options, logw io.Writer) (int, error) {
	work := o.work
	if work == "" {
		dir, err := os.MkdirTemp("", "git2fvs-*")
		if err != nil {
			return 0, err
		}
		work = dir
	} else if err := os.MkdirAll(work, 0o755); err != nil {
		return 0, err
	}
	if !o.keep {
		defer os.RemoveAll(work)
	}
	clone := filepath.Join(work, "clone")
	fvsDir := filepath.Join(work, "fvs")

	if _, err := os.Stat(filepath.Join(clone, ".git")); err != nil {
		if _, err := git("", "clone", o.git, clone); err != nil {
			return 0, err
		}
	}
	shas, err := commitList(clone, o.branch, o.limit)
	if err != nil {
		return 0, err
	}
	if len(shas) == 0 {
		return 0, errors.New("no commits to convert")
	}

	if err := os.MkdirAll(fvsDir, 0o755); err != nil {
		return 0, err
	}
	if _, err := fvsrepo.Init(fvsDir, 0); err != nil {
		return 0, err
	}

	created := 0
	for i, sha := range shas {
		if _, err := git(clone, "checkout", "--force", "--detach", sha); err != nil {
			return created, err
		}
		subject, err := git(clone, "log", "-1", "--format=%s", sha)
		if err != nil {
			return created, err
		}
		message := subject
		if o.authors {
			author, err := git(clone, "log", "-1", "--format=%an", sha)
			if err != nil {
				return created, err
			}
			message = fmt.Sprintf("%s (%s)", subject, author)
		}
		if err := syncTree(clone, fvsDir); err != nil {
			return created, err
		}
		res, err := fvsrepo.Commit(fvsDir, message, false, nil)
		if err != nil {
			return created, err
		}
		if !res.Created {
			fmt.Fprintf(logw, "warning: %.12s %q produced no new state, skipping\n", sha, subject)
			continue
		}
		created++
		fmt.Fprintf(logw, "[%d/%d] %.12s -> %.12s %q\n", i+1, len(shas), sha, res.StateID, subject)
		if o.remote != "" {
			push, err := fvsrepo.Push(fvsDir, fvsrepo.Remote{URL: o.remote, Token: o.token}, "main", false)
			if err != nil {
				return created, fmt.Errorf("push %.12s: %w", sha, err)
			}
			fmt.Fprintf(logw, "        pushed %d/%d blocks\n", push.UploadedBlocks, push.TotalBlocks)
		}
	}
	fmt.Fprintf(logw, "converted %d of %d commits\n", created, len(shas))
	return created, nil
}

// commitList returns the branch's first-parent commits oldest to newest,
// limited to the last n when n > 0.
func commitList(clone, branch string, n int) ([]string, error) {
	ref := "HEAD"
	if branch != "" {
		ref = branch
		if _, err := git(clone, "rev-parse", "--verify", "--quiet", ref+"^{commit}"); err != nil {
			ref = "origin/" + branch
		}
	}
	out, err := git(clone, "rev-list", "--first-parent", ref)
	if err != nil {
		return nil, err
	}
	shas := strings.Fields(out)
	if n > 0 && len(shas) > n {
		shas = shas[:n]
	}
	for i, j := 0, len(shas)-1; i < j; i, j = i+1, j-1 {
		shas[i], shas[j] = shas[j], shas[i]
	}
	return shas, nil
}

func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// syncTree mirrors src into dst, skipping src/.git and preserving dst/.fvs2.
// Files are compared by mode, size and content; copies get an mtime strictly
// past the replaced file's, so the FVS dirty check (second-granularity
// mtime+size) cannot miss a same-size rewrite within one second.
func syncTree(src, dst string) error {
	kept := map[string]bool{}
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".git" {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		kept[rel] = true
		if entry.Type()&os.ModeSymlink != 0 {
			return syncSymlink(path, target)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		return syncFile(path, target)
	})
	if err != nil {
		return err
	}
	return deleteExtraneous(dst, kept)
}

func syncSymlink(src, dst string) error {
	want, err := os.Readlink(src)
	if err != nil {
		return err
	}
	if cur, err := os.Readlink(dst); err == nil && cur == want {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	_ = os.Remove(dst)
	return os.Symlink(want, dst)
}

func syncFile(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	var prevMod time.Time
	if dstInfo, err := os.Lstat(dst); err == nil {
		if dstInfo.Mode().IsRegular() {
			prevMod = dstInfo.ModTime()
			if dstInfo.Size() == srcInfo.Size() && dstInfo.Mode().Perm() == srcInfo.Mode().Perm() {
				same, err := sameContent(src, dst)
				if err != nil {
					return err
				}
				if same {
					return nil
				}
			}
		} else {
			_ = os.Remove(dst)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := copyFile(src, dst, srcInfo.Mode().Perm()); err != nil {
		return err
	}
	// Force the mtime past the replaced file's so a same-size rewrite in the
	// same second is still seen as dirty.
	now := time.Now()
	if !prevMod.IsZero() && now.Unix() <= prevMod.Unix() {
		now = prevMod.Add(time.Second)
	}
	return os.Chtimes(dst, now, now)
}

func sameContent(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer fa.Close()
	fb, err := os.Open(b)
	if err != nil {
		return false, err
	}
	defer fb.Close()
	bufA := make([]byte, 64<<10)
	bufB := make([]byte, 64<<10)
	for {
		na, errA := io.ReadFull(fa, bufA)
		nb, errB := io.ReadFull(fb, bufB)
		if na != nb || !bytes.Equal(bufA[:na], bufB[:nb]) {
			return false, nil
		}
		if errA == io.EOF || errA == io.ErrUnexpectedEOF {
			return errB == io.EOF || errB == io.ErrUnexpectedEOF, nil
		}
		if errA != nil {
			return false, errA
		}
		if errB != nil {
			return false, errB
		}
	}
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Chmod(perm); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// deleteExtraneous removes files and symlinks under dst that are not in keep,
// then prunes emptied directories. dst/.fvs2 is never touched.
func deleteExtraneous(dst string, keep map[string]bool) error {
	var files, dirs []string
	err := filepath.WalkDir(dst, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(dst, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == ".fvs2" {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		if !keep[rel] {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range files {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err == nil && len(entries) == 0 {
			_ = os.Remove(dir)
		}
	}
	return nil
}
