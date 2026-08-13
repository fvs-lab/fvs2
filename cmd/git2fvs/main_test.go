package main

import (
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/fvs-lab/fvs2/remote"
	fvsrepo "github.com/fvs-lab/fvs2/repo"
)

func gitT(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func writeT(t *testing.T, dir, rel, data string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newTestRemote(t *testing.T, token string) string {
	t.Helper()
	srv, err := remote.NewServer(t.TempDir(), token)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestConvertPreservesHistory(t *testing.T) {
	src := t.TempDir()
	gitT(t, src, "init", "-b", "main")

	writeT(t, src, "readme.txt", "first version\n")
	writeT(t, src, "sub/data.bin", "AAAA")
	gitT(t, src, "add", ".")
	gitT(t, src, "commit", "-m", "initial import")

	writeT(t, src, "readme.txt", "second version\n")
	gitT(t, src, "add", ".")
	gitT(t, src, "commit", "-m", "update readme")

	// Same size as the previous content: exercises the same-second dirty check.
	writeT(t, src, "sub/data.bin", "BBBB")
	writeT(t, src, "extra.txt", "extra\n")
	gitT(t, src, "add", ".")
	gitT(t, src, "commit", "-m", "change data, add extra")

	gitT(t, src, "rm", "-q", "extra.txt")
	gitT(t, src, "commit", "-m", "drop extra")

	work := t.TempDir()
	url := newTestRemote(t, "tok")
	created, err := convert(options{
		git:    src,
		limit:  0,
		work:   work,
		remote: url,
		token:  "tok",
		keep:   true,
	}, io.Discard)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if created != 4 {
		t.Fatalf("created %d states, want 4", created)
	}

	fvsDir := filepath.Join(work, "fvs")
	states, err := fvsrepo.States(fvsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 4 {
		t.Fatalf("got %d states, want 4", len(states))
	}
	if states[0].Message != "drop extra" || states[3].Message != "initial import" {
		t.Fatalf("unexpected state order: %+v", states)
	}

	oldest := states[len(states)-1]
	dest := t.TempDir()
	if _, err := fvsrepo.Restore(fvsDir, oldest.ID, fvsrepo.RestoreOptions{To: dest}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "readme.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first version\n" {
		t.Fatalf("oldest state readme = %q", got)
	}
	data, err := os.ReadFile(filepath.Join(dest, "sub/data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "AAAA" {
		t.Fatalf("oldest state data.bin = %q", data)
	}
	if _, err := os.Stat(filepath.Join(dest, "extra.txt")); !os.IsNotExist(err) {
		t.Fatalf("extra.txt must not exist in the oldest state: %v", err)
	}

	// The remote ref points at the newest state after the ordered pushes.
	client := remote.NewClient(url, "tok")
	ref, err := client.GetRef("main")
	if err != nil {
		t.Fatal(err)
	}
	if ref != states[0].ID {
		t.Fatalf("remote ref = %.12s, want newest state %.12s", ref, states[0].ID)
	}
}

func TestConvertLimit(t *testing.T) {
	src := t.TempDir()
	gitT(t, src, "init", "-b", "main")
	for _, step := range []string{"one", "two", "three"} {
		writeT(t, src, "f.txt", step+"\n")
		gitT(t, src, "add", ".")
		gitT(t, src, "commit", "-m", step)
	}

	work := t.TempDir()
	created, err := convert(options{git: src, limit: 2, work: work, keep: true}, io.Discard)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if created != 2 {
		t.Fatalf("created %d states, want 2", created)
	}
	states, err := fvsrepo.States(filepath.Join(work, "fvs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].Message != "three" || states[1].Message != "two" {
		t.Fatalf("unexpected states: %+v", states)
	}
}
