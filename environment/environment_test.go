package environment

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"fvs2/internal/meta"
	"fvs2/remote"
	fvsrepo "fvs2/repo"
)

func initRepo(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := fvsrepo.Init(dir, 0); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fvsrepo.Commit(dir, "state", false, nil); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestResolveAndVerify(t *testing.T) {
	base := t.TempDir()
	initRepo(t, filepath.Join(base, "runtime"), map[string]string{"windows/vcruntime.dll": "shared"})
	initRepo(t, filepath.Join(base, "app"), map[string]string{"drive_c/app.exe": "the app"})

	m := Manifest{
		Name: "demo",
		Layers: []Layer{
			{Name: "runtime", Repo: "runtime"},
			{Name: "app", Repo: "app"},
		},
		Mount: "/tmp/demo",
	}
	if err := m.Save(filepath.Join(base, ManifestName)); err != nil {
		t.Fatal(err)
	}

	lock, err := Resolve(m, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(lock.Layers) != 2 || lock.Layers[0].Name != "runtime" || lock.Layers[1].Name != "app" {
		t.Fatalf("lock layers wrong: %+v", lock.Layers)
	}
	for _, l := range lock.Layers {
		if len(l.StateID) != 64 {
			t.Fatalf("layer %q not pinned to a full state id: %q", l.Name, l.StateID)
		}
	}

	if err := Verify(lock, base); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// Reproducibility: resolving again pins the identical states.
	again, err := Resolve(m, base)
	if err != nil {
		t.Fatal(err)
	}
	for i := range lock.Layers {
		if again.Layers[i].StateID != lock.Layers[i].StateID {
			t.Fatal("resolve is not deterministic")
		}
	}
}

func TestResolveBranchAndState(t *testing.T) {
	base := t.TempDir()
	repo := filepath.Join(base, "r")
	initRepo(t, repo, map[string]string{"f": "v1"})
	firstID, err := meta.ResolveHeadCommit(repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.CreateBranch(repo, "stable"); err != nil {
		t.Fatal(err)
	}

	m := Manifest{Name: "e", Layers: []Layer{
		{Name: "by-state", Repo: "r", State: firstID[:10]},
		{Name: "by-branch", Repo: "r", Branch: "stable"},
	}}
	lock, err := Resolve(m, base)
	if err != nil {
		t.Fatal(err)
	}
	if lock.Layers[0].StateID != firstID {
		t.Fatalf("state prefix resolved to %s, want %s", lock.Layers[0].StateID, firstID)
	}
	if lock.Layers[1].StateID != firstID {
		t.Fatalf("branch head resolved to %s, want %s", lock.Layers[1].StateID, firstID)
	}
}

func TestRejectStateAndBranchTogether(t *testing.T) {
	base := t.TempDir()
	initRepo(t, filepath.Join(base, "r"), map[string]string{"f": "x"})
	m := Manifest{Name: "e", Layers: []Layer{
		{Name: "bad", Repo: "r", State: "abc", Branch: "main"},
	}}
	if err := m.Save(filepath.Join(base, ManifestName)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(filepath.Join(base, ManifestName)); err == nil {
		t.Fatal("manifest with both state and branch must be rejected")
	}
}

// TestSyncPullsPinnedLayer models the B2B flow: a golden layer is published to
// a remote, a consumer machine has only the lockfile and an empty layer repo,
// and env sync fetches the exact pinned state.
func TestSyncPullsPinnedLayer(t *testing.T) {
	// Publisher builds the golden runtime and pushes it.
	pub := t.TempDir()
	runtime := initRepo(t, filepath.Join(pub, "runtime"), map[string]string{"windows/base.dll": "golden"})

	srv, err := remote.NewServer(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	if err := meta.AddRemote(runtime, "origin", meta.Remote{URL: ts.URL}); err != nil {
		t.Fatal(err)
	}
	if _, err := fvsrepo.Push(runtime, meta.Remote{URL: ts.URL}, "main", false); err != nil {
		t.Fatal(err)
	}

	m := Manifest{Name: "golden", Layers: []Layer{
		{Name: "runtime", Repo: "runtime", Branch: "main", Pull: &Pull{Remote: "origin", Branch: "main"}},
	}}
	lock, err := Resolve(m, pub)
	if err != nil {
		t.Fatal(err)
	}

	// Consumer has the lockfile and an initialized-but-empty layer repo
	// pointed at the same remote.
	cons := t.TempDir()
	consRepo := filepath.Join(cons, "runtime")
	if _, err := fvsrepo.Init(consRepo, 0); err != nil {
		t.Fatal(err)
	}
	if err := meta.AddRemote(consRepo, "origin", meta.Remote{URL: ts.URL}); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(filepath.Join(cons, ManifestName)); err != nil {
		t.Fatal(err)
	}
	if err := lock.Save(filepath.Join(cons, LockName)); err != nil {
		t.Fatal(err)
	}

	if err := Verify(lock, cons); err == nil {
		t.Fatal("verify should fail before sync: the state is not local yet")
	}
	if err := Sync(m, lock, cons); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := Verify(lock, cons); err != nil {
		t.Fatalf("verify after sync: %v", err)
	}

	// The synced state materializes to the golden content.
	if _, err := fvsrepo.Restore(consRepo, lock.Layers[0].StateID, fvsrepo.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(consRepo, "windows/base.dll"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "golden" {
		t.Fatalf("synced layer content = %q, want golden", got)
	}
}
