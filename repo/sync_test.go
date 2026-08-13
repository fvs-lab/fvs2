package repo

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fvs-lab/fvs2/internal/meta"
	"github.com/fvs-lab/fvs2/remote"
)

func newSyncRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := Init(dir, 0); err != nil {
		t.Fatal(err)
	}
	return dir
}

func newRemote(t *testing.T, token string) (*httptest.Server, meta.Remote) {
	t.Helper()
	srv, err := remote.NewServer(t.TempDir(), token)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, meta.Remote{URL: ts.URL, Token: token}
}

func writeSync(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPushPullRoundTrip(t *testing.T) {
	_, rm := newRemote(t, "")

	src := newSyncRepo(t)
	data := make([]byte, 300<<10)
	rand.New(rand.NewSource(21)).Read(data)
	writeSync(t, src, "drive_c/game.bin", data)
	writeSync(t, src, "note.txt", []byte("hello"))
	if _, err := Commit(src, "first", false, nil); err != nil {
		t.Fatal(err)
	}

	push, err := Push(src, rm, "", false)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if push.UploadedBlocks == 0 || push.UploadedBlocks != push.TotalBlocks {
		t.Fatalf("first push should upload every block: %+v", push)
	}

	// A second push of the same state transfers nothing.
	again, err := Push(src, rm, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if again.UploadedBlocks != 0 {
		t.Fatalf("re-push uploaded %d blocks", again.UploadedBlocks)
	}

	dst := newSyncRepo(t)
	pull, err := Pull(dst, rm, "main")
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if pull.StateID != push.StateID || pull.DownloadedBlocks != push.TotalBlocks {
		t.Fatalf("pull = %+v, push = %+v", pull, push)
	}

	if _, err := Restore(dst, pull.StateID, RestoreOptions{}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "drive_c/game.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("pulled state differs from pushed state")
	}

	// Pulling again is a no-op.
	pull, err = Pull(dst, rm, "main")
	if err != nil {
		t.Fatal(err)
	}
	if !pull.UpToDate {
		t.Fatalf("second pull not up to date: %+v", pull)
	}
}

func TestPushDedupAcrossRepos(t *testing.T) {
	_, rm := newRemote(t, "")

	shared := make([]byte, 200<<10)
	rand.New(rand.NewSource(5)).Read(shared)

	a := newSyncRepo(t)
	writeSync(t, a, "runtime.dll", shared)
	if _, err := Commit(a, "a", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(a, rm, "", false); err != nil {
		t.Fatal(err)
	}

	// A different repo with the same content plus one small file: only the
	// new content crosses the wire.
	b := newSyncRepo(t)
	writeSync(t, b, "runtime.dll", shared)
	writeSync(t, b, "extra.txt", []byte("only new bytes"))
	if _, err := Commit(b, "b", false, nil); err != nil {
		t.Fatal(err)
	}
	if err := meta.CreateBranch(b, "other"); err != nil {
		t.Fatal(err)
	}
	push, err := Push(b, rm, "other", false)
	if err != nil {
		t.Fatal(err)
	}
	// The shared 200 KiB content must not travel again: only the new file's
	// block and the few changed tree objects may upload.
	if push.UploadedBlocks > 4 || push.UploadedBlocks == push.TotalBlocks {
		t.Fatalf("dedup failed: uploaded %d of %d blocks", push.UploadedBlocks, push.TotalBlocks)
	}
}

func TestPushRefusesUnknownRemoteState(t *testing.T) {
	_, rm := newRemote(t, "")

	a := newSyncRepo(t)
	writeSync(t, a, "f", []byte("from a"))
	if _, err := Commit(a, "a", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(a, rm, "", false); err != nil {
		t.Fatal(err)
	}

	// Repo b never pulled a's state: pushing over it must fail without force.
	b := newSyncRepo(t)
	writeSync(t, b, "f", []byte("from b"))
	if _, err := Commit(b, "b", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(b, rm, "", false); err == nil {
		t.Fatal("push over an unknown remote state must fail")
	}
	if _, err := Push(b, rm, "", true); err != nil {
		t.Fatalf("forced push: %v", err)
	}
}

func TestRemoteRequiresToken(t *testing.T) {
	_, rm := newRemote(t, "secret")

	src := newSyncRepo(t)
	writeSync(t, src, "f", []byte("x"))
	if _, err := Commit(src, "c", false, nil); err != nil {
		t.Fatal(err)
	}

	bad := rm
	bad.Token = ""
	if _, err := Push(src, bad, "", false); err == nil {
		t.Fatal("push without token must fail")
	}
	if _, err := Push(src, rm, "", false); err != nil {
		t.Fatalf("push with token: %v", err)
	}
}

func newMultiUserRemote(t *testing.T, users []remote.User) *httptest.Server {
	t.Helper()
	srv, err := remote.NewServerWithUsers(t.TempDir(), users)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func TestRefsAreIsolatedPerUser(t *testing.T) {
	ts := newMultiUserRemote(t, []remote.User{
		{Name: "alice", Token: "ta"},
		{Name: "bob", Token: "tb"},
	})
	alice := meta.Remote{URL: ts.URL, Token: "ta"}
	bob := meta.Remote{URL: ts.URL, Token: "tb"}

	a := newSyncRepo(t)
	writeSync(t, a, "f", []byte("from alice"))
	if _, err := Commit(a, "a", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(a, alice, "", false); err != nil {
		t.Fatal(err)
	}

	// bob has his own "main": he sees no ref, and his push cannot touch
	// alice's.
	b := newSyncRepo(t)
	if _, err := Pull(b, bob, "main"); err == nil {
		t.Fatal("bob must not see alice's ref")
	}
	writeSync(t, b, "f", []byte("from bob"))
	if _, err := Commit(b, "b", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(b, bob, "", false); err != nil {
		t.Fatalf("bob's first push must not conflict with alice's ref: %v", err)
	}

	check := newSyncRepo(t)
	pull, err := Pull(check, alice, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(check, pull.StateID, RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(check, "f"))
	if string(got) != "from alice" {
		t.Fatalf("alice's ref was clobbered: %q", got)
	}
}

func TestQuotaEnforced(t *testing.T) {
	ts := newMultiUserRemote(t, []remote.User{
		{Name: "tiny", Token: "tt", QuotaBytes: 10 << 10},
	})
	rm := meta.Remote{URL: ts.URL, Token: "tt"}

	src := newSyncRepo(t)
	data := make([]byte, 200<<10)
	rand.New(rand.NewSource(31)).Read(data)
	writeSync(t, src, "big.bin", data)
	if _, err := Commit(src, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(src, rm, "", false); err == nil {
		t.Fatal("push exceeding the quota must fail")
	}
}

func TestServerGCReclaimsUnreferencedState(t *testing.T) {
	ts, rm := newRemote(t, "admintoken")

	src := newSyncRepo(t)
	data := make([]byte, 150<<10)
	rand.New(rand.NewSource(41)).Read(data)
	writeSync(t, src, "f.bin", data)
	if _, err := Commit(src, "c1", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(src, rm, "", false); err != nil {
		t.Fatal(err)
	}
	_ = ts

	client := remote.NewClient(rm.URL, rm.Token)

	// Nothing is unreferenced yet: gc removes nothing.
	res, err := client.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedBlocks != 0 || res.RemovedStates != 0 {
		t.Fatalf("gc removed live data: %+v", res)
	}

	// Drop the ref: the state and its blocks become garbage.
	if err := client.DeleteRef("main"); err != nil {
		t.Fatal(err)
	}
	res, err = client.GC(0)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedBlocks == 0 || res.RemovedStates != 1 {
		t.Fatalf("gc did not reclaim the dropped state: %+v", res)
	}

	// The store is usable afterwards: the same content pushes again in full.
	push, err := Push(src, rm, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if push.UploadedBlocks != push.TotalBlocks {
		t.Fatalf("blocks should be gone after gc: %+v", push)
	}
}

func TestGCGraceKeepsRecentObjects(t *testing.T) {
	_, rm := newRemote(t, "tok")

	src := newSyncRepo(t)
	writeSync(t, src, "f", []byte("fresh"))
	if _, err := Commit(src, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Push(src, rm, "", false); err != nil {
		t.Fatal(err)
	}

	client := remote.NewClient(rm.URL, rm.Token)
	if err := client.DeleteRef("main"); err != nil {
		t.Fatal(err)
	}
	// Everything is seconds old: with the default-style grace nothing goes.
	res, err := client.GC(3600 * 1000000000)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedBlocks != 0 || res.RemovedStates != 0 {
		t.Fatalf("gc ignored the grace window: %+v", res)
	}
}

func TestGCRequiresAdmin(t *testing.T) {
	ts := newMultiUserRemote(t, []remote.User{
		{Name: "user", Token: "tu"},
	})
	client := remote.NewClient(ts.URL, "tu")
	if _, err := client.GC(0); err == nil {
		t.Fatal("gc from a non-admin must fail")
	}
}

// TestPullFallsBackWithoutServerExpansion proxies the remote, rewriting
// expanded state requests into raw ones (the pre-expansion server behavior):
// the puller must detect the missing closure and walk the tree level by
// level, ending with the same bytes.
func TestPullFallsBackWithoutServerExpansion(t *testing.T) {
	ts, _ := newRemote(t, "")
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/states/") {
			r.URL.RawQuery = "" // strip expand=1: serve the raw document
		}
		target, _ := url.Parse(ts.URL)
		r.Host = target.Host
		httputil.NewSingleHostReverseProxy(target).ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	rm := meta.Remote{URL: proxy.URL}

	src := newSyncRepo(t)
	data := make([]byte, 100<<10)
	rand.New(rand.NewSource(77)).Read(data)
	writeSync(t, src, "deep/nested/dirs/file.bin", data)
	writeSync(t, src, "top.txt", []byte("fallback"))
	if _, err := Commit(src, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	push, err := Push(src, rm, "", false)
	if err != nil {
		t.Fatal(err)
	}

	dst := newSyncRepo(t)
	pull, err := Pull(dst, rm, "main")
	if err != nil {
		t.Fatalf("pull without server expansion: %v", err)
	}
	if pull.StateID != push.StateID || pull.DownloadedBlocks != push.TotalBlocks {
		t.Fatalf("pull = %+v, push = %+v", pull, push)
	}
	if _, err := Restore(dst, pull.StateID, RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "deep/nested/dirs/file.bin"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("fallback pull restored wrong content: %v", err)
	}
}

// TestPullKeepsLocalStateDocLean checks that a pulled format-3 state stores
// only the root tree pointer locally, not the server-expanded file list.
func TestPullKeepsLocalStateDocLean(t *testing.T) {
	_, rm := newRemote(t, "")

	src := newSyncRepo(t)
	writeSync(t, src, "a/b.txt", []byte("lean"))
	if _, err := Commit(src, "c", false, nil); err != nil {
		t.Fatal(err)
	}
	push, err := Push(src, rm, "", false)
	if err != nil {
		t.Fatal(err)
	}

	dst := newSyncRepo(t)
	if _, err := Pull(dst, rm, "main"); err != nil {
		t.Fatal(err)
	}
	commit, err := meta.LoadCommit(dst, push.StateID)
	if err != nil {
		t.Fatal(err)
	}
	if commit.RootTree == "" || len(commit.Files) != 0 {
		t.Fatalf("pulled doc must keep the tree pointer only: root=%q files=%d", commit.RootTree, len(commit.Files))
	}
}
