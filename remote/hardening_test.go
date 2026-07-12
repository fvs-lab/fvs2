package remote

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "fvs-v2-core"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func newTestServer(t *testing.T, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	if cfg.Root == "" {
		cfg.Root = t.TempDir()
	}
	srv, err := NewServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})
	return srv, ts
}

func TestRuntimeAccountManagement(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{{Name: "root", Token: "admin", Admin: true}}})
	admin := NewClient(ts.URL, "admin")

	// A brand-new account can be added and immediately used.
	if err := admin.AddUser(User{Name: "alice", Token: "ta", QuotaBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	users, err := admin.ListUsers()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("accounts = %d, want 2", len(users))
	}
	for _, u := range users {
		if u.Token != "" {
			t.Fatal("ListUsers must redact tokens")
		}
	}

	alice := NewClient(ts.URL, "ta")
	putEmptyState(t, alice, strings.Repeat("a", 64))
	if err := alice.PutRef("main", strings.Repeat("a", 64), ""); err != nil {
		t.Fatalf("new account should work without a restart: %v", err)
	}

	// A non-admin cannot manage accounts.
	if err := alice.AddUser(User{Name: "eve", Token: "te"}); err == nil {
		t.Fatal("non-admin must not add accounts")
	}

	// Removing the account revokes its token.
	if err := admin.RemoveUser("alice"); err != nil {
		t.Fatal(err)
	}
	if err := alice.PutRef("main", strings.Repeat("b", 64), strings.Repeat("a", 64)); err == nil {
		t.Fatal("removed account token must stop working")
	}
}

func TestAccountsPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	accountsFile := filepath.Join(dir, "accounts.json")
	if err := os.WriteFile(accountsFile, []byte(`[{"name":"root","token":"admin","admin":true}]`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, ts := newTestServer(t, Config{Root: dir, AccountsFile: accountsFile})
	if err := NewClient(ts.URL, "admin").AddUser(User{Name: "bob", Token: "tb"}); err != nil {
		t.Fatal(err)
	}

	// A fresh server over the same files must know bob.
	srv2, err := NewServerConfig(Config{Root: dir, AccountsFile: accountsFile})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := srv2.accounts.authenticate("tb"); !ok {
		t.Fatal("account added at runtime did not persist")
	}
}

func TestTeamNamespaceSharing(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{
		{Name: "alice", Token: "ta", Teams: []string{"acme"}},
		{Name: "bob", Token: "tb", Teams: []string{"acme"}},
		{Name: "mallory", Token: "tm"},
	}})

	id := strings.Repeat("c", 64)
	// alice pushes a ref into the shared team namespace.
	aliceTeam := NewClientNS(ts.URL, "ta", "acme")
	putEmptyState(t, aliceTeam, id)
	if err := aliceTeam.PutRef("release", id, ""); err != nil {
		t.Fatal(err)
	}
	// bob, in the same team, sees it.
	bobTeam := NewClientNS(ts.URL, "tb", "acme")
	got, err := bobTeam.GetRef("release")
	if err != nil || got != id {
		t.Fatalf("teammate GetRef = %q, %v; want %q", got, err, id)
	}
	// mallory, not in the team, is refused.
	mallory := NewClientNS(ts.URL, "tm", "acme")
	if _, err := mallory.GetRef("release"); err == nil {
		t.Fatal("non-member must not read a team ref")
	}
	// A personal ref of the same name is a different object.
	if got, _ := NewClient(ts.URL, "tb").GetRef("release"); got != "" {
		t.Fatal("team and personal namespaces must not collide")
	}
}

func TestRateLimit(t *testing.T) {
	_, ts := newTestServer(t, Config{
		Users:      []User{{Name: "u", Token: "t"}},
		RatePerSec: 1,
		RateBurst:  2,
	})
	c := NewClient(ts.URL, "t")
	// Burst of 2 read requests should pass, the third is limited.
	_, _ = c.GetRef("x")
	_, _ = c.GetRef("x")
	if _, err := c.GetRef("x"); err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("third request should be rate limited, got %v", err)
	}
}

func TestAuditLog(t *testing.T) {
	dir := t.TempDir()
	auditFile := filepath.Join(dir, "audit.log")
	_, ts := newTestServer(t, Config{Root: dir, Users: []User{{Name: "u", Token: "t"}}, AuditFile: auditFile})

	c := NewClient(ts.URL, "t")
	putEmptyState(t, c, strings.Repeat("d", 64))
	if err := c.PutRef("main", strings.Repeat("d", 64), ""); err != nil {
		t.Fatal(err)
	}
	_, _ = c.GetRef("main") // read: must NOT be audited

	data, err := os.ReadFile(auditFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1
	if lines != 2 {
		t.Fatalf("audit log has %d lines, want 2 (the mutating state and ref PUTs)", lines)
	}
	if !strings.Contains(string(data), `"account":"u"`) || !strings.Contains(string(data), `"method":"PUT"`) {
		t.Fatalf("audit entry missing fields: %s", data)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}})
	c := NewClient(ts.URL, "t")
	_ = c.PutBlockRoundTrip(t)

	resp, err := ts.Client().Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	body := buf.String()
	for _, want := range []string{"fvs_requests_total", "fvs_blocks_added_total", "fvs_bytes_uploaded_total"} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

// PutBlockRoundTrip uploads one block so metrics have something to report.
func (c *Client) PutBlockRoundTrip(t *testing.T) error {
	t.Helper()
	data := []byte("metrics-block")
	id := core.ContentID(data)
	if _, err := c.PutBlocks([]core.BlockID{id}, func(core.BlockID) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	return nil
}

func TestS3BackendRoundTrip(t *testing.T) {
	// A real S3-compatible server (gofakes3) in-process.
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(s3srv.Close)

	backend, err := NewS3Backend(S3Config{
		Endpoint:  strings.TrimPrefix(s3srv.URL, "http://"),
		Bucket:    "fvs",
		AccessKey: "key",
		SecretKey: "secret",
		Region:    "us-east-1",
		Prefix:    "blocks",
	})
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("s3"), 4096)
	id, err := backend.Put(data)
	if err != nil {
		t.Fatal(err)
	}
	if id != core.ContentID(data) {
		t.Fatal("Put returned the wrong content id")
	}
	ok, err := backend.Has(id)
	if err != nil || !ok {
		t.Fatalf("Has after Put = %v, %v", ok, err)
	}
	got, err := backend.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("S3 round trip corrupted the block")
	}
	list, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != id || list[0].Size != int64(len(data)) {
		t.Fatalf("List = %+v", list)
	}
	if err := backend.Delete(id); err != nil {
		t.Fatal(err)
	}
	if ok, _ := backend.Has(id); ok {
		t.Fatal("block still present after Delete")
	}
}

// TestS3BackedServer wires the fake S3 as the block store behind a full remote
// server and pushes/pulls through it.
func TestS3BackedServer(t *testing.T) {
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(s3srv.Close)

	backend, err := NewS3Backend(S3Config{
		Endpoint:  strings.TrimPrefix(s3srv.URL, "http://"),
		Bucket:    "fvs",
		AccessKey: "key",
		SecretKey: "secret",
		Region:    "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}, Blocks: backend})

	c := NewClient(ts.URL, "t")
	data := bytes.Repeat([]byte("payload"), 2000)
	id := core.ContentID(data)

	missing, err := c.MissingBlocks([]core.BlockID{id})
	if err != nil || len(missing) != 1 {
		t.Fatalf("missing = %v, %v", missing, err)
	}
	if _, err := c.PutBlocks([]core.BlockID{id}, func(core.BlockID) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	missing, err = c.MissingBlocks([]core.BlockID{id})
	if err != nil || len(missing) != 0 {
		t.Fatalf("after upload, missing = %v, %v", missing, err)
	}
	var got []byte
	if err := c.FetchBlocks([]core.BlockID{id}, func(_ core.BlockID, b []byte) error { got = b; return nil }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("block fetched from the S3-backed server differs")
	}
}

func TestWhoamiAndListRefs(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{
		{Name: "alice", Token: "ta", QuotaBytes: 5 << 20, Teams: []string{"acme"}},
	}})
	c := NewClient(ts.URL, "ta")

	resp, err := ts.Client().Do(mustReq(t, "GET", ts.URL+"/v1/whoami", "ta", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var who struct {
		Name       string   `json:"name"`
		Admin      bool     `json:"admin"`
		Teams      []string `json:"teams"`
		QuotaBytes int64    `json:"quota_bytes"`
		UsedBytes  int64    `json:"used_bytes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&who); err != nil {
		t.Fatal(err)
	}
	if who.Name != "alice" || who.QuotaBytes != 5<<20 || len(who.Teams) != 1 {
		t.Fatalf("whoami = %+v", who)
	}

	// Two personal refs and one team ref: the list is namespace-scoped.
	for _, seed := range []string{"a", "b", "c"} {
		putEmptyState(t, c, strings.Repeat(seed, 64))
	}
	if err := c.PutRef("main", strings.Repeat("a", 64), ""); err != nil {
		t.Fatal(err)
	}
	if err := c.PutRef("dev", strings.Repeat("b", 64), ""); err != nil {
		t.Fatal(err)
	}
	if err := NewClientNS(ts.URL, "ta", "acme").PutRef("release", strings.Repeat("c", 64), ""); err != nil {
		t.Fatal(err)
	}

	list := func(ns string) map[string]string {
		req := mustReq(t, "GET", ts.URL+"/v1/refs", "ta", ns)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Refs []struct{ Name, ID string } `json:"refs"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		m := map[string]string{}
		for _, r := range out.Refs {
			m[r.Name] = r.ID
		}
		return m
	}
	personal := list("")
	if len(personal) != 2 || personal["main"] == "" || personal["dev"] == "" {
		t.Fatalf("personal refs = %v", personal)
	}
	team := list("acme")
	if len(team) != 1 || team["release"] == "" {
		t.Fatalf("team refs = %v", team)
	}
}

func TestCORSHeadersAndPreflight(t *testing.T) {
	_, ts := newTestServer(t, Config{
		Users:      []User{{Name: "u", Token: "t"}},
		CORSOrigin: "https://ui.example.org",
	})

	// Preflight passes without auth.
	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/v1/refs", nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://ui.example.org" {
		t.Fatalf("allow-origin = %q", got)
	}
	if !strings.Contains(resp.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatal("allow-headers missing Authorization")
	}

	// Regular responses carry the origin too.
	resp2, err := ts.Client().Do(mustReq(t, "GET", ts.URL+"/v1/whoami", "t", ""))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if got := resp2.Header.Get("Access-Control-Allow-Origin"); got != "https://ui.example.org" {
		t.Fatalf("allow-origin on GET = %q", got)
	}
}

func mustReq(t *testing.T, method, url, token, namespace string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if namespace != "" {
		req.Header.Set(namespaceHeader, namespace)
	}
	return req
}
