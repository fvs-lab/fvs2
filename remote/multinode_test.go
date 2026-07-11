package remote

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	core "fvs-v2-core"
)

// twoServers runs two independent Server instances over the same root, the
// multi-node layout (two processes or machines sharing one storage root).
func twoServers(t *testing.T, cfg Config) (*httptest.Server, *httptest.Server, string) {
	t.Helper()
	if cfg.Root == "" {
		cfg.Root = t.TempDir()
	}
	a, err := NewServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewServerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tsA, tsB := httptest.NewServer(a), httptest.NewServer(b)
	t.Cleanup(func() { tsA.Close(); tsB.Close() })
	return tsA, tsB, cfg.Root
}

func hexOf(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

// TestCASSerializesAcrossServers hammers the same ref through two server
// instances. Every successful swap must have observed a unique previous
// value: a duplicate would mean two writers won against the same state, i.e.
// a lost update.
func TestCASSerializesAcrossServers(t *testing.T) {
	tsA, tsB, _ := twoServers(t, Config{Users: []User{{Name: "u", Token: "t"}}})
	clients := []*Client{NewClient(tsA.URL, "t"), NewClient(tsB.URL, "t")}

	const workers = 8
	var mu sync.Mutex
	seenOld := map[string]bool{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := clients[w%2]
			id := hexOf(w + 100)
			for {
				cur, err := c.GetRef("main")
				if err != nil {
					t.Error(err)
					return
				}
				err = c.PutRef("main", id, cur)
				if err == nil {
					mu.Lock()
					if seenOld[cur] {
						t.Errorf("two swaps won against the same old value %q: lost update", cur)
					}
					seenOld[cur] = true
					mu.Unlock()
					return
				}
				var conflict *ErrRefConflict
				if !errorsAs(err, &conflict) {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if len(seenOld) != workers {
		t.Fatalf("successful swaps = %d, want %d", len(seenOld), workers)
	}
}

func errorsAs(err error, target **ErrRefConflict) bool {
	c, ok := err.(*ErrRefConflict)
	if ok {
		*target = c
	}
	return ok
}

// TestQuotaSharedAcrossServers uploads through both instances against one
// quota: the shared counter must reject the overflow no matter which node
// takes the request.
func TestQuotaSharedAcrossServers(t *testing.T) {
	tsA, tsB, root := twoServers(t, Config{Users: []User{
		{Name: "u", Token: "t", QuotaBytes: 100 << 10},
	}})
	clients := []*Client{NewClient(tsA.URL, "t"), NewClient(tsB.URL, "t")}

	blockSize := 30 << 10
	accepted := 0
	for i := 0; i < 6; i++ {
		data := append([]byte{byte(i)}, make([]byte, blockSize-1)...)
		id := core.ContentID(data)
		_, err := clients[i%2].PutBlocks([]core.BlockID{id}, func(core.BlockID) ([]byte, error) { return data, nil })
		if err == nil {
			accepted++
		} else if !strings.Contains(err.Error(), "quota exceeded") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	// 100KiB quota, 30KiB blocks: exactly 3 fit.
	if accepted != 3 {
		t.Fatalf("accepted %d blocks, want 3", accepted)
	}

	usage := map[string]int64{}
	b, err := os.ReadFile(filepath.Join(root, "usage.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &usage); err != nil {
		t.Fatal(err)
	}
	if usage["u"] != int64(3*blockSize) {
		t.Fatalf("persisted usage = %d, want %d", usage["u"], 3*blockSize)
	}
}

// TestAccountChangesVisibleAcrossServers adds an account through one instance
// and authenticates against the other, which shares only the accounts file.
func TestAccountChangesVisibleAcrossServers(t *testing.T) {
	root := t.TempDir()
	accountsFile := filepath.Join(root, "accounts.json")
	if err := os.WriteFile(accountsFile, []byte(`[{"name":"root","token":"admin","admin":true}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	tsA, tsB, _ := twoServers(t, Config{Root: root, AccountsFile: accountsFile})

	if err := NewClient(tsA.URL, "admin").AddUser(User{Name: "carol", Token: "tc"}); err != nil {
		t.Fatal(err)
	}
	// The other instance never saw this token: it must reload and accept it.
	carol := NewClient(tsB.URL, "tc")
	if err := carol.PutRef("main", hexOf(7), ""); err != nil {
		t.Fatalf("account added on node A must work on node B: %v", err)
	}
}
