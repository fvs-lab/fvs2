package remote

import (
	"fmt"
	"strings"
	"testing"

	core "fvs-v2-core"
)

// putEmptyState uploads a minimal valid state so tests can point refs at it.
func putEmptyState(t *testing.T, c *Client, id string) {
	t.Helper()
	doc := fmt.Sprintf(`{"id":%q,"format":2,"block_size":4096,"files":[]}`, id)
	if err := c.PutState(id, []byte(doc)); err != nil {
		t.Fatal(err)
	}
}

func TestPutStateRejectsInvalidDocuments(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}})
	c := NewClient(ts.URL, "t")

	id := strings.Repeat("a", 64)
	missing := strings.Repeat("b", 64)
	cases := []struct {
		name string
		doc  string
	}{
		{"id mismatch", fmt.Sprintf(`{"id":%q,"format":2,"files":[]}`, strings.Repeat("f", 64))},
		{"newer format", fmt.Sprintf(`{"id":%q,"format":99,"files":[]}`, id)},
		{"escaping path", fmt.Sprintf(`{"id":%q,"format":2,"files":[{"path":"../evil","mode":420,"size":0,"blocks":[]}]}`, id)},
		{"absolute path", fmt.Sprintf(`{"id":%q,"format":2,"files":[{"path":"/etc/passwd","mode":420,"size":0,"blocks":[]}]}`, id)},
		{"missing block", fmt.Sprintf(`{"id":%q,"format":2,"files":[{"path":"f","mode":420,"size":4,"blocks":[%q],"block_sizes":[4]}]}`, id, missing)},
		{"size mismatch", fmt.Sprintf(`{"id":%q,"format":2,"files":[{"path":"f","mode":420,"size":9,"blocks":[%q],"block_sizes":[4]}]}`, id, missing)},
		{"missing tree closure", fmt.Sprintf(`{"id":%q,"format":3,"root_tree":%q}`, id, missing)},
	}
	for _, tc := range cases {
		if err := c.PutState(id, []byte(tc.doc)); err == nil {
			t.Errorf("%s: PutState must fail", tc.name)
		}
	}

	// A well-formed state whose blocks were uploaded first passes.
	data := []byte("valid block")
	bid := core.ContentID(data)
	if _, err := c.PutBlocks([]core.BlockID{bid}, func(core.BlockID) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	good := fmt.Sprintf(`{"id":%q,"format":2,"files":[{"path":"f","mode":420,"size":%d,"blocks":[%q],"block_sizes":[%d]}]}`,
		id, len(data), bid, len(data))
	if err := c.PutState(id, []byte(good)); err != nil {
		t.Fatalf("valid state refused: %v", err)
	}
}

func TestPutRefRequiresExistingValidState(t *testing.T) {
	root := t.TempDir()
	_, ts := newTestServer(t, Config{Root: root, Users: []User{{Name: "u", Token: "t"}}})
	c := NewClient(ts.URL, "t")

	// A ref pointing at a state the remote never stored is refused.
	if err := c.PutRef("main", strings.Repeat("e", 64), ""); err == nil {
		t.Fatal("ref to a nonexistent state must fail")
	}

	id := strings.Repeat("1", 64)
	putEmptyState(t, c, id)
	if err := c.PutRef("main", id, ""); err != nil {
		t.Fatalf("ref to a stored valid state: %v", err)
	}

	// A state that fails validation now (e.g. tampered on disk after upload)
	// cannot be pointed at either.
	bad := strings.Repeat("2", 64)
	if err := writeFileAtomic(statePathOf(root, bad), []byte(`{"id":"nope"}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.PutRef("main", bad, id); err == nil {
		t.Fatal("ref to an invalid stored state must fail")
	}
}

func statePathOf(root, id string) string {
	return root + "/states/" + id + ".json"
}
