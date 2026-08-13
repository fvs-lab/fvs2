package remote

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"testing"

	core "github.com/fvs-lab/core"
)

func TestSingleBlockPutRejectsOversizedBody(t *testing.T) {
	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}})
	c := NewClient(ts.URL, "t")

	big := make([]byte, maxFrame+1)
	if err := c.PutBlock(core.ContentID(big), big); err == nil {
		t.Fatal("a block past the frame cap must be rejected, not truncated")
	}

	fits := bytes.Repeat([]byte("x"), 4096)
	if err := c.PutBlock(core.ContentID(fits), fits); err != nil {
		t.Fatalf("block within the cap: %v", err)
	}
}

func TestBatchUploadCapsFrameCount(t *testing.T) {
	old := maxBatchFrames
	maxBatchFrames = 3
	t.Cleanup(func() { maxBatchFrames = old })

	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}})
	c := NewClient(ts.URL, "t")

	var ids []core.BlockID
	data := map[core.BlockID][]byte{}
	for i := 0; i < 4; i++ {
		b := []byte{byte(i), byte(i), byte(i)}
		id := core.ContentID(b)
		ids = append(ids, id)
		data[id] = b
	}
	_, err := c.PutBlocks(ids, func(id core.BlockID) ([]byte, error) { return data[id], nil })
	if err == nil {
		t.Fatal("a batch over the frame count cap must be rejected")
	}
}

func TestGzipUploadCapsDecompressedBytes(t *testing.T) {
	old := maxDecompressedPerRequest
	maxDecompressedPerRequest = 1 << 16
	t.Cleanup(func() { maxDecompressedPerRequest = old })

	_, ts := newTestServer(t, Config{Users: []User{{Name: "u", Token: "t"}}})

	// A gzip body expanding past the cap: a few frames of highly
	// compressible zeros.
	var raw bytes.Buffer
	gz := gzip.NewWriter(&raw)
	for i := 0; i < 8; i++ {
		frame := make([]byte, 32<<10)
		frame[0] = byte(i)
		if err := writeFrame(gz, frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/blocks/batch", bytes.NewReader(raw.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer t")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("a gzip bomb past the decompressed cap must be rejected")
	}
}
