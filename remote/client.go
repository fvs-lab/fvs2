package remote

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	core "fvs-v2-core"
)

// batchBytes is the upload batch target: blocks are grouped until a batch
// reaches this many bytes, then sent as one compressed request.
const batchBytes = 8 << 20

// ErrRefConflict is returned when a compare-and-swap ref update loses against
// a concurrent push; RemoteID carries the value currently on the remote.
type ErrRefConflict struct {
	Ref      string
	RemoteID string
}

func (e *ErrRefConflict) Error() string {
	return fmt.Sprintf("ref %q changed on the remote (now %.12s); pull first or force", e.Ref, e.RemoteID)
}

// Client talks to an FVS remote.
type Client struct {
	base  string
	token string
	http  *http.Client
}

func NewClient(base, token string) *Client {
	return &Client{
		base:  strings.TrimRight(base, "/"),
		token: token,
		http:  &http.Client{Timeout: 5 * time.Minute},
	}
}

func (c *Client) do(method, path string, body io.Reader, contentType, contentEncoding string) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return c.http.Do(req)
}

func unexpected(resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("remote: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
}

// MissingBlocks returns the subset of ids the remote does not have yet.
func (c *Client) MissingBlocks(ids []core.BlockID) ([]core.BlockID, error) {
	req := struct {
		Blocks []core.BlockID `json:"blocks"`
	}{Blocks: ids}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(http.MethodPost, "/v1/blocks/check", bytes.NewReader(payload), "application/json", "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, unexpected(resp)
	}
	var out struct {
		Missing []core.BlockID `json:"missing"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Missing, nil
}

// PutBlocks uploads blocks in gzip-compressed batches; getData provides each
// block's content. It returns the number of bytes read from getData.
func (c *Client) PutBlocks(ids []core.BlockID, getData func(core.BlockID) ([]byte, error)) (int64, error) {
	var total int64
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	batched := 0

	flush := func() error {
		if batched == 0 {
			return nil
		}
		if err := gz.Close(); err != nil {
			return err
		}
		resp, err := c.do(http.MethodPost, "/v1/blocks/batch", bytes.NewReader(buf.Bytes()), "application/octet-stream", "gzip")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return unexpected(resp)
		}
		buf.Reset()
		gz.Reset(&buf)
		batched = 0
		return nil
	}

	for _, id := range ids {
		data, err := getData(id)
		if err != nil {
			return total, fmt.Errorf("read block %s: %w", id, err)
		}
		total += int64(len(data))
		if err := writeFrame(gz, data); err != nil {
			return total, err
		}
		batched++
		if buf.Len() >= batchBytes {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	return total, flush()
}

// FetchBlocks downloads blocks in one framed request and hands each verified
// block to put. A block failing its content check aborts the fetch.
func (c *Client) FetchBlocks(ids []core.BlockID, put func(core.BlockID, []byte) error) error {
	if len(ids) == 0 {
		return nil
	}
	payload, err := json.Marshal(struct {
		Blocks []core.BlockID `json:"blocks"`
	}{Blocks: ids})
	if err != nil {
		return err
	}
	resp, err := c.do(http.MethodPost, "/v1/blocks/fetch", bytes.NewReader(payload), "application/json", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return unexpected(resp)
	}
	for _, id := range ids {
		data, err := readFrame(resp.Body)
		if err != nil {
			return fmt.Errorf("remote: fetch of %s: %w", id, err)
		}
		if core.ContentID(data) != id {
			return fmt.Errorf("remote: block %s failed integrity check", id)
		}
		if err := put(id, data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) PutBlock(id core.BlockID, data []byte) error {
	resp, err := c.do(http.MethodPut, "/v1/blocks/"+string(id), bytes.NewReader(data), "application/octet-stream", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return unexpected(resp)
	}
	return nil
}

func (c *Client) GetBlock(id core.BlockID) ([]byte, error) {
	resp, err := c.do(http.MethodGet, "/v1/blocks/"+string(id), nil, "", "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, unexpected(resp)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// Verify the content address before handing the block out, so a
	// misbehaving remote cannot inject data.
	if core.ContentID(data) != id {
		return nil, fmt.Errorf("remote: block %s failed integrity check", id)
	}
	return data, nil
}

func (c *Client) PutState(id string, doc []byte) error {
	resp, err := c.do(http.MethodPut, "/v1/states/"+id, bytes.NewReader(doc), "application/json", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return unexpected(resp)
	}
	return nil
}

func (c *Client) GetState(id string) ([]byte, error) {
	resp, err := c.do(http.MethodGet, "/v1/states/"+id, nil, "", "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, unexpected(resp)
	}
	return io.ReadAll(resp.Body)
}

// GetRef returns the state id a ref points at, or "" when the ref does not
// exist on the remote.
func (c *Client) GetRef(name string) (string, error) {
	resp, err := c.do(http.MethodGet, "/v1/refs/"+name, nil, "", "")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", unexpected(resp)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.ID, nil
}

// PutRef updates a ref from old to id; old == "" asserts the ref does not
// exist yet. A concurrent change surfaces as *ErrRefConflict.
func (c *Client) PutRef(name, id, old string) error {
	payload, err := json.Marshal(map[string]string{"id": id, "old": old})
	if err != nil {
		return err
	}
	resp, err := c.do(http.MethodPut, "/v1/refs/"+name, bytes.NewReader(payload), "application/json", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		var out struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return &ErrRefConflict{Ref: name, RemoteID: out.ID}
	default:
		return unexpected(resp)
	}
}

// DeleteRef removes a ref on the remote.
func (c *Client) DeleteRef(name string) error {
	resp, err := c.do(http.MethodDelete, "/v1/refs/"+name, nil, "", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return unexpected(resp)
	}
	return nil
}

// GCResult reports what a server-side garbage collection removed.
type GCResult struct {
	RemovedBlocks int64 `json:"removed_blocks"`
	RemovedStates int64 `json:"removed_states"`
	FreedBytes    int64 `json:"freed_bytes"`
}

// GC runs garbage collection on the remote (admin only). Objects newer than
// grace are kept so in-flight pushes are never collected.
func (c *Client) GC(grace time.Duration) (GCResult, error) {
	path := fmt.Sprintf("/v1/gc?grace_seconds=%d", int(grace.Seconds()))
	resp, err := c.do(http.MethodPost, path, nil, "", "")
	if err != nil {
		return GCResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return GCResult{}, unexpected(resp)
	}
	var out GCResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return GCResult{}, err
	}
	return out, nil
}
