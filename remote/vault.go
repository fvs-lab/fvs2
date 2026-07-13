package remote

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"

	"fvs2/vault"
)

// hubRoot strips the repository path from the base URL, leaving the hub origin
// where account and well-known endpoints live.
func (c *Client) hubRoot() string {
	u, err := url.Parse(c.base)
	if err != nil {
		return c.base
	}
	return u.Scheme + "://" + u.Host
}

// Host returns the hub host, used to key the client's tree-head pin.
func (c *Client) Host() string {
	u, err := url.Parse(c.base)
	if err != nil || u.Host == "" {
		return c.base
	}
	return u.Host
}

// DepositKey registers a signing public key on the caller's account.
func (c *Client) DepositKey(public string) error {
	payload, _ := json.Marshal(map[string]string{"public": public})
	req, err := http.NewRequest(http.MethodPost, c.hubRoot()+"/api/v1/account/keys", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return unexpected(resp)
	}
	return nil
}

// VaultLogKey fetches the transparency log's published identity.
func (c *Client) VaultLogKey() (vault.LogKey, error) {
	var out vault.LogKey
	err := c.getJSON("/v1/vault/log-key", &out)
	return out, err
}

// VaultSTH fetches a fresh signed tree head.
func (c *Client) VaultSTH() (vault.SignedTreeHead, error) {
	var out vault.SignedTreeHead
	err := c.getJSON("/v1/vault/sth", &out)
	return out, err
}

// VaultProof fetches an attestation's inclusion proof. ok is false when the
// hub has not admitted the attestation.
func (c *Client) VaultProof(attestationID string) (vault.Proof, bool, error) {
	resp, err := c.do(http.MethodGet, "/v1/vault/proof?attestation="+url.QueryEscape(attestationID), nil, "", "")
	if err != nil {
		return vault.Proof{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return vault.Proof{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return vault.Proof{}, false, unexpected(resp)
	}
	var out vault.Proof
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return vault.Proof{}, false, err
	}
	return out, true, nil
}

// VaultConsistency fetches a consistency proof from a prior tree size to the
// current head, with the current head.
func (c *Client) VaultConsistency(first uint64) ([]string, vault.SignedTreeHead, error) {
	var out struct {
		Proof []string             `json:"proof"`
		STH   vault.SignedTreeHead `json:"sth"`
	}
	err := c.getJSON("/v1/vault/consistency?first="+url.QueryEscape(uitoa(first)), &out)
	return out.Proof, out.STH, err
}

func (c *Client) getJSON(path string, v any) error {
	resp, err := c.do(http.MethodGet, path, nil, "", "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return unexpected(resp)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// hubGetJSON performs an authenticated GET against a hub-root path.
func (c *Client) hubGetJSON(path string, v any) error {
	req, err := http.NewRequest(http.MethodGet, c.hubRoot()+path, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return unexpected(resp)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func (c *Client) hubPost(path string, body any, wantStatus int) error {
	payload, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, c.hubRoot()+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		return unexpected(resp)
	}
	return nil
}

// Witness is a registered log observer.
type Witness struct {
	Public string `json:"public"`
	Label  string `json:"label,omitempty"`
}

// RegisterWitness enrolls a witness public key (admin).
func (c *Client) RegisterWitness(public, label string) error {
	return c.hubPost("/api/v1/vault/witnesses", map[string]string{"public": public, "label": label}, http.StatusCreated)
}

// Witnesses lists the registered witnesses.
func (c *Client) Witnesses() ([]Witness, error) {
	var out struct {
		Witnesses []Witness `json:"witnesses"`
	}
	err := c.hubGetJSON("/api/v1/vault/witnesses", &out)
	return out.Witnesses, err
}

// Cosign submits a witness cosignature over a tree head.
func (c *Client) Cosign(cs vault.Cosignature) error {
	return c.hubPost("/api/v1/vault/cosign", cs, http.StatusNoContent)
}

// Cosignatures lists the cosignatures stored at a tree size.
func (c *Client) Cosignatures(size uint64) ([]vault.Cosignature, error) {
	var out struct {
		Cosignatures []vault.Cosignature `json:"cosignatures"`
	}
	err := c.hubGetJSON("/api/v1/vault/cosignatures?size="+url.QueryEscape(uitoa(size)), &out)
	return out.Cosignatures, err
}

// Anchor is a tree head's public-timestamping status.
type Anchor struct {
	Size   uint64 `json:"size"`
	Root   string `json:"root"`
	Status string `json:"status"`
	Height uint64 `json:"height,omitempty"`
}

// AnchorHead submits the log's current tree head for public timestamping (admin).
func (c *Client) AnchorHead() (Anchor, error) {
	payload, _ := json.Marshal(struct{}{})
	req, err := http.NewRequest(http.MethodPost, c.hubRoot()+"/api/v1/vault/anchor", bytes.NewReader(payload))
	if err != nil {
		return Anchor{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Anchor{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return Anchor{}, unexpected(resp)
	}
	var a Anchor
	return a, json.NewDecoder(resp.Body).Decode(&a)
}

// UpgradeAnchors asks the hub to complete pending anchors against the
// calendars (admin).
func (c *Client) UpgradeAnchors() error {
	return c.hubPost("/api/v1/vault/anchor/upgrade", struct{}{}, http.StatusNoContent)
}
