package remote

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"

	"github.com/fvs-lab/fvs2/attest"
)

func (s *Server) attestDir() string { return filepath.Join(s.root, "attestations") }

func (s *Server) attestPath(id string) string {
	return filepath.Join(s.attestDir(), id+".json")
}

// putAttestations verifies each signature and stores the valid ones,
// content-addressed by attestation id. Binding a key to an account is a
// forge-layer concern, not the bare protocol's.
func (s *Server) putAttestations(w http.ResponseWriter, r *http.Request) {
	rd, err := body(w, r, maxJSONBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rd.Close()
	var req struct {
		Attestations []attest.Attestation `json:"attestations"`
	}
	if err := json.NewDecoder(io.LimitReader(rd, 8<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(s.attestDir(), 0o755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stored, rejected := 0, 0
	for _, a := range req.Attestations {
		if attest.Verify(a) != nil {
			rejected++
			continue
		}
		if err := writeFileAtomic(s.attestPath(a.ID()), a.Encode()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stored++
	}
	writeJSON(w, map[string]int{"stored": stored, "rejected": rejected})
}

// getAttestations lists stored attestations, optionally filtered to one state.
func (s *Server) getAttestations(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	entries, err := os.ReadDir(s.attestDir())
	if os.IsNotExist(err) {
		writeJSON(w, map[string]any{"attestations": []attest.Attestation{}})
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]attest.Attestation, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.attestDir(), e.Name()))
		if err != nil {
			continue
		}
		var a attest.Attestation
		if json.Unmarshal(b, &a) != nil {
			continue
		}
		if state != "" && a.State != state {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SignedAt < out[j].SignedAt })
	writeJSON(w, map[string]any{"attestations": out})
}

// capabilities advertises optional protocol families so clients can skip one
// the server does not support.
func (s *Server) capabilities(w http.ResponseWriter) {
	writeJSON(w, map[string]any{"attestations": true, "expand": true})
}
