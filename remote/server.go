package remote

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	core "fvs-v2-core"
)

// namespaceHeader carries the ref namespace (an account name or a team) a
// request targets. Empty means the caller's own namespace.
const namespaceHeader = "X-Fvs-Namespace"

// maxFrame bounds a single block frame in batch transfers.
const maxFrame = 16 << 20

// Config configures a remote server.
type Config struct {
	Root         string       // local directory for states, refs and (by default) blocks
	AccountsFile string       // JSON file the server owns; enables runtime account changes
	Users        []User       // seed accounts (used when AccountsFile is empty, e.g. tests)
	Blocks       BlockBackend // block store; nil means a filesystem store under Root/blocks
	RatePerSec   float64      // per-account request rate limit; 0 disables
	RateBurst    int          // rate limiter burst
	AuditFile    string       // append-only audit log; "" disables
}

// Server is the reference implementation of the FVS remote protocol. See
// docs/REMOTE.md for the endpoints.
type Server struct {
	root     string
	blocks   BlockBackend
	accounts *accounts
	limiter  *rateLimiter
	metrics  *metrics
	audit    *auditLog

	mu    sync.Mutex // guards usage and gc
	usage map[string]int64
}

// NewServer serves a remote from root. token == "" leaves the server open;
// otherwise it behaves as a single admin account without quota.
func NewServer(root, token string) (*Server, error) {
	cfg := Config{Root: root}
	if token != "" {
		cfg.Users = []User{{Name: "default", Token: token, Admin: true}}
	}
	return NewServerConfig(cfg)
}

// NewServerWithUsers serves a remote with per-account access, seeded in memory.
func NewServerWithUsers(root string, users []User) (*Server, error) {
	return NewServerConfig(Config{Root: root, Users: users})
}

// NewServerConfig builds a server from a full configuration.
func NewServerConfig(cfg Config) (*Server, error) {
	for _, dir := range []string{"states", "refs"} {
		if err := os.MkdirAll(filepath.Join(cfg.Root, dir), 0o755); err != nil {
			return nil, err
		}
	}

	blocks := cfg.Blocks
	if blocks == nil {
		fs, err := newFSBackend(filepath.Join(cfg.Root, "blocks"))
		if err != nil {
			return nil, err
		}
		blocks = fs
	}

	seed := cfg.Users
	if cfg.AccountsFile != "" {
		if existing, err := LoadUsers(cfg.AccountsFile); err == nil {
			seed = existing
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	accts, err := newAccounts(cfg.AccountsFile, seed)
	if err != nil {
		return nil, err
	}

	audit, err := newAuditLog(cfg.AuditFile)
	if err != nil {
		return nil, err
	}

	s := &Server{
		root:     cfg.Root,
		blocks:   blocks,
		accounts: accts,
		limiter:  newRateLimiter(cfg.RatePerSec, cfg.RateBurst),
		metrics:  newMetrics(),
		audit:    audit,
		usage:    map[string]int64{},
	}
	if err := s.loadUsage(); err != nil {
		return nil, err
	}
	return s, nil
}

var (
	hexID   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	refName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
)

func (s *Server) usagePath() string { return filepath.Join(s.root, "usage.json") }

func (s *Server) loadUsage() error {
	b, err := os.ReadFile(s.usagePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &s.usage)
}

// chargeNewBytes reserves n bytes of quota for account; it fails without
// charging when the quota would be exceeded.
func (s *Server) chargeNewBytes(u User, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u.QuotaBytes > 0 && s.usage[u.Name]+n > u.QuotaBytes {
		return fmt.Errorf("quota exceeded: %d of %d bytes used", s.usage[u.Name], u.QuotaBytes)
	}
	s.usage[u.Name] += n
	b, err := json.MarshalIndent(s.usage, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.usagePath(), append(b, '\n'))
}

// body returns the request body, transparently decompressing gzip uploads.
func body(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		return gzip.NewReader(r.Body)
	}
	return r.Body, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.metrics.requests.Add(1)

	// /metrics is unauthenticated so a scraper needs no account.
	if r.URL.Path == "/metrics" {
		s.metrics.write(w)
		return
	}

	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	user, ok := s.accounts.authenticate(token)
	if !ok {
		s.metrics.requestErrors.Add(1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.limiter.allow(user.Name) {
		s.metrics.rateLimited.Add(1)
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	rec := &statusRecorder{ResponseWriter: w}
	s.route(rec, r, user)

	if rec.status >= 400 {
		s.metrics.requestErrors.Add(1)
	}
	if isMutating(r.Method) {
		s.audit.record(user.Name, r.Method, r.URL.Path, rec.status)
	}
}

func (s *Server) route(w *statusRecorder, r *http.Request, user User) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	switch {
	case path == "blocks/check" && r.Method == http.MethodPost:
		s.checkBlocks(w, r)
	case path == "blocks/batch" && r.Method == http.MethodPost:
		s.putBlocks(w, r, user)
	case path == "blocks/fetch" && r.Method == http.MethodPost:
		s.fetchBlocks(w, r)
	case strings.HasPrefix(path, "blocks/"):
		s.block(w, r, user, strings.TrimPrefix(path, "blocks/"))
	case strings.HasPrefix(path, "states/"):
		s.state(w, r, strings.TrimPrefix(path, "states/"))
	case strings.HasPrefix(path, "refs/"):
		s.ref(w, r, user, strings.TrimPrefix(path, "refs/"))
	case path == "admin/accounts" && r.Method == http.MethodGet:
		s.listAccounts(w, user)
	case path == "admin/accounts" && r.Method == http.MethodPost:
		s.addAccount(w, r, user)
	case strings.HasPrefix(path, "admin/accounts/") && r.Method == http.MethodDelete:
		s.removeAccount(w, user, strings.TrimPrefix(path, "admin/accounts/"))
	case path == "gc" && r.Method == http.MethodPost:
		s.gc(w, r, user)
	default:
		http.NotFound(w, r)
	}
}

// checkBlocks answers which of the posted block ids are missing on this
// remote. This is the dedup primitive: a push only uploads what the remote
// does not already have, whoever uploaded it first.
func (s *Server) checkBlocks(w http.ResponseWriter, r *http.Request) {
	rd, err := body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rd.Close()
	var req struct {
		Blocks []string `json:"blocks"`
	}
	if err := json.NewDecoder(io.LimitReader(rd, 64<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	missing := make([]string, 0)
	for _, id := range req.Blocks {
		if !hexID.MatchString(id) {
			http.Error(w, "invalid block id", http.StatusBadRequest)
			return
		}
		ok, err := s.blocks.Has(core.BlockID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			missing = append(missing, id)
		}
	}
	writeJSON(w, map[string][]string{"missing": missing})
}

// putBlocks stores a stream of length-prefixed frames. Every frame is
// content-addressed on arrival; only bytes new to the store count against the
// uploader's quota.
func (s *Server) putBlocks(w http.ResponseWriter, r *http.Request, user User) {
	rd, err := body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rd.Close()

	added := 0
	for {
		data, err := readFrame(rd)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		id := core.ContentID(data)
		ok, err := s.blocks.Has(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if ok {
			continue
		}
		if err := s.chargeNewBytes(user, int64(len(data))); err != nil {
			s.metrics.quotaRejected.Add(1)
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if _, err := s.blocks.Put(data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.metrics.blocksAdded.Add(1)
		s.metrics.bytesUploaded.Add(int64(len(data)))
		added++
	}
	writeJSON(w, map[string]int{"added": added})
}

// fetchBlocks streams the requested blocks back as length-prefixed frames, in
// the requested order, gzip-compressed when the client accepts it.
func (s *Server) fetchBlocks(w http.ResponseWriter, r *http.Request) {
	rd, err := body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer rd.Close()
	var req struct {
		Blocks []string `json:"blocks"`
	}
	if err := json.NewDecoder(io.LimitReader(rd, 64<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, id := range req.Blocks {
		if !hexID.MatchString(id) {
			http.Error(w, "invalid block id", http.StatusBadRequest)
			return
		}
		if ok, err := s.blocks.Has(core.BlockID(id)); err != nil || !ok {
			http.Error(w, "block not found: "+id, http.StatusNotFound)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	var out io.Writer = w
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		out = gz
	}
	for _, id := range req.Blocks {
		data, err := s.blocks.Get(core.BlockID(id))
		if err != nil {
			return // headers already sent; the client's hash check catches truncation
		}
		if err := writeFrame(out, data); err != nil {
			return
		}
		s.metrics.bytesServed.Add(int64(len(data)))
	}
}

func (s *Server) block(w http.ResponseWriter, r *http.Request, user User, id string) {
	if !hexID.MatchString(id) {
		http.Error(w, "invalid block id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, err := s.blocks.Get(core.BlockID(id))
		if errors.Is(err, core.ErrBlockNotFound) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
		s.metrics.bytesServed.Add(int64(len(data)))
	case http.MethodPut:
		rd, err := body(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer rd.Close()
		data, err := io.ReadAll(io.LimitReader(rd, maxFrame))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if string(core.ContentID(data)) != id {
			http.Error(w, "content does not match block id", http.StatusBadRequest)
			return
		}
		ok, err := s.blocks.Has(core.BlockID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			if err := s.chargeNewBytes(user, int64(len(data))); err != nil {
				s.metrics.quotaRejected.Add(1)
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			if _, err := s.blocks.Put(data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			s.metrics.blocksAdded.Add(1)
			s.metrics.bytesUploaded.Add(int64(len(data)))
		}
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) statePath(id string) string {
	return filepath.Join(s.root, "states", id+".json")
}

func (s *Server) state(w http.ResponseWriter, r *http.Request, id string) {
	if !hexID.MatchString(id) {
		http.Error(w, "invalid state id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		http.ServeFile(w, r, s.statePath(id))
	case http.MethodPut:
		rd, err := body(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer rd.Close()
		data, err := io.ReadAll(io.LimitReader(rd, 256<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !json.Valid(data) {
			http.Error(w, "state must be valid JSON", http.StatusBadRequest)
			return
		}
		if err := writeFileAtomic(s.statePath(id), data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// resolveNamespace picks the ref namespace for a request and checks the
// account may use it.
func (s *Server) resolveNamespace(r *http.Request, user User) (string, bool) {
	ns := r.Header.Get(namespaceHeader)
	if ns == "" {
		ns = user.Name
	}
	if !refName.MatchString(ns) {
		return "", false
	}
	return ns, s.accounts.allows(user, ns)
}

// refPath places refs under a namespace directory, so accounts cannot read or
// move branches outside their own namespace or their teams'.
func (s *Server) refPath(namespace, name string) string {
	return filepath.Join(s.root, "refs", namespace, name)
}

func (s *Server) ref(w http.ResponseWriter, r *http.Request, user User, name string) {
	if !refName.MatchString(name) {
		http.Error(w, "invalid ref name", http.StatusBadRequest)
		return
	}
	namespace, allowed := s.resolveNamespace(r, user)
	if !allowed {
		http.Error(w, "not a member of that namespace", http.StatusForbidden)
		return
	}
	refPath := s.refPath(namespace, name)
	switch r.Method {
	case http.MethodGet:
		data, err := os.ReadFile(refPath)
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"id": strings.TrimSpace(string(data))})
	case http.MethodPut:
		rd, err := body(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer rd.Close()
		var req struct {
			ID string `json:"id"`
			// Old is the expected current value: empty means "must not
			// exist". A mismatch fails with 409 so concurrent pushes cannot
			// silently overwrite each other.
			Old string `json:"old"`
		}
		if err := json.NewDecoder(io.LimitReader(rd, 1<<20)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !hexID.MatchString(req.ID) {
			http.Error(w, "invalid state id", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		cur := ""
		if data, err := os.ReadFile(refPath); err == nil {
			cur = strings.TrimSpace(string(data))
		} else if !errors.Is(err, os.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if cur != req.Old {
			writeJSONStatus(w, http.StatusConflict, map[string]string{"id": cur})
			return
		}
		if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := writeFileAtomic(refPath, []byte(req.ID+"\n")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		err := os.Remove(refPath)
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) listAccounts(w http.ResponseWriter, user User) {
	if !user.Admin {
		http.Error(w, "admin only", http.StatusForbidden)
		return
	}
	writeJSON(w, map[string][]User{"accounts": s.accounts.list()})
}

func (s *Server) addAccount(w http.ResponseWriter, r *http.Request, user User) {
	if !user.Admin {
		http.Error(w, "admin only", http.StatusForbidden)
		return
	}
	var u User
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.accounts.add(u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) removeAccount(w http.ResponseWriter, user User, name string) {
	if !user.Admin {
		http.Error(w, "admin only", http.StatusForbidden)
		return
	}
	if err := s.accounts.remove(name); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// gc removes blocks and states no ref reaches anymore. Only objects older
// than the grace window are swept, so a push in flight (blocks uploaded, ref
// not moved yet) is never collected. Admin only.
func (s *Server) gc(w http.ResponseWriter, r *http.Request, user User) {
	if !user.Admin {
		http.Error(w, "admin only", http.StatusForbidden)
		return
	}
	grace := time.Hour
	if v := r.URL.Query().Get("grace_seconds"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs < 0 {
			http.Error(w, "invalid grace_seconds", http.StatusBadRequest)
			return
		}
		grace = time.Duration(secs) * time.Second
	}
	cutoff := time.Now().Add(-grace)

	s.mu.Lock()
	defer s.mu.Unlock()

	liveStates := map[string]bool{}
	liveBlocks := map[core.BlockID]bool{}
	err := filepath.WalkDir(filepath.Join(s.root, "refs"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		id := strings.TrimSpace(string(data))
		if !hexID.MatchString(id) || liveStates[id] {
			return nil
		}
		liveStates[id] = true
		doc, err := os.ReadFile(s.statePath(id))
		if err != nil {
			return nil // dangling ref: nothing to mark
		}
		var commit struct {
			Files []struct {
				Blocks []core.BlockID `json:"blocks"`
			} `json:"files"`
		}
		if err := json.Unmarshal(doc, &commit); err != nil {
			return nil
		}
		for _, f := range commit.Files {
			for _, b := range f.Blocks {
				liveBlocks[b] = true
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	removedStates := 0
	states, _ := os.ReadDir(filepath.Join(s.root, "states"))
	for _, e := range states {
		id := strings.TrimSuffix(e.Name(), ".json")
		if e.IsDir() || liveStates[id] {
			continue
		}
		if info, err := e.Info(); err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if os.Remove(s.statePath(id)) == nil {
			removedStates++
		}
	}

	blocks, err := s.blocks.List()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	removedBlocks := 0
	var freed int64
	for _, b := range blocks {
		if liveBlocks[b.ID] || b.ModTime.After(cutoff) {
			continue
		}
		if s.blocks.Delete(b.ID) == nil {
			removedBlocks++
			freed += b.Size
		}
	}

	writeJSON(w, map[string]int64{
		"removed_blocks": int64(removedBlocks),
		"removed_states": int64(removedStates),
		"freed_bytes":    freed,
	})
}

func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > maxFrame {
		return nil, fmt.Errorf("frame of %d bytes exceeds the %d limit", size, maxFrame)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("truncated frame: %w", err)
	}
	return data, nil
}

func writeFrame(w io.Writer, data []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func writeJSON(w http.ResponseWriter, v any) {
	writeJSONStatus(w, http.StatusOK, v)
}

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func writeFileAtomic(path string, data []byte) error {
	return writeFileAtomicMode(path, data, 0o644)
}

func writeFileAtomicMode(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}

// Close releases server resources (the audit log).
func (s *Server) Close() error { return s.audit.Close() }

// ListenAndServe runs the server on addr over plain HTTP.
func (s *Server) ListenAndServe(addr string) error {
	srv := &http.Server{Addr: addr, Handler: s}
	fmt.Fprintf(os.Stderr, "remote: serving %s on %s\n", s.root, addr)
	return srv.ListenAndServe()
}

// ListenAndServeTLS runs the server on addr over HTTPS with the given
// certificate and key.
func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	srv := &http.Server{Addr: addr, Handler: s}
	fmt.Fprintf(os.Stderr, "remote: serving %s on %s (TLS)\n", s.root, addr)
	return srv.ListenAndServeTLS(certFile, keyFile)
}
