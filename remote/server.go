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

// User is one account on a remote. Blocks are shared across users (identical
// content is stored once, whoever uploads it); refs are namespaced per user;
// the quota counts the bytes of new blocks an account brought to the store.
type User struct {
	Name       string `json:"name"`
	Token      string `json:"token"`
	QuotaBytes int64  `json:"quota_bytes,omitempty"`
	Admin      bool   `json:"admin,omitempty"`
}

// Server is the reference implementation of the FVS remote protocol over a
// directory. See docs/REMOTE.md for the endpoints.
type Server struct {
	root    string
	byToken map[string]User
	open    bool // no users configured: every caller is the anonymous admin

	mu    sync.Mutex // guards usage and gc
	usage map[string]int64
	store *core.DiskBlockStore
}

// maxFrame bounds a single block frame in batch transfers.
const maxFrame = 16 << 20

// NewServer serves a remote from root. token == "" leaves the server open;
// otherwise it behaves as a single admin account without quota.
func NewServer(root, token string) (*Server, error) {
	if token == "" {
		return NewServerWithUsers(root, nil)
	}
	return NewServerWithUsers(root, []User{{Name: "default", Token: token, Admin: true}})
}

// NewServerWithUsers serves a remote with per-user accounts.
func NewServerWithUsers(root string, users []User) (*Server, error) {
	for _, dir := range []string{"blocks", "states", "refs"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return nil, err
		}
	}
	store, err := core.NewDiskBlockStore(filepath.Join(root, "blocks"))
	if err != nil {
		return nil, err
	}
	byToken := map[string]User{}
	for _, u := range users {
		if u.Name == "" || u.Token == "" {
			return nil, errors.New("every user needs a name and a token")
		}
		if !refName.MatchString(u.Name) {
			return nil, fmt.Errorf("invalid user name: %s", u.Name)
		}
		if _, dup := byToken[u.Token]; dup {
			return nil, fmt.Errorf("duplicate token for user %s", u.Name)
		}
		byToken[u.Token] = u
	}
	s := &Server{
		root:    root,
		byToken: byToken,
		open:    len(byToken) == 0,
		usage:   map[string]int64{},
		store:   store,
	}
	if err := s.loadUsage(); err != nil {
		return nil, err
	}
	return s, nil
}

// LoadUsers reads a JSON array of accounts, the format --users consumes.
func LoadUsers(path string) ([]User, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var users []User
	if err := json.Unmarshal(b, &users); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return users, nil
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

// chargeNewBytes reserves n bytes of quota for user; it fails without
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

func (s *Server) auth(r *http.Request) (User, bool) {
	if s.open {
		return User{Name: "default", Admin: true}, true
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return User{}, false
	}
	u, ok := s.byToken[token]
	return u, ok
}

// body returns the request body, transparently decompressing gzip uploads.
func body(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		return gzip.NewReader(r.Body)
	}
	return r.Body, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, ok := s.auth(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

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
		ok, err := s.store.Has(core.BlockID(id))
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
		ok, err := s.store.Has(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if ok {
			continue
		}
		if err := s.chargeNewBytes(user, int64(len(data))); err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if _, err := s.store.Put(data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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
		if ok, err := s.store.Has(core.BlockID(id)); err != nil || !ok {
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
		data, err := s.store.Get(core.BlockID(id))
		if err != nil {
			return // headers already sent; the client's hash check catches truncation
		}
		if err := writeFrame(out, data); err != nil {
			return
		}
	}
}

func (s *Server) block(w http.ResponseWriter, r *http.Request, user User, id string) {
	if !hexID.MatchString(id) {
		http.Error(w, "invalid block id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, err := s.store.Get(core.BlockID(id))
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
		ok, err := s.store.Has(core.BlockID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			if err := s.chargeNewBytes(user, int64(len(data))); err != nil {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			if _, err := s.store.Put(data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
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

// refPath places refs under a per-user namespace, so accounts cannot read or
// move each other's branches.
func (s *Server) refPath(user User, name string) string {
	return filepath.Join(s.root, "refs", user.Name, name)
}

func (s *Server) ref(w http.ResponseWriter, r *http.Request, user User, name string) {
	if !refName.MatchString(name) {
		http.Error(w, "invalid ref name", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, err := os.ReadFile(s.refPath(user, name))
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
		cur := ""
		if data, err := os.ReadFile(s.refPath(user, name)); err == nil {
			cur = strings.TrimSpace(string(data))
		} else if !errors.Is(err, os.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if cur != req.Old {
			writeJSONStatus(w, http.StatusConflict, map[string]string{"id": cur})
			return
		}
		if err := os.MkdirAll(filepath.Dir(s.refPath(user, name)), 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := writeFileAtomic(s.refPath(user, name), []byte(req.ID+"\n")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		err := os.Remove(s.refPath(user, name))
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

	removedBlocks := 0
	var freed int64
	blocks, _ := os.ReadDir(filepath.Join(s.root, "blocks"))
	for _, e := range blocks {
		if e.IsDir() || liveBlocks[core.BlockID(e.Name())] {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if s.store.Delete(core.BlockID(e.Name())) == nil {
			removedBlocks++
			freed += info.Size()
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

func writeFileAtomic(path string, data []byte) error {
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

// ListenAndServe runs the server on addr.
func (s *Server) ListenAndServe(addr string) error {
	srv := &http.Server{Addr: addr, Handler: s}
	fmt.Fprintf(os.Stderr, "remote: serving %s on %s\n", s.root, addr)
	return srv.ListenAndServe()
}
