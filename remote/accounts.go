package remote

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
)

// User is one account on a remote. Blocks are shared across accounts (identical
// content is stored once, whoever uploads it first); refs live under a
// namespace the account owns or shares through a team; the quota counts the
// bytes of new blocks the account brought to the store.
type User struct {
	Name       string   `json:"name"`
	Token      string   `json:"token"`
	QuotaBytes int64    `json:"quota_bytes,omitempty"`
	Admin      bool     `json:"admin,omitempty"`
	Teams      []string `json:"teams,omitempty"`
}

// accounts is the runtime account registry. It authenticates tokens, decides
// which namespaces an account may use, and can be mutated (add/remove
// accounts) without a restart, persisting to a file when one is configured.
type accounts struct {
	mu      sync.RWMutex
	path    string // "" keeps accounts in memory only
	byToken map[string]User
	byName  map[string]User
	open    bool // no accounts configured: every caller is the anonymous admin
}

func newAccounts(path string, seed []User) (*accounts, error) {
	a := &accounts{
		path:    path,
		byToken: map[string]User{},
		byName:  map[string]User{},
	}
	for _, u := range seed {
		if err := a.insert(u); err != nil {
			return nil, err
		}
	}
	a.open = len(a.byName) == 0
	return a, nil
}

func validateUser(u User) error {
	if u.Name == "" || u.Token == "" {
		return errors.New("every account needs a name and a token")
	}
	if !refName.MatchString(u.Name) {
		return fmt.Errorf("invalid account name: %s", u.Name)
	}
	for _, t := range u.Teams {
		if !refName.MatchString(t) {
			return fmt.Errorf("invalid team name: %s", t)
		}
	}
	return nil
}

// insert adds an account without locking; callers hold the lock or are in the
// constructor.
func (a *accounts) insert(u User) error {
	if err := validateUser(u); err != nil {
		return err
	}
	if _, dup := a.byToken[u.Token]; dup {
		return fmt.Errorf("duplicate token for account %s", u.Name)
	}
	a.byToken[u.Token] = u
	a.byName[u.Name] = u
	return nil
}

func (a *accounts) authenticate(token string) (User, bool) {
	a.mu.RLock()
	if a.open {
		a.mu.RUnlock()
		return User{Name: "default", Admin: true}, true
	}
	u, ok := a.byToken[token]
	a.mu.RUnlock()
	if ok || a.path == "" {
		return u, ok
	}
	// Unknown token with a backing file: another server process may have
	// added the account. Reload once and retry.
	a.mu.Lock()
	_ = a.reloadLocked()
	u, ok = a.byToken[token]
	a.mu.Unlock()
	return u, ok
}

// reloadLocked replaces the in-memory account set with the backing file's
// content. Callers hold the write lock.
func (a *accounts) reloadLocked() error {
	users, err := LoadUsers(a.path)
	if err != nil {
		return err
	}
	byToken := map[string]User{}
	byName := map[string]User{}
	for _, u := range users {
		if err := validateUser(u); err != nil {
			return err
		}
		byToken[u.Token] = u
		byName[u.Name] = u
	}
	a.byToken = byToken
	a.byName = byName
	a.open = len(byName) == 0
	return nil
}

// allows reports whether the account may use the given ref namespace: its own
// name, or a team it belongs to. An empty namespace means the account's own.
func (a *accounts) allows(u User, namespace string) bool {
	if namespace == "" || namespace == u.Name {
		return true
	}
	for _, t := range u.Teams {
		if t == namespace {
			return true
		}
	}
	return false
}

// mutate runs fn on the freshest account set, under a file lock when a
// backing file exists so concurrent server processes serialize their changes.
func (a *accounts) mutate(fn func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.path == "" {
		return fn()
	}
	return withFileLock(a.path+".lock", func() error {
		if _, err := os.Stat(a.path); err == nil {
			if err := a.reloadLocked(); err != nil {
				return err
			}
		}
		return fn()
	})
}

func (a *accounts) add(u User) error {
	return a.mutate(func() error {
		if _, exists := a.byName[u.Name]; exists {
			return fmt.Errorf("account already exists: %s", u.Name)
		}
		if err := a.insert(u); err != nil {
			return err
		}
		a.open = false
		return a.save()
	})
}

func (a *accounts) remove(name string) error {
	return a.mutate(func() error {
		u, exists := a.byName[name]
		if !exists {
			return fmt.Errorf("account not found: %s", name)
		}
		delete(a.byToken, u.Token)
		delete(a.byName, name)
		return a.save()
	})
}

func (a *accounts) list() []User {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]User, 0, len(a.byName))
	for _, u := range a.byName {
		redacted := u
		redacted.Token = ""
		out = append(out, redacted)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// save persists the account set when a path is configured. Callers hold the
// write lock.
func (a *accounts) save() error {
	if a.path == "" {
		return nil
	}
	users := make([]User, 0, len(a.byName))
	for _, u := range a.byName {
		users = append(users, u)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	b, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomicMode(a.path, append(b, '\n'), 0o600)
}

// LoadUsers reads a JSON array of accounts, the format --users consumes.
func LoadUsers(path string) ([]User, error) {
	b, err := readFile(path)
	if err != nil {
		return nil, err
	}
	var users []User
	if err := json.Unmarshal(b, &users); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return users, nil
}
