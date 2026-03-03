package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Head struct {
	Type string `json:"type"` // "branch" | "commit"
	Name string `json:"name,omitempty"`
	ID   string `json:"id,omitempty"`
}

func refsHeadsDir(root string) string { return filepath.Join(metaDir(root), "refs", "heads") }
func headPath(root string) string     { return filepath.Join(metaDir(root), "HEAD.json") }

func validateRefName(name string) error {
	if name == "" {
		return errors.New("empty name")
	}
	if strings.Contains(name, "..") {
		return errors.New("invalid name")
	}
	if strings.ContainsRune(name, os.PathSeparator) || strings.Contains(name, "/") {
		return errors.New("invalid name")
	}
	return nil
}

func branchRefPath(root, name string) string { return filepath.Join(refsHeadsDir(root), name) }

func ensureRefs(root string) error {
	return ensureDir(refsHeadsDir(root))
}

func ListBranches(root string) ([]string, error) {
	if _, err := LoadConfig(root); err != nil {
		return nil, err
	}
	if err := ensureRefs(root); err != nil {
		return nil, err
	}
	ents, err := os.ReadDir(refsHeadsDir(root))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "" || strings.HasPrefix(name, ".") {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func BranchExists(root, name string) (bool, error) {
	if err := validateRefName(name); err != nil {
		return false, err
	}
	if _, err := LoadConfig(root); err != nil {
		return false, err
	}
	_, err := os.Stat(branchRefPath(root, name))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func ReadBranchHead(root, name string) (string, error) {
	if err := validateRefName(name); err != nil {
		return "", err
	}
	b, err := os.ReadFile(branchRefPath(root, name))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func WriteBranchHead(root, name, id string) error {
	if err := validateRefName(name); err != nil {
		return err
	}
	if _, err := LoadConfig(root); err != nil {
		return err
	}
	if err := ensureRefs(root); err != nil {
		return err
	}
	p := branchRefPath(root, name)
	return writeFileAtomic(p, []byte(strings.TrimSpace(id)+"\n"), 0o644)
}

func CreateBranch(root, name string) error {
	if err := validateRefName(name); err != nil {
		return err
	}
	if _, err := LoadConfig(root); err != nil {
		return err
	}
	if err := ensureRefs(root); err != nil {
		return err
	}
	exists, err := BranchExists(root, name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("branch exists: %s", name)
	}
	id, _ := ResolveHeadCommit(root)
	return WriteBranchHead(root, name, id)
}

func DeleteBranch(root, name string) error {
	if err := validateRefName(name); err != nil {
		return err
	}
	h, err := GetHead(root)
	if err != nil {
		return err
	}
	if h.Type == "branch" && h.Name == name {
		return fmt.Errorf("cannot delete current branch: %s", name)
	}
	if err := os.Remove(branchRefPath(root, name)); err != nil {
		return err
	}
	return nil
}

func GetHead(root string) (Head, error) {
	if _, err := LoadConfig(root); err != nil {
		return Head{}, err
	}
	b, err := os.ReadFile(headPath(root))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// default
			return Head{Type: "branch", Name: "main"}, nil
		}
		return Head{}, err
	}
	var h Head
	if err := json.Unmarshal(b, &h); err != nil {
		return Head{}, err
	}
	if h.Type == "" {
		h.Type = "branch"
		h.Name = "main"
	}
	return h, nil
}

func SetHeadBranch(root, name string) error {
	if err := validateRefName(name); err != nil {
		return err
	}
	exists, err := BranchExists(root, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("branch not found: %s", name)
	}
	return writeJSONAtomic(headPath(root), Head{Type: "branch", Name: name})
}

func SetHeadCommit(root, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("empty commit id")
	}
	return writeJSONAtomic(headPath(root), Head{Type: "commit", ID: id})
}

func ResolveHeadCommit(root string) (string, error) {
	h, err := GetHead(root)
	if err != nil {
		return "", err
	}
	if h.Type == "commit" {
		return strings.TrimSpace(h.ID), nil
	}
	if h.Name == "" {
		return "", nil
	}
	id, err := ReadBranchHead(root, h.Name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(id), nil
}

func AdvanceHeadAfterCommit(root, newID string) error {
	h, err := GetHead(root)
	if err != nil {
		return err
	}
	if h.Type == "branch" {
		return WriteBranchHead(root, h.Name, newID)
	}
	return SetHeadCommit(root, newID)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
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
	if err := tmp.Chmod(mode); err != nil {
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
