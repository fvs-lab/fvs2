package meta

import (
	"fmt"
	"runtime"
	"strings"
)

// State and tree metadata are untrusted input: a pulled state, a corrupted
// document or a malicious remote must never be able to steer a restore
// outside its destination. Every consumer validates entry names and paths
// before turning them into filesystem operations.

// ValidateEntryName rejects tree entry names that could redirect a restore:
// empty names, ".", "..", and names containing a separator or NUL.
func ValidateEntryName(name string) error {
	switch name {
	case "":
		return fmt.Errorf("empty entry name")
	case ".", "..":
		return fmt.Errorf("invalid entry name %q", name)
	}
	if strings.ContainsAny(name, "/\x00") || runtime.GOOS == "windows" && strings.ContainsRune(name, '\\') {
		return fmt.Errorf("invalid entry name %q", name)
	}
	return nil
}

// ValidateRelPath rejects state file paths that could escape a restore
// destination: absolute paths, empty paths, and any empty, "." or ".."
// component. Paths are slash-separated, as stored in state metadata.
func ValidateRelPath(p string) error {
	if p == "" {
		return fmt.Errorf("empty path")
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("absolute path %q", p)
	}
	for _, part := range strings.Split(p, "/") {
		if err := ValidateEntryName(part); err != nil {
			return fmt.Errorf("path %q: %w", p, err)
		}
	}
	return nil
}
