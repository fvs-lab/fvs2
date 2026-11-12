//go:build darwin

package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/fvs-lab/fvs2/internal/meta"
	"golang.org/x/text/unicode/norm"
)

func preflightNativeNames(destination string, files []meta.FileEntry) error {
	for _, file := range files {
		if !utf8.ValidString(file.Path) || !utf8.ValidString(file.Link) {
			return fmt.Errorf("macOS cannot represent a non-UTF-8 snapshot name or symlink: %w", syscall.EILSEQ)
		}
	}
	// Query the destination volume before creating any restored paths.
	probeRoot := destination
	for {
		_, err := os.Stat(probeRoot)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(probeRoot)
		if parent == probeRoot {
			return err
		}
		probeRoot = parent
	}
	probe, err := os.MkdirTemp(probeRoot, ".fvs-name-check-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(probe)
	existsAs := func(original, alias string) (bool, error) {
		f, err := os.OpenFile(filepath.Join(probe, original), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return false, err
		}
		if err := f.Close(); err != nil {
			return false, err
		}
		_, err = os.Lstat(filepath.Join(probe, alias))
		if os.IsNotExist(err) {
			return false, nil
		}
		return err == nil, err
	}
	caseAlias, err := existsAs("case", "CASE")
	if err != nil {
		return err
	}
	unicodeAlias, err := existsAs("\u00e9", "e\u0301")
	if err != nil {
		return err
	}
	probeEntries, err := os.ReadDir(probe)
	if err != nil {
		return err
	}
	unicodePreserved := false
	for _, entry := range probeEntries {
		if entry.Name() == "\u00e9" {
			unicodePreserved = true
		}
	}
	if !unicodePreserved {
		for _, entry := range files {
			if norm.NFD.String(entry.Path) != entry.Path {
				return fmt.Errorf("destination does not preserve the Unicode spelling of %q: %w", entry.Path, syscall.EILSEQ)
			}
		}
	}
	if err := checkNativeCollisions(files, !caseAlias, !unicodeAlias); err != nil {
		return err
	}
	// Existing entries must match byte for byte before a replace is allowed.
	for _, file := range files {
		dir := destination
		for _, component := range strings.Split(file.Path, "/") {
			info, err := os.Lstat(filepath.Join(dir, component))
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				return err
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			exact := false
			for _, entry := range entries {
				if entry.Name() == component {
					exact = true
					break
				}
			}
			if !exact {
				return fmt.Errorf("destination aliases the case or Unicode spelling of %q: %w", file.Path, syscall.EILSEQ)
			}
			if !info.IsDir() {
				break
			}
			dir = filepath.Join(dir, component)
		}
	}
	return nil
}
