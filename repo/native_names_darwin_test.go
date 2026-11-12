//go:build darwin

package repo

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/fvs-lab/fvs2/internal/meta"
)

func TestNativePreflightPreservesExistingSpelling(t *testing.T) {
	for _, names := range [][2]string{{"case", "CASE"}, {"\u00e9", "e\u0301"}} {
		destination := t.TempDir()
		original := filepath.Join(destination, names[0])
		if err := os.WriteFile(original, []byte("original"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, aliasErr := os.Lstat(filepath.Join(destination, names[1]))
		err := preflightNativeNames(destination, []meta.FileEntry{{Path: names[1]}})
		if aliasErr == nil {
			if !errors.Is(err, syscall.EILSEQ) {
				t.Fatalf("accepted an alias of %q: %v", names[0], err)
			}
		} else if !os.IsNotExist(aliasErr) {
			t.Fatal(aliasErr)
		} else if err != nil {
			t.Fatalf("distinct spelling %q: %v", names[1], err)
		}
		data, err := os.ReadFile(original)
		if err != nil || string(data) != "original" {
			t.Fatalf("preflight changed the destination: %q %v", data, err)
		}
	}
}
