package repo

import (
	"fmt"
	"path"
	"strings"
	"syscall"

	"github.com/fvs-lab/fvs2/internal/meta"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

func checkNativeCollisions(files []meta.FileEntry, caseSensitive, normalizationSensitive bool) error {
	seen := map[string]string{}
	fold := cases.Fold()
	for _, file := range files {
		if err := meta.ValidateRelPath(file.Path); err != nil {
			return err
		}
		for name := file.Path; name != "."; name = path.Dir(name) {
			key := name
			if !normalizationSensitive {
				key = norm.NFD.String(key)
			}
			if !caseSensitive {
				key = fold.String(key)
			}
			if previous, ok := seen[key]; ok && previous != name {
				return fmt.Errorf("destination cannot distinguish %q and %q: %w", previous, name, syscall.EILSEQ)
			}
			seen[key] = name
		}
		if strings.ContainsRune(file.Link, 0) {
			return syscall.EINVAL
		}
	}
	return nil
}
