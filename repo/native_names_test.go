package repo

import (
	"errors"
	"syscall"
	"testing"

	"github.com/fvs-lab/fvs2/internal/meta"
)

func TestNativeNamesRejectCaseAndUnicodeAliases(t *testing.T) {
	for _, paths := range [][]string{{"Foo/data", "foo/other"}, {"\u00e9", "e\u0301"}} {
		files := []meta.FileEntry{{Path: paths[0]}, {Path: paths[1]}}
		if err := checkNativeCollisions(files, false, false); !errors.Is(err, syscall.EILSEQ) {
			t.Fatalf("aliases %q: %v", paths, err)
		}
		if err := checkNativeCollisions(files, true, true); err != nil {
			t.Fatalf("byte-sensitive destination: %v", err)
		}
	}
	if err := checkNativeCollisions([]meta.FileEntry{{Path: "dir/a"}, {Path: "dir/b"}}, false, false); err != nil {
		t.Fatal(err)
	}
}
