package repo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIgnoreMatcherPatterns(t *testing.T) {
	cases := []struct {
		name  string
		rules string
		path  string
		isDir bool
		want  bool
	}{
		{"unanchored extension", "*.o\n", "obj/main.o", false, true},
		{"unanchored dir at any depth", "build/\n", "sub/build", true, true},
		{"unanchored dir does not match file", "build/\n", "build", false, false},
		{"anchored path only at root", "/dist\n", "sub/dist", false, false},
		{"anchored path matches root entry", "/dist\n", "dist", false, true},
		{"comment and blank lines skipped", "# comment\n\n*.log\n", "app.log", false, true},
		{"negation re-includes", "*.log\nkeep.log\n!keep.log\n", "keep.log", false, false},
		{"negation does not affect others", "*.log\n!keep.log\n", "other.log", false, true},
		{"escaped leading bang literal", `\!important` + "\n", "!important", false, true},
		{"no rules matches nothing", "", "anything", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			if c.rules != "" {
				if err := os.WriteFile(filepath.Join(dir, ".fvsignore"), []byte(c.rules), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			m, err := loadIgnore(dir)
			if err != nil {
				t.Fatal(err)
			}
			if got := m.Match(c.path, c.isDir); got != c.want {
				t.Fatalf("Match(%q, dir=%v) = %v, want %v", c.path, c.isDir, got, c.want)
			}
		})
	}
}

func TestLoadIgnoreMissingFile(t *testing.T) {
	m, err := loadIgnore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if m.Match("anything", false) {
		t.Fatal("expected no rules to match nothing")
	}
}
