package repo

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// ignoreRule is one compiled line from .fvsignore. A trailing "/" restricts
// the rule to directories, a leading "!" re-includes a path an earlier rule
// excluded, and a pattern containing an interior "/" is anchored to the
// repo root instead of matching at any depth. Glob syntax follows
// path/filepath.Match: "*" and "?" do not cross a "/", "**" is not
// special.
type ignoreRule struct {
	pattern  string
	negate   bool
	dirOnly  bool
	anchored bool
}

// ignoreMatcher holds the compiled rules for one commit walk. Rules are
// evaluated in file order and the last matching rule wins, mirroring
// .gitignore semantics.
type ignoreMatcher struct {
	rules []ignoreRule
}

// loadIgnore reads .fvsignore from the repository root. A missing file is
// not an error: nothing is excluded.
func loadIgnore(root string) (*ignoreMatcher, error) {
	f, err := os.Open(filepath.Join(root, ".fvsignore"))
	if os.IsNotExist(err) {
		return &ignoreMatcher{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rules []ignoreRule
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), " \t\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rule := ignoreRule{}
		if strings.HasPrefix(line, "!") {
			rule.negate = true
			line = line[1:]
		}
		if strings.HasPrefix(line, `\!`) || strings.HasPrefix(line, `\#`) {
			line = line[1:]
		}
		if strings.HasSuffix(line, "/") {
			rule.dirOnly = true
			line = strings.TrimSuffix(line, "/")
		}
		// A leading "/" anchors explicitly; an interior "/" (once any
		// leading one is gone) anchors implicitly, same as .gitignore.
		rule.anchored = strings.HasPrefix(line, "/") || strings.Contains(strings.TrimPrefix(line, "/"), "/")
		line = strings.TrimPrefix(line, "/")
		if line == "" {
			continue
		}
		rule.pattern = line
		rules = append(rules, rule)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return &ignoreMatcher{rules: rules}, nil
}

// Match reports whether the repo-relative, forward-slash path rel (a file or
// a directory, isDir tells which) is excluded by the loaded rules.
func (m *ignoreMatcher) Match(rel string, isDir bool) bool {
	if m == nil {
		return false
	}
	ignored := false
	for _, r := range m.rules {
		if r.dirOnly && !isDir {
			continue
		}
		if r.matches(rel) {
			ignored = !r.negate
		}
	}
	return ignored
}

func (r ignoreRule) matches(rel string) bool {
	if r.anchored {
		ok, _ := filepath.Match(r.pattern, rel)
		return ok
	}
	// Unanchored: the pattern matches the whole path or any single segment,
	// so "*.o" or "build" exclude at any depth.
	if ok, _ := filepath.Match(r.pattern, rel); ok {
		return true
	}
	for _, seg := range strings.Split(rel, "/") {
		if ok, _ := filepath.Match(r.pattern, seg); ok {
			return true
		}
	}
	return false
}
