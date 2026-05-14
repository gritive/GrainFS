//go:build compat

package compat

import (
	"bufio"
	"os"
	"regexp"
	"runtime"
	"strings"
)

var changelogVersionRe = regexp.MustCompile(`^## \[([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\]`)

// prevVersion parses CHANGELOG.md and returns the version entry
// just before the current (first) entry.
// Returns "" if the file cannot be parsed or there is no previous version.
func prevVersion() string {
	path := findRepoRoot() + "/CHANGELOG.md"
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	var versions []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if m := changelogVersionRe.FindStringSubmatch(line); m != nil {
			versions = append(versions, m[1])
			if len(versions) == 2 {
				break
			}
		}
	}
	if len(versions) < 2 {
		return ""
	}
	return versions[1]
}

// findRepoRoot walks up from this file's directory until it finds CHANGELOG.md.
func findRepoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	dir := thisFile
	for {
		parent := dir[:strings.LastIndex(dir, "/")]
		if parent == dir {
			break
		}
		dir = parent
		if _, err := os.Stat(dir + "/CHANGELOG.md"); err == nil {
			return dir
		}
	}
	return "."
}
