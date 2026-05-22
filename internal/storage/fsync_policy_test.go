package storage_test

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFsyncCallsStayInExplicitLogOwners is a repository audit that prevents new
// direct on-disk fsync calls from leaking into data paths. The data WAL is the
// only durability owner for object/shard/pack/segment bytes; anything else that
// needs to fsync a file must be either (a) a log/store package, or (b) an
// explicitly approved narrow file (e.g. write-once cluster PSK rotation).
//
// The audit walks every non-test .go file under internal/ and flags lines that
// contain a no-arg `.Sync()` call. Interface methods like `Storage.Sync(bucket,
// key)` and `Router.Sync(assignments)` are excluded by the no-arg shape; the
// remaining hits are real file/db fsyncs.
//
// To add an allowlisted owner: add the file or directory prefix to allowed
// below, and document why crash safety is preserved there. Adding a whole
// package is fine for true log owners; data paths should justify per-file.
func TestFsyncCallsStayInExplicitLogOwners(t *testing.T) {
	repoRoot := repoRootForTest(t)
	root := filepath.Join(repoRoot, "internal")

	// Allowed prefixes are evaluated against the repo-relative file path
	// (forward slashes). Trailing slash means "directory", no trailing slash
	// means "exact file".
	allowed := []string{
		// Data WAL itself — the durability owner.
		"internal/storage/datawal/",
		// PITR logical WAL.
		"internal/storage/wal/",
		// Raft log/snapshot/stable stores (Badger db.Sync()).
		"internal/raft/",
		// Role ledger journal + quarantine manifest — append-only log files.
		"internal/badgerrole/",
		// Write-once cluster PSK rotation. Pre-dates the data WAL and is not a
		// data path.
		"internal/transport/keystore.go",
		// Repair replica is not WAL-covered: it materializes bytes pulled from
		// a healthy peer. The explicit fsync + dir sync is the durability
		// boundary here. See repair_replica.go writeRepairedReplica comment.
		"internal/cluster/repair_replica.go",
		// Shard pack uses the data WAL for per-record durability; the dir
		// sync after rotate is once-per-blob metadata durability that WAL
		// replay cannot recover. Allowed as a narrow file exception.
		"internal/cluster/shard_pack.go",
		// LocalBackend.Sync degrades to file fsync when no data WAL is wired
		// (test/embedded callers). Production wires WAL via WithDataWAL and
		// this fallback never runs. TODO: remove once all callers enforce WAL.
		"internal/storage/local.go",
	}

	noArgSyncRe := regexp.MustCompile(`\b\w+\.Sync\(\s*\)`)

	type violation struct {
		path string
		line int
		text string
	}
	var violations []violation

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		// Quick allowlist check: skip files entirely if they're allowlisted.
		if isAllowed(rel, allowed) {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		lineNo := 0
		inBlockComment := false
		for scanner.Scan() {
			lineNo++
			raw := scanner.Text()
			trimmed := strings.TrimSpace(raw)
			// Track /* ... */ block comments.
			if inBlockComment {
				if idx := strings.Index(trimmed, "*/"); idx >= 0 {
					trimmed = trimmed[idx+2:]
					inBlockComment = false
				} else {
					continue
				}
			}
			if i := strings.Index(trimmed, "/*"); i >= 0 {
				// Strip inline block comment open; if it closes on same line,
				// keep both segments. Cheap approximation is enough for an
				// audit grep.
				if j := strings.Index(trimmed[i:], "*/"); j >= 0 {
					trimmed = trimmed[:i] + trimmed[i+j+2:]
				} else {
					trimmed = trimmed[:i]
					inBlockComment = true
				}
			}
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			// Strip trailing // comment.
			if i := strings.Index(trimmed, "//"); i >= 0 {
				trimmed = trimmed[:i]
			}
			if !noArgSyncRe.MatchString(trimmed) {
				continue
			}
			violations = append(violations, violation{path: rel, line: lineNo, text: strings.TrimSpace(raw)})
		}
		return scanner.Err()
	})
	require.NoError(t, err)

	if len(violations) == 0 {
		return
	}
	var msg strings.Builder
	msg.WriteString("direct fsync outside explicit WAL/log owner; data durability must go through internal/storage/datawal\n")
	for _, v := range violations {
		msg.WriteString("  ")
		msg.WriteString(v.path)
		msg.WriteString(":")
		msg.WriteString(itoa(v.line))
		msg.WriteString(": ")
		msg.WriteString(v.text)
		msg.WriteString("\n")
	}
	t.Fatal(msg.String())
}

func isAllowed(rel string, allowed []string) bool {
	for _, prefix := range allowed {
		if strings.HasSuffix(prefix, "/") {
			if strings.HasPrefix(rel, prefix) {
				return true
			}
			continue
		}
		if rel == prefix {
			return true
		}
	}
	return false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// repoRootForTest walks upward from the test source file until it finds the
// repository root (identified by go.mod). Test runs cd into the package
// directory, so we anchor by the source file location instead of os.Getwd.
func repoRootForTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	dir := filepath.Dir(file)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("repo root (go.mod) not found from %s", file)
	return ""
}
