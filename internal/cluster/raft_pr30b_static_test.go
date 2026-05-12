package cluster

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPR30BNoV1RaftProductionReferences(t *testing.T) {
	root := repoRootForPR30B(t)
	forbidden := []string{
		"github.com/gritive/GrainFS/internal/raft/v2",
		"raft.NewNode(",
		"raft.NewBadgerLogStore(",
		"raft.OpenSharedLogStore(",
		"raft.NewSnapshotManager(",
		"RaftNode() *raft.Node",
	}
	allowedPathParts := []string{
		filepath.Join("docs", "superpowers", "plans"),
		filepath.Join("internal", "cluster", "raft_pr30b_static_test.go"),
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".worktrees", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") && rel != "TODOS.md" {
			return nil
		}
		for _, allowed := range allowedPathParts {
			if strings.Contains(rel, allowed) {
				return nil
			}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		for _, needle := range forbidden {
			if strings.Contains(text, needle) {
				t.Errorf("%s still contains %q", rel, needle)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func repoRootForPR30B(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
