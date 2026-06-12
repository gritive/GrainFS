package cluster

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoBadgerImportsInProductionFiles enforces the Phase 6.5 completion
// criterion: internal/cluster (and its putpipeline subpackage) must not
// import BadgerDB or its adapters in non-test files. The Domain layer talks
// to metadata storage only through the metastore contract; the composition
// root (serveruntime) opens the concrete DB and injects it.
//
// Test files are deliberately exempt: they verify badger-backed deployments
// (on-disk encryption frames, corruption injection) and construct the
// badgermeta adapter, which is legitimate test-fixture work.
func TestNoBadgerImportsInProductionFiles(t *testing.T) {
	forbidden := []string{
		"dgraph-io/badger",
		"GrainFS/internal/badgermeta",
		"GrainFS/internal/badgerutil",
	}
	// Both directories listed explicitly — filepath.WalkDir from "." would
	// also work today, but an explicit list keeps the guard honest if the
	// package ever gains nested dirs that should be scoped differently.
	dirs := []string{".", "putpipeline"}

	fset := token.NewFileSet()
	var violations []string
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, `"`)
				for _, bad := range forbidden {
					if strings.Contains(p, bad) {
						violations = append(violations, path+" imports "+p)
					}
				}
			}
		}
	}
	if len(violations) > 0 {
		t.Fatalf("Phase 6.5 guard: production files in internal/cluster must not "+
			"import badger/badgermeta/badgerutil (inject a MetadataStore from the "+
			"composition root instead):\n  %s", strings.Join(violations, "\n  "))
	}
}
