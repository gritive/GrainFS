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
	// Recursive walk so every current and future subpackage (putpipeline,
	// clusterpb, ecspike, ...) is covered — an explicit dir list would
	// silently exempt new subdirectories.
	fset := token.NewFileSet()
	var violations []string
	walkErr := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			for _, bad := range forbidden {
				if strings.Contains(p, bad) {
					violations = append(violations, path+" imports "+p)
				}
			}
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk: %v", walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("Phase 6.5 guard: production files in internal/cluster must not "+
			"import badger/badgermeta/badgerutil (inject a MetadataStore from the "+
			"composition root instead):\n  %s", strings.Join(violations, "\n  "))
	}
}
