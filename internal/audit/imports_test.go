package audit

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestImports_NoServerPackage statically asserts that no production file
// in internal/audit imports internal/server. This is the F#25 recursion
// guard: if a future refactor adds such an import, the
// request → audit emit → audit writer → request handler → audit emit → …
// loop would be reintroduced silently. Catching it at unit-test time keeps
// the bypass property of internal/audit.Committer enforceable.
//
// Note: this test relies on Go's convention that `go test` runs with the
// working directory set to the package directory, so the `*.go` glob is
// resolved against internal/audit/.
func TestImports_NoServerPackage(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob *.go: %v", err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		a, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, imp := range a.Imports {
			if strings.Contains(imp.Path.Value, "internal/server") {
				t.Errorf("internal/audit/%s imports %s — recursion risk (F#25)", f, imp.Path.Value)
			}
		}
	}
}
