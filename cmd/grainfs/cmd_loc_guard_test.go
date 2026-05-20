package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	cmdLOCContractMaxFile = 250
	cmdLOCContractMaxFunc = 90
)

// TestCmdLOCContract enforces the cmd thin-runner contract:
//
//	each non-test .go file under cmd/grainfs/ must satisfy
//	  (a) file ≤ 250 LOC, OR
//	  (b) every top-level function ≤ 90 LOC.
//
// Files like volume.go (411 LOC) exceed (a) but every RunE/factory function
// is small enough to satisfy (b). The contract exists to prevent regression
// of the cmd thin-runner refactor — business logic must move to
// internal/<feature>admin/ rather than fattening cmd files.
//
// See docs/superpowers/specs/2026-05-20-cmd-thin-runner-design.md and
// docs/superpowers/specs/2026-05-21-cmd-thin-runner-step6-7-nfs-lint-design.md.
func TestCmdLOCContract(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read cmd/grainfs dir: %v", err)
	}
	fset := token.NewFileSet()
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(".", name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		fileLOC := strings.Count(string(data), "\n")
		if len(data) > 0 && data[len(data)-1] != '\n' {
			fileLOC++
		}
		if fileLOC <= cmdLOCContractMaxFile {
			continue
		}
		f, err := parser.ParseFile(fset, path, data, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			start := fset.Position(fn.Pos()).Line
			end := fset.Position(fn.End()).Line
			funcLOC := end - start + 1
			if funcLOC > cmdLOCContractMaxFunc {
				t.Errorf("%s: file %d LOC > %d AND func %s %d LOC > %d. "+
					"Shrink the file under %d LOC, split the function, or move "+
					"logic into internal/<feature>admin/ per the cmd thin-runner contract.",
					path, fileLOC, cmdLOCContractMaxFile,
					fn.Name.Name, funcLOC, cmdLOCContractMaxFunc,
					cmdLOCContractMaxFile)
			}
		}
	}
}
