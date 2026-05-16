package reference_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompatibilityMatricesDoNotUseNotTestedStatus(t *testing.T) {
	files, err := filepath.Glob("*-compatibility.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no compatibility docs found")
	}

	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			body, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(strings.ToLower(string(body)), "not tested") {
				t.Fatalf("%s uses Not tested; compatibility matrices must choose Supported, Partial, Not supported, or Not planned", file)
			}
		})
	}
}
