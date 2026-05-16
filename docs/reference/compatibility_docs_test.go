package reference_test

import (
	"fmt"
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
			if file == "s3-compatibility.md" {
				if err := validateProductionEssentialS3Evidence(string(body)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestProductionEssentialS3SupportedRowsRequireEvidence(t *testing.T) {
	for _, operation := range []string{
		"Multipart listing APIs",
		"SSE-S3/SSE-KMS headers",
		"Lifecycle expiration",
	} {
		t.Run(operation, func(t *testing.T) {
			table := strings.Join([]string{
				"| Area | Operation or surface | Status | Notes |",
				"| --- | --- | --- | --- |",
				"| S3 | " + operation + " | Supported | |",
			}, "\n")

			if err := validateProductionEssentialS3Evidence(table); err == nil {
				t.Fatal("expected Supported production-essential S3 row without evidence to fail")
			}
		})
	}
}

func validateProductionEssentialS3Evidence(markdown string) error {
	evidenceMarkers := []string{
		"cluster e2e",
		"server tests",
		"lifecycle worker tests",
		"real client",
		"conformance",
	}

	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) < 4 {
			continue
		}

		operation := strings.ToLower(cells[1])
		status := strings.ToLower(cells[2])
		notes := strings.ToLower(cells[3])
		if status != "supported" || !isProductionEssentialS3Operation(operation) {
			continue
		}
		if containsAny(notes, evidenceMarkers) {
			continue
		}

		return fmt.Errorf("Supported production-essential S3 row %q must include an evidence marker in Notes", cells[1])
	}

	return nil
}

func markdownTableCells(line string) []string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
		return nil
	}

	rawCells := strings.Split(strings.Trim(line, "|"), "|")
	cells := make([]string, 0, len(rawCells))
	for _, cell := range rawCells {
		cells = append(cells, strings.TrimSpace(cell))
	}
	return cells
}

func isProductionEssentialS3Operation(operation string) bool {
	return operation == "multipart listing apis" ||
		(strings.Contains(operation, "sse-s3/sse-kms") && strings.Contains(operation, "headers")) ||
		strings.Contains(operation, "lifecycle expiration")
}

func containsAny(s string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
