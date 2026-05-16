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
				if err := validateRequiredS3CompatibilityRows(string(body)); err != nil {
					t.Fatal(err)
				}
				if err := validateProductionEssentialS3Evidence(string(body)); err != nil {
					t.Fatal(err)
				}
				if err := validateUnsupportedS3FailClosedEvidence(string(body)); err != nil {
					t.Fatal(err)
				}
				if err := validateObjectGovernanceBoundary(string(body)); err != nil {
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
		"single-node e2e",
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

func validateUnsupportedS3FailClosedEvidence(markdown string) error {
	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) < 4 {
			continue
		}

		operation := cells[1]
		status := strings.ToLower(cells[2])
		notes := strings.ToLower(cells[3])
		if status != "not supported" || !requiresFailClosedEvidence(operation) {
			continue
		}
		if strings.Contains(notes, "fail-closed") && strings.Contains(notes, "server tests") {
			continue
		}

		return fmt.Errorf("Not supported S3 row %q must include fail-closed server test evidence in Notes", operation)
	}

	return nil
}

func validateRequiredS3CompatibilityRows(markdown string) error {
	required := []string{
		"Lifecycle Expiration.Days",
		"Lifecycle transition effects",
		"SSE-S3 headers",
		"SSE-KMS headers",
		"SSE-C headers",
		"Object Lock / retention / legal hold",
	}
	rows := make(map[string]bool, len(required))
	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) < 4 {
			continue
		}
		rows[cells[1]] = true
	}

	for _, operation := range required {
		if !rows[operation] {
			return fmt.Errorf("S3 compatibility matrix must include split row %q", operation)
		}
	}
	return nil
}

func validateObjectGovernanceBoundary(markdown string) error {
	requiredNotes := []string{
		"separate governance design",
		"versioning",
		"deletes",
		"lifecycle",
		"permissions",
	}
	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) < 4 || cells[1] != "Object Lock / retention / legal hold" {
			continue
		}
		if strings.ToLower(cells[2]) != "not supported" {
			return fmt.Errorf("Object Lock / retention / legal hold must remain Not supported until governance design lands")
		}
		notes := strings.ToLower(cells[3])
		if !containsAll(notes, requiredNotes) {
			return fmt.Errorf("Object Lock / retention / legal hold row must name the separate governance design boundary")
		}
		return nil
	}
	return fmt.Errorf("Object Lock / retention / legal hold row is required")
}

func requiresFailClosedEvidence(operation string) bool {
	return operation == "SSE-KMS headers" || operation == "SSE-C headers"
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
		(operation == "sse-s3 headers") ||
		(strings.Contains(operation, "sse-s3/sse-kms") && strings.Contains(operation, "headers")) ||
		strings.Contains(operation, "lifecycle expiration")
}

func containsAll(s string, needles []string) bool {
	for _, needle := range needles {
		if !strings.Contains(s, needle) {
			return false
		}
	}
	return true
}

func containsAny(s string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
