package reference_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestLatestS3BenchmarkTableSatisfiesDocumentedGates(t *testing.T) {
	body, err := os.ReadFile("benchmarks.md")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLatestS3BenchmarkGates(string(body)); err != nil {
		t.Fatal(err)
	}
}

func TestReadmePerformancePublishesOnlyPutGet(t *testing.T) {
	body, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReadmePerformanceScope(string(body)); err != nil {
		t.Fatal(err)
	}
}

func TestReadmePerformanceMatchesBenchmarkReference(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	benchmarks, err := os.ReadFile("benchmarks.md")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateReadmePerformanceMatchesBenchmarks(string(readme), string(benchmarks)); err != nil {
		t.Fatal(err)
	}
}

func TestLatestIcebergBenchmarkTableSatisfiesDocumentedGates(t *testing.T) {
	body, err := os.ReadFile("benchmarks.md")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLatestIcebergBenchmarkGates(string(body)); err != nil {
		t.Fatal(err)
	}
}

func validateLatestS3BenchmarkGates(markdown string) error {
	required := map[string]bool{
		"put":           false,
		"get":           false,
		"delete":        false,
		"mixed":         false,
		"list":          false,
		"stat":          false,
		"versioned":     false,
		"retention":     false,
		"multipart":     false,
		"multipart-put": false,
		"append":        false,
	}

	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) != 14 {
			continue
		}
		op := cells[0]
		if _, ok := required[op]; !ok {
			continue
		}
		required[op] = true

		grainErrors, err := strconv.Atoi(cells[11])
		if err != nil {
			return fmt.Errorf("%s GrainFS errors must be numeric: %w", op, err)
		}
		if grainErrors != 0 {
			return fmt.Errorf("%s GrainFS errors=%d, want 0", op, grainErrors)
		}

		if op == "append" {
			if cells[1] != "n/a" || cells[2] != "n/a" || cells[5] != "n/a" || cells[6] != "n/a" {
				return fmt.Errorf("append invalid baselines must publish throughput as n/a")
			}
			continue
		}

		minioThroughput, err := benchmarkThroughput(cells[1], cells[2])
		if err != nil {
			return fmt.Errorf("%s MinIO throughput: %w", op, err)
		}
		rustfsThroughput, err := benchmarkThroughput(cells[5], cells[6])
		if err != nil {
			return fmt.Errorf("%s RustFS throughput: %w", op, err)
		}
		grainThroughput, err := benchmarkThroughput(cells[9], cells[10])
		if err != nil {
			return fmt.Errorf("%s GrainFS throughput: %w", op, err)
		}
		if grainThroughput <= minioThroughput {
			return fmt.Errorf("%s GrainFS throughput %.2f must exceed MinIO %.2f", op, grainThroughput, minioThroughput)
		}
		if grainThroughput <= rustfsThroughput {
			return fmt.Errorf("%s GrainFS throughput %.2f must exceed RustFS %.2f", op, grainThroughput, rustfsThroughput)
		}

		minioRSS, err := strconv.ParseFloat(cells[4], 64)
		if err != nil {
			return fmt.Errorf("%s MinIO RSS must be numeric: %w", op, err)
		}
		grainRSS, err := strconv.ParseFloat(cells[12], 64)
		if err != nil {
			return fmt.Errorf("%s GrainFS RSS must be numeric: %w", op, err)
		}
		if grainRSS >= minioRSS {
			return fmt.Errorf("%s GrainFS RSS %.2f must be below MinIO %.2f", op, grainRSS, minioRSS)
		}
	}

	for op, seen := range required {
		if !seen {
			return fmt.Errorf("latest S3 benchmark table missing %s", op)
		}
	}
	for _, required := range []string{"S3 Express", "best-effort"} {
		if !strings.Contains(markdown, required) {
			return fmt.Errorf("append caveat must mention %q", required)
		}
	}
	return nil
}

func validateLatestIcebergBenchmarkGates(markdown string) error {
	required := map[string]bool{
		"catalog-read":    false,
		"catalog-commits": false,
		"catalog-mixed":   false,
		"sustained":       false,
	}
	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) != 6 {
			continue
		}
		op := cells[0]
		if _, ok := required[op]; !ok {
			continue
		}
		required[op] = true
		if cells[1] == "" {
			return fmt.Errorf("%s Iceberg throughput must be present", op)
		}
		errorsValue, err := strconv.Atoi(cells[4])
		if err != nil {
			return fmt.Errorf("%s Iceberg errors must be numeric: %w", op, err)
		}
		if errorsValue != 0 {
			return fmt.Errorf("%s Iceberg errors=%d, want 0", op, errorsValue)
		}
		if !strings.Contains(cells[5], "benchmarks/profiles/iceberg-single-") {
			return fmt.Errorf("%s Iceberg artifact must point at benchmarks/profiles/iceberg-single-*", op)
		}
	}
	for op, seen := range required {
		if !seen {
			return fmt.Errorf("latest Iceberg benchmark table missing %s", op)
		}
	}
	return nil
}

func validateReadmePerformanceMatchesBenchmarks(readme, benchmarks string) error {
	want, err := readmePerformanceFromBenchmarks(benchmarks)
	if err != nil {
		return err
	}
	got, err := readmePerformanceRows(readme)
	if err != nil {
		return err
	}
	for target, wantRow := range want {
		gotRow, ok := got[target]
		if !ok {
			return fmt.Errorf("README Performance section missing %s row", target)
		}
		if gotRow != wantRow {
			return fmt.Errorf("README Performance %s row = %v, want %v", target, gotRow, wantRow)
		}
	}
	return nil
}

func readmePerformanceFromBenchmarks(markdown string) (map[string][4]string, error) {
	rows := map[string][2]string{}
	for _, line := range strings.Split(markdown, "\n") {
		cells := markdownTableCells(line)
		if len(cells) != 14 {
			continue
		}
		switch cells[0] {
		case "put":
			rows["MinIO"] = [2]string{cells[1], ""}
			rows["RustFS"] = [2]string{cells[5], ""}
			rows["GrainFS"] = [2]string{cells[9], ""}
		case "get":
			for target, value := range map[string]string{
				"MinIO":   cells[1],
				"RustFS":  cells[5],
				"GrainFS": cells[9],
			} {
				row := rows[target]
				row[1] = value
				rows[target] = row
			}
		}
	}
	minio, ok := rows["MinIO"]
	if !ok || minio[0] == "" || minio[1] == "" {
		return nil, fmt.Errorf("benchmark reference missing MinIO put/get rows")
	}
	out := make(map[string][4]string, len(rows))
	for target, row := range rows {
		if row[0] == "" || row[1] == "" {
			return nil, fmt.Errorf("benchmark reference missing %s put/get values", target)
		}
		putRatio, err := formattedRatio(row[0], minio[0])
		if err != nil {
			return nil, fmt.Errorf("%s PUT ratio: %w", target, err)
		}
		getRatio, err := formattedRatio(row[1], minio[1])
		if err != nil {
			return nil, fmt.Errorf("%s GET ratio: %w", target, err)
		}
		out[target] = [4]string{row[0], row[1], putRatio, getRatio}
	}
	return out, nil
}

func readmePerformanceRows(markdown string) (map[string][4]string, error) {
	section, err := markdownSection(markdown, "## Performance")
	if err != nil {
		return nil, err
	}
	rows := map[string][4]string{}
	for _, line := range strings.Split(section, "\n") {
		cells := markdownTableCells(line)
		if len(cells) != 5 {
			continue
		}
		target := strings.Trim(cells[0], "`")
		if target != "GrainFS" && target != "MinIO" && target != "RustFS" {
			continue
		}
		rows[target] = [4]string{cells[1], cells[2], cells[3], cells[4]}
	}
	return rows, nil
}

func formattedRatio(value, baseline string) (string, error) {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}
	base, err := strconv.ParseFloat(baseline, 64)
	if err != nil {
		return "", err
	}
	if base == 0 {
		return "", fmt.Errorf("baseline is zero")
	}
	return fmt.Sprintf("%.2fx", v/base), nil
}

func validateReadmePerformanceScope(markdown string) error {
	section, err := markdownSection(markdown, "## Performance")
	if err != nil {
		return err
	}
	for _, required := range []string{"PUT MiB/s", "GET MiB/s", "vs MinIO PUT", "vs MinIO GET"} {
		if !strings.Contains(section, required) {
			return fmt.Errorf("README Performance section must include %q", required)
		}
	}
	for _, forbidden := range []string{
		"DELETE",
		"MIXED",
		"LIST",
		"STAT",
		"VERSIONED",
		"RETENTION",
		"MULTIPART",
		"APPEND",
		"Iceberg",
		"catalog-read",
		"catalog-commits",
		"catalog-mixed",
		"sustained",
	} {
		if strings.Contains(section, forbidden) {
			return fmt.Errorf("README Performance section must not publish %s results; use docs/reference/benchmarks.md", forbidden)
		}
	}
	return nil
}

func markdownSection(markdown, heading string) (string, error) {
	start := strings.Index(markdown, heading)
	if start < 0 {
		return "", fmt.Errorf("missing section %q", heading)
	}
	rest := markdown[start+len(heading):]
	end := strings.Index(rest, "\n## ")
	if end < 0 {
		return rest, nil
	}
	return rest[:end], nil
}

func benchmarkThroughput(mib, objs string) (float64, error) {
	if mib != "0.00" {
		return strconv.ParseFloat(mib, 64)
	}
	return strconv.ParseFloat(objs, 64)
}
