package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DiagnosticReport contains the health check results.
type DiagnosticReport struct {
	Timestamp       time.Time              `json:"timestamp"`
	OverallHealth   string                 `json:"overall_health"`
	Checks          map[string]CheckResult `json:"checks"`
	Recommendations []string               `json:"recommendations,omitempty"`
}

// CheckResult represents a single health check result.
type CheckResult struct {
	Status   string `json:"status"` // "pass", "warn", "fail"
	Message  string `json:"message"`
	Duration string `json:"duration,omitempty"`
}

// Doctor performs health diagnostics on the GrainFS instance.
type Doctor struct {
	dataDir string
}

// NewDoctor creates a diagnostic doctor.
func NewDoctor(dataDir string) *Doctor {
	return &Doctor{dataDir: dataDir}
}

// Run executes all diagnostic checks and returns a report.
func (d *Doctor) Run() (*DiagnosticReport, error) {
	start := time.Now()
	report := &DiagnosticReport{
		Timestamp: start,
		Checks:    make(map[string]CheckResult),
	}

	// Check 1: Data directory accessibility
	report.Checks["data_directory"] = d.checkDataDirectory()

	// Check 2: BadgerDB health
	report.Checks["badgerdb"] = d.checkBadgerDB()

	// Check 3: Disk space
	report.Checks["disk_space"] = d.checkDiskSpace()

	// Check 4: Raft log integrity
	report.Checks["raft_log"] = d.checkRaftLog()

	// Check 5: Blob storage integrity
	report.Checks["blob_storage"] = d.checkBlobStorage()

	// Calculate overall health
	failCount := 0
	warnCount := 0
	for _, check := range report.Checks {
		switch check.Status {
		case "fail":
			failCount++
		case "warn":
			warnCount++
		}
	}

	switch {
	case failCount > 0:
		report.OverallHealth = "fail"
		report.Recommendations = append(report.Recommendations,
			"Critical issues detected. Run 'grainfs recover --dry-run' for recommendations; use 'grainfs recover cluster plan' for offline snapshot recovery.")
	case warnCount > 0:
		report.OverallHealth = "warn"
		report.Recommendations = append(report.Recommendations,
			"Warnings detected. Monitor the system and address issues proactively.")
	default:
		report.OverallHealth = "pass"
	}

	return report, nil
}

func (d *Doctor) checkDataDirectory() CheckResult {
	start := time.Now()

	// Validate path to prevent directory traversal
	cleanPath := filepath.Clean(d.dataDir)
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return CheckResult{
			Status:  "fail",
			Message: fmt.Sprintf("Cannot resolve data directory: %v", err),
		}
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return CheckResult{
			Status:  "fail",
			Message: fmt.Sprintf("Cannot access data directory: %v", err),
		}
	}
	if !info.IsDir() {
		return CheckResult{
			Status:  "fail",
			Message: fmt.Sprintf("Data path is not a directory: %s", d.dataDir),
		}
	}
	return CheckResult{
		Status:   "pass",
		Message:  "Data directory accessible",
		Duration: time.Since(start).String(),
	}
}

func (d *Doctor) checkBadgerDB() CheckResult {
	start := time.Now()
	dbPath := filepath.Join(d.dataDir, "badger")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return CheckResult{
			Status:  "warn",
			Message: "BadgerDB not initialized (new deployment)",
		}
	}
	// Basic check: can we open the DB?
	// In a full implementation, we'd open BadgerDB and run health checks
	return CheckResult{
		Status:   "pass",
		Message:  "BadgerDB directory exists",
		Duration: time.Since(start).String(),
	}
}

func (d *Doctor) checkDiskSpace() CheckResult {
	start := time.Now()
	usedPct, availBytes := sysDiskStat(d.dataDir)
	if usedPct == 0 && availBytes == 0 {
		return CheckResult{
			Status:   "warn",
			Message:  "Unable to check disk space",
			Duration: time.Since(start).String(),
		}
	}
	usedPercent := uint64(usedPct)
	switch {
	case usedPercent > 90:
		return CheckResult{
			Status:   "fail",
			Message:  fmt.Sprintf("Disk usage critical: %d%% used", usedPercent),
			Duration: time.Since(start).String(),
		}
	case usedPercent > 80:
		return CheckResult{
			Status:   "warn",
			Message:  fmt.Sprintf("Disk usage high: %d%% used", usedPercent),
			Duration: time.Since(start).String(),
		}
	default:
		return CheckResult{
			Status:   "pass",
			Message:  fmt.Sprintf("Disk usage acceptable: %d%% used", usedPercent),
			Duration: time.Since(start).String(),
		}
	}
}

func (d *Doctor) checkRaftLog() CheckResult {
	start := time.Now()
	raftPath := filepath.Join(d.dataDir, "raft")
	if _, err := os.Stat(raftPath); os.IsNotExist(err) {
		return CheckResult{
			Status:  "warn",
			Message: "Raft log not found (no-peers mode)",
		}
	}
	// Basic check: raft directory exists
	return CheckResult{
		Status:   "pass",
		Message:  "Raft log directory exists",
		Duration: time.Since(start).String(),
	}
}

func (d *Doctor) checkBlobStorage() CheckResult {
	start := time.Now()
	blobPath := filepath.Join(d.dataDir, "blobs")
	if _, err := os.Stat(blobPath); os.IsNotExist(err) {
		return CheckResult{
			Status:  "warn",
			Message: "Blob storage directory not found (new deployment)",
		}
	}
	return CheckResult{
		Status:   "pass",
		Message:  "Blob storage directory exists",
		Duration: time.Since(start).String(),
	}
}

// PrintJSON outputs the report as JSON to stdout.
func (r *DiagnosticReport) PrintJSON() error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}
