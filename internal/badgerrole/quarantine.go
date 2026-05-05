package badgerrole

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type QuarantineRequest struct {
	Role    Role
	GroupID string
	Path    string
	Reason  string
	Now     time.Time
}

type QuarantineResult struct {
	Role           Role      `json:"role"`
	GroupID        string    `json:"group_id,omitempty"`
	OriginalPath   string    `json:"original_path"`
	QuarantinePath string    `json:"quarantine_path"`
	Reason         string    `json:"reason"`
	CommittedAt    time.Time `json:"committed_at"`
}

func QuarantineRole(req QuarantineRequest) (QuarantineResult, error) {
	if req.Path == "" {
		return QuarantineResult{}, fmt.Errorf("quarantine %s: path required", req.Role)
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	parent := filepath.Dir(req.Path)
	base := filepath.Base(req.Path)
	dst := filepath.Join(parent, fmt.Sprintf("%s.quarantine.%s", base, req.Now.Format("20060102T150405.000000000Z")))

	if err := os.Rename(req.Path, dst); err != nil {
		return QuarantineResult{}, fmt.Errorf("quarantine %s: rename %s to %s: %w", req.Role, req.Path, dst, err)
	}

	result := QuarantineResult{
		Role:           req.Role,
		GroupID:        req.GroupID,
		OriginalPath:   req.Path,
		QuarantinePath: dst,
		Reason:         req.Reason,
		CommittedAt:    req.Now,
	}
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return QuarantineResult{}, fmt.Errorf("quarantine %s: marshal manifest: %w", req.Role, err)
	}
	manifestPath := filepath.Join(dst, "recovery-manifest.json")
	if err := os.WriteFile(manifestPath, append(body, '\n'), 0o644); err != nil {
		return QuarantineResult{}, fmt.Errorf("quarantine %s: write manifest: %w", req.Role, err)
	}
	if err := syncDir(dst); err != nil {
		return QuarantineResult{}, err
	}
	if err := syncDir(parent); err != nil {
		return QuarantineResult{}, err
	}
	return result, nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("sync dir %s: %w", path, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", path, err)
	}
	return nil
}
