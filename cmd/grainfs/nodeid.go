package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// generateNodeID returns a persistent node ID for the given data directory.
// If a node-id file already exists, it is reused; otherwise a new UUID is generated and saved.
func generateNodeID(dataDir string) (string, error) {
	idFile := filepath.Join(dataDir, "node-id")
	if data, err := os.ReadFile(idFile); err == nil {
		id := strings.TrimSpace(string(data))
		if id != "" {
			return id, nil
		}
	}

	id := uuid.New().String()
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(idFile, []byte(id+"\n"), 0o644); err != nil {
		return "", err
	}
	return id, nil
}
