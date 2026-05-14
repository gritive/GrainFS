package badgerrole

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const recoveryDirName = ".recovery"

type JournalOptions struct {
	DataDir       string
	NodeID        string
	BootID        string
	BinaryVersion string
	Now           func() time.Time
}

type JournalWriter struct {
	opts JournalOptions
}

type JournalDecision struct {
	Role           Role           `json:"role"`
	GroupID        string         `json:"group_id,omitempty"`
	Path           string         `json:"path,omitempty"`
	Status         DecisionStatus `json:"status"`
	Action         RecoveryAction `json:"action"`
	Reason         string         `json:"reason,omitempty"`
	ProbeDuration  string         `json:"probe_duration,omitempty"`
	OpenedReadOnly bool           `json:"opened_read_only,omitempty"`
}

type JournalEntry struct {
	ID            string          `json:"id"`
	IncidentID    string          `json:"incident_id"`
	NodeID        string          `json:"node_id,omitempty"`
	BootID        string          `json:"boot_id,omitempty"`
	BinaryVersion string          `json:"binary_version,omitempty"`
	ObservedAt    time.Time       `json:"observed_at"`
	StartupMode   StartupMode     `json:"startup_mode,omitempty"`
	Decision      JournalDecision `json:"decision"`
}

func NewJournalWriter(opts JournalOptions) *JournalWriter {
	return &JournalWriter{opts: opts}
}

func (w *JournalWriter) Record(decision Decision, startupMode StartupMode) (JournalEntry, error) {
	if w == nil {
		return JournalEntry{}, fmt.Errorf("badger recovery journal: nil writer")
	}
	if decision.Status == DecisionOK {
		return JournalEntry{}, nil
	}
	if w.opts.DataDir == "" {
		return JournalEntry{}, fmt.Errorf("badger recovery journal: data dir required")
	}
	now := time.Now().UTC()
	if w.opts.Now != nil {
		now = w.opts.Now().UTC()
	}
	jd := JournalDecision{
		Role:           decision.Role,
		GroupID:        decision.GroupID,
		Path:           relativeJournalPath(w.opts.DataDir, decision.Path),
		Status:         decision.Status,
		Action:         decision.Action,
		Reason:         scrubJournalReason(w.opts.DataDir, decision.Reason),
		OpenedReadOnly: decision.OpenedReadOnly,
	}
	if decision.ProbeDuration > 0 {
		jd.ProbeDuration = decision.ProbeDuration.String()
	}
	entryID, err := randomID()
	if err != nil {
		return JournalEntry{}, fmt.Errorf("badger recovery journal: entry id: %w", err)
	}
	entry := JournalEntry{
		ID:            entryID,
		IncidentID:    incidentIDFor(w.opts.NodeID, jd),
		NodeID:        w.opts.NodeID,
		BootID:        w.opts.BootID,
		BinaryVersion: w.opts.BinaryVersion,
		ObservedAt:    now,
		StartupMode:   startupMode,
		Decision:      jd,
	}
	path := filepath.Join(w.opts.DataDir, recoveryDirName, "entries", entry.ID+".json")
	if err := writeJSONFileAtomic(path, entry, 0o644); err != nil {
		return JournalEntry{}, err
	}
	return entry, nil
}

func PendingJournalEntries(dataDir string) ([]JournalEntry, error) {
	entriesDir := filepath.Join(dataDir, recoveryDirName, "entries")
	dirEntries, err := os.ReadDir(entriesDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("badger recovery journal: read entries: %w", err)
	}
	out := make([]JournalEntry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() || filepath.Ext(dirEntry.Name()) != ".json" {
			continue
		}
		id := strings.TrimSuffix(dirEntry.Name(), ".json")
		if journalEntryImported(dataDir, id) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(entriesDir, dirEntry.Name()))
		if err != nil {
			return nil, fmt.Errorf("badger recovery journal: read %s: %w", dirEntry.Name(), err)
		}
		var entry JournalEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("badger recovery journal: decode %s: %w", dirEntry.Name(), err)
		}
		out = append(out, entry)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	return out, nil
}

func MarkJournalEntryImported(dataDir, entryID string) error {
	if entryID == "" {
		return fmt.Errorf("badger recovery journal: imported entry id required")
	}
	marker := struct {
		EntryID    string    `json:"entry_id"`
		ImportedAt time.Time `json:"imported_at"`
	}{
		EntryID:    entryID,
		ImportedAt: time.Now().UTC(),
	}
	return writeJSONFileAtomic(filepath.Join(dataDir, recoveryDirName, "imported", entryID+".json"), marker, 0o644)
}

func journalEntryImported(dataDir, entryID string) bool {
	_, err := os.Stat(filepath.Join(dataDir, recoveryDirName, "imported", entryID+".json"))
	return err == nil
}

func relativeJournalPath(dataDir, path string) string {
	if path == "" {
		return ""
	}
	if dataDir != "" {
		if rel, err := filepath.Rel(dataDir, path); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ".." {
			return filepath.ToSlash(rel)
		}
	}
	if filepath.IsAbs(path) {
		return filepath.Base(path)
	}
	return filepath.ToSlash(filepath.Clean(path))
}

func scrubJournalReason(dataDir, reason string) string {
	if dataDir == "" || reason == "" {
		return reason
	}
	clean := filepath.Clean(dataDir)
	replacements := []struct {
		old string
		new string
	}{
		{clean + string(os.PathSeparator), ""},
		{clean, "."},
		{filepath.ToSlash(clean) + "/", ""},
		{filepath.ToSlash(clean), "."},
	}
	out := reason
	for _, repl := range replacements {
		out = strings.ReplaceAll(out, repl.old, repl.new)
	}
	return out
}

func incidentIDFor(nodeID string, decision JournalDecision) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s", nodeID, decision.Role, decision.GroupID, decision.Path, decision.Status)
	sum := h.Sum(nil)
	return "badger-" + hex.EncodeToString(sum[:12])
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func writeJSONFileAtomic(path string, v any, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("write json %s: mkdir: %w", path, err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("write json %s: create temp: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write json %s: encode: %w", path, err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write json %s: chmod: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write json %s: sync: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write json %s: close: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("write json %s: rename: %w", path, err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}
