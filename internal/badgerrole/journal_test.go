package badgerrole

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalWriterRecordsPendingEntryWithRelativePath(t *testing.T) {
	root := t.TempDir()
	observed := time.Date(2026, 5, 15, 1, 2, 3, 0, time.UTC)
	writer := NewJournalWriter(JournalOptions{
		DataDir:       root,
		NodeID:        "n1",
		BootID:        "boot-1",
		BinaryVersion: "test-version",
		Now:           func() time.Time { return observed },
	})

	entry, err := writer.Record(Decision{
		Role:   RoleMeta,
		Path:   filepath.Join(root, "meta"),
		Status: DecisionOpenFailed,
		Action: RecoveryActionBlockStart,
		Reason: "open failed",
	}, StartupModeBlocked)
	require.NoError(t, err)
	require.NotEmpty(t, entry.ID)
	require.NotEmpty(t, entry.IncidentID)
	require.Equal(t, "meta", entry.Decision.Path)
	require.Equal(t, observed, entry.ObservedAt)

	pending, err := PendingJournalEntries(root)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, entry.ID, pending[0].ID)
	require.Equal(t, entry.IncidentID, pending[0].IncidentID)

	raw, err := os.ReadFile(filepath.Join(root, ".recovery", "entries", entry.ID+".json"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), root, "journal entries must not expose absolute dataDir paths")

	var decoded JournalEntry
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "meta", decoded.Decision.Path)
}

func TestJournalWriterScrubsAbsoluteDataDirFromReason(t *testing.T) {
	root := t.TempDir()
	writer := NewJournalWriter(JournalOptions{DataDir: root, NodeID: "n1"})

	entry, err := writer.Record(Decision{
		Role:   RoleMeta,
		Path:   filepath.Join(root, "meta"),
		Status: DecisionOpenFailed,
		Action: RecoveryActionBlockStart,
		Reason: "open metadata db at " + filepath.Join(root, "meta") + ": lock held",
	}, StartupModeBlocked)
	require.NoError(t, err)
	require.NotContains(t, entry.Decision.Reason, root)
	require.Contains(t, entry.Decision.Reason, "meta")

	raw, err := os.ReadFile(filepath.Join(root, ".recovery", "entries", entry.ID+".json"))
	require.NoError(t, err)
	require.NotContains(t, string(raw), root)
}

func TestJournalImportedMarkerRemovesEntryFromPendingSet(t *testing.T) {
	root := t.TempDir()
	writer := NewJournalWriter(JournalOptions{DataDir: root, NodeID: "n1"})
	entry, err := writer.Record(Decision{
		Role:   RoleSharedFSM,
		Path:   filepath.Join(root, "shared-fsm"),
		Status: DecisionOpenFailed,
		Action: RecoveryActionBlockStart,
		Reason: "disk full",
	}, StartupModeBlocked)
	require.NoError(t, err)

	require.NoError(t, MarkJournalEntryImported(root, entry.ID))

	pending, err := PendingJournalEntries(root)
	require.NoError(t, err)
	require.Empty(t, pending)
	require.FileExists(t, filepath.Join(root, ".recovery", "imported", entry.ID+".json"))
}
