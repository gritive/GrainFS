package cluster

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/migration"
)

func newMigrationTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func makeMigrationCmd(t *testing.T, cmdType MetaCmdType, payload []byte) []byte {
	t.Helper()
	cmd, err := encodeMetaCmd(cmdType, payload)
	require.NoError(t, err)
	return cmd
}

// TestMetaFSM_Migration_StoreNotWired verifies that all three apply methods
// return an error when SetMigration has not been called.
func TestMetaFSM_Migration_StoreNotWired(t *testing.T) {
	ts := time.Now().UnixNano()
	tests := []struct {
		name    string
		cmdType clusterpb.MetaCmdType
		payload []byte
	}{
		{"JobStart", clusterpb.MetaCmdTypeMigrationJobStart, migration.EncodeJobStartPayload("b", ts)},
		{"JobDone", clusterpb.MetaCmdTypeMigrationJobDone, migration.EncodeJobDonePayload("b", 0, 0, ts)},
		{"JobFailed", clusterpb.MetaCmdTypeMigrationJobFailed, migration.EncodeJobFailedPayload("b", "err", 0, ts)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := NewMetaFSM()
			cmd := makeMigrationCmd(t, tc.cmdType, tc.payload)
			err := f.applyCmd(cmd)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "migration store not wired")
		})
	}
}

// TestMetaFSM_MigrationJobStart verifies the happy path and decode-error path.
func TestMetaFSM_MigrationJobStart(t *testing.T) {
	store := migration.NewJobStore(newMigrationTestDB(t))
	f := NewMetaFSM()
	f.SetMigration(store)

	ts := time.Now().Truncate(time.Nanosecond)
	cmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobStart,
		migration.EncodeJobStartPayload("bucket-a", ts.UnixNano()))

	require.NoError(t, f.applyCmd(cmd))

	job, err := store.GetJob("bucket-a")
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, migration.StatusRunning, job.Status)
	assert.Equal(t, "bucket-a", job.Bucket)
	assert.Equal(t, ts.UnixNano(), job.StartedAt.UnixNano())

	// decode error: truncated payload
	badCmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobStart, []byte{0x01})
	err = f.applyCmd(badCmd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MigrationJobStart")
}

// TestMetaFSM_MigrationJobDone verifies status transition to Complete and job==nil path.
func TestMetaFSM_MigrationJobDone(t *testing.T) {
	store := migration.NewJobStore(newMigrationTestDB(t))
	f := NewMetaFSM()
	f.SetMigration(store)

	// Seed a running job first.
	require.NoError(t, store.SaveJob(&migration.JobState{
		Bucket: "bucket-b",
		Status: migration.StatusRunning,
		Copied: 0,
		Errors: 0,
	}))

	ts := time.Now()
	cmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobDone,
		migration.EncodeJobDonePayload("bucket-b", 42, 3, ts.UnixNano()))
	require.NoError(t, f.applyCmd(cmd))

	job, err := store.GetJob("bucket-b")
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, migration.StatusComplete, job.Status)
	assert.Equal(t, int64(42), job.Copied)
	assert.Equal(t, int64(3), job.Errors)

	// job==nil path: bucket not in store → creates a new record.
	cmd2 := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobDone,
		migration.EncodeJobDonePayload("bucket-new", 5, 0, ts.UnixNano()))
	require.NoError(t, f.applyCmd(cmd2))
	job2, err := store.GetJob("bucket-new")
	require.NoError(t, err)
	require.NotNil(t, job2)
	assert.Equal(t, migration.StatusComplete, job2.Status)

	// decode error
	badCmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobDone, []byte{0x01})
	require.Error(t, f.applyCmd(badCmd))
}

// TestMetaFSM_MigrationJobFailed verifies status transition to Failed and job==nil path.
func TestMetaFSM_MigrationJobFailed(t *testing.T) {
	store := migration.NewJobStore(newMigrationTestDB(t))
	f := NewMetaFSM()
	f.SetMigration(store)

	require.NoError(t, store.SaveJob(&migration.JobState{
		Bucket: "bucket-c",
		Status: migration.StatusRunning,
	}))

	ts := time.Now()
	cmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobFailed,
		migration.EncodeJobFailedPayload("bucket-c", "list error", 7, ts.UnixNano()))
	require.NoError(t, f.applyCmd(cmd))

	job, err := store.GetJob("bucket-c")
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, migration.StatusFailed, job.Status)
	assert.Equal(t, "list error", job.Reason)
	assert.Equal(t, int64(7), job.Errors)

	// job==nil path
	cmd2 := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobFailed,
		migration.EncodeJobFailedPayload("bucket-none", "oops", 1, ts.UnixNano()))
	require.NoError(t, f.applyCmd(cmd2))
	job2, err := store.GetJob("bucket-none")
	require.NoError(t, err)
	require.NotNil(t, job2)
	assert.Equal(t, migration.StatusFailed, job2.Status)

	// decode error
	badCmd := makeMigrationCmd(t, clusterpb.MetaCmdTypeMigrationJobFailed, []byte{0x01})
	require.Error(t, f.applyCmd(badCmd))
}
