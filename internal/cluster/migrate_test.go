package cluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupSoloData creates a solo-mode data directory with metadata in BadgerDB,
// simulating what a Phase 1 LocalBackend would have produced.
func setupSoloData(t *testing.T, dir string, buckets []string, objects map[string]map[string][]byte) {
	t.Helper()

	metaDir := filepath.Join(dir, "meta")
	require.NoError(t, os.MkdirAll(metaDir, 0o755))

	dataDir := filepath.Join(dir, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	opts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		for _, bucket := range buckets {
			if err := txn.Set([]byte("bucket:"+bucket), []byte(`{}`)); err != nil {
				return err
			}
			bucketDir := filepath.Join(dataDir, bucket)
			if err := os.MkdirAll(bucketDir, 0o755); err != nil {
				return err
			}
		}

		for bucket, files := range objects {
			for key, content := range files {
				// Write data file
				objPath := filepath.Join(dataDir, bucket, key)
				if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
					return err
				}
				if err := os.WriteFile(objPath, content, 0o644); err != nil {
					return err
				}

				// Write metadata
				meta, _ := json.Marshal(map[string]any{
					"Key":          key,
					"Size":         len(content),
					"ContentType":  "application/octet-stream",
					"ETag":         "fakeetag",
					"LastModified": 1700000000,
				})
				mk := []byte("obj:" + bucket + "/" + key)
				if err := txn.Set(mk, meta); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestMigrateSoloToCluster_Basic(t *testing.T) {
	dir := t.TempDir()

	setupSoloData(t, dir, []string{"docs", "images"}, map[string]map[string][]byte{
		"docs":   {"readme.md": []byte("# Hello"), "guide.txt": []byte("guide content")},
		"images": {"logo.png": []byte("PNG fake data")},
	})

	err := MigrateSoloToCluster(dir, "node-1")
	require.NoError(t, err)

	// Verify Raft log was created
	raftDir := filepath.Join(dir, "raft")
	_, err = os.Stat(raftDir)
	require.NoError(t, err, "raft directory should exist after migration")

	// Verify metadata is still intact by re-opening BadgerDB
	metaDir := filepath.Join(dir, "meta")
	opts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Check buckets
	var bucketCount int
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("bucket:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			bucketCount++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, bucketCount)

	// Check objects
	var objCount int
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("obj:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			objCount++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, objCount)
}

func TestMigrateSoloToCluster_EmptyData(t *testing.T) {
	dir := t.TempDir()

	setupSoloData(t, dir, nil, nil)

	err := MigrateSoloToCluster(dir, "node-1")
	require.NoError(t, err)
}

func TestMigrateSoloToCluster_NoMetaDir(t *testing.T) {
	dir := t.TempDir()

	err := MigrateSoloToCluster(dir, "node-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata directory not found")
}

func TestMigrateSoloToCluster_ManyObjects(t *testing.T) {
	dir := t.TempDir()

	objects := make(map[string][]byte)
	for i := range 50 {
		key := filepath.Join("dir", string(rune('a'+i%26)), "file.txt")
		objects[key] = []byte("content")
	}

	setupSoloData(t, dir, []string{"bucket"}, map[string]map[string][]byte{
		"bucket": objects,
	})

	err := MigrateSoloToCluster(dir, "node-1")
	require.NoError(t, err)
}
