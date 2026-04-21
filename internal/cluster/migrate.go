package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

// MigrateLegacyMetaToCluster converts a legacy data directory to cluster format.
// It reads existing metadata from BadgerDB and re-proposes it through Raft
// as a single-node cluster, establishing a clean Raft log baseline.
func MigrateLegacyMetaToCluster(dataDir, nodeID string) error {
	logger := slog.With("component", "migrate")

	metaDir := filepath.Join(dataDir, "meta")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return fmt.Errorf("metadata directory not found: %s", metaDir)
	}

	// Open the existing metadata DB
	dbOpts := badger.DefaultOptions(metaDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db: %w", err)
	}
	defer db.Close()

	// Collect all existing metadata entries
	var buckets []string
	type objEntry struct {
		bucket string
		key    string
		meta   []byte
	}
	var objects []objEntry
	var multiparts [][]byte

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())

			if strings.HasPrefix(k, "bucket:") {
				buckets = append(buckets, strings.TrimPrefix(k, "bucket:"))
				continue
			}
			if strings.HasPrefix(k, "obj:") {
				rest := strings.TrimPrefix(k, "obj:")
				slashIdx := strings.Index(rest, "/")
				if slashIdx == -1 {
					continue
				}
				bucket := rest[:slashIdx]
				key := rest[slashIdx+1:]
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				objects = append(objects, objEntry{bucket: bucket, key: key, meta: val})
				continue
			}
			if strings.HasPrefix(k, "mpu:") {
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				multiparts = append(multiparts, val)
				continue
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan metadata: %w", err)
	}

	logger.Info("metadata scan complete",
		"buckets", len(buckets),
		"objects", len(objects),
		"multiparts", len(multiparts),
	)

	// Create Raft log store
	raftDir := filepath.Join(dataDir, "raft")
	logStore, err := raft.NewBadgerLogStore(raftDir)
	if err != nil {
		return fmt.Errorf("create raft store: %w", err)
	}
	defer logStore.Close()

	// Create a single-node Raft cluster and propose all entries
	cfg := raft.DefaultConfig(nodeID, nil) // no peers = legacy bootstrap
	node := raft.NewNode(cfg, logStore)

	// For a legacy node, set transport stubs (no peers to communicate with)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers during migration")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers during migration")
		},
	)

	// Start the node — it will become leader since it's the only voter
	node.Start()
	defer node.Stop()

	// Wait for leadership (legacy node should become leader within a few election timeouts)
	for range 200 {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		return fmt.Errorf("node did not become leader during migration")
	}

	// Start FSM apply loop to keep metadata in sync
	fsm := NewFSM(db)
	stopApply := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopApply:
				return
			case entry := <-node.ApplyCh():
				if err := fsm.Apply(entry.Command); err != nil {
					logger.Error("fsm apply error during migration", "index", entry.Index, "error", err)
				}
			}
		}
	}()
	defer close(stopApply)

	// Propose bucket creations
	for _, bucket := range buckets {
		data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
		if err != nil {
			return fmt.Errorf("encode bucket cmd: %w", err)
		}
		if err := node.Propose(data); err != nil {
			return fmt.Errorf("propose create bucket %s: %w", bucket, err)
		}
	}

	// Propose object metadata
	for _, obj := range objects {
		var meta struct {
			Key          string `json:"Key"`
			Size         int64  `json:"Size"`
			ContentType  string `json:"ContentType"`
			ETag         string `json:"ETag"`
			LastModified int64  `json:"LastModified"`
		}
		if err := json.Unmarshal(obj.meta, &meta); err != nil {
			logger.Warn("skip malformed object meta", "bucket", obj.bucket, "key", obj.key, "error", err)
			continue
		}
		data, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      obj.bucket,
			Key:         meta.Key,
			Size:        meta.Size,
			ContentType: meta.ContentType,
			ETag:        meta.ETag,
			ModTime:     meta.LastModified,
		})
		if err != nil {
			return fmt.Errorf("encode object cmd: %w", err)
		}
		if err := node.Propose(data); err != nil {
			return fmt.Errorf("propose put object %s/%s: %w", obj.bucket, obj.key, err)
		}
	}

	logger.Info("migration complete",
		"proposed_buckets", len(buckets),
		"proposed_objects", len(objects),
	)

	return nil
}
