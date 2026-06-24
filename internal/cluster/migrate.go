package cluster

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/raft"
)

// MigrateLegacyMetaToCluster converts a legacy data directory to cluster
// format. It reads existing metadata from legacyStore (the caller opens the
// legacy DB under dataDir/meta and owns its lifecycle — Phase 6.5 S3 moved
// the badger.Open to the composition root, see serveruntime bootAutoMigrate)
// and re-proposes it through Raft as a single-node cluster, establishing a
// clean Raft log baseline.
func MigrateLegacyMetaToCluster(legacyStore MetadataStore, dataDir, nodeID string) error {
	if legacyStore == nil {
		return fmt.Errorf("migrate: nil legacy metadata store")
	}
	logger := log.With().Str("component", "migrate").Logger()
	store := legacyStore

	// Collect all existing metadata entries
	var buckets []string
	type objEntry struct {
		bucket string
		key    string
		meta   []byte
	}
	var objects []objEntry
	var multiparts [][]byte

	err := store.View(func(txn MetadataTxn) error {
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
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

	logger.Info().Int("buckets", len(buckets)).Int("objects", len(objects)).Int("multiparts", len(multiparts)).Msg("metadata scan complete")

	raftDir := filepath.Join(dataDir, "raft")
	cfg := raft.DefaultConfig(nodeID, nil) // no peers = legacy bootstrap
	node, closeRaft, err := NewRaftV2NodeForServeruntime(cfg, raftDir)
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}
	defer func() {
		if closeRaft != nil {
			_ = closeRaft()
		}
	}()

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
	defer node.Close()

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
	fsm := NewFSM(store, newStateKeyspaceEmpty())
	stopApply := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopApply:
				return
			case entry, ok := <-node.ApplyCh():
				if !ok {
					// ApplyCh closed (node stopped); exit cleanly to avoid
					// busy-looping on zero-value reads.
					logger.Debug().Msg("migration apply loop: ApplyCh closed; exiting")
					return
				}
				if err := fsm.Apply(entry.Command); err != nil {
					logger.Error().Uint64("index", entry.Index).Err(err).Msg("fsm apply error during migration")
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

	// Legacy object metadata is no longer re-proposed: per-object FSM commands
	// (CmdPutObjectMeta) were retired in data-plane raft-free Slice 2 — their apply
	// is a no-op, so re-proposing migrated nothing. Objects now live as off-raft
	// quorum-meta blobs; greenfield clusters never reach bootAutoMigrate. The scan
	// above still reports legacy object/multipart counts for operator visibility.

	logger.Info().Int("proposed_buckets", len(buckets)).Msg("migration complete")

	return nil
}
