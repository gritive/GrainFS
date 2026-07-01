package cluster

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
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

	// Bucket control-plane moved to meta-raft and per-object metadata moved
	// off-raft, so there is nothing to propose. The raft node setup + proposal
	// loop that used to live here is removed; the metadata scan above still logs
	// counts for operator visibility. Greenfield clusters never reach bootAutoMigrate.
	logger.Info().Int("buckets", len(buckets)).Int("objects", len(objects)).Int("multiparts", len(multiparts)).Msg("migration: legacy metadata scan complete (no proposals needed)")

	return nil
}
