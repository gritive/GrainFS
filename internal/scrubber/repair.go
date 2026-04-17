package scrubber

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// RepairEngine reconstructs and rewrites degraded EC shards.
type RepairEngine struct {
	backend Scrubbable
}

// NewRepairEngine creates a RepairEngine.
func NewRepairEngine(b Scrubbable) *RepairEngine {
	return &RepairEngine{backend: b}
}

// Repair reconstructs missing/corrupt shards using Reed-Solomon.
// Returns an error if more shards are damaged than parity allows.
func (r *RepairEngine) Repair(rec ObjectRecord, status ShardStatus) error {
	damaged := len(status.Missing) + len(status.Corrupt)
	if damaged > rec.ParityShards {
		return fmt.Errorf("unrepairable: %d shards lost, only %d parity", damaged, rec.ParityShards)
	}

	total := rec.DataShards + rec.ParityShards
	paths := r.backend.ShardPaths(rec.Bucket, rec.Key, rec.VersionID, total)

	shards := make([][]byte, total)
	for i, path := range paths {
		data, err := r.backend.ReadShard(rec.Bucket, rec.Key, path)
		if err != nil {
			shards[i] = nil // nil → reedsolomon treats as missing
			continue
		}
		shards[i] = data
	}

	enc, err := reedsolomon.New(rec.DataShards, rec.ParityShards)
	if err != nil {
		return fmt.Errorf("create encoder: %w", err)
	}
	if err := enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("reconstruct: %w", err)
	}

	// Rewrite only damaged shards
	toRepair := append(append([]int{}, status.Missing...), status.Corrupt...)
	for _, idx := range toRepair {
		if err := r.backend.WriteShard(rec.Bucket, rec.Key, paths[idx], shards[idx]); err != nil {
			return fmt.Errorf("write shard %d: %w", idx, err)
		}
	}
	return nil
}
