package cluster

// Phase 18 Cluster EC: production EC helpers shared by PutObject/GetObject.
// Placement formula matches internal/cluster/ecspike/ intentionally — the spike
// validated this choice. When cluster size N == k+m, each key's shards land on
// N distinct nodes.

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"path/filepath"
)

// Default EC parameters for 4+2 Reed-Solomon. Exposed so serve flags
// and tests share a single source of truth.
const (
	DefaultDataShards   = 4
	DefaultParityShards = 2
	// MinECNodes is the minimum cluster size at which EC activates.
	// Clusters of 1–2 nodes use N× replication; 3+ always use proportional EC.
	MinECNodes = 3
)

// ECConfig controls cluster erasure coding behavior.
type ECConfig struct {
	// DataShards (k) — number of data shards (e.g. 4 for 4+2).
	DataShards int
	// ParityShards (m) — number of parity shards (e.g. 2 for 4+2).
	ParityShards int
}

// NumShards returns k+m.
func (c ECConfig) NumShards() int { return c.DataShards + c.ParityShards }

// IsActive returns true iff the cluster has at least MinECNodes nodes.
// 1–2 node clusters use N× replication; 3+ always activate proportional EC.
func (c ECConfig) IsActive(clusterSize int) bool {
	return c.DataShards > 0 && c.ParityShards > 0 && clusterSize >= MinECNodes
}

// EffectiveConfig returns the ECConfig that should be used for a cluster of n nodes,
// proportionally scaling k and m from the target config. For n < MinECNodes it returns
// a zero-value config. For n >= target.NumShards() it returns target as-is.
// Formula: m_eff = max(1, round(n × m_target / (k_target+m_target))), k_eff = n - m_eff.
func EffectiveConfig(n int, target ECConfig) ECConfig {
	if n < MinECNodes {
		return ECConfig{}
	}
	if n >= target.NumShards() {
		return target
	}
	mEff := int(math.Round(float64(n) * float64(target.ParityShards) / float64(target.NumShards())))
	if mEff < 1 {
		mEff = 1
	}
	return ECConfig{DataShards: n - mEff, ParityShards: mEff}
}

// Placement returns the node index (into the ordered node slice) that holds
// shardIdx for the given key. Formula: (FNV32(key) + shardIdx) mod N.
// When shardCount == N, all shards for a key land on N distinct nodes.
func Placement(key string, shardIdx, numNodes int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return (int(h.Sum32()) + shardIdx) % numNodes
}

// PlacementForNodes returns the ordered list of nodeIDs responsible for each
// shardIdx of the given key. Length equals cfg.NumShards().
// nodes must be deterministically ordered across the cluster (sorted).
func PlacementForNodes(cfg ECConfig, nodes []string, key string) []string {
	n := cfg.NumShards()
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = nodes[Placement(key, i, len(nodes))]
	}
	return out
}

// shardHeaderSize is the per-shard prefix that records original object size
// so Reconstruct can call reedsolomon.Join(writer, shards, dataLen).
const shardHeaderSize = 8

func encodeShardHeader(origSize int64) [shardHeaderSize]byte {
	var h [shardHeaderSize]byte
	binary.BigEndian.PutUint64(h[:], uint64(origSize))
	return h
}

func decodeShardHeader(data []byte) (origSize int64, body []byte, err error) {
	if len(data) < shardHeaderSize {
		return 0, nil, fmt.Errorf("shard too small for header: %d bytes", len(data))
	}
	size := int64(binary.BigEndian.Uint64(data[:shardHeaderSize]))
	return size, data[shardHeaderSize:], nil
}

// ECSplit encodes object data into k+m shards ready for per-node storage.
// Each returned shard already contains the size header. Result length == cfg.NumShards().
func ECSplit(cfg ECConfig, data []byte) ([][]byte, error) {
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("ec encoder: %w", err)
	}
	shards, err := enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("ec split: %w", err)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec encode: %w", err)
	}
	header := encodeShardHeader(int64(len(data)))
	out := make([][]byte, len(shards))
	for i, s := range shards {
		payload := make([]byte, shardHeaderSize+len(s))
		copy(payload, header[:])
		copy(payload[shardHeaderSize:], s)
		out[i] = payload
	}
	return out, nil
}

// ECReconstruct assembles the original data from at least k of k+m shards.
// Missing shards are represented by nil entries. Returns the original bytes.
func ECReconstruct(cfg ECConfig, shards [][]byte) ([]byte, error) {
	if len(shards) != cfg.NumShards() {
		return nil, fmt.Errorf("shard count mismatch: got %d, want %d", len(shards), cfg.NumShards())
	}
	var origSize int64 = -1
	bodies := make([][]byte, len(shards))
	for i, s := range shards {
		if s == nil {
			continue
		}
		size, body, err := decodeShardHeader(s)
		if err != nil {
			continue
		}
		if origSize < 0 {
			origSize = size
		}
		bodies[i] = body
	}
	if origSize < 0 {
		return nil, fmt.Errorf("no readable shards")
	}
	if origSize == 0 {
		return []byte{}, nil
	}
	enc, err := getEncoder(cfg)
	if err != nil {
		return nil, fmt.Errorf("ec decoder: %w", err)
	}
	if err := enc.ReconstructData(bodies); err != nil {
		return nil, fmt.Errorf("ec reconstruct: %w", err)
	}
	var buf writeBuffer
	buf.b = make([]byte, 0, origSize)
	if err := enc.Join(&buf, bodies, int(origSize)); err != nil {
		return nil, fmt.Errorf("ec join: %w", err)
	}
	return buf.b, nil
}

// writeBuffer is an io.Writer that appends to an internal slice — used to
// avoid allocating bytes.Buffer (smaller footprint for in-memory join).
type writeBuffer struct {
	b []byte
}

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// shardFilePath returns the on-disk filename used when a node stores its
// local shard of an EC-encoded object. The path is distinct from the object
// path so we can keep old N×-replicated full-object files coexisting during
// migration (Slice 5).
//
//nolint:unused // package tests pin shard path compatibility.
func shardFilePath(dataRoot, bucket, key string, shardIdx int) string {
	return filepath.Join(dataRoot, "ec-shards", bucket, key, fmt.Sprintf("shard_%d", shardIdx))
}
