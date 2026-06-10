package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// quorumMetaSubDir is the per-dataDir subdirectory where per-node quorum object
// metadata is durably stored. Sibling to shard files on the same device so the
// write rides the same I/O path (same spindle/NVMe).
const quorumMetaSubDir = ".quorum_meta"

// quorumMetaWriteTimeout bounds the synchronous quorum meta write. A node that
// does not ack within this window is treated as failed; the write succeeds as
// long as a quorum (K data shards) of nodes acked.
const quorumMetaWriteTimeout = 30 * time.Second

func (b *DistributedBackend) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	// Internal buckets stay on raft (control-plane; headObjectMeta reads BadgerDB
	// for them). Non-internal user buckets use per-node quorum (data_raft bypass).
	if storage.IsInternalBucket(cmd.Bucket) {
		return b.propose(ctx, CmdPutObjectMeta, cmd)
	}
	if b.shardSvc == nil || len(cmd.NodeIDs) == 0 {
		return fmt.Errorf("quorum meta write: no shard service or empty placement")
	}
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	if err != nil {
		return fmt.Errorf("quorum meta write encode: %w", err)
	}
	self := b.currentSelfAddr()
	k := int(cmd.ECData)
	if k <= 0 {
		k = 1
	}
	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	return fanOutQuorumMeta(wctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeQuorumMetaLocal(cmd.Bucket, cmd.Key, blob)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteQuorumMeta(fctx, addr, cmd.Bucket, cmd.Key, blob)
	})
}

// readQuorumMeta reads object metadata from the local quorum store.
// Returns (nil, PlacementMeta{}, storage.ErrObjectNotFound) when the object is
// not in the local store. Callers may fall back to BadgerDB for objects written
// before Phase 3 or by repair/scrubber paths.
func (b *DistributedBackend) readQuorumMeta(bucket, key string) (*storage.Object, PlacementMeta, error) {
	if b.shardSvc == nil {
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	return b.shardSvc.readQuorumMetaLocalDecoded(bucket, key)
}

// fanOutQuorumMeta dispatches to every placement node concurrently and returns
// as soon as K acks arrive. Returns an error only when the quorum becomes
// unreachable (more than N-K failures or context cancellation). Errors are
// propagated to the caller — unlike the Phase 0 shadow, failures here fail
// the PUT.
func fanOutQuorumMeta(ctx context.Context, nodes []string, k int, dispatch func(context.Context, string) error) error {
	if k <= 0 {
		k = 1
	}
	n := len(nodes)
	if n < k {
		return fmt.Errorf("quorum meta: placement nodes %d < quorum size %d", n, k)
	}
	results := make(chan error, n)
	for _, node := range nodes {
		node := node
		go func() { results <- dispatch(ctx, node) }()
	}
	var ok, failed int
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if err == nil {
				ok++
				if ok >= k {
					return nil
				}
			} else {
				failed++
				if failed > n-k {
					return fmt.Errorf("quorum meta: %d/%d nodes failed, quorum %d unreachable", failed, n, k)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// writeQuorumMetaLocal durably writes the encoded quorum meta blob for
// (bucket, key) under {dataDirs[0]}/.quorum_meta/{bucket}/{key}. One fsync —
// same durability cost as the shard write it co-locates with.
func (s *ShardService) writeQuorumMetaLocal(bucket, key string, data []byte) error {
	if len(s.dataDirs) == 0 {
		return fmt.Errorf("quorum meta: no data dir")
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta: key %q escapes root", key)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("quorum meta mkdir: %w", err)
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("quorum meta create: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("quorum meta write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("quorum meta fsync: %w", err)
	}
	return f.Close()
}

// readQuorumMetaLocalDecoded reads and decodes the quorum meta blob for
// (bucket, key) from the local store. Returns storage.ErrObjectNotFound if the
// file does not exist (object not yet written via Phase 3 PUT, or on a node
// that was not part of the write quorum).
func (s *ShardService) readQuorumMetaLocalDecoded(bucket, key string) (*storage.Object, PlacementMeta, error) {
	if len(s.dataDirs) == 0 {
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta read: key %q escapes root", key)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, PlacementMeta{}, storage.ErrObjectNotFound
		}
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta read: %w", err)
	}
	cmd, err := DecodeCommand(data)
	if err != nil {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta decode command: %w", err)
	}
	if cmd.Type != CmdPutObjectMeta {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta: unexpected command type %d", cmd.Type)
	}
	putCmd, err := decodePutObjectMetaCmd(cmd.Data)
	if err != nil {
		return nil, PlacementMeta{}, fmt.Errorf("quorum meta decode put cmd: %w", err)
	}
	m := buildPutObjectMeta(putCmd)
	obj := &storage.Object{
		Key:              m.Key,
		Size:             m.Size,
		ContentType:      m.ContentType,
		ETag:             m.ETag,
		LastModified:     m.LastModified,
		VersionID:        putCmd.VersionID,
		UserMetadata:     cloneStringMap(m.UserMetadata),
		SSEAlgorithm:     m.SSEAlgorithm,
		PlacementGroupID: m.PlacementGroupID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		Parts:            m.Parts,
		Tags:             append([]storage.Tag(nil), m.Tags...),
	}
	placement := PlacementMeta{
		VersionID:        putCmd.VersionID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		PlacementGroupID: m.PlacementGroupID,
	}
	return obj, placement, nil
}

// WriteQuorumMeta sends the quorum meta blob to a remote placement node via the
// shard transport (mirrors WriteShadowMeta but routes to the primary handler).
func (s *ShardService) WriteQuorumMeta(ctx context.Context, addr, bucket, key string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta: no transport")
	}
	fw := buildShardEnvelope("WriteQuorumMeta", bucket, key, 0, data)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, addr, fw)
	if err != nil {
		return fmt.Errorf("write quorum meta to %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta error from %s", addr)
	}
	return nil
}
