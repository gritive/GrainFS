package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	quorumMetaAppendSubDir = ".quorum_meta_append"
	appendSideSummaryLeaf  = "summary"
	appendSideSegmentsDir  = "segments"
	appendSideSeqWidth     = 20
)

func appendSideSummarySubpath(key, versionID string) string {
	return path.Join(key, versionID, appendSideSummaryLeaf)
}

func appendSideSegmentSubpath(key, versionID string, seq int) string {
	return path.Join(key, versionID, appendSideSegmentsDir, fmt.Sprintf("%0*d", appendSideSeqWidth, seq))
}

func (m *LocalQuorumMetaStore) writeQuorumMetaAppendLocal(bucket, subpath string, data []byte) error {
	if len(m.dataDirs) == 0 {
		return fmt.Errorf("quorum meta append: no data dir")
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaAppendSubDir)
	target := filepath.Join(root, bucket, subpath)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta append: path %q escapes root", subpath)
	}
	unlock := m.quorumMetaTargetLock(target)
	defer unlock()
	return m.writeQuorumMetaFileAtomic(target, data, "quorum meta append", root)
}

func (m *LocalQuorumMetaStore) readQuorumMetaAppendRawLocal(bucket, subpath string) ([]byte, error) {
	if len(m.dataDirs) == 0 {
		return nil, storage.ErrObjectNotFound
	}
	root := filepath.Join(m.dataDirs[0], quorumMetaAppendSubDir)
	target := filepath.Join(root, bucket, subpath)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta append read: path %q escapes root", subpath)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("quorum meta append read: %w", err)
	}
	return data, nil
}

func (s *ShardService) WriteQuorumMetaAppend(ctx context.Context, addr, bucket, subpath string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("quorum meta append: no transport")
	}
	envb := buildShardEnvelope("WriteQuorumMetaAppend", bucket, subpath, 0, data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write quorum meta append to %s: %w", addr, err)
	}
	rpcType, body, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("unmarshal quorum meta append response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote quorum meta append write error from %s: %s", addr, string(body))
	}
	return nil
}

func (s *ShardService) ReadQuorumMetaAppendRaw(ctx context.Context, addr, bucket, subpath string) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta append read: no transport")
	}
	envb := buildShardEnvelope("ReadQuorumMetaAppend", bucket, subpath, 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("read quorum meta append from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("unmarshal quorum meta append read response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta append read error from %s", addr)
	}
	return data, nil
}

func (b *DistributedBackend) writeClusterAppendSideBlob(ctx context.Context, bucket, subpath string, data []byte, nodeIDs []string, k int) error {
	if b.shardSvc == nil || b.shardSvc.qmeta == nil {
		return fmt.Errorf("append side record: quorum-meta store unavailable")
	}
	if k <= 0 {
		k = 1
	}
	if len(nodeIDs) == 0 {
		nodeIDs = []string{b.currentSelfAddr()}
	}
	self := b.currentSelfAddr()
	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	return fanOutQuorumMeta(wctx, nodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.qmeta.writeQuorumMetaAppendLocal(bucket, subpath, data)
		}
		addr, err := b.shardSvc.resolvePeerAddress(node)
		if err != nil {
			return err
		}
		return b.shardSvc.WriteQuorumMetaAppend(fctx, addr, bucket, subpath, data)
	})
}

func (b *DistributedBackend) readClusterAppendSideRaw(ctx context.Context, bucket, subpath string, nodeIDs []string) ([]byte, error) {
	if b.shardSvc == nil || b.shardSvc.qmeta == nil {
		return nil, storage.ErrObjectNotFound
	}
	if data, err := b.shardSvc.qmeta.readQuorumMetaAppendRawLocal(bucket, subpath); err == nil {
		return data, nil
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, err
	}
	self := b.currentSelfAddr()
	ctx, cancel := context.WithTimeout(ctx, quorumMetaReadTimeout)
	defer cancel()
	for _, node := range nodeIDs {
		if node == "" || node == self {
			continue
		}
		addr, err := b.shardSvc.resolvePeerAddress(node)
		if err != nil {
			continue
		}
		data, err := b.shardSvc.ReadQuorumMetaAppendRaw(ctx, addr, bucket, subpath)
		if err == nil && len(data) > 0 {
			return data, nil
		}
	}
	return nil, storage.ErrObjectNotFound
}

func (b *DistributedBackend) readClusterAppendSummary(ctx context.Context, bucket, key, versionID string, nodeIDs []string) (storage.AppendSummary, error) {
	data, err := b.readClusterAppendSideRaw(ctx, bucket, appendSideSummarySubpath(key, versionID), nodeIDs)
	if err != nil {
		return storage.AppendSummary{}, err
	}
	return storage.DecodeAppendSummary(data)
}

func (b *DistributedBackend) writeClusterAppendSideRecords(ctx context.Context, bucket, key, versionID string, nodeIDs []string, k int, summary storage.AppendSummary, segments map[int]storage.SegmentRef) error {
	for seq, seg := range segments {
		if err := b.writeClusterAppendSideBlob(ctx, bucket, appendSideSegmentSubpath(key, versionID, seq), storage.EncodeAppendSegment(seg), nodeIDs, k); err != nil {
			return fmt.Errorf("append side segment %d: %w", seq, err)
		}
	}
	if err := b.writeClusterAppendSideBlob(ctx, bucket, appendSideSummarySubpath(key, versionID), storage.EncodeAppendSummary(summary), nodeIDs, k); err != nil {
		return fmt.Errorf("append side summary: %w", err)
	}
	return nil
}

func (b *DistributedBackend) hydrateClusterAppendSideSegments(ctx context.Context, bucket, key string, obj *storage.Object) error {
	if obj == nil || !obj.IsAppendable || obj.Size == 0 || len(obj.Segments) > 0 || len(obj.Coalesced) > 0 {
		return nil
	}
	summary, err := b.readClusterAppendSummary(ctx, bucket, key, obj.VersionID, obj.NodeIDs)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return fmt.Errorf("append side summary missing for %s/%s", bucket, key)
		}
		return err
	}
	if summary.Size != obj.Size {
		return fmt.Errorf("append side summary size %d does not match object size %d", summary.Size, obj.Size)
	}
	segments := make([]storage.SegmentRef, 0, summary.SegmentCount)
	var total int64
	for seq := 1; seq <= summary.SegmentCount; seq++ {
		data, err := b.readClusterAppendSideRaw(ctx, bucket, appendSideSegmentSubpath(key, obj.VersionID, seq), obj.NodeIDs)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotFound) {
				return fmt.Errorf("append side segment %d missing for %s/%s", seq, bucket, key)
			}
			return err
		}
		seg, err := storage.DecodeAppendSegment(data)
		if err != nil {
			return err
		}
		total += seg.Size
		segments = append(segments, seg)
	}
	if total != obj.Size {
		return fmt.Errorf("append side segment size %d does not match object size %d", total, obj.Size)
	}
	obj.Segments = segments
	return nil
}
