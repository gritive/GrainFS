package cluster

// manifest_blob.go — multipart manifest quorum-meta blob primitive.
//
// Each in-progress multipart upload has a clusterMultipartMeta manifest stored
// at:
//
//	dataDirs[0]/.qmeta_mpu/{bucket}/{uploadID}
//
// This is a SIBLING ROOT under dataDirs[0], completely separate from the
// per-object blobs in .quorum_meta and .quorum_meta_versions. The object-store
// walkers (ScanQuorumMetaBucket / ScanQuorumMetaVersionsBucket) never touch
// .qmeta_mpu, so manifests are invisible to them.
//
// Placement: the manifest is written to the uploadID's OWNING GROUP node set.
// The caller (Create, M2b) passes nodeIDs explicitly; this primitive only
// implements the storage layer.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/storage/directio"
)

// manifestMPUSubDir is the sibling-root sub-directory for multipart manifest blobs.
const manifestMPUSubDir = ".qmeta_mpu"

// manifestEntry is a decoded manifest blob plus its uploadID.
type manifestEntry struct {
	UploadID string
	Meta     clusterMultipartMeta
}

// --- ShardService local primitives ---

// writeManifestBlobLocal durably writes the encoded manifest blob for
// (bucket, uploadID) under {dataDirs[0]}/.qmeta_mpu/{bucket}/{uploadID}.
// Mirrors writeQuorumMetaLocal: atomic temp+fsync+rename, no LWW guard
// (manifests are append-only: create writes once, delete removes).
func (s *ShardService) writeManifestBlobLocal(bucket, uploadID string, data []byte) error {
	if len(s.dataDirs) == 0 {
		return fmt.Errorf("manifest blob write: no data dir")
	}
	root := filepath.Join(s.dataDirs[0], manifestMPUSubDir)
	target := filepath.Join(root, bucket, uploadID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("manifest blob write: uploadID %q escapes root", uploadID)
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("manifest blob mkdir: %w", err)
	}
	// Atomic publish: temp + fsync + rename (same pattern as writeQuorumMetaLocal).
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("manifest blob tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("manifest blob write: %w", err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("manifest blob fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("manifest blob tmp close: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("manifest blob rename: %w", err)
	}
	return nil
}

// readManifestBlobLocal reads the raw manifest blob for (bucket, uploadID) from
// the local filesystem. Returns (nil, false, nil) when the file is absent.
func (s *ShardService) readManifestBlobLocal(bucket, uploadID string) ([]byte, bool, error) {
	if len(s.dataDirs) == 0 {
		return nil, false, nil
	}
	root := filepath.Join(s.dataDirs[0], manifestMPUSubDir)
	target := filepath.Join(root, bucket, uploadID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, false, fmt.Errorf("manifest blob read: uploadID %q escapes root", uploadID)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("manifest blob read: %w", err)
	}
	return data, true, nil
}

// deleteManifestBlobLocal removes the local manifest blob for (bucket, uploadID).
// Absent file is not an error (idempotent).
func (s *ShardService) deleteManifestBlobLocal(bucket, uploadID string) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], manifestMPUSubDir)
	target := filepath.Join(root, bucket, uploadID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("manifest blob delete: uploadID %q escapes root", uploadID)
	}
	err = os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest blob delete: %w", err)
	}
	return nil
}

// scanManifestBlobsLocalStrict walks .qmeta_mpu/{bucket}/ and returns one
// manifestEntry per uploadID. Fail-closed: any read or decode error returns an
// error rather than silently dropping an entry (mirrors scanQuorumMetaBucketStrict).
func (s *ShardService) scanManifestBlobsLocalStrict(bucket string) ([]manifestEntry, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(s.dataDirs[0], manifestMPUSubDir)
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, nil
	}
	var out []manifestEntry
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr // fail-closed
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil // in-flight temp, skip
		}
		uploadID, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return rerr // fail-closed
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return fmt.Errorf("manifest blob scan: read %s/%s: %w", bucket, uploadID, rerr)
		}
		meta, derr := unmarshalClusterMultipartMeta(data)
		if derr != nil {
			return fmt.Errorf("manifest blob scan: decode %s/%s: %w", bucket, uploadID, derr)
		}
		out = append(out, manifestEntry{UploadID: uploadID, Meta: meta})
		return nil
	})
	return out, err
}

// --- RPC handlers (ShardService) ---

// handleManifestBlobWrite receives a WriteManifestBlob RPC and durably stores
// the manifest locally. sr.Key carries the uploadID.
func (s *ShardService) handleManifestBlobWrite(sr *shardRequest) []byte {
	if err := s.writeManifestBlobLocal(sr.Bucket, sr.Key, sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleManifestBlobRead serves a ReadManifestBlob RPC: reads the local manifest
// file and returns its raw bytes, or OK with empty payload when absent.
func (s *ShardService) handleManifestBlobRead(sr *shardRequest) []byte {
	data, ok, err := s.readManifestBlobLocal(sr.Bucket, sr.Key)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	if !ok {
		return s.okResponse(nil) // empty payload = not found on this node
	}
	return s.okResponse(data)
}

// handleManifestBlobDelete serves a DeleteManifestBlob RPC: removes the local
// manifest file. Absent file is not an error.
func (s *ShardService) handleManifestBlobDelete(sr *shardRequest) []byte {
	if err := s.deleteManifestBlobLocal(sr.Bucket, sr.Key); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleManifestBlobScan serves a ScanManifestBlobs RPC: walks .qmeta_mpu/{bucket}/
// on this node (fail-closed) and returns a packManifestEntries-encoded payload where
// each frame is [4-byte-uploadID-len][uploadID][4-byte-meta-len][meta-bytes]
// (see packManifestEntries / unpackManifestEntries).
func (s *ShardService) handleManifestBlobScan(sr *shardRequest) []byte {
	entries, err := s.scanManifestBlobsLocalStrict(sr.Bucket)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	packed, perr := packManifestEntries(entries)
	if perr != nil {
		return s.errorResponse(perr.Error())
	}
	return s.okResponse(packed)
}

// --- Peer RPC callers (ShardService) ---

// WriteManifestBlob sends a manifest blob to a remote node via the shard transport.
func (s *ShardService) WriteManifestBlob(ctx context.Context, addr, bucket, uploadID string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("manifest blob write: no transport")
	}
	envb := buildShardEnvelope("WriteManifestBlob", bucket, uploadID, 0, data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("write manifest blob to %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("manifest blob write: unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote manifest blob write error from %s: %s", addr, data)
	}
	return nil
}

// ReadManifestBlobRaw fetches the raw manifest blob from a remote node.
// Returns (nil, nil) when the remote node has no blob for this uploadID.
func (s *ShardService) ReadManifestBlobRaw(ctx context.Context, addr, bucket, uploadID string) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("manifest blob read: no transport")
	}
	envb := buildShardEnvelope("ReadManifestBlob", bucket, uploadID, 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("read manifest blob from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("manifest blob read: unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote manifest blob read error from %s: %s", addr, data)
	}
	return data, nil // nil data = not found on peer
}

// DeleteManifestBlob sends a DeleteManifestBlob RPC to a remote node.
func (s *ShardService) DeleteManifestBlob(ctx context.Context, addr, bucket, uploadID string) error {
	if s.transport == nil {
		return fmt.Errorf("manifest blob delete: no transport")
	}
	envb := buildShardEnvelope("DeleteManifestBlob", bucket, uploadID, 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return fmt.Errorf("delete manifest blob on %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("manifest blob delete: unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote manifest blob delete error from %s: %s", addr, data)
	}
	return nil
}

// ScanManifestBlobsRPC fetches all manifest entries from a remote node.
func (s *ShardService) ScanManifestBlobsRPC(ctx context.Context, addr, bucket string) ([]manifestEntry, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("manifest blob scan: no transport")
	}
	envb := buildShardEnvelope("ScanManifestBlobs", bucket, "", 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, addr, envb)
	if err != nil {
		return nil, fmt.Errorf("scan manifest blobs from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return nil, fmt.Errorf("manifest blob scan: unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote manifest blob scan error from %s: %s", addr, data)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return unpackManifestEntries(data)
}

// --- DistributedBackend methods ---

// dataDir0 returns the first data directory of the shard service (test/accessor helper).
// This is the root under which .qmeta_mpu, .quorum_meta, and .quorum_meta_versions live.
//
//nolint:unused // referenced by manifest_blob_test.go.
func (b *DistributedBackend) dataDir0() string {
	if b.shardSvc == nil || len(b.shardSvc.dataDirs) == 0 {
		return ""
	}
	return b.shardSvc.dataDirs[0]
}

// selfNodeIDs returns a slice containing only this node's address, for use as a
// single-node placement set in writeManifestBlob.
func (b *DistributedBackend) selfNodeIDs() []string {
	return []string{b.currentSelfAddr()}
}

// writeManifestBlob durably writes the manifest for a multipart upload to
// the owning group's node set, K-of-N fail-closed via fanOutQuorumMeta.
// nodeIDs must be the placement nodes of the owning group (the caller — Create,
// M2b — computes them from PickVoters / the group peer set).
func (b *DistributedBackend) writeManifestBlob(ctx context.Context, m clusterMultipartMeta, uploadID string, nodeIDs []string) error {
	if b.shardSvc == nil || len(nodeIDs) == 0 {
		return fmt.Errorf("manifest blob write: no shard service or empty placement")
	}
	data, err := marshalClusterMultipartMeta(m)
	if err != nil {
		return fmt.Errorf("manifest blob encode: %w", err)
	}
	self := b.currentSelfAddr()
	// K = 1 for single-node; for multi-node use majority (len(nodeIDs)/2+1).
	// Mirror writeQuorumMeta which uses ECData as K. For manifests we don't
	// have an ECData concept, so use a simple majority quorum.
	k := len(nodeIDs)/2 + 1
	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	return fanOutQuorumMeta(wctx, nodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeManifestBlobLocal(m.Bucket, uploadID, data)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteManifestBlob(fctx, addr, m.Bucket, uploadID, data)
	})
}

// readManifestBlob reads the manifest for (bucket, uploadID): local-first fast
// path, then peer fan-out on a local miss. Returns (zero, false, nil) when no
// node has the manifest.
func (b *DistributedBackend) readManifestBlob(bucket, uploadID string) (clusterMultipartMeta, bool, error) {
	if b.shardSvc == nil {
		return clusterMultipartMeta{}, false, nil
	}
	// Local-first fast path.
	localRaw, ok, err := b.shardSvc.readManifestBlobLocal(bucket, uploadID)
	if err != nil {
		return clusterMultipartMeta{}, false, err
	}
	if ok {
		meta, derr := unmarshalClusterMultipartMeta(localRaw)
		if derr != nil {
			return clusterMultipartMeta{}, false, fmt.Errorf("manifest blob decode: %w", derr)
		}
		return meta, true, nil
	}
	// Peer fan-out on miss.
	if b.shardGroup == nil {
		return clusterMultipartMeta{}, false, nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if seen[p] {
				continue
			}
			seen[p] = true
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				continue
			}
			raw, rerr := b.shardSvc.ReadManifestBlobRaw(ctx, addr, bucket, uploadID)
			if rerr != nil || len(raw) == 0 {
				continue
			}
			meta, derr := unmarshalClusterMultipartMeta(raw)
			if derr != nil {
				continue
			}
			return meta, true, nil
		}
	}
	return clusterMultipartMeta{}, false, nil
}

// deleteManifestBlob removes the manifest for (bucket, uploadID): idempotent
// local delete + best-effort peer deletes.
func (b *DistributedBackend) deleteManifestBlob(bucket, uploadID string) error {
	if b.shardSvc == nil {
		return nil
	}
	if err := b.shardSvc.deleteManifestBlobLocal(bucket, uploadID); err != nil {
		return err
	}
	if b.shardGroup == nil {
		return nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaWriteTimeout)
	defer cancel()
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if seen[p] {
				continue
			}
			seen[p] = true
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				continue // best-effort
			}
			_ = b.shardSvc.DeleteManifestBlob(ctx, addr, bucket, uploadID) // best-effort
		}
	}
	return nil
}

// scanManifestBlobsLocalStrict is the node-local fail-closed scan of
// .qmeta_mpu/{bucket}. Returns an error if any manifest cannot be read or decoded.
func (b *DistributedBackend) scanManifestBlobsLocalStrict(bucket string) ([]manifestEntry, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	return b.shardSvc.scanManifestBlobsLocalStrict(bucket)
}

// scanManifestBlobsCluster is the STRICT scatter-gather scan across self + every
// peer. Any peer address resolution error or RPC error aborts the scan (mirrors
// scanQuorumMetaVersionsClusterAll). When shardGroup is nil (single-node), returns
// the local STRICT scan.
func (b *DistributedBackend) scanManifestBlobsCluster(bucket string) ([]manifestEntry, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	// self (STRICT, fail-closed)
	local, lerr := b.shardSvc.scanManifestBlobsLocalStrict(bucket)
	if lerr != nil {
		return nil, lerr
	}
	byUploadID := make(map[string]manifestEntry, len(local))
	for _, e := range local {
		byUploadID[e.UploadID] = e
	}
	if b.shardGroup == nil {
		out := make([]manifestEntry, 0, len(byUploadID))
		for _, e := range byUploadID {
			out = append(out, e)
		}
		return out, nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if seen[p] {
				continue
			}
			seen[p] = true
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				return nil, aerr // fail-closed
			}
			remote, rerr := b.shardSvc.ScanManifestBlobsRPC(ctx, addr, bucket)
			if rerr != nil {
				return nil, rerr // fail-closed: partial set is data loss
			}
			for _, e := range remote {
				if _, exists := byUploadID[e.UploadID]; !exists {
					// First-writer-wins is correct by design: a manifest is
					// immutable from Create until Abort/Complete (no ModTime/
					// MetaSeq LWW field), so any replica is authoritative.
					byUploadID[e.UploadID] = e
				}
			}
		}
	}
	out := make([]manifestEntry, 0, len(byUploadID))
	for _, e := range byUploadID {
		out = append(out, e)
	}
	return out, nil
}

// --- Wire format helpers for scan RPC payload ---

// packManifestEntries serialises a []manifestEntry as a sequence of
// [4-byte-uploadID-len][uploadID-bytes][4-byte-meta-len][meta-bytes] frames.
// This is a simple length-prefixed wire format local to this file (no FB).
func packManifestEntries(entries []manifestEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	// Pre-encode all meta blobs.
	type frame struct {
		id   []byte
		meta []byte
	}
	frames := make([]frame, 0, len(entries))
	for _, e := range entries {
		meta, err := marshalClusterMultipartMeta(e.Meta)
		if err != nil {
			return nil, fmt.Errorf("pack manifest entries: encode %s: %w", e.UploadID, err)
		}
		frames = append(frames, frame{id: []byte(e.UploadID), meta: meta})
	}
	// Compute total size.
	total := 0
	for _, f := range frames {
		total += 4 + len(f.id) + 4 + len(f.meta)
	}
	buf := make([]byte, 0, total)
	put4 := func(n int) {
		buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	for _, f := range frames {
		put4(len(f.id))
		buf = append(buf, f.id...)
		put4(len(f.meta))
		buf = append(buf, f.meta...)
	}
	return buf, nil
}

// unpackManifestEntries deserialises the wire format written by packManifestEntries.
// Uses unsigned length decoding (binary.BigEndian.Uint32) to match unpackBlobList,
// so a crafted/corrupt length with bit-31 set never produces a negative slice index.
func unpackManifestEntries(data []byte) ([]manifestEntry, error) {
	var out []manifestEntry
	get4 := func() (uint32, error) {
		if len(data) < 4 {
			return 0, fmt.Errorf("unpack manifest entries: short read")
		}
		n := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		return n, nil
	}
	getBytes := func(n uint32) ([]byte, error) {
		if uint32(len(data)) < n {
			return nil, fmt.Errorf("unpack manifest entries: short read (%d < %d)", len(data), n)
		}
		b := data[:n]
		data = data[n:]
		return b, nil
	}
	for len(data) > 0 {
		idLen, err := get4()
		if err != nil {
			return nil, err
		}
		idBytes, err := getBytes(idLen)
		if err != nil {
			return nil, err
		}
		metaLen, err := get4()
		if err != nil {
			return nil, err
		}
		metaBytes, err := getBytes(metaLen)
		if err != nil {
			return nil, err
		}
		meta, derr := unmarshalClusterMultipartMeta(metaBytes)
		if derr != nil {
			return nil, fmt.Errorf("unpack manifest entries: decode: %w", derr)
		}
		out = append(out, manifestEntry{UploadID: string(idBytes), Meta: meta})
	}
	return out, nil
}
