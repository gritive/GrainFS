package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

// quorumMetaReadTimeout bounds the peer fan-out read in fetchQuorumMetaFromPeers.
// Shorter than the write timeout: reads are latency-sensitive (GET path).
const quorumMetaReadTimeout = 5 * time.Second

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
	// K-of-N write quorum: ECData nodes must ack. Parity nodes are best-effort.
	// Any node that misses the write can still read via fetchQuorumMetaFromPeers.
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

// readQuorumMeta reads object metadata from the local quorum store, falling
// back to a peer fan-out when the local file is absent (e.g. parity node that
// missed the K-of-N write). Returns ErrObjectNotFound only when no peer has
// the file; callers then fall through to BadgerDB for pre-Phase-3 objects.
func (b *DistributedBackend) readQuorumMeta(bucket, key string) (*storage.Object, PlacementMeta, error) {
	if b.shardSvc == nil {
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	raw, err := b.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return b.shardSvc.decodeQuorumMetaBlob(raw)
}

// quorumMetaBlobWins reports whether candidate (modA, verA) beats (modB, verB)
// in the quorum-meta last-writer-wins comparison: higher ModTime wins; on an
// equal ModTime (second granularity — see time.Now().Unix() writers) the
// lexicographically greater VersionID wins. The VersionID tiebreak gives the
// point-GET merge, the peer fan-out, and scatter-gather LIST a single
// deterministic ordering so they agree on the winner of a same-second tie. It is
// NOT a recency guarantee at second granularity — only deterministic agreement.
func quorumMetaBlobWins(modA int64, verA string, modB int64, verB string) bool {
	if modA != modB {
		return modA > modB
	}
	return verA > verB
}

// readQuorumMetaWinningRaw returns the raw quorum-meta blob that wins the LWW
// comparison for (bucket, key). It is the shared read funnel for both
// readQuorumMeta (decodes to storage.Object) and readQuorumMetaCmd (decodes to
// PutObjectMetaCmd).
//
// Single-generation default (multiGeneration false): local-first fast path —
// return the local blob on hit, fan out to peers only on a local miss. This is
// byte-identical to the legacy readQuorumMeta/readQuorumMetaCmd behavior.
//
// Multi-generation (S7-6): a routed (newest-generation) leader holding a stale
// local copy must not shadow a fresher copy that an add-window split-brain write
// landed in an older generation. So merge the local blob AND the cross-generation
// peer fan-out (fetchQuorumMetaFromPeers already spans every ShardGroups() peer),
// returning the LWW winner. fetchQuorumMetaFromPeers excludes self, so the local
// blob is merged in separately here rather than dropped.
func (b *DistributedBackend) readQuorumMetaWinningRaw(bucket, key string) ([]byte, error) {
	localRaw, localErr := b.shardSvc.readQuorumMetaRaw(bucket, key)
	if localErr != nil && !errors.Is(localErr, storage.ErrObjectNotFound) {
		return nil, localErr
	}

	if !b.multiGeneration.Load() {
		// Local-first fast path (byte-identical to legacy).
		if localErr == nil {
			return localRaw, nil
		}
		raw, ok := b.fetchQuorumMetaFromPeers(bucket, key)
		if !ok {
			return nil, storage.ErrObjectNotFound
		}
		return raw, nil
	}

	// Multi-generation cross-generation LWW merge: combine the local blob with
	// the peer-best blob and pick the winner.
	peerRaw, peerOK := b.fetchQuorumMetaFromPeers(bucket, key)
	switch {
	case localErr == nil && peerOK:
		return b.pickQuorumMetaWinner(localRaw, peerRaw), nil
	case localErr == nil:
		return localRaw, nil
	case peerOK:
		return peerRaw, nil
	default:
		return nil, storage.ErrObjectNotFound
	}
}

// pickQuorumMetaWinner returns whichever of two raw quorum-meta blobs wins the
// LWW comparison (quorumMetaBlobWins). A blob that fails to decode loses to a
// decodable one; if both fail it returns a (the caller already holds both as
// candidate winners, so returning either preserves liveness).
func (b *DistributedBackend) pickQuorumMetaWinner(a, bRaw []byte) []byte {
	cmdA, errA := b.shardSvc.decodeQuorumMetaCmdBlob(a)
	cmdB, errB := b.shardSvc.decodeQuorumMetaCmdBlob(bRaw)
	switch {
	case errA != nil && errB != nil:
		return a
	case errA != nil:
		return bRaw
	case errB != nil:
		return a
	}
	if quorumMetaBlobWins(cmdB.ModTime, cmdB.VersionID, cmdA.ModTime, cmdA.VersionID) {
		return bRaw
	}
	return a
}

// fetchQuorumMetaFromPeers fans out ReadQuorumMeta RPCs to all shard group
// peers concurrently and returns the blob with the highest ModTime (LWW).
// Waits for all peers within quorumMetaReadTimeout so that a concurrent PUT
// race resolves to the latest write rather than the fastest responder.
// Returns (nil, false) when no peer has the file or b.shardGroup is nil.
func (b *DistributedBackend) fetchQuorumMetaFromPeers(bucket, key string) ([]byte, bool) {
	if b.shardSvc == nil || b.shardGroup == nil {
		return nil, false
	}
	// Collect unique peer addresses from all shard groups, excluding self.
	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	var peers []string
	for _, g := range b.shardGroup.ShardGroups() {
		for _, p := range g.PeerIDs {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
	}
	if len(peers) == 0 {
		return nil, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()

	type peerResult struct {
		data      []byte
		modTime   int64
		versionID string
	}
	ch := make(chan peerResult, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := b.shardSvc.resolvePeerAddress(p)
			if err != nil {
				return
			}
			data, err := b.shardSvc.ReadQuorumMetaRaw(ctx, addr, bucket, key)
			if err != nil || len(data) == 0 {
				return
			}
			// Decode ModTime+VersionID for LWW: pick the blob written latest,
			// breaking same-second ties deterministically by VersionID.
			var (
				modTime   int64
				versionID string
			)
			if cmd, decErr := b.shardSvc.decodeQuorumMetaCmdBlob(data); decErr == nil {
				modTime = cmd.ModTime
				versionID = cmd.VersionID
			}
			ch <- peerResult{data: data, modTime: modTime, versionID: versionID}
		}()
	}
	go func() { wg.Wait(); close(ch) }()

	// Collect all peer responses; return the LWW winner (highest ModTime, then
	// highest VersionID on a tie). hasBest guards the zero-ModTime case: two
	// blobs with ModTime=0 must still resolve to a deterministic winner.
	var best peerResult
	hasBest := false
	for r := range ch {
		if !hasBest || quorumMetaBlobWins(r.modTime, r.versionID, best.modTime, best.versionID) {
			best = r
			hasBest = true
		}
	}
	return best.data, hasBest
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
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("quorum meta mkdir: %w", err)
	}
	// Atomic publish: write to a unique temp file in the same directory, fsync,
	// then rename over the target. An in-place O_TRUNC write exposes a window where
	// the file is 0 bytes (truncated, data not yet written); a concurrent reader of
	// the same key — a same-key overwrite racing a GET/HEAD, or a lingering
	// best-effort quorum write still in flight after fanOutQuorumMeta returned on
	// the k-th ack — would read that empty file and report the object as missing.
	// rename is atomic on a POSIX filesystem, so a reader sees either the previous
	// complete blob or the new one, never a torn one.
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("quorum meta tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("quorum meta fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("quorum meta tmp close: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("quorum meta rename: %w", err)
	}
	return nil
}

// readQuorumMetaRaw reads the raw quorum meta blob for (bucket, key) from the
// local filesystem. Returns (nil, ErrObjectNotFound) when the file is absent.
func (s *ShardService) readQuorumMetaRaw(bucket, key string) ([]byte, error) {
	if len(s.dataDirs) == 0 {
		return nil, storage.ErrObjectNotFound
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("quorum meta read raw: key %q escapes root", key)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("quorum meta read raw: %w", err)
	}
	return data, nil
}

// decodeQuorumMetaBlob decodes a raw quorum meta blob into storage.Object and
// PlacementMeta. Used by both the local-read and peer-fallback paths.
func (s *ShardService) decodeQuorumMetaBlob(data []byte) (*storage.Object, PlacementMeta, error) {
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
		ACL:              m.ACL,
		UserMetadata:     cloneStringMap(m.UserMetadata),
		SSEAlgorithm:     m.SSEAlgorithm,
		PlacementGroupID: m.PlacementGroupID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		Segments:         append([]storage.SegmentRef(nil), m.Segments...),
		Coalesced:        coalescedRefsToStorage(m.Coalesced),
		IsAppendable:     m.IsAppendable,
		Parts:            m.Parts,
		Tags:             append([]storage.Tag(nil), m.Tags...),
		// S4-4c: carry the delete-marker flag so versioned reads
		// (GetObjectVersion/HeadObjectVersion) fold a quorum-meta delete marker
		// to 405 MethodNotAllowed instead of trying to read its (absent) body.
		IsDeleteMarker: putCmd.IsDeleteMarker,
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

// readQuorumMetaLocalDecoded reads and decodes the quorum meta blob for
// (bucket, key) from the local store. Returns storage.ErrObjectNotFound if the
// file does not exist.
func (s *ShardService) readQuorumMetaLocalDecoded(bucket, key string) (*storage.Object, PlacementMeta, error) {
	data, err := s.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return s.decodeQuorumMetaBlob(data)
}

// ScanQuorumMetaBucket returns all PutObjectMetaCmd entries (including
// IsDeleteMarker tombstones) stored locally for bucket. prefix is an
// optional key-prefix filter (empty string = return all). Unreadable entries
// are silently skipped. Callers decide whether to filter tombstones.
func (s *ShardService) ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if len(s.dataDirs) == 0 {
		return nil, nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, nil
	}
	var results []PutObjectMetaCmd
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		key, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return nil
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		cmd, qerr := s.readQuorumMetaRawCmd(bucket, key)
		if qerr != nil {
			return nil
		}
		results = append(results, cmd)
		return nil
	})
	return results, err
}

// deleteQuorumMetaLocal removes the local quorum meta file for (bucket, key).
// Called by deleteObjectWithMarker after the raft CmdDeleteObject commit so
// subsequent reads fall through to BadgerDB and find the delete marker.
// Errors are silently ignored: the raft marker is the source of truth; a
// stale quorum meta file is handled on the next read.
func (s *ShardService) deleteQuorumMetaLocal(bucket, key string) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quorum meta delete: key %q escapes root", key)
	}
	err = os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("quorum meta delete: %w", err)
	}
	return nil
}

// decodeQuorumMetaCmdBlob decodes a raw quorum meta blob to a PutObjectMetaCmd.
func (s *ShardService) decodeQuorumMetaCmdBlob(data []byte) (PutObjectMetaCmd, error) {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("quorum meta decode raw: %w", err)
	}
	if cmd.Type != CmdPutObjectMeta {
		return PutObjectMetaCmd{}, fmt.Errorf("quorum meta read raw: unexpected command type %d", cmd.Type)
	}
	return decodePutObjectMetaCmd(cmd.Data)
}

// readQuorumMetaRawCmd reads and decodes the PutObjectMetaCmd from the local
// quorum meta store. Returns storage.ErrObjectNotFound if the file is absent.
// Use DistributedBackend.readQuorumMetaCmd when peer fallback is needed.
func (s *ShardService) readQuorumMetaRawCmd(bucket, key string) (PutObjectMetaCmd, error) {
	data, err := s.readQuorumMetaRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return s.decodeQuorumMetaCmdBlob(data)
}

// ReadQuorumMetaRaw fetches the raw quorum meta blob from a remote peer via
// the shard transport. Returns (nil, nil) when the peer has no file for the
// object (not-found is not an error — caller treats nil data as a miss).
func (s *ShardService) ReadQuorumMetaRaw(ctx context.Context, addr, bucket, key string) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("quorum meta read: no transport")
	}
	fw := buildShardEnvelope("ReadQuorumMeta", bucket, key, 0, nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, addr, fw)
	if err != nil {
		return nil, fmt.Errorf("read quorum meta from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal quorum meta read response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote quorum meta read error from %s", addr)
	}
	return data, nil
}

// readQuorumMetaCmd is the DistributedBackend-level read for PutObjectMetaCmd,
// with peer fan-out fallback when the local quorum meta file is absent.
// Used by SetObjectACLPropose, SetObjectTagsPropose, and AppendObject migration.
func (b *DistributedBackend) readQuorumMetaCmd(bucket, key string) (PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return PutObjectMetaCmd{}, storage.ErrObjectNotFound
	}
	raw, err := b.readQuorumMetaWinningRaw(bucket, key)
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return b.shardSvc.decodeQuorumMetaCmdBlob(raw)
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

// IterQuorumMetaECShardTargets walks all quorum meta files under
// {dataDirs[0]}/.quorum_meta/ and emits ECShardScanTarget entries for every
// object whose segments or coalesced refs carry EC placement. Used by
// ShardPlacementMonitor.Scan to cover Phase 3 objects that bypass BadgerDB.
//
// fn returning a non-nil error stops iteration.
func (s *ShardService) IterQuorumMetaECShardTargets(fn func(ECShardScanTarget) error) error {
	if len(s.dataDirs) == 0 {
		return nil
	}
	root := filepath.Join(s.dataDirs[0], quorumMetaSubDir)
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	// Walk: root/{bucket}/{key} — exactly two levels deep.
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		if len(parts) != 2 {
			return nil // not bucket/key shape
		}
		bucket, key := parts[0], parts[1]
		obj, _, qerr := s.readQuorumMetaLocalDecoded(bucket, key)
		if qerr != nil {
			return nil // corrupt or not found; skip
		}
		// Build ECShardScanTarget entries mirroring buildECShardTargets in
		// shard_placement.go, but sourced from the quorum meta store.
		for i := range obj.Segments {
			seg := obj.Segments[i]
			if seg.ECData == 0 || len(seg.NodeIDs) == 0 {
				continue
			}
			if !validateECRefPlacement(seg.ECData, seg.ECParity, seg.NodeIDs) {
				continue
			}
			if ferr := fn(ECShardScanTarget{
				Kind:      ECShardSegment,
				Bucket:    bucket,
				ObjectKey: key,
				VersionID: obj.VersionID,
				ShardKey:  key + "/segments/" + seg.BlobID,
				Placement: PlacementRecord{
					Nodes:       seg.NodeIDs,
					K:           int(seg.ECData),
					M:           int(seg.ECParity),
					StripeBytes: int(seg.StripeBytes),
				},
			}); ferr != nil {
				return ferr
			}
		}
		for i := range obj.Coalesced {
			cs := obj.Coalesced[i]
			if cs.ECData == 0 || len(cs.NodeIDs) == 0 {
				continue
			}
			if !validateECRefPlacement(cs.ECData, cs.ECParity, cs.NodeIDs) {
				continue
			}
			if ferr := fn(ECShardScanTarget{
				Kind:      ECShardCoalesced,
				Bucket:    bucket,
				ObjectKey: key,
				VersionID: obj.VersionID,
				ShardKey:  cs.ShardKey,
				Placement: PlacementRecord{
					Nodes:       cs.NodeIDs,
					K:           int(cs.ECData),
					M:           int(cs.ECParity),
					StripeBytes: int(cs.StripeBytes),
				},
			}); ferr != nil {
				return ferr
			}
		}
		// Non-segmented / non-coalesced EC objects (single-blob phase-3 objects).
		if len(obj.Segments) == 0 && len(obj.Coalesced) == 0 && obj.ECData > 0 && len(obj.NodeIDs) > 0 {
			if ferr := fn(ECShardScanTarget{
				Kind:             ECShardObjectVersion,
				Bucket:           bucket,
				ObjectKey:        key,
				VersionID:        obj.VersionID,
				ECData:           obj.ECData,
				ECParity:         obj.ECParity,
				NodeIDs:          obj.NodeIDs,
				PlacementGroupID: obj.PlacementGroupID,
			}); ferr != nil {
				return ferr
			}
		}
		return nil
	})
}

// packBlobList encodes a slice of blobs as a concatenated length-prefixed stream.
// Each blob is preceded by a 4-byte big-endian length. Used for ScanQuorumMeta RPC.
func packBlobList(blobs [][]byte) []byte {
	var total int
	for _, b := range blobs {
		total += 4 + len(b)
	}
	out := make([]byte, 0, total)
	for _, b := range blobs {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
		out = append(out, hdr[:]...)
		out = append(out, b...)
	}
	return out
}

// unpackBlobList decodes a packBlobList-encoded byte slice.
func unpackBlobList(data []byte) ([][]byte, error) {
	var out [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("unpack blob list: truncated length prefix")
		}
		n := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < n {
			return nil, fmt.Errorf("unpack blob list: truncated blob (%d bytes, want %d)", len(data), n)
		}
		out = append(out, data[:n])
		data = data[n:]
	}
	return out, nil
}

// ScanQuorumMeta fans out a ScanQuorumMeta RPC to a remote node and returns
// all PutObjectMetaCmds (including tombstones) for the given bucket and prefix.
func (s *ShardService) ScanQuorumMeta(ctx context.Context, addr, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("scan quorum meta: no transport")
	}
	fw := buildShardEnvelope("ScanQuorumMeta", bucket, prefix, 0, nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, addr, fw)
	if err != nil {
		return nil, fmt.Errorf("scan quorum meta from %s: %w", addr, err)
	}
	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal scan quorum meta response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote scan quorum meta error from %s", addr)
	}
	if len(data) == 0 {
		return nil, nil
	}
	blobs, err := unpackBlobList(data)
	if err != nil {
		return nil, fmt.Errorf("unpack scan quorum meta response: %w", err)
	}
	var cmds []PutObjectMetaCmd
	for _, blob := range blobs {
		cmd, qerr := s.decodeQuorumMetaCmdBlob(blob)
		if qerr == nil {
			cmds = append(cmds, cmd)
		}
	}
	return cmds, nil
}

// scatterGatherList fans out ScanQuorumMeta calls to all shard group peers
// (including self), applies per-key LWW (max ModTime wins), filters
// IsDeleteMarker tombstones, and returns the surviving entries sorted by key.
// ScanObjectMetaEntries scatter-gathers the live (tombstone-filtered) object
// metadata for bucket under prefix and returns each as an ObjectIndexEntry
// carrying the EC placement fields (PlacementGroupID, NodeIDs, ECData, ECParity)
// that ClassifyObjectLayout needs. S4-4d uses it to rebuild admin volume replica
// facts from quorum meta now that the object index is gone.
func (b *DistributedBackend) ScanObjectMetaEntries(ctx context.Context, bucket, prefix string) ([]ObjectIndexEntry, error) {
	cmds, err := b.scatterGatherList(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	entries := make([]ObjectIndexEntry, 0, len(cmds))
	for _, cmd := range cmds {
		entries = append(entries, ObjectIndexEntry{
			Bucket:           cmd.Bucket,
			Key:              cmd.Key,
			VersionID:        cmd.VersionID,
			PlacementGroupID: cmd.PlacementGroupID,
			Size:             cmd.Size,
			ContentType:      cmd.ContentType,
			ETag:             cmd.ETag,
			ModTime:          cmd.ModTime,
			ECData:           cmd.ECData,
			ECParity:         cmd.ECParity,
			NodeIDs:          cmd.NodeIDs,
			IsDeleteMarker:   cmd.IsDeleteMarker,
		})
	}
	return entries, nil
}

func (b *DistributedBackend) scatterGatherList(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, nil
	}
	self := b.currentSelfAddr()
	seen := map[string]bool{}
	var peerIDs []string
	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if !seen[p] {
					seen[p] = true
					peerIDs = append(peerIDs, p)
				}
			}
		}
	}
	if !seen[self] {
		peerIDs = append(peerIDs, self)
	}
	if len(peerIDs) == 0 {
		entries, err := b.shardSvc.ScanQuorumMetaBucket(bucket, prefix)
		if err != nil {
			return nil, err
		}
		return filterAndSortEntries(entries), nil
	}

	type nodeResult struct {
		entries []PutObjectMetaCmd
	}
	rctx, cancel := context.WithTimeout(ctx, quorumMetaReadTimeout)
	defer cancel()
	ch := make(chan nodeResult, len(peerIDs))
	for _, p := range peerIDs {
		p := p
		go func() {
			if p == self {
				entries, _ := b.shardSvc.ScanQuorumMetaBucket(bucket, prefix)
				ch <- nodeResult{entries: entries}
				return
			}
			addr, aerr := b.shardSvc.resolvePeerAddress(p)
			if aerr != nil {
				ch <- nodeResult{}
				return
			}
			entries, _ := b.shardSvc.ScanQuorumMeta(rctx, addr, bucket, prefix)
			ch <- nodeResult{entries: entries}
		}()
	}

	lww := map[string]PutObjectMetaCmd{}
	for range peerIDs {
		r := <-ch
		for _, e := range r.entries {
			if cur, ok := lww[e.Key]; !ok || quorumMetaBlobWins(e.ModTime, e.VersionID, cur.ModTime, cur.VersionID) {
				lww[e.Key] = e
			}
		}
	}

	all := make([]PutObjectMetaCmd, 0, len(lww))
	for _, e := range lww {
		all = append(all, e)
	}
	return filterAndSortEntries(all), nil
}

// filterAndSortEntries removes tombstones and sorts by key.
func filterAndSortEntries(entries []PutObjectMetaCmd) []PutObjectMetaCmd {
	out := entries[:0]
	for _, e := range entries {
		if !e.IsDeleteMarker {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
