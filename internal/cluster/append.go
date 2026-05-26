package cluster

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// ErrStalePlacement signals the placement group changed between the
// coordinator's placement resolve and FSM apply (rebalance window). The
// coordinator (Task 21) performs transparent retry up to 2 times before
// returning 503 SlowDown to the client.
var ErrStalePlacement = errors.New("append: placement group changed mid-request")

const appendLockStripeCount = 256
const appendLockFNV32AOffset = 2166136261
const appendLockFNV32APrime = 16777619

func (b *DistributedBackend) appendAdmissionLock(bucket, key string) *sync.Mutex {
	h := uint32(appendLockFNV32AOffset)
	h = appendLockHashString(h, bucket)
	h *= appendLockFNV32APrime
	h = appendLockHashString(h, key)
	return &b.appendLocks[h&(appendLockStripeCount-1)]
}

func appendLockHashString(h uint32, s string) uint32 {
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= appendLockFNV32APrime
	}
	return h
}

// AppendObject implements storage.AppendObjecter for DistributedBackend.
// Owner-node entry point — ClusterCoordinator handles non-owner forwarding
// (Task 21).
//
// Phase A flow:
//  1. Cluster-aware pre-check via HeadObject (fast reject for offset / cap).
//  2. Write segment blob to owner-node disk.
//  3. Propose CmdAppendObject via data-Raft; b.propose surfaces apply errors
//     transparently (Phase A Tasks 14-16).
//  4. Re-HeadObject for fresh result reflecting committed segment list.
func (b *DistributedBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	lock := b.appendAdmissionLock(bucket, key)
	lock.Lock()
	defer lock.Unlock()

	// Step 1: cluster-aware pre-check.
	existing, err := b.HeadObject(ctx, bucket, key)
	if err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, err
	}

	// Size-cap fast-reject hint (design § Follow-up 2). Tolerance contract:
	// false-positive (reject what FSM would accept due to stale HeadObject)
	// is FORBIDDEN — only reject when we are conservative-safe. existing
	// here comes from HeadObject which can be stale OLDER than reality,
	// meaning existing.Size <= real.Size. existing.Size+chunkSize > cap is
	// therefore "real.Size+chunk > cap" — strict-greater on a lower bound
	// is conservative-safe (real total is at LEAST this large). OK to
	// reject.
	//
	// Body is not yet read; segment blob is not yet written; no orphan.
	sizeCapBytes := int64(0)
	if cfg := b.coalesceCfg.Load(); cfg != nil {
		sizeCapBytes = cfg.SizeCapBytes
	}
	if err := planAppendObjectAdmission(appendObjectAdmissionInput{
		Existing:       existing,
		ExpectedOffset: expectedOffset,
		ChunkSize:      appendChunkSize(r),
		SizeCapBytes:   sizeCapBytes,
	}); err != nil {
		return nil, err
	}
	if b.testBeforeAppendSegmentWrite != nil {
		b.testBeforeAppendSegmentWrite()
	}

	// Step 2: write segment blob to owner-node disk.
	seg, err := b.writeSegmentBlobForAppend(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment blob: %w", err)
	}

	// Step 3: resolve placement group at propose time (cmd captures PG so the
	// FSM can reject if it has moved since — see apply.go ErrStalePlacement).
	pgID := b.lookupPlacementGroupForAppend(ctx, existing)

	// Step 4: propose via data-Raft. b.propose returns FSM apply error
	// transparently (Phase A). AppendObject is rejected for versioning-enabled
	// buckets, so subsequent appends mutate the same latest version instead of
	// creating one metadata/index version per segment.
	versionID := ""
	if existing != nil {
		versionID = existing.VersionID
	}
	if versionID == "" {
		versionID = uuid.Must(uuid.NewV7()).String()
	}
	modifiedUnixSec := time.Now().Unix()
	cmd := buildAppendObjectCommand(appendObjectCommandInput{
		Bucket:           bucket,
		Key:              key,
		ExpectedOffset:   expectedOffset,
		Segment:          seg,
		PlacementGroupID: pgID,
		VersionID:        versionID,
		ModifiedUnixSec:  modifiedUnixSec,
	})
	if err := b.propose(ctx, CmdAppendObject, cmd); err != nil {
		// Best-effort cleanup of orphan segment blob on apply rejection
		// (full sweep deferred — see TODOS.md "Scrubber orphan sweep production wiring [P1]").
		_ = os.Remove(b.segmentBlobPath(bucket, key, seg.BlobID))
		return nil, err
	}

	obj := appendObjectResult(existing, key, versionID, seg, modifiedUnixSec)
	if obj != nil && obj.IsAppendable {
		b.maybeTriggerCoalesce(bucket, key, obj.Segments)
	}
	if obj != nil {
		metrics.AppendCoalescedDepth.Observe(float64(len(obj.Coalesced)))
		metrics.AppendCoalescedTotalBytes.Observe(float64(obj.Size))
	}
	return obj, nil
}

func appendObjectResult(existing *storage.Object, key, versionID string, seg storage.SegmentRef, modifiedUnixSec int64) *storage.Object {
	if existing == nil {
		return &storage.Object{
			Key:          key,
			Size:         seg.Size,
			ContentType:  "application/octet-stream",
			ETag:         storage.CompositeETag([][]byte{seg.Checksum}),
			LastModified: modifiedUnixSec,
			VersionID:    versionID,
			Segments:     []storage.SegmentRef{cloneSegmentRef(seg)},
			IsAppendable: true,
		}
	}

	obj := &storage.Object{
		Key:            existing.Key,
		Size:           existing.Size + seg.Size,
		ContentType:    existing.ContentType,
		LastModified:   modifiedUnixSec,
		VersionID:      versionID,
		ACL:            existing.ACL,
		UserMetadata:   cloneStringMap(existing.UserMetadata),
		SSEAlgorithm:   existing.SSEAlgorithm,
		Coalesced:      cloneStorageCoalescedRefs(existing.Coalesced),
		IsAppendable:   true,
		Parts:          cloneMultipartPartEntries(existing.Parts),
		Tags:           append([]storage.Tag(nil), existing.Tags...),
		AppendCallMD5s: nil,
	}
	if obj.Key == "" {
		obj.Key = key
	}
	if obj.ContentType == "" {
		obj.ContentType = "application/octet-stream"
	}
	if !existing.IsAppendable && len(existing.Segments) == 0 && len(existing.Coalesced) == 0 && existing.Size > 0 {
		coalescedID := "base"
		if versionID != "" {
			coalescedID = "base-" + versionID
		}
		obj.Coalesced = []storage.CoalescedRef{{
			CoalescedID: coalescedID,
			Size:        existing.Size,
			ETag:        existing.ETag,
		}}
	}
	obj.Segments = append(cloneSegmentRefs(existing.Segments), cloneSegmentRef(seg))
	callDigests := make([][]byte, 0, len(obj.Segments))
	for _, s := range obj.Segments {
		if len(s.Checksum) > 0 {
			callDigests = append(callDigests, s.Checksum)
		}
	}
	obj.ETag = storage.CompositeETag(callDigests)
	return obj
}

func cloneSegmentRefs(in []storage.SegmentRef) []storage.SegmentRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]storage.SegmentRef, len(in))
	for i := range in {
		out[i] = cloneSegmentRef(in[i])
	}
	return out
}

func cloneSegmentRef(in storage.SegmentRef) storage.SegmentRef {
	in.Checksum = append([]byte(nil), in.Checksum...)
	in.NodeIDs = append([]string(nil), in.NodeIDs...)
	return in
}

func cloneStorageCoalescedRefs(in []storage.CoalescedRef) []storage.CoalescedRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]storage.CoalescedRef, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].NodeIDs = append([]string(nil), in[i].NodeIDs...)
	}
	return out
}

// writeSegmentBlobForAppend writes one segment blob to owner-node disk under
// <root>/data/<bucket>/<key>_segments/<blobID>. Mirrors LocalBackend.WriteSegmentBlob
// but uses the cluster backend's own root and (optional) shard-service encryptor.
func (b *DistributedBackend) writeSegmentBlobForAppend(bucket, key string, r io.Reader) (storage.SegmentRef, error) {
	blobID := uuid.Must(uuid.NewV7()).String()
	path := b.segmentBlobPath(bucket, key, blobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return storage.SegmentRef{}, err
	}

	h := md5.New()
	f, err := os.Create(path)
	if err != nil {
		return storage.SegmentRef{}, err
	}
	tr := io.TeeReader(r, h)
	size, copyErr := io.Copy(f, tr)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return storage.SegmentRef{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return storage.SegmentRef{}, closeErr
	}

	// TODO(Phase 2): replace MD5 with xxhash3-128 to match storage-side
	// segment checksum. For now, the raw 16-byte MD5 digest is stashed in
	// Checksum so the cluster wire path (AppendObjectCmd.SegmentETag) can
	// still propagate the per-segment digest in hex form.
	return storage.SegmentRef{
		BlobID:   blobID,
		Size:     size,
		Checksum: h.Sum(nil),
	}, nil
}

// segmentBlobPath returns the on-disk path for one append-segment blob.
func (b *DistributedBackend) segmentBlobPath(bucket, key, blobID string) string {
	return filepath.Join(b.objectPath(bucket, key)+"_segments", blobID)
}

// openAppendableSegments returns a ReadCloser that stitches all of an
// appendable object's segment blobs into a single byte stream. Each blob is
// opened lazily as Read advances; matches LocalBackend.OpenSegmentedReader's
// behavior for the cluster path. Note: cluster-side segments are written
// unencrypted by writeSegmentBlobForAppend, so this path mirrors that.
//
// Phase B1 (forward-on-read): segment blobs only live on the owner-node's
// disk. When a non-owner serves the GET, local Open returns ENOENT; we
// then fetch the segment from a peer over StreamReadAppendSegment.
// Encryption note: writeSegmentBlobForAppend writes plaintext today; if
// at-rest encryption is later wired into that path, the peer-fetch path
// must reuse the same envelope (see append_segment_transport.go).
func (b *DistributedBackend) openAppendableSegments(bucket, key string, obj *storage.Object) io.ReadCloser {
	total := len(obj.Coalesced) + len(obj.Segments)
	paths := make([]string, 0, total)
	blobIDs := make([]string, 0, total)
	kinds := make([]byte, 0, total)
	ecRefs := make([]*storage.CoalescedRef, 0, total)
	// Coalesced blobs come first — they represent the older bytes of the object.
	for i := range obj.Coalesced {
		c := obj.Coalesced[i]
		paths = append(paths, b.coalescedBlobPath(bucket, key, c.CoalescedID))
		blobIDs = append(blobIDs, c.CoalescedID)
		kinds = append(kinds, appendSegKindCoalesced)
		ecRefs = append(ecRefs, &c)
	}
	for _, s := range obj.Segments {
		paths = append(paths, b.segmentBlobPath(bucket, key, s.BlobID))
		blobIDs = append(blobIDs, s.BlobID)
		kinds = append(kinds, appendSegKindSegment)
		ecRefs = append(ecRefs, nil)
	}
	return &appendableSegmentReader{
		backend: b,
		bucket:  bucket,
		key:     key,
		paths:   paths,
		blobIDs: blobIDs,
		kinds:   kinds,
		ecRefs:  ecRefs,
	}
}

// openCoalescedECReader opens an EC-reconstructed stream for one coalesced
// blob. Returns nil rc when the ref has no EC params (legacy/B2 owner-local
// entries) so the caller can fall back to local + forward-on-read.
func (b *DistributedBackend) openCoalescedECReader(ctx context.Context, bucket string, ref *storage.CoalescedRef) (io.ReadCloser, error) {
	if ref == nil || len(ref.NodeIDs) == 0 || ref.ECData == 0 {
		return nil, nil
	}
	rec := PlacementRecord{
		Nodes: append([]string(nil), ref.NodeIDs...),
		K:     int(ref.ECData),
		M:     int(ref.ECParity),
	}
	return b.newECObjectReader().OpenObject(ctx, bucket, ref.ShardKey, rec, ref.Size)
}

type appendableSegmentReader struct {
	backend *DistributedBackend
	bucket  string
	key     string
	paths   []string
	blobIDs []string
	kinds   []byte // appendSegKindSegment | appendSegKindCoalesced per entry
	// ecRefs[i] points to the storage.CoalescedRef when kinds[i] is
	// appendSegKindCoalesced, otherwise nil. Used to drive EC reconstruct
	// when the owner-local file is absent (B3 path).
	ecRefs []*storage.CoalescedRef
	idx    int
	cur    io.ReadCloser
}

func (r *appendableSegmentReader) Read(p []byte) (int, error) {
	for {
		if r.cur == nil {
			if r.idx >= len(r.paths) {
				return 0, io.EOF
			}
			rc, err := r.openCurrent()
			if err != nil {
				return 0, err
			}
			r.cur = rc
			r.idx++
		}
		n, err := r.cur.Read(p)
		if n > 0 {
			return n, nil
		}
		if errors.Is(err, io.EOF) {
			_ = r.cur.Close()
			r.cur = nil
			continue
		}
		return n, err
	}
}

// openCurrent opens the segment at r.idx.
//
//   - B3 coalesced entry with EC params: open via EC reader (peer shards are
//     the source of truth).
//   - Otherwise try the owner-local file first; on ENOENT fall back to
//     forward-on-read (Phase B1 + B2 owner-local coalesced).
func (r *appendableSegmentReader) openCurrent() (io.ReadCloser, error) {
	kind := byte(appendSegKindSegment)
	if r.idx < len(r.kinds) {
		kind = r.kinds[r.idx]
	}
	// B3 coalesced: prefer EC reconstruct when EC params are present. Falls
	// back to local/forward path when params are absent (legacy/B2 entry).
	if kind == appendSegKindCoalesced && r.backend != nil && r.idx < len(r.ecRefs) && r.ecRefs[r.idx] != nil {
		rc, err := r.backend.openCoalescedECReader(context.Background(), r.bucket, r.ecRefs[r.idx])
		if err == nil && rc != nil {
			return rc, nil
		}
		// Fall through to local/peer-fetch on nil rc (no EC params) or transient
		// EC reader error; the local/forward path is the legacy fallback.
	}

	path := r.paths[r.idx]
	f, err := os.Open(path)
	if err == nil {
		return f, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	if r.backend == nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	rc, ferr := r.backend.fetchAppendBlobFromAnyPeer(context.Background(), r.bucket, r.key, r.blobIDs[r.idx], kind)
	if ferr != nil {
		return nil, fmt.Errorf("open segment %s (local missing, peer fetch failed): %w", path, ferr)
	}
	return rc, nil
}

func (r *appendableSegmentReader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		r.cur = nil
		return err
	}
	return nil
}

// readAtAppendable serves a range read against an appendable object. It
// builds a prefix-sum index over Coalesced + Segments, binary-searches the
// starting chunk, and dispatches per-chunk partial reads:
//
//   - Coalesced with EC params → ecObjectReader.ReadAt (peer shards)
//   - Coalesced without EC params (B2/legacy) → owner-local file ReadAt with
//     forward-on-read fallback (whole-chunk fetch + slice)
//   - Raw segment → owner-local segment file ReadAt with forward-on-read
//     fallback
//
// This avoids the full GET + discard fallback when the caller only wants a
// small window into a large object.
func (b *DistributedBackend) readAtAppendable(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	// Build per-chunk plan: kind + size + ec-ref (for coalesced).
	type chunk struct {
		kind   byte
		size   int64
		blobID string
		path   string
		ec     *storage.CoalescedRef // non-nil for coalesced entry
	}
	chunks := make([]chunk, 0, len(obj.Coalesced)+len(obj.Segments))
	for i := range obj.Coalesced {
		c := obj.Coalesced[i]
		chunks = append(chunks, chunk{
			kind:   appendSegKindCoalesced,
			size:   c.Size,
			blobID: c.CoalescedID,
			path:   b.coalescedBlobPath(bucket, key, c.CoalescedID),
			ec:     &c,
		})
	}
	for _, s := range obj.Segments {
		chunks = append(chunks, chunk{
			kind:   appendSegKindSegment,
			size:   s.Size,
			blobID: s.BlobID,
			path:   b.segmentBlobPath(bucket, key, s.BlobID),
		})
	}

	// Locate the starting chunk via prefix sum.
	var cumOffset int64
	startIdx := -1
	var startInChunk int64
	for i, ch := range chunks {
		if offset < cumOffset+ch.size {
			startIdx = i
			startInChunk = offset - cumOffset
			break
		}
		cumOffset += ch.size
	}
	if startIdx < 0 {
		return 0, io.EOF
	}

	totalRead := 0
	for i := startIdx; i < len(chunks) && totalRead < len(buf); i++ {
		ch := chunks[i]
		var localOff int64
		if i == startIdx {
			localOff = startInChunk
		}
		remaining := ch.size - localOff
		want := int64(len(buf) - totalRead)
		if want > remaining {
			want = remaining
		}
		dst := buf[totalRead : totalRead+int(want)]
		n, err := b.readAtChunk(ctx, bucket, key, ch.kind, ch.blobID, ch.path, ch.ec, localOff, dst)
		totalRead += n
		if err != nil {
			if errors.Is(err, io.EOF) && totalRead == len(buf) {
				return totalRead, nil
			}
			return totalRead, err
		}
	}
	return totalRead, nil
}

// readAtChunk performs a single-chunk partial read with the appropriate
// backend (EC reader, owner-local file, or forward-on-read peer fetch).
func (b *DistributedBackend) readAtChunk(ctx context.Context, bucket, key string, kind byte, blobID, path string, ec *storage.CoalescedRef, offset int64, buf []byte) (int, error) {
	// B3 coalesced: prefer EC ReadAt when params are present.
	if kind == appendSegKindCoalesced && ec != nil && len(ec.NodeIDs) > 0 && ec.ECData > 0 {
		rec := PlacementRecord{
			Nodes: append([]string(nil), ec.NodeIDs...),
			K:     int(ec.ECData),
			M:     int(ec.ECParity),
		}
		n, err := b.newECObjectReader().ReadAt(ctx, bucket, ec.ShardKey, rec, ec.Size, offset, buf)
		if err == nil {
			return n, nil
		}
		// Fall through to local/forward on transient EC error.
	}
	if f, err := os.Open(path); err == nil {
		n, rerr := f.ReadAt(buf, offset)
		_ = f.Close()
		if rerr == nil || errors.Is(rerr, io.EOF) {
			return n, nil
		}
		return n, rerr
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("open chunk %s: %w", path, err)
	}
	// Forward-on-read fallback: pull the whole chunk and slice. The chunk
	// transport doesn't expose a range API today; coalesced range reads in
	// B3 normally hit the EC path above so this only fires for raw segments
	// served from a peer.
	rc, ferr := b.fetchAppendBlobFromAnyPeer(ctx, bucket, key, blobID, kind)
	if ferr != nil {
		return 0, fmt.Errorf("fetch chunk %s (local missing): %w", blobID, ferr)
	}
	defer rc.Close()
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			return 0, err
		}
	}
	return io.ReadFull(rc, buf)
}

// lookupPlacementGroupForAppend resolves the placement group ID to freeze into
// AppendObjectCmd. Order:
//  1. existing objectMeta's PG (anchors subsequent appends to the original PG).
//  2. PlacementGroupFromContext (coordinator-provided).
//  3. default "group-0" (single-node / test path).
func (b *DistributedBackend) lookupPlacementGroupForAppend(ctx context.Context, existing *storage.Object) string {
	// Phase A: storage.Object does not carry PlacementGroupID directly — the FSM
	// reads it from objectMeta at apply time. For the propose-time hint we fall
	// back to context / default; the FSM's stale-placement check still works
	// because applyAppendObjectFromCmd compares cmd.PlacementGroupID against
	// the freshly-read existing objectMeta.PlacementGroupID.
	if existing != nil {
		// existing was decoded from objectMeta; PG isn't exposed on storage.Object,
		// so fall through to context / default. (Task 21 coordinator threads PG
		// via context explicitly.)
		_ = existing
	}
	if pg, ok := PlacementGroupFromContext(ctx); ok {
		return pg
	}
	return "group-0"
}
