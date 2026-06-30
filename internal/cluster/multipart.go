package cluster

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/uuidutil"
)

func (b *DistributedBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	uploadID, createdAt, err := b.createMultipartUploadInternal(ctx, bucket, key, contentType, nil)
	if err != nil {
		return nil, err
	}
	return &storage.MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   createdAt,
	}, nil
}

// CreateMultipartUploadWithTags creates a multipart upload with tags in cluster mode.
// Tags are stored in the clusterMultipartMeta manifest blob (written to
// .qmeta_mpu/{bucket}/{uploadID}) and carried through to the finalised object's
// per-version quorum-meta blob at CompleteMultipartUpload (no Raft proposal;
// blob path only).
func (b *DistributedBackend) CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	uploadID, _, err := b.createMultipartUploadInternal(ctx, bucket, key, contentType, tags)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

// createMultipartUploadInternal is the shared body for CreateMultipartUpload and
// CreateMultipartUploadWithTags. If tags is non-empty it is defensively copied at
// this cluster API boundary so the propose path can safely retain it (propose is
// async); downstream code must not introduce further copies.
func (b *DistributedBackend) createMultipartUploadInternal(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, int64, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return "", 0, err
	}
	unlockBucketWrite, err := b.enterBucketObjectWrite(ctx, bucket)
	if err != nil {
		return "", 0, err
	}
	defer unlockBucketWrite()

	// Mint a UUIDv7 uploadID: deriveMultipartVID reuses its 48-bit ms timestamp
	// so the completed object's deterministic VersionID stays create-time ordered.
	uploadID := uuidutil.MustNewV7()
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return "", 0, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	// GroupBackend (bypassBucketCheck=true) always injects a placement-group ID via
	// context; missing one there is a programming error. Direct DistributedBackend
	// callers (bypassBucketCheck=false) may omit it — putObjectECSpooled resolves
	// placement from the stored empty string using the object's bucket assignment.
	if !ok && b.shardSvc != nil && b.bypassBucketCheck {
		return "", 0, fmt.Errorf("create multipart: missing placement_group_id")
	}

	var tagsCopy []storage.Tag
	if len(tags) > 0 {
		tagsCopy = make([]storage.Tag, len(tags))
		copy(tagsCopy, tags)
	}

	// Off-FSM manifest (M2b hinge): the in-progress upload's manifest lives on the
	// .qmeta_mpu blob — written FAIL-CLOSED to the owning group's node set — not the
	// FSM mpu: key. UploadPart/ListParts/Complete/Abort and the lifecycle abort scan
	// all derive the session from this blob; there is no CmdCreateMultipartUpload
	// propose. The node set is the same one routing resolves for the owning group:
	// the placement-group PeerIDs carried in context (GroupBackend / forward path),
	// falling back to this node alone for the single-node / direct-backend wiring.
	manifest := clusterMultipartMeta{
		Bucket:           bucket,
		Key:              key,
		CreatedAt:        now,
		ContentType:      contentType,
		PlacementGroupID: placementGroupID,
		Tags:             tagsCopy,
	}
	if err := b.writeManifestBlob(ctx, manifest, uploadID, b.multipartManifestNodeIDs(ctx)); err != nil {
		os.RemoveAll(b.partDir(uploadID))
		_ = b.deleteManifestBlob(manifest.Bucket, uploadID) // best-effort cleanup of any partial replicas
		return "", 0, fmt.Errorf("create multipart: write manifest blob: %w", err)
	}
	return uploadID, now, nil
}

// multipartManifestNodeIDs resolves the owning group's node set for the manifest
// blob write — the same node set routing uses for the upload's group. The
// placement-group entry carried in context (injected by GroupBackend on the
// owning-group node, both local-exec and forwarded paths) holds the resolved
// voter set; on the single-node / direct-backend wiring (no context placement)
// the manifest is written to this node alone.
func (b *DistributedBackend) multipartManifestNodeIDs(ctx context.Context) []string {
	if group, ok := PlacementGroupEntryFromContext(ctx); ok && len(group.PeerIDs) > 0 {
		return group.PeerIDs
	}
	return b.selfNodeIDs()
}

func (b *DistributedBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader, contentMD5Hex string) (*storage.Part, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.RLock()
	defer lifeMu.RUnlock()

	// Verify the upload exists by reading its off-FSM manifest blob (local-first,
	// peer fan-out on a local miss).
	if _, ok, err := b.readManifestBlob(bucket, uploadID); err != nil {
		return nil, err
	} else if !ok {
		return nil, storage.ErrUploadNotFound
	}

	// Data write is local — no Raft needed for part data
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}
	partFile := b.partPath(uploadID, partNumber)
	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h := md5Pool.Get().(hash.Hash)
	h.Reset()
	defer md5Pool.Put(h)
	var partWriter io.Writer = f
	if b.encryptedShardStorage() {
		partWriter = &encryptedSpoolRecordWriter{
			w:      f,
			seam:   b.shardSvc.segEnc(),
			domain: clusterMultipartPartDomain(uploadID, partNumber),
		}
	}
	w := io.MultiWriter(partWriter, h)
	size, err := copyToSpoolChunked(w, r)
	f.Close()
	if err != nil {
		os.Remove(partFile)
		return nil, fmt.Errorf("write part: %w", err)
	}

	etag := hex.EncodeToString(h.Sum(nil))
	if contentMD5Hex != "" && etag != contentMD5Hex {
		os.Remove(partFile) // delete the staged part so it cannot be completed
		return nil, fmt.Errorf("%w: client %s, part %s", storage.ErrContentMD5Mismatch, contentMD5Hex, etag)
	}

	return &storage.Part{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       size,
	}, nil
}

// buildMultipartMetaBlob encodes the completed multipart object's PutObjectMetaCmd
// — the per-version blob authority — for a versioning-enabled, non-internal bucket,
// returning nil for non-versioned/Suspended/internal buckets (which use the
// latest-only quorum-meta blob path). Built on the PROPOSER because CompleteMultipartCmd
// does not carry UserMetadata/ACL/SSE.
func (b *DistributedBackend) buildMultipartMetaBlob(ctx context.Context, cmd PutObjectMetaCmd) ([]byte, error) {
	if cmd.VersionID == "" || !b.bucketVersioningEnabled(ctx, cmd.Bucket) {
		return nil, nil
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	if err != nil {
		return nil, fmt.Errorf("multipart complete: encode meta_blob: %w", err)
	}
	return blob, nil
}

// writeCompletedMultipartBlob decodes the completed multipart object's encoded
// PutObjectMetaCmd and durably writes its per-version quorum-meta blob, FAIL-CLOSED.
// The per-version blob is the blob authority for the completed versioned object
// (reads/LIST/GC/DEK all derive from it). Idempotent: a dup write of the same
// vid+bytes no-ops via the LWW guard, so the blob lands regardless of which complete
// attempt (concurrent completer or idempotent retry) writes it. Returns the decoded
// object so callers can answer the S3 CompleteMultipartUpload response.
func (b *DistributedBackend) writeCompletedMultipartBlob(ctx context.Context, metaBlob []byte) (PutObjectMetaCmd, error) {
	cmd, err := decodeQuorumMetaBlob(metaBlob)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: decode meta_blob cmd: %w", err)
	}
	if err := b.fanOutPerVersionBlob(ctx, cmd, metaBlob); err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: per-version blob: %w", err)
	}
	return cmd, nil
}

func (b *DistributedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	unlockBucketWrite, err := b.enterBucketObjectWrite(ctx, bucket)
	if err != nil {
		return nil, err
	}
	defer unlockBucketWrite()
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	// Deterministic, manifest-independent VersionID derived from the (raw)
	// uploadID: concurrent completes of the same upload converge on one version and
	// an idempotent retry re-derives the same id. uploadID here is RAW (the
	// coordinator strips any "mpg:" group prefix before the backend call).
	versionID, verr := deriveMultipartVID(uploadID)
	if verr != nil {
		return nil, verr
	}

	// Det-vid existence short-circuit (M3): the completed object's per-version /
	// latest-only blob IS the idempotency record — there is no done-marker (the
	// CmdCompleteMultipart propose is gone). If the det-vid object already exists for
	// this (bucket, key), this is an idempotent retry (or a concurrent loser whose
	// twin already won) — return the committed object WITHOUT re-assembling. The check
	// is version-agnostic (per-version blob for versioned buckets, latest-only blob
	// whose VersionID == det-vid for non-versioned), so it works on every retry path
	// regardless of whether this completer's manifest blob was already deleted.
	//
	// multipartCompletedObjectExists returns the matched object directly so versioned
	// buckets always get the det-vid version (not the latest, which may be a newer PUT).
	if obj, done, derr := b.multipartCompletedObjectExists(ctx, bucket, key, uploadID); derr != nil {
		return nil, derr
	} else if done {
		return obj, nil
	}

	// Read the upload manifest off the .qmeta_mpu blob (M2b). With the det-vid object
	// absent (no short-circuit above), the manifest is the sole session record: its
	// absence means the upload never existed / was aborted / already completed for a
	// DIFFERENT key — all NoSuchUpload (ErrUploadNotFound) under the per-(bucket,key)
	// det-vid model.
	manifestMeta, manifestOK, err := b.readManifestBlob(bucket, uploadID)
	if err != nil {
		return nil, err
	}
	if !manifestOK {
		return nil, storage.ErrUploadNotFound
	}
	// The manifest blob is the upload's authority: validate the requested (bucket,
	// key) against it (the check the FSM apply used to do against the mpu: manifest
	// before M2b moved it off-raft). A mismatch means the client addressed the wrong
	// object for this uploadID.
	if manifestMeta.Bucket != bucket || manifestMeta.Key != key {
		return nil, fmt.Errorf("complete multipart upload %s mismatch: manifest is for %s/%s, got %s/%s",
			uploadID, manifestMeta.Bucket, manifestMeta.Key, bucket, key)
	}
	meta := manifestMeta
	if meta.PlacementGroupID != "" {
		var ctxErr error
		ctx, ctxErr = contextForMultipartComplete(ctx, meta, func(id string) (ShardGroupEntry, bool) {
			group, ok := PlacementGroupEntryFromContext(ctx)
			if !ok || group.ID != id {
				return ShardGroupEntry{}, false
			}
			return group, true
		})
		if ctxErr != nil {
			return nil, ctxErr
		}
	}

	manifest, err := b.buildMultipartCompleteManifest(uploadID, parts)
	if err != nil {
		return nil, err
	}

	var obj *storage.Object
	if b.currentECConfig().NumShards() > 0 && b.shardSvc != nil && b.shardGroup != nil {
		// Single multipart-complete path: every completion (any total size) takes
		// the chunked/segment path so the route does not branch on size — matching
		// the simple-PUT single path. putMultipartObjectChunked emits one segment
		// for small/empty totals. Requires a ShardGroupSource (always wired in
		// production; test backends wire a 1-node group by default).
		beforeCommit := b.testBeforeChunkedMultipartCommit
		obj, err = b.putMultipartObjectChunked(ctx, bucket, key, versionID, uploadID, manifest, meta.ContentType, nil, "", 0, false, "", beforeCommit, meta.Tags)
	} else {
		err = fmt.Errorf("complete multipart: EC storage is required")
	}
	if err != nil {
		return nil, err
	}
	if serr := b.writeCompletedMultipartSentinel(ctx, bucket, uploadID, obj); serr != nil {
		b.logger.Debug().Err(serr).Str("upload_id", uploadID).Msg("multipart completion sentinel write failed")
	}
	if err := os.RemoveAll(b.partDir(uploadID)); err != nil {
		b.logger.Debug().Err(err).Str("upload_id", uploadID).Msg("multipart part cleanup after complete failed")
	}
	// The completed object's blob is durable (obj is non-nil), so the session is
	// finished — drop the off-FSM manifest blob. Best-effort: a leaked manifest is
	// reconciled by ListMultipartUploads (the det-vid object exists) and aborted by
	// the lifecycle age backstop.
	if derr := b.deleteManifestBlob(bucket, uploadID); derr != nil {
		b.logger.Debug().Err(derr).Str("upload_id", uploadID).Msg("multipart manifest blob cleanup after complete failed")
	}
	return obj, nil
}

func completedMultipartCmdFromObject(bucket string, obj *storage.Object) PutObjectMetaCmd {
	if obj == nil {
		return PutObjectMetaCmd{}
	}
	return PutObjectMetaCmd{
		Bucket:           bucket,
		Key:              obj.Key,
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		ETag:             obj.ETag,
		ModTime:          obj.LastModified,
		VersionID:        obj.VersionID,
		ECData:           obj.ECData,
		ECParity:         obj.ECParity,
		StripeBytes:      obj.StripeBytes,
		NodeIDs:          cloneStringSlice(obj.NodeIDs),
		PlacementGroupID: obj.PlacementGroupID,
		UserMetadata:     cloneStringMap(obj.UserMetadata),
		SSEAlgorithm:     obj.SSEAlgorithm,
		IsDeleteMarker:   obj.IsDeleteMarker,
		Parts:            append([]storage.MultipartPartEntry(nil), obj.Parts...),
		Segments:         segmentRefsToMetaEntries(obj.Segments),
		Tags:             append([]storage.Tag(nil), obj.Tags...),
		ACL:              obj.ACL,
		Coalesced:        coalescedStorageRefsToMeta(obj.Coalesced),
		IsAppendable:     obj.IsAppendable,
		AppendCallMD5s:   cloneBytesSlice(obj.AppendCallMD5s),
	}
}

func coalescedStorageRefsToMeta(in []storage.CoalescedRef) []CoalescedShardRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]CoalescedShardRef, len(in))
	for i, c := range in {
		out[i] = CoalescedShardRef{
			CoalescedID: c.CoalescedID,
			Size:        c.Size,
			ETag:        c.ETag,
			ShardKey:    c.ShardKey,
			ECData:      c.ECData,
			ECParity:    c.ECParity,
			StripeBytes: c.StripeBytes,
			NodeIDs:     cloneStringSlice(c.NodeIDs),
		}
	}
	return out
}

func cloneBytesSlice(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = append([]byte(nil), in[i]...)
	}
	return out
}

func (b *DistributedBackend) writeCompletedMultipartSentinel(ctx context.Context, bucket, uploadID string, obj *storage.Object) error {
	cmd := completedMultipartCmdFromObject(bucket, obj)
	if cmd.Key == "" || cmd.VersionID == "" {
		return nil
	}
	blob, err := encodeQuorumMetaBlob(cmd)
	if err != nil {
		return fmt.Errorf("multipart completion sentinel encode: %w", err)
	}
	return b.writeManifestRawBlob(ctx, bucket, completedMultipartUploadKey(uploadID), blob, b.multipartManifestNodeIDs(ctx))
}

func (b *DistributedBackend) readCompletedMultipartSentinel(bucket, key, uploadID string) (*storage.Object, bool, error) {
	raw, ok, err := b.readManifestRawBlob(bucket, completedMultipartUploadKey(uploadID))
	if err != nil || !ok {
		return nil, ok, err
	}
	cmd, err := decodeQuorumMetaBlob(raw)
	if err != nil {
		return nil, false, fmt.Errorf("multipart completion sentinel decode: %w", err)
	}
	if cmd.Bucket != bucket || cmd.Key != key {
		return nil, false, nil
	}
	obj, _ := objectAndPlacementFromCmd(cmd)
	return obj, true, nil
}

func contextForMultipartComplete(
	ctx context.Context,
	meta clusterMultipartMeta,
	lookup func(string) (ShardGroupEntry, bool),
) (context.Context, error) {
	if meta.PlacementGroupID == "" {
		return ctx, nil
	}
	if group, ok := PlacementGroupEntryFromContext(ctx); ok && group.ID == meta.PlacementGroupID {
		return ctx, nil
	}
	group, ok := lookup(meta.PlacementGroupID)
	if !ok {
		return ctx, &ErrInsufficientPlacementTargets{
			Operation:     "complete_multipart",
			GroupID:       meta.PlacementGroupID,
			FailureReason: "multipart placement group is missing",
		}
	}
	return ContextWithPlacementGroupEntry(ctx, group), nil
}

// ListMultipartUploads enumerates in-progress uploads off the .qmeta_mpu blobs
// (M2b). It runs a STRICT cluster scan (scanManifestBlobsCluster dedups an
// uploadID across its K replicas), keeps only manifests this backend's group
// owns (a GroupBackend sees the shared meta-FSM's every-group peer set, so it
// must filter to its own PlacementGroupID; the single-node / direct backend has
// no group and keeps all), applies the bucket/prefix filter, and reconciles a
// LEAKED manifest — one whose completed object already exists (its complete
// dropped the per-version/latest blob's twin but a best-effort manifest delete
// missed). A leaked manifest is filtered out and best-effort re-deleted; the
// lifecycle age-abort is the backstop if reconcile misses.
func (b *DistributedBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	entries, err := b.scanManifestBlobsCluster(bucket)
	if err != nil {
		return nil, err
	}
	var uploads []*storage.MultipartUpload
	for _, e := range entries {
		m := e.Meta
		if m.Bucket == "" || m.Key == "" {
			continue
		}
		if m.Bucket != bucket || !strings.HasPrefix(m.Key, prefix) {
			continue
		}
		// A GroupBackend owns exactly one group; the cluster scan reaches every
		// group's nodes via the shared meta-FSM, so drop manifests another group
		// owns (the coordinator lists per-group and wraps each result with that
		// group's ID). A group-less single-node backend keeps all.
		if b.groupID != "" && m.PlacementGroupID != b.groupID {
			continue
		}
		// Reconcile a leaked manifest: a completed upload whose object exists.
		if _, done, derr := b.multipartCompletedObjectExists(ctx, m.Bucket, m.Key, e.UploadID); derr != nil {
			return nil, derr
		} else if done {
			_ = b.deleteManifestBlob(m.Bucket, e.UploadID) // best-effort re-delete
			continue
		}
		uploads = append(uploads, &storage.MultipartUpload{
			UploadID:    e.UploadID,
			Bucket:      m.Bucket,
			Key:         m.Key,
			ContentType: m.ContentType,
			CreatedAt:   m.CreatedAt,
		})
	}
	sort.Slice(uploads, func(i, j int) bool {
		return multipartUploadLess(*uploads[i], *uploads[j])
	})
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}
	return uploads, nil
}

// multipartCompletedObjectExists reports whether the multipart upload rawUploadID
// already completed — i.e. its deterministic det-vid object exists — and returns
// the matched object directly so the caller does not need a second read. The check
// is VERSION-AGNOSTIC: it returns (obj, true, nil) if EITHER the per-version blob
// (covers versioned buckets) OR the latest-only blob whose VersionID matches det-vid
// (covers non-versioned buckets) is present. Checking both blobs avoids the need to
// call bucketVersioningEnabled, which requires a versioning-context stamp that is not
// available on the ListMultipartUploads path (GroupBackend reads per-group BadgerDB
// which has no bucket keys and always returns "Unversioned").
//
// Returning the matched cmd's object directly is important for versioned buckets: the
// short-circuit caller must return the det-vid version's object, not headObjectMeta's
// latest (which may be a newer PutObject that landed between the original complete and
// a duplicate/retry call).
func (b *DistributedBackend) multipartCompletedObjectExists(_ context.Context, bucket, key, rawUploadID string) (*storage.Object, bool, error) {
	detVID, err := deriveMultipartVID(rawUploadID)
	if err != nil {
		return nil, false, err
	}
	// Per-version blob: authoritative for versioned buckets.
	cmd, ok, verr := b.readQuorumMetaVersion(bucket, key, detVID)
	if verr != nil {
		return nil, false, verr
	}
	if ok {
		obj, _ := objectAndPlacementFromCmd(cmd)
		return obj, true, nil
	}
	// Latest-only blob: authoritative for non-versioned buckets.
	// An unrelated later PUT to the same key produces a different VersionID, so
	// we only treat the latest blob as evidence of completion when its VID matches.
	latestCmd, lerr := b.readQuorumMetaCmd(bucket, key)
	if lerr != nil {
		if errors.Is(lerr, storage.ErrObjectNotFound) {
			return nil, false, nil
		}
		return nil, false, lerr
	}
	if latestCmd.VersionID != detVID {
		if latestCmd.IsHardDeleted || latestCmd.IsDeleteMarker || latestCmd.ETag == deleteMarkerETag {
			return nil, false, nil
		}
		return b.readCompletedMultipartSentinel(bucket, key, rawUploadID)
	}
	obj, _ := objectAndPlacementFromCmd(latestCmd)
	return obj, true, nil
}

func multipartUploadLess(a, b storage.MultipartUpload) bool {
	if a.CreatedAt != b.CreatedAt {
		return a.CreatedAt < b.CreatedAt
	}
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	return a.UploadID < b.UploadID
}

// ListParts walks the local node's partDir for the given uploadID. Parts
// uploaded against another node's routed group are not visible from here;
// callers that need the full set should target the routed group directly
// (see ClusterCoordinator routing). uploadID existence is checked against
// the off-FSM manifest blob so a missing uploadID returns ErrUploadNotFound
// consistently across the owning group's nodes even when no parts landed
// locally.
func (b *DistributedBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	_ = ctx
	_ = key
	if _, ok, err := b.readManifestBlob(bucket, uploadID); err != nil {
		return nil, err
	} else if !ok {
		return nil, storage.ErrUploadNotFound
	}
	entries, err := os.ReadDir(b.partDir(uploadID))
	if os.IsNotExist(err) {
		return []storage.Part{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read part dir: %w", err)
	}
	out := make([]storage.Part, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		partNumber, parseErr := strconv.Atoi(entry.Name())
		if parseErr != nil || partNumber <= 0 {
			continue
		}
		f, err := b.openMultipartPart(uploadID, partNumber)
		if err != nil {
			return nil, fmt.Errorf("open part %d: %w", partNumber, err)
		}
		h := md5Pool.Get().(hash.Hash)
		h.Reset()
		size, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			md5Pool.Put(h)
			return nil, fmt.Errorf("hash part %d: %w", partNumber, err)
		}
		out = append(out, storage.Part{
			PartNumber: partNumber,
			ETag:       hex.EncodeToString(h.Sum(nil)),
			Size:       size,
		})
		md5Pool.Put(h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	if maxParts > 0 && len(out) > maxParts {
		out = out[:maxParts]
	}
	return out, nil
}

func (b *DistributedBackend) openMultipartPart(uploadID string, partNumber int) (io.ReadCloser, error) {
	full := b.partPath(uploadID, partNumber)
	if b.encryptedShardStorage() {
		return openSpoolEncryptedRecordFile(full, b.shardSvc.segEnc(), clusterMultipartPartDomain(uploadID, partNumber))
	}
	return os.Open(full)
}

func clusterMultipartPartDomain(uploadID string, partNumber int) string {
	return fmt.Sprintf("cluster-multipart-part:%s:%d", uploadID, partNumber)
}

func (b *DistributedBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return err
	}
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	if _, ok, err := b.readManifestBlob(bucket, uploadID); err != nil {
		return err
	} else if !ok {
		return storage.ErrUploadNotFound
	}

	// Off-FSM abort (M2b): drop the manifest blob (idempotent local + best-effort
	// peer deletes) — no CmdAbortMultipart propose. The local staged parts are then
	// removed. A K-fold duplicate abort (each owning-group voter, or a re-scan) is a
	// harmless idempotent no-op.
	if err := b.deleteManifestBlob(bucket, uploadID); err != nil {
		return err
	}

	os.RemoveAll(b.partDir(uploadID))
	return nil
}

// --- Versioning ---
