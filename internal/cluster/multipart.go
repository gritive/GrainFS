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

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/storage"
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
// Tags travel through CreateMultipartUploadCmd (Raft replicated) and live on
// clusterMultipartMeta until CompleteMultipartUpload, where they are materialised
// onto the finalised object's objectMeta.Tags via the same Raft path that
// materialises content (CmdPutObjectMeta carries the Tags vector — no separate
// SetObjectTags proposal).
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
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return "", 0, err
	}

	// Mint a UUIDv7 uploadID: deriveMultipartVID reuses its 48-bit ms timestamp
	// so the completed object's deterministic VersionID stays create-time ordered.
	uploadID := uuid.Must(uuid.NewV7()).String()
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
			seam:   b.shardSvc.segEnc,
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
// returning nil for non-versioned/Suspended/internal buckets (which keep the legacy
// FSM obj:/lat: write). The opaque bytes travel through CmdCompleteMultipart into the
// done-marker (applyCompleteMultipart stores them verbatim), so any retry or
// concurrent-complete loser can re-write the WINNER's blob from the marker. Built on
// the PROPOSER because CompleteMultipartCmd does not carry UserMetadata/ACL/SSE.
func (b *DistributedBackend) buildMultipartMetaBlob(ctx context.Context, cmd PutObjectMetaCmd) ([]byte, error) {
	if storage.IsInternalBucket(cmd.Bucket) || cmd.VersionID == "" || !b.bucketVersioningEnabled(ctx, cmd.Bucket) {
		return nil, nil
	}
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	if err != nil {
		return nil, fmt.Errorf("multipart complete: encode meta_blob: %w", err)
	}
	return blob, nil
}

// writeCompletedMultipartBlob decodes the winning multipart object's encoded
// PutObjectMetaCmd (the done-marker's meta_blob) and durably writes its per-version
// quorum-meta blob, FAIL-CLOSED. The per-version blob is the sole authority for the
// completed versioned object (reads/LIST/GC/DEK all derive from it). The meta_blob
// bytes are stored verbatim, so the per-version blob is byte-identical to the
// done-marker copy. Idempotent: a dup write of the same vid+bytes no-ops via the LWW
// guard, so the winner's blob lands regardless of which complete attempt (winner or
// concurrent loser, original or retry) observed the done-marker. Returns the decoded
// winning object so callers can answer the S3 CompleteMultipartUpload response.
func (b *DistributedBackend) writeCompletedMultipartBlob(ctx context.Context, metaBlob []byte) (PutObjectMetaCmd, error) {
	env, err := DecodeCommand(metaBlob)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: decode meta_blob: %w", err)
	}
	cmd, err := decodePutObjectMetaCmd(env.Data)
	if err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: decode meta_blob cmd: %w", err)
	}
	if err := b.fanOutPerVersionBlob(ctx, cmd, metaBlob); err != nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: per-version blob: %w", err)
	}
	return cmd, nil
}

func (b *DistributedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	// Read the upload manifest off the .qmeta_mpu blob (M2b). The done-marker is
	// still an FSM key (written by applyCompleteMultipart) so an idempotent retry —
	// whose manifest blob the original complete already deleted — still resolves to
	// the committed object. Read the blob first; on a miss, fall back to the marker.
	manifestMeta, manifestOK, err := b.readManifestBlob(bucket, uploadID)
	if err != nil {
		return nil, err
	}
	var meta clusterMultipartMeta
	var doneMarker *multipartDone
	if manifestOK {
		// The manifest blob is now the upload's authority: validate the requested
		// (bucket, key) against it (the check the FSM apply used to do against the
		// mpu: manifest before M2b moved it off-raft). A mismatch means the client
		// addressed the wrong object for this uploadID.
		if manifestMeta.Bucket != bucket || manifestMeta.Key != key {
			return nil, fmt.Errorf("complete multipart upload %s mismatch: manifest is for %s/%s, got %s/%s",
				uploadID, manifestMeta.Bucket, manifestMeta.Key, bucket, key)
		}
		meta = manifestMeta
	} else {
		derr := b.store.View(func(txn MetadataTxn) error {
			doneItem, gerr := txn.Get(b.ks().MultipartDoneKey(uploadID))
			if gerr == ErrMetaKeyNotFound {
				return storage.ErrUploadNotFound
			}
			if gerr != nil {
				return gerr
			}
			raw, gerr := b.itemValueCopy(doneItem)
			if gerr != nil {
				return gerr
			}
			marker, gerr := unmarshalMultipartDone(raw)
			if gerr != nil {
				return gerr
			}
			if marker.Bucket != bucket || marker.Key != key {
				return fmt.Errorf("multipart upload %s already completed for %s/%s", uploadID, marker.Bucket, marker.Key)
			}
			doneMarker = &marker
			return nil
		})
		if derr != nil {
			return nil, derr
		}
	}
	// Idempotent retry: manifest is gone but marker exists; return committed object.
	if doneMarker != nil {
		// Blob-authoritative: if the marker carries the winning meta_blob, re-write
		// the per-version blob FAIL-CLOSED before the readback. The original complete
		// may have crashed/failed after the propose but before the blob landed — the
		// marker is the only metadata copy until the blob is durable, so without this
		// the object would be permanently 404. Idempotent: same vid+bytes, LWW no-op.
		if len(doneMarker.MetaBlob) > 0 {
			if _, werr := b.writeCompletedMultipartBlob(ctx, doneMarker.MetaBlob); werr != nil {
				return nil, werr
			}
		}
		obj, _, err := b.headObjectMetaV(ctx, bucket, key, doneMarker.VersionID)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}
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

	// Deterministic, manifest-independent VersionID derived from the (raw)
	// uploadID: concurrent completes of the same upload converge on one version
	// and an idempotent retry re-derives the same id. uploadID here is RAW (the
	// coordinator strips any "mpg:" group prefix before the backend call).
	versionID, verr := deriveMultipartVID(uploadID)
	if verr != nil {
		return nil, verr
	}
	var obj *storage.Object
	if b.currentECConfig().NumShards() > 0 && b.shardSvc != nil {
		// Single multipart-complete path: every completion (any total size) takes
		// the chunked/segment path so the route does not branch on size — matching
		// the simple-PUT single path. putMultipartObjectChunked emits one segment
		// for small/empty totals. The legacy spool path below is kept only for
		// backends without a ShardGroupSource (never production).
		if b.shardGroup != nil {
			beforeCommit := b.testBeforeChunkedMultipartCommit
			obj, err = b.putMultipartObjectChunked(ctx, bucket, key, versionID, uploadID, manifest, meta.ContentType, nil, "", 0, false, "", beforeCommit, meta.Tags)
		} else {
			var sp *spooledObject
			cleanupSpool := true
			if len(manifest.Parts) == 1 {
				sp = b.multipartPartSpooledObject(uploadID, manifest.Parts[0])
				cleanupSpool = false
			} else {
				var spoolErr error
				sp, spoolErr = b.spoolMultipartCompleteManifest(ctx, uploadID, versionID, bucket, manifest)
				if spoolErr != nil {
					return nil, spoolErr
				}
			}
			if cleanupSpool {
				defer sp.Cleanup()
			}
			obj, err = b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, versionID, sp, meta.ContentType, nil, "", 0, 0, false, "", nil, manifest.Parts, meta.Tags, uploadID)
		}
	} else {
		err = fmt.Errorf("complete multipart: EC storage is required")
	}
	if err != nil {
		return nil, err
	}
	if err := os.RemoveAll(b.partDir(uploadID)); err != nil {
		b.logger.Debug().Err(err).Str("upload_id", uploadID).Msg("multipart part cleanup after complete failed")
	}
	// The CmdCompleteMultipart propose committed (obj is non-nil), so the done-marker
	// is durable and the session is finished — drop the off-FSM manifest blob.
	// Best-effort: a leaked manifest is reconciled by ListMultipartUploads (the
	// completed object exists) and aborted by the lifecycle age backstop.
	if derr := b.deleteManifestBlob(bucket, uploadID); derr != nil {
		b.logger.Debug().Err(derr).Str("upload_id", uploadID).Msg("multipart manifest blob cleanup after complete failed")
	}
	return obj, nil
}

func (b *DistributedBackend) multipartPartSpooledObject(uploadID string, part storage.MultipartPartEntry) *spooledObject {
	sp := &spooledObject{
		Path: b.partPath(uploadID, part.PartNumber),
		Size: part.Size,
		ETag: part.ETag,
	}
	if b.encryptedShardStorage() {
		sp.encrypted = true
		sp.seam = b.shardSvc.segEnc
		sp.domain = clusterMultipartPartDomain(uploadID, part.PartNumber)
	}
	return sp
}

func (b *DistributedBackend) spoolMultipartCompleteManifest(ctx context.Context, uploadID, versionID, bucket string, manifest multipartCompleteManifest) (*spooledObject, error) {
	body, err := manifest.Open()
	if err != nil {
		return nil, fmt.Errorf("open multipart manifest: %w", err)
	}
	defer body.Close()
	if b.encryptedShardStorage() {
		return spoolObjectEncrypted(ctx, b.spoolDir(), body, bucket, b.shardSvc.segEnc, clusterMultipartSpoolDomain(uploadID, versionID))
	}
	return spoolObject(ctx, b.spoolDir(), body, bucket)
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
		if done, derr := b.multipartCompletedObjectExists(ctx, m.Bucket, m.Key, e.UploadID); derr != nil {
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
// already completed — i.e. its deterministic det-vid object exists. VERSIONED:
// the per-version blob at deriveMultipartVID(raw) is present. NON-VERSIONED:
// the latest-only blob exists AND its VersionID equals the det-vid (so an
// unrelated later PUT to the same key does not mask an in-flight upload).
func (b *DistributedBackend) multipartCompletedObjectExists(ctx context.Context, bucket, key, rawUploadID string) (bool, error) {
	detVID, err := deriveMultipartVID(rawUploadID)
	if err != nil {
		return false, err
	}
	if b.bucketVersioningEnabled(ctx, bucket) {
		_, ok, verr := b.readQuorumMetaVersion(bucket, key, detVID)
		if verr != nil {
			return false, verr
		}
		return ok, nil
	}
	cmd, lerr := b.readQuorumMetaCmd(bucket, key)
	if lerr != nil {
		if errors.Is(lerr, storage.ErrObjectNotFound) {
			return false, nil
		}
		return false, lerr
	}
	return cmd.VersionID == detVID, nil
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
		return openSpoolEncryptedRecordFile(full, b.shardSvc.segEnc, clusterMultipartPartDomain(uploadID, partNumber))
	}
	return os.Open(full)
}

func clusterMultipartPartDomain(uploadID string, partNumber int) string {
	return fmt.Sprintf("cluster-multipart-part:%s:%d", uploadID, partNumber)
}

func clusterMultipartSpoolDomain(uploadID, versionID string) string {
	return fmt.Sprintf("cluster-multipart-spool:%s:%s", uploadID, versionID)
}

func (b *DistributedBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
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
