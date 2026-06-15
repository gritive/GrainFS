package cluster

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
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

	uploadID := uuid.New().String()
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

	err := b.propose(ctx, CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:         uploadID,
		Bucket:           bucket,
		Key:              key,
		ContentType:      contentType,
		CreatedAt:        now,
		PlacementGroupID: placementGroupID,
		Tags:             tagsCopy,
	})
	if err != nil {
		os.RemoveAll(b.partDir(uploadID))
		return "", 0, err
	}
	return uploadID, now, nil
}

func (b *DistributedBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader, contentMD5Hex string) (*storage.Part, error) {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.RLock()
	defer lifeMu.RUnlock()

	// Verify upload exists (read local metadata)
	err := b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
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

func (b *DistributedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	// Read upload metadata
	var meta clusterMultipartMeta
	err := b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalClusterMultipartMeta(val)
		if err != nil {
			return err
		}
		meta = m
		return nil
	})
	if err != nil {
		return nil, err
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

	versionID := newVersionID()
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

func (b *DistributedBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	_ = ctx
	var uploads []*storage.MultipartUpload
	bucketBytes := []byte(bucket)
	prefixBytes := []byte(prefix)
	err := b.store.View(func(txn MetadataTxn) error {
		return b.ks().scanGroupPrefix(txn, []byte("mpu:"), func(rawKey []byte, item MetaItem) error {
			raw, err := b.itemValueCopy(item)
			if err != nil {
				return err
			}
			meta, err := fbSafe(raw, func(d []byte) *clusterpb.MultipartMeta {
				return clusterpb.GetRootAsMultipartMeta(d, 0)
			})
			if err != nil {
				return fmt.Errorf("unmarshal MultipartMeta: %w", err)
			}
			metaBucket := meta.Bucket()
			metaKey := meta.Key()
			if len(metaBucket) == 0 || len(metaKey) == 0 {
				return nil
			}
			if !bytes.Equal(metaBucket, bucketBytes) || !bytes.HasPrefix(metaKey, prefixBytes) {
				return nil
			}
			uploadID := strings.TrimPrefix(string(rawKey), "mpu:")
			uploads = append(uploads, &storage.MultipartUpload{
				UploadID:    uploadID,
				Bucket:      string(metaBucket),
				Key:         string(metaKey),
				ContentType: string(meta.ContentType()),
				CreatedAt:   meta.CreatedAt(),
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(uploads, func(i, j int) bool {
		return multipartUploadLess(*uploads[i], *uploads[j])
	})
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}
	return uploads, nil
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
// the FSM-replicated multipart record so a missing uploadID returns
// ErrUploadNotFound consistently across nodes even when no parts landed
// locally.
func (b *DistributedBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	_ = ctx
	_ = bucket
	_ = key
	err := b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
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

	err := b.store.View(func(txn MetadataTxn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == ErrMetaKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return err
	}

	if err := b.propose(ctx, CmdAbortMultipart, AbortMultipartCmd{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}); err != nil {
		return err
	}

	os.RemoveAll(b.partDir(uploadID))
	return nil
}

// --- Versioning ---
