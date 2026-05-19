package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

func (b *LocalBackend) multipartKey(uploadID string) []byte {
	return []byte("mpu:" + uploadID)
}

func (b *LocalBackend) partDir(uploadID string) string {
	return filepath.Join(b.root, "parts", uploadID)
}

func (b *LocalBackend) partPath(uploadID string, partNumber int) string {
	return filepath.Join(b.partDir(uploadID), fmt.Sprintf("%05d", partNumber))
}

func (b *LocalBackend) multipartPartDomain(uploadID string, partNumber int) string {
	return fmt.Sprintf("multipart-part:%s:%d", uploadID, partNumber)
}

type multipartMeta struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
}

func (b *LocalBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	uploadID := uuid.New().String()
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()
	meta := multipartMeta{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	}

	data, err := marshalMultipartMeta(&meta)
	if err != nil {
		return nil, fmt.Errorf("marshal multipart meta: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, b.encryptor, badgerDomainMultipart, b.multipartKey(uploadID), data)
	})
	if err != nil {
		return nil, err
	}

	return &MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	}, nil
}

func (b *LocalBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
	_ = ctx
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	partFile := b.partPath(uploadID, partNumber)
	if b.encryptor != nil {
		h, release := hashForBucket(bucket)
		size, err := writeEncryptedObjectFile(partFile, b.encryptor, b.multipartPartDomain(uploadID, partNumber), r, h)
		if err != nil {
			release()
			os.Remove(partFile)
			return nil, fmt.Errorf("write encrypted part: %w", err)
		}
		etag := etagFromHash(h)
		release()
		return &Part{
			PartNumber: partNumber,
			ETag:       etag,
			Size:       size,
		}, nil
	}

	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h, release := hashForBucket(bucket)
	w := io.MultiWriter(f, h)
	size, err := io.Copy(w, r)
	f.Close()
	if err != nil {
		release()
		os.Remove(partFile)
		return nil, fmt.Errorf("write part: %w", err)
	}
	etag := etagFromHash(h)
	release()

	return &Part{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       size,
	}, nil
}

func (b *LocalBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error) {
	_ = ctx
	var meta multipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainMultipart, b.multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		decoded, err := unmarshalMultipartMeta(val)
		if err != nil {
			return err
		}
		meta = *decoded
		return nil
	})
	if err != nil {
		return nil, err
	}

	// sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	var totalSize int64
	var etag string

	if b.encryptor != nil {
		partReader := &encryptedMultipartPartsReader{backend: b, uploadID: uploadID, parts: parts}
		defer partReader.Close()
		h, release := hashForBucket(bucket)
		totalSize, err = writeEncryptedObjectFile(objPath, b.encryptor, encryptedObjectFileDomain(bucket, key), partReader, h)
		if err != nil {
			release()
			os.Remove(objPath)
			return nil, fmt.Errorf("write encrypted final object: %w", err)
		}
		etag = etagFromHash(h)
		release()
	} else {
		out, err := os.Create(objPath)
		if err != nil {
			return nil, fmt.Errorf("create final object: %w", err)
		}

		h, release := hashForBucket(bucket)
		w := io.MultiWriter(out, h)

		for _, p := range parts {
			partFile := b.partPath(uploadID, p.PartNumber)
			f, err := os.Open(partFile)
			if err != nil {
				out.Close()
				release()
				os.Remove(objPath)
				return nil, fmt.Errorf("open part %d: %w", p.PartNumber, err)
			}
			n, err := io.Copy(w, f)
			f.Close()
			if err != nil {
				out.Close()
				release()
				os.Remove(objPath)
				return nil, fmt.Errorf("copy part %d: %w", p.PartNumber, err)
			}
			totalSize += n
		}
		out.Close()
		etag = etagFromHash(h)
		release()
	}
	now := time.Now().Unix()

	partsCopy := make([]MultipartPartEntry, len(parts))
	for i, p := range parts {
		partsCopy[i] = MultipartPartEntry{
			PartNumber: p.PartNumber,
			Size:       p.Size,
			ETag:       p.ETag,
		}
	}
	obj := &Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  meta.ContentType,
		ETag:         etag,
		LastModified: now,
		Parts:        partsCopy,
	}

	objMeta, _ := marshalObject(obj)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := setBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(bucket, key), objMeta); err != nil {
			return err
		}
		return txn.Delete(b.multipartKey(uploadID))
	})
	if err != nil {
		return nil, err
	}

	// cleanup parts
	os.RemoveAll(b.partDir(uploadID))

	return obj, nil
}

type encryptedMultipartPartsReader struct {
	backend  *LocalBackend
	uploadID string
	parts    []Part
	idx      int
	current  io.ReadCloser
}

func (r *encryptedMultipartPartsReader) Read(p []byte) (int, error) {
	for {
		if r.current == nil {
			if r.idx >= len(r.parts) {
				return 0, io.EOF
			}
			part := r.parts[r.idx]
			partPath := r.backend.partPath(r.uploadID, part.PartNumber)
			size := part.Size
			if size <= 0 {
				var err error
				size, err = encryptedObjectFilePlainSize(partPath)
				if err != nil {
					return 0, fmt.Errorf("size encrypted part %d: %w", part.PartNumber, err)
				}
			}
			rc, err := openEncryptedObjectFile(
				partPath,
				r.backend.encryptor,
				r.backend.multipartPartDomain(r.uploadID, part.PartNumber),
				size,
			)
			if err != nil {
				return 0, fmt.Errorf("open encrypted part %d: %w", part.PartNumber, err)
			}
			r.current = rc
		}
		n, err := r.current.Read(p)
		if err == io.EOF {
			_ = r.current.Close()
			r.current = nil
			r.idx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *encryptedMultipartPartsReader) Close() error {
	if r.current == nil {
		return nil
	}
	err := r.current.Close()
	r.current = nil
	return err
}

// ListMultipartUploads scans the multipart-meta keyspace and returns uploads
// matching bucket and prefix. The scan is metadata-only (no part files) so
// the cost is bounded by the number of in-progress uploads, not their byte
// count. Sorted by CreatedAt ascending so resumed uploads are stable across
// repeated calls. maxUploads <= 0 means no cap.
func (b *LocalBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*MultipartUpload, error) {
	_ = ctx
	if err := b.HeadBucket(context.Background(), bucket); err != nil {
		return nil, err
	}
	out := make([]*MultipartUpload, 0)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		mpuPrefix := []byte("mpu:")
		for it.Seek(mpuPrefix); it.ValidForPrefix(mpuPrefix); it.Next() {
			item := it.Item()
			itemKey := item.KeyCopy(nil)
			err := item.Value(func(val []byte) error {
				plain, err := openBadgerValue(b.encryptor, badgerDomainMultipart, itemKey, val)
				if err != nil {
					return err
				}
				meta, err := unmarshalMultipartMeta(plain)
				if err != nil {
					return err
				}
				if meta.Bucket != bucket {
					return nil
				}
				if prefix != "" && !strings.HasPrefix(meta.Key, prefix) {
					return nil
				}
				out = append(out, &MultipartUpload{
					UploadID:    meta.UploadID,
					Bucket:      meta.Bucket,
					Key:         meta.Key,
					ContentType: meta.ContentType,
					CreatedAt:   meta.CreatedAt,
				})
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].UploadID < out[j].UploadID
	})
	if maxUploads > 0 && len(out) > maxUploads {
		out = out[:maxUploads]
	}
	return out, nil
}

// ListParts returns parts already uploaded for an in-progress multipart, sorted
// by part number ascending. ETag is recomputed by re-hashing the part file —
// fine for first slice (ListParts is not a hot path) and avoids storing a
// sidecar. maxParts <= 0 means no cap. Returns ErrUploadNotFound when no
// matching multipart record exists, even if the partDir happens to be empty.
func (b *LocalBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]Part, error) {
	_ = ctx
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(b.partDir(uploadID))
	if os.IsNotExist(err) {
		return []Part{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read part dir: %w", err)
	}
	partNumbers := make([]int, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		partNumber, parseErr := strconv.Atoi(name)
		if parseErr != nil || partNumber <= 0 {
			continue
		}
		partNumbers = append(partNumbers, partNumber)
	}
	sort.Ints(partNumbers)
	if maxParts > 0 && len(partNumbers) > maxParts {
		partNumbers = partNumbers[:maxParts]
	}

	out := make([]Part, 0, len(partNumbers))
	for _, partNumber := range partNumbers {
		h, release := hashForBucket(bucket)
		var size int64
		name := fmt.Sprintf("%05d", partNumber)
		if b.encryptor != nil {
			var err error
			size, err = hashEncryptedObjectFile(filepath.Join(b.partDir(uploadID), name), b.encryptor, b.multipartPartDomain(uploadID, partNumber), h)
			if err != nil {
				release()
				return nil, fmt.Errorf("hash encrypted part %d: %w", partNumber, err)
			}
		} else {
			full := filepath.Join(b.partDir(uploadID), name)
			f, err := os.Open(full)
			if err != nil {
				release()
				return nil, fmt.Errorf("open part %d: %w", partNumber, err)
			}
			size, err = io.Copy(h, f)
			f.Close()
			if err != nil {
				release()
				return nil, fmt.Errorf("hash part %d: %w", partNumber, err)
			}
		}
		partETag := etagFromHash(h)
		release()
		out = append(out, Part{
			PartNumber: partNumber,
			ETag:       partETag,
			Size:       size,
		})
	}
	return out, nil
}

func (b *LocalBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_ = ctx
	err := b.db.Update(func(txn *badger.Txn) error {
		mk := b.multipartKey(uploadID)
		_, err := txn.Get(mk)
		if err == badger.ErrKeyNotFound {
			return ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		return txn.Delete(mk)
	})
	if err != nil {
		return err
	}

	os.RemoveAll(b.partDir(uploadID))
	return nil
}

// OrphanMultipartSweepResult summarizes one sweep pass over the multipart
// staging directory. RemovedPaths contains the absolute filesystem paths of
// each removed uploadID directory so observability layers (HealEvent
// emitters, dashboards) can render them per-entry. Removed equals
// len(RemovedPaths). Errors collects per-entry failures that did not abort
// the whole sweep.
type OrphanMultipartSweepResult struct {
	Removed      int
	RemovedPaths []string
	Errors       []string
}

// OrphanMultipartSweeper is the optional capability a backend exposes when it
// owns a sweepable multipart staging area. The storage operations facade
// discovers it through the decorator stack so callers don't reach through
// wrappers (RecoveryWriteGate, SwappableBackend, ...) to find the
// filesystem-aware backend.
type OrphanMultipartSweeper interface {
	SweepOrphanMultiparts(ctx context.Context, before time.Time) (OrphanMultipartSweepResult, error)
}

// SweepOrphanMultiparts walks <root>/parts and removes uploadID directories
// whose mtime is at or before the before cutoff. Files at the parts/ root
// (e.g. accidental drops) are intentionally ignored — only directories are
// considered as in-progress upload staging. Per-entry errors are collected
// into Errors so a single bad subtree does not abort the whole sweep.
func (b *LocalBackend) SweepOrphanMultiparts(ctx context.Context, before time.Time) (OrphanMultipartSweepResult, error) {
	var res OrphanMultipartSweepResult
	partsRoot := filepath.Join(b.root, "parts")
	entries, err := os.ReadDir(partsRoot)
	if os.IsNotExist(err) {
		return res, nil
	}
	if err != nil {
		res.Errors = append(res.Errors, err.Error())
		return res, nil
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !entry.IsDir() {
			continue
		}
		full := filepath.Join(partsRoot, entry.Name())
		info, err := entry.Info()
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			continue
		}
		if info.ModTime().After(before) {
			continue
		}
		if err := os.RemoveAll(full); err != nil {
			res.Errors = append(res.Errors, err.Error())
			continue
		}
		res.Removed++
		res.RemovedPaths = append(res.RemovedPaths, full)
	}
	return res, nil
}
