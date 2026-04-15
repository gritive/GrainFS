package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

type multipartMeta struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
}

func (b *LocalBackend) CreateMultipartUpload(bucket, key, contentType string) (*MultipartUpload, error) {
	if err := b.HeadBucket(bucket); err != nil {
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
		return txn.Set(b.multipartKey(uploadID), data)
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

func (b *LocalBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
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
	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h := md5.New()
	w := io.MultiWriter(f, h)
	size, err := io.Copy(w, r)
	f.Close()
	if err != nil {
		os.Remove(partFile)
		return nil, fmt.Errorf("write part: %w", err)
	}

	return &Part{
		PartNumber: partNumber,
		ETag:       hex.EncodeToString(h.Sum(nil)),
		Size:       size,
	}, nil
}

func (b *LocalBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []Part) (*Object, error) {
	var meta multipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			decoded, err := unmarshalMultipartMeta(val)
			if err != nil {
				return err
			}
			meta = *decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// assemble final object
	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	out, err := os.Create(objPath)
	if err != nil {
		return nil, fmt.Errorf("create final object: %w", err)
	}

	h := md5.New()
	w := io.MultiWriter(out, h)
	var totalSize int64

	for _, p := range parts {
		partFile := b.partPath(uploadID, p.PartNumber)
		f, err := os.Open(partFile)
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("open part %d: %w", p.PartNumber, err)
		}
		n, err := io.Copy(w, f)
		f.Close()
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("copy part %d: %w", p.PartNumber, err)
		}
		totalSize += n
	}
	out.Close()

	etag := hex.EncodeToString(h.Sum(nil))
	now := time.Now().Unix()

	obj := &Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  meta.ContentType,
		ETag:         etag,
		LastModified: now,
	}

	objMeta, _ := marshalObject(obj)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(b.objectMetaKey(bucket, key), objMeta); err != nil {
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

func (b *LocalBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
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
