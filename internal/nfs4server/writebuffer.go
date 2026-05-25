package nfs4server

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	writeBufferDefaultIdleTimeout = 30 * time.Second
)

// writeBufferBackend is the minimal slice of storage.Backend that writeBuffer
// needs. Defined here at the use site (per project Coding Rules). The real
// storage.Backend implementations satisfy it automatically.
type writeBufferBackend interface {
	HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error)
	PutObject(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*storage.Object, error)
}

type writeBufferEntry struct {
	mu           sync.Mutex
	dataPath     string
	metaPath     string
	bucket       string
	key          string
	contentType  string
	lastTouch    time.Time
	materialized bool // false until existing object is fetched into the file
}

type writeBuffer struct {
	dir                string
	idleTimeout        time.Duration
	recoveryRetryDelay time.Duration // overridable in tests
	backend            writeBufferBackend
	entries            sync.Map // key="bucket/key" → *writeBufferEntry
}

func newWriteBuffer(dir string, backend writeBufferBackend) *writeBuffer {
	return &writeBuffer{
		dir:                dir,
		idleTimeout:        writeBufferDefaultIdleTimeout,
		recoveryRetryDelay: time.Second,
		backend:            backend,
	}
}

func (wb *writeBuffer) entryKey(bucket, key string) string {
	return bucket + "/" + key
}

func (wb *writeBuffer) bufferFileName(bucket, key string) string {
	h := sha1.Sum([]byte(bucket + "/" + key))
	return hex.EncodeToString(h[:])
}

func (wb *writeBuffer) getOrCreate(bucket, key, contentType string) *writeBufferEntry {
	k := wb.entryKey(bucket, key)
	if v, ok := wb.entries.Load(k); ok {
		return v.(*writeBufferEntry)
	}
	name := wb.bufferFileName(bucket, key)
	entry := &writeBufferEntry{
		dataPath:    filepath.Join(wb.dir, name),
		metaPath:    filepath.Join(wb.dir, name+".meta"),
		bucket:      bucket,
		key:         key,
		contentType: contentType,
		lastTouch:   time.Now(),
	}
	actual, _ := wb.entries.LoadOrStore(k, entry)
	return actual.(*writeBufferEntry)
}

func (wb *writeBuffer) Write(ctx context.Context, bucket, key string, offset uint64, data []byte, contentType string) error {
	if err := os.MkdirAll(wb.dir, 0o755); err != nil {
		return fmt.Errorf("writebuffer mkdir: %w", err)
	}
	entry := wb.getOrCreate(bucket, key, contentType)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !entry.materialized {
		if err := wb.materializeLocked(ctx, entry); err != nil {
			return err
		}
		if err := wb.writeMetaLocked(entry); err != nil {
			return err
		}
		entry.materialized = true
	}

	f, err := os.OpenFile(entry.dataPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("writebuffer open: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, int64(offset)); err != nil {
		return fmt.Errorf("writebuffer pwrite: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("writebuffer fsync: %w", err)
	}
	entry.lastTouch = time.Now()
	return nil
}

func (wb *writeBuffer) materializeLocked(ctx context.Context, entry *writeBufferEntry) error {
	rc, _, err := wb.backend.GetObject(ctx, entry.bucket, entry.key)
	if err != nil {
		// Only treat a definite NotFound as "new object, start with empty buffer".
		// A transient backend error (cluster still joining, network blip) MUST
		// fail the Write — initializing the buffer to empty here would let the
		// subsequent pwrite happily overwrite the real backend object with a
		// truncated copy on the next flush. Data loss.
		if errors.Is(err, storage.ErrObjectNotFound) {
			f, ferr := os.Create(entry.dataPath)
			if ferr != nil {
				return fmt.Errorf("writebuffer create: %w", ferr)
			}
			return f.Close()
		}
		return fmt.Errorf("writebuffer get existing object: %w", err)
	}
	defer rc.Close()
	f, err := os.Create(entry.dataPath)
	if err != nil {
		return fmt.Errorf("writebuffer create: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("writebuffer materialize: %w", err)
	}
	return nil
}

func (wb *writeBuffer) writeMetaLocked(entry *writeBufferEntry) error {
	meta := struct {
		Bucket      string `json:"bucket"`
		Key         string `json:"key"`
		ContentType string `json:"content_type"`
	}{entry.bucket, entry.key, entry.contentType}
	b, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("writebuffer marshal meta: %w", err)
	}
	return os.WriteFile(entry.metaPath, append(b, '\n'), 0o600)
}
