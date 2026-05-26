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
	"strings"
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

// NewWriteBuffer is the exported constructor used by serveruntime to build
// the buffer before passing it to the server via WithWriteBuffer.
func NewWriteBuffer(dir string, backend writeBufferBackend) *writeBuffer {
	return newWriteBuffer(dir, backend)
}

// SetIdleTimeout overrides the default 30s idle flush timeout. Call before
// passing the buffer to Run.
func (wb *writeBuffer) SetIdleTimeout(d time.Duration) {
	wb.idleTimeout = d
}

// SetRecoveryRetryDelay overrides the default 1s recovery retry delay.
// Useful for tests or operators tuning startup behavior.
func (wb *writeBuffer) SetRecoveryRetryDelay(d time.Duration) {
	wb.recoveryRetryDelay = d
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

// Size returns the buffered object's current on-disk size. ok=false means
// the caller should fall back to backend metadata (HeadObject etc.). Used by
// NFS metadata ops (GETATTR, LOOKUP) so client `stat` sees the buffered file
// size — without this, stat reports backend size (often 0 for new objects),
// the client thinks the file is empty, and `read` short-circuits without
// issuing a READ op.
func (wb *writeBuffer) Size(bucket, key string) (int64, bool) {
	k := wb.entryKey(bucket, key)
	v, ok := wb.entries.Load(k)
	if !ok {
		return 0, false
	}
	entry := v.(*writeBufferEntry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.materialized {
		return 0, false
	}
	fi, err := os.Stat(entry.dataPath)
	if err != nil {
		return 0, false
	}
	return fi.Size(), true
}

// Read returns (data, hit, err). hit=false means caller should fall back to backend.
//
// Concurrency: holds the entry mutex only through state check + os.Open.
// The pread syscall runs without the mutex — a concurrent Flush (which holds
// the mutex through PutObject) cannot block reads. The open *os.File pins the
// inode, so if Flush completes and removes the path before our ReadAt, we
// still read the original data.
func (wb *writeBuffer) Read(ctx context.Context, bucket, key string, offset uint64, length uint32) ([]byte, bool, error) {
	k := wb.entryKey(bucket, key)
	v, ok := wb.entries.Load(k)
	if !ok {
		return nil, false, nil
	}
	entry := v.(*writeBufferEntry)
	entry.mu.Lock()
	if !entry.materialized {
		entry.mu.Unlock()
		return nil, false, nil
	}
	f, err := os.Open(entry.dataPath)
	if err != nil {
		entry.mu.Unlock()
		if errors.Is(err, os.ErrNotExist) {
			// Concurrent Flush removed the file after we found the entry in
			// the map but before we could open it. Backend now has the flushed
			// data; treat as a miss so the caller falls back to backend.GetObject.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("writebuffer open for read: %w", err)
	}
	entry.lastTouch = time.Now()
	entry.mu.Unlock()
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		return nil, false, fmt.Errorf("writebuffer pread: %w", err)
	}
	return buf[:n], true, nil
}

// lenFile satisfies interface { Len() int } so dataWAL AppendReader takes
// the sized fast path (one make+ReadFull, no bytes.Buffer doubling). The
// underlying *os.File is the actual io.Reader; Len() reports the on-disk
// size captured at the time of Flush.
type lenFile struct {
	*os.File
	size int
}

func (lf *lenFile) Len() int { return lf.size }

// Flush uploads the buffered object to backend via PutObject and removes the
// entry. The entry stays visible to concurrent readers throughout PutObject
// (D10) — we use Load + mu.Lock + Delete-on-success, not LoadAndDelete.
// On PutObject failure the entry is preserved so the next flush attempt
// retries; readers continue to hit the buffer in the meantime.
func (wb *writeBuffer) Flush(ctx context.Context, bucket, key string) error {
	k := wb.entryKey(bucket, key)
	v, ok := wb.entries.Load(k)
	if !ok {
		return nil
	}
	entry := v.(*writeBufferEntry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.materialized {
		// Entry created but no write reached materialize — nothing to flush.
		wb.entries.Delete(k)
		_ = os.Remove(entry.dataPath) // may not exist; ignore
		_ = os.Remove(entry.metaPath)
		return nil
	}
	f, err := os.Open(entry.dataPath)
	if err != nil {
		return fmt.Errorf("writebuffer open for flush: %w", err)
	}
	fi, statErr := f.Stat()
	if statErr != nil {
		f.Close()
		return fmt.Errorf("writebuffer stat for flush: %w", statErr)
	}
	body := &lenFile{File: f, size: int(fi.Size())}
	_, putErr := wb.backend.PutObject(ctx, entry.bucket, entry.key, body, entry.contentType)
	f.Close()
	if putErr != nil {
		// Leave entry in place; READ keeps hitting the buffer; next flush retries.
		return fmt.Errorf("writebuffer putobject: %w", putErr)
	}
	wb.entries.Delete(k)
	_ = os.Remove(entry.dataPath)
	_ = os.Remove(entry.metaPath)
	return nil
}

// Discard drops the in-memory entry and on-disk files without calling PutObject.
// Used when an upstream op (SETATTR truncate, explicit invalidate) semantically
// subsumes pending buffer state.
//
// Note: must mirror Flush's lifecycle order — take the entry mutex BEFORE
// removing the map entry. If we LoadAndDelete first then Lock, a concurrent
// Write can race in via getOrCreate, create a new entry pointing to the same
// sha1 dataPath, and run materialize → os.Create on the same file that Discard
// is about to remove.
func (wb *writeBuffer) Discard(_ context.Context, bucket, key string) error {
	k := wb.entryKey(bucket, key)
	v, ok := wb.entries.Load(k)
	if !ok {
		return nil
	}
	entry := v.(*writeBufferEntry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	wb.entries.Delete(k)
	_ = os.Remove(entry.dataPath)
	_ = os.Remove(entry.metaPath)
	return nil
}

// Run is the background flusher. Periodically (every idleTimeout/2) walks
// entries and flushes ones whose lastTouch is older than idleTimeout. On
// ctx.Done() it drains all outstanding entries before returning, so server
// shutdown doesn't strand ACK'd writes in local files.
func (wb *writeBuffer) Run(ctx context.Context) {
	tick := time.NewTicker(wb.idleTimeout / 2)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wb.flushAll(context.Background())
			return
		case <-tick.C:
			wb.flushIdle(ctx)
		}
	}
}

func (wb *writeBuffer) flushIdle(ctx context.Context) {
	now := time.Now()
	var stale []string
	wb.entries.Range(func(k, v any) bool {
		entry := v.(*writeBufferEntry)
		entry.mu.Lock()
		if now.Sub(entry.lastTouch) >= wb.idleTimeout {
			stale = append(stale, k.(string))
		}
		entry.mu.Unlock()
		return true
	})
	for _, k := range stale {
		bucket, key, ok := splitEntryKey(k)
		if !ok {
			continue
		}
		_ = wb.Flush(ctx, bucket, key)
	}
}

func (wb *writeBuffer) flushAll(ctx context.Context) {
	var all []string
	wb.entries.Range(func(k, _ any) bool {
		all = append(all, k.(string))
		return true
	})
	for _, k := range all {
		bucket, key, ok := splitEntryKey(k)
		if !ok {
			continue
		}
		_ = wb.Flush(ctx, bucket, key)
	}
}

func splitEntryKey(k string) (bucket, key string, ok bool) {
	for i := 0; i < len(k); i++ {
		if k[i] == '/' {
			return k[:i], k[i+1:], true
		}
	}
	return "", "", false
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

// Recover replays leftover buffer files from a previous run. Called once
// at server startup, before Run. For each data file with a valid .meta
// sidecar, attempts PutObject (3 retries × recoveryRetryDelay). On
// permanent failure, quarantines the pair so a fresh post-restart Write
// for the same key cannot collide on the same sha1 path.
func (wb *writeBuffer) Recover(ctx context.Context) error {
	entries, err := os.ReadDir(wb.dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("writebuffer scan: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := filepath.Ext(name)
		if ext == ".meta" || strings.Contains(name, ".failed.") {
			continue
		}
		metaPath := filepath.Join(wb.dir, name+".meta")
		mb, err := os.ReadFile(metaPath)
		if err != nil {
			// Orphan data file with no meta — quarantine so normal writes don't
			// collide on the same sha1 name.
			wb.quarantine(name, "orphan-no-meta")
			continue
		}
		var meta struct {
			Bucket      string `json:"bucket"`
			Key         string `json:"key"`
			ContentType string `json:"content_type"`
		}
		if err := json.Unmarshal(mb, &meta); err != nil {
			wb.quarantine(name, "meta-decode-error")
			continue
		}
		dataPath := filepath.Join(wb.dir, name)
		f, err := os.Open(dataPath)
		if err != nil {
			wb.quarantine(name, "open-error")
			continue
		}
		fi, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			wb.quarantine(name, "stat-error")
			continue
		}
		body := &lenFile{File: f, size: int(fi.Size())}
		// Retry transient backend failures (cluster may still be joining at startup).
		// 3 attempts × wb.recoveryRetryDelay.
		var putErr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				if _, sErr := f.Seek(0, io.SeekStart); sErr != nil {
					putErr = sErr
					break
				}
				time.Sleep(wb.recoveryRetryDelay)
			}
			_, putErr = wb.backend.PutObject(ctx, meta.Bucket, meta.Key, body, meta.ContentType)
			if putErr == nil {
				break
			}
		}
		f.Close()
		if putErr != nil {
			// Critical: a normal write for the same (bucket, key) would derive
			// the same sha1 name and silently overwrite this file's leftover
			// state. Quarantine it so the operator notices and replays manually.
			wb.quarantine(name, "putobject-failed")
			continue
		}
		_ = os.Remove(dataPath)
		_ = os.Remove(metaPath)
	}
	return nil
}

func (wb *writeBuffer) quarantine(name, reason string) {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	suffix := fmt.Sprintf(".failed.%s.%s", reason, stamp)
	_ = os.Rename(filepath.Join(wb.dir, name), filepath.Join(wb.dir, name+suffix))
	_ = os.Rename(filepath.Join(wb.dir, name+".meta"), filepath.Join(wb.dir, name+".meta"+suffix))
}
