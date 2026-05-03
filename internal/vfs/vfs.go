// Package vfs implements a billy.Filesystem backed by GrainFS object storage.
// Files and directories are stored as objects in a dedicated bucket per volume.
package vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

const (
	dirMarkerSuffix = "/.dir"
)

var grainBufPool = pool.New(func() *bytes.Buffer { return new(bytes.Buffer) })

func getBuf() *bytes.Buffer {
	b := grainBufPool.Get()
	b.Reset()
	return b
}

func putBuf(b *bytes.Buffer) {
	if b != nil {
		b.Reset()
		grainBufPool.Put(b)
	}
}

// GrainVFS implements billy.Filesystem on top of a storage.Backend.
type GrainVFS struct {
	backend storage.Backend
	bucket  string
	root    string // chroot prefix
	// Caching — CoW + atomic.Pointer: readers do zero-lock Load(), writers clone under thin Mutex.
	statCacheTTL time.Duration
	statCache    atomic.Pointer[map[string]*statCacheEntry]
	statWriteMu  sync.Mutex
	dirCacheTTL  time.Duration
	dirCache     atomic.Pointer[map[string]*dirCacheEntry]
	dirWriteMu   sync.Mutex
	// Optional thin-provisioning accounting.
	volMgr  *volume.Manager
	volName string
}

type statCacheEntry struct {
	info    os.FileInfo
	expires time.Time
}

type dirCacheEntry struct {
	entries []os.FileInfo
	expires time.Time
}

// VFSOption configures GrainVFS.
type VFSOption func(*GrainVFS)

// WithStatCacheTTL sets the TTL for Stat result caching.
func WithStatCacheTTL(ttl time.Duration) VFSOption {
	return func(fs *GrainVFS) { fs.statCacheTTL = ttl }
}

// WithDirCacheTTL sets the TTL for ReadDir result caching.
func WithDirCacheTTL(ttl time.Duration) VFSOption {
	return func(fs *GrainVFS) { fs.dirCacheTTL = ttl }
}

// WithVolumeManager wires a volume.Manager for thin-provisioning accounting.
// When set, Remove() and file truncation via Close() call RecordFreedBytes()
// to keep AllocatedBlocks consistent. Pass nil to disable (default).
func WithVolumeManager(m *volume.Manager, volName string) VFSOption {
	return func(fs *GrainVFS) {
		fs.volMgr = m
		fs.volName = volName
	}
}

var (
	_ billy.Filesystem = (*GrainVFS)(nil)
	_ billy.Change     = (*GrainVFS)(nil)
)

// New creates a new GrainVFS for the given volume name.
func New(backend storage.Backend, volumeName string, opts ...VFSOption) (*GrainVFS, error) {
	bucket := storage.VFSBucketPrefix + volumeName
	_ = backend.CreateBucket(context.Background(), bucket)
	if err := backend.HeadBucket(context.Background(), bucket); err != nil {
		return nil, fmt.Errorf("ensure vfs bucket: %w", err)
	}
	fs := &GrainVFS{backend: backend, bucket: bucket, root: ""}
	for _, o := range opts {
		o(fs)
	}
	if fs.statCacheTTL > 0 {
		m := make(map[string]*statCacheEntry)
		fs.statCache.Store(&m)
	}
	if fs.dirCacheTTL > 0 {
		m := make(map[string]*dirCacheEntry)
		fs.dirCache.Store(&m)
	}
	return fs, nil
}

// NewDirect creates a new GrainVFS that uses the bucket name directly (without prefix).
// This is used for cross-protocol access where VFS needs to access S3 buckets directly.
func NewDirect(backend storage.Backend, bucketName string, opts ...VFSOption) (*GrainVFS, error) {
	_ = backend.CreateBucket(context.Background(), bucketName)
	if err := backend.HeadBucket(context.Background(), bucketName); err != nil {
		return nil, fmt.Errorf("ensure bucket: %w", err)
	}
	fs := &GrainVFS{backend: backend, bucket: bucketName, root: ""}
	for _, o := range opts {
		o(fs)
	}
	if fs.statCacheTTL > 0 {
		m := make(map[string]*statCacheEntry)
		fs.statCache.Store(&m)
	}
	if fs.dirCacheTTL > 0 {
		m := make(map[string]*dirCacheEntry)
		fs.dirCache.Store(&m)
	}
	return fs, nil
}

func (fs *GrainVFS) fullPath(name string) string {
	name = cleanPath(name)
	if fs.root == "" {
		return name
	}
	return path.Join(fs.root, name)
}

func cleanPath(p string) string {
	p = path.Clean("/" + p)
	p = strings.TrimPrefix(p, "/")
	if p == "." || p == "" {
		return ""
	}
	return p
}

// Create creates the named file, truncating it if it exists.
func (fs *GrainVFS) Create(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// Open opens the named file for reading.
func (fs *GrainVFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile is the generalized open call.
func (fs *GrainVFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	fp := fs.fullPath(filename)
	name := path.Base(filename)

	f := &grainFile{
		fs:   fs,
		path: fp,
		name: name,
		flag: flag,
		perm: perm,
	}

	if flag&os.O_CREATE != 0 {
		if flag&os.O_TRUNC != 0 {
			if fs.volMgr != nil {
				if obj, err := fs.backend.HeadObject(context.Background(), fs.bucket, fp); err == nil {
					f.oldSize = obj.Size
				}
			}
			f.buf = getBuf()
		} else {
			// Try to load existing
			if err := f.loadExisting(); err != nil && !errors.Is(err, os.ErrNotExist) {
				putBuf(f.buf)
				return nil, err
			}
			if fs.volMgr != nil && f.buf != nil {
				f.oldSize = int64(f.buf.Len())
			}
		}
		return f, nil
	}

	// Must exist
	if flag == os.O_RDONLY {
		// 읽기 전용: rc를 저장해 스트리밍 모드로 진입 (io.ReadAll 버퍼링 없음)
		rc, _, err := fs.backend.GetObject(context.Background(), fs.bucket, fp)
		if err != nil {
			return nil, os.ErrNotExist
		}
		f.rc = rc
		return f, nil
	}
	if err := f.loadExisting(); err != nil {
		return nil, err
	}
	if fs.volMgr != nil && f.buf != nil {
		f.oldSize = int64(f.buf.Len())
	}
	if flag&os.O_TRUNC != 0 {
		putBuf(f.buf)
		f.buf = getBuf()
	}
	return f, nil
}

// Stat returns file info, using the stat cache if enabled.
func (fs *GrainVFS) Stat(filename string) (os.FileInfo, error) {
	fp := fs.fullPath(filename)

	// Check if file was deleted (ESTALE support for cross-protocol coherency)
	// Extract volume name from bucket: "__grainfs_vfs_" + volumeName
	volName := strings.TrimPrefix(fs.bucket, storage.VFSBucketPrefix)
	if fs.IsDeleted(volName, fp) {
		return nil, os.ErrNotExist
	}

	// Check stat cache
	if cached := fs.getStatCache(fp); cached != nil {
		return cached, nil
	}

	// Cache miss - track metric
	metrics.CacheStatMisses.Add(1)

	// Check if it's a directory
	if fs.isDir(fp) {
		info := &fileInfo{name: path.Base(filename), size: 0, isDir: true, modTime: time.Now()}
		fs.putStatCache(fp, info)
		return info, nil
	}

	// Check as file
	obj, err := fs.backend.HeadObject(context.Background(), fs.bucket, fp)
	if err != nil {
		return nil, os.ErrNotExist
	}

	info := &fileInfo{
		name:    path.Base(filename),
		size:    obj.Size,
		isDir:   false,
		modTime: time.Unix(obj.LastModified, 0),
	}
	fs.putStatCache(fp, info)
	return info, nil
}

// Rename renames a file.
func (fs *GrainVFS) Rename(oldpath, newpath string) error {
	oldFP := fs.fullPath(oldpath)
	newFP := fs.fullPath(newpath)

	rc, _, err := fs.backend.GetObject(context.Background(), fs.bucket, oldFP)
	if err != nil {
		return os.ErrNotExist
	}
	defer rc.Close()

	pr, pw := io.Pipe()
	defer pr.Close() // PutObject panic 시 writer goroutine 종료 보장

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, copyErr := io.Copy(pw, rc)
		pw.CloseWithError(copyErr)
	}()

	_, err = fs.backend.PutObject(context.Background(), fs.bucket, newFP, pr, "application/octet-stream")
	pr.CloseWithError(err)
	wg.Wait()
	if err != nil {
		return fmt.Errorf("write new file: %w", err)
	}

	if err := fs.backend.DeleteObject(context.Background(), fs.bucket, oldFP); err != nil {
		return err
	}

	fs.invalidateStatCache(oldFP)
	fs.invalidateStatCache(newFP)
	fs.invalidateParentDirCache(oldFP)
	fs.invalidateParentDirCache(newFP)
	return nil
}

// Remove removes a file or empty directory.
func (fs *GrainVFS) Remove(filename string) error {
	fp := fs.fullPath(filename)

	var oldSize int64
	if fs.volMgr != nil {
		if obj, err := fs.backend.HeadObject(context.Background(), fs.bucket, fp); err == nil {
			oldSize = obj.Size
		}
	}

	if err := fs.backend.DeleteObject(context.Background(), fs.bucket, fp); err != nil {
		// Try as directory marker
		if err := fs.backend.DeleteObject(context.Background(), fs.bucket, fp+dirMarkerSuffix); err != nil {
			return err
		}
	}
	fs.invalidateStatCache(fp)
	fs.invalidateParentDirCache(fp)

	if fs.volMgr != nil && oldSize > 0 {
		fs.volMgr.RecordFreedBytes(fs.volName, oldSize) //nolint:errcheck
	}
	return nil
}

// Join joins path elements.
func (fs *GrainVFS) Join(elem ...string) string {
	return path.Join(elem...)
}

// ReadDir reads a directory, using the dir cache if enabled.
func (fs *GrainVFS) ReadDir(dirPath string) ([]os.FileInfo, error) {
	fp := fs.fullPath(dirPath)

	// Check dir cache
	if cached := fs.getDirCache(fp); cached != nil {
		return cached, nil
	}

	prefix := ""
	if fp != "" {
		prefix = fp + "/"
	}

	objs, err := fs.backend.ListObjects(context.Background(), fs.bucket, prefix, 10000)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	seen := make(map[string]os.FileInfo)
	for _, obj := range objs {
		rel := strings.TrimPrefix(obj.Key, prefix)
		if rel == "" {
			continue
		}

		parts := strings.SplitN(rel, "/", 2)
		entryName := parts[0]

		if len(parts) > 1 {
			// It's a subdirectory
			if _, ok := seen[entryName]; !ok {
				seen[entryName] = &fileInfo{name: entryName, isDir: true, modTime: time.Now()}
			}
		} else {
			// It's a file (skip dir markers)
			if strings.HasSuffix(entryName, dirMarkerSuffix) || entryName == ".dir" {
				dirName := strings.TrimSuffix(entryName, dirMarkerSuffix)
				if dirName == "" {
					continue
				}
				seen[dirName] = &fileInfo{name: dirName, isDir: true, modTime: time.Now()}
				continue
			}
			seen[entryName] = &fileInfo{
				name:    entryName,
				size:    obj.Size,
				isDir:   false,
				modTime: time.Unix(obj.LastModified, 0),
			}
		}
	}

	result := make([]os.FileInfo, 0, len(seen))
	for _, fi := range seen {
		result = append(result, fi)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name() < result[j].Name()
	})

	fs.putDirCache(fp, result)
	return result, nil
}

// MkdirAll creates a directory and parents.
func (fs *GrainVFS) MkdirAll(dirPath string, perm os.FileMode) error {
	fp := fs.fullPath(dirPath)
	marker := fp + dirMarkerSuffix
	_, err := fs.backend.PutObject(context.Background(), fs.bucket, marker, strings.NewReader(""), "application/x-directory")
	return err
}

// TempFile creates a temp file.
func (fs *GrainVFS) TempFile(dir, prefix string) (billy.File, error) {
	name := fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	p := path.Join(dir, name)
	return fs.Create(p)
}

// Lstat returns file info (symlinks not supported, same as Stat).
func (fs *GrainVFS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

// Symlink is not supported.
func (fs *GrainVFS) Symlink(target, link string) error {
	return fmt.Errorf("symlinks not supported")
}

// Readlink is not supported.
func (fs *GrainVFS) Readlink(link string) (string, error) {
	return "", fmt.Errorf("symlinks not supported")
}

// Chroot returns a new filesystem rooted at the given path.
func (fs *GrainVFS) Chroot(dir string) (billy.Filesystem, error) {
	newRoot := fs.fullPath(dir)
	return &GrainVFS{
		backend: fs.backend,
		bucket:  fs.bucket,
		root:    newRoot,
	}, nil
}

// Root returns the root path.
func (fs *GrainVFS) Root() string {
	if fs.root == "" {
		return "/"
	}
	return "/" + fs.root
}

// billy.Change implementation

func (fs *GrainVFS) Chmod(name string, mode os.FileMode) error         { return nil }
func (fs *GrainVFS) Lchown(name string, uid, gid int) error            { return nil }
func (fs *GrainVFS) Chown(name string, uid, gid int) error             { return nil }
func (fs *GrainVFS) Chtimes(name string, atime, mtime time.Time) error { return nil }

func (fs *GrainVFS) isDir(fp string) bool {
	if fp == "" || fp == "/" || fp == "." {
		return true // root is always a directory
	}
	// Check for dir marker
	if _, err := fs.backend.HeadObject(context.Background(), fs.bucket, fp+dirMarkerSuffix); err == nil {
		return true
	}
	// Check if any objects have this as prefix (implicit directory)
	objs, err := fs.backend.ListObjects(context.Background(), fs.bucket, fp+"/", 1)
	if err == nil && len(objs) > 0 {
		return true
	}
	return false
}

// fileInfo implements os.FileInfo.
type fileInfo struct {
	name    string
	size    int64
	isDir   bool
	modTime time.Time
}

func (fi *fileInfo) Name() string      { return fi.name }
func (fi *fileInfo) Size() int64       { return fi.size }
func (fi *fileInfo) Mode() os.FileMode { return 0644 }
func (fi *fileInfo) ModTime() time.Time {
	if fi.modTime.IsZero() {
		return time.Now()
	}
	return fi.modTime
}
func (fi *fileInfo) IsDir() bool      { return fi.isDir }
func (fi *fileInfo) Sys() interface{} { return nil }

// grainFile implements billy.File.
type grainFile struct {
	fs      *GrainVFS
	path    string
	name    string
	flag    int
	perm    os.FileMode
	buf     *bytes.Buffer
	pos     int64
	closed  bool
	rc      io.ReadCloser // 스트리밍 모드: 읽기 전용 Open 시 설정, Seek/ReadAt/Close 시 해제
	oldSize int64         // backend size before this write session; 0 = new file or volMgr == nil
	mu      sync.Mutex    // guards rc and pos for concurrent ReadAt (io.ReaderAt contract)
}

func (f *grainFile) loadExisting() error {
	if f.rc != nil {
		f.rc.Close()
		f.rc = nil
	}
	rc, _, err := f.fs.backend.GetObject(context.Background(), f.fs.bucket, f.path)
	if err != nil {
		return os.ErrNotExist
	}
	putBuf(f.buf)
	f.buf = getBuf()
	_, err = f.buf.ReadFrom(rc)
	rc.Close()
	if err != nil {
		putBuf(f.buf)
		f.buf = nil
		return fmt.Errorf("read file: %w", err)
	}
	return nil
}

func (f *grainFile) Name() string { return f.name }

// Write honors f.pos: appends when pos==len, overlays when pos<len, and
// zero-pads + appends when pos>len. The previous version always appended
// to buf regardless of pos, which inflated random-write workloads to size
// proportional to write count rather than offset extent (NFS rand_write_4k
// hit OOM in Phase 2 measurement before this fix).
func (f *grainFile) Write(p []byte) (int, error) {
	if f.buf == nil {
		f.buf = getBuf()
	}
	cur := int64(f.buf.Len())
	switch {
	case f.pos == cur:
		// Sequential append — original fast path.
		n, err := f.buf.Write(p)
		f.pos += int64(n)
		return n, err
	case f.pos < cur:
		// Overlay write: mutate in-place up to current end, append remainder.
		dst := f.buf.Bytes() // slice into buf's internal storage
		end := f.pos + int64(len(p))
		if end <= cur {
			// Wholly within existing extent.
			n := copy(dst[f.pos:end], p)
			f.pos += int64(n)
			return n, nil
		}
		// Partial overlay + tail extend.
		head := int(cur - f.pos)
		copy(dst[f.pos:cur], p[:head])
		n2, err := f.buf.Write(p[head:])
		f.pos = cur + int64(n2)
		return head + n2, err
	default:
		// pos > cur: zero-pad gap, then append payload.
		gap := f.pos - cur
		// bytes.Buffer.Grow + zero fill via Write of zero slice.
		if gap > 0 {
			zeros := make([]byte, gap)
			if _, err := f.buf.Write(zeros); err != nil {
				return 0, err
			}
		}
		n, err := f.buf.Write(p)
		f.pos += int64(n)
		return n, err
	}
}

func (f *grainFile) Read(p []byte) (int, error) {
	if f.rc != nil {
		n, err := f.rc.Read(p)
		f.pos += int64(n)
		return n, err
	}
	if f.buf == nil {
		return 0, io.EOF
	}
	data := f.buf.Bytes()
	if f.pos >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *grainFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.rc != nil {
		if off == f.pos {
			// 순차 접근: rc에서 직접 읽어 GetObject 추가 호출 없이 스트리밍.
			// io.ReadFull 사용으로 단축 읽기 시 비-nil 에러 반환 (io.ReaderAt 계약).
			n, err := io.ReadFull(f.rc, p)
			f.pos += int64(n)
			if err == io.ErrUnexpectedEOF {
				return n, io.EOF
			}
			return n, err
		}
		// 랜덤 접근: buf 모드로 전환
		if err := f.loadExisting(); err != nil {
			return 0, err
		}
	}
	if f.buf == nil {
		return 0, io.EOF
	}
	data := f.buf.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *grainFile) Seek(offset int64, whence int) (int64, error) {
	if f.rc != nil {
		if err := f.loadExisting(); err != nil {
			return 0, err
		}
	}
	size := int64(0)
	if f.buf != nil {
		size = int64(f.buf.Len())
	}

	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = size + offset
	}

	if f.pos < 0 {
		f.pos = 0
	}
	return f.pos, nil
}

func (f *grainFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	if f.rc != nil {
		f.rc.Close()
		f.rc = nil
	}

	// Flush writes to storage
	if f.flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 && f.buf != nil {
		data := f.buf.Bytes()
		newSize := int64(len(data))
		_, err := f.fs.backend.PutObject(context.Background(), f.fs.bucket, f.path,
			bytes.NewReader(data), "application/octet-stream")
		putBuf(f.buf)
		f.buf = nil
		if err != nil {
			return err
		}
		if f.fs.volMgr != nil && f.oldSize > 0 && newSize < f.oldSize {
			f.fs.volMgr.RecordFreedBytes(f.fs.volName, f.oldSize-newSize) //nolint:errcheck
		}
		// Invalidate caches after write
		f.fs.invalidateStatCache(f.path)
		f.fs.invalidateParentDirCache(f.path)
		return nil
	}
	// O_RDONLY 파일이 buf 모드로 전환된 경우 pool에 반환
	if f.buf != nil {
		putBuf(f.buf)
		f.buf = nil
	}
	return nil
}

func (f *grainFile) Lock() error   { return nil }
func (f *grainFile) Unlock() error { return nil }

func (f *grainFile) Truncate(size int64) error {
	if f.buf == nil {
		f.buf = getBuf()
	}
	old := append([]byte(nil), f.buf.Bytes()...)
	if int64(len(old)) > size {
		old = old[:size]
	} else {
		old = append(old, make([]byte, int(size)-len(old))...)
	}
	f.buf.Reset()
	f.buf.Write(old)
	return nil
}

// --- Stat cache helpers ---

func (fs *GrainVFS) getStatCache(fp string) os.FileInfo {
	m := fs.statCache.Load()
	if m == nil {
		return nil
	}
	entry, ok := (*m)[fp]
	if !ok || time.Now().After(entry.expires) {
		return nil
	}
	metrics.CacheStatHits.Add(1)
	return entry.info
}

func (fs *GrainVFS) putStatCache(fp string, info os.FileInfo) {
	m := fs.statCache.Load()
	if m == nil {
		return
	}
	fs.statWriteMu.Lock()
	m = fs.statCache.Load()
	next := make(map[string]*statCacheEntry, len(*m)+1)
	for k, v := range *m {
		next[k] = v
	}
	next[fp] = &statCacheEntry{info: info, expires: time.Now().Add(fs.statCacheTTL)}
	fs.statCache.Store(&next)
	fs.statWriteMu.Unlock()
}

func (fs *GrainVFS) invalidateStatCache(fp string) {
	m := fs.statCache.Load()
	if m == nil {
		return
	}
	fs.statWriteMu.Lock()
	m = fs.statCache.Load()
	if _, ok := (*m)[fp]; !ok {
		fs.statWriteMu.Unlock()
		return
	}
	next := make(map[string]*statCacheEntry, len(*m))
	for k, v := range *m {
		next[k] = v
	}
	delete(next, fp)
	fs.statCache.Store(&next)
	fs.statWriteMu.Unlock()
}

// --- Dir cache helpers ---

func (fs *GrainVFS) getDirCache(fp string) []os.FileInfo {
	m := fs.dirCache.Load()
	if m == nil {
		return nil
	}
	entry, ok := (*m)[fp]
	if !ok || time.Now().After(entry.expires) {
		return nil
	}
	return entry.entries
}

func (fs *GrainVFS) putDirCache(fp string, entries []os.FileInfo) {
	m := fs.dirCache.Load()
	if m == nil {
		return
	}
	fs.dirWriteMu.Lock()
	m = fs.dirCache.Load()
	next := make(map[string]*dirCacheEntry, len(*m)+1)
	for k, v := range *m {
		next[k] = v
	}
	next[fp] = &dirCacheEntry{entries: entries, expires: time.Now().Add(fs.dirCacheTTL)}
	fs.dirCache.Store(&next)
	fs.dirWriteMu.Unlock()
}

// invalidateParentDirCache invalidates the dir cache for the parent directory
// of the given path, so subsequent ReadDir calls reflect the change.
func (fs *GrainVFS) invalidateParentDirCache(fp string) {
	m := fs.dirCache.Load()
	if m == nil {
		return
	}
	parent := path.Dir(fp)
	if parent == "." {
		parent = ""
	}
	fs.dirWriteMu.Lock()
	m = fs.dirCache.Load()
	if _, ok := (*m)[parent]; !ok {
		fs.dirWriteMu.Unlock()
		return
	}
	next := make(map[string]*dirCacheEntry, len(*m))
	for k, v := range *m {
		next[k] = v
	}
	delete(next, parent)
	fs.dirCache.Store(&next)
	fs.dirWriteMu.Unlock()
}

// MarkDeleted marks a file as deleted in the VFS deleted file tracking.
// This is used for NFS ESTALE error propagation - when S3 deletes a file,
// NFS should return ESTALE on subsequent access attempts.
//
// Called by Raft OnApply callback after DeleteObject commands commit.
// Stores deletion marker in stat cache (persistent in future with BadgerDB).
//
// PRODUCTION LIMITATION: Deleted markers are stored in-memory only.
// If GrainFS crashes, all deletion markers are lost, causing "zombie" files
// to reappear in NFS. Before production deployment, implement BadgerDB
// persistence for crash recovery. Tracked as P1 in project backlog.
//
// Parameters:
// - bucket: S3 bucket name (e.g., "default")
// - key: S3 object key (e.g., "path/to/file.txt")
//
// Note: Bucket is included in cache key to support multi-bucket VFS instances.
func (fs *GrainVFS) MarkDeleted(bucket, key string) error {
	m := fs.statCache.Load()
	if m == nil {
		return nil
	}

	cacheKey := "deleted:" + bucket + ":" + key
	fs.statWriteMu.Lock()
	m = fs.statCache.Load()
	_, exists := (*m)[cacheKey]
	next := make(map[string]*statCacheEntry, len(*m)+1)
	for k, v := range *m {
		next[k] = v
	}
	next[cacheKey] = &statCacheEntry{info: nil, expires: time.Now().Add(24 * time.Hour)}
	fs.statCache.Store(&next)
	fs.statWriteMu.Unlock()

	if !exists {
		metrics.DeletedMarkersTotal.Inc()
	}
	return nil
}

// IsDeleted checks if a file has been marked as deleted.
// Used by NFS handler to return ESTALE error.
//
// Parameters:
// - bucket: S3 bucket name (e.g., "default")
// - key: S3 object key (e.g., "path/to/file.txt")
//
// Returns true if file is marked as deleted and marker hasn't expired.
func (fs *GrainVFS) IsDeleted(bucket, key string) bool {
	m := fs.statCache.Load()
	if m == nil {
		return false
	}
	cacheKey := "deleted:" + bucket + ":" + key
	entry, ok := (*m)[cacheKey]
	if !ok {
		return false
	}
	return time.Now().Before(entry.expires)
}

// Invalidate clears caches for the given S3 bucket and key.
// This implements cluster.CacheInvalidator for cross-protocol cache coherency.
//
// Maps S3 bucket/key to VFS file path:
// - S3 bucket="default", key="path/to/file.txt"
// - VFS bucket="__grainfs_vfs_default"
// - VFS file path="path/to/file.txt"
//
// Called by DistributedBackend.onApply after Raft commits mutations.
func (fs *GrainVFS) Invalidate(bucket, key string) {
	start := time.Now()
	defer func() {
		metrics.CacheInvalidationDuration.Observe(time.Since(start).Seconds())
		metrics.CacheInvalidationTotal.WithLabelValues(bucket, "vfs").Add(1)
	}()

	// Check if this VFS instance serves the specified bucket
	if fs.bucket != storage.VFSBucketPrefix+bucket {
		return // Different bucket, not our concern
	}

	// CoW: remove deleted marker + stat entry atomically in one clone
	hadDeletedMarker := false
	m := fs.statCache.Load()
	if m != nil {
		deletedKey := "deleted:" + bucket + ":" + key
		fs.statWriteMu.Lock()
		m = fs.statCache.Load()
		_, hadDeletedMarker = (*m)[deletedKey]
		next := make(map[string]*statCacheEntry, len(*m))
		for k, v := range *m {
			next[k] = v
		}
		delete(next, deletedKey)
		delete(next, key)
		fs.statCache.Store(&next)
		fs.statWriteMu.Unlock()
	}

	if hadDeletedMarker {
		metrics.DeletedMarkersTotal.Dec()
	}

	// Invalidate parent directory cache (so ReadDir reflects changes)
	fs.invalidateParentDirCache(key)
}
