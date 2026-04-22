// Package vfs implements a billy.Filesystem backed by GrainFS object storage.
// Files and directories are stored as objects in a dedicated bucket per volume.
package vfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

const (
	vfsBucketPrefix = "__grainfs_vfs_"
	dirMarkerSuffix = "/.dir"
)

// GrainVFS implements billy.Filesystem on top of a storage.Backend.
type GrainVFS struct {
	backend storage.Backend
	bucket  string
	root    string // chroot prefix
	mu      sync.RWMutex

	// Caching
	statCacheTTL time.Duration
	dirCacheTTL  time.Duration
	statCache    map[string]*statCacheEntry
	dirCache     map[string]*dirCacheEntry
	cacheMu      sync.RWMutex
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

var (
	_ billy.Filesystem = (*GrainVFS)(nil)
	_ billy.Change     = (*GrainVFS)(nil)
)

// New creates a new GrainVFS for the given volume name.
func New(backend storage.Backend, volumeName string, opts ...VFSOption) (*GrainVFS, error) {
	bucket := vfsBucketPrefix + volumeName
	_ = backend.CreateBucket(bucket)
	if err := backend.HeadBucket(bucket); err != nil {
		return nil, fmt.Errorf("ensure vfs bucket: %w", err)
	}
	fs := &GrainVFS{backend: backend, bucket: bucket, root: ""}
	for _, o := range opts {
		o(fs)
	}
	if fs.statCacheTTL > 0 {
		fs.statCache = make(map[string]*statCacheEntry)
	}
	if fs.dirCacheTTL > 0 {
		fs.dirCache = make(map[string]*dirCacheEntry)
	}
	return fs, nil
}

// NewDirect creates a new GrainVFS that uses the bucket name directly (without prefix).
// This is used for cross-protocol access where VFS needs to access S3 buckets directly.
func NewDirect(backend storage.Backend, bucketName string, opts ...VFSOption) (*GrainVFS, error) {
	_ = backend.CreateBucket(bucketName)
	if err := backend.HeadBucket(bucketName); err != nil {
		return nil, fmt.Errorf("ensure bucket: %w", err)
	}
	fs := &GrainVFS{backend: backend, bucket: bucketName, root: ""}
	for _, o := range opts {
		o(fs)
	}
	if fs.statCacheTTL > 0 {
		fs.statCache = make(map[string]*statCacheEntry)
	}
	if fs.dirCacheTTL > 0 {
		fs.dirCache = make(map[string]*dirCacheEntry)
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
			f.buf = &bytes.Buffer{}
		} else {
			// Try to load existing
			f.loadExisting()
		}
		return f, nil
	}

	// Must exist
	if err := f.loadExisting(); err != nil {
		return nil, err
	}
	return f, nil
}

// Stat returns file info, using the stat cache if enabled.
func (fs *GrainVFS) Stat(filename string) (os.FileInfo, error) {
	fp := fs.fullPath(filename)

	// Check if file was deleted (ESTALE support for cross-protocol coherency)
	// Extract volume name from bucket: "__grainfs_vfs_" + volumeName
	volName := strings.TrimPrefix(fs.bucket, vfsBucketPrefix)
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
	obj, err := fs.backend.HeadObject(fs.bucket, fp)
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

	// Read old file
	rc, _, err := fs.backend.GetObject(fs.bucket, oldFP)
	if err != nil {
		return os.ErrNotExist
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read old file: %w", err)
	}

	// Write to new location
	if _, err := fs.backend.PutObject(fs.bucket, newFP, bytes.NewReader(data), "application/octet-stream"); err != nil {
		return fmt.Errorf("write new file: %w", err)
	}

	// Delete old
	if err := fs.backend.DeleteObject(fs.bucket, oldFP); err != nil {
		return err
	}

	// Invalidate caches
	fs.invalidateStatCache(oldFP)
	fs.invalidateStatCache(newFP)
	fs.invalidateParentDirCache(oldFP)
	fs.invalidateParentDirCache(newFP)
	return nil
}

// Remove removes a file or empty directory.
func (fs *GrainVFS) Remove(filename string) error {
	fp := fs.fullPath(filename)
	// Try as file first
	if err := fs.backend.DeleteObject(fs.bucket, fp); err != nil {
		// Try as directory marker
		if err := fs.backend.DeleteObject(fs.bucket, fp+dirMarkerSuffix); err != nil {
			return err
		}
	}
	fs.invalidateStatCache(fp)
	fs.invalidateParentDirCache(fp)
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

	objs, err := fs.backend.ListObjects(fs.bucket, prefix, 10000)
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
	_, err := fs.backend.PutObject(fs.bucket, marker, strings.NewReader(""), "application/x-directory")
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
	if _, err := fs.backend.HeadObject(fs.bucket, fp+dirMarkerSuffix); err == nil {
		return true
	}
	// Check if any objects have this as prefix (implicit directory)
	objs, err := fs.backend.ListObjects(fs.bucket, fp+"/", 1)
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
	fs     *GrainVFS
	path   string
	name   string
	flag   int
	perm   os.FileMode
	buf    *bytes.Buffer
	pos    int64
	closed bool
}

func (f *grainFile) loadExisting() error {
	rc, _, err := f.fs.backend.GetObject(f.fs.bucket, f.path)
	if err != nil {
		return os.ErrNotExist
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	f.buf = bytes.NewBuffer(data)
	return nil
}

func (f *grainFile) Name() string { return f.name }

func (f *grainFile) Write(p []byte) (int, error) {
	if f.buf == nil {
		f.buf = &bytes.Buffer{}
	}
	return f.buf.Write(p)
}

func (f *grainFile) Read(p []byte) (int, error) {
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

	// Flush writes to storage
	if f.flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 && f.buf != nil {
		data := f.buf.Bytes()
		_, err := f.fs.backend.PutObject(f.fs.bucket, f.path,
			bytes.NewReader(data), "application/octet-stream")
		if err != nil {
			return err
		}
		// Invalidate caches after write
		f.fs.invalidateStatCache(f.path)
		f.fs.invalidateParentDirCache(f.path)
		return nil
	}
	return nil
}

func (f *grainFile) Lock() error   { return nil }
func (f *grainFile) Unlock() error { return nil }

func (f *grainFile) Truncate(size int64) error {
	if f.buf == nil {
		f.buf = &bytes.Buffer{}
	}
	data := f.buf.Bytes()
	if int64(len(data)) > size {
		data = data[:size]
	} else {
		for int64(len(data)) < size {
			data = append(data, 0)
		}
	}
	f.buf = bytes.NewBuffer(data)
	return nil
}

// --- Stat cache helpers ---

func (fs *GrainVFS) getStatCache(fp string) os.FileInfo {
	if fs.statCache == nil {
		return nil
	}
	fs.cacheMu.RLock()
	entry, ok := fs.statCache[fp]
	fs.cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expires) {
		return nil
	}

	metrics.CacheStatHits.Add(1)
	return entry.info
}

func (fs *GrainVFS) putStatCache(fp string, info os.FileInfo) {
	if fs.statCache == nil {
		return
	}
	fs.cacheMu.Lock()
	fs.statCache[fp] = &statCacheEntry{info: info, expires: time.Now().Add(fs.statCacheTTL)}
	fs.cacheMu.Unlock()
}

func (fs *GrainVFS) invalidateStatCache(fp string) {
	if fs.statCache == nil {
		return
	}
	fs.cacheMu.Lock()
	delete(fs.statCache, fp)
	fs.cacheMu.Unlock()
}

// --- Dir cache helpers ---

func (fs *GrainVFS) getDirCache(fp string) []os.FileInfo {
	if fs.dirCache == nil {
		return nil
	}
	fs.cacheMu.RLock()
	entry, ok := fs.dirCache[fp]
	fs.cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expires) {
		return nil
	}
	return entry.entries
}

func (fs *GrainVFS) putDirCache(fp string, entries []os.FileInfo) {
	if fs.dirCache == nil {
		return
	}
	fs.cacheMu.Lock()
	fs.dirCache[fp] = &dirCacheEntry{entries: entries, expires: time.Now().Add(fs.dirCacheTTL)}
	fs.cacheMu.Unlock()
}

// invalidateParentDirCache invalidates the dir cache for the parent directory
// of the given path, so subsequent ReadDir calls reflect the change.
func (fs *GrainVFS) invalidateParentDirCache(fp string) {
	if fs.dirCache == nil {
		return
	}
	parent := path.Dir(fp)
	if parent == "." {
		parent = ""
	}
	fs.cacheMu.Lock()
	delete(fs.dirCache, parent)
	fs.cacheMu.Unlock()
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
	fs.cacheMu.Lock()
	defer fs.cacheMu.Unlock()

	if fs.statCache == nil {
		return nil // No cache, nothing to mark
	}

	// Store deleted marker with bucket+key as cache key
	cacheKey := "deleted:" + bucket + ":" + key
	_, exists := fs.statCache[cacheKey]
	fs.statCache[cacheKey] = &statCacheEntry{
		info:    nil,
		expires: time.Now().Add(24 * time.Hour), // Persist for 24h
	}

	// Update metrics
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
	fs.cacheMu.RLock()
	defer fs.cacheMu.RUnlock()

	if fs.statCache == nil {
		return false
	}

	cacheKey := "deleted:" + bucket + ":" + key
	entry, ok := fs.statCache[cacheKey]
	if !ok {
		return false
	}

	// Check if entry expired
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
	if fs.bucket != vfsBucketPrefix+bucket {
		return // Different bucket, not our concern
	}

	// Clear deleted marker (file re-created after deletion)
	fs.cacheMu.Lock()
	_, hadDeletedMarker := fs.statCache["deleted:"+bucket+":"+key]
	delete(fs.statCache, "deleted:"+bucket+":"+key)
	fs.cacheMu.Unlock()

	// Update metrics: decrement deleted marker count if we cleared one
	if hadDeletedMarker {
		metrics.DeletedMarkersTotal.Dec()
	}

	// Invalidate stat cache for the specific file
	fs.invalidateStatCache(key)

	// Invalidate parent directory cache (so ReadDir reflects changes)
	fs.invalidateParentDirCache(key)
}
