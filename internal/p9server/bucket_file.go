package p9server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"

	"github.com/gritive/GrainFS/internal/storage"
)

var errStopReaddir = errors.New("p9server: stop readdir")

type objectKeyWalker interface {
	WalkObjectKeys(ctx context.Context, bucket, prefix string, fn func(string) error) error
}

type bucketFile struct {
	noopFile
	backend storage.Backend
	locks   *objectLocks
	bucket  string
	prefix  string
}

func (f *bucketFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &bucketFile{backend: f.backend, locks: f.locks, bucket: f.bucket, prefix: f.prefix}, nil
	}
	name := names[0]
	if isP9ReservedName(name) {
		return nil, nil, syscall.ENOENT
	}
	key := f.childKey(name)
	if isP9ReservedKey(key) {
		return nil, nil, syscall.ENOENT
	}
	if len(names) == 1 {
		if obj, err := f.backend.HeadObject(context.Background(), f.bucket, key); err == nil {
			oqid := p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, key)}
			of := &objectFile{backend: f.backend, locks: f.locks, bucket: f.bucket, key: key, meta: obj}
			return []p9.QID{oqid}, of, nil
		}
	}
	if !f.hasPrefix(key + "/") {
		return nil, nil, syscall.ENOENT
	}
	bqid := p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket, key)}
	bf := &bucketFile{backend: f.backend, locks: f.locks, bucket: f.bucket, prefix: key + "/"}
	if len(names) == 1 {
		return []p9.QID{bqid}, bf, nil
	}
	qids, file, err := bf.Walk(names[1:])
	if err != nil {
		return append([]p9.QID{bqid}, qids...), nil, err
	}
	return append([]p9.QID{bqid}, qids...), file, nil
}

func (f *bucketFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	return p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket, f.prefix)}, 0, nil
}

func (f *bucketFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	qid := p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket, f.prefix)}
	valid := p9.AttrMask{Mode: true, NLink: true}
	mode := uint32(0755)
	if f.prefix != "" {
		mode = loadP9FileMeta(context.Background(), f.backend, f.bucket, dirMarkerKey(f.prefix)).Mode
	}
	attr := p9.Attr{Mode: p9.ModeDirectory | p9.FileMode(mode&0777), NLink: 2}
	return qid, valid, attr, nil
}

func (f *bucketFile) Create(name string, flags p9.OpenFlags, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.File, p9.QID, uint32, error) {
	if isP9ReservedName(name) {
		return nil, p9.QID{}, 0, syscall.EPERM
	}
	switch flags.Mode() {
	case p9.ReadOnly, p9.WriteOnly, p9.ReadWrite:
	default:
		return nil, p9.QID{}, 0, syscall.EINVAL
	}
	key := f.childKey(name)
	if isP9ReservedKey(key) {
		return nil, p9.QID{}, 0, syscall.EPERM
	}
	lockKeys := []objectLockKey{{bucket: f.bucket, key: key}}
	if parentKey, ok := f.parentDirLockKey(); ok {
		lockKeys = append(lockKeys, parentKey)
	}
	unlock := f.locks.lockMany(lockKeys...)
	defer unlock()

	ctx := context.Background()
	if _, err := f.backend.HeadObject(ctx, f.bucket, key); err == nil {
		return nil, p9.QID{}, 0, syscall.EEXIST
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, p9.QID{}, 0, syscall.EIO
	}
	if f.hasPrefix(dirMarkerKey(key)) {
		return nil, p9.QID{}, 0, syscall.EISDIR
	}
	obj, err := f.backend.PutObject(ctx, f.bucket, key, bytes.NewReader(nil), "application/octet-stream")
	if err != nil {
		return nil, p9.QID{}, 0, syscall.EIO
	}
	meta := p9FileMeta{
		Mode:  uint32(permissions) & 0777,
		Mtime: time.Now().UnixNano(),
	}
	if err := saveP9FileMeta(ctx, f.backend, f.bucket, key, meta); err != nil {
		_ = f.backend.DeleteObject(ctx, f.bucket, key)
		return nil, p9.QID{}, 0, syscall.EIO
	}
	qid := p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, key)}
	return &objectFile{backend: f.backend, locks: f.locks, bucket: f.bucket, key: key, meta: obj}, qid, 0, nil
}

func (f *bucketFile) Mkdir(name string, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.QID, error) {
	if !validObjectName(name) {
		return p9.QID{}, syscall.EINVAL
	}
	if isP9ReservedName(name) {
		return p9.QID{}, syscall.EPERM
	}
	key := f.childKey(name)
	if isP9ReservedKey(key) {
		return p9.QID{}, syscall.EPERM
	}
	markerKey := dirMarkerKey(key)
	lockKeys := []objectLockKey{
		objectLockKey{bucket: f.bucket, key: key},
		objectLockKey{bucket: f.bucket, key: markerKey},
	}
	if parentKey, ok := f.parentDirLockKey(); ok {
		lockKeys = append(lockKeys, parentKey)
	}
	unlock := f.locks.lockMany(lockKeys...)
	defer unlock()

	ctx := context.Background()
	if _, err := f.backend.HeadObject(ctx, f.bucket, key); err == nil {
		return p9.QID{}, syscall.EEXIST
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return p9.QID{}, syscall.EIO
	}
	if f.hasPrefix(markerKey) {
		return p9.QID{}, syscall.EEXIST
	}
	if _, err := f.backend.PutObject(ctx, f.bucket, markerKey, bytes.NewReader(nil), "application/x-directory"); err != nil {
		return p9.QID{}, syscall.EIO
	}
	meta := p9FileMeta{
		Mode:  uint32(permissions) & 0777,
		Mtime: time.Now().UnixNano(),
	}
	if err := saveP9FileMeta(ctx, f.backend, f.bucket, markerKey, meta); err != nil {
		_ = f.backend.DeleteObject(ctx, f.bucket, markerKey)
		return p9.QID{}, syscall.EIO
	}
	return p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket, key)}, nil
}

func (f *bucketFile) UnlinkAt(name string, flags uint32) error {
	const atRemovedir = 0x200
	if flags != 0 && flags != atRemovedir {
		return syscall.EINVAL
	}
	if !validObjectName(name) {
		return syscall.EINVAL
	}
	if isP9ReservedName(name) {
		return syscall.EPERM
	}
	key := f.childKey(name)
	if isP9ReservedKey(key) {
		return syscall.EPERM
	}
	if flags == atRemovedir {
		return f.rmdir(key)
	}
	lockKeys := []objectLockKey{
		objectLockKey{bucket: f.bucket, key: key},
		objectLockKey{bucket: f.bucket, key: p9MetaSidecarKey(key)},
	}
	if parentKey, ok := f.parentDirLockKey(); ok {
		lockKeys = append(lockKeys, parentKey)
	}
	unlock := f.locks.lockMany(lockKeys...)
	defer unlock()

	ctx := context.Background()
	if _, err := f.backend.HeadObject(ctx, f.bucket, key); err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}
	if err := f.backend.DeleteObject(ctx, f.bucket, key); err != nil {
		return syscall.EIO
	}
	_ = f.backend.DeleteObject(ctx, f.bucket, p9MetaSidecarKey(key))
	return nil
}

func (f *bucketFile) rmdir(key string) error {
	markerKey := dirMarkerKey(key)
	unlock := f.locks.lock(f.bucket, markerKey)
	defer unlock()

	ctx := context.Background()
	hasMarker := false
	if _, err := f.backend.HeadObject(ctx, f.bucket, markerKey); err == nil {
		hasMarker = true
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return syscall.EIO
	}

	hasChild := false
	walk := func(foundKey string) error {
		if foundKey == markerKey {
			return nil
		}
		if isP9ReservedKey(foundKey) {
			return nil
		}
		hasChild = true
		return errStopReaddir
	}
	var err error
	if walker, ok := f.backend.(objectKeyWalker); ok {
		err = walker.WalkObjectKeys(ctx, f.bucket, markerKey, walk)
	} else {
		err = f.backend.WalkObjects(ctx, f.bucket, markerKey, func(obj *storage.Object) error {
			return walk(obj.Key)
		})
	}
	if err != nil && !errors.Is(err, errStopReaddir) {
		return syscall.EIO
	}
	if hasChild {
		return syscall.ENOTEMPTY
	}
	if !hasMarker {
		return syscall.ENOENT
	}
	if err := f.backend.DeleteObject(ctx, f.bucket, markerKey); err != nil {
		return syscall.EIO
	}
	_ = f.backend.DeleteObject(ctx, f.bucket, p9MetaSidecarKey(markerKey))
	return nil
}

// RenameAt performs a best-effort object rename. Object stores do not provide
// crash-atomic rename, so this copies destination data and metadata before
// deleting the source.
func (f *bucketFile) RenameAt(oldName string, newDir p9.File, newName string) error {
	dst, ok := newDir.(*bucketFile)
	if !ok {
		return syscall.EXDEV
	}
	if dst.bucket != f.bucket {
		return syscall.EXDEV
	}
	if !validObjectName(oldName) || !validObjectName(newName) {
		return syscall.EINVAL
	}
	if isP9ReservedName(oldName) || isP9ReservedName(newName) {
		return syscall.EPERM
	}
	oldKey := f.childKey(oldName)
	newKey := dst.childKey(newName)
	if isP9ReservedKey(oldKey) || isP9ReservedKey(newKey) {
		return syscall.EPERM
	}
	lockKeys := []objectLockKey{
		objectLockKey{bucket: f.bucket, key: oldKey},
		objectLockKey{bucket: f.bucket, key: newKey},
		objectLockKey{bucket: f.bucket, key: p9MetaSidecarKey(oldKey)},
		objectLockKey{bucket: f.bucket, key: p9MetaSidecarKey(newKey)},
	}
	if parentKey, ok := f.parentDirLockKey(); ok {
		lockKeys = append(lockKeys, parentKey)
	}
	if parentKey, ok := dst.parentDirLockKey(); ok {
		lockKeys = append(lockKeys, parentKey)
	}
	unlock := f.locks.lockMany(lockKeys...)
	defer unlock()

	ctx := context.Background()
	obj, err := f.backend.HeadObject(ctx, f.bucket, oldKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}
	if obj.IsDeleteMarker {
		return syscall.ENOENT
	}
	if oldKey == newKey {
		return nil
	}
	if f.hasPrefix(dirMarkerKey(newKey)) {
		return syscall.EISDIR
	}
	if copier, ok := f.backend.(storage.Copier); ok {
		if _, err := copier.CopyObject(f.bucket, oldKey, f.bucket, newKey); err != nil {
			return syscall.EIO
		}
	} else {
		rc, _, err := f.backend.GetObject(ctx, f.bucket, oldKey)
		if err != nil {
			return syscall.EIO
		}
		if _, err := f.backend.PutObject(ctx, f.bucket, newKey, rc, obj.ContentType); err != nil {
			rc.Close()
			return syscall.EIO
		}
		rc.Close()
	}
	if err := saveP9FileMeta(ctx, f.backend, f.bucket, newKey, loadP9FileMeta(ctx, f.backend, f.bucket, oldKey)); err != nil {
		return syscall.EIO
	}
	if err := f.backend.DeleteObject(ctx, f.bucket, oldKey); err != nil {
		return syscall.EIO
	}
	_ = f.backend.DeleteObject(ctx, f.bucket, p9MetaSidecarKey(oldKey))
	return nil
}

func (f *bucketFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	if count == 0 {
		return nil, nil
	}
	out := make(p9.Dirents, 0, direntCap(count))
	idx := uint64(0)
	seen := make(map[string]struct{})
	emit := func(key string) error {
		if isP9ReservedKey(key) {
			return nil
		}
		rest := strings.TrimPrefix(key, f.prefix)
		if rest == "" {
			return nil
		}
		name, typ, qidKey := direntForRest(f.prefix, rest)
		if _, ok := seen[name]; ok {
			return nil
		}
		seen[name] = struct{}{}
		if idx < offset {
			idx++
			return nil
		}
		if uint32(len(out)) >= count {
			return errStopReaddir
		}
		out = append(out, p9.Dirent{
			QID:    p9.QID{Type: typ, Path: qidPath(f.bucket, qidKey)},
			Offset: idx + 1,
			Type:   typ,
			Name:   name,
		})
		idx++
		if uint32(len(out)) >= count {
			return errStopReaddir
		}
		return nil
	}
	if walker, ok := f.backend.(objectKeyWalker); ok {
		err := walker.WalkObjectKeys(context.Background(), f.bucket, f.prefix, emit)
		if errors.Is(err, errStopReaddir) {
			return out, nil
		}
		if err != nil {
			return nil, syscall.EIO
		}
		return out, nil
	}
	err := f.backend.WalkObjects(context.Background(), f.bucket, f.prefix, func(obj *storage.Object) error {
		return emit(obj.Key)
	})
	if errors.Is(err, errStopReaddir) {
		return out, nil
	}
	if err != nil {
		return nil, syscall.EIO
	}
	return out, nil
}

func (f *bucketFile) childKey(name string) string {
	return f.prefix + name
}

func (f *bucketFile) parentDirLockKey() (objectLockKey, bool) {
	if f.prefix == "" {
		return objectLockKey{}, false
	}
	return objectLockKey{bucket: f.bucket, key: dirMarkerKey(f.prefix)}, true
}

func validObjectName(name string) bool {
	return name != "" && !strings.Contains(name, "/")
}

func dirMarkerKey(key string) string {
	return strings.TrimSuffix(key, "/") + "/"
}

func (f *bucketFile) hasPrefix(prefix string) bool {
	found := false
	walk := func(key string) error {
		if isP9ReservedKey(key) {
			return nil
		}
		found = true
		return errStopReaddir
	}
	var err error
	if walker, ok := f.backend.(objectKeyWalker); ok {
		err = walker.WalkObjectKeys(context.Background(), f.bucket, prefix, walk)
	} else {
		err = f.backend.WalkObjects(context.Background(), f.bucket, prefix, func(obj *storage.Object) error {
			return walk(obj.Key)
		})
	}
	return found && (err == nil || errors.Is(err, errStopReaddir))
}

func direntForRest(prefix, rest string) (name string, typ p9.QIDType, qidKey string) {
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		name = rest[:slash]
		return name, p9.TypeDir, prefix + name
	}
	return rest, p9.TypeRegular, prefix + rest
}

func direntCap(count uint32) int {
	const maxPrealloc = 1024
	if count > maxPrealloc {
		return maxPrealloc
	}
	return int(count)
}
