package p9server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/fsmeta"
	"github.com/gritive/GrainFS/internal/storage"
)

type objectFile struct {
	noopFile
	backend storage.Backend
	locks   *objectLocks
	bucket  string
	key     string
	meta    *storage.Object

	dirtyLoaded bool
	dirty       bool
	dirtyData   []byte
}

const maxFallbackObjectSize = 64 << 20

func (f *objectFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &objectFile{backend: f.backend, locks: f.locks, bucket: f.bucket, key: f.key, meta: f.meta}, nil
	}
	return nil, nil, syscall.ENOTDIR
}

func (f *objectFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	switch mode.Mode() {
	case p9.ReadOnly, p9.WriteOnly, p9.ReadWrite:
	default:
		return p9.QID{}, 0, syscall.EINVAL
	}
	if isP9ReservedKey(f.key) {
		return p9.QID{}, 0, syscall.EPERM
	}
	typ := p9.TypeRegular
	if loadP9FileMeta(context.Background(), f.backend, f.bucket, f.key).IsSymlink() {
		typ = p9.TypeSymlink
	}
	return p9.QID{Type: typ, Path: qidPath(f.bucket, f.key)}, 0, nil
}

func (f *objectFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	fileMeta := loadP9FileMeta(context.Background(), f.backend, f.bucket, f.key)
	qidType := p9.TypeRegular
	modeType := p9.ModeRegular
	if fileMeta.IsSymlink() {
		qidType = p9.TypeSymlink
		modeType = p9.ModeSymlink
	}
	qid := p9.QID{Type: qidType, Path: qidPath(f.bucket, f.key)}
	if f.dirtyLoaded {
		valid := p9.AttrMask{Mode: true, Size: true, MTime: true}
		size := uint64(len(f.dirtyData))
		if fileMeta.IsSymlink() {
			size = uint64(len(fileMeta.Target))
		}
		attr := p9.Attr{
			Mode: modeType | p9.FileMode(fileMeta.Mode&0777),
			Size: size,
		}
		return qid, valid, attr, nil
	}
	obj, err := f.backend.HeadObject(context.Background(), f.bucket, f.key)
	if err != nil {
		return qid, p9.AttrMask{}, p9.Attr{}, syscall.ENOENT
	}
	f.meta = obj
	mtimeSec := uint64(obj.LastModified)
	mtimeNsec := uint64(0)
	if fileMeta.Mtime != 0 {
		mtime := time.Unix(0, fileMeta.Mtime)
		mtimeSec = uint64(mtime.Unix())
		mtimeNsec = uint64(mtime.Nanosecond())
	}
	valid := p9.AttrMask{Mode: true, Size: true, MTime: true}
	size := uint64(obj.Size)
	if fileMeta.IsSymlink() {
		size = uint64(len(fileMeta.Target))
	}
	attr := p9.Attr{
		Mode:             modeType | p9.FileMode(fileMeta.Mode&0777),
		Size:             size,
		MTimeSeconds:     mtimeSec,
		MTimeNanoSeconds: mtimeNsec,
	}
	return qid, valid, attr, nil
}

func (f *objectFile) SetAttr(valid p9.SetAttrMask, attr p9.SetAttr) error {
	if isP9ReservedKey(f.key) {
		return syscall.EPERM
	}
	unlock := f.locks.lock(f.bucket, f.key)
	defer unlock()

	ctx := context.Background()
	if valid.Size {
		if attr.Size > uint64(math.MaxInt64) {
			return syscall.EFBIG
		}
		if err := f.resize(ctx, int64(attr.Size)); err != nil {
			if errors.Is(err, storage.ErrObjectNotFound) {
				return syscall.ENOENT
			}
			if errors.Is(err, syscall.EFBIG) {
				return syscall.EFBIG
			}
			return syscall.EIO
		}
	}

	meta := loadP9FileMeta(ctx, f.backend, f.bucket, f.key)
	changed := false
	if valid.Permissions {
		meta.Mode = uint32(attr.Permissions) & 0777
		changed = true
	}
	if valid.MTime {
		if valid.MTimeNotSystemTime {
			meta.Mtime = time.Unix(int64(attr.MTimeSeconds), int64(attr.MTimeNanoSeconds)).UnixNano()
		} else {
			meta.Mtime = time.Now().UnixNano()
		}
		changed = true
	}
	if changed {
		if err := saveP9FileMeta(ctx, f.backend, f.bucket, f.key, meta); err != nil {
			return syscall.EIO
		}
	}
	if obj, err := f.backend.HeadObject(ctx, f.bucket, f.key); err == nil {
		f.meta = obj
	}
	return nil
}

func (f *objectFile) resize(ctx context.Context, size int64) error {
	if tr, ok := truncatableBackend(f.backend); ok && preferWriteAt(f.backend, f.bucket) {
		return tr.Truncate(ctx, f.bucket, f.key, size)
	}
	if size > maxFallbackObjectSize {
		return syscall.EFBIG
	}
	obj, err := f.backend.HeadObject(ctx, f.bucket, f.key)
	if err != nil {
		return err
	}
	if obj.Size > maxFallbackObjectSize {
		return syscall.EFBIG
	}
	rc, _, err := f.backend.GetObject(ctx, f.bucket, f.key)
	if err != nil {
		return err
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, maxFallbackObjectSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxFallbackObjectSize {
		return syscall.EFBIG
	}
	if int64(len(data)) > size {
		data = data[:size]
	} else if int64(len(data)) < size {
		data = append(data, make([]byte, size-int64(len(data)))...)
	}
	_, err = f.backend.PutObject(ctx, f.bucket, f.key, bytes.NewReader(data), "application/octet-stream")
	return err
}

func (f *objectFile) ReadAt(buf []byte, offset int64) (int, error) {
	if f.dirtyLoaded {
		return bytes.NewReader(f.dirtyData).ReadAt(buf, offset)
	}
	ctx := context.Background()
	if pio, ok := partialIOBackend(f.backend); ok && preferReadAt(f.backend, f.bucket) {
		return pio.ReadAt(ctx, f.bucket, f.key, offset, buf)
	}
	rc, _, err := f.backend.GetObject(ctx, f.bucket, f.key)
	if err != nil {
		return 0, syscall.EIO
	}
	defer rc.Close()
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 0, syscall.EIO
		}
	}
	n, err := io.ReadFull(rc, buf)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	return n, err
}

func (f *objectFile) Readlink() (string, error) {
	meta, err := fsmeta.LoadStrict(context.Background(), f.backend, f.bucket, f.key)
	if err != nil {
		return "", syscall.EIO
	}
	if !meta.IsSymlink() {
		return "", syscall.EINVAL
	}
	return meta.Target, nil
}

func (f *objectFile) WriteAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	if isP9ReservedKey(f.key) {
		return 0, syscall.EPERM
	}
	unlock := f.locks.lock(f.bucket, f.key)
	defer unlock()

	ctx := context.Background()
	if pio, ok := partialIOBackend(f.backend); ok && preferWriteAt(f.backend, f.bucket) {
		obj, err := pio.WriteAt(ctx, f.bucket, f.key, uint64(offset), buf)
		if err != nil {
			return 0, syscall.EIO
		}
		f.meta = obj
		return len(buf), nil
	}

	end := offset + int64(len(buf))
	if end < offset {
		return 0, syscall.EFBIG
	}
	if end > maxFallbackObjectSize {
		return 0, syscall.EFBIG
	}
	if offset == 0 && !f.dirtyLoaded && f.meta != nil && f.meta.Size == 0 {
		f.dirtyLoaded = true
		f.dirtyData = nil
	} else {
		if err := f.loadDirty(ctx, offset == 0); err != nil {
			if errors.Is(err, syscall.EFBIG) {
				return 0, syscall.EFBIG
			}
			return 0, syscall.EIO
		}
	}
	if int64(len(f.dirtyData)) < end {
		f.dirtyData = append(f.dirtyData, make([]byte, end-int64(len(f.dirtyData)))...)
	}
	copy(f.dirtyData[offset:end], buf)
	f.dirty = true
	if isRecoveryWriteGate(f.backend) {
		if err := f.flush(ctx); err != nil {
			return 0, syscall.EIO
		}
	}
	return len(buf), nil
}

func (f *objectFile) loadDirty(ctx context.Context, allowMissing bool) error {
	if f.dirtyLoaded {
		return nil
	}
	obj, err := f.backend.HeadObject(ctx, f.bucket, f.key)
	if err != nil {
		if allowMissing && errors.Is(err, storage.ErrObjectNotFound) {
			f.dirtyLoaded = true
			f.dirtyData = nil
			return nil
		}
		return err
	}
	if obj.Size > maxFallbackObjectSize {
		return syscall.EFBIG
	}
	rc, _, err := f.backend.GetObject(ctx, f.bucket, f.key)
	if err != nil {
		return err
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, maxFallbackObjectSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxFallbackObjectSize {
		return syscall.EFBIG
	}
	f.dirtyLoaded = true
	f.dirtyData = data
	return nil
}

func isRecoveryWriteGate(backend storage.Backend) bool {
	for backend != nil {
		if _, ok := backend.(*storage.RecoveryWriteGate); ok {
			return true
		}
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			return false
		}
		next := unwrapper.Unwrap()
		if next == backend {
			return false
		}
		backend = next
	}
	return false
}

func (f *objectFile) flush(ctx context.Context) error {
	if !f.dirty {
		return nil
	}
	obj, err := f.backend.PutObject(ctx, f.bucket, f.key, bytes.NewReader(f.dirtyData), "application/octet-stream")
	if err != nil {
		log.Warn().Err(err).Str("bucket", f.bucket).Str("key", f.key).Int("bytes", len(f.dirtyData)).Msg("9p flush: put object failed")
		return err
	}
	f.meta = obj
	f.dirty = false
	return nil
}

func (f *objectFile) FSync() error {
	unlock := f.locks.lock(f.bucket, f.key)
	defer unlock()
	if err := f.flush(context.Background()); err != nil {
		return syscall.EIO
	}
	if syncable, ok := f.backend.(storage.Syncable); ok {
		if err := syncable.Sync(f.bucket, f.key); err != nil {
			return syscall.EIO
		}
	}
	return nil
}

func (f *objectFile) Close() error {
	unlock := f.locks.lock(f.bucket, f.key)
	defer unlock()
	if err := f.flush(context.Background()); err != nil {
		return syscall.EIO
	}
	return nil
}
