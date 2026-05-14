package p9server

import (
	"context"
	"errors"
	"strings"
	"syscall"

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
	bucket  string
	prefix  string
}

func (f *bucketFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &bucketFile{backend: f.backend, bucket: f.bucket, prefix: f.prefix}, nil
	}
	name := names[0]
	key := f.childKey(name)
	if len(names) == 1 {
		if obj, err := f.backend.HeadObject(context.Background(), f.bucket, key); err == nil {
			oqid := p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, key)}
			of := &objectFile{backend: f.backend, bucket: f.bucket, key: key, meta: obj}
			return []p9.QID{oqid}, of, nil
		}
	}
	if !f.hasPrefix(key + "/") {
		return nil, nil, syscall.ENOENT
	}
	bqid := p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket, key)}
	bf := &bucketFile{backend: f.backend, bucket: f.bucket, prefix: key + "/"}
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
	attr := p9.Attr{Mode: p9.ModeDirectory | 0555, NLink: 2}
	return qid, valid, attr, nil
}

func (f *bucketFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	if count == 0 {
		return nil, nil
	}
	out := make(p9.Dirents, 0, direntCap(count))
	idx := uint64(0)
	seen := make(map[string]struct{})
	emit := func(key string) error {
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

func (f *bucketFile) hasPrefix(prefix string) bool {
	found := false
	walk := func(key string) error {
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
