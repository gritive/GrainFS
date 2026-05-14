package p9server

import (
	"context"
	"syscall"

	"github.com/hugelgupf/p9/p9"

	"github.com/gritive/GrainFS/internal/storage"
)

type bucketFile struct {
	noopFile
	backend storage.Backend
	bucket  string
}

func (f *bucketFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &bucketFile{backend: f.backend, bucket: f.bucket}, nil
	}
	key := names[0]
	obj, err := f.backend.HeadObject(context.Background(), f.bucket, key)
	if err != nil {
		return nil, nil, syscall.ENOENT
	}
	oqid := p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, key)}
	of := &objectFile{backend: f.backend, bucket: f.bucket, key: key, meta: obj}
	if len(names) == 1 {
		return []p9.QID{oqid}, of, nil
	}
	// objects are leaves — no subdirectories
	return []p9.QID{oqid}, nil, syscall.ENOTDIR
}

func (f *bucketFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	return p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket)}, 0, nil
}

func (f *bucketFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	qid := p9.QID{Type: p9.TypeDir, Path: qidPath(f.bucket)}
	valid := p9.AttrMask{Mode: true, NLink: true}
	attr := p9.Attr{Mode: p9.ModeDirectory | 0555, NLink: 2}
	return qid, valid, attr, nil
}

func (f *bucketFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	var out p9.Dirents
	idx := uint64(0)
	err := f.backend.WalkObjects(context.Background(), f.bucket, "", func(obj *storage.Object) error {
		if idx < offset {
			idx++
			return nil
		}
		if uint32(len(out)) >= count {
			return nil
		}
		out = append(out, p9.Dirent{
			QID:    p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, obj.Key)},
			Offset: idx + 1,
			Type:   p9.TypeRegular,
			Name:   obj.Key,
		})
		idx++
		return nil
	})
	if err != nil {
		return nil, syscall.EIO
	}
	return out, nil
}
