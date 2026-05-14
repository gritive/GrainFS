package p9server

import (
	"context"
	"io"
	"syscall"

	"github.com/hugelgupf/p9/p9"

	"github.com/gritive/GrainFS/internal/storage"
)

type objectFile struct {
	noopFile
	backend storage.Backend
	bucket  string
	key     string
	meta    *storage.Object
}

func (f *objectFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &objectFile{backend: f.backend, bucket: f.bucket, key: f.key, meta: f.meta}, nil
	}
	return nil, nil, syscall.ENOTDIR
}

func (f *objectFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	if mode.Mode() != p9.ReadOnly {
		return p9.QID{}, 0, syscall.EROFS
	}
	return p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, f.key)}, 0, nil
}

func (f *objectFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	qid := p9.QID{Type: p9.TypeRegular, Path: qidPath(f.bucket, f.key)}
	valid := p9.AttrMask{Mode: true, Size: true, MTime: true}
	attr := p9.Attr{
		Mode:             p9.ModeRegular | 0444,
		Size:             uint64(f.meta.Size),
		MTimeSeconds:     uint64(f.meta.LastModified),
		MTimeNanoSeconds: 0,
	}
	return qid, valid, attr, nil
}

func (f *objectFile) ReadAt(buf []byte, offset int64) (int, error) {
	ctx := context.Background()
	if pio, ok := f.backend.(storage.PartialIO); ok {
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
