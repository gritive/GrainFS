package p9server

import (
	"context"
	"hash/fnv"
	"syscall"

	"github.com/hugelgupf/p9/p9"

	"github.com/gritive/GrainFS/internal/storage"
)

type rootFile struct {
	noopFile
	backend storage.Backend
}

func (f *rootFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &rootFile{backend: f.backend}, nil
	}
	bucket := names[0]
	if err := f.backend.HeadBucket(context.Background(), bucket); err != nil {
		return nil, nil, syscall.ENOENT
	}
	bqid := p9.QID{Type: p9.TypeDir, Path: qidPath(bucket)}
	bf := &bucketFile{backend: f.backend, bucket: bucket}
	if len(names) == 1 {
		return []p9.QID{bqid}, bf, nil
	}
	qids, file, err := bf.Walk(names[1:])
	if err != nil {
		return append([]p9.QID{bqid}, qids...), nil, err
	}
	return append([]p9.QID{bqid}, qids...), file, nil
}

func (f *rootFile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	return p9.QID{Type: p9.TypeDir, Path: 0}, 0, nil
}

func (f *rootFile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	qid := p9.QID{Type: p9.TypeDir, Path: 0}
	valid := p9.AttrMask{Mode: true, NLink: true}
	attr := p9.Attr{Mode: p9.ModeDirectory | 0555, NLink: 2}
	return qid, valid, attr, nil
}

func (f *rootFile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	buckets, err := f.backend.ListBuckets(context.Background())
	if err != nil {
		return nil, syscall.EIO
	}
	var out p9.Dirents
	for i, name := range buckets {
		if uint64(i) < offset {
			continue
		}
		if uint32(len(out)) >= count {
			break
		}
		out = append(out, p9.Dirent{
			QID:    p9.QID{Type: p9.TypeDir, Path: qidPath(name)},
			Offset: uint64(i + 1),
			Type:   p9.TypeDir,
			Name:   name,
		})
	}
	return out, nil
}

func (f *rootFile) StatFS() (p9.FSStat, error) {
	return p9.FSStat{
		Type:       0x01021997, // v9fs magic
		BlockSize:  4096,
		NameLength: 255,
	}, nil
}

// qidPath computes a stable uint64 QID path from path components.
func qidPath(parts ...string) uint64 {
	h := fnv.New64a()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{':'})
		}
		h.Write([]byte(p))
	}
	return h.Sum64()
}

// bucketFile stub — full implementation in Task 3 (bucket_file.go).
// Needed here so root_file.go compiles before Task 3 is executed.
type bucketFile struct {
	noopFile
	backend storage.Backend
	bucket  string
}
