package cluster

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

const s3MultipartMinNonFinalPartSize = int64(5 << 20)

type multipartCompleteManifest struct {
	UploadID   string
	Parts      []storage.MultipartPartEntry
	TotalSize  int64
	openPartFn func(partNumber int) (io.ReadCloser, error)
}

func (m multipartCompleteManifest) Open() (io.ReadCloser, error) {
	if m.openPartFn == nil {
		return nil, fmtInvalidPart("missing part opener")
	}
	return &multipartCompleteReader{manifest: m}, nil
}

type multipartCompleteReader struct {
	manifest multipartCompleteManifest
	idx      int
	current  io.ReadCloser
	closed   bool
}

func (r *multipartCompleteReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	for {
		if r.current == nil {
			if r.idx >= len(r.manifest.Parts) {
				return 0, io.EOF
			}
			part := r.manifest.Parts[r.idx]
			rc, err := r.manifest.openPartFn(part.PartNumber)
			if err != nil {
				return 0, fmt.Errorf("open part %d: %w", part.PartNumber, err)
			}
			r.current = rc
		}
		n, err := r.current.Read(p)
		if err == io.EOF {
			if closeErr := r.current.Close(); closeErr != nil {
				r.current = nil
				r.idx++
				if n > 0 {
					return n, nil
				}
				return 0, fmt.Errorf("close part %d: %w", r.manifest.Parts[r.idx-1].PartNumber, closeErr)
			}
			r.current = nil
			r.idx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (r *multipartCompleteReader) Close() error {
	r.closed = true
	if r.current == nil {
		return nil
	}
	err := r.current.Close()
	r.current = nil
	return err
}

func (b *DistributedBackend) buildMultipartCompleteManifest(uploadID string, requested []storage.Part) (multipartCompleteManifest, error) {
	parts := append([]storage.Part(nil), requested...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	if len(parts) == 0 {
		return multipartCompleteManifest{}, fmtInvalidPart("complete request has no parts")
	}

	out := make([]storage.MultipartPartEntry, 0, len(parts))
	var total int64
	prev := 0
	for i, p := range parts {
		if p.PartNumber <= 0 {
			return multipartCompleteManifest{}, fmtInvalidPart(fmt.Sprintf("invalid part number %d", p.PartNumber))
		}
		if p.PartNumber == prev {
			return multipartCompleteManifest{}, fmtInvalidPart(fmt.Sprintf("duplicate part number %d", p.PartNumber))
		}
		prev = p.PartNumber

		size, etag, err := b.hashMultipartPart(uploadID, p.PartNumber)
		if err != nil {
			return multipartCompleteManifest{}, fmtInvalidPart(fmt.Sprintf("part %d unavailable: %v", p.PartNumber, err))
		}
		if etag != trimCompletePartETag(p.ETag) {
			return multipartCompleteManifest{}, fmtInvalidPart(fmt.Sprintf("part %d etag mismatch", p.PartNumber))
		}
		if i < len(parts)-1 && size < s3MultipartMinNonFinalPartSize {
			return multipartCompleteManifest{}, fmtInvalidPart(fmt.Sprintf("part %d below 5 MiB minimum", p.PartNumber))
		}
		total += size
		out = append(out, storage.MultipartPartEntry{
			PartNumber: p.PartNumber,
			Size:       size,
			ETag:       etag,
		})
	}

	return multipartCompleteManifest{
		UploadID:  uploadID,
		Parts:     out,
		TotalSize: total,
		openPartFn: func(partNumber int) (io.ReadCloser, error) {
			return b.openMultipartPart(uploadID, partNumber)
		},
	}, nil
}

func (b *DistributedBackend) hashMultipartPart(uploadID string, partNumber int) (int64, string, error) {
	rc, err := b.openMultipartPart(uploadID, partNumber)
	if err != nil {
		return 0, "", err
	}
	defer rc.Close()
	h := md5.New()
	n, err := io.Copy(h, rc)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func trimCompletePartETag(etag string) string {
	return strings.Trim(etag, `"`)
}

func fmtInvalidPart(msg string) error {
	return fmt.Errorf("%w: %s", storage.ErrInvalidPart, msg)
}
