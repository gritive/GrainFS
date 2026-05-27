package storage

import (
	"fmt"
	"io"
	"os"
)

// SegmentedReader stitches multiple segment blobs into a single byte stream,
// supporting Range reads. Opens each segment lazily as the read offset advances.
// For encrypted backends, each segment is decrypted independently (its own
// XAES-256-GCM nonce lives in the blob header).
type SegmentedReader struct {
	backend  *LocalBackend
	bucket   string
	key      string
	segments []SegmentRef
	starts   []int64 // prefix-sum: starts[i] = sum(segments[0..i-1].Size)
	start    int64   // absolute start offset of the requested range
	end      int64   // absolute end offset (inclusive)
	pos      int64   // current absolute offset
	curIdx   int     // current segment index
	cur      io.ReadCloser
	curOff   int64 // bytes read within the current segment
}

// OpenSegmentedReader opens a reader over [start, end] inclusive of an
// appendable object. start and end must be within [0, obj.Size).
func (b *LocalBackend) OpenSegmentedReader(bucket, key string, obj *Object, start, end int64) (*SegmentedReader, error) {
	if obj == nil || obj.Segments == nil {
		return nil, fmt.Errorf("object has no segments")
	}
	if start < 0 || end < start || end >= obj.Size {
		return nil, fmt.Errorf("invalid range [%d, %d] for size %d", start, end, obj.Size)
	}
	starts := make([]int64, len(obj.Segments))
	var acc int64
	for i, s := range obj.Segments {
		starts[i] = acc
		acc += s.Size
	}
	r := &SegmentedReader{
		backend:  b,
		bucket:   bucket,
		key:      key,
		segments: obj.Segments,
		starts:   starts,
		start:    start,
		end:      end,
		pos:      start,
	}
	if err := r.seekToOffset(start); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *SegmentedReader) seekToOffset(off int64) error {
	idx := 0
	for i := range r.segments {
		if off < r.starts[i]+r.segments[i].Size {
			idx = i
			break
		}
		idx = i + 1
	}
	if idx >= len(r.segments) {
		return fmt.Errorf("offset %d beyond segments", off)
	}
	r.curIdx = idx
	r.curOff = off - r.starts[idx]
	return r.openCurrent()
}

func (r *SegmentedReader) openCurrent() error {
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
	seg := r.segments[r.curIdx]
	path := r.backend.segmentPath(r.bucket, r.key, seg.BlobID)
	var rc io.ReadCloser
	if r.backend.segEnc != nil {
		f, err := openEncryptedObjectFile(path, r.backend.segEnc, segmentFileAADFields(r.bucket, r.key, seg.BlobID), seg.Size)
		if err != nil {
			return err
		}
		rc = f
	} else {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		rc = f
	}
	if r.curOff > 0 {
		if _, err := io.CopyN(io.Discard, rc, r.curOff); err != nil {
			rc.Close()
			return err
		}
	}
	r.cur = rc
	return nil
}

func (r *SegmentedReader) Read(p []byte) (int, error) {
	if r.pos > r.end {
		return 0, io.EOF
	}
	if r.cur == nil {
		return 0, io.EOF
	}
	remaining := r.end - r.pos + 1
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.cur.Read(p)
	r.pos += int64(n)
	r.curOff += int64(n)
	if err == io.EOF && r.pos <= r.end {
		r.curIdx++
		if r.curIdx >= len(r.segments) {
			return n, io.EOF
		}
		r.curOff = 0
		if openErr := r.openCurrent(); openErr != nil {
			return n, openErr
		}
		return n, nil
	}
	return n, err
}

func (r *SegmentedReader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		r.cur = nil
		return err
	}
	return nil
}
