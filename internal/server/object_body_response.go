package server

import (
	"fmt"
	"io"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

const bufferedObjectBodyLimit = 128 * 1024

type rawBodyReadCloser interface {
	io.ReadCloser
	RawBody() []byte
}

func writeObjectBody(c *app.RequestContext, rc io.ReadCloser, obj *storage.Object, rangeHeader string) (bool, error) {
	if rangeHeader != "" {
		start, end, ok := parseByteRange(rangeHeader, obj.Size)
		if !ok {
			c.Status(consts.StatusRequestedRangeNotSatisfiable)
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
			return false, nil
		}

		if seeker, ok := rc.(io.Seeker); ok {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				return false, err
			}
		} else {
			if _, err := io.CopyN(io.Discard, rc, start); err != nil {
				return false, err
			}
		}

		length := end - start + 1
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
		c.Header("Content-Length", strconv.FormatInt(length, 10))
		c.Set(auditBytesOutKey, length)
		c.Response.SetBodyStream(newExactLengthReadCloser(rc, length), int(length))
		c.Status(consts.StatusPartialContent)
		return true, nil
	}

	c.Set(auditBytesOutKey, obj.Size)
	if obj.Size > bufferedObjectBodyLimit {
		c.Response.SetBodyStream(newExactLengthReadCloser(rc, obj.Size), int(obj.Size))
		c.Status(consts.StatusOK)
		return true, nil
	}

	c.Header("Content-Length", strconv.FormatInt(obj.Size, 10))
	if rawReader, ok := rc.(rawBodyReadCloser); ok {
		data := rawReader.RawBody()
		if int64(len(data)) == obj.Size {
			c.Header("Content-Type", obj.ContentType)
			c.Response.SetBodyRaw(data)
			c.Status(consts.StatusOK)
			return false, nil
		}
	}
	// Pre-allocate the exact-size body buffer in one shot. The old io.ReadAll
	// over an exactLengthReadCloser grew its buffer geometrically (~16 doublings
	// to reach a 64 KiB warp object) and wrapped rc in a length-limiting reader,
	// stacking up ~16 throwaway allocations per request. ReadFull on a properly
	// sized buffer reads exactly obj.Size bytes (or returns io.ErrUnexpectedEOF
	// if the backend reader is short, which is preferable to silently emitting
	// a response shorter than the Content-Length header advertised).
	data := make([]byte, obj.Size)
	if _, err := io.ReadFull(rc, data); err != nil {
		return false, err
	}
	c.Header("Content-Type", obj.ContentType)
	c.Response.SetBodyRaw(data)
	c.Status(consts.StatusOK)
	return false, nil
}

type exactLengthReadCloser struct {
	rc        io.ReadCloser
	remaining int64
}

func newExactLengthReadCloser(rc io.ReadCloser, length int64) io.ReadCloser {
	if length < 0 {
		length = 0
	}
	return &exactLengthReadCloser{rc: rc, remaining: length}
}

func (r *exactLengthReadCloser) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.rc.Read(p)
	r.remaining -= int64(n)
	if r.remaining == 0 {
		return n, nil
	}
	return n, err
}

func (r *exactLengthReadCloser) Close() error {
	return r.rc.Close()
}
