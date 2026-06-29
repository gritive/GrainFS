// Package storage — SegmentWriter implements the streaming PUT pipeline:
// chunker → bounded worker pool → aggregator. Memory is bounded by
// chunkSize * (workers + 1) regardless of object size.
package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"
)

// DefaultChunkSize is the standard segment size for streaming PUTs.
const DefaultChunkSize = 16 << 20 // 16 MiB

// DefaultPutWorkers is the per-request concurrency for segment writes.
const DefaultPutWorkers = 8

const minPooledSegmentChunkSize = 8 << 20

var segmentChunkPool = sync.Pool{
	New: func() any {
		buf := make([]byte, DefaultChunkSize)
		return &buf
	},
}

type ownedSegmentChunk struct {
	body []byte
	ref  *[]byte
}

func acquireSegmentChunk(capacity, chunkSize int) ownedSegmentChunk {
	if chunkSize == DefaultChunkSize && capacity >= minPooledSegmentChunkSize && capacity <= DefaultChunkSize {
		ref := segmentChunkPool.Get().(*[]byte)
		return ownedSegmentChunk{body: (*ref)[:capacity], ref: ref}
	}
	return ownedSegmentChunk{body: make([]byte, capacity)}
}

func (c ownedSegmentChunk) release() {
	if c.ref == nil {
		return
	}
	clear(c.body)
	*c.ref = (*c.ref)[:cap(*c.ref)]
	segmentChunkPool.Put(c.ref)
}

type segmentWriteResult struct {
	idx int
	ref SegmentRef
	err error
}

// SegmentWriter streams an io.Reader through a chunker, worker pool, and
// aggregator to build an Object made of N SegmentRefs.
type SegmentWriter struct {
	backend   segmentWriterBackend
	chunkSize int
	workers   int
}

// segmentWriterBackend abstracts the storage primitive so the writer is
// testable without a full LocalBackend.
type segmentWriterBackend interface {
	WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (SegmentRef, error)
}

type segmentBytesWriterBackend interface {
	// WriteSegmentBytes may read data only during the call. SegmentWriter owns
	// the backing buffer and may reuse it as soon as this method returns.
	WriteSegmentBytes(ctx context.Context, bucket, key string, idx int, data []byte) (SegmentRef, error)
}

// NewSegmentWriter constructs a writer with the standard defaults.
func NewSegmentWriter(b segmentWriterBackend) *SegmentWriter {
	return &SegmentWriter{backend: b, chunkSize: DefaultChunkSize, workers: DefaultPutWorkers}
}

// NewSegmentWriterWithChunkSize constructs a writer with a custom chunk size.
func NewSegmentWriterWithChunkSize(b segmentWriterBackend, chunkSize int) *SegmentWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &SegmentWriter{backend: b, chunkSize: chunkSize, workers: DefaultPutWorkers}
}

func NewSegmentWriterWithChunkSizeAndWorkers(b segmentWriterBackend, chunkSize, workers int) *SegmentWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if workers <= 0 {
		workers = 1
	}
	return &SegmentWriter{backend: b, chunkSize: chunkSize, workers: workers}
}

// Write streams r, returning an Object with segments populated and
// Object.ETag set to the bucket-aware whole-object hash (MD5 for S3-exposed
// buckets, xxhash3 for internal __grainfs_* buckets) — the simple-PUT rule.
//
// On any worker or chunker error, the entire PUT fails: the caller MUST NOT
// persist the returned Object (which is nil). Any segment blobs already
// written to disk become orphans cleaned up by the scrubber.
func (w *SegmentWriter) Write(ctx context.Context, bucket, key, contentType string, r io.Reader) (*Object, error) {
	return w.WriteSized(ctx, bucket, key, contentType, r, -1)
}

// WriteSized is Write with an advisory object size (typically the request
// Content-Length). The hint right-sizes the chunk buffers so a small object
// does not allocate a full DefaultChunkSize buffer when the body cannot be
// size-sniffed (an opaque HTTP stream, a packblob passthrough io.MultiReader).
// The hint is NOT trusted as a hard length: a body longer than the hint is
// still written in full, and a shorter body persists only its real bytes
// (chunkLoop confirms the boundary with a 1-byte probe). Pass a negative hint
// when the size is unknown.
func (w *SegmentWriter) WriteSized(ctx context.Context, bucket, key, contentType string, r io.Reader, sizeHint int64) (*Object, error) {
	// Bucket-aware whole-object ETag: MD5 for S3-exposed buckets, xxhash3 for
	// internal __grainfs_* buckets (whose ETag is the EC-rewrap corruption
	// oracle) — matches spoolHashForBucket so chunked PUTs keep ETag parity.
	wholeHash, releaseHash := hashForBucket(bucket)
	defer releaseHash()
	tee := io.TeeReader(r, wholeHash)

	workCh := make(chan chunkJob, w.workers)
	resultCh := make(chan segmentWriteResult, w.workers)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range workCh {
				var (
					ref SegmentRef
					err error
				)
				if bw, ok := w.backend.(segmentBytesWriterBackend); ok {
					ref, err = bw.WriteSegmentBytes(cctx, bucket, key, job.idx, job.body)
				} else {
					ref, err = w.backend.WriteSegment(cctx, bucket, key, job.idx, bytes.NewReader(job.body))
				}
				if job.release != nil {
					job.release()
				}
				resultCh <- segmentWriteResult{idx: job.idx, ref: ref, err: err}
			}
		}()
	}

	chunkerErr := make(chan error, 1)
	go func() {
		firstChunkSize, exactFirstChunk := firstChunkBufferSize(r, w.chunkSize)
		chunkerErr <- w.chunkLoop(cctx, tee, workCh, firstChunkSize, exactFirstChunk, sizeHint)
		close(workCh)
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]segmentWriteResult, 0, 64)
	var firstErr error
	for res := range resultCh {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
			cancel()
		}
		results = append(results, res)
	}
	if cerr := <-chunkerErr; cerr != nil && firstErr == nil {
		// Surface context.Canceled only if no upstream error was the trigger.
		if !errors.Is(cerr, context.Canceled) {
			firstErr = cerr
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	segs := make([]SegmentRef, len(results))
	var totalSize int64
	for i, res := range results {
		segs[i] = res.ref
		totalSize += res.ref.Size
	}

	obj := &Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  contentType,
		ETag:         etagFromHash(wholeHash),
		LastModified: time.Now().Unix(),
		Segments:     segs,
	}
	return obj, nil
}

type chunkJob struct {
	idx     int
	body    []byte
	release func()
}

// chunkLoop reads from r in chunkSize blocks and emits chunkJob values until EOF.
//
// Empty-object case: zero-byte input must still produce ONE empty trailing
// segment so Object.Segments is never empty.
//
// Error handling: we fill the buffer with a manual io.Read loop (not
// io.ReadFull) so we can distinguish a clean io.EOF from the underlying
// reader (end-of-stream) from any other error (truncated input,
// io.ErrUnexpectedEOF surfaced by the upstream, etc.). io.ReadFull would
// rewrite EOF→ErrUnexpectedEOF mid-buffer, making real ErrUnexpectedEOF
// errors from the source indistinguishable from a normal short final chunk.
func (w *SegmentWriter) chunkLoop(ctx context.Context, r io.Reader, workCh chan<- chunkJob, firstChunkSize int, exactFirstChunk bool, sizeHint int64) error {
	emit := func(idx int, body []byte, release func()) error {
		select {
		case workCh <- chunkJob{idx: idx, body: body, release: release}:
			return nil
		case <-ctx.Done():
			if release != nil {
				release()
			}
			return ctx.Err()
		}
	}

	idx := 0
	// hintRemaining sizes every chunk to the advisory Content-Length not yet
	// read, so a small object never allocates a full DefaultChunkSize buffer.
	// <0 disables hinting (unknown size, exact-Len reader, or the hint proved
	// short). The exact-Len fast path owns idx 0 and keeps hinting off.
	hintRemaining := int64(-1)
	if sizeHint >= 0 && !exactFirstChunk {
		hintRemaining = sizeHint
	}
	// carry holds bytes read by the EOF probe when the body outran its hint;
	// they are prepended to the next full-size chunk rather than emitted as a
	// tiny segment.
	var carry []byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if idx == 0 && exactFirstChunk && firstChunkSize == 0 {
			return emit(idx, nil, nil) // empty object, exact
		}

		// Decide this chunk's read capacity (excluding any carried prefix).
		capacity := w.chunkSize
		probe := false
		switch {
		case idx == 0 && firstChunkSize > 0 && firstChunkSize < capacity:
			capacity = firstChunkSize
		case hintRemaining == 0:
			// Exactly the hinted bytes have been read; confirm EOF with a 1-byte
			// probe instead of a fresh DefaultChunkSize buffer.
			capacity = 1
			probe = true
		case hintRemaining > 0 && hintRemaining < int64(capacity):
			capacity = int(hintRemaining)
		}

		chunk := acquireSegmentChunk(len(carry)+capacity, w.chunkSize)
		body := chunk.body
		copy(body, carry)
		carried := len(carry)
		carry = nil
		n, readErr := fillChunk(r, body[carried:])
		total := carried + n

		if probe {
			if total == 0 {
				// Clean EOF exactly at the hinted length. A zero-length object
				// (hint 0, nothing emitted yet) still owes its one empty segment.
				if idx == 0 {
					chunk.release()
					return emit(idx, nil, nil)
				}
				chunk.release()
				return nil
			}
			// Body exceeds its hint: stop trusting it and carry the probed
			// byte(s) into a real chunk rather than a 1-byte segment.
			hintRemaining = -1
			if errors.Is(readErr, io.EOF) {
				return emit(idx, body[:total], chunk.release)
			}
			if readErr != nil {
				chunk.release()
				return readErr
			}
			carry = append([]byte(nil), body[:total]...)
			chunk.release()
			continue
		}

		// Empty-object case: first iteration, no bytes, clean EOF.
		if total == 0 && errors.Is(readErr, io.EOF) && idx == 0 {
			chunk.release()
			return emit(idx, nil, nil)
		}
		if total > 0 {
			if err := emit(idx, body[:total], chunk.release); err != nil {
				return err
			}
			idx++
			if hintRemaining > 0 {
				if hintRemaining -= int64(total); hintRemaining < 0 {
					hintRemaining = 0
				}
			}
			if exactFirstChunk {
				return nil
			}
		}
		// Clean end-of-stream from the source.
		if errors.Is(readErr, io.EOF) {
			if total == 0 {
				chunk.release()
			}
			return nil
		}
		if readErr != nil {
			if total == 0 {
				chunk.release()
			}
			return readErr
		}
	}
}

// firstChunkBufferSize right-sizes the first chunk buffer to the object size
// when the source reader reports an authoritative remaining length. Any reader
// exposing `Len() int` with bytes-remaining semantics qualifies — the stdlib
// in-memory readers (*bytes.Reader, *bytes.Buffer, *strings.Reader) and the
// server's buffered PUT body (*putObjectBodyReader). Opaque streams (an HTTP
// BodyStream, an aws-chunked decoder) have no Len() and fall back to the full
// DefaultChunkSize. The length is only trusted from in-memory buffers where it
// is exact, so this never truncates a stream whose real size differs.
func firstChunkBufferSize(r io.Reader, defaultSize int) (int, bool) {
	if lr, ok := r.(interface{ Len() int }); ok {
		n := lr.Len()
		return n, n <= defaultSize
	}
	return defaultSize, false
}

// fillChunk reads up to len(buf) bytes from r using a plain io.Read loop.
// Returns (n, io.EOF) when the source reaches a clean end (whether before or
// after partially filling the buffer) and (n, err) for any non-EOF error.
// Unlike io.ReadFull, EOF is never rewritten to io.ErrUnexpectedEOF, so a real
// ErrUnexpectedEOF from the source is preserved verbatim.
func fillChunk(r io.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
